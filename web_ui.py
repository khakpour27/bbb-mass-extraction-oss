"""FastAPI web UI for the BBB mass extraction pipeline.

Usage:
    python web_ui.py
    # Opens http://localhost:8501
"""

import asyncio
import json
import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

import config as _config_module
from pipeline_worker import PipelineWorker

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(title="BBB Mass Extraction")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

worker = PipelineWorker()

CONFIG_OVERRIDES_PATH = Path(__file__).parent / "config_overrides.json"
OUTPUT_DIR = Path(__file__).parent / "output"

# Config keys grouped for the UI
CONFIG_GROUPS = {
    "Paths": [
        "MODEL_FOLDER_PATH", "TERRAIN_PATH", "BERG_PATH",
        "GRID_PATH", "MUNKEBOTN_MASK",
    ],
    "Processing": [
        "CELL_SIZE", "CRS", "GRID_CELL_SIZE", "SPORSYSTEM_Z_OFFSET",
        "MAX_TILE_DIMENSION", "MAX_CORES", "MAX_MODEL_FILES",
    ],
    "Slopes": [
        "ROCK_SLOPE_FACTOR", "SOIL_SLOPE_DIVISOR", "BUFFER_DISTANCE",
    ],
    "Density": [
        "ROCK_DENSITY", "SEDIMENT_DIESEL_FACTOR", "TUNNEL_ROCK_DENSITY",
    ],
}

# Flat list of all config keys
ALL_CONFIG_KEYS = [k for group in CONFIG_GROUPS.values() for k in group]

# Type hints for config values (for input type in the UI)
CONFIG_TYPES = {}
for key in ALL_CONFIG_KEYS:
    val = getattr(_config_module, key, None)
    if isinstance(val, float):
        CONFIG_TYPES[key] = "number"
    elif isinstance(val, int):
        CONFIG_TYPES[key] = "integer"
    else:
        CONFIG_TYPES[key] = "text"

# Presets
PRESETS = {
    "test": {
        "CELL_SIZE": 1.0,
        "MAX_CORES": 2,
        "GRID_CELL_SIZE": 200,
        "MAX_MODEL_FILES": 10,
    },
    "prod": {
        # Original production values — just use the defaults from config.py
    },
}


def _load_overrides() -> dict:
    """Load user overrides from config_overrides.json."""
    if CONFIG_OVERRIDES_PATH.is_file():
        try:
            return json.loads(CONFIG_OVERRIDES_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_overrides(overrides: dict) -> None:
    """Persist user overrides to config_overrides.json."""
    CONFIG_OVERRIDES_PATH.write_text(
        json.dumps(overrides, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _get_effective_config() -> dict:
    """Module defaults merged with user overrides."""
    cfg = {}
    for key in ALL_CONFIG_KEYS:
        cfg[key] = getattr(_config_module, key)
    overrides = _load_overrides()
    for key, val in overrides.items():
        if key in cfg:
            cfg[key] = val
    return cfg


def _coerce_value(key: str, raw: str):
    """Coerce a string value to the appropriate Python type."""
    default = getattr(_config_module, key, None)
    if isinstance(default, float):
        return float(raw)
    elif isinstance(default, int):
        return int(raw)
    return raw


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "config_groups": CONFIG_GROUPS,
        "config_types": CONFIG_TYPES,
    })


@app.get("/api/config")
async def get_config():
    cfg = _get_effective_config()
    return {
        "values": cfg,
        "groups": CONFIG_GROUPS,
        "types": CONFIG_TYPES,
    }


@app.post("/api/config")
async def set_config(request: Request):
    body = await request.json()
    values = body.get("values", {})

    # Load existing overrides and merge
    overrides = _load_overrides()
    for key, raw in values.items():
        if key in ALL_CONFIG_KEYS:
            overrides[key] = _coerce_value(key, str(raw))

    _save_overrides(overrides)
    return {"ok": True, "config": _get_effective_config()}


@app.post("/api/config/preset/{name}")
async def apply_preset(name: str):
    if name not in PRESETS:
        return JSONResponse({"error": f"Unknown preset: {name}"}, status_code=400)

    if name == "prod":
        # Reset to defaults by clearing overrides
        _save_overrides({})
    else:
        preset = PRESETS[name]
        overrides = _load_overrides()
        overrides.update(preset)
        _save_overrides(overrides)

    return {"ok": True, "config": _get_effective_config()}


@app.post("/api/config/reset")
async def reset_config():
    _save_overrides({})
    return {"ok": True, "config": _get_effective_config()}


@app.post("/api/run")
async def start_run():
    if worker.is_running:
        return JSONResponse({"error": "Pipeline is already running"}, status_code=409)

    config = _get_effective_config()
    started = worker.start(config=config)
    if not started:
        return JSONResponse({"error": "Failed to start pipeline"}, status_code=500)

    return {"ok": True, "message": "Pipeline started"}


@app.post("/api/stop")
async def stop_run():
    if not worker.is_running:
        return JSONResponse({"error": "Pipeline is not running"}, status_code=409)

    worker.stop()
    return {"ok": True, "message": "Cancel signal sent"}


@app.get("/api/status")
async def get_status():
    return worker.get_status()


@app.get("/api/logs")
async def stream_logs(request: Request):
    """SSE endpoint streaming log lines and progress updates."""
    async def event_generator():
        while True:
            if await request.is_disconnected():
                break

            entries = worker.drain_logs(max_items=50)
            for entry in entries:
                if entry.get("type") == "progress":
                    yield {
                        "event": "progress",
                        "data": json.dumps({
                            "step": entry["step"],
                            "pct": entry["pct"],
                        }),
                    }
                else:
                    yield {
                        "event": "log",
                        "data": json.dumps({
                            "level": entry.get("level", "INFO"),
                            "message": entry.get("message", ""),
                        }),
                    }

            # Send status heartbeat
            status = worker.get_status()
            yield {
                "event": "status",
                "data": json.dumps(status),
            }

            await asyncio.sleep(0.3)

    return EventSourceResponse(event_generator())


@app.get("/api/results")
async def list_results():
    """List completed output folders."""
    results = []
    if OUTPUT_DIR.is_dir():
        for d in sorted(OUTPUT_DIR.iterdir(), reverse=True):
            if d.is_dir() and d.name.startswith("results_"):
                files = [f.name for f in d.iterdir() if f.is_file()]
                results.append({
                    "name": d.name,
                    "files": files,
                })
    return {"results": results}


@app.get("/api/results/{name}/{file}")
async def download_result(name: str, file: str):
    """Download a specific result file."""
    path = OUTPUT_DIR / name / file
    if not path.is_file():
        return JSONResponse({"error": "File not found"}, status_code=404)
    # Prevent path traversal
    try:
        path.resolve().relative_to(OUTPUT_DIR.resolve())
    except ValueError:
        return JSONResponse({"error": "Invalid path"}, status_code=400)
    return FileResponse(path, filename=file)


# ── Lifecycle ─────────────────────────────────────────────────────────────────

@app.on_event("shutdown")
async def shutdown_event():
    """Terminate the pipeline subprocess (if any) on server shutdown."""
    if worker._process and worker._process.is_alive():
        worker._process.terminate()
        worker._process.join(timeout=3)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting BBB Mass Extraction Web UI...")
    print("Open http://localhost:8502 in your browser")
    uvicorn.run(app, host="0.0.0.0", port=8502, log_level="info")
