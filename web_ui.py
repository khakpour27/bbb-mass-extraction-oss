"""FastAPI web UI for the BBB mass extraction pipeline.

Supports OSS, Legacy, and Benchmark run modes with real-time progress
via SSE, config management, benchmark comparison, and AGOL publishing.

Usage:
    python web_ui.py
    # Opens http://localhost:8502
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
from benchmark_worker import BenchmarkWorker
from legacy_adapter import is_legacy_available
from publish_adapter import is_publish_available

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(title="BBB Mass Extraction")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

worker = BenchmarkWorker()

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
        "TEST_AREA_PREFIX",
    ],
    "Slopes": [
        "ROCK_SLOPE_FACTOR", "SOIL_SLOPE_DIVISOR", "BUFFER_DISTANCE",
    ],
    "Density": [
        "ROCK_DENSITY", "SEDIMENT_DIESEL_FACTOR", "TUNNEL_ROCK_DENSITY",
    ],
    "Benchmark": [
        "LEGACY_PYTHON_PATH",
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
        cfg[key] = getattr(_config_module, key, "")
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
async def start_run(request: Request):
    if worker.is_running:
        return JSONResponse({"error": "Pipeline is already running"}, status_code=409)

    # Parse mode from request body
    mode = "oss"
    try:
        body = await request.json()
        mode = body.get("mode", "oss")
    except Exception:
        pass  # default to oss if no body

    if mode not in ("oss", "legacy", "benchmark"):
        return JSONResponse({"error": f"Invalid mode: {mode}"}, status_code=400)

    # Check legacy availability for legacy/benchmark modes
    config = _get_effective_config()
    if mode in ("legacy", "benchmark"):
        if not is_legacy_available(config.get("LEGACY_PYTHON_PATH")):
            return JSONResponse(
                {"error": "ArcGIS Pro Python not available for legacy mode"},
                status_code=400,
            )

    started = worker.start(config=config, mode=mode)
    if not started:
        return JSONResponse({"error": "Failed to start pipeline"}, status_code=500)

    return {"ok": True, "message": f"Pipeline started in {mode} mode", "mode": mode}


@app.post("/api/stop")
async def stop_run():
    if not worker.is_running:
        return JSONResponse({"error": "Pipeline is not running"}, status_code=409)

    worker.stop()
    return {"ok": True, "message": "Cancel signal sent"}


@app.get("/api/status")
async def get_status():
    return worker.get_status()


@app.get("/api/legacy/available")
async def check_legacy():
    """Check if ArcGIS Pro Python environment is available."""
    config = _get_effective_config()
    python_path = config.get("LEGACY_PYTHON_PATH", "")
    available = is_legacy_available(python_path)
    publish_ok = is_publish_available(python_path) if available else False
    return {
        "available": available,
        "python_path": python_path,
        "publish_available": publish_ok,
    }


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
    """List completed output folders with benchmark/publish metadata."""
    results = []
    if OUTPUT_DIR.is_dir():
        for d in sorted(OUTPUT_DIR.iterdir(), reverse=True):
            if d.is_dir() and d.name.startswith("results_"):
                files = [f.name for f in d.iterdir() if f.is_file()]
                entry = {
                    "name": d.name,
                    "files": files,
                    "has_benchmark": "benchmark_results.json" in files,
                    "has_oss": (d / "oss").is_dir(),
                    "has_legacy": (d / "legacy").is_dir(),
                }
                results.append(entry)
    return {"results": results}


@app.get("/api/results/{name}/{file}")
async def download_result(name: str, file: str):
    """Download a specific result file."""
    path = OUTPUT_DIR / name / file
    if not path.is_file():
        return JSONResponse({"error": "File not found"}, status_code=404)
    try:
        path.resolve().relative_to(OUTPUT_DIR.resolve())
    except ValueError:
        return JSONResponse({"error": "Invalid path"}, status_code=400)
    return FileResponse(path, filename=file)


@app.get("/api/benchmark/{run_name}")
async def get_benchmark_results(run_name: str):
    """Return benchmark_results.json for a specific run."""
    path = OUTPUT_DIR / run_name / "benchmark_results.json"
    if not path.is_file():
        return JSONResponse({"error": "Benchmark results not found"}, status_code=404)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/benchmark/{run_name}/diff_raster")
async def download_diff_raster(run_name: str):
    """Download the difference GeoTIFF for a benchmark run."""
    path = OUTPUT_DIR / run_name / "diff_FINAL_RESULT.tif"
    if not path.is_file():
        return JSONResponse({"error": "Diff raster not found"}, status_code=404)
    return FileResponse(path, filename="diff_FINAL_RESULT.tif")


@app.post("/api/publish/{run_name}")
async def publish_to_agol(run_name: str):
    """Trigger AGOL publishing for a completed run's legacy output."""
    config = _get_effective_config()
    python_path = config.get("LEGACY_PYTHON_PATH", "")

    if not is_legacy_available(python_path):
        return JSONResponse(
            {"error": "ArcGIS Pro Python not available"},
            status_code=400,
        )

    run_dir = OUTPUT_DIR / run_name
    # Check for legacy output
    legacy_dir = run_dir / "legacy"
    if legacy_dir.is_dir():
        output_dir = str(legacy_dir)
    elif run_dir.is_dir():
        output_dir = str(run_dir)
    else:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    try:
        from publish_adapter import run_publish
        result = run_publish(config, output_dir)
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


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
