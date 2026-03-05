"""Legacy pipeline adapter — runs the arcpy-based mass_calc.py as a subprocess.

Maps OSS config keys to legacy equivalents and launches the legacy pipeline
using ArcGIS Pro Python, parsing stdout for timing markers and log lines.
"""

import json
import logging
import os
import re
import subprocess
import tempfile
import time

logger = logging.getLogger(__name__)

# Timing marker pattern: @@TIMER:stage_name:start@@ / @@TIMER:stage_name:end@@
_TIMER_PATTERN = re.compile(r"@@TIMER:(\w+):(start|end)@@")

# Default path to legacy mass_calc.py
_LEGACY_SCRIPT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "bbb_mass_extraction"
)


def build_legacy_config(unified_config: dict) -> dict:
    """Map OSS config keys to legacy mass_calc.py equivalents.

    Returns a dict suitable for writing as --config-json input.
    """
    return {
        "MODEL_FOLDER_PATH": unified_config.get("MODEL_FOLDER_PATH", ""),
        "TERRAIN_PATH": unified_config.get("TERRAIN_PATH", ""),
        "BERG_PATH": unified_config.get("BERG_PATH", ""),
        "CELL_SIZE": unified_config.get("CELL_SIZE", 0.2),
        "CRS": unified_config.get("CRS", "EPSG:25832"),
        "GRID_CELL_SIZE": unified_config.get("GRID_CELL_SIZE", 200),
        "SPORSYSTEM_Z_OFFSET": unified_config.get("SPORSYSTEM_Z_OFFSET", -0.9),
        "ROCK_SLOPE_FACTOR": unified_config.get("ROCK_SLOPE_FACTOR", 10.0),
        "SOIL_SLOPE_DIVISOR": unified_config.get("SOIL_SLOPE_DIVISOR", 1.5),
        "BUFFER_DISTANCE": unified_config.get("BUFFER_DISTANCE", 1.0),
        "ROCK_DENSITY": unified_config.get("ROCK_DENSITY", 0.7),
        "SEDIMENT_DIESEL_FACTOR": unified_config.get("SEDIMENT_DIESEL_FACTOR", 1.98),
        "TUNNEL_ROCK_DENSITY": unified_config.get("TUNNEL_ROCK_DENSITY", 1.8),
        "MAX_CORES": unified_config.get("MAX_CORES", 12),
        "MAX_MODEL_FILES": unified_config.get("MAX_MODEL_FILES", 0),
    }


def is_legacy_available(python_path: str | None = None) -> bool:
    """Check if the legacy ArcGIS Pro Python environment exists."""
    if python_path is None:
        python_path = _get_legacy_python()
    return os.path.isfile(python_path)


def _get_legacy_python(config: dict | None = None) -> str:
    """Get the path to ArcGIS Pro Python."""
    if config and config.get("LEGACY_PYTHON_PATH"):
        return config["LEGACY_PYTHON_PATH"]
    return r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"


def _get_legacy_script() -> str:
    """Get the path to legacy mass_calc.py."""
    candidates = [
        os.path.join(_LEGACY_SCRIPT_DIR, "mass_calc.py"),
        os.path.join(os.path.expanduser("~"), "bbb_mass_extraction", "mass_calc.py"),
    ]
    for path in candidates:
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(
        "Legacy mass_calc.py not found. Expected at: " + " or ".join(candidates)
    )


def run_legacy_pipeline(
    config: dict,
    file_manifest: dict | None = None,
    output_dir: str = "",
    log_queue=None,
    cancel_event=None,
    timeout: int = 7200,
) -> dict:
    """Launch the legacy arcpy pipeline as a subprocess.

    Parameters
    ----------
    config : dict
        Unified config (will be mapped to legacy format).
    file_manifest : dict, optional
        File manifest from resolve_files() for consistent inputs.
    output_dir : str
        Where to write legacy output.
    log_queue : multiprocessing.Queue, optional
        Queue for forwarding log lines.
    cancel_event : multiprocessing.Event, optional
        If set, kills the subprocess.
    timeout : int
        Maximum runtime in seconds.

    Returns
    -------
    dict with keys: total_time_s, stages, volumes, return_code
    """
    legacy_python = _get_legacy_python(config)
    legacy_script = _get_legacy_script()

    if not os.path.isfile(legacy_python):
        raise FileNotFoundError(f"ArcGIS Pro Python not found: {legacy_python}")

    # Write config and manifest to temp files
    legacy_cfg = build_legacy_config(config)
    config_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, prefix="legacy_config_"
    )
    json.dump(legacy_cfg, config_file, indent=2)
    config_file.close()

    manifest_file = None
    if file_manifest:
        manifest_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, prefix="legacy_manifest_"
        )
        json.dump(file_manifest, manifest_file, indent=2)
        manifest_file.close()

    # Build command
    cmd = [legacy_python, legacy_script, "--config-json", config_file.name]
    if output_dir:
        cmd.extend(["--output-dir", output_dir])
    if manifest_file:
        cmd.extend(["--file-manifest", manifest_file.name])

    logger.info("Launching legacy pipeline: %s", " ".join(cmd[:3]))

    # Run subprocess
    t0 = time.time()
    stages: dict[str, dict] = {}
    volumes: dict[str, float] = {}

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=os.path.dirname(legacy_script),
        )

        for line in proc.stdout:
            line = line.rstrip("\n")

            # Check for cancellation
            if cancel_event and cancel_event.is_set():
                proc.kill()
                proc.wait()
                raise InterruptedError("Legacy pipeline cancelled")

            # Parse timing markers
            timer_match = _TIMER_PATTERN.search(line)
            if timer_match:
                stage_name = timer_match.group(1)
                event = timer_match.group(2)
                ts = time.time()
                if event == "start":
                    stages[stage_name] = {"start": ts}
                elif event == "end" and stage_name in stages:
                    stages[stage_name]["end"] = ts
                    stages[stage_name]["time_s"] = ts - stages[stage_name]["start"]
                continue  # Don't forward marker lines

            # Forward log line
            if log_queue:
                try:
                    log_queue.put_nowait({
                        "level": "INFO",
                        "message": f"[LEGACY] {line}",
                        "time": time.time(),
                    })
                except Exception:
                    pass

            logger.info("[LEGACY] %s", line)

        proc.wait(timeout=timeout)
        return_code = proc.returncode

    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        return_code = -1
        logger.error("Legacy pipeline timed out after %ds", timeout)
    finally:
        # Clean up temp files
        try:
            os.unlink(config_file.name)
        except OSError:
            pass
        if manifest_file:
            try:
                os.unlink(manifest_file.name)
            except OSError:
                pass

    total_time = time.time() - t0

    # Try to read volumes from legacy output
    if output_dir:
        volumes = _read_legacy_volumes(output_dir)

    return {
        "total_time_s": total_time,
        "stages": {k: {"time_s": v.get("time_s", 0)} for k, v in stages.items()},
        "volumes": volumes,
        "return_code": return_code,
    }


def _read_legacy_volumes(output_dir: str) -> dict[str, float]:
    """Read volumes from legacy output CSV or Excel."""
    import csv

    csv_path = os.path.join(output_dir, "volumes.csv")
    if os.path.isfile(csv_path):
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    return {k: float(v) for k, v in row.items() if v}
        except Exception as e:
            logger.warning("Failed to read legacy volumes.csv: %s", e)

    return {}
