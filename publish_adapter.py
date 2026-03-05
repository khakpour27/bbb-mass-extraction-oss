"""AGOL publishing adapter — wraps legacy publish.py as a subprocess.

Requires arcpy — only available when ArcGIS Pro Python is present.
Triggered manually from the web UI after a successful pipeline run.
"""

import logging
import os
import subprocess
import time

logger = logging.getLogger(__name__)

_LEGACY_SCRIPT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "bbb_mass_extraction"
)


def _get_publish_script() -> str:
    """Get the path to legacy publish.py."""
    candidates = [
        os.path.join(_LEGACY_SCRIPT_DIR, "publish.py"),
        os.path.join(os.path.expanduser("~"), "bbb_mass_extraction", "publish.py"),
    ]
    for path in candidates:
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(
        "Legacy publish.py not found. Expected at: " + " or ".join(candidates)
    )


def is_publish_available(legacy_python: str) -> bool:
    """Check if AGOL publishing is available (legacy Python + publish.py exist)."""
    if not os.path.isfile(legacy_python):
        return False
    try:
        _get_publish_script()
        return True
    except FileNotFoundError:
        return False


def run_publish(
    config: dict,
    output_dir: str,
    log_queue=None,
    timeout: int = 1800,
) -> dict:
    """Launch publish.py as a subprocess using ArcGIS Pro Python.

    Parameters
    ----------
    config : dict
        Pipeline config (needs LEGACY_PYTHON_PATH).
    output_dir : str
        Path to the results folder to publish.
    log_queue : multiprocessing.Queue, optional
        Queue for forwarding log lines.
    timeout : int
        Maximum runtime in seconds.

    Returns
    -------
    dict with keys: success, return_code, total_time_s, error
    """
    legacy_python = config.get(
        "LEGACY_PYTHON_PATH",
        r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe",
    )
    publish_script = _get_publish_script()

    if not os.path.isfile(legacy_python):
        return {
            "success": False,
            "return_code": -1,
            "total_time_s": 0,
            "error": f"ArcGIS Pro Python not found: {legacy_python}",
        }

    cmd = [legacy_python, publish_script, "--output-dir", output_dir]
    logger.info("Launching AGOL publish: %s", " ".join(cmd[:3]))

    t0 = time.time()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=os.path.dirname(publish_script),
        )

        for line in proc.stdout:
            line = line.rstrip("\n")
            if log_queue:
                try:
                    log_queue.put_nowait({
                        "level": "INFO",
                        "message": f"[PUBLISH] {line}",
                        "time": time.time(),
                    })
                except Exception:
                    pass
            logger.info("[PUBLISH] %s", line)

        proc.wait(timeout=timeout)
        return_code = proc.returncode

    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        return_code = -1
        logger.error("Publish timed out after %ds", timeout)

    total_time = time.time() - t0

    return {
        "success": return_code == 0,
        "return_code": return_code,
        "total_time_s": total_time,
        "error": None if return_code == 0 else f"Exited with code {return_code}",
    }
