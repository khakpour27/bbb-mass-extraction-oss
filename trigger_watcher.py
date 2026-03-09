"""
trigger_watcher.py — File-based trigger for mass extraction pipeline.

Watches a SharePoint-synced folder for .trigger files. When one appears,
runs runner_v3.py and streams progress to status.json in the same folder.

Setup:
    1. Set TRIGGER_DIR to a SharePoint/OneDrive-synced folder
    2. Run as Windows Scheduled Task (on login, run indefinitely)
    3. From Power Automate: create a .trigger file in the folder to start a run

The .trigger file can contain JSON config:
    {"tier": "aggressive", "publish": true, "no_filter": false}
    Or be empty (uses defaults: moderate + publish).

Status output (synced back to SharePoint):
    status.json  — small, updates every 15s, for Power Automate polling
    run.log      — full pipeline log, copied every 30s
"""
import time
import os
import sys
import json
import shutil
import subprocess
import threading
import logging
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
TRIGGER_DIR = r"C:\Users\MHKK\DC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\pipeline_trigger"
PIPELINE_DIR = r"C:\Users\MHKK\bbb_mass_extraction"
PROPY = r"C:\Program Files\ArcGIS\Pro\bin\Python\scripts\propy.bat"
POLL_INTERVAL = 30  # seconds between checks for trigger files
LOG_SYNC_INTERVAL = 30  # seconds between log copies to synced folder
STATUS_INTERVAL = 15  # seconds between status.json updates

# ============================================================
# STATUS MANAGER
# ============================================================
class StatusManager:
    """Writes status.json to the synced folder at regular intervals."""

    def __init__(self, trigger_dir):
        self.status_path = os.path.join(trigger_dir, "status.json")
        self.log_mirror_path = os.path.join(trigger_dir, "run.log")
        self.state = {
            "status": "idle",
            "phase": None,
            "progress": None,
            "started_at": None,
            "updated_at": None,
            "elapsed_s": 0,
            "tier": None,
            "last_line": None,
            "volumes": None,
            "error": None,
        }
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._source_log = None
        self._thread = None

    def start(self, tier, source_log_path):
        """Begin a run — start background thread that syncs status + log."""
        with self._lock:
            self.state = {
                "status": "running",
                "phase": "starting",
                "progress": 0,
                "started_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "elapsed_s": 0,
                "tier": tier,
                "last_line": "Pipeline starting...",
                "volumes": None,
                "error": None,
            }
            self._source_log = source_log_path
            self._start_time = time.time()
        self._stop.clear()
        self._write_status()
        self._thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._thread.start()

    def update(self, **kwargs):
        """Update status fields (thread-safe)."""
        with self._lock:
            self.state.update(kwargs)
            self.state["updated_at"] = datetime.now().isoformat()
            self.state["elapsed_s"] = round(time.time() - self._start_time)

    def finish(self, success=True, error=None, volumes=None):
        """Mark run as complete."""
        with self._lock:
            self.state["status"] = "completed" if success else "failed"
            self.state["phase"] = "done" if success else "error"
            self.state["progress"] = 100 if success else self.state.get("progress", 0)
            self.state["updated_at"] = datetime.now().isoformat()
            self.state["elapsed_s"] = round(time.time() - self._start_time)
            if error:
                self.state["error"] = str(error)[:500]
            if volumes:
                self.state["volumes"] = volumes
        self._stop.set()
        self._write_status()
        self._sync_log()
        if self._thread:
            self._thread.join(timeout=5)

    def set_idle(self):
        """Reset to idle state."""
        with self._lock:
            self.state = {
                "status": "idle",
                "phase": None,
                "progress": None,
                "started_at": None,
                "updated_at": datetime.now().isoformat(),
                "elapsed_s": 0,
                "tier": None,
                "last_line": None,
                "volumes": self.state.get("volumes"),
                "error": None,
            }
        self._write_status()

    def _sync_loop(self):
        """Background loop: update status.json and mirror log."""
        last_log_sync = 0
        while not self._stop.is_set():
            self._write_status()
            now = time.time()
            if now - last_log_sync >= LOG_SYNC_INTERVAL:
                self._sync_log()
                last_log_sync = now
            self._stop.wait(STATUS_INTERVAL)

    def _write_status(self):
        """Write status.json atomically."""
        try:
            tmp = self.status_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.status_path)
        except Exception as e:
            logging.warning("Could not write status.json: %s", e)

    def _sync_log(self):
        """Copy current log to synced folder."""
        if self._source_log and os.path.exists(self._source_log):
            try:
                shutil.copy2(self._source_log, self.log_mirror_path)
            except Exception as e:
                logging.warning("Could not sync log: %s", e)


# ============================================================
# LOG TAILER — reads pipeline stdout and updates status
# ============================================================
PHASE_MARKERS = [
    ("IFC import (model) done", "ifc_import_model", 10),
    ("IFC import (tunnel) done", "ifc_import_tunnel", 15),
    ("Deep model filter done", "deep_model_filter", 20),
    ("IFC models to raster complete", "model_rasterization", 30),
    ("Creating tunnel mask", "tunnel_processing", 35),
    ("Clipping model raster", "tunnel_clip", 40),
    ("Converting terrain", "terrain_conversion", 45),
    ("Berg layers converted", "berg_conversion", 50),
    ("Phase A:", "grid_phase_a", 55),
    ("Phase A complete", "grid_phase_a_done", 65),
    ("Merging intermediate", "grid_barrier", 68),
    ("Phase B:", "grid_phase_b", 70),
    ("Phase B complete", "grid_phase_b_done", 80),
    ("Final result complete", "grid_done", 85),
    ("Calculating volumes", "volume_calc", 90),
    ("Volumes:", "volumes_done", 95),
    ("tunnel_vol.py", "tunnel_vol", 97),
    ("publish_v2.py", "publishing", 98),
]


def parse_output_line(line, status_mgr):
    """Parse a pipeline stdout line and update status accordingly."""
    line = line.strip()
    if not line:
        return

    status_mgr.update(last_line=line[:200])

    for marker, phase, progress in PHASE_MARKERS:
        if marker in line:
            status_mgr.update(phase=phase, progress=progress)
            break

    # Extract volumes if present
    if line.startswith("Volumes:"):
        try:
            parts = line.split("berg=")[1].split(",")
            berg = parts[0].strip().replace(" m3", "")
            sed = parts[1].split("=")[1].strip().replace(" m3", "")
            status_mgr.update(volumes={"berg_m3": berg, "sediment_m3": sed})
        except Exception:
            pass


# ============================================================
# PIPELINE RUNNER
# ============================================================
def run_pipeline(trigger_config, status_mgr):
    """Run the v3 pipeline with given config, streaming output to status."""
    tier = trigger_config.get("tier", "moderate")
    publish = trigger_config.get("publish", True)
    no_filter = trigger_config.get("no_filter", False)

    # Build command
    cmd = [PROPY, "runner_v3.py", f"--{tier}"]
    if publish:
        cmd.append("--publish")
    if no_filter:
        cmd.append("--no-filter")

    # Log file for this run
    run_log = os.path.join(PIPELINE_DIR, "v3_run.log")

    status_mgr.start(tier, run_log)
    logging.info("Starting pipeline: %s", " ".join(cmd))

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=PIPELINE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        with open(run_log, "w", encoding="utf-8") as log_f:
            for line in proc.stdout:
                log_f.write(line)
                log_f.flush()
                parse_output_line(line, status_mgr)

        proc.wait()

        if proc.returncode == 0:
            status_mgr.finish(success=True, volumes=status_mgr.state.get("volumes"))
            logging.info("Pipeline completed successfully")
        else:
            status_mgr.finish(success=False, error=f"Exit code {proc.returncode}")
            logging.error("Pipeline failed with exit code %d", proc.returncode)

    except Exception as e:
        status_mgr.finish(success=False, error=str(e))
        logging.error("Pipeline error: %s", e)


def parse_trigger_file(path):
    """Read config from trigger file. Returns dict."""
    try:
        with open(path, "r") as f:
            content = f.read().strip()
        if content:
            return json.loads(content)
    except (json.JSONDecodeError, Exception):
        pass
    return {}


# ============================================================
# MAIN LOOP
# ============================================================
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(PIPELINE_DIR, "trigger_watcher.log")),
            logging.StreamHandler(),
        ]
    )

    os.makedirs(TRIGGER_DIR, exist_ok=True)

    status_mgr = StatusManager(TRIGGER_DIR)
    status_mgr.set_idle()

    logging.info("Trigger watcher started. Watching: %s", TRIGGER_DIR)
    logging.info("Poll interval: %ds, status interval: %ds", POLL_INTERVAL, STATUS_INTERVAL)

    while True:
        try:
            trigger_files = [
                f for f in os.listdir(TRIGGER_DIR)
                if f.endswith(".trigger")
            ]

            if trigger_files and status_mgr.state["status"] != "running":
                # Take the first trigger file
                trigger_path = os.path.join(TRIGGER_DIR, trigger_files[0])
                config = parse_trigger_file(trigger_path)
                logging.info("Trigger detected: %s, config: %s", trigger_files[0], config)

                # Remove all trigger files
                for f in trigger_files:
                    try:
                        os.remove(os.path.join(TRIGGER_DIR, f))
                    except Exception:
                        pass

                # Run pipeline (blocks until done)
                run_pipeline(config, status_mgr)

        except Exception as e:
            logging.error("Watcher error: %s", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
