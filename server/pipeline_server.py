"""
pipeline_server.py — SharePoint-triggered pipeline server.

Polls a SharePoint folder via Graph API for trigger files, runs mass_calc_v3
pipeline, and writes live logs + manifest back to SharePoint under run-specific
subfolders.

SharePoint folder structure:
    pipeline_runs/
        trigger.json                ← Power Automate creates this to start a run
        RUN_20260308_143000/
            manifest.json           ← status, phase, progress, volumes (overwritten live)
            log.txt                 ← full pipeline output (overwritten live)
        RUN_20260308_160000/
            manifest.json
            log.txt

Install:
    pip install msal requests

Run:
    python pipeline_server.py               # foreground
    python pipeline_server.py --install     # install as Windows service (requires nssm)

Config:
    server/config.json  (Azure app + SharePoint + pipeline settings)
"""
import json
import logging
import os
import subprocess
import sys
import threading
import time
import argparse
from datetime import datetime

from graph_client import GraphClient

# ============================================================
# CONFIG
# ============================================================
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# PHASE DETECTION — maps stdout lines to progress
# ============================================================
PHASE_MAP = [
    ("Checking cache", "ifc_cache", 5),
    ("IFC import (model) done", "ifc_import_model", 10),
    ("IFC import (tunnel) done", "ifc_import_tunnel", 15),
    ("Running deep model filter", "deep_model_filter", 18),
    ("Deep model filter done", "deep_model_filter_done", 22),
    ("WARNING: --no-filter active", "no_filter_warning", 20),
    ("Sinking Sporsystem", "sporsystem_fix", 24),
    ("Converting", "model_rasterize", 26),
    ("Enumerating:", "model_enumerate", 28),
    ("IFC models to raster complete", "model_rasterize_done", 32),
    ("Creating tunnel mask", "tunnel_mask", 35),
    ("Clipping model raster", "tunnel_clip", 40),
    ("WARNING:", "warning_detected", None),  # None = don't update progress, just capture
    ("Converting", "terrain_convert", 45),
    ("Terrain layers converted", "terrain_done", 48),
    ("Berg layers converted", "berg_done", 52),
    ("Grid processing:", "grid_start", 55),
    ("Phase A:", "grid_phase_a", 58),
    ("Phase A complete", "grid_phase_a_done", 68),
    ("Merging intermediate", "grid_barrier", 70),
    ("Phase B:", "grid_phase_b", 72),
    ("Phase B complete", "grid_phase_b_done", 82),
    ("Final result complete", "grid_done", 85),
    ("Calculating volumes", "volume_calc", 88),
    ("Volumes:", "volumes_done", 92),
    ("PIPELINE SUMMARY", "summary", 95),
    ("Cleaning up", "cleanup", 98),
]


def detect_phase(line):
    """Return (phase, progress) if line matches a marker, else (None, None).
    If progress is None, only update phase name (for warnings etc.)."""
    for marker, phase, progress in PHASE_MAP:
        if marker in line:
            return phase, progress
    return None, None


def parse_volumes(line):
    """Extract volumes from a line like 'Volumes: berg=245079.0 m3, sediment=1231379.0 m3'."""
    try:
        berg = float(line.split("berg=")[1].split(" m3")[0])
        sed = float(line.split("sediment=")[1].split(" m3")[0])
        return {"berg_m3": round(berg, 1), "sediment_m3": round(sed, 1)}
    except Exception:
        return None


# ============================================================
# RUN MANAGER — executes pipeline and syncs to SharePoint
# ============================================================
class RunManager:
    """Manages a single pipeline run with live SharePoint uploads."""

    def __init__(self, graph, drive_id, folder_path, run_id, config):
        self.graph = graph
        self.drive_id = drive_id
        self.base_path = folder_path
        self.run_id = run_id
        self.run_path = f"{folder_path}/{run_id}"
        self.cfg = config

        self.manifest = {
            "run_id": run_id,
            "status": "queued",
            "phase": "initializing",
            "progress": 0,
            "started_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "elapsed_s": 0,
            "pipeline": None,
            "tier": None,
            "publish_target": None,
            "last_line": "",
            "volumes": None,
            "warnings": [],
            "error": None,
        }
        self._log_buffer = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._start_time = time.time()
        self._upload_thread = None

    # Maps pipeline variant -> runner script
    PIPELINE_RUNNERS = {
        "v3": "runner_v3.py",
        "legacy": "run_legacy_full.py",
        "fixed": "run_fixed_full.py",
    }
    # Maps publish_target -> publisher script
    PUBLISH_SCRIPTS = {
        "optimized": "publish_v2.py",
        "legacy": "publish_legacy.py",
        "fixed": "publish_fixed.py",
        "production": "publish.py",
    }
    # Auto-resolution: pipeline -> default publish target
    AUTO_PUBLISH_TARGET = {
        "v3": "optimized",
        "legacy": "legacy",
        "fixed": "fixed",
    }

    def start(self, trigger_config):
        """Create run folder, start pipeline, stream results."""
        # Parse trigger config with defaults
        pipeline = trigger_config.get("pipeline", "v3")
        tier = trigger_config.get("tier", self.cfg["pipeline"]["default_tier"])
        publish = trigger_config.get("publish", self.cfg["pipeline"]["default_publish"])
        publish_target = trigger_config.get("publish_target", "auto")
        no_filter = trigger_config.get("no_filter", False)
        test_mode = trigger_config.get("test", False)

        # Resolve publish target
        if publish_target == "auto":
            publish_target = self.AUTO_PUBLISH_TARGET.get(pipeline, "optimized")

        # Validate pipeline variant
        if pipeline not in self.PIPELINE_RUNNERS:
            log.error("[%s] Unknown pipeline variant: %s. Valid: %s",
                      self.run_id, pipeline, list(self.PIPELINE_RUNNERS.keys()))
            pipeline = "v3"

        # Validate tier
        if tier not in ("moderate", "aggressive", "sequential"):
            log.warning("[%s] Unknown tier: %s, defaulting to moderate", self.run_id, tier)
            tier = "moderate"

        self.manifest["pipeline"] = pipeline
        self.manifest["tier"] = tier
        self.manifest["publish_target"] = publish_target if publish else None
        self.manifest["config"] = trigger_config
        self.manifest["status"] = "running"

        # Create run folder + initial manifest
        self.graph.create_folder(self.drive_id, self.base_path, self.run_id)
        self._upload_manifest()
        self._upload_log()

        # Start background upload thread
        self._upload_thread = threading.Thread(target=self._upload_loop, daemon=True)
        self._upload_thread.start()

        # Build command based on pipeline variant
        runner = self.PIPELINE_RUNNERS[pipeline]
        if pipeline == "v3":
            cmd = [
                self.cfg["pipeline"]["propy_path"],
                runner,
                f"--{tier}",
            ]
            if publish:
                cmd.append("--publish")
                cmd.append(f"--publish-target={publish_target}")
            if no_filter:
                cmd.append("--no-filter")
            if test_mode:
                cmd.append("--test")
        else:
            # Legacy and fixed runners don't take tier/filter/publish-target flags
            cmd = [
                self.cfg["pipeline"]["propy_path"],
                runner,
            ]

        log.info("[%s] Starting: %s", self.run_id, " ".join(cmd))

        try:
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            env["PYTHONIOENCODING"] = "utf-8"
            proc = subprocess.Popen(
                cmd,
                cwd=self.cfg["pipeline"]["working_dir"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )

            for line in proc.stdout:
                self._process_line(line)

            proc.wait()

            if proc.returncode == 0:
                self._finish(success=True)
            else:
                self._finish(success=False, error=f"Exit code {proc.returncode}")

        except Exception as e:
            log.error("[%s] Pipeline error: %s", self.run_id, e)
            self._finish(success=False, error=str(e))

    def _process_line(self, line):
        """Parse a pipeline stdout line, update manifest and log buffer."""
        line = line.rstrip("\n\r")
        if not line:
            return

        with self._lock:
            self._log_buffer.append(line)
            self.manifest["last_line"] = line[:300]
            self.manifest["updated_at"] = datetime.now().isoformat()
            self.manifest["elapsed_s"] = round(time.time() - self._start_time)

        phase, progress = detect_phase(line)
        if phase:
            with self._lock:
                self.manifest["phase"] = phase
                if progress is not None:
                    self.manifest["progress"] = progress

        # Capture warnings in manifest
        if "WARNING:" in line:
            with self._lock:
                if "warnings" not in self.manifest:
                    self.manifest["warnings"] = []
                self.manifest["warnings"].append(line.strip()[:200])

        if "Volumes:" in line:
            vols = parse_volumes(line)
            if vols:
                with self._lock:
                    self.manifest["volumes"] = vols

    def _finish(self, success, error=None):
        """Mark run complete, do final upload."""
        with self._lock:
            self.manifest["status"] = "completed" if success else "failed"
            self.manifest["phase"] = "done" if success else "error"
            self.manifest["progress"] = 100 if success else self.manifest["progress"]
            self.manifest["elapsed_s"] = round(time.time() - self._start_time)
            self.manifest["finished_at"] = datetime.now().isoformat()
            if error:
                self.manifest["error"] = str(error)[:1000]

        self._stop.set()
        if self._upload_thread:
            self._upload_thread.join(timeout=10)

        # Final uploads
        self._upload_manifest()
        self._upload_log()
        log.info("[%s] Run %s (%.0fs)", self.run_id,
                 self.manifest["status"], self.manifest["elapsed_s"])

    def _upload_loop(self):
        """Background thread: periodically upload manifest and log."""
        manifest_interval = self.cfg["server"]["manifest_upload_interval_s"]
        log_interval = self.cfg["server"]["log_upload_interval_s"]
        last_manifest = 0
        last_log = 0

        while not self._stop.is_set():
            now = time.time()
            try:
                if now - last_manifest >= manifest_interval:
                    self._upload_manifest()
                    last_manifest = now
                if now - last_log >= log_interval:
                    self._upload_log()
                    last_log = now
            except Exception as e:
                log.warning("[%s] Upload error: %s", self.run_id, e)
            self._stop.wait(min(manifest_interval, log_interval))

    def _upload_manifest(self):
        """Upload current manifest to SharePoint."""
        with self._lock:
            data = dict(self.manifest)
        try:
            self.graph.upload_json(self.drive_id, f"{self.run_path}/manifest.json", data)
        except Exception as e:
            log.warning("[%s] Manifest upload failed: %s", self.run_id, e)

    def _upload_log(self):
        """Upload accumulated log to SharePoint."""
        with self._lock:
            text = "\n".join(self._log_buffer)
        if not text:
            return
        try:
            self.graph.upload_text(self.drive_id, f"{self.run_path}/log.txt", text)
        except Exception as e:
            log.warning("[%s] Log upload failed: %s", self.run_id, e)


# ============================================================
# SERVER — polls SharePoint for triggers
# ============================================================
class PipelineServer:
    """Main server loop: poll for triggers, dispatch runs."""

    def __init__(self, config):
        self.cfg = config
        self.graph = GraphClient(
            config["azure"]["tenant_id"],
            config["azure"]["client_id"],
            config["azure"]["client_secret"],
        )
        self._running = False
        self._current_run = None

        # Resolve SharePoint IDs
        log.info("Connecting to SharePoint...")
        sp = config["sharepoint"]
        self.site_id = self.graph.get_site_id(sp["site_hostname"], sp["site_path"])
        self.drive_id = self.graph.get_drive_id(self.site_id, sp.get("doc_library", "Documents"))
        self.folder_path = sp["folder_path"]
        log.info("Connected. Site=%s, Drive=%s, Folder=%s",
                 self.site_id[:20], self.drive_id[:20], self.folder_path)

        # Only create the leaf folder (pipeline_runs) — parent path must already exist
        parent = "/".join(self.folder_path.split("/")[:-1])
        leaf = self.folder_path.split("/")[-1]
        if parent and leaf:
            try:
                self.graph.create_folder(self.drive_id, parent, leaf)
                log.info("Created folder: %s/%s", parent, leaf)
            except Exception as e:
                log.info("Folder already exists or could not create: %s", e)

    def run(self):
        """Main loop: poll for trigger.json, run pipeline, repeat."""
        poll_interval = self.cfg["server"]["poll_interval_s"]
        log.info("Server started. Polling every %ds for: %s/trigger.json",
                 poll_interval, self.folder_path)

        poll_count = 0
        while True:
            try:
                self._poll_and_dispatch()
                poll_count += 1
                if poll_count % 20 == 0:  # heartbeat every ~5 min
                    log.info("Heartbeat: %d polls, idle", poll_count)
                    for h in logging.getLogger().handlers:
                        h.flush()
            except KeyboardInterrupt:
                log.info("Server stopped by user")
                break
            except Exception as e:
                log.error("Poll error: %s", e, exc_info=True)
                for h in logging.getLogger().handlers:
                    h.flush()
            time.sleep(poll_interval)

    def _poll_and_dispatch(self):
        """Check for trigger file, start run if found."""
        trigger_path = f"{self.folder_path}/trigger.json"
        try:
            content = self.graph.download_file(self.drive_id, trigger_path, quiet_404=True)
        except Exception as e:
            log.warning("Poll download failed: %s", e)
            return
        if content is None:
            return

        # Parse trigger
        try:
            trigger_config = json.loads(content.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            trigger_config = {}

        log.info("Trigger detected: %s", trigger_config)

        # Delete trigger immediately to prevent re-runs
        self.graph.delete_item(self.drive_id, trigger_path)

        # Generate run ID
        run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Execute pipeline
        runner = RunManager(self.graph, self.drive_id, self.folder_path, run_id, self.cfg)
        runner.start(trigger_config)


# ============================================================
# ENTRY POINT
# ============================================================
log = logging.getLogger("pipeline_server")


def setup_logging(log_dir=None):
    os.environ["PYTHONIOENCODING"] = "utf-8"
    # Console handler with error replacement (Windows cp1252 can't handle all chars)
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                                           datefmt="%Y-%m-%d %H:%M:%S"))
    handlers = [console]
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        # File handler: write raw UTF-8 bytes to avoid double-encoding
        fh = logging.FileHandler(
            os.path.join(log_dir, "pipeline_server.log"), mode="a", encoding="utf-8"
        )
        fh.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                                           datefmt="%Y-%m-%d %H:%M:%S"))
        handlers.append(fh)
    logging.basicConfig(level=logging.INFO, handlers=handlers)


def install_service():
    """Print instructions for installing as a Windows service via NSSM."""
    server_path = os.path.abspath(__file__)
    python_path = sys.executable
    print("To install as a Windows service using NSSM:")
    print()
    print(f'  nssm install PipelineServer "{python_path}" "{server_path}"')
    print(f'  nssm set PipelineServer AppDirectory "{os.path.dirname(server_path)}"')
    print('  nssm set PipelineServer DisplayName "BB5 Pipeline Server"')
    print('  nssm set PipelineServer Description "SharePoint-triggered mass extraction pipeline"')
    print('  nssm set PipelineServer Start SERVICE_AUTO_START')
    print('  nssm start PipelineServer')
    print()
    print("Or use Task Scheduler:")
    print(f'  Program: "{python_path}"')
    print(f'  Arguments: "{server_path}"')
    print(f'  Start in: "{os.path.dirname(server_path)}"')
    print('  Trigger: At system startup')
    print('  Settings: Run whether user is logged on or not')


def main():
    parser = argparse.ArgumentParser(description="BB5 Pipeline Server")
    parser.add_argument("--install", action="store_true", help="Show service installation instructions")
    parser.add_argument("--test-auth", action="store_true", help="Test Graph API authentication only")
    args = parser.parse_args()

    if args.install:
        install_service()
        return

    config = load_config()
    setup_logging(os.path.dirname(os.path.abspath(__file__)))

    if args.test_auth:
        log.info("Testing Graph API authentication...")
        graph = GraphClient(
            config["azure"]["tenant_id"],
            config["azure"]["client_id"],
            config["azure"]["client_secret"],
        )
        sp = config["sharepoint"]
        site_id = graph.get_site_id(sp["site_hostname"], sp["site_path"])
        drive_id = graph.get_drive_id(site_id)
        items = graph.list_folder(drive_id, sp["folder_path"])
        log.info("Auth OK. Site=%s, Drive=%s, %d items in folder",
                 site_id[:30], drive_id[:30], len(items))
        return

    server = PipelineServer(config)
    server.run()


if __name__ == "__main__":
    main()
