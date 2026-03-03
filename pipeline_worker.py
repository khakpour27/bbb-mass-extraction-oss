"""Subprocess-based pipeline worker for the mass extraction pipeline.

Runs the pipeline in a **separate process** so CPU-intensive work (numpy,
scipy, Open3D raycasting) does not block the FastAPI async event loop via
GIL contention — which was the root cause of the server becoming
unresponsive during long steps like terrain parsing.

Communication between the web server and the pipeline process uses a
multiprocessing.Queue (log + progress + finish signals) and a
multiprocessing.Event (cancel flag).
"""

import logging
import multiprocessing
import os
import time
import traceback


# ── Queue-based logging handler ──────────────────────────────────────────────

class QueueLogHandler(logging.Handler):
    """Logging handler that pushes records into a multiprocessing queue."""

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            self.log_queue.put_nowait({
                "level": record.levelname,
                "message": record.getMessage(),
                "time": time.time(),
            })
        except Exception:
            pass  # drop if queue is full


# ── Subprocess entry point ───────────────────────────────────────────────────

def _pipeline_entry(log_queue, cancel_event, config):
    """Entry point for the pipeline subprocess.

    Sets up logging to push every record into *log_queue*, then runs the
    full pipeline.  On completion (success, cancel, or error) a special
    ``{"type": "finished", ...}`` message is placed on the queue.
    """
    # Install queue-based log handler on root logger in this process
    handler = QueueLogHandler(log_queue)
    handler.setLevel(logging.DEBUG)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    def progress_cb(step, pct):
        try:
            log_queue.put_nowait({
                "type": "progress",
                "step": step,
                "pct": pct,
                "time": time.time(),
            })
        except Exception:
            pass

    try:
        from runner import PipelineCancelled, run as pipeline_run

        pipeline_run(
            config=config,
            progress_cb=progress_cb,
            cancel_flag=cancel_event,
        )
        log_queue.put({"type": "finished", "error": None, "time": time.time()})

    except Exception as e:
        # Check for cancellation (compare by name to avoid import issues)
        if "PipelineCancelled" in type(e).__name__:
            log_queue.put({
                "type": "finished",
                "error": "Cancelled by user",
                "time": time.time(),
            })
        else:
            tb = traceback.format_exc()
            log_queue.put({
                "type": "finished",
                "error": f"{e}\n{tb}",
                "time": time.time(),
            })


# ── Pipeline worker ──────────────────────────────────────────────────────────

class PipelineWorker:
    """Manages a single pipeline run in a separate process."""

    def __init__(self):
        self._running = False
        self._step = ""
        self._pct = 0
        self._error: str | None = None
        self._output_folder: str | None = None
        self._started_at: float | None = None
        self._finished_at: float | None = None
        self._log_queue: multiprocessing.Queue = multiprocessing.Queue(maxsize=10_000)
        self._cancel_flag: multiprocessing.Event = multiprocessing.Event()
        self._process: multiprocessing.Process | None = None

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self, config: dict | None = None) -> bool:
        """Start the pipeline in a background process.  Returns False if already running."""
        if self._running:
            return False

        # Reset state
        self._running = True
        self._step = ""
        self._pct = 0
        self._error = None
        self._output_folder = None
        self._started_at = time.time()
        self._finished_at = None
        self._cancel_flag.clear()

        # Drain leftover queue entries
        while True:
            try:
                self._log_queue.get_nowait()
            except Exception:
                break

        self._process = multiprocessing.Process(
            target=_pipeline_entry,
            args=(self._log_queue, self._cancel_flag, config),
            daemon=False,  # must be non-daemon so Pool can spawn children
        )
        self._process.start()
        return True

    def stop(self) -> bool:
        """Signal the pipeline to cancel.  Returns False if not running."""
        if not self._running:
            return False
        self._cancel_flag.set()
        return True

    def get_status(self) -> dict:
        """Return current pipeline status as a dict."""
        self._check_process()
        return {
            "running": self._running,
            "step": self._step,
            "progress_pct": self._pct,
            "error": self._error,
            "output_folder": self._output_folder,
            "started_at": self._started_at,
            "finished_at": self._finished_at,
        }

    def drain_logs(self, max_items: int = 200) -> list[dict]:
        """Non-blocking drain of log entries from the queue."""
        entries: list[dict] = []
        for _ in range(max_items):
            try:
                entry = self._log_queue.get_nowait()
            except Exception:
                break

            msg_type = entry.get("type")

            if msg_type == "progress":
                self._step = entry["step"]
                self._pct = entry["pct"]
                entries.append(entry)  # forward to SSE too

            elif msg_type == "finished":
                self._running = False
                self._finished_at = time.time()
                err = entry.get("error")
                if err:
                    self._error = err
                    entries.append({
                        "level": "ERROR" if err != "Cancelled by user" else "WARNING",
                        "message": err,
                        "time": time.time(),
                    })
                else:
                    self._find_output_folder()

            else:
                entries.append(entry)

        self._check_process()
        return entries

    # ── internal helpers ─────────────────────────────────────────────────

    def _find_output_folder(self):
        output_dir = "output"
        if os.path.isdir(output_dir):
            results = sorted(
                [d for d in os.listdir(output_dir) if d.startswith("results_")],
                reverse=True,
            )
            if results:
                self._output_folder = results[0]

    def _check_process(self):
        """Detect if subprocess died unexpectedly."""
        if self._running and self._process and not self._process.is_alive():
            self._running = False
            self._finished_at = time.time()
            if not self._error:
                code = self._process.exitcode
                if code and code != 0:
                    self._error = f"Pipeline process exited with code {code}"
