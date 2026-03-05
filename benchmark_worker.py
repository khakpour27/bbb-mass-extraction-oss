"""Dual-pipeline benchmark orchestrator.

Supports three modes: "oss" (OSS only), "legacy" (legacy only),
"benchmark" (both pipelines with comparison).

In benchmark mode: resolves files once, runs OSS pipeline, then legacy
pipeline, then compares results.
"""

import json
import logging
import multiprocessing
import os
import time
import traceback

logger = logging.getLogger(__name__)


class BenchmarkWorker:
    """Manages benchmark runs in a separate process.

    Same pattern as pipeline_worker.py:PipelineWorker.
    """

    def __init__(self):
        self._running = False
        self._mode = "oss"
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

    @property
    def mode(self) -> str:
        return self._mode

    def start(self, config: dict, mode: str = "oss") -> bool:
        """Start a pipeline run. mode: 'oss', 'legacy', or 'benchmark'."""
        if self._running:
            return False

        self._running = True
        self._mode = mode
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
            target=_benchmark_entry,
            args=(self._log_queue, self._cancel_flag, config, mode),
            daemon=False,
        )
        self._process.start()
        return True

    def stop(self) -> bool:
        if not self._running:
            return False
        self._cancel_flag.set()
        return True

    def get_status(self) -> dict:
        self._check_process()
        return {
            "running": self._running,
            "mode": self._mode,
            "step": self._step,
            "progress_pct": self._pct,
            "error": self._error,
            "output_folder": self._output_folder,
            "started_at": self._started_at,
            "finished_at": self._finished_at,
        }

    def drain_logs(self, max_items: int = 200) -> list[dict]:
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
                entries.append(entry)
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
                    self._output_folder = entry.get("output_folder")
            else:
                entries.append(entry)

        self._check_process()
        return entries

    def _check_process(self):
        if self._running and self._process and not self._process.is_alive():
            self._running = False
            self._finished_at = time.time()
            if not self._error:
                code = self._process.exitcode
                if code and code != 0:
                    self._error = f"Benchmark process exited with code {code}"


def _benchmark_entry(log_queue, cancel_event, config, mode):
    """Entry point for the benchmark subprocess."""
    from pipeline_worker import QueueLogHandler

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
        from file_resolver import resolve_files
        from runner import PipelineCancelled, run as oss_run

        # Resolve files once for both pipelines
        file_manifest = resolve_files(config)

        # Save manifest
        from datetime import datetime
        run_time = datetime.now().strftime("%Y_%m_%d_%H_%M")
        output_base = os.path.join("output", f"results_{run_time}")
        os.makedirs(output_base, exist_ok=True)

        manifest_path = os.path.join(output_base, "file_manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(file_manifest, f, indent=2, default=str)

        results = {
            "mode": mode,
            "config": {k: v for k, v in config.items() if not k.startswith("_")},
            "file_manifest_path": manifest_path,
        }

        # ── OSS Pipeline ─────────────────────────────────────────────────
        if mode in ("oss", "benchmark"):
            logger.info("=" * 60)
            logger.info("Running OSS pipeline...")
            logger.info("=" * 60)

            oss_subdir = "oss" if mode == "benchmark" else ""

            def oss_timing(stage, event, ts):
                pass  # collected in runner return value

            oss_result = oss_run(
                config=config,
                progress_cb=lambda s, p: progress_cb(f"[OSS] {s}", p // 2 if mode == "benchmark" else p),
                cancel_flag=cancel_event,
                timing_cb=oss_timing,
                output_subdir=oss_subdir,
                file_manifest=file_manifest,
            )

            results["oss"] = {
                "total_time_s": sum(
                    v.get("time_s", 0) for v in oss_result.get("timings", {}).values()
                ),
                "stages": oss_result.get("timings", {}),
                "volumes": oss_result.get("volumes", {}),
                "output_folder": oss_result.get("output_folder", ""),
            }

        # ── Legacy Pipeline ──────────────────────────────────────────────
        if mode in ("legacy", "benchmark"):
            logger.info("=" * 60)
            logger.info("Running Legacy pipeline...")
            logger.info("=" * 60)

            try:
                from legacy_adapter import is_legacy_available, run_legacy_pipeline

                if not is_legacy_available(config.get("LEGACY_PYTHON_PATH")):
                    raise FileNotFoundError("ArcGIS Pro Python not available")

                legacy_output = os.path.join(output_base, "legacy") if mode == "benchmark" else output_base
                os.makedirs(legacy_output, exist_ok=True)

                pct_offset = 50 if mode == "benchmark" else 0

                legacy_result = run_legacy_pipeline(
                    config=config,
                    file_manifest=file_manifest,
                    output_dir=legacy_output,
                    log_queue=log_queue,
                    cancel_event=cancel_event,
                )

                results["legacy"] = {
                    "total_time_s": legacy_result.get("total_time_s", 0),
                    "stages": legacy_result.get("stages", {}),
                    "volumes": legacy_result.get("volumes", {}),
                    "return_code": legacy_result.get("return_code", -1),
                }

            except FileNotFoundError as e:
                logger.error("Legacy pipeline unavailable: %s", e)
                results["legacy"] = {
                    "error": str(e),
                    "total_time_s": 0,
                    "stages": {},
                    "volumes": {},
                }

        # ── Comparison ───────────────────────────────────────────────────
        if mode == "benchmark" and "oss" in results and "legacy" in results:
            logger.info("=" * 60)
            logger.info("Comparing results...")
            logger.info("=" * 60)
            progress_cb("Comparing results", 95)

            comparison = _generate_comparison(results, output_base)
            results["comparison"] = comparison

        # ── Write benchmark results ──────────────────────────────────────
        results_path = os.path.join(output_base, "benchmark_results.json")
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)
        logger.info("Benchmark results written to: %s", results_path)

        progress_cb("Benchmark complete", 100)
        log_queue.put({
            "type": "finished",
            "error": None,
            "output_folder": output_base,
            "time": time.time(),
        })

    except Exception as e:
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


def _generate_comparison(results: dict, output_base: str) -> dict:
    """Generate comparison data between OSS and legacy results."""
    comparison = {}

    # Volume comparison
    oss_volumes = results.get("oss", {}).get("volumes", {})
    legacy_volumes = results.get("legacy", {}).get("volumes", {})

    if oss_volumes and legacy_volumes:
        try:
            from raster_compare import compare_volumes
            comparison["volume_diffs"] = compare_volumes(oss_volumes, legacy_volumes)
        except Exception as e:
            logger.warning("Volume comparison failed: %s", e)

    # Time ratio
    oss_time = results.get("oss", {}).get("total_time_s", 0)
    legacy_time = results.get("legacy", {}).get("total_time_s", 0)
    if legacy_time > 0:
        comparison["time_ratio"] = oss_time / legacy_time

    # Raster comparison (if both produced FINAL_RESULT_RASTER.tif)
    oss_raster = os.path.join(output_base, "oss", "FINAL_RESULT_RASTER.tif")
    legacy_raster = os.path.join(output_base, "legacy", "FINAL_RESULT_RASTER.tif")

    if os.path.isfile(oss_raster) and os.path.isfile(legacy_raster):
        try:
            from raster_compare import compare_rasters, generate_diff_raster

            raster_diff = compare_rasters(oss_raster, legacy_raster)
            comparison["raster_diff"] = raster_diff

            diff_path = os.path.join(output_base, "diff_FINAL_RESULT.tif")
            generate_diff_raster(oss_raster, legacy_raster, diff_path)
            comparison["diff_raster_path"] = diff_path

            logger.info(
                "Raster comparison: mean_abs=%.4fm, pct_within_1cm=%.1f%%",
                raster_diff.get("mean_abs_m", 0),
                raster_diff.get("pct_within_1cm", 0),
            )
        except Exception as e:
            logger.warning("Raster comparison failed: %s", e)

    return comparison
