"""Main pipeline orchestrator for the BBB mass extraction calculation.

Replicates the processing flow of the original arcpy-based mass_calc.py
using open-source libraries.

Features:
- File resolution via file_resolver module
- Timing callbacks for benchmark instrumentation
- Output subdirectory support for benchmark mode
- Parallel tile processing with ProcessPoolExecutor
- Memory-aware concurrency control

Usage:
    python runner.py
"""

import gc
import logging
import os
import shutil
import stat
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

import numpy as np
import config as _config_module
from file_resolver import manifest_paths, resolve_files
from grid_processor import (
    apply_exclusion_mask,
    clip_raster_to_bounds,
    clip_raster_to_file,
    create_exclusion_mask,
    filter_model_under_berg,
    get_intersecting_tiles,
    get_model_footprint,
    load_grid_index,
    merge_berg_with_models,
    merge_buffer_with_berg,
    merge_rasters_min,
    validate_tile_dimensions,
)
from ifc_parser import (
    adjust_sporsystem_z,
    import_ifcs_parallel,
    list_berg_ifcs,
    list_model_ifcs,
    list_tunnel_ifcs,
    parse_ifc,
)
from output_writer import append_tunnel_volumes, write_volumes_csv, write_volumes_excel
from rasterizer import meshes_to_merged_raster, read_geotiff, snap_transform, write_geotiff
from slope_propagation import buffer_excavation, propagate_rock_slope, propagate_soil_slope
from terrain_parser import list_land_xmls, parse_and_rasterize_terrain
from tunnel_vol import calculate_tunnel_volume
from utils import estimate_raster_memory, get_available_memory, list_files_by_ext, log_memory_usage, setup_logging
from volume_calc import calculate_all_volumes

logger = logging.getLogger(__name__)


# Names of all config keys that can be overridden at runtime
_CONFIG_KEYS = [
    "MODEL_FOLDER_PATH", "TERRAIN_PATH", "BERG_PATH",
    "CELL_SIZE", "CRS", "GRID_CELL_SIZE", "SPORSYSTEM_Z_OFFSET",
    "ROCK_SLOPE_FACTOR", "SOIL_SLOPE_DIVISOR", "BUFFER_DISTANCE",
    "ROCK_DENSITY", "SEDIMENT_DIESEL_FACTOR", "TUNNEL_ROCK_DENSITY",
    "MAX_TILE_DIMENSION", "GRID_PATH", "MUNKEBOTN_MASK", "MAX_CORES",
    "MAX_MODEL_FILES",
    # New keys
    "TEST_AREA_PREFIX", "LEGACY_PYTHON_PATH",
    "EXTENT_X_MIN", "EXTENT_X_MAX", "EXTENT_Y_MIN", "EXTENT_Y_MAX",
]


class PipelineCancelled(Exception):
    """Raised when the pipeline is cancelled by the user."""


def _build_config(overrides: dict | None = None) -> dict:
    """Build a config dict from module defaults merged with runtime overrides."""
    cfg = {}
    for key in _CONFIG_KEYS:
        cfg[key] = getattr(_config_module, key, None)
    if overrides:
        for key, val in overrides.items():
            if key in cfg:
                cfg[key] = val
    return cfg


def _elapsed(t0: float) -> str:
    """Return a human-readable elapsed time string."""
    dt = time.time() - t0
    if dt < 60:
        return f"{dt:.1f}s"
    m, s = divmod(int(dt), 60)
    return f"{m}m {s}s"


# ── Cleanup helpers ───────────────────────────────────────────────────────────

def _remove_readonly(func, path, _excinfo):
    os.chmod(path, stat.S_IWRITE)
    func(path)


def safe_delete(path: str, retries: int = 3) -> None:
    for attempt in range(retries):
        try:
            if os.path.isdir(path):
                shutil.rmtree(path, onerror=_remove_readonly)
            elif os.path.isfile(path):
                os.chmod(path, stat.S_IWRITE)
                os.remove(path)
            return
        except PermissionError:
            logger.warning("PermissionError on %s, retry %d", path, attempt + 1)
            time.sleep(1)
        except Exception as e:
            logger.error("Error deleting %s: %s", path, e)
            break


# ── Tile processing worker (for parallel execution) ─────────────────────────

def _process_rock_tile(args):
    """Process a single tile for rock slope BFS (Pass 1). Designed for ProcessPoolExecutor."""
    (tile_id, tile_bounds, model_raster_path, berg_raster_path,
     cell_size, slope_factor, crs, scratch_folder) = args

    model_clip, model_clip_tf = clip_raster_to_bounds(model_raster_path, tile_bounds)
    berg_clip, berg_clip_tf = clip_raster_to_bounds(berg_raster_path, tile_bounds)

    # Skip tiles with no model or berg data
    if model_clip is None or berg_clip is None:
        return None

    if not validate_tile_dimensions(model_clip):
        model_clip = np.full_like(model_clip, np.nan)
    if not validate_tile_dimensions(berg_clip):
        berg_clip = np.full_like(berg_clip, np.nan)

    filtered = filter_model_under_berg(model_clip, berg_clip)
    berg_exc = propagate_rock_slope(filtered, berg_clip, cell_size, slope_factor)

    berg_exc_path = os.path.join(scratch_folder, f"berg_exc_tile_{tile_id}.tif")
    write_geotiff(berg_exc, model_clip_tf, crs, berg_exc_path)

    return {
        "tile_id": tile_id,
        "bounds": tile_bounds,
        "berg_exc_path": berg_exc_path,
        "model_clip_tf": model_clip_tf,
        "model_clip_shape": model_clip.shape,
    }


def _process_soil_tile(args):
    """Process a single tile for soil slope BFS (Pass 2). Designed for ProcessPoolExecutor.

    Reads buffer_zone from disk (buffer_zone_path) to avoid pickling huge arrays
    through Windows process pipes.
    """
    (tile_id, tile_bounds, model_raster_path, berg_raster_path,
     complete_berg_exc_path, terrain_raster_path,
     buffer_zone_path,
     cell_size, soil_slope_divisor, crs, scratch_folder) = args

    model_clip, model_clip_tf = clip_raster_to_bounds(model_raster_path, tile_bounds)
    berg_clip, _ = clip_raster_to_bounds(berg_raster_path, tile_bounds)
    berg_exc_clip, _ = clip_raster_to_bounds(complete_berg_exc_path, tile_bounds)
    terrain_clip, _ = clip_raster_to_bounds(terrain_raster_path, tile_bounds)

    # Skip tiles with no model data or no terrain data
    if model_clip is None or terrain_clip is None:
        return None

    # Handle missing berg data — create NaN arrays of same shape as model
    if berg_clip is None:
        berg_clip = np.full_like(model_clip, np.nan)
    if berg_exc_clip is None:
        berg_exc_clip = np.full_like(model_clip, np.nan)

    # Read buffer zone clip from disk (avoid pickling large arrays)
    buf_clip, _ = clip_raster_to_bounds(buffer_zone_path, tile_bounds)
    if buf_clip is None:
        buf_clip = np.full_like(model_clip, np.nan)

    buff_berg = merge_buffer_with_berg(berg_clip, berg_exc_clip, buf_clip, cell_size)
    merged_model = merge_berg_with_models(buff_berg, model_clip)

    final_exc = propagate_soil_slope(merged_model, berg_exc_clip, terrain_clip, cell_size)

    final_exc_path = os.path.join(scratch_folder, f"final_tile_{tile_id}.tif")
    write_geotiff(final_exc, model_clip_tf, crs, final_exc_path)

    return {
        "tile_id": tile_id,
        "final_exc_path": final_exc_path,
    }


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(
    config: dict | None = None,
    progress_cb=None,
    cancel_flag=None,
    timing_cb=None,
    output_subdir: str = "",
    file_manifest: dict | None = None,
) -> dict:
    """Run the full mass extraction pipeline.

    Parameters
    ----------
    config : dict, optional
        Runtime overrides for config.py values.
    progress_cb : callable(step_name: str, pct: int), optional
        Called at each major step with a label and cumulative percentage.
    cancel_flag : threading.Event, optional
        If set, the pipeline will abort between steps.
    timing_cb : callable(stage_name: str, event: str, timestamp: float), optional
        Called at start/end of each pipeline stage for benchmarking.
    output_subdir : str, optional
        Subdirectory under the output folder (e.g. "oss/" for benchmark mode).
    file_manifest : dict, optional
        Pre-resolved file manifest from file_resolver. If None, resolves files internally.

    Returns
    -------
    dict with keys: volumes, output_folder, file_manifest, timings
    """
    cfg = _build_config(config)
    t0 = time.time()
    stage_timings: dict[str, dict] = {}

    # Limit GDAL block cache to 512MB to prevent memory bloat during tile processing
    os.environ.setdefault("GDAL_CACHEMAX", "512")

    def _progress(step: str, pct: int):
        if progress_cb:
            progress_cb(step, pct)

    def _check_cancel():
        if cancel_flag and cancel_flag.is_set():
            raise PipelineCancelled("Pipeline cancelled by user")

    def _step_log(msg: str, *args):
        prefix = f"[{_elapsed(t0)}]"
        logger.info(f"{prefix} {msg}", *args)

    def _timer(stage_name: str, event: str):
        ts = time.time()
        if timing_cb:
            timing_cb(stage_name, event, ts)
        if event == "start":
            stage_timings[stage_name] = {"start": ts}
        elif event == "end" and stage_name in stage_timings:
            stage_timings[stage_name]["end"] = ts
            stage_timings[stage_name]["time_s"] = ts - stage_timings[stage_name]["start"]

    # ── 1. Setup ──────────────────────────────────────────────────────────
    _timer("setup", "start")
    _progress("Setting up directories", 1)
    _step_log("Pipeline starting...")
    _step_log("Config: CELL_SIZE=%.2f, CRS=%s, MAX_CORES=%d",
              cfg["CELL_SIZE"], cfg["CRS"], cfg.get("MAX_CORES", 24))

    log_memory_usage(logger, "Pipeline start")

    run_time = datetime.now().strftime("%Y_%m_%d_%H_%M")
    output_folder = "output"
    scratch_folder = "scratch"

    os.makedirs(output_folder, exist_ok=True)
    if os.path.exists(scratch_folder):
        safe_delete(scratch_folder)
    os.makedirs(scratch_folder, exist_ok=True)

    final_out_path = os.path.join(output_folder, f"results_{run_time}")
    if output_subdir:
        final_out_path = os.path.join(final_out_path, output_subdir)
    os.makedirs(final_out_path, exist_ok=True)

    log_path = os.path.join(final_out_path, "results.log")
    setup_logging(log_path)
    _step_log("Output folder: %s", final_out_path)
    _timer("setup", "end")

    _check_cancel()

    # ── 2. File resolution ────────────────────────────────────────────────
    _timer("file_resolution", "start")
    _progress("Scanning input files", 3)

    if file_manifest is None:
        file_manifest = resolve_files(cfg)
    else:
        _step_log("Using pre-resolved file manifest")

    model_list = manifest_paths(file_manifest, "model_files")
    tunnel_list = manifest_paths(file_manifest, "tunnel_files")
    berg_list = manifest_paths(file_manifest, "berg_files")
    terrain_xml_list = manifest_paths(file_manifest, "terrain_files")

    skipped = file_manifest.get("skipped_files", [])
    if skipped:
        _step_log("Skipped %d files with bad extents/readability", len(skipped))
        for entry in skipped:
            _step_log("  SKIPPED: %s — %s", os.path.basename(entry["path"]), entry["reason"])

    _step_log("Model: %d, Tunnel: %d, Berg: %d, Terrain: %d",
              len(model_list), len(tunnel_list), len(berg_list), len(terrain_xml_list))

    if not model_list:
        raise RuntimeError("No readable model IFC files found. "
                           "Pin/download files in Autodesk Desktop Connector first.")

    _timer("file_resolution", "end")
    _check_cancel()

    # ── 3. Parse IFCs in parallel → trimesh meshes ────────────────────────
    _timer("ifc_import", "start")
    _progress("Importing model IFCs (0/%d)" % len(model_list), 5)
    _step_log("Starting model IFC import (%d files)...", len(model_list))

    # Separate sporsystem from others before parsing
    spor_paths = [p for p in model_list if "Sporsystem" in p]
    non_spor_paths = [p for p in model_list if "Sporsystem" not in p]

    ifc_import_pct_base = 5
    ifc_import_pct_range = 12  # 5% → 17%

    def _on_model_ifc_done(path, mesh_count, done, total):
        pct = ifc_import_pct_base + int(ifc_import_pct_range * done / max(total, 1))
        _progress(f"Importing model IFCs ({done}/{total})", pct)
        _step_log("  [%d/%d] %s → %d meshes", done, total, os.path.basename(path), mesh_count)

    if spor_paths:
        _step_log("Parsing %d non-Sporsystem models...", len(non_spor_paths))
        non_spor_meshes = import_ifcs_parallel(
            non_spor_paths, on_file_done=_on_model_ifc_done,
        )
        _check_cancel()

        _step_log("Parsing %d Sporsystem models (will adjust Z by %.2fm)...",
                   len(spor_paths), cfg["SPORSYSTEM_Z_OFFSET"])
        spor_meshes = import_ifcs_parallel(spor_paths, on_file_done=_on_model_ifc_done)

        _step_log("Sinking %d Sporsystem meshes by %.2fm...",
                   len(spor_meshes), cfg["SPORSYSTEM_Z_OFFSET"])
        adjust_sporsystem_z(spor_meshes, cfg["SPORSYSTEM_Z_OFFSET"])
        all_model_meshes = non_spor_meshes + spor_meshes
        _step_log("Total model meshes: %d (non-spor: %d, spor: %d)",
                   len(all_model_meshes), len(non_spor_meshes), len(spor_meshes))
    else:
        _step_log("No Sporsystem models found. Parsing %d model IFCs...", len(model_list))
        all_model_meshes = import_ifcs_parallel(
            model_list, on_file_done=_on_model_ifc_done,
        )
        _step_log("Total model meshes: %d", len(all_model_meshes))

    _timer("ifc_import", "end")
    _check_cancel()

    # ── 5. Rasterize models → merged model raster GeoTIFF ────────────────
    _timer("model_rasterize", "start")
    _progress("Rasterizing model meshes", 20)
    _step_log("Rasterizing %d model meshes (cell_size=%.2f, method=MINIMUM_HEIGHT)...",
              len(all_model_meshes), cfg["CELL_SIZE"])
    t_raster = time.time()
    model_raster_arr, model_transform = meshes_to_merged_raster(
        all_model_meshes, cfg["CELL_SIZE"], "MINIMUM_HEIGHT"
    )
    model_raster_path = os.path.join(scratch_folder, "MERGED_MODEL_RASTER.tif")
    write_geotiff(model_raster_arr, model_transform, cfg["CRS"], model_raster_path)
    valid_cells = int(np.count_nonzero(~np.isnan(model_raster_arr)))
    _step_log("Model raster complete: %dx%d pixels, %d valid cells (%.1fs)",
              model_raster_arr.shape[1], model_raster_arr.shape[0],
              valid_cells, time.time() - t_raster)
    del model_raster_arr; gc.collect()
    _timer("model_rasterize", "end")

    # ── 6. Reference transform for snapping ───────────────────────────────
    ref_transform = model_transform
    del all_model_meshes; gc.collect()

    _check_cancel()

    # ── 7. Rasterize tunnels ──────────────────────────────────────────────
    _timer("tunnel_rasterize", "start")
    _progress("Rasterizing tunnels", 25)
    tunnel_raster_path = None
    tunnel_meshes = []
    if tunnel_list:
        _step_log("Importing %d tunnel IFCs...", len(tunnel_list))

        def _on_tunnel_ifc_done(path, mesh_count, done, total):
            _step_log("  Tunnel [%d/%d] %s → %d meshes",
                      done, total, os.path.basename(path), mesh_count)

        tunnel_meshes = import_ifcs_parallel(
            tunnel_list, on_file_done=_on_tunnel_ifc_done,
        )
        _step_log("Rasterizing %d tunnel meshes...", len(tunnel_meshes))
        t_raster = time.time()
        tunnel_arr, tunnel_tf = meshes_to_merged_raster(
            tunnel_meshes, cfg["CELL_SIZE"], "MINIMUM_HEIGHT"
        )
        tunnel_raster_path = os.path.join(scratch_folder, "MERGED_TUNNEL_RASTER.tif")
        write_geotiff(tunnel_arr, tunnel_tf, cfg["CRS"], tunnel_raster_path)
        _step_log("Tunnel raster complete: %dx%d (%.1fs)",
                  tunnel_arr.shape[1], tunnel_arr.shape[0], time.time() - t_raster)
    else:
        _step_log("No tunnel IFCs found — skipping")
    _timer("tunnel_rasterize", "end")

    _check_cancel()

    # ── 8. Create + apply exclusion mask ──────────────────────────────────
    _timer("exclusion_mask", "start")
    _progress("Applying exclusion mask", 30)
    _step_log("Creating exclusion mask...")
    mask_path = create_exclusion_mask(
        tunnel_raster_path,
        cfg["MUNKEBOTN_MASK"] if os.path.isfile(cfg["MUNKEBOTN_MASK"]) else None,
        os.path.join(scratch_folder, "exclusion_mask.tif"),
        cfg["CRS"],
    )

    if mask_path:
        _step_log("Applying exclusion mask to model raster...")
        clipped_model_path = os.path.join(scratch_folder, "CLIPPED_MODEL_RASTER.tif")
        apply_exclusion_mask(model_raster_path, mask_path, clipped_model_path, cfg["CRS"])
        model_raster_path = clipped_model_path
        _step_log("Exclusion mask applied")
    else:
        _step_log("No exclusion mask sources — skipping")
    _timer("exclusion_mask", "end")

    _check_cancel()

    # ── 9. Model footprint polygon ────────────────────────────────────────
    _timer("footprint", "start")
    _progress("Generating model footprint", 33)
    _step_log("Computing model footprint polygon...")
    footprint = get_model_footprint(model_raster_path)
    _step_log("Model footprint area: %.0f m²", footprint.area)
    _timer("footprint", "end")

    _check_cancel()

    # ── 10. Parse terrain → merged terrain raster ─────────────────────────
    _timer("terrain_parse", "start")
    terrain_raster_path = None
    has_terrain = len(terrain_xml_list) > 0
    if has_terrain:
        _progress("Parsing terrain LandXML", 36)
        _step_log("Parsing %d terrain LandXML files...", len(terrain_xml_list))
        t_terrain = time.time()
        # Clip terrain to model footprint bounds (P2 optimization)
        fp_bounds = footprint.bounds if footprint else None
        terrain_arr, terrain_tf = parse_and_rasterize_terrain(
            terrain_xml_list, cfg["CELL_SIZE"], bounds=fp_bounds,
        )
        terrain_tf = snap_transform(terrain_tf, ref_transform, cfg["CELL_SIZE"])
        terrain_raster_path = os.path.join(scratch_folder, "TERRAIN_MERGED_RASTER.tif")
        write_geotiff(terrain_arr, terrain_tf, cfg["CRS"], terrain_raster_path)
        write_geotiff(terrain_arr, terrain_tf, cfg["CRS"], os.path.join(final_out_path, "TERRAIN_MERGED_RASTER.tif"))
        _step_log("Terrain raster complete: %dx%d (%.1fs)",
                  terrain_arr.shape[1], terrain_arr.shape[0], time.time() - t_terrain)
        del terrain_arr; gc.collect()
    else:
        _step_log("WARNING: No readable terrain XML files — skipping terrain raster. "
                  "Volume calculations will be unavailable.")
    _timer("terrain_parse", "end")

    _check_cancel()

    # ── 11. Rasterize berg → merged berg raster ───────────────────────────
    _timer("berg_import", "start")
    _progress("Importing berg IFCs", 44)
    _step_log("Importing %d berg IFCs...", len(berg_list))

    def _on_berg_ifc_done(path, mesh_count, done, total):
        pct = 44 + int(6 * done / max(total, 1))
        _progress(f"Importing berg IFCs ({done}/{total})", pct)
        _step_log("  Berg [%d/%d] %s → %d meshes",
                  done, total, os.path.basename(path), mesh_count)

    berg_meshes = import_ifcs_parallel(berg_list, on_file_done=_on_berg_ifc_done)

    _progress("Rasterizing berg meshes", 50)
    _step_log("Rasterizing %d berg meshes...", len(berg_meshes))
    t_raster = time.time()
    berg_arr, berg_tf = meshes_to_merged_raster(berg_meshes, cfg["CELL_SIZE"], "MINIMUM_HEIGHT")
    berg_tf = snap_transform(berg_tf, ref_transform, cfg["CELL_SIZE"])
    berg_raster_path = os.path.join(scratch_folder, "MERGED_BERG_RASTER.tif")
    write_geotiff(berg_arr, berg_tf, cfg["CRS"], berg_raster_path)
    write_geotiff(berg_arr, berg_tf, cfg["CRS"], os.path.join(final_out_path, "BERG_MERGED_RASTER.tif"))
    _step_log("Berg raster complete: %dx%d (%.1fs)",
              berg_arr.shape[1], berg_arr.shape[0], time.time() - t_raster)
    del berg_arr, berg_meshes; gc.collect()
    _timer("berg_import", "end")

    _check_cancel()

    # ── 12. Load grid index, get intersecting tiles ───────────────────────
    _timer("grid_index", "start")
    _progress("Loading grid index", 54)
    _step_log("Loading grid index from %s...", cfg["GRID_PATH"])
    grid_gdf = load_grid_index(cfg["GRID_PATH"])
    tiles = get_intersecting_tiles(grid_gdf, footprint)
    num_tiles = len(tiles)
    _step_log("Found %d intersecting tiles (of %d total grid cells)", num_tiles, len(grid_gdf))
    _timer("grid_index", "end")

    _check_cancel()

    # ── 13. Per-tile processing ───────────────────────────────────────────
    _timer("rock_slope_bfs", "start")
    _progress("Per-tile rock slope BFS (0/%d)" % num_tiles, 56)
    _step_log("Starting per-tile processing: %d tiles...", num_tiles)
    log_memory_usage(logger, "Before tile processing")

    clipped_berg_exc_tiles: list[str] = []
    tile_data: list[dict] = []

    # Determine parallelism for tile processing
    max_cores = int(cfg.get("MAX_CORES", 24))
    avail_mem = get_available_memory()
    # Each worker opens several large GeoTIFFs. GDAL caches blocks per process.
    # At 0.2m: tiles are 1000x1000, ~4MB per array, but GDAL block caching adds ~200MB/worker.
    # Cap at 8 workers for production (0.2m) to avoid memory pressure on 64GB system.
    if cfg["CELL_SIZE"] <= 0.3:
        tile_mem_gb = 2.0  # conservative for production resolution
    elif cfg["CELL_SIZE"] <= 0.5:
        tile_mem_gb = 0.5
    else:
        tile_mem_gb = 0.1
    max_parallel_tiles = max(1, min(max_cores, min(8, int(avail_mem / tile_mem_gb))))

    # For rock BFS pass, use parallel processing if enough tiles and resources
    use_parallel_tiles = num_tiles >= 3 and max_parallel_tiles >= 2

    if use_parallel_tiles:
        _step_log("Using parallel tile processing: up to %d concurrent tiles (%.1f GB free)",
                  max_parallel_tiles, avail_mem)

        # Prepare args for all tiles
        tile_args = []
        for idx, (_, tile_row) in enumerate(tiles.iterrows()):
            tile_bounds = tile_row.geometry.bounds
            tile_id = tile_row.get("GRIDNR", idx)
            tile_args.append((
                tile_id, tile_bounds, model_raster_path, berg_raster_path,
                cfg["CELL_SIZE"], cfg.get("ROCK_SLOPE_FACTOR", 10.0),
                cfg["CRS"], scratch_folder,
            ))

        # Process tiles in parallel
        with ProcessPoolExecutor(max_workers=max_parallel_tiles) as executor:
            futures = {executor.submit(_process_rock_tile, args): args[0] for args in tile_args}
            for idx, future in enumerate(as_completed(futures)):
                _check_cancel()
                result = future.result()
                if result is None:
                    continue  # tile had no overlap with rasters
                clipped_berg_exc_tiles.append(result["berg_exc_path"])
                tile_data.append(result)

                pct = 56 + int(12 * (idx + 1) / max(num_tiles, 1))
                _progress(f"Rock slope BFS ({idx + 1}/{num_tiles})", pct)
                _step_log("Tile %s: rock slope done", result["tile_id"])
    else:
        # Sequential processing (original behavior)
        for idx, (_, tile_row) in enumerate(tiles.iterrows()):
            _check_cancel()
            tile_bounds = tile_row.geometry.bounds
            tile_id = tile_row.get("GRIDNR", idx)
            t_tile = time.time()

            _step_log("Tile %s [%d/%d]: clipping model + berg...", tile_id, idx + 1, num_tiles)

            model_clip, model_clip_tf = clip_raster_to_bounds(model_raster_path, tile_bounds)
            berg_clip, berg_clip_tf = clip_raster_to_bounds(berg_raster_path, tile_bounds)

            # Skip tiles with no model or berg data
            if model_clip is None or berg_clip is None:
                _step_log("Tile %s: no overlap with rasters — skipping", tile_id)
                continue

            if not validate_tile_dimensions(model_clip):
                _step_log("Tile %s: model clip too large (%dx%d) — replacing with NaN",
                          tile_id, model_clip.shape[1], model_clip.shape[0])
                model_clip = np.full_like(model_clip, np.nan)
            if not validate_tile_dimensions(berg_clip):
                _step_log("Tile %s: berg clip too large (%dx%d) — replacing with NaN",
                          tile_id, berg_clip.shape[1], berg_clip.shape[0])
                berg_clip = np.full_like(berg_clip, np.nan)

            filtered = filter_model_under_berg(model_clip, berg_clip)
            valid_before = int(np.count_nonzero(~np.isnan(model_clip)))
            valid_after = int(np.count_nonzero(~np.isnan(filtered)))
            _step_log("Tile %s: filtered model cells %d → %d (under berg)", tile_id, valid_before, valid_after)

            _step_log("Tile %s: running rock slope BFS (factor=%.1f)...", tile_id, cfg.get("ROCK_SLOPE_FACTOR", 10.0))
            berg_exc = propagate_rock_slope(filtered, berg_clip, cfg["CELL_SIZE"])

            berg_exc_path = os.path.join(scratch_folder, f"berg_exc_tile_{tile_id}.tif")
            write_geotiff(berg_exc, model_clip_tf, cfg["CRS"], berg_exc_path)
            clipped_berg_exc_tiles.append(berg_exc_path)

            tile_data.append({
                "tile_id": tile_id,
                "bounds": tile_bounds,
                "berg_exc_path": berg_exc_path,
                "model_clip_tf": model_clip_tf,
                "model_clip_shape": model_clip.shape,
            })

            pct = 56 + int(12 * (idx + 1) / max(num_tiles, 1))
            _progress(f"Rock slope BFS ({idx + 1}/{num_tiles})", pct)
            _step_log("Tile %s: rock slope done (%.1fs)", tile_id, time.time() - t_tile)

    _timer("rock_slope_bfs", "end")
    _check_cancel()

    # Merge all berg excavation tiles
    _timer("berg_merge", "start")
    _progress("Merging berg excavation tiles", 69)
    _step_log("Merging %d berg excavation tiles...", len(clipped_berg_exc_tiles))
    t_merge = time.time()
    complete_berg_exc_path = os.path.join(scratch_folder, "berg_exc_complete.tif")
    if clipped_berg_exc_tiles:
        merge_rasters_min(clipped_berg_exc_tiles, complete_berg_exc_path)
    _step_log("Berg excavation merge complete (%.1fs)", time.time() - t_merge)
    _timer("berg_merge", "end")

    _check_cancel()

    # Buffer the merged berg excavation
    _timer("buffer", "start")
    _progress("Buffering berg excavation (+1m)", 72)
    _step_log("Buffering berg excavation (distance=%.1fm, cells=%d)...",
              cfg.get("BUFFER_DISTANCE", 1.0),
              int(cfg.get("BUFFER_DISTANCE", 1.0) / cfg["CELL_SIZE"]))
    berg_exc_complete_arr, berg_exc_complete_tf, _ = read_geotiff(complete_berg_exc_path)
    buffer_zone = buffer_excavation(berg_exc_complete_arr, cell_size=cfg["CELL_SIZE"])
    _step_log("Buffer zone: %d new cells", int(buffer_zone.sum()))

    # Save buffer zone as GeoTIFF so workers can read it from disk
    buffer_zone_path = os.path.join(scratch_folder, "buffer_zone.tif")
    write_geotiff(buffer_zone.astype(np.float32), berg_exc_complete_tf, cfg["CRS"], buffer_zone_path)
    # Free large arrays — all tile processing reads from disk via file paths
    del buffer_zone, berg_exc_complete_arr, berg_exc_complete_tf
    gc.collect()
    log_memory_usage(logger, "After buffer (freed large arrays)")
    _timer("buffer", "end")

    _check_cancel()

    # Second pass: buffer + merge + soil slope (requires terrain)
    _timer("soil_slope_bfs", "start")
    final_exc_tiles: list[str] = []
    if has_terrain:
        _progress("Per-tile soil slope BFS (0/%d)" % num_tiles, 74)
        _step_log("Starting second pass: buffer merge + soil slope BFS (%d tiles)...", num_tiles)

        # Soil BFS is memory-intensive (each worker runs BFS on 1000x1000 tiles at 0.2m).
        # On Windows, parallel workers get killed when system memory is tight.
        # Use sequential processing at fine resolution to avoid OOM.
        use_parallel_soil = (num_tiles >= 3 and max_parallel_tiles >= 2
                             and cfg["CELL_SIZE"] >= 0.5)

        if use_parallel_soil:
            _step_log("Using parallel soil slope BFS: up to %d concurrent tiles", max_parallel_tiles)

            # Prepare args for all tiles (pass file paths, not arrays, to avoid pickle issues)
            soil_args = []
            for td in tile_data:
                soil_args.append((
                    td["tile_id"], td["bounds"],
                    model_raster_path, berg_raster_path,
                    complete_berg_exc_path, terrain_raster_path,
                    buffer_zone_path,
                    cfg["CELL_SIZE"], cfg.get("SOIL_SLOPE_DIVISOR", 1.5),
                    cfg["CRS"], scratch_folder,
                ))

            with ProcessPoolExecutor(max_workers=max_parallel_tiles) as executor:
                futures = {executor.submit(_process_soil_tile, args): args[0] for args in soil_args}
                for idx, future in enumerate(as_completed(futures)):
                    _check_cancel()
                    result = future.result()
                    if result is None:
                        continue  # tile had no overlap
                    final_exc_tiles.append(result["final_exc_path"])

                    pct = 74 + int(12 * (idx + 1) / max(num_tiles, 1))
                    _progress(f"Soil slope BFS ({idx + 1}/{num_tiles})", pct)
                    _step_log("Tile %s: soil slope done", result["tile_id"])
        else:
            # Sequential processing — all data read from disk per tile (no large arrays in memory)
            for idx, td in enumerate(tile_data):
                _check_cancel()
                tile_id = td["tile_id"]
                tile_bounds = td["bounds"]
                t_tile = time.time()

                _step_log("Tile %s [%d/%d]: clipping rasters from disk...",
                          tile_id, idx + 1, num_tiles)

                model_clip, model_clip_tf = clip_raster_to_bounds(model_raster_path, tile_bounds)
                berg_clip, _ = clip_raster_to_bounds(berg_raster_path, tile_bounds)
                berg_exc_clip, _ = clip_raster_to_bounds(complete_berg_exc_path, tile_bounds)
                terrain_clip, _ = clip_raster_to_bounds(terrain_raster_path, tile_bounds)
                buf_clip, _ = clip_raster_to_bounds(buffer_zone_path, tile_bounds)

                # Skip tiles with no model data or no terrain
                if model_clip is None or terrain_clip is None:
                    _step_log("Tile %s: no overlap with rasters — skipping", tile_id)
                    continue
                if berg_clip is None:
                    berg_clip = np.full_like(model_clip, np.nan)
                if berg_exc_clip is None:
                    berg_exc_clip = np.full_like(model_clip, np.nan)
                if buf_clip is None:
                    buf_clip = np.full_like(model_clip, np.nan)

                buff_berg = merge_buffer_with_berg(berg_clip, berg_exc_clip, buf_clip, cfg["CELL_SIZE"])
                merged_model = merge_berg_with_models(buff_berg, model_clip)

                _step_log("Tile %s: running soil slope BFS (divisor=%.1f)...",
                          tile_id, cfg.get("SOIL_SLOPE_DIVISOR", 1.5))
                final_exc = propagate_soil_slope(
                    merged_model, berg_exc_clip, terrain_clip, cfg["CELL_SIZE"]
                )

                final_exc_path = os.path.join(scratch_folder, f"final_tile_{tile_id}.tif")
                write_geotiff(final_exc, model_clip_tf, cfg["CRS"], final_exc_path)

                # Explicit cleanup to prevent memory accumulation over 141 tiles
                del model_clip, berg_clip, berg_exc_clip, terrain_clip, buf_clip
                del buff_berg, merged_model, final_exc
                if idx % 20 == 0:
                    gc.collect()
                final_exc_tiles.append(final_exc_path)

                pct = 74 + int(12 * (idx + 1) / max(num_tiles, 1))
                _progress(f"Soil slope BFS ({idx + 1}/{num_tiles})", pct)
                _step_log("Tile %s: soil slope done (%.1fs)", tile_id, time.time() - t_tile)
    else:
        _step_log("Skipping soil slope BFS — no terrain data available")
        final_exc_tiles = clipped_berg_exc_tiles
        _step_log("Using %d berg excavation tiles as final result", len(final_exc_tiles))
    _timer("soil_slope_bfs", "end")

    _check_cancel()

    # ── 14. Merge all tiles → FINAL_RESULT_RASTER.tif ────────────────────
    _timer("final_merge", "start")
    _progress("Merging final tiles", 88)
    _step_log("Merging %d final tiles...", len(final_exc_tiles))
    t_merge = time.time()
    final_result_path = os.path.join(final_out_path, "FINAL_RESULT_RASTER.tif")
    if final_exc_tiles:
        merge_rasters_min(final_exc_tiles, final_result_path)
    _step_log("Final result raster complete (%.1fs)", time.time() - t_merge)
    _timer("final_merge", "end")

    _check_cancel()

    # ── 15. Volume calculations ───────────────────────────────────────────
    _timer("volume_calc", "start")
    _progress("Calculating volumes", 91)
    volumes = {}
    if has_terrain:
        _step_log("Calculating cut/fill volumes...")
        t_vol = time.time()
        volumes = calculate_all_volumes(
            os.path.join(final_out_path, "TERRAIN_MERGED_RASTER.tif"),
            os.path.join(final_out_path, "BERG_MERGED_RASTER.tif"),
            final_result_path,
            cfg["CELL_SIZE"],
        )
        _step_log("Volume results (%.1fs):", time.time() - t_vol)
        for k, v in volumes.items():
            _step_log("  %s = %.2f", k, v)
    else:
        _step_log("Skipping volume calculations — no terrain data available")
        volumes = {
            "VOL_BERG_DAGSONE_m3": 0.0,
            "VEKT_BERG_DAGSONE_kg": 0.0,
            "VOL_SEDIMENT_m3": 0.0,
            "VOL_SEDIMENT_DIESEL_LITER": 0.0,
        }
        _step_log("WARNING: Volume results are zero — pin terrain XMLs in ACC and re-run")
    _timer("volume_calc", "end")

    _check_cancel()

    # ── 16. Tunnel volumes ────────────────────────────────────────────────
    _timer("tunnel_volume", "start")
    _progress("Calculating tunnel volumes", 94)
    tunnel_vol = 0.0
    tunnel_weight = 0.0
    if tunnel_meshes:
        _step_log("Calculating tunnel volumes from %d meshes...", len(tunnel_meshes))
        t_tv = time.time()
        tunnel_vol, tunnel_weight = calculate_tunnel_volume(tunnel_meshes, cfg["CELL_SIZE"])
        _step_log("Tunnel volume: %.2f m³, weight: %.2f kg (%.1fs)",
                  tunnel_vol, tunnel_weight, time.time() - t_tv)
    else:
        _step_log("No tunnel meshes — skipping tunnel volume calculation")

    volumes["VOL_BERG_TUNNEL_m3"] = tunnel_vol
    volumes["VEKT_BERG_TUNNEL_kg"] = tunnel_weight
    _timer("tunnel_volume", "end")

    _check_cancel()

    # ── 17. Write output ──────────────────────────────────────────────────
    _timer("output", "start")
    _progress("Writing output files", 97)
    csv_path = os.path.join(final_out_path, "volumes.csv")
    excel_path = os.path.join(final_out_path, "masseuttak_bb5.xlsx")
    _step_log("Writing CSV: %s", csv_path)
    write_volumes_csv(volumes, csv_path)
    _step_log("Writing Excel: %s", excel_path)
    write_volumes_excel(volumes, excel_path)
    _timer("output", "end")

    # ── 18. Cleanup ───────────────────────────────────────────────────────
    _progress("Cleaning up scratch", 99)
    _step_log("Cleaning up scratch folder...")
    safe_delete(scratch_folder)

    _progress("Pipeline complete", 100)
    total_time = _elapsed(t0)
    _step_log("Pipeline complete in %s", total_time)
    _step_log("Output folder: %s", final_out_path)
    _step_log("Excel: %s", excel_path)
    _step_log("Final volumes:")
    for k, v in volumes.items():
        _step_log("  %s = %.2f", k, v)

    log_memory_usage(logger, "Pipeline end")

    return {
        "volumes": volumes,
        "output_folder": final_out_path,
        "file_manifest": file_manifest,
        "timings": stage_timings,
    }


if __name__ == "__main__":
    run()
