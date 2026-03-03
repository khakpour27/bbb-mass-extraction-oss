"""Main pipeline orchestrator for the BBB mass extraction calculation.

Replicates the processing flow of the original arcpy-based mass_calc.py
using open-source libraries.

Usage:
    python runner.py
"""

import logging
import os
import shutil
import stat
import time
from datetime import datetime

import numpy as np
import rasterio.windows

import config as _config_module
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
from utils import list_files_by_ext, setup_logging
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
]


class PipelineCancelled(Exception):
    """Raised when the pipeline is cancelled by the user."""


def _build_config(overrides: dict | None = None) -> dict:
    """Build a config dict from module defaults merged with runtime overrides."""
    cfg = {}
    for key in _CONFIG_KEYS:
        cfg[key] = getattr(_config_module, key)
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


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(config: dict | None = None,
        progress_cb=None,
        cancel_flag=None) -> None:
    """Run the full mass extraction pipeline.

    Parameters
    ----------
    config : dict, optional
        Runtime overrides for config.py values.
    progress_cb : callable(step_name: str, pct: int), optional
        Called at each major step with a label and cumulative percentage.
    cancel_flag : threading.Event, optional
        If set, the pipeline will abort between steps.
    """
    cfg = _build_config(config)
    t0 = time.time()

    def _progress(step: str, pct: int):
        if progress_cb:
            progress_cb(step, pct)

    def _check_cancel():
        if cancel_flag and cancel_flag.is_set():
            raise PipelineCancelled("Pipeline cancelled by user")

    def _step_log(msg: str, *args):
        """Log a message with elapsed time prefix."""
        prefix = f"[{_elapsed(t0)}]"
        logger.info(f"{prefix} {msg}", *args)

    # ── 1. Setup ──────────────────────────────────────────────────────────
    _progress("Setting up directories", 1)
    _step_log("Pipeline starting...")
    _step_log("Config: CELL_SIZE=%.2f, CRS=%s, MAX_CORES=%d",
              cfg["CELL_SIZE"], cfg["CRS"], cfg.get("MAX_CORES", 12))

    run_time = datetime.now().strftime("%Y_%m_%d_%H_%M")
    output_folder = "output"
    scratch_folder = "scratch"

    os.makedirs(output_folder, exist_ok=True)
    if os.path.exists(scratch_folder):
        safe_delete(scratch_folder)
    os.makedirs(scratch_folder, exist_ok=True)

    final_out_path = os.path.join(output_folder, f"results_{run_time}")
    os.makedirs(final_out_path, exist_ok=True)

    log_path = os.path.join(final_out_path, "results.log")
    setup_logging(log_path)
    _step_log("Output folder: %s", final_out_path)

    _check_cancel()

    # ── 2. List & filter input files ──────────────────────────────────────
    _progress("Scanning input files", 3)
    _step_log("Scanning for IFC files in: %s", cfg["MODEL_FOLDER_PATH"])
    ifc_list = list_files_by_ext(cfg["MODEL_FOLDER_PATH"], "*.ifc")
    _step_log("Found %d total IFC files", len(ifc_list))

    model_list = list_model_ifcs(ifc_list)
    tunnel_list = list_tunnel_ifcs(ifc_list)
    berg_list = list_berg_ifcs(cfg["BERG_PATH"])
    terrain_xml_list = list_land_xmls(cfg["TERRAIN_PATH"])

    _step_log("Model IFCs: %d files", len(model_list))
    for m in model_list:
        _step_log("  Model: %s", os.path.basename(m))

    _step_log("Tunnel IFCs: %d files", len(tunnel_list))
    for t in tunnel_list:
        _step_log("  Tunnel: %s", os.path.basename(t))

    _step_log("Berg IFCs: %d files", len(berg_list))
    for b in berg_list:
        _step_log("  Berg: %s", os.path.basename(b))

    _step_log("Terrain XMLs: %d files", len(terrain_xml_list))
    for t in terrain_xml_list:
        _step_log("  Terrain: %s", os.path.basename(t))

    # Apply file limit if set — pick files from same domain for coherent tests
    max_files = int(cfg.get("MAX_MODEL_FILES", 0))
    if max_files > 0:
        # Domain categories recognised in IFC filenames
        _DOMAIN_CATEGORIES = {
            "VA": "fm_VA",
            "Veg": "fm_Veg",
            "FVG": "fm_FVG",
            "Ele": "fm_Ele",
            "Spo": "fm_Spo",
            "KONS": "fm_KONS",
            "Geo": "fm_Geo",
        }

        def _group_by_domain(paths):
            """Group file paths by domain category extracted from filename."""
            groups: dict[str, list[str]] = {}
            ungrouped: list[str] = []
            for p in paths:
                basename = os.path.basename(p)
                matched = False
                for cat_name, cat_substr in _DOMAIN_CATEGORIES.items():
                    if cat_substr in basename:
                        groups.setdefault(cat_name, []).append(p)
                        matched = True
                        break
                if not matched:
                    ungrouped.append(p)
            return groups, ungrouped

        def _apply_limit(paths, label):
            if len(paths) <= max_files:
                return paths

            groups, ungrouped = _group_by_domain(paths)

            _step_log("MAX_MODEL_FILES=%d — selecting %d %s files (of %d total)",
                      max_files, max_files, label, len(paths))

            # Log discovered domain groups
            if groups:
                _step_log("Domain groups for %s:", label)
                for cat, files in sorted(groups.items()):
                    _step_log("  %s: %d files", cat, len(files))
                if ungrouped:
                    _step_log("  (ungrouped): %d files", len(ungrouped))

            if groups:
                # Pick from the largest category, sorted by name for geo-proximity
                largest_cat = max(groups, key=lambda k: len(groups[k]))
                cat_files = sorted(groups[largest_cat])
                selected = cat_files[:max_files]
                _step_log("Selected %d files from category '%s' (sorted by name for geo-proximity)",
                          len(selected), largest_cat)
            else:
                # Fallback: sort alphabetically (geo-proximity by convention)
                selected = sorted(paths)[:max_files]
                _step_log("No domain categories found — picking first %d alphabetically", len(selected))

            for p in selected:
                try:
                    sz = os.path.getsize(p) / (1024 * 1024)
                except OSError:
                    sz = 0
                _step_log("  %.1f MB  %s", sz, os.path.basename(p))
            return selected

        model_list = _apply_limit(model_list, "model")
        tunnel_list = _apply_limit(tunnel_list, "tunnel")
        berg_list = _apply_limit(berg_list, "berg")
        terrain_xml_list = _apply_limit(terrain_xml_list, "terrain")

    # Verify files are readable (catch ACC cloud stubs early)
    all_input_files = model_list + tunnel_list + berg_list + terrain_xml_list
    unreadable = []
    for p in all_input_files:
        try:
            with open(p, "rb") as fh:
                header = fh.read(16)
            if len(header) == 0:
                unreadable.append((p, "empty file (0 bytes) — ACC cloud stub not synced"))
        except OSError as e:
            unreadable.append((p, str(e)))

    if unreadable:
        _step_log("WARNING: %d file(s) cannot be read:", len(unreadable))
        for p, reason in unreadable:
            _step_log("  UNREADABLE: %s — %s", os.path.basename(p), reason)
        _step_log("")
        _step_log("These are ACC (Autodesk Construction Cloud) cloud placeholders.")
        _step_log("The Autodesk Desktop Connector service is not running.")
        _step_log("FIX: Start 'Autodesk Desktop Connector' from the Start Menu,")
        _step_log("     wait for files to sync, then re-run the pipeline.")
        _step_log("ALT: Copy the XML files manually to a local folder and update")
        _step_log("     TERRAIN_PATH in the config to point there.")
        _step_log("")
        # Remove unreadable files from all lists
        bad_paths = {p for p, _ in unreadable}
        before = len(model_list) + len(tunnel_list) + len(berg_list) + len(terrain_xml_list)
        model_list = [p for p in model_list if p not in bad_paths]
        tunnel_list = [p for p in tunnel_list if p not in bad_paths]
        berg_list = [p for p in berg_list if p not in bad_paths]
        terrain_xml_list = [p for p in terrain_xml_list if p not in bad_paths]
        after = len(model_list) + len(tunnel_list) + len(berg_list) + len(terrain_xml_list)
        _step_log("Removed %d unreadable files. Continuing with %d files.",
                  before - after, after)

    if not model_list:
        raise RuntimeError("No readable model IFC files found. "
                           "Pin/download files in Autodesk Desktop Connector first.")

    _check_cancel()

    # ── 3. Parse IFCs in parallel → trimesh meshes ────────────────────────
    _progress("Importing model IFCs (0/%d)" % len(model_list), 5)
    _step_log("Starting model IFC import (%d files)...", len(model_list))

    # Separate sporsystem from others before parsing
    spor_paths = [p for p in model_list if "Sporsystem" in p]
    non_spor_paths = [p for p in model_list if "Sporsystem" not in p]

    # Progress callback for per-file feedback
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

    _check_cancel()

    # ── 5. Rasterize models → merged model raster GeoTIFF ────────────────
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

    # ── 6. Reference transform for snapping ───────────────────────────────
    ref_transform = model_transform

    _check_cancel()

    # ── 7. Rasterize tunnels ──────────────────────────────────────────────
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

    _check_cancel()

    # ── 8. Create + apply exclusion mask ──────────────────────────────────
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

    _check_cancel()

    # ── 9. Model footprint polygon ────────────────────────────────────────
    _progress("Generating model footprint", 33)
    _step_log("Computing model footprint polygon...")
    footprint = get_model_footprint(model_raster_path)
    _step_log("Model footprint area: %.0f m²", footprint.area)

    _check_cancel()

    # ── 10. Parse terrain → merged terrain raster ─────────────────────────
    terrain_raster_path = None
    has_terrain = len(terrain_xml_list) > 0
    if has_terrain:
        _progress("Parsing terrain LandXML", 36)
        _step_log("Parsing %d terrain LandXML files...", len(terrain_xml_list))
        t_terrain = time.time()
        terrain_arr, terrain_tf = parse_and_rasterize_terrain(terrain_xml_list, cfg["CELL_SIZE"])
        terrain_tf = snap_transform(terrain_tf, ref_transform, cfg["CELL_SIZE"])
        terrain_raster_path = os.path.join(scratch_folder, "TERRAIN_MERGED_RASTER.tif")
        write_geotiff(terrain_arr, terrain_tf, cfg["CRS"], terrain_raster_path)
        write_geotiff(terrain_arr, terrain_tf, cfg["CRS"], os.path.join(final_out_path, "TERRAIN_MERGED_RASTER.tif"))
        _step_log("Terrain raster complete: %dx%d (%.1fs)",
                  terrain_arr.shape[1], terrain_arr.shape[0], time.time() - t_terrain)
    else:
        _step_log("WARNING: No readable terrain XML files — skipping terrain raster. "
                  "Volume calculations will be unavailable.")

    _check_cancel()

    # ── 11. Rasterize berg → merged berg raster ───────────────────────────
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

    _check_cancel()

    # ── 12. Load grid index, get intersecting tiles ───────────────────────
    _progress("Loading grid index", 54)
    _step_log("Loading grid index from %s...", cfg["GRID_PATH"])
    grid_gdf = load_grid_index(cfg["GRID_PATH"])
    tiles = get_intersecting_tiles(grid_gdf, footprint)
    num_tiles = len(tiles)
    _step_log("Found %d intersecting tiles (of %d total grid cells)", num_tiles, len(grid_gdf))

    _check_cancel()

    # ── 13. Per-tile processing ───────────────────────────────────────────
    _progress("Per-tile rock slope BFS (0/%d)" % num_tiles, 56)
    _step_log("Starting per-tile processing: %d tiles...", num_tiles)
    clipped_berg_exc_tiles: list[str] = []
    final_exc_tiles: list[str] = []

    # First pass: clip model and berg tiles, filter, rock slope BFS
    tile_data: list[dict] = []
    for idx, (_, tile_row) in enumerate(tiles.iterrows()):
        _check_cancel()
        tile_bounds = tile_row.geometry.bounds
        tile_id = tile_row.get("GRIDNR", idx)
        t_tile = time.time()

        _step_log("Tile %s [%d/%d]: clipping model + berg...", tile_id, idx + 1, num_tiles)

        model_clip, model_clip_tf = clip_raster_to_bounds(model_raster_path, tile_bounds)
        berg_clip, berg_clip_tf = clip_raster_to_bounds(berg_raster_path, tile_bounds)

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
            "model_clip": model_clip,
            "model_clip_tf": model_clip_tf,
            "berg_clip": berg_clip,
            "berg_clip_tf": berg_clip_tf,
        })

        pct = 56 + int(12 * (idx + 1) / max(num_tiles, 1))
        _progress(f"Rock slope BFS ({idx + 1}/{num_tiles})", pct)
        _step_log("Tile %s: rock slope done (%.1fs)", tile_id, time.time() - t_tile)

    _check_cancel()

    # Merge all berg excavation tiles
    _progress("Merging berg excavation tiles", 69)
    _step_log("Merging %d berg excavation tiles...", len(clipped_berg_exc_tiles))
    t_merge = time.time()
    complete_berg_exc_path = os.path.join(scratch_folder, "berg_exc_complete.tif")
    if clipped_berg_exc_tiles:
        merge_rasters_min(clipped_berg_exc_tiles, complete_berg_exc_path)
    _step_log("Berg excavation merge complete (%.1fs)", time.time() - t_merge)

    _check_cancel()

    # Buffer the merged berg excavation
    _progress("Buffering berg excavation (+1m)", 72)
    _step_log("Buffering berg excavation (distance=%.1fm, cells=%d)...",
              cfg.get("BUFFER_DISTANCE", 1.0),
              int(cfg.get("BUFFER_DISTANCE", 1.0) / cfg["CELL_SIZE"]))
    berg_exc_complete_arr, berg_exc_complete_tf, _ = read_geotiff(complete_berg_exc_path)
    buffer_zone = buffer_excavation(berg_exc_complete_arr, cell_size=cfg["CELL_SIZE"])
    _step_log("Buffer zone: %d new cells", int(buffer_zone.sum()))

    _check_cancel()

    # Second pass: buffer + merge + soil slope (requires terrain)
    if has_terrain:
        _progress("Per-tile soil slope BFS (0/%d)" % num_tiles, 74)
        _step_log("Starting second pass: buffer merge + soil slope BFS (%d tiles)...", num_tiles)
        for idx, td in enumerate(tile_data):
            _check_cancel()
            tile_id = td["tile_id"]
            tile_bounds = td["bounds"]
            model_clip = td["model_clip"]
            model_clip_tf = td["model_clip_tf"]
            berg_clip = td["berg_clip"]
            t_tile = time.time()

            _step_log("Tile %s [%d/%d]: clipping buffer + berg excavation...",
                      tile_id, idx + 1, num_tiles)

            berg_exc_clip, _ = clip_raster_to_bounds(complete_berg_exc_path, tile_bounds)
            berg_flate_clip = berg_clip

            buf_window = rasterio.windows.from_bounds(*tile_bounds, transform=berg_exc_complete_tf)
            buf_window = buf_window.intersection(
                rasterio.windows.Window(
                    0, 0, buffer_zone.shape[1], buffer_zone.shape[0]
                )
            )
            r_start = max(0, int(buf_window.row_off))
            r_end = min(buffer_zone.shape[0], int(buf_window.row_off + buf_window.height))
            c_start = max(0, int(buf_window.col_off))
            c_end = min(buffer_zone.shape[1], int(buf_window.col_off + buf_window.width))
            buf_clip = buffer_zone[r_start:r_end, c_start:c_end]

            buff_berg = merge_buffer_with_berg(berg_flate_clip, berg_exc_clip, buf_clip, cfg["CELL_SIZE"])
            merged_model = merge_berg_with_models(buff_berg, model_clip)

            terrain_clip, terrain_clip_tf = clip_raster_to_bounds(
                terrain_raster_path, tile_bounds
            )

            _step_log("Tile %s: running soil slope BFS (divisor=%.1f)...",
                      tile_id, cfg.get("SOIL_SLOPE_DIVISOR", 1.5))
            final_exc = propagate_soil_slope(
                merged_model, berg_exc_clip, terrain_clip, cfg["CELL_SIZE"]
            )

            final_exc_path = os.path.join(scratch_folder, f"final_tile_{tile_id}.tif")
            write_geotiff(final_exc, model_clip_tf, cfg["CRS"], final_exc_path)
            final_exc_tiles.append(final_exc_path)

            pct = 74 + int(12 * (idx + 1) / max(num_tiles, 1))
            _progress(f"Soil slope BFS ({idx + 1}/{num_tiles})", pct)
            _step_log("Tile %s: soil slope done (%.1fs)", tile_id, time.time() - t_tile)
    else:
        _step_log("Skipping soil slope BFS — no terrain data available")
        # Use berg excavation tiles as final output
        final_exc_tiles = clipped_berg_exc_tiles
        _step_log("Using %d berg excavation tiles as final result", len(final_exc_tiles))

    _check_cancel()

    # ── 14. Merge all tiles → FINAL_RESULT_RASTER.tif ────────────────────
    _progress("Merging final tiles", 88)
    _step_log("Merging %d final tiles...", len(final_exc_tiles))
    t_merge = time.time()
    final_result_path = os.path.join(final_out_path, "FINAL_RESULT_RASTER.tif")
    if final_exc_tiles:
        merge_rasters_min(final_exc_tiles, final_result_path)
    _step_log("Final result raster complete (%.1fs)", time.time() - t_merge)

    _check_cancel()

    # ── 15. Volume calculations ───────────────────────────────────────────
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

    _check_cancel()

    # ── 16. Tunnel volumes ────────────────────────────────────────────────
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

    _check_cancel()

    # ── 17. Write output ──────────────────────────────────────────────────
    _progress("Writing output files", 97)
    csv_path = os.path.join(final_out_path, "volumes.csv")
    excel_path = os.path.join(final_out_path, "masseuttak_bb5.xlsx")
    _step_log("Writing CSV: %s", csv_path)
    write_volumes_csv(volumes, csv_path)
    _step_log("Writing Excel: %s", excel_path)
    write_volumes_excel(volumes, excel_path)

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


if __name__ == "__main__":
    run()
