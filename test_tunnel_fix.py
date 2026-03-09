"""
Test harness for tunnel fix strategies.

Reuses pre-computed rasters from a completed run, processes only tiles overlapping
the tunnel area, and compares results. Target: <90 seconds per iteration.

Usage:
    python test_tunnel_fix.py --strategy A --threshold 5
    python test_tunnel_fix.py --strategy B --threshold 3
    python test_tunnel_fix.py --strategy D --threshold 10
    python test_tunnel_fix.py --strategy baseline  # run without fix for comparison
"""

import argparse
import logging
import math
import os
import sys
import time
from collections import deque

import arcpy
import numpy as np

# Import processing functions from mass_calc_v2 (they're standalone)
from mass_calc_v2 import (
    filter_model_under_berg,
    generate_berg_excavation,
    merge_buffer_with_berg,
    merge_berg_with_existing_models,
    generate_final_excavation,
)
from tunnel_fix_strategies import (
    strategy_a_depth_aware_clip,
    strategy_b_post_clip_restore,
    strategy_d_depth_aware_bfs_block,
    strategy_e_terrain_fill,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("test_tunnel_fix.log", mode="w"),
    ],
)

CELL_SIZE = 0.2
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GRID_PATH = os.path.join(SCRIPT_DIR, r"SCRIPT_HELP_FILES\AOI.gdb\INDEX_GRID_200_overlap")
MUNKEBOTN_MASK = os.path.join(SCRIPT_DIR, r"SCRIPT_HELP_FILES\munkebotn_mask.tif")
EXPAND_RFT = os.path.join(SCRIPT_DIR, r"SCRIPT_HELP_FILES\Expand.rft.xml")


def find_latest_output():
    """Find the latest results folder that has all required rasters."""
    output_dir = os.path.join(SCRIPT_DIR, "output")
    required = ["TERRAIN_MERGED_RASTER.tif", "BERG_MERGED_RASTER.tif", "FINAL_RESULT_RASTER.tif"]
    for folder in sorted(os.listdir(output_dir), reverse=True):
        path = os.path.join(output_dir, folder)
        if os.path.isdir(path) and all(os.path.exists(os.path.join(path, r)) for r in required):
            return path
    # Fallback to last folder
    folders = sorted(os.listdir(output_dir))
    return os.path.join(output_dir, folders[-1])


def get_cached_paths(output_folder, scratch_source=None):
    """Get paths to pre-computed rasters.

    scratch_source: folder containing cached rasters (default: scratch_backup/)
    """
    if scratch_source is None:
        # Prefer scratch_backup (survives legacy runs that delete scratch/)
        backup = os.path.join(SCRIPT_DIR, "scratch_backup")
        scratch_source = backup if os.path.exists(backup) else os.path.join(SCRIPT_DIR, "scratch")

    gdb_name = [f for f in os.listdir(output_folder) if f.endswith(".gdb")][0]
    results_gdb = os.path.join(output_folder, gdb_name)

    return {
        "model_pre_clip": os.path.join(scratch_source, "MERGED_MODEL_RASTER.tif"),
        "model_post_clip": os.path.join(scratch_source, "CLIPPED_MODEL_RASTER.tif"),
        "tunnel_multipatch": os.path.join(results_gdb, "MERGED_TUNNEL_RASTER"),
        "tunnel_min_raster": os.path.join(scratch_source, "MERGED_TUNNEL_RASTER.tif"),
        "terrain": os.path.join(output_folder, "TERRAIN_MERGED_RASTER.tif"),
        "berg": os.path.join(output_folder, "BERG_MERGED_RASTER.tif"),
        "final_result": os.path.join(output_folder, "FINAL_RESULT_RASTER.tif"),
    }


def ensure_tunnel_max_raster(paths, scratch_folder):
    """Generate TUNNEL_MAX_HEIGHT_RASTER.tif from the multipatch if not cached."""
    max_path = os.path.join(scratch_folder, "TUNNEL_MAX_HEIGHT_RASTER.tif")
    if os.path.exists(max_path):
        logging.info("Using cached tunnel max raster: %s", max_path)
        return max_path

    logging.info("Generating tunnel MAXIMUM_HEIGHT raster (one-time)...")
    t0 = time.time()
    tunnel_mp = paths["tunnel_multipatch"]
    if not arcpy.Exists(tunnel_mp):
        logging.error("Tunnel multipatch not found: %s", tunnel_mp)
        sys.exit(1)

    result = arcpy.conversion.MultipatchToRaster(
        tunnel_mp, max_path, CELL_SIZE, "MAXIMUM_HEIGHT"
    )
    logging.info("Tunnel max raster generated in %.1fs: %s", time.time() - t0, max_path)
    return max_path


def get_tunnel_aoi_tiles(tunnel_raster_path):
    """Find grid tiles that overlap the tunnel raster extent."""
    tunnel_ras = arcpy.Raster(tunnel_raster_path)
    t_ext = tunnel_ras.extent

    # Create a polygon from the tunnel extent for spatial query
    tunnel_polygon = t_ext.polygon

    tiles = []
    with arcpy.da.SearchCursor(
        GRID_PATH, ["GRIDNR", "SHAPE@"],
        spatial_filter=tunnel_polygon,
        spatial_relationship="INTERSECTS",
    ) as cursor:
        for row in cursor:
            ext = row[1].extent
            bbox = f"{ext.XMin} {ext.YMin} {ext.XMax} {ext.YMax}"
            tiles.append((row[0], bbox, ext))

    logging.info("Found %d tiles overlapping tunnel extent", len(tiles))
    for t in tiles:
        logging.info("  Tile %s: (%.0f,%.0f)-(%.0f,%.0f)",
                     t[0], t[2].XMin, t[2].YMin, t[2].XMax, t[2].YMax)
    return tiles


def run_tiles_sequential(tiles, full_model_raster, full_berg_raster, full_terrain_raster,
                          scratch_folder):
    """Run Phase A + barrier + Phase B on the given tiles sequentially."""
    t0 = time.time()

    # Phase A: clip + filter + berg excavation
    phase_a_results = []
    for grid_id, bbox, ext in tiles:
        t_tile = time.time()
        out_model = os.path.join(scratch_folder, f"model_clip_{grid_id}.tif")
        out_berg = os.path.join(scratch_folder, f"berg_clip_{grid_id}.tif")

        arcpy.management.Clip(full_model_raster, bbox, out_model,
                              nodata_value="3,4e+38",
                              maintain_clipping_extent="NO_MAINTAIN_EXTENT")
        arcpy.management.Clip(full_berg_raster, bbox, out_berg,
                              nodata_value="3,4e+38",
                              maintain_clipping_extent="NO_MAINTAIN_EXTENT")

        # Bad clip check
        cell_ext = arcpy.Extent(ext.XMin, ext.YMin, ext.XMax, ext.YMax)
        threshold = (250 / CELL_SIZE) ** 2
        for rp in [out_berg, out_model]:
            tmp = arcpy.Raster(rp)
            if tmp.width * tmp.height > threshold:
                const = arcpy.sa.CreateConstantRaster(1, "FLOAT", CELL_SIZE, cell_ext)
                null_r = arcpy.sa.SetNull(const, const, "VALUE > 0")
                null_r.save(rp)

        suffix = f"{grid_id}.tif"
        filtered = filter_model_under_berg(out_model, out_berg, CELL_SIZE,
                                           f"filtered_tile_{suffix}", scratch_folder)
        berg_exc = generate_berg_excavation(filtered, out_berg, CELL_SIZE,
                                            f"berg_exc_tile_{suffix}", scratch_folder)

        phase_a_results.append({
            "grid_id": grid_id, "model_clip": out_model, "berg_clip": out_berg,
            "filtered": filtered, "berg_exc": berg_exc,
            "elapsed_s": time.time() - t_tile,
        })
        logging.info("Phase A tile %s: %.1fs", grid_id, time.time() - t_tile)

    t_a = time.time() - t0
    logging.info("Phase A total: %.1fs (%d tiles)", t_a, len(tiles))

    # Barrier: merge + buffer
    t_barrier = time.time()
    berg_exc_tiles = [r["berg_exc"] for r in phase_a_results]
    complete_berg_exc = arcpy.ia.Merge(berg_exc_tiles, "MIN")
    berg_exc_out = os.path.join(scratch_folder, "berg_exc_complete_test.tif")
    complete_berg_exc.save(berg_exc_out)

    is_null = arcpy.ia.Apply(complete_berg_exc.catalogPath, "IsNull")
    berg_exc_buffered = arcpy.ia.Apply(
        is_null, EXPAND_RFT,
        {"number_cells": int(1 / CELL_SIZE), "zone_values": "0"},
    )
    buffered_path = os.path.join(scratch_folder, "distance_raster_test.tif")
    berg_exc_buffered.save(buffered_path)
    logging.info("Barrier: %.1fs", time.time() - t_barrier)

    # Phase B: buffer merge + model merge + terrain clip + final excavation
    phase_b_results = []
    for ra, (grid_id, bbox, ext) in zip(phase_a_results, tiles):
        t_tile = time.time()
        suffix = f"{grid_id}.tif"

        out_buff = os.path.join(scratch_folder, f"buffer_clip_{grid_id}.tif")
        arcpy.management.Clip(buffered_path, bbox, out_buff,
                              nodata_value="3,4e+38",
                              maintain_clipping_extent="NO_MAINTAIN_EXTENT")

        buff_exc = merge_buffer_with_berg(ra["berg_clip"], ra["berg_exc"], out_buff,
                                          CELL_SIZE, f"buff_exc_tile_{suffix}", scratch_folder)

        final_model = merge_berg_with_existing_models(buff_exc, ra["model_clip"], CELL_SIZE,
                                                       f"final_model_tile_{suffix}", scratch_folder)

        terrain_clip = os.path.join(scratch_folder, f"terrain_clip_{grid_id}.tif")
        arcpy.management.Clip(full_terrain_raster, bbox, terrain_clip,
                              nodata_value="3,4e+38",
                              maintain_clipping_extent="NO_MAINTAIN_EXTENT")

        final_tile = generate_final_excavation(buff_exc, final_model, terrain_clip,
                                               CELL_SIZE, f"final_tile_{suffix}", scratch_folder)

        phase_b_results.append({
            "grid_id": grid_id, "final_model": final_model, "final_tile": final_tile,
            "elapsed_s": time.time() - t_tile,
        })
        logging.info("Phase B tile %s: %.1fs", grid_id, time.time() - t_tile)

    t_total = time.time() - t0
    logging.info("Phase B total: %.1fs", time.time() - t0 - t_a)

    # Merge final tiles
    all_tiles = [r["final_model"] for r in phase_b_results] + \
                [r["final_tile"] for r in phase_b_results]
    merged = arcpy.ia.Merge(all_tiles, "MIN")
    merged_path = os.path.join(scratch_folder, "FINAL_RESULT_TEST.tif")
    merged.save(merged_path)

    return merged_path, t_total


def compute_volumes(final_raster_path, terrain_raster_path, berg_raster_path, scratch_folder):
    """Compute terrain and berg excavation volumes using CutFill."""
    volumes = {}

    try:
        terrain_cut = arcpy.ddd.CutFill(
            in_before_surface=terrain_raster_path,
            in_after_surface=final_raster_path,
        )
        terrain_ras = arcpy.Raster(terrain_cut)
        terrain_vol = sum(v for v in terrain_ras.RAT["VOLUME"] if v > 0)
        volumes["terrain_m3"] = terrain_vol
    except Exception as e:
        logging.warning("Terrain volume calc failed: %s", e)
        volumes["terrain_m3"] = None

    try:
        berg_cut = arcpy.ddd.CutFill(
            in_before_surface=berg_raster_path,
            in_after_surface=final_raster_path,
        )
        berg_ras = arcpy.Raster(berg_cut)
        berg_vol = sum(v for v in berg_ras.RAT["VOLUME"] if v > 0)
        volumes["berg_m3"] = berg_vol
    except Exception as e:
        logging.warning("Berg volume calc failed: %s", e)
        volumes["berg_m3"] = None

    return volumes


def compare_rasters(test_path, baseline_path, scratch_folder):
    """Compare test result against baseline (current buggy result)."""
    test = arcpy.Raster(test_path)
    baseline = arcpy.Raster(baseline_path)

    # Compute diff raster
    diff = test - baseline
    diff_path = os.path.join(scratch_folder, "DIFF_RASTER.tif")
    diff.save(diff_path)

    # Stats
    try:
        logging.info("Diff raster stats: min=%.2f, max=%.2f, mean=%.2f",
                     diff.minimum or 0, diff.maximum or 0, diff.mean or 0)
    except:
        pass

    # Count pixels that changed (may fail on very large rasters)
    try:
        test_arr = arcpy.RasterToNumPyArray(test, nodata_to_value=np.nan)
        base_arr = arcpy.RasterToNumPyArray(baseline, nodata_to_value=np.nan)
        restored = np.isnan(base_arr) & ~np.isnan(test_arr)
        removed = ~np.isnan(base_arr) & np.isnan(test_arr)
        logging.info("Pixel changes: %d restored (were NaN), %d removed (now NaN)",
                     np.sum(restored), np.sum(removed))
    except RuntimeError as e:
        logging.warning("Pixel comparison skipped (raster too large): %s", e)

    return diff_path


def main():
    parser = argparse.ArgumentParser(description="Test tunnel fix strategies")
    parser.add_argument("--strategy", choices=["A", "B", "D", "E", "baseline"],
                        default="A", help="Fix strategy (default: A)")
    parser.add_argument("--threshold", type=float, default=5.0,
                        help="Depth threshold in meters (default: 5.0)")
    parser.add_argument("--output-folder", type=str, default=None,
                        help="Specific output folder to use (default: latest)")
    parser.add_argument("--scratch-source", type=str, default=None,
                        help="Folder with cached rasters (default: scratch_backup/ or scratch/)")
    args = parser.parse_args()

    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(25832)
    arcpy.env.overwriteOutput = True

    print(f"=== Tunnel Fix Test: strategy={args.strategy}, threshold={args.threshold}m ===")
    t_total = time.time()

    # Locate cached data
    output_folder = args.output_folder or find_latest_output()
    paths = get_cached_paths(output_folder, args.scratch_source)
    logging.info("Using output folder: %s", output_folder)

    # Verify inputs exist
    for name, path in paths.items():
        exists = arcpy.Exists(path) if ".gdb" in path else os.path.exists(path)
        status = "OK" if exists else "MISSING"
        logging.info("  %s: %s [%s]", name, path, status)
        if not exists and name != "tunnel_multipatch":
            logging.error("Required input missing: %s", path)
            sys.exit(1)

    # Create test scratch folder
    scratch_folder = os.path.join(SCRIPT_DIR, "scratch_test")
    os.makedirs(scratch_folder, exist_ok=True)

    # Ensure tunnel max raster exists (save alongside other cached rasters)
    scratch_source = args.scratch_source or (
        os.path.join(SCRIPT_DIR, "scratch_backup") if os.path.exists(os.path.join(SCRIPT_DIR, "scratch_backup"))
        else os.path.join(SCRIPT_DIR, "scratch")
    )
    tunnel_max_path = ensure_tunnel_max_raster(paths, scratch_source)

    # Set snap raster
    arcpy.env.snapRaster = paths["model_pre_clip"]

    # Apply strategy to get the clipped model raster
    bfs_block_mask = None
    if args.strategy == "baseline":
        # Use the existing clipped model (with the bug)
        clipped_model = paths["model_post_clip"]
        logging.info("Baseline mode: using existing clipped model")
    elif args.strategy == "A":
        clipped_model = strategy_a_depth_aware_clip(
            paths["model_pre_clip"], paths["tunnel_min_raster"],
            tunnel_max_path, paths["terrain"],
            MUNKEBOTN_MASK, scratch_folder, args.threshold,
        )
    elif args.strategy == "B":
        clipped_model = strategy_b_post_clip_restore(
            paths["model_pre_clip"], paths["tunnel_min_raster"],
            tunnel_max_path, paths["terrain"],
            MUNKEBOTN_MASK, scratch_folder, args.threshold,
        )
    elif args.strategy == "D":
        clipped_model, bfs_block_mask = strategy_d_depth_aware_bfs_block(
            paths["model_pre_clip"], paths["tunnel_min_raster"],
            tunnel_max_path, paths["terrain"],
            MUNKEBOTN_MASK, scratch_folder, args.threshold,
        )
    elif args.strategy == "E":
        clipped_model = strategy_e_terrain_fill(
            paths["model_pre_clip"], paths["tunnel_min_raster"],
            tunnel_max_path, paths["terrain"],
            MUNKEBOTN_MASK, scratch_folder, args.threshold,
        )

    t_strategy = time.time() - t_total
    print(f"Strategy applied in {t_strategy:.1f}s")

    # Find tiles overlapping tunnel
    tiles = get_tunnel_aoi_tiles(paths["tunnel_min_raster"])
    if not tiles:
        logging.error("No tiles overlap tunnel — check tunnel raster")
        sys.exit(1)

    # Run tile processing on tunnel-area tiles
    print(f"Processing {len(tiles)} tunnel-area tiles...")
    result_path, t_tiles = run_tiles_sequential(
        tiles, clipped_model, paths["berg"], paths["terrain"], scratch_folder,
    )
    print(f"Tile processing: {t_tiles:.1f}s")

    # Compute volumes
    print("Computing volumes...")
    volumes = compute_volumes(result_path, paths["terrain"], paths["berg"], scratch_folder)
    print(f"\n{'='*60}")
    print(f"Strategy: {args.strategy} (threshold={args.threshold}m)")
    print(f"Terrain volume: {volumes.get('terrain_m3', 'N/A'):,.0f} m3")
    print(f"Berg volume:    {volumes.get('berg_m3', 'N/A'):,.0f} m3")
    print(f"{'='*60}")

    # Compare against baseline if not running baseline
    if args.strategy != "baseline" and os.path.exists(paths["final_result"]):
        print("Comparing against baseline...")
        diff_path = compare_rasters(result_path, paths["final_result"], scratch_folder)
        print(f"Diff raster saved: {diff_path}")

    t_elapsed = time.time() - t_total
    print(f"\nTotal time: {t_elapsed:.1f}s")
    logging.info("Test complete in %.1fs", t_elapsed)

    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")


if __name__ == "__main__":
    main()
