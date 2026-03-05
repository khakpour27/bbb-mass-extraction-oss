"""
mass_calc_v2.py — Performance-optimized version of mass_calc.py

Changes vs original:
  1. Grid processing parallelized with multiprocessing.Pool (Phase A / barrier / Phase B)
  2. NumPy nested loops vectorized in filter_model_under_berg, merge_buffer_with_berg, merge_berg_with_existing_models
  3. Terrain raster object cached outside loop
  4. Adjacent loops combined (loops 5+6, loops 7a+7b)
  5. Multipatch metadata validation parallelized in merge_and_rasterize_multipatches
  6. --sequential fallback flag for debugging, --workers N flag

Original mass_calc.py is unchanged — rollback by running runner.py instead of runner_v2.py.
"""
import arcpy
import math
import glob
import os
import shutil
import time
import stat
import json
import argparse
from collections import deque
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count
import csv
import logging


#############################################################################################################
### FUNCTIONS FOR RASTER CREATION
#############################################################################################################
def list_files_by_ext(path: str, ext: str) -> list[str]:
    pattern = os.path.join(path, f"*{ext}")
    files = glob.glob(pattern)
    return files

def list_model_ifcs(ifc_list: list) -> list[str]:
    substrings = ["fm_Veg", "fm_VA", "fm_FVG", "fm_Ele", "fm_Spo_Sporsystem"]
    models = [f for f in ifc_list if any(sub in f for sub in substrings) and "_alt" not in f]
    return models

def list_tunnel_ifcs(ifc_list: list) -> list[str]:
    tunnels = [f for f in ifc_list if f.endswith("sprengning.ifc") and "fm_Geo" in f]
    return tunnels

def list_berg_ifcs(berg_path: str) -> list[str]:
    berg_ifcs = [os.path.join(berg_path, f) for f in os.listdir(berg_path) if "Antatt-bergoverflate" in f and f.endswith(".ifc")]
    return berg_ifcs

def list_land_xmls(xml_folder: str) -> list[str]:
    land_xmls = [os.path.join(xml_folder, f) for f in os.listdir(xml_folder) if f.endswith(".xml") and "Terrengoverflate" in f]
    return land_xmls

def clean_file_name(filename: str) -> str:
    """cleans names so that the requirements for feature classes in gdb are satisfied"""
    name = filename.replace(".ifc", "")
    if name[0] in ["_", "0","1","2","3","4","5","6","7","8","9"]:
        name = "x_"+name

    illegals = ["-", ".", "(", ")", "[", "]", ":", " "]
    return "".join([c if c not in illegals else "_" for c in name])

IFC_CACHE_DIR = "ifc_cache"
IFC_CACHE_MANIFEST = os.path.join(IFC_CACHE_DIR, "manifest.json")

def _load_cache_manifest():
    if os.path.exists(IFC_CACHE_MANIFEST):
        with open(IFC_CACHE_MANIFEST, "r") as f:
            return json.load(f)
    return {}

def _save_cache_manifest(manifest):
    with open(IFC_CACHE_MANIFEST, "w") as f:
        json.dump(manifest, f, indent=2)

def import_ifc_worker(args):
    ifc_path, scratch_gdb, spatial_ref = args
    name = clean_file_name(os.path.basename(ifc_path).replace(".ifc", ""))
    gdb_path = os.path.join(IFC_CACHE_DIR, f"{name}.gdb")
    if os.path.exists(gdb_path):
        shutil.rmtree(gdb_path, ignore_errors=True)
    temp_gdb = arcpy.management.CreateFileGDB(IFC_CACHE_DIR, f"{name}.gdb")
    result = arcpy.conversion.BIMFileToGeodatabase(
        ifc_path,
        temp_gdb,
        name,
        spatial_ref,
        include_floorplan='EXCLUDE_FLOORPLAN'
    )
    return result.getOutput(0)

def import_ifcs_as_multipatch(ifc_path_list, scratch_gdb, spatial_ref):
    print(f"Checking cache for {len(ifc_path_list)} IFC files...")
    manifest = _load_cache_manifest()

    to_convert = []
    cached_results = {}
    for ifc_path in ifc_path_list:
        mtime = str(os.path.getmtime(ifc_path))
        cached = manifest.get(ifc_path)
        cached_output = cached.get("output", "") if cached else ""
        cached_gdb = cached_output.split(".gdb")[0] + ".gdb" if ".gdb" in cached_output else ""
        if cached and cached.get("mtime") == mtime and cached_gdb and os.path.exists(cached_gdb):
            cached_results[ifc_path] = cached["output"]
        else:
            to_convert.append(ifc_path)

    print(f"  {len(cached_results)} cached, {len(to_convert)} to convert...")

    new_results = {}
    if to_convert:
        args_list = [(path, scratch_gdb, spatial_ref) for path in to_convert]
        with Pool(cpu_count()) as pool:
            results = pool.map(import_ifc_worker, args_list)
        for ifc_path, result in zip(to_convert, results):
            new_results[ifc_path] = result
            manifest[ifc_path] = {
                "mtime": str(os.path.getmtime(ifc_path)),
                "output": result
            }
        _save_cache_manifest(manifest)

    bim_files = []
    for ifc_path in ifc_path_list:
        if ifc_path in cached_results:
            bim_files.append(cached_results[ifc_path])
        else:
            bim_files.append(new_results[ifc_path])
    return bim_files


def _validate_multipatch(fc_path):
    """Worker: validate a single multipatch feature class (GetCount + extent check).
    Returns a dict with status info for statistics gathering."""
    try:
        count = int(arcpy.management.GetCount(fc_path).getOutput(0))
        if count == 0:
            return {"status": "empty", "path": fc_path, "name": os.path.basename(fc_path), "count": 0}
        fc_ext = arcpy.Describe(fc_path).extent
        if (not math.isnan(fc_ext.XMin) and
            fc_ext.XMin > 200000 and fc_ext.XMax < 400000 and
            fc_ext.YMin > 6600000 and fc_ext.YMax < 6800000):
            return {"status": "valid", "path": fc_path, "name": os.path.basename(fc_path),
                    "count": count,
                    "xmin": fc_ext.XMin, "ymin": fc_ext.YMin, "xmax": fc_ext.XMax, "ymax": fc_ext.YMax}
        else:
            return {"status": "bad_extent", "path": fc_path, "name": os.path.basename(fc_path),
                    "count": count,
                    "xmin": fc_ext.XMin, "ymin": fc_ext.YMin, "xmax": fc_ext.XMax, "ymax": fc_ext.YMax}
    except Exception as e:
        return {"status": "error", "path": fc_path, "name": os.path.basename(fc_path), "error": str(e)}


def merge_and_rasterize_multipatches(multipatches, cell_size=0.1, outname="MERGED_MODEL_RASTER",
                                      *, scratch_folder="scratch", output_gdb=None, num_workers=1):
    """Merges multipatch input in memory and rasterizes the result.

    Change vs original: metadata validation (GetCount + extent) parallelized across workers.
    Returns arcpy.Result. Also populates stats on the result object as result._mp_stats.
    """
    print(f"Converting {len(multipatches)} multipatches to merged raster...")

    # Phase 1: enumerate all children per feature dataset (tracks per-GDB child counts)
    candidate_fcs = []
    gdb_child_counts = {}  # gdb_name -> {multipatch: N, other: N}
    non_multipatch_types = {}  # shapeType -> count
    for f in multipatches:
        gdb_name = os.path.basename(f.split(".gdb")[0]) if ".gdb" in f else os.path.basename(f)
        gdb_child_counts[gdb_name] = {"multipatch": 0, "other": 0, "children_total": 0}
        try:
            desc = arcpy.Describe(f)
            for child in desc.children:
                gdb_child_counts[gdb_name]["children_total"] += 1
                try:
                    if child.shapeType == "MultiPatch":
                        candidate_fcs.append(child.catalogPath)
                        gdb_child_counts[gdb_name]["multipatch"] += 1
                    else:
                        gdb_child_counts[gdb_name]["other"] += 1
                        non_multipatch_types[child.shapeType] = non_multipatch_types.get(child.shapeType, 0) + 1
                except Exception as e:
                    logging.warning("Skipping unreadable feature class in %s: %s", f, e)
        except Exception as e:
            logging.warning("Skipping unreadable GDB %s: %s", f, e)

    logging.info("[%s] Enumerated %d feature datasets -> %d multipatch candidates", outname, len(multipatches), len(candidate_fcs))
    if non_multipatch_types:
        logging.info("[%s] Non-multipatch geometry types skipped: %s", outname,
                     ", ".join(f"{t}={n}" for t, n in sorted(non_multipatch_types.items())))
    for gdb, counts in gdb_child_counts.items():
        logging.info("[%s]   %s: %d children (%d multipatch, %d other)",
                     outname, gdb, counts["children_total"], counts["multipatch"], counts["other"])

    # Phase 2: validate (GetCount + extent) in parallel
    if num_workers > 1 and len(candidate_fcs) > 10:
        with Pool(min(num_workers, len(candidate_fcs))) as pool:
            results = pool.map(_validate_multipatch, candidate_fcs)
    else:
        results = [_validate_multipatch(fc) for fc in candidate_fcs]

    # Tally validation outcomes
    multipatch_fts = []
    stats = {"valid": 0, "empty": 0, "bad_extent": 0, "error": 0,
             "total_objects": 0, "valid_objects": 0, "fc_details": []}
    for r in results:
        if r is None:
            stats["empty"] += 1
            continue
        status = r["status"]
        stats[status] = stats.get(status, 0) + 1
        if status == "valid":
            multipatch_fts.append(r["path"])
            stats["valid_objects"] += r["count"]
            stats["total_objects"] += r["count"]
            stats["fc_details"].append(r)
        elif status == "bad_extent":
            stats["total_objects"] += r["count"]
            logging.warning("[%s] Skipping FC with bad extent: %s (%d objects, %.0f,%.0f - %.0f,%.0f)",
                outname, r["name"], r["count"], r["xmin"], r["ymin"], r["xmax"], r["ymax"])
        elif status == "error":
            logging.warning("[%s] Skipping unreadable FC: %s (%s)", outname, r["name"], r.get("error", ""))
        elif status == "empty":
            pass  # already counted

    logging.info("[%s] Validation: %d valid (%d objects), %d empty, %d bad_extent, %d error — of %d candidates",
                 outname, stats["valid"], stats["valid_objects"], stats["empty"], stats["bad_extent"], stats["error"],
                 len(candidate_fcs))

    # Log top feature classes by object count
    top_fcs = sorted(stats["fc_details"], key=lambda x: x["count"], reverse=True)
    for fc in top_fcs[:15]:
        logging.info("[%s]   %s: %d objects (%.0f,%.0f - %.0f,%.0f)",
                     outname, fc["name"], fc["count"], fc["xmin"], fc["ymin"], fc["xmax"], fc["ymax"])
    if len(top_fcs) > 15:
        logging.info("[%s]   ... and %d more feature classes", outname, len(top_fcs) - 15)

    if len(multipatch_fts) == 0:
        logging.warning("No valid multipatch features found for %s, skipping", outname)
        return None
    merged_mps = arcpy.management.Merge(multipatch_fts, f"memory/{outname}")
    merged_count = int(arcpy.management.GetCount(f"memory/{outname}").getOutput(0))
    logging.info("[%s] Merged feature class: %d total objects from %d FCs", outname, merged_count, len(multipatch_fts))
    if output_gdb:
        arcpy.management.Merge(multipatch_fts, os.path.join(output_gdb, outname))
    outpath = os.path.join(scratch_folder, f"{outname}.tif")

    raster_result = arcpy.conversion.MultipatchToRaster(merged_mps, outpath, cell_size, "MINIMUM_HEIGHT")

    # Log raster properties
    try:
        out_ras = arcpy.Raster(raster_result.getOutput(0))
        ext = out_ras.extent
        logging.info("[%s] Raster: %d x %d px (%.1f x %.1f m), cell=%.2f, extent=(%.1f,%.1f)-(%.1f,%.1f), "
                     "Z range=%.2f - %.2f",
                     outname, out_ras.width, out_ras.height,
                     out_ras.width * cell_size, out_ras.height * cell_size, cell_size,
                     ext.XMin, ext.YMin, ext.XMax, ext.YMax,
                     out_ras.minimum if out_ras.minimum is not None else 0,
                     out_ras.maximum if out_ras.maximum is not None else 0)
    except Exception as e:
        logging.warning("[%s] Could not read raster properties: %s", outname, e)

    # Attach stats to result for later summary use
    raster_result._mp_stats = stats
    raster_result._mp_stats["merged_objects"] = merged_count
    raster_result._mp_stats["outname"] = outname

    return raster_result

def convert_landxml_to_tin(landxml_path: str, tin_output_folder: str, basename: str) -> str:
    output_path = os.path.join(scratch_folder, tin_output_folder)

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    output_tin_folder = arcpy.ddd.LandXMLToTin(landxml_path, output_path, basename)
    return output_tin_folder.getOutput(0)

def tins_to_merged_raster(tin_folder_path: str, cell_size=0.1) -> str:
    rasters = []
    tin_desc = arcpy.Describe(tin_folder_path)
    tin_list = [child.catalogPath for child in tin_desc.children]
    data_type = "FLOAT"
    z_factor = "1"
    sampling = "CELLSIZE"
    method = "LINEAR"
    for tin in tin_list:
        tin_name = os.path.basename(tin)
        raster_out_path = os.path.join(tin_folder_path, f"{tin_name}.tif")
        raster_out = arcpy.ddd.TinRaster(tin, raster_out_path, data_type,
                                            method,
                                            sampling,
                                            z_factor,
                                            cell_size)

        rasters.append(arcpy.Raster(raster_out))

    merged_raster = arcpy.ia.Merge(rasters, "MIN")
    merged_raster.save(f"{os.path.basename(tin_folder_path)}_MERGED_RASTER.tif")

    return merged_raster.catalogPath

#############################################################################################################
### FUNCTIONS FOR RASTER PROCESSING  (vectorized + scratch_folder as explicit parameter)
#############################################################################################################
def merge_buffer_with_berg(berg_flate, berg_excavation, berg_buffer, cell_size, out_name, scratch_folder) -> str:

    berg_flate = arcpy.Raster(berg_flate)
    berg_excavation = arcpy.Raster(berg_excavation)
    berg_buffer = arcpy.Raster(berg_buffer)

    rows = min(berg_excavation.height, berg_flate.height, berg_buffer.height)
    cols = min(berg_excavation.width, berg_flate.width, berg_buffer.width)

    np_berg_flate = arcpy.RasterToNumPyArray(berg_flate, nodata_to_value=np.nan)
    np_berg_exc = arcpy.RasterToNumPyArray(berg_excavation, nodata_to_value=np.nan)
    np_berg_buff = arcpy.RasterToNumPyArray(berg_buffer, nodata_to_value=9999)

    # Vectorized replacement of double nested loop (order matters — exc overrides buffer)
    out_raster = np.full((rows, cols), np.nan)
    mask_buffer = (np_berg_buff[:rows, :cols] == 0) & ~np.isnan(np_berg_flate[:rows, :cols])
    out_raster[mask_buffer] = np_berg_flate[:rows, :cols][mask_buffer]
    mask_exc = ~np.isnan(np_berg_exc[:rows, :cols])
    out_raster[mask_exc] = np_berg_exc[:rows, :cols][mask_exc]

    lower_left = arcpy.Point(berg_excavation.extent.XMin, berg_excavation.extent.YMin)
    out_raster = arcpy.NumPyArrayToRaster(out_raster, lower_left, cell_size, cell_size, value_to_nodata=np.nan)

    out_raster.save(os.path.join(scratch_folder, out_name))
    return out_raster.catalogPath


def filter_model_under_berg(model_raster, berg_raster, cell_size, out_name, scratch_folder) -> str:
    model_raster = arcpy.Raster(model_raster)
    berg_raster = arcpy.Raster(berg_raster)

    rows = min(model_raster.height, berg_raster.height)
    cols = min(model_raster.width, berg_raster.width)

    np_model = arcpy.RasterToNumPyArray(model_raster, nodata_to_value=np.nan)
    np_berg = arcpy.RasterToNumPyArray(berg_raster, nodata_to_value=np.nan)

    # Vectorized replacement of double nested loop
    mask = (~np.isnan(np_berg[:rows, :cols])
            & ~np.isnan(np_model[:rows, :cols])
            & (np_model[:rows, :cols] <= np_berg[:rows, :cols]))
    output_raster = np.where(mask, np_model[:rows, :cols], np.nan)

    lower_left = arcpy.Point(model_raster.extent.XMin, model_raster.extent.YMin)
    out_raster = arcpy.NumPyArrayToRaster(output_raster, lower_left, cell_size, cell_size, value_to_nodata=np.nan)

    out_path = os.path.join(scratch_folder, out_name)
    out_raster.save(out_path)
    return out_raster.catalogPath

def generate_berg_excavation(filtered_berg_model_raster, berg_raster, cell_size, out_name, scratch_folder) -> str:
    filtered_berg = arcpy.Raster(filtered_berg_model_raster)
    berg = arcpy.Raster(berg_raster)

    rows = min(filtered_berg.height, berg.height)
    cols = min(filtered_berg.width, berg.height)

    np_f_berg = arcpy.RasterToNumPyArray(filtered_berg, nodata_to_value=np.nan)
    np_berg = arcpy.RasterToNumPyArray(berg, nodata_to_value=np.nan)
    output_raster = np.copy(np_f_berg)

    neighbors = [
    (-1, 0, cell_size),        # up
    (1, 0, cell_size),         # down
    (0, -1, cell_size),        # left
    (0, 1, cell_size),         # right
    (-1, -1, cell_size * math.sqrt(2)), # up-left
    (-1, 1, cell_size * math.sqrt(2)),  # up-right
    (1, -1, cell_size * math.sqrt(2)),  # down-left
    (1, 1, cell_size * math.sqrt(2))    # down-right
    ]

    queue = deque()

    for r in range(rows):
        for c in range(cols):
            if not np.isnan(output_raster[r,c]):
                queue.append((r,c))

    in_queue = np.zeros(output_raster.shape, dtype=bool)

    while queue:
        r,c = queue.popleft()
        in_queue[r,c] = False
        current_elev = output_raster[r,c]
        berg_elev = np_berg[r,c]

        for dr, dc, dist in neighbors:
            nr, nc = r + dr, c + dc

            if 0 <= nr < rows and 0 <= nc < cols:
                neighbor_elev = output_raster[nr,nc]
                n_berg_elev = np_berg[nr, nc]

                berg_rise = dist * 10.0
                tent_berg_elev = current_elev + berg_rise

                if (not np.isnan(n_berg_elev) and (np.isnan(neighbor_elev) or tent_berg_elev < neighbor_elev)) and n_berg_elev > tent_berg_elev:
                    output_raster [nr,nc] = tent_berg_elev
                    if not in_queue[nr,nc]:
                        queue.append((nr,nc))
                        in_queue[nr, nc] = True


    lower_left = arcpy.Point(filtered_berg.extent.XMin, filtered_berg.extent.YMin)
    output = arcpy.NumPyArrayToRaster(output_raster, lower_left, cell_size, cell_size, value_to_nodata=np.nan)
    out_path = os.path.join(scratch_folder, out_name)
    output.save(out_path)
    return output.catalogPath

def generate_final_excavation(berg_excavation_raster, merged_model_raster, terrain_raster, cell_size, outname, scratch_folder):
    berg_exc = arcpy.Raster(berg_excavation_raster)
    model_merge = arcpy.Raster(merged_model_raster)
    terrain = arcpy.Raster(terrain_raster)

    rows = min(model_merge.height, berg_exc.height, terrain.height)
    cols = min(model_merge.width, berg_exc.width, terrain.width)

    np_berg = arcpy.RasterToNumPyArray(berg_exc, nodata_to_value=np.nan)
    np_terrain = arcpy.RasterToNumPyArray(terrain, nodata_to_value=np.nan)
    out_models = arcpy.RasterToNumPyArray(model_merge, nodata_to_value=np.nan)

    neighbors = [
    (-1, 0, cell_size),        # up
    (1, 0, cell_size),         # down
    (0, -1, cell_size),        # left
    (0, 1, cell_size),         # right
    (-1, -1, cell_size * math.sqrt(2)), # up-left
    (-1, 1, cell_size * math.sqrt(2)),  # up-right
    (1, -1, cell_size * math.sqrt(2)),  # down-left
    (1, 1, cell_size * math.sqrt(2))    # down-right
    ]

    queue = deque()
    for r in range(rows):
        for c in range(cols):
            if not np.isnan(out_models[r,c]):
                queue.append((r,c))

    in_queue = np.zeros(out_models.shape, dtype=bool)

    while queue:
        r,c = queue.popleft()
        in_queue[r,c] = False
        current_elev = out_models[r,c]

        for dr, dc, dist in neighbors:
            nr, nc = r + dr, c + dc

            if 0 <= nr < rows and 0 <= nc < cols:
                neighbor_elev = out_models[nr,nc]
                neighbor_berg = np_berg[nr,nc]
                terrain_elev = np_terrain[nr,nc]
                rise = dist/1.5
                tentativ_elev = current_elev + rise

                if (np.isnan(neighbor_elev) or tentativ_elev < neighbor_elev) and np.isnan(neighbor_berg) and tentativ_elev < terrain_elev:
                    out_models[nr,nc] = tentativ_elev
                    if not in_queue[nr,nc]:
                        queue.append((nr,nc))
                        in_queue[nr,nc] = True

    lower_left = arcpy.Point(model_merge.extent.XMin, model_merge.extent.YMin)
    output = arcpy.NumPyArrayToRaster(out_models, lower_left, cell_size, cell_size, value_to_nodata=np.nan)
    out_path = os.path.join(scratch_folder, outname)
    output.save(out_path)
    return output.catalogPath

def merge_berg_with_existing_models(berg_excavation_raster, model_raster, cell_size, outname, scratch_folder):
    berg_model = arcpy.Raster(berg_excavation_raster)
    models = arcpy.Raster(model_raster)

    rows = min(models.height, berg_model.height)
    cols = min(models.width, berg_model.width)

    np_berg_m = arcpy.RasterToNumPyArray(berg_model, nodata_to_value=np.nan)
    np_models = arcpy.RasterToNumPyArray(models, nodata_to_value=np.nan)

    # Vectorized replacement: np.fmin ignores NaN automatically
    # - Where model is NaN and berg has value → berg value (fmin(NaN, berg) = berg)
    # - Where both exist and berg < model → berg value (fmin(model, berg) = min)
    # - Where berg is NaN → model stays (masked out by has_berg)
    out_models = np.copy(np_models[:rows, :cols])
    berg_slice = np_berg_m[:rows, :cols]
    has_berg = ~np.isnan(berg_slice)
    out_models[has_berg] = np.fmin(out_models[has_berg], berg_slice[has_berg])

    lower_left = arcpy.Point(models.extent.XMin, models.extent.YMin)
    output = arcpy.NumPyArrayToRaster(out_models, lower_left, cell_size, cell_size, value_to_nodata=np.nan)
    out_name = os.path.join(scratch_folder, outname)
    output.save(out_name)
    return output.catalogPath


#############################################################################################################
### MULTIPROCESSING WORKER FUNCTIONS FOR GRID PROCESSING
#############################################################################################################
def init_grid_worker(spatial_ref_wkid, snap_raster_path):
    """Pool initializer: check out extensions and configure arcpy env once per worker process."""
    import arcpy
    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(spatial_ref_wkid)
    arcpy.env.overwriteOutput = True
    if snap_raster_path:
        arcpy.env.snapRaster = snap_raster_path


def process_tile_phase_a(args):
    """Phase A worker: clip model+berg → filter_model_under_berg → generate_berg_excavation.

    Args is a tuple: (grid_id, bbox, ext_tuple, full_model_raster, full_berg_raster, cell_size, scratch_folder)
    ext_tuple is (XMin, YMin, XMax, YMax) — reconstructed into arcpy.Extent inside worker.
    """
    grid_id, bbox, ext_tuple, full_model_raster, full_berg_raster, cell_size, scratch_folder = args
    t_start = time.time()

    out_path_model = os.path.join(scratch_folder, f"model_clip_{grid_id}.tif")
    out_path_berg = os.path.join(scratch_folder, f"berg_clip_{grid_id}.tif")

    arcpy.management.Clip(
        in_raster=full_model_raster,
        rectangle=bbox,
        out_raster=out_path_model,
        nodata_value="3,4e+38",
        maintain_clipping_extent="NO_MAINTAIN_EXTENT"
    )

    arcpy.management.Clip(
        in_raster=full_berg_raster,
        rectangle=bbox,
        out_raster=out_path_berg,
        nodata_value="3,4e+38",
        maintain_clipping_extent="NO_MAINTAIN_EXTENT"
    )

    # Check for bad clips (oversized rasters from arcpy bug)
    cell_ext = arcpy.Extent(*ext_tuple)
    temp_berg = arcpy.Raster(out_path_berg)
    temp_model = arcpy.Raster(out_path_model)
    threshold_pixel_count = (250 / cell_size)**2

    if temp_berg.width * temp_berg.height > threshold_pixel_count:
        const_berg = arcpy.sa.CreateConstantRaster(1, "FLOAT", cell_size, cell_ext)
        null_berg = arcpy.sa.SetNull(const_berg, const_berg, "VALUE > 0")
        null_berg.save(out_path_berg)

    if temp_model.width * temp_model.height > threshold_pixel_count:
        const_model = arcpy.sa.CreateConstantRaster(1, "FLOAT", cell_size, cell_ext)
        null_model = arcpy.sa.SetNull(const_model, const_model, "VALUE > 0")
        null_model.save(out_path_model)

    # Filter model under berg
    suffix = f"{grid_id}.tif"
    filtered_tile = filter_model_under_berg(out_path_model, out_path_berg, cell_size, f"filtered_tile_{suffix}", scratch_folder)

    # Generate berg excavation
    berg_exc_tile = generate_berg_excavation(filtered_tile, out_path_berg, cell_size, f"berg_exc_tile_{suffix}", scratch_folder)

    return {
        "grid_id": grid_id,
        "model_clip": out_path_model,
        "berg_clip": out_path_berg,
        "filtered": filtered_tile,
        "berg_exc": berg_exc_tile,
        "elapsed_s": time.time() - t_start,
    }


def process_tile_phase_b(args):
    """Phase B worker: clip buffer → merge_buffer_with_berg → merge_berg_with_existing_models →
    clip terrain → generate_final_excavation.

    Args is a tuple: (grid_id, bbox, berg_exc_buffered, berg_clip, berg_exc_tile,
                       model_clip, full_terrain_raster, cell_size, scratch_folder)
    """
    (grid_id, bbox, berg_exc_buffered, berg_clip, berg_exc_tile,
     model_clip, full_terrain_raster, cell_size, scratch_folder) = args
    t_start = time.time()

    suffix = f"{grid_id}.tif"

    # Clip buffer to tile
    out_path_buff = os.path.join(scratch_folder, f"buffer_clip_{grid_id}.tif")
    arcpy.management.Clip(
        in_raster=berg_exc_buffered,
        rectangle=bbox,
        out_raster=out_path_buff,
        nodata_value="3,4e+38",
        maintain_clipping_extent="NO_MAINTAIN_EXTENT"
    )

    # Merge buffer with berg (loops 5+6 combined)
    buff_exc_tile = merge_buffer_with_berg(berg_clip, berg_exc_tile, out_path_buff, cell_size,
                                           f"buff_exc_tile_{suffix}", scratch_folder)

    # Merge berg with existing models
    final_model_tile = merge_berg_with_existing_models(buff_exc_tile, model_clip, cell_size,
                                                        f"final_model_tile_{suffix}", scratch_folder)

    # Clip terrain to tile
    terrain_clip = os.path.join(scratch_folder, f"terrain_clip_{grid_id}.tif")
    arcpy.management.Clip(
        in_raster=full_terrain_raster,
        rectangle=bbox,
        out_raster=terrain_clip,
        nodata_value="3,4e+38",
        maintain_clipping_extent="NO_MAINTAIN_EXTENT"
    )

    # Generate final excavation
    final_tile = generate_final_excavation(buff_exc_tile, final_model_tile, terrain_clip, cell_size,
                                            f"final_tile_{suffix}", scratch_folder)

    return {
        "grid_id": grid_id,
        "buff_exc": buff_exc_tile,
        "final_model": final_model_tile,
        "terrain_clip": terrain_clip,
        "final_tile": final_tile,
        "elapsed_s": time.time() - t_start,
    }


###############################################################################################################
### CLEANUP FUNCTIONS
###############################################################################################################

def remove_readonly(func, path, excinfo):
    os.chmod(path, stat.S_IWRITE)
    func(path)

def safe_delete(path, retries=3):
    for attempt in range(retries):
        try:
            if os.path.isdir(path):
                shutil.rmtree(path, onexc=remove_readonly)
            elif os.path.isfile(path):
                os.chmod(path, stat.S_IWRITE)
                os.remove(path)
            print(f"Deleted: {path}")
            break
        except PermissionError as e:
            print(f"PermissionError on {path}, retrying...")
            time.sleep(1)
        except Exception as e:
            print(f"Error: {e}")
            break

def cleanup_results(output_folder: str) -> None:
    results = [os.path.join(output_folder, f) for f in os.listdir(output_folder)]
    rm = results[0]
    safe_delete(rm)

###############################################################################################################
### SETUP AND ENTRY POINT
###############################################################################################################
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Test mode: process only 10 IFC files")
    parser.add_argument("--sequential", action="store_true", help="Disable multiprocessing for grid processing (debug mode)")
    parser.add_argument("--workers", type=int, default=12, help="Number of worker processes for grid processing (default: 12)")
    parser.add_argument("--prefix", type=str, default=None, help="Only include IFC files starting with this prefix (e.g. F03)")
    args = parser.parse_args()
    TEST_MODE = args.test
    NUM_WORKERS = 1 if args.sequential else min(args.workers, cpu_count())

    if not os.path.exists('output'):
        os.mkdir('output')

    if os.path.exists("scratch"):
        safe_delete("scratch")

    if not os.path.exists(IFC_CACHE_DIR):
        os.mkdir(IFC_CACHE_DIR)

    if len(os.listdir('output')) > 1:
        cleanup_results('output')

    arcpy.CheckOutExtension("3D")
    arcpy.CheckOutExtension("Spatial")


    run_time = datetime.now().strftime("%Y_%m_%d_%H_%M")
    output_folder = "output"

    os.mkdir(os.path.join(output_folder, f"results_{run_time}"))
    os.mkdir("scratch")
    arcpy.management.CreateFileGDB("scratch", "scratch.gdb")
    final_out_path = os.path.join(output_folder, f"results_{run_time}")
    log_path = os.path.join(final_out_path, "results.log")
    arcpy.management.CreateFileGDB(final_out_path, f"Results_{run_time}")
    output_gdb = os.path.join(final_out_path, f"Results_{run_time}.gdb")
    scratch_folder = "scratch"
    scratch_gdb = os.path.join(scratch_folder, "scratch.gdb")
    arcpy.env.scratchWorkspace = scratch_folder
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(25832)
    arcpy.env.overwriteOutput = True

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        encoding='utf-8',
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    mode_str = "TEST MODE (10 files)" if TEST_MODE else "FULL MODE"
    seq_str = "sequential" if args.sequential else f"{NUM_WORKERS} workers"
    logging.info("=== Script start (%s, %s) ===", mode_str, seq_str)
    logging.info("Environment: CPUs=%d, workers=%d", cpu_count(), NUM_WORKERS)
    print(f"=== mass_calc_v2.py ({mode_str}, {seq_str}) ===")

    pipeline_start = time.time()
    timings = {}  # phase_name -> seconds

    MODEL_FOLDER_PATH = r"C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Discipline models"
    TERRAIN_PATH = r"C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Existing condition models (CORAV)\Terrengflater"
    BERG_PATH = r"C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Existing condition models (CORAV)"
    GRID_PATH = r"SCRIPT_HELP_FILES\AOI.gdb\INDEX_GRID_200_overlap"
    MUNKEBOTN_MASK = r"SCRIPT_HELP_FILES\munkebotn_mask.tif"
    CELL_SIZE = 1.0 if TEST_MODE else 0.2
    logging.info("Cell size: %.2f, grid: %s", CELL_SIZE, GRID_PATH)


    #################################################################################################################################
    ### PROCESSSING STEPS - IFC, LANDXML INPUT to RASTER
    #################################################################################################################################

    ifc_list = list_files_by_ext(MODEL_FOLDER_PATH, "*.ifc")
    model_list = list_model_ifcs(ifc_list)
    tunnel_list = list_tunnel_ifcs(ifc_list)
    berg_list = list_berg_ifcs(BERG_PATH)
    terrain_xml_list = list_land_xmls(TERRAIN_PATH)

    # Optional prefix filter (e.g. --prefix F03 to match scbm's F03-only run)
    if args.prefix:
        pfx = args.prefix
        model_list = [m for m in model_list if os.path.basename(m).startswith(pfx)]
        tunnel_list = [t for t in tunnel_list if os.path.basename(t).startswith(pfx)]
        berg_list = [b for b in berg_list if os.path.basename(b).startswith(pfx)]
        terrain_xml_list = [t for t in terrain_xml_list if os.path.basename(t).startswith(pfx)]
        logging.info("PREFIX FILTER: %s — %d models, %d tunnels, %d berg, %d terrain",
                     pfx, len(model_list), len(tunnel_list), len(berg_list), len(terrain_xml_list))
        print(f"Prefix filter '{pfx}': {len(model_list)} models, {len(tunnel_list)} tunnels, {len(berg_list)} berg, {len(terrain_xml_list)} terrain")

    if TEST_MODE:
        test_prefix = "E03_011"
        model_list = [m for m in model_list if os.path.basename(m).startswith(test_prefix)]
        tunnel_list = [t for t in tunnel_list if os.path.basename(t).startswith(test_prefix)]
        if not tunnel_list:
            tunnel_list = tunnel_list[:1]
        berg_list = berg_list[:1]
        terrain_xml_list = terrain_xml_list[:1]
        logging.info("TEST MODE: area prefix=%s, %d models, %d tunnels, %d berg, %d terrain",
                     test_prefix, len(model_list), len(tunnel_list), len(berg_list), len(terrain_xml_list))

    logging.info("Input counts: %d models, %d tunnels, %d berg, %d terrain",
                 len(model_list), len(tunnel_list), len(berg_list), len(terrain_xml_list))
    print(f"Input: {len(model_list)} models, {len(tunnel_list)} tunnels, {len(berg_list)} berg, {len(terrain_xml_list)} terrain")

    # --- Input file inventory ---
    def _file_size_mb(path):
        try:
            return os.path.getsize(path) / (1024 * 1024)
        except OSError:
            return 0.0

    discipline_counts = {}  # discipline_code -> count
    area_counts = {}  # area_code (e.g. E03_011) -> count
    all_input_files = []

    for label, file_list in [("model", model_list), ("tunnel", tunnel_list), ("berg", berg_list), ("terrain", terrain_xml_list)]:
        total_mb = 0
        for f in file_list:
            name = os.path.basename(f)
            sz = _file_size_mb(f)
            total_mb += sz
            logging.info("Input %s: %s (%.1f MB)", label, name, sz)
            all_input_files.append({"type": label, "name": name, "size_mb": sz, "path": f})

            # Extract discipline code (e.g. fm_Veg, fm_VA, fm_Ele, gm_Geo)
            for part in name.replace("-", "_").split("_"):
                if part in ("fm", "gm"):
                    idx = name.replace("-", "_").split("_").index(part)
                    parts = name.replace("-", "_").split("_")
                    if idx + 1 < len(parts):
                        disc = f"{part}_{parts[idx+1]}"
                        discipline_counts[disc] = discipline_counts.get(disc, 0) + 1
                    break

            # Extract area code (e.g. E03_011)
            parts = name.split("_")
            if len(parts) >= 2:
                area = f"{parts[0]}_{parts[1]}"
                area_counts[area] = area_counts.get(area, 0) + 1

        if file_list:
            logging.info("  %s total: %d files, %.1f MB", label, len(file_list), total_mb)

    if discipline_counts:
        logging.info("Discipline breakdown: %s", ", ".join(f"{k}={v}" for k, v in sorted(discipline_counts.items())))
    if area_counts:
        logging.info("Area breakdown: %s", ", ".join(f"{k}={v}" for k, v in sorted(area_counts.items())))

    logging.info("Starting IFC import (model)...")
    t0 = time.time()
    bim_mps = import_ifcs_as_multipatch(model_list, scratch_gdb, arcpy.env.outputCoordinateSystem)
    timings["ifc_import_model"] = time.time() - t0
    logging.info("IFC import (model) completed in %.1f seconds", timings["ifc_import_model"])
    print(f"IFC import done in {timings['ifc_import_model']:.1f}s")
    print("Sinking Sporsystem 900mm...")

    # FIX: Copy Sporsystem feature datasets to scratch before adjusting Z,
    # so the cached GDBs are not modified (avoids cumulative -0.9m per run).
    spor_fds = [f for f in bim_mps if "Sporsystem" in f]
    adjusted_spor = []
    for spor_mod in spor_fds:
        ds_name = os.path.basename(spor_mod)
        copy_path = os.path.join(scratch_gdb, ds_name)
        arcpy.management.Copy(spor_mod, copy_path)
        desc = arcpy.Describe(copy_path)
        for child in desc.children:
            if child.shapeType == "MultiPatch":
                arcpy.management.Adjust3DZ(child.catalogPath, "NO_REVERSE", -0.9)
        adjusted_spor.append((spor_mod, copy_path))
        logging.info("Sporsystem %s: copied to scratch and adjusted Z by -0.9m", ds_name)

    # Replace cached paths with adjusted copies in the bim_mps list
    for orig, copy in adjusted_spor:
        idx = bim_mps.index(orig)
        bim_mps[idx] = copy

    print("Merging model multipatches and converting to raster...")
    logging.info("Merging and rasterizing model multipatches...")
    t0 = time.time()
    rasterize_stats = {}  # outname -> stats dict
    model_raster_result = merge_and_rasterize_multipatches(
        bim_mps, CELL_SIZE, "MERGED_MODEL_RASTER",
        scratch_folder=scratch_folder, output_gdb=output_gdb, num_workers=NUM_WORKERS
    )
    full_model_raster = model_raster_result.getOutput(0)
    if hasattr(model_raster_result, '_mp_stats'):
        rasterize_stats["MERGED_MODEL_RASTER"] = model_raster_result._mp_stats
    timings["model_rasterization"] = time.time() - t0
    logging.info("Model rasterization completed in %.1f seconds", timings["model_rasterization"])
    print(f"IFC models to raster complete ({timings['model_rasterization']:.1f}s)")

    arcpy.env.snapRaster = full_model_raster

    print("Creating tunnel mask...")
    logging.info("Starting IFC import (tunnel)...")
    t_tunnel = time.time()
    t0 = time.time()
    tunnel_mps = import_ifcs_as_multipatch(tunnel_list, scratch_gdb, arcpy.env.outputCoordinateSystem)
    logging.info("IFC import (tunnel) completed in %.1f seconds", time.time() - t0)
    tunnel_result = merge_and_rasterize_multipatches(
        tunnel_mps, CELL_SIZE, "MERGED_TUNNEL_RASTER",
        scratch_folder=scratch_folder, output_gdb=output_gdb, num_workers=NUM_WORKERS
    )
    if tunnel_result is not None and hasattr(tunnel_result, '_mp_stats'):
        rasterize_stats["MERGED_TUNNEL_RASTER"] = tunnel_result._mp_stats

    if tunnel_result is not None:
        full_tunnel_raster = tunnel_result.getOutput(0)
        tunnel_raster_mem = arcpy.Raster(full_tunnel_raster)
        merged_tunnel_mask = arcpy.ia.Merge([tunnel_raster_mem, MUNKEBOTN_MASK], "First")
    else:
        logging.info("No tunnel data, using munkebotn mask only")
        merged_tunnel_mask = arcpy.Raster(MUNKEBOTN_MASK)

    print("Clipping model raster against tunnel mask...")

    model_raster_mem = arcpy.Raster(full_model_raster)
    clipped_models_mem = arcpy.ia.Apply(model_raster_mem,
                                        "Clip",
                                        {"ClippingType":2, "ClippingRaster":merged_tunnel_mask, "Extent":model_raster_mem.extent.JSON})

    clipped_models_mem.save(os.path.join(scratch_folder, "CLIPPED_MODEL_RASTER.tif"))
    full_model_raster = os.path.join(scratch_folder, "CLIPPED_MODEL_RASTER.tif")

    timings["tunnel_processing"] = time.time() - t_tunnel
    logging.info("Tunnel processing completed in %.1f seconds", timings["tunnel_processing"])

    print("Generating model raster footprint...")
    model_raster_domain = arcpy.ddd.RasterDomain(
        in_raster=full_model_raster,
        out_feature_class=os.path.join(scratch_gdb, "MERGED_MODEL_RA_RasterDomain"),
        out_geometry_type="POLYGON"
    ).getOutput(0)


    print("Converting terrain and berg layers to raster...")
    logging.info("Converting %d terrain XMLs to raster...", len(terrain_xml_list))
    t0 = time.time()
    for terrain_xml in terrain_xml_list:
        basename = terrain_xml.split("-")[-1].replace(".xml","")
        convert_landxml_to_tin(terrain_xml, "terrain_TIN", basename)

    terrain_tin_folder = os.path.join(scratch_folder, "terrain_TIN")
    full_terrain_raster = tins_to_merged_raster(terrain_tin_folder, CELL_SIZE)
    terrain_output = arcpy.Raster(full_terrain_raster)
    terrain_output.save(os.path.join(final_out_path, "TERRAIN_MERGED_RASTER.tif"))
    try:
        t_ext = terrain_output.extent
        logging.info("[TERRAIN] Raster: %d x %d px (%.1f x %.1f m), cell=%.2f, extent=(%.1f,%.1f)-(%.1f,%.1f), "
                     "Z range=%.2f - %.2f",
                     terrain_output.width, terrain_output.height,
                     terrain_output.width * CELL_SIZE, terrain_output.height * CELL_SIZE, CELL_SIZE,
                     t_ext.XMin, t_ext.YMin, t_ext.XMax, t_ext.YMax,
                     terrain_output.minimum if terrain_output.minimum is not None else 0,
                     terrain_output.maximum if terrain_output.maximum is not None else 0)
    except Exception as e:
        logging.warning("[TERRAIN] Could not read raster properties: %s", e)
    timings["terrain_conversion"] = time.time() - t0
    logging.info("Terrain conversion completed in %.1f seconds", timings["terrain_conversion"])
    print(f"Terrain layers converted to raster ({timings['terrain_conversion']:.1f}s)")

    logging.info("Starting IFC import (berg)...")
    t0 = time.time()
    berg_mps = import_ifcs_as_multipatch(berg_list, scratch_gdb, arcpy.env.outputCoordinateSystem)
    logging.info("IFC import (berg) completed in %.1f seconds", time.time() - t0)
    berg_raster_result = merge_and_rasterize_multipatches(
        berg_mps, CELL_SIZE, "MERGED_BERG_RASTER",
        scratch_folder=scratch_folder, output_gdb=output_gdb, num_workers=NUM_WORKERS
    )
    full_berg_raster = berg_raster_result.getOutput(0)
    if hasattr(berg_raster_result, '_mp_stats'):
        rasterize_stats["MERGED_BERG_RASTER"] = berg_raster_result._mp_stats
    berg_output = arcpy.Raster(full_berg_raster)
    berg_output.save(os.path.join(final_out_path, "BERG_MERGED_RASTER.tif"))
    timings["berg_conversion"] = time.time() - t0
    logging.info("Berg conversion completed in %.1f seconds", timings["berg_conversion"])
    print(f"Berg layers converted to raster ({timings['berg_conversion']:.1f}s)")

    #################################################################################################################################
    ### PROCESSSING STEPS - RASTER (GRID PROCESSING)
    #################################################################################################################################

    grid_extents = []

    model_domain_geom = None

    with arcpy.da.SearchCursor(model_raster_domain, ["SHAPE@"]) as cursor:
        for row in cursor:
            model_domain_geom = row[0]

    with arcpy.da.SearchCursor(GRID_PATH, ["GRIDNR", "SHAPE@"], spatial_filter=model_domain_geom, spatial_relationship="INTERSECTS") as cursor:
        for row in cursor:
            ext = row[1].extent
            xmin = str(ext.XMin)
            ymin = str(ext.YMin)
            xmax = str(ext.XMax)
            ymax = str(ext.YMax)
            bbox = " ".join([xmin, ymin, xmax, ymax])
            grid_extents.append((row[0], bbox, ext))

    logging.info("Grid processing: %d tiles to process (%s)", len(grid_extents), seq_str)
    print(f"Grid processing: {len(grid_extents)} tiles ({seq_str})")
    t0 = time.time()

    if NUM_WORKERS > 1:
        # =====================================================================
        # PARALLEL GRID PROCESSING
        # =====================================================================

        # Build Phase A args (all picklable — strings, floats, tuples)
        tile_args_a = []
        for cell in grid_extents:
            ext = cell[2]
            tile_args_a.append((
                cell[0],                                    # grid_id
                cell[1],                                    # bbox string
                (ext.XMin, ext.YMin, ext.XMax, ext.YMax),   # extent as tuple
                full_model_raster,                          # path
                full_berg_raster,                            # path
                CELL_SIZE,
                scratch_folder,
            ))

        print(f"Phase A: clip + filter + berg excavation ({len(tile_args_a)} tiles, {NUM_WORKERS} workers)...")
        with Pool(NUM_WORKERS, initializer=init_grid_worker, initargs=(25832, full_model_raster)) as pool:
            phase_a_results = pool.map(process_tile_phase_a, tile_args_a)

        t_phase_a = time.time() - t0
        timings["grid_phase_a"] = t_phase_a
        tile_times_a = [r["elapsed_s"] for r in phase_a_results]
        logging.info("Phase A completed in %.1f seconds (%d tiles: min=%.1fs, max=%.1fs, avg=%.1fs)",
                     t_phase_a, len(tile_times_a), min(tile_times_a), max(tile_times_a),
                     sum(tile_times_a) / len(tile_times_a))
        for r in phase_a_results:
            logging.info("  Tile %s: %.1fs", r["grid_id"], r["elapsed_s"])
        print(f"Phase A complete ({t_phase_a:.1f}s, tiles: min={min(tile_times_a):.1f}s max={max(tile_times_a):.1f}s avg={sum(tile_times_a)/len(tile_times_a):.1f}s)")

        # ----- Barrier: merge all berg excavation tiles + buffer -----
        t_barrier = time.time()
        print("Merging intermediate results...")
        berg_exc_tiles = [r["berg_exc"] for r in phase_a_results]
        complete_berg_exc = arcpy.ia.Merge(berg_exc_tiles, "MIN")
        berg_exc_out = os.path.join(scratch_folder, "berg_exc_complete.tif")
        complete_berg_exc.save(berg_exc_out)
        print("Merge complete.")

        print("Buffering result...")
        is_null = arcpy.ia.Apply(complete_berg_exc.catalogPath, "IsNull")
        berg_exc_buffered = arcpy.ia.Apply(is_null,
                                           os.path.join(os.path.dirname(__file__), "SCRIPT_HELP_FILES", "Expand.rft.xml"),
                                           {"number_cells": int(1/CELL_SIZE), "zone_values": "0"}
                                            )
        berg_exc_buffered.save(os.path.join(scratch_folder, "distance_raster.tif"))
        berg_exc_buffered_path = os.path.join(scratch_folder, "distance_raster.tif")
        timings["grid_barrier"] = time.time() - t_barrier
        logging.info("Barrier (merge + buffer) completed in %.1f seconds", timings["grid_barrier"])

        # Build Phase B args
        tile_args_b = []
        for ra, cell in zip(phase_a_results, grid_extents):
            tile_args_b.append((
                ra["grid_id"],
                cell[1],                    # bbox string
                berg_exc_buffered_path,     # path
                ra["berg_clip"],            # path
                ra["berg_exc"],             # path
                ra["model_clip"],           # path
                full_terrain_raster,        # path
                CELL_SIZE,
                scratch_folder,
            ))

        print(f"Phase B: buffer merge + model merge + terrain clip + final excavation ({len(tile_args_b)} tiles, {NUM_WORKERS} workers)...")
        with Pool(NUM_WORKERS, initializer=init_grid_worker, initargs=(25832, full_model_raster)) as pool:
            phase_b_results = pool.map(process_tile_phase_b, tile_args_b)

        t_phase_b = time.time() - t0 - t_phase_a - timings["grid_barrier"]
        timings["grid_phase_b"] = t_phase_b
        tile_times_b = [r["elapsed_s"] for r in phase_b_results]
        logging.info("Phase B completed in %.1f seconds (%d tiles: min=%.1fs, max=%.1fs, avg=%.1fs)",
                     t_phase_b, len(tile_times_b), min(tile_times_b), max(tile_times_b),
                     sum(tile_times_b) / len(tile_times_b))
        for r in phase_b_results:
            logging.info("  Tile %s: %.1fs", r["grid_id"], r["elapsed_s"])
        print(f"Phase B complete ({t_phase_b:.1f}s, tiles: min={min(tile_times_b):.1f}s max={max(tile_times_b):.1f}s avg={sum(tile_times_b)/len(tile_times_b):.1f}s)")

        # Collect results — separate lists for model tiles and excavation tiles
        final_model_tiles = [r["final_model"] for r in phase_b_results]
        final_exc_tiles = [r["final_tile"] for r in phase_b_results]

    else:
        # =====================================================================
        # SEQUENTIAL GRID PROCESSING (--sequential flag or 1 worker)
        # =====================================================================
        t_seq_clip = time.time()
        clipped_model_tiles = []
        clipped_berg_tiles = []
        clipped_filtered_tiles = []
        clipped_berg_exc_tiles = []

        print("Clipping input rasters...")
        for cell in grid_extents:
            bbox = cell[1]
            id = cell[0]
            cell_ext = cell[2]
            out_path_model = os.path.join(scratch_folder, f"model_clip_{id}.tif")
            out_path_berg = os.path.join(scratch_folder, f"berg_clip_{id}.tif")
            clipped_model_tiles.append(out_path_model)
            clipped_berg_tiles.append(out_path_berg)

            arcpy.management.Clip(
                in_raster=full_model_raster,
                rectangle=bbox,
                out_raster=out_path_model,
                nodata_value="3,4e+38",
                maintain_clipping_extent="NO_MAINTAIN_EXTENT"
            )

            arcpy.management.Clip(
                in_raster=full_berg_raster,
                rectangle=bbox,
                out_raster=out_path_berg,
                nodata_value="3,4e+38",
                maintain_clipping_extent="NO_MAINTAIN_EXTENT"
            )

            temp_berg = arcpy.Raster(out_path_berg)
            temp_model = arcpy.Raster(out_path_model)

            threshold_pixel_count = (250 / CELL_SIZE)**2
            if temp_berg.width * temp_berg.height > threshold_pixel_count:
                const_berg = arcpy.sa.CreateConstantRaster(1, "FLOAT", CELL_SIZE, cell_ext)
                null_berg = arcpy.sa.SetNull(const_berg, const_berg, "VALUE > 0")
                null_berg.save(out_path_berg)

            if temp_model.width * temp_model.height > threshold_pixel_count:
                const_model = arcpy.sa.CreateConstantRaster(1, "FLOAT", CELL_SIZE, cell_ext)
                null_model = arcpy.sa.SetNull(const_model, const_model, "VALUE > 0")
                null_model.save(out_path_model)

        for model, berg in zip(clipped_model_tiles, clipped_berg_tiles):
            suffix = os.path.basename(model).split("_")[-1]
            filtered_tile = filter_model_under_berg(model, berg, CELL_SIZE, f"filtered_tile_{suffix}", scratch_folder)
            clipped_filtered_tiles.append(filtered_tile)

        timings["seq_clip_filter"] = time.time() - t_seq_clip
        logging.info("Sequential clip + filter completed in %.1f seconds", timings["seq_clip_filter"])

        print("Generating berg excavation")
        t_seq_berg = time.time()
        for model, berg in zip(clipped_filtered_tiles, clipped_berg_tiles):
            suffix = os.path.basename(model).split("_")[-1]
            berg_exc_tile = generate_berg_excavation(model, berg, CELL_SIZE, f"berg_exc_tile_{suffix}", scratch_folder)
            clipped_berg_exc_tiles.append(berg_exc_tile)

        timings["seq_berg_excavation"] = time.time() - t_seq_berg
        logging.info("Sequential berg excavation completed in %.1f seconds", timings["seq_berg_excavation"])

        print("Merging intermediate results...")
        t_barrier = time.time()
        complete_berg_exc = arcpy.ia.Merge(clipped_berg_exc_tiles, "MIN")
        berg_exc_out = os.path.join(scratch_folder, "berg_exc_complete.tif")
        complete_berg_exc.save(berg_exc_out)
        print("Merge complete.")

        print("Buffering result...")
        is_null = arcpy.ia.Apply(complete_berg_exc.catalogPath, "IsNull")
        berg_exc_buffered = arcpy.ia.Apply(is_null,
                                           os.path.join(os.path.dirname(__file__), "SCRIPT_HELP_FILES", "Expand.rft.xml"),
                                           {"number_cells": int(1/CELL_SIZE), "zone_values": "0"}
                                            )
        berg_exc_buffered.save(os.path.join(scratch_folder, "distance_raster.tif"))
        berg_exc_buffered = os.path.join(scratch_folder, "distance_raster.tif")
        timings["grid_barrier"] = time.time() - t_barrier
        logging.info("Barrier (merge + buffer) completed in %.1f seconds", timings["grid_barrier"])

        # Combined loops 4+5+6: clip buffer, merge buffer with berg, merge with models
        clipped_buff_berg_tiles = []
        final_model_tiles = []
        t_seq_merge = time.time()
        print("Clipping buffered result + merging with berg + merging with models...")
        for cell, berg, exc, model in zip(grid_extents, clipped_berg_tiles, clipped_berg_exc_tiles, clipped_model_tiles):
            bbox = cell[1]
            id = cell[0]
            suffix = f"{id}.tif"

            out_path_buff = os.path.join(scratch_folder, f"buffer_clip_{id}.tif")
            arcpy.management.Clip(
                in_raster=berg_exc_buffered,
                rectangle=bbox,
                out_raster=out_path_buff,
                nodata_value="3,4e+38",
                maintain_clipping_extent="NO_MAINTAIN_EXTENT"
            )

            buff_exc_tile = merge_buffer_with_berg(berg, exc, out_path_buff, CELL_SIZE, f"buff_exc_tile_{suffix}", scratch_folder)
            clipped_buff_berg_tiles.append(buff_exc_tile)

            final_model_tile = merge_berg_with_existing_models(buff_exc_tile, model, CELL_SIZE, f"final_model_tile_{suffix}", scratch_folder)
            final_model_tiles.append(final_model_tile)

        timings["seq_buffer_merge"] = time.time() - t_seq_merge
        logging.info("Sequential buffer + model merge completed in %.1f seconds", timings["seq_buffer_merge"])

        # Combined loops 7a+7b: clip terrain + generate final excavation
        final_exc_tiles = []
        t_seq_final = time.time()
        terrain_raster_obj = arcpy.Raster(full_terrain_raster)  # Cache outside loop
        print("Generating final excavation...")
        for cell, berg, mod in zip(grid_extents, clipped_buff_berg_tiles, final_model_tiles):
            bbox = cell[1]
            id = cell[0]
            suffix = f"{id}.tif"

            terrain_clip = os.path.join(scratch_folder, f"terrain_clip_{id}.tif")
            arcpy.management.Clip(
                in_raster=terrain_raster_obj,
                rectangle=bbox,
                out_raster=terrain_clip,
                nodata_value="3,4e+38",
                maintain_clipping_extent="NO_MAINTAIN_EXTENT"
            )

            final_tile = generate_final_excavation(berg, mod, terrain_clip, CELL_SIZE, f"final_tile_{suffix}", scratch_folder)
            final_exc_tiles.append(final_tile)

        timings["seq_final_excavation"] = time.time() - t_seq_final
        logging.info("Sequential terrain clip + final excavation completed in %.1f seconds", timings["seq_final_excavation"])

    # Final merge (both parallel and sequential paths converge here)
    t_merge = time.time()
    all_final_tiles = final_model_tiles + final_exc_tiles
    logging.info("Final merge: %d model tiles + %d excavation tiles = %d total", len(final_model_tiles), len(final_exc_tiles), len(all_final_tiles))
    final_result = arcpy.ia.Merge(all_final_tiles, "MIN")
    final_result.save(os.path.join(final_out_path, "FINAL_RESULT_RASTER.tif"))
    try:
        final_ras = arcpy.Raster(os.path.join(final_out_path, "FINAL_RESULT_RASTER.tif"))
        f_ext = final_ras.extent
        logging.info("[FINAL] Raster: %d x %d px (%.1f x %.1f m), cell=%.2f, extent=(%.1f,%.1f)-(%.1f,%.1f), "
                     "Z range=%.2f - %.2f",
                     final_ras.width, final_ras.height,
                     final_ras.width * CELL_SIZE, final_ras.height * CELL_SIZE, CELL_SIZE,
                     f_ext.XMin, f_ext.YMin, f_ext.XMax, f_ext.YMax,
                     final_ras.minimum if final_ras.minimum is not None else 0,
                     final_ras.maximum if final_ras.maximum is not None else 0)
    except Exception as e:
        logging.warning("[FINAL] Could not read raster properties: %s", e)
    timings["grid_final_merge"] = time.time() - t_merge
    timings["grid_total"] = time.time() - t0
    logging.info("Final merge completed in %.1f seconds", timings["grid_final_merge"])
    logging.info("Grid processing total: %.1f seconds", timings["grid_total"])
    print(f"Final result complete ({timings['grid_total']:.1f}s)")

    print("Calculating volumes...")
    t_vol = time.time()
    logging.info("Calculating volumes...")

    terrain_vol = 0
    berg_vol = 0

    try:
        terrain_cut = arcpy.ddd.CutFill(
            in_before_surface=os.path.join(final_out_path, "TERRAIN_MERGED_RASTER.tif"),
            in_after_surface=os.path.join(final_out_path, "FINAL_RESULT_RASTER.tif"),
            z_factor=1
        )
        terrain_cut = arcpy.Raster(terrain_cut)
        terrain_vol = sum([v for v in terrain_cut.RAT['VOLUME'] if v > 0])
    except Exception as e:
        logging.warning("Terrain CutFill failed (surfaces may not overlap): %s", e)

    try:
        berg_cut = arcpy.ddd.CutFill(
            in_before_surface=os.path.join(final_out_path, "BERG_MERGED_RASTER.tif"),
            in_after_surface=os.path.join(final_out_path, "FINAL_RESULT_RASTER.tif"),
            z_factor=1
        )
        berg_cut = arcpy.Raster(berg_cut)
        berg_vol = sum([v for v in berg_cut.RAT['VOLUME'] if v > 0])
    except Exception as e:
        logging.warning("Berg CutFill failed (surfaces may not overlap): %s", e)

    sediment_vol = terrain_vol - berg_vol

    with open(os.path.join(final_out_path, "volumes.csv"), "w", newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(["VOL_BERG_DAGSONE_m3", "VEKT_BERG_DAGSONE_kg", "VOL_SEDIMENT_m3", "VOL_SEDIMENT_DIESEL_LITER"])
        writer.writerow([berg_vol, berg_vol*0.7, sediment_vol, sediment_vol*1.98])

    import pandas as pd

    df = pd.read_csv(os.path.join(final_out_path, "volumes.csv"), sep=";")
    df.to_excel(os.path.join(final_out_path, "masseuttak_bb5.xlsx"), sheet_name="Mengder", index=False)

    timings["volume_calculation"] = time.time() - t_vol
    logging.info("Volume calculation completed in %.1f seconds", timings["volume_calculation"])
    logging.info("Volumes: berg=%.1f m3, sediment=%.1f m3, total_terrain=%.1f m3", berg_vol, sediment_vol, terrain_vol)
    print(f"Volumes: berg={berg_vol:.1f} m3, sediment={sediment_vol:.1f} m3")

    # =========================================================================
    # PIPELINE SUMMARY
    # =========================================================================
    total_elapsed = time.time() - pipeline_start
    timings["total"] = total_elapsed

    summary_phases = [
        ("IFC import (model)",     timings.get("ifc_import_model", 0)),
        ("Model rasterization",    timings.get("model_rasterization", 0)),
        ("Tunnel processing",      timings.get("tunnel_processing", 0)),
        ("Terrain conversion",     timings.get("terrain_conversion", 0)),
        ("Berg conversion",        timings.get("berg_conversion", 0)),
    ]
    if NUM_WORKERS > 1:
        summary_phases += [
            ("Grid: Phase A",          timings.get("grid_phase_a", 0)),
            ("Grid: Barrier",          timings.get("grid_barrier", 0)),
            ("Grid: Phase B",          timings.get("grid_phase_b", 0)),
        ]
    else:
        summary_phases += [
            ("Grid: Clip + filter",    timings.get("seq_clip_filter", 0)),
            ("Grid: Berg excavation",  timings.get("seq_berg_excavation", 0)),
            ("Grid: Barrier",          timings.get("grid_barrier", 0)),
            ("Grid: Buffer + merge",   timings.get("seq_buffer_merge", 0)),
            ("Grid: Final excavation", timings.get("seq_final_excavation", 0)),
        ]
    summary_phases += [
        ("Grid: Final merge",      timings.get("grid_final_merge", 0)),
        ("Volume calculation",     timings.get("volume_calculation", 0)),
    ]

    def _fmt_time(secs):
        if secs >= 3600:
            return f"{secs/3600:.1f}h"
        if secs >= 60:
            return f"{secs/60:.1f}m"
        return f"{secs:.1f}s"

    summary_lines = []
    summary_lines.append("=" * 70)
    summary_lines.append("PIPELINE SUMMARY")
    summary_lines.append("=" * 70)
    summary_lines.append(f"  Mode: {mode_str}, {seq_str}, cell_size={CELL_SIZE}")
    summary_lines.append(f"  Grid tiles: {len(grid_extents)}")

    # --- Input inventory ---
    summary_lines.append("-" * 70)
    summary_lines.append("  INPUT FILES")
    summary_lines.append(f"  {'Type':<12} {'Count':>6} {'Total Size':>12}")
    summary_lines.append(f"  {'-'*12} {'-'*6} {'-'*12}")
    for label, file_list in [("Models", model_list), ("Tunnels", tunnel_list), ("Berg", berg_list), ("Terrain", terrain_xml_list)]:
        total_mb = sum(_file_size_mb(f) for f in file_list)
        summary_lines.append(f"  {label:<12} {len(file_list):>6} {total_mb:>10.1f} MB")
    total_files = len(model_list) + len(tunnel_list) + len(berg_list) + len(terrain_xml_list)
    total_size = sum(_file_size_mb(f) for f in model_list + tunnel_list + berg_list + terrain_xml_list)
    summary_lines.append(f"  {'TOTAL':<12} {total_files:>6} {total_size:>10.1f} MB")
    if discipline_counts:
        summary_lines.append(f"  Disciplines: {', '.join(f'{k}={v}' for k,v in sorted(discipline_counts.items()))}")
    if area_counts:
        summary_lines.append(f"  Areas: {', '.join(f'{k}={v}' for k,v in sorted(area_counts.items()))}")

    # --- Multipatch / object statistics ---
    summary_lines.append("-" * 70)
    summary_lines.append("  MULTIPATCH OBJECTS")
    summary_lines.append(f"  {'Dataset':<28} {'FCs':>5} {'Objects':>9} {'Empty':>6} {'BadExt':>7} {'Error':>6}")
    summary_lines.append(f"  {'-'*28} {'-'*5} {'-'*9} {'-'*6} {'-'*7} {'-'*6}")
    grand_fcs = 0
    grand_objs = 0
    for ds_name in ["MERGED_MODEL_RASTER", "MERGED_TUNNEL_RASTER", "MERGED_BERG_RASTER"]:
        st = rasterize_stats.get(ds_name)
        if st:
            grand_fcs += st["valid"]
            grand_objs += st.get("merged_objects", st["valid_objects"])
            summary_lines.append(f"  {ds_name:<28} {st['valid']:>5} {st.get('merged_objects', st['valid_objects']):>9,} "
                                 f"{st['empty']:>6} {st['bad_extent']:>7} {st['error']:>6}")
        else:
            summary_lines.append(f"  {ds_name:<28}     0         0      0       0      0")
    summary_lines.append(f"  {'TOTAL':<28} {grand_fcs:>5} {grand_objs:>9,}")

    summary_lines.append("-" * 70)
    summary_lines.append("  TIMING")
    summary_lines.append(f"  {'Phase':<28} {'Duration':>10} {'% Total':>10}")
    summary_lines.append(f"  {'-'*28} {'-'*10} {'-'*10}")
    accounted = 0
    for name, dur in summary_phases:
        pct = (dur / total_elapsed * 100) if total_elapsed > 0 else 0
        summary_lines.append(f"  {name:<28} {_fmt_time(dur):>10} {pct:>9.1f}%")
        accounted += dur
    overhead = total_elapsed - accounted
    if overhead > 0.5:
        pct = overhead / total_elapsed * 100
        summary_lines.append(f"  {'Other (setup/cleanup)':<28} {_fmt_time(overhead):>10} {pct:>9.1f}%")
    summary_lines.append(f"  {'-'*28} {'-'*10} {'-'*10}")
    summary_lines.append(f"  {'TOTAL':<28} {_fmt_time(total_elapsed):>10} {'100.0%':>10}")
    # --- Volumes ---
    summary_lines.append("-" * 70)
    summary_lines.append("  VOLUMES")
    summary_lines.append(f"  {'Type':<28} {'Volume (m3)':>15} {'Weight/Diesel':>18}")
    summary_lines.append(f"  {'-'*28} {'-'*15} {'-'*18}")
    summary_lines.append(f"  {'Berg (dagsone)':<28} {berg_vol:>15,.1f} {berg_vol*0.7:>15,.1f} kg")
    summary_lines.append(f"  {'Sediment':<28} {sediment_vol:>15,.1f} {sediment_vol*1.98:>15,.1f} L")
    summary_lines.append(f"  {'Terrain (total)':<28} {terrain_vol:>15,.1f}")

    # --- Tile stats ---
    if NUM_WORKERS > 1:
        summary_lines.append("-" * 70)
        summary_lines.append("  GRID TILE STATISTICS")
        summary_lines.append(f"  Phase A tiles: n={len(tile_times_a)}, min={min(tile_times_a):.1f}s, max={max(tile_times_a):.1f}s, avg={sum(tile_times_a)/len(tile_times_a):.1f}s, sum={sum(tile_times_a):.1f}s")
        summary_lines.append(f"  Phase B tiles: n={len(tile_times_b)}, min={min(tile_times_b):.1f}s, max={max(tile_times_b):.1f}s, avg={sum(tile_times_b)/len(tile_times_b):.1f}s, sum={sum(tile_times_b):.1f}s")
        speedup_a = sum(tile_times_a) / timings.get("grid_phase_a", 1)
        speedup_b = sum(tile_times_b) / timings.get("grid_phase_b", 1)
        summary_lines.append(f"  Effective speedup: Phase A {speedup_a:.1f}x, Phase B {speedup_b:.1f}x (of {NUM_WORKERS} workers)")
    summary_lines.append("=" * 70)

    summary_text = "\n".join(summary_lines)
    logging.info("\n%s", summary_text)
    print(summary_text)

    print("Cleaning up...")
    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")
    arcpy.management.Delete(scratch_folder)
    arcpy.management.Delete(scratch_gdb)
    safe_delete('scratch')
    logging.info("Script end")
