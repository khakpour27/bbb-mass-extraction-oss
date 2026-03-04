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
def list_files_by_ext(path:str, ext:str) -> list[str]:
    pattern = os.path.join(path, f"*{ext}")
    files = glob.glob(pattern)
    return files

def list_model_ifcs(ifc_list:list) -> list[str]:
    substrings = ["fm_Veg", "fm_VA", "fm_FVG", "fm_Ele", "fm_Spo_Sporsystem"]
    models = [f for f in ifc_list if any(sub in f for sub in substrings) and "_alt" not in f]
    return models

def list_tunnel_ifcs(ifc_list:list) -> list[str]:
    tunnels = [f for f in ifc_list if f.endswith("sprengning.ifc") and "fm_Geo" in f]
    return tunnels

def list_berg_ifcs(berg_path:str) -> list[str]:
    berg_ifcs = [os.path.join(berg_path, f) for f in os.listdir(berg_path) if "Antatt-bergoverflate" in f and f.endswith(".ifc")]
    return berg_ifcs

def list_land_xmls(xml_folder:str) -> list[str]:
    land_xmls = [os.path.join(xml_folder, f) for f in os.listdir(xml_folder) if f.endswith(".xml") and "Terrengoverflate" in f]
    return land_xmls

def clean_file_name(filename:str) -> str:
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
    result =  arcpy.conversion.BIMFileToGeodatabase(
        ifc_path,
        temp_gdb,
        name,
        spatial_ref,
        include_floorplan='EXCLUDE_FLOORPLAN'
    )
    return result.getOutput(0)#should be a filepath for multiprocessing serialization. MP doesn't play well with arcpy objects

def import_ifcs_as_multipatch(ifc_path_list, scratch_gdb, spatial_ref):
    print(f"Checking cache for {len(ifc_path_list)} IFC files...")
    manifest = _load_cache_manifest()

    to_convert = []
    cached_results = {}
    for ifc_path in ifc_path_list:
        mtime = str(os.path.getmtime(ifc_path))
        cached = manifest.get(ifc_path)
        # Check GDB parent folder exists (feature dataset path inside GDB is virtual, not on filesystem)
        cached_output = cached.get("output", "") if cached else ""
        cached_gdb = cached_output.split(".gdb")[0] + ".gdb" if ".gdb" in cached_output else ""
        if cached and cached.get("mtime") == mtime and cached_gdb and os.path.exists(cached_gdb):
            cached_results[ifc_path] = cached["output"]
        else:
            to_convert.append(ifc_path)

    print(f"  {len(cached_results)} cached, {len(to_convert)} to convert...")

    # Convert only changed/new IFCs in parallel
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

    # Return results in original order
    bim_files = []
    for ifc_path in ifc_path_list:
        if ifc_path in cached_results:
            bim_files.append(cached_results[ifc_path])
        else:
            bim_files.append(new_results[ifc_path])
    return bim_files
    
def merge_and_rasterize_multipatches(multipatches:list[arcpy.Result], cell_size=0.1, outname: str = "MERGED_MODEL_RASTER") -> arcpy.Result:
    """merges multipatch input in memory and rasterizes the result"""
    multipatch_fts = []
    print(f"Converting {len(multipatches)} multipatches to merged raster...")
    for f in multipatches:
        try:
            desc = arcpy.Describe(f)
            for child in desc.children:
                try:
                    if child.shapeType == "MultiPatch":
                        # Skip empty feature classes and validate readability
                        count = int(arcpy.management.GetCount(child.catalogPath).getOutput(0))
                        if count > 0:
                            # Filter out feature classes with bad coordinates (e.g. unshifted local coords)
                            fc_ext = arcpy.Describe(child.catalogPath).extent
                            if (not math.isnan(fc_ext.XMin) and
                                fc_ext.XMin > 200000 and fc_ext.XMax < 400000 and
                                fc_ext.YMin > 6600000 and fc_ext.YMax < 6800000):
                                multipatch_fts.append(child.catalogPath)
                            else:
                                logging.warning("Skipping feature class with bad extent: %s (%.0f,%.0f - %.0f,%.0f)",
                                    child.name, fc_ext.XMin, fc_ext.YMin, fc_ext.XMax, fc_ext.YMax)
                        else:
                            logging.debug("Skipping empty feature class: %s", child.catalogPath)
                except Exception as e:
                    logging.warning("Skipping unreadable feature class in %s: %s", f, e)
        except Exception as e:
            logging.warning("Skipping unreadable GDB %s: %s", f, e)

    logging.info("Merging %d non-empty multipatch feature classes for %s", len(multipatch_fts), outname)
    if len(multipatch_fts) == 0:
        logging.warning("No valid multipatch features found for %s, skipping", outname)
        return None
    merged_mps = arcpy.management.Merge(multipatch_fts, f"memory/{outname}")
    arcpy.management.Merge(multipatch_fts, os.path.join(output_gdb, outname)) #saving model bases to output for publishing to AGOL potentially
    outpath = os.path.join(scratch_folder, f"{outname}.tif")

    return arcpy.conversion.MultipatchToRaster(merged_mps,
                                   outpath,
                                   cell_size,
                                   "MINIMUM_HEIGHT")

def convert_landxml_to_tin(landxml_path:str, tin_output_folder:str, basename:str) -> str:
    output_path = os.path.join(scratch_folder, tin_output_folder)
    
    if not os.path.exists(output_path):
        os.mkdir(output_path)
        
    output_tin_folder = arcpy.ddd.LandXMLToTin(landxml_path, output_path, basename)
    return output_tin_folder.getOutput(0)

def tins_to_merged_raster(tin_folder_path:str, cell_size=0.1) -> str:
    rasters = []
    tin_desc = arcpy.Describe(tin_folder_path)
    tin_list = [child.catalogPath for child in tin_desc.children]
    data_type = "FLOAT"
    z_factor = "1"
    sampling = "CELLSIZE"
    method = "LINEAR"
    for tin in tin_list:
        tin_name = os.path.basename(tin)
        raster_out_path = os.path.join(tin_folder_path, f"{tin_name}.tif" )
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
### FUNCTIONS FOR RASTER PROCESSING 
#############################################################################################################
def merge_buffer_with_berg(berg_flate, berg_excavation, berg_buffer, cell_size, out_name) -> str:
    
    berg_flate = arcpy.Raster(berg_flate)
    berg_excavation = arcpy.Raster(berg_excavation)
    berg_buffer = arcpy.Raster(berg_buffer)

    rows = min(berg_excavation.height, berg_flate.height, berg_buffer.height)
    cols = min(berg_excavation.width, berg_flate.width, berg_buffer.width)

    np_berg_flate = arcpy.RasterToNumPyArray(berg_flate, nodata_to_value=np.nan)
    np_berg_exc = arcpy.RasterToNumPyArray(berg_excavation, nodata_to_value=np.nan)
    np_berg_buff = arcpy.RasterToNumPyArray(berg_buffer, nodata_to_value=9999)
    out_raster = np.full((rows,cols), np.nan)

    for r in range(rows):
        for c in range(cols):
            #if not np.isnan(np_berg_buff[r,c]) and np_berg_buff[r,c] == 0 and not np.isnan(np_berg_flate[r,c]):
            if np_berg_buff[r,c] == 0 and not np.isnan(np_berg_flate[r,c]):
                out_raster[r,c] = np_berg_flate[r,c]

            if not np.isnan(np_berg_exc[r,c]):
                out_raster[r,c] = np_berg_exc[r,c]

    lower_left = arcpy.Point(berg_excavation.extent.XMin, berg_excavation.extent.YMin)
    out_raster = arcpy.NumPyArrayToRaster(out_raster, lower_left, cell_size, cell_size, value_to_nodata=np.nan)
        
    out_raster.save(os.path.join(scratch_folder, out_name))
    return out_raster.catalogPath


def filter_model_under_berg(model_raster, berg_raster, cell_size, out_name) -> str:
    model_raster = arcpy.Raster(model_raster)
    berg_raster = arcpy.Raster(berg_raster)

    rows = min(model_raster.height, berg_raster.height)
    cols = min(model_raster.width, berg_raster.width)

    np_model = arcpy.RasterToNumPyArray(model_raster, nodata_to_value=np.nan)
    np_berg = arcpy.RasterToNumPyArray(berg_raster, nodata_to_value=np.nan)
    output_raster = np.full((rows,cols), np.nan)

    #print("Filtering model cells under berg elevation")
    for r in range(rows):
        for c in range(cols):
            if not np.isnan(np_berg[r,c]) and not np.isnan(np_model[r,c]) and np_model[r,c] <= np_berg[r,c]: 
                output_raster[r,c] = np_model[r,c]

    lower_left = arcpy.Point(model_raster.extent.XMin, model_raster.extent.YMin)
    out_raster = arcpy.NumPyArrayToRaster(output_raster, lower_left, cell_size, cell_size, value_to_nodata=np.nan)

    #print("Filtering complete.")
    out_path = os.path.join(scratch_folder, out_name)
    out_raster.save(out_path)
    return out_raster.catalogPath

def generate_berg_excavation(filtered_berg_model_raster, berg_raster, cell_size, out_name) -> str:
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
                queue.append((r,c)) #queuing all existing model cells for processing

    in_queue = np.zeros(output_raster.shape, dtype=bool) #bool-array used to mark cells that are awaiting processing
    
    #print("Processing berg raster cells for excavation model...")
    while queue: #main processing loop. Runs as long as there are cells to process.
        r,c = queue.popleft() #take the first available cell from the queue
        in_queue[r,c] = False 
        current_elev = output_raster[r,c] #elev of the cell to be processed
        berg_elev = np_berg[r,c]

        for dr, dc, dist in neighbors: #checking all the neighboring cell values
            nr, nc = r + dr, c + dc #neighbor row, neighbor cell = current cell plus directions 

            if 0 <= nr < rows and 0 <= nc < cols: #if cell in bounds
                neighbor_elev = output_raster[nr,nc]
                n_berg_elev = np_berg[nr, nc]

                berg_rise = dist * 10.0 #berg slope 
                tent_berg_elev = current_elev + berg_rise

                # case: berg - if neighbor is nan and berg_elev at neighbor not nan 
                # or tent berg elev lower than existing excavation level and berg level above tentativ excavation level at neighbor
                if (not np.isnan(n_berg_elev) and (np.isnan(neighbor_elev) or tent_berg_elev < neighbor_elev)) and n_berg_elev > tent_berg_elev:
                    output_raster [nr,nc] = tent_berg_elev
                    if not in_queue[nr,nc]:
                        queue.append((nr,nc)) #adding this cell to the queue to get its neighbors
                        in_queue[nr, nc] = True
                    

    lower_left = arcpy.Point(filtered_berg.extent.XMin, filtered_berg.extent.YMin)
    output = arcpy.NumPyArrayToRaster(output_raster, lower_left, cell_size, cell_size, value_to_nodata=np.nan) 
    out_path = os.path.join(scratch_folder, out_name)
    output.save(out_path)
    #print("Berg excavation processing complete") 
    return output.catalogPath

def generate_final_excavation(berg_excavation_raster, merged_model_raster, terrain_raster, cell_size, outname):
    berg_exc = arcpy.Raster(berg_excavation_raster)
    model_merge = arcpy.Raster(merged_model_raster)
    terrain = arcpy.Raster(terrain_raster)

    rows = min(model_merge.height, berg_exc.height, terrain.height)
    cols = min(model_merge.width, berg_exc.width, terrain.width)

    np_berg = arcpy.RasterToNumPyArray(berg_exc, nodata_to_value=np.nan)
    #np_model = arcpy.RasterToNumPyArray(model_merge, nodata_to_value=np.nan)
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
    #print("Processing raster cells for final excavation...")

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

                if (np.isnan(neighbor_elev) or tentativ_elev < neighbor_elev) and np.isnan(neighbor_berg) and tentativ_elev < terrain_elev: #neighbor cell empty, or new height lower than existing and neighbor cell not already in berg excv model
                    out_models[nr,nc] = tentativ_elev
                    if not in_queue[nr,nc]:
                        queue.append((nr,nc))
                        in_queue[nr,nc] = True

    lower_left = arcpy.Point(model_merge.extent.XMin, model_merge.extent.YMin)
    output = arcpy.NumPyArrayToRaster(out_models, lower_left, cell_size, cell_size, value_to_nodata=np.nan)
    out_path = os.path.join(scratch_folder, outname)
    output.save(out_path)
    #print("Processing complete.")
    return output.catalogPath
    
def merge_berg_with_existing_models(berg_excavation_raster, model_raster, cell_size, outname):
    berg_model = arcpy.Raster(berg_excavation_raster)
    models = arcpy.Raster(model_raster)

    rows = min(models.height, berg_model.height)
    cols = min(models.width, berg_model.width)

    np_berg_m = arcpy.RasterToNumPyArray(berg_model, nodata_to_value=np.nan)
    np_models = arcpy.RasterToNumPyArray(models, nodata_to_value=np.nan)
    out_models = np.copy(np_models)
    #print("Merging models with berg excavation")
    for r in range(rows):
        for c in range(cols):
            model_elev = np_models[r,c]
            berg_elev = np_berg_m[r,c]

            if np.isnan(model_elev) and not np.isnan(berg_elev): #empty model cell, filled berg cell
                out_models[r,c] = berg_elev

            if not np.isnan(model_elev) and not np.isnan(berg_elev) and berg_elev < model_elev:
                out_models[r,c] = berg_elev

    lower_left = arcpy.Point(models.extent.XMin, models.extent.YMin)
    output = arcpy.NumPyArrayToRaster(out_models, lower_left, cell_size, cell_size, value_to_nodata=np.nan)
    out_name = os.path.join(scratch_folder, outname)
    output.save(out_name)
    #print("Merge complete.")
    return output.catalogPath

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
                os.chmod(path, stat.S_IWRITE)  # Remove read-only flag
                os.remove(path)
            print(f"Deleted: {path}")
            break
        except PermissionError as e:
            print(f"PermissionError on {path}, retrying...")
            time.sleep(1)
        except Exception as e:
            print(f"Error: {e}")
            break

def cleanup_results(output_folder:str) -> None:
    results = [os.path.join(output_folder, f) for f in os.listdir(output_folder)]
    rm = results[0]
    safe_delete(rm)
###############################################################################################################
### SETUP AND ENTRY POINT
###############################################################################################################
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Test mode: process only 10 IFC files")
    args = parser.parse_args()
    TEST_MODE = args.test

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
    logging.info("=== Script start (%s) ===", mode_str)
    print(f"=== mass_calc.py ({mode_str}) ===")

    MODEL_FOLDER_PATH = r"C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Discipline models"
    TERRAIN_PATH = r"C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Existing condition models (CORAV)\Terrengflater"
    BERG_PATH = r"C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Existing condition models (CORAV)"
    GRID_PATH = r"SCRIPT_HELP_FILES\AOI.gdb\INDEX_GRID_200_overlap"
    MUNKEBOTN_MASK = r"SCRIPT_HELP_FILES\munkebotn_mask.tif"
    CELL_SIZE = 1.0 if TEST_MODE else 0.2  # 1m for test, 20cm for production


    #################################################################################################################################
    ### PROCESSSING STEPS - IFC, LANDXML INPUT to RASTER
    #################################################################################################################################

    ifc_list = list_files_by_ext(MODEL_FOLDER_PATH, "*.ifc")
    model_list = list_model_ifcs(ifc_list)
    tunnel_list = list_tunnel_ifcs(ifc_list)
    berg_list = list_berg_ifcs(BERG_PATH)
    terrain_xml_list = list_land_xmls(TERRAIN_PATH)

    if TEST_MODE:
        # Pick models from a single area code to keep geographic extent small
        test_prefix = "E03_011"
        model_list = [m for m in model_list if os.path.basename(m).startswith(test_prefix)]
        tunnel_list = [t for t in tunnel_list if os.path.basename(t).startswith(test_prefix)]
        if not tunnel_list:
            tunnel_list = tunnel_list[:1]  # need at least 1 for pipeline
        berg_list = berg_list[:1]
        terrain_xml_list = terrain_xml_list[:1]
        logging.info("TEST MODE: area prefix=%s, %d models, %d tunnels, %d berg, %d terrain",
                     test_prefix, len(model_list), len(tunnel_list), len(berg_list), len(terrain_xml_list))

    logging.info("Input counts: %d models, %d tunnels, %d berg, %d terrain",
                 len(model_list), len(tunnel_list), len(berg_list), len(terrain_xml_list))
    print(f"Input: {len(model_list)} models, {len(tunnel_list)} tunnels, {len(berg_list)} berg, {len(terrain_xml_list)} terrain")

    for model in model_list:
        logging.info("Input model: %s", os.path.basename(model))

    for tunnel in tunnel_list:
        logging.info("Input tunnel model: %s", os.path.basename(tunnel))

    for berg in berg_list:
        logging.info("Input berg model: %s", os.path.basename(berg))

    for terr in terrain_xml_list:
        logging.info("Input terrain model: %s", os.path.basename(terr))

    logging.info("Starting IFC import (model)...")
    t0 = time.time()
    bim_mps = import_ifcs_as_multipatch(model_list, scratch_gdb, arcpy.env.outputCoordinateSystem)
    logging.info("IFC import (model) completed in %.1f seconds", time.time() - t0)
    print(f"IFC import done in {time.time() - t0:.1f}s")
    print("Sinking Sporsystem 900mm...")
    # Find all multipatches in sporsystem feature datasets and adjust all elevations downward 900mm.
     
    spor_fds = [f for f in bim_mps if "Sporsystem" in f]
    for spor_mod in spor_fds:
        desc = arcpy.Describe(spor_mod)
        for child in desc.children:
            if child.shapeType == "MultiPatch":
                arcpy.management.Adjust3DZ(child.catalogPath, "NO_REVERSE", -0.9)

    print("Merging model multipatches and converting to raster...")
    logging.info("Merging and rasterizing model multipatches...")
    t0 = time.time()
    full_model_raster = merge_and_rasterize_multipatches(bim_mps, CELL_SIZE, "MERGED_MODEL_RASTER").getOutput(0)
    logging.info("Model rasterization completed in %.1f seconds", time.time() - t0)
    print(f"IFC models to raster complete ({time.time() - t0:.1f}s)")

    arcpy.env.snapRaster = full_model_raster #snapping all following raster processing to model raster grid

    print("Creating tunnel mask...")
    logging.info("Starting IFC import (tunnel)...")
    t0 = time.time()
    tunnel_mps = import_ifcs_as_multipatch(tunnel_list, scratch_gdb, arcpy.env.outputCoordinateSystem)
    logging.info("IFC import (tunnel) completed in %.1f seconds", time.time() - t0)
    tunnel_result = merge_and_rasterize_multipatches(tunnel_mps, CELL_SIZE, "MERGED_TUNNEL_RASTER")

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
    logging.info("Terrain conversion completed in %.1f seconds", time.time() - t0)
    print(f"Terrain layers converted to raster ({time.time() - t0:.1f}s)")

    logging.info("Starting IFC import (berg)...")
    t0 = time.time()
    berg_mps = import_ifcs_as_multipatch(berg_list, scratch_gdb, arcpy.env.outputCoordinateSystem)
    logging.info("IFC import (berg) completed in %.1f seconds", time.time() - t0)
    full_berg_raster = merge_and_rasterize_multipatches(berg_mps, CELL_SIZE, "MERGED_BERG_RASTER").getOutput(0)
    berg_output = arcpy.Raster(full_berg_raster)
    berg_output.save(os.path.join(final_out_path, "BERG_MERGED_RASTER.tif"))
    print(f"Berg layers converted to raster ({time.time() - t0:.1f}s)")

    #################################################################################################################################
    ### PROCESSSING STEPS - RASTER
    #################################################################################################################################


    grid_extents = []
    clipped_model_tiles = []
    clipped_berg_tiles = []
    clipped_filtered_tiles = []
    clipped_berg_exc_tiles = []
    clipped_buffered_tiles = []
    clipped_buff_berg_tiles = []
    model_berg_exc_tiles = []
    final_model_tiles = []
    terrain_tiles = []
    final_exc_tiles = []

    model_domain_geom = None

    with arcpy.da.SearchCursor(model_raster_domain, ["SHAPE@"]) as cursor:
        for row in cursor:
            model_domain_geom = row[0]

    with arcpy.da.SearchCursor(GRID_PATH, ["GRIDNR", "SHAPE@"], spatial_filter=model_domain_geom, spatial_relationship="INTERSECTS") as cursor: #only reading out the grid cells with model data 
        for row in cursor:
            ext = row[1].extent
            xmin = str(ext.XMin)
            ymin = str(ext.YMin)
            xmax = str(ext.XMax)
            ymax = str(ext.YMax)
            bbox = " ".join([xmin, ymin, xmax, ymax])
            grid_extents.append((row[0],bbox, ext))

    logging.info("Grid processing: %d tiles to process", len(grid_extents))
    print(f"Grid processing: {len(grid_extents)} tiles")
    t0 = time.time()

    #Clipping input rasters
    print("Clipping input rasters...")
    for cell in grid_extents:
        bbox = cell[1]
        id = cell[0]
        cell_ext = cell[2]
        out_path_model = os.path.join(scratch_folder, f"model_clip_{id}.tif")
        out_path_berg = os.path.join(scratch_folder, f"berg_clip_{id}.tif" )
        clipped_model_tiles.append(out_path_model)
        clipped_berg_tiles.append(out_path_berg)

        #print(f"Clipping cell ID: {id}")
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

        #checking weird raster behavior where there is no clip run and the complete raster randomly comes out in one of the tiles
        temp_berg = arcpy.Raster(out_path_berg)
        temp_model = arcpy.Raster(out_path_model)

        threshold_pixel_count = (250 / CELL_SIZE)**2 #250 meters grid cell
        #if the resulting clipped raster is too big, make a NoData raster for the grid extent and overwrite the bad output from earlier.
        if temp_berg.width * temp_berg.height > threshold_pixel_count:
            const_berg = arcpy.sa.CreateConstantRaster(1, "FLOAT", CELL_SIZE, cell_ext)
            null_berg = arcpy.sa.SetNull(const_berg, const_berg, "VALUE > 0")
            null_berg.save(out_path_berg)

        if temp_model.width * temp_model.height > threshold_pixel_count:
            const_model = arcpy.sa.CreateConstantRaster(1, "FLOAT", CELL_SIZE, cell_ext)
            null_model = arcpy.sa.SetNull(const_model, const_model, "VALUE > 0")
            null_model.save(out_path_model)

    # Filtering model under berg 

    for model, berg in zip(clipped_model_tiles, clipped_berg_tiles):
        suffix = os.path.basename(model).split("_")[-1] #tile nr.tif
        #print(f"Filtering tile {suffix}")
        filtered_tile = filter_model_under_berg(model, berg, CELL_SIZE, f"filtered_tile_{suffix}")
        clipped_filtered_tiles.append(filtered_tile)

    #Might not need this? Clip -> merge -> clip seems wrong. 

    # print("Merging results...")
    # complete_filter = arcpy.ia.Merge(filter_merge, "MIN")
    # filter_out = os.path.join(scratch_gdb, "model_under_berg")
    # complete_filter.save(filter_out)
    # print("Merge complete.")

    # Clipping filtered model tiles and berg excavation

    print("Generating berg excavation")
    for model, berg in zip(clipped_filtered_tiles, clipped_berg_tiles):
        suffix = os.path.basename(model).split("_")[-1] #tile nr
        #print(f"Generating berg excavation for tile {suffix}")
        berg_exc_tile = generate_berg_excavation(model, berg, CELL_SIZE, f"berg_exc_tile_{suffix}" )
        clipped_berg_exc_tiles.append(berg_exc_tile)

    print("Merging intermediate results...")
    complete_berg_exc = arcpy.ia.Merge(clipped_berg_exc_tiles, "MIN")
    berg_exc_out = os.path.join(scratch_folder, "berg_exc_complete.tif")
    complete_berg_exc.save(berg_exc_out)
    complete_berg_exc_path = complete_berg_exc.catalogPath
    print("Merge complete.")

    print("Buffering result...")
    #berg_exc_buffered = arcpy.gp.DistanceAccumulation_sa(complete_berg_exc_path, os.path.join(scratch_folder, "distance_raster.tif")).getOutput(0)
    #Bug, or different method of handling this call after ArcGIS Pro 3.6. Below is using alternativ raster processing calls to achieve same result.
    is_null = isNull = arcpy.ia.Apply(complete_berg_exc_path, "IsNull")
    berg_exc_buffered = arcpy.ia.Apply(is_null,
                                       os.path.join(os.path.dirname(__file__), "SCRIPT_HELP_FILES", "Expand.rft.xml"),
                                       {"number_cells": int(1/CELL_SIZE), "zone_values": "0"}
                                        )
    
    berg_exc_buffered.save(os.path.join(scratch_folder, "distance_raster.tif"))
    berg_exc_buffered = os.path.join(scratch_folder, "distance_raster.tif")

    print("Clipping buffered result")
    for cell in grid_extents:
        bbox = cell[1]
        id = cell[0]
        out_path_buff = os.path.join(scratch_folder, f"buffer_clip_{id}.tif")
        clipped_buffered_tiles.append(out_path_buff)

        arcpy.management.Clip(
            in_raster=berg_exc_buffered,
            rectangle=bbox,
            out_raster=out_path_buff,
            nodata_value="3,4e+38",
            maintain_clipping_extent="NO_MAINTAIN_EXTENT"
        )

    print("Adding buffer to berg excavation results...")
    for berg, exc, buff in zip(clipped_berg_tiles, clipped_berg_exc_tiles, clipped_buffered_tiles):
        suffix = os.path.basename(berg).split("_")[-1] #tile nr
        #print(f"Generating buffered berg excavation for tile {suffix}")
        buff_exc_tile = merge_buffer_with_berg(berg, exc, buff, CELL_SIZE, f"buff_exc_tile_{suffix}" )
        clipped_buff_berg_tiles.append(buff_exc_tile)

    print("Merging with existing model raster...")
    for berg_m, model in zip(clipped_buff_berg_tiles, clipped_model_tiles):
        suffix = os.path.basename(berg_m).split("_")[-1]
        final_model_tile = merge_berg_with_existing_models(berg_m, model, CELL_SIZE, f"final_model_tile_{suffix}")
        final_model_tiles.append(final_model_tile)

    print("generating final excavation...")
    print("clipping terrain...")

    for cell in grid_extents:
        bbox = cell[1]
        id = cell[0]
        out_path = os.path.join(scratch_folder, f"terrain_clip_{id}.tif")
        terrain_tiles.append(out_path)

        arcpy.management.Clip(
            in_raster=arcpy.Raster(full_terrain_raster),
            rectangle=bbox,
            out_raster=out_path,
            nodata_value="3,4e+38",
            maintain_clipping_extent="NO_MAINTAIN_EXTENT"
        )


    for berg, mod, terr in zip(clipped_buff_berg_tiles, final_model_tiles, terrain_tiles):
        suffix = os.path.basename(mod).split("_")[-1]
        final_tile = generate_final_excavation(berg, mod, terr, CELL_SIZE, f"final_tile_{suffix}")
        final_model_tiles.append(final_tile)

    final_result = arcpy.ia.Merge(final_model_tiles, "MIN")
    final_result.save(os.path.join(final_out_path, "FINAL_RESULT_RASTER.tif"))
    logging.info("Grid processing completed in %.1f seconds", time.time() - t0)
    print(f"Final result complete ({time.time() - t0:.1f}s)")

    print("Calculating volumes...")
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

    logging.info("Volumes: berg=%.1f m3, sediment=%.1f m3, total_terrain=%.1f m3", berg_vol, sediment_vol, terrain_vol)
    print(f"Volumes: berg={berg_vol:.1f} m3, sediment={sediment_vol:.1f} m3")
    print("Cleaning up...")
    arcpy.CheckInExtension("3D")
    arcpy.CheckInExtension("Spatial")
    arcpy.management.Delete(scratch_folder)
    arcpy.management.Delete(scratch_gdb)
    safe_delete('scratch')
    logging.info("Script end")