import arcpy
import math
import glob
import os
import stat
import shutil
from collections import deque
import numpy as np
from datetime import datetime

###############################################################################################################
### SETUP
###############################################################################################################
if os.path.exists("temp/scratch"): #deleting results from previous runs
    os.chmod("temp/scratch", stat.S_IWRITE)
    shutil.rmtree("temp/scratch")

if os.path.exists("temp/scratch.gdb"):
    os.chmod("temp/scratch.gdb", stat.S_IWRITE)
    shutil.rmtree("temp/scratch.gdb")
    
run_time = datetime.now().strftime("%Y_%m_%d_%H_%M")
arcpy.env.scratchWorkspace = "temp"
output_folder = "output"
arcpy.management.CreateFileGDB(output_folder, f"PME_results_{run_time}")
scratch_folder = arcpy.env.scratchFolder
scratch_gdb = arcpy.env.scratchGDB
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(25832)
arcpy.env.overwriteOutput = True

MODEL_FOLDER_PATH = r"C:\Users\scbm\OneDrive - COWI\Desktop\fagmodeller_grøftefag"
TERRAIN_PATH = r"C:\Users\scbm\OneDrive - COWI\Desktop\terrengoverflate\E03_000_gm_GeMa_Terrengoverflate_DS1_1.xml"
BERG_PATH = r"C:\Users\scbm\OneDrive - COWI\Desktop\bergoverflate\E03_000_gm_Geo_Antatt-bergoverflate.xml"
GRID_PATH = r"C:\Users\scbm\OneDrive - COWI\Projects\A240636 - Bybanen\A240636 - Bybanen_RCI_TESTING\PME.gdb\index_grid_start"
CONTAMINATED_SOIL_PATH = ""
CELL_SIZE = 0.1 #10 cm

#############################################################################################################
### FUNCTIONS FOR RASTER CREATION
#############################################################################################################
def list_files_by_ext(path:str, ext:str) -> list[str]:
    pattern = os.path.join(path, f"*{ext}")
    files = glob.glob(pattern)
    return files

def clean_file_name(filename:str) -> str:
    """cleans names so that the requirements for feature classes in gdb are satisfied"""
    name = filename.replace(".ifc", "")
    if name[0] in ["_", "0","1","2","3","4","5","6","7","8","9"]:
        name = "x_"+name
        
    illegals = ["-", ".", "(", ")", "[", "]", ":", " "]
    return "".join([c if c not in illegals else "_" for c in name])
    
def import_ifcs_as_multipatch(ifc_path_list) -> list:
    """imports a list of IFC paths as multipatches and returns the GDB file paths to result"""
    bim_files = []
    print(f"Converting {len(ifc_path_list)} IFC files to multipatch...")
    for path in ifc_path_list:
        name = clean_file_name(os.path.basename(path).replace(".ifc", ""))
        bim_files.append(arcpy.conversion.BIMFileToGeodatabase(path, 
                                      scratch_gdb, 
                                      name, 
                                      arcpy.env.outputCoordinateSystem,
                                     include_floorplan='EXCLUDE_FLOORPLAN'))

    return bim_files
    
def merge_and_rasterize_multipatches(multipatches:list[arcpy.Result], cell_size=0.1) -> arcpy.Result:
    """merges multipatch input in memory and rasterizes the result"""
    multipatch_fts = []
    print(f"Converting {len(multipatches)} multipatches to merged raster...")
    for f in multipatches:
        desc = arcpy.Describe(f)
        for child in desc.children:
            if child.shapeType == "MultiPatch":
                multipatch_fts.append(child.catalogPath)

    merged_mps = arcpy.management.Merge(multipatch_fts, "memory/merged_mps")
    outpath = os.path.join(scratch_folder, "MERGED_MODEL_RASTER.tif")
    
    return arcpy.conversion.MultipatchToRaster(merged_mps, 
                                   outpath,
                                   cell_size,
                                   "MINIMUM_HEIGHT")


def convert_landxml_to_tin(landxml_path:str, basename:str) -> str:
    output_path = os.path.join(scratch_folder, basename)
    os.makedirs(output_path, exist_ok=True)
    output_tin_folder = arcpy.ddd.LandXMLToTin(landxml_path, output_path, basename)
    return output_tin_folder.getOutput(0)

def tins_to_merged_raster(tin_folder_path:str, cell_size=0.1) -> list:
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

    rows = berg_excavation.height
    cols = berg_excavation.width

    np_berg_flate = arcpy.RasterToNumPyArray(berg_flate, nodata_to_value=np.nan)
    np_berg_exc = arcpy.RasterToNumPyArray(berg_excavation, nodata_to_value=np.nan)
    np_berg_buff = arcpy.RasterToNumPyArray(berg_buffer, nodata_to_value=np.nan)
    out_raster = np.full((rows,cols), np.nan)

    for r in range(rows):
        for c in range(cols):
            if not np.isnan(np_berg_buff[r,c]) and 0 < np_berg_buff[r,c] <= 1 and not np.isnan(np_berg_flate[r,c]):
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

    print("Filtering model cells under berg elevation")
    for r in range(rows):
        for c in range(cols):
            if not np.isnan(np_berg[r,c]) and not np.isnan(np_model[r,c]) and np_model[r,c] <= np_berg[r,c]: 
                output_raster[r,c] = np_model[r,c]

    lower_left = arcpy.Point(model_raster.extent.XMin, model_raster.extent.YMin)
    out_raster = arcpy.NumPyArrayToRaster(output_raster, lower_left, cell_size, cell_size, value_to_nodata=np.nan)

    print("Filtering complete.")
    out_path = os.path.join(scratch_folder, out_name)
    out_raster.save(out_path)
    return out_raster.catalogPath

def generate_berg_excavation(filtered_berg_model_raster, berg_raster, cell_size, out_name) -> str:
    filtered_berg = arcpy.Raster(filtered_berg_model_raster)
    berg = arcpy.Raster(berg_raster)

    rows = filtered_berg.height
    cols = filtered_berg.width

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
    
    print("Processing berg raster cells for excavation model...")
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
    print("Berg excavation processing complete") 
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
    print("Processing raster cells for final excavation...")

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
    print("Processing complete.")
    return output.catalogPath
    
def merge_berg_with_existing_models(berg_excavation_raster, model_raster, cell_size, outname):
    berg_model = arcpy.Raster(berg_excavation_raster)
    models = arcpy.Raster(model_raster)

    rows = min(models.height, berg_model.height)
    cols = min(models.width, berg_model.width)

    np_berg_m = arcpy.RasterToNumPyArray(berg_model, nodata_to_value=np.nan)
    np_models = arcpy.RasterToNumPyArray(models, nodata_to_value=np.nan)
    out_models = np.copy(np_models)
    print("Merging models with berg excavation")
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
    print("Merge complete.")
    return output.catalogPath

#################################################################################################################################
### PROCESSSING STEPS - IFC, LANDXML INPUT to RASTER
#################################################################################################################################

ifc_list = list_files_by_ext(MODEL_FOLDER_PATH, "*.ifc")

print("Importing as multipatch...")

bim_mps = import_ifcs_as_multipatch(ifc_list)

print("Merging multipatches and converting to raster...")

full_model_raster = merge_and_rasterize_multipatches(bim_mps).getOutput(0)

print("IFC to raster complete.")

arcpy.env.snapRaster = full_model_raster #snapping all following raster processing to model raster grid

print("Converting terrain layers to raster...")
berg_tin = convert_landxml_to_tin(BERG_PATH, "berg")
full_berg_raster = tins_to_merged_raster(berg_tin, CELL_SIZE)

terrain_tin = convert_landxml_to_tin(TERRAIN_PATH, "terrain")
full_terrain_raster = tins_to_merged_raster(terrain_tin, CELL_SIZE)
print("Terrain layers successfully converted to raster.")

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



with arcpy.da.SearchCursor(GRID_PATH, ["GRIDNR", "SHAPE@"]) as cursor:
    for row in cursor:
        ext = row[1].extent
        xmin = str(ext.XMin)
        ymin = str(ext.YMin)
        xmax = str(ext.XMax)
        ymax = str(ext.YMax)
        bbox = " ".join([xmin, ymin, xmax, ymax])
        grid_extents.append((row[0],bbox))

#Clipping input rasters 

for cell in grid_extents:
    bbox = cell[1]
    id = cell[0]
    out_path_model = os.path.join(scratch_folder, f"model_clip_{id}.tif")
    out_path_berg = os.path.join(scratch_folder, f"berg_clip_{id}.tif" )
    clipped_model_tiles.append(out_path_model)
    clipped_berg_tiles.append(out_path_berg)

    print(f"Clipping cell ID: {id}")
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

# Filtering model under berg 

for model, berg in zip(clipped_model_tiles, clipped_berg_tiles):
    suffix = os.path.basename(model).split("_")[-1] #tile nr.tif
    print(f"Filtering tile {suffix}")
    filtered_tile = filter_model_under_berg(model, berg, 0.1, f"filtered_tile_{suffix}")
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
    print(f"Generating berg excavation for tile {suffix}")
    berg_exc_tile = generate_berg_excavation(model, berg, CELL_SIZE, f"berg_exc_tile_{suffix}" )
    clipped_berg_exc_tiles.append(berg_exc_tile)

print("Merging intermediate results...")
complete_berg_exc = arcpy.ia.Merge(clipped_berg_exc_tiles, "MIN")
berg_exc_out = os.path.join(scratch_folder, "berg_exc_complete.tif")
complete_berg_exc.save(berg_exc_out)
complete_berg_exc_path = complete_berg_exc.catalogPath
print("Merge complete.")

print("Buffering result...")
berg_exc_buffered = arcpy.gp.DistanceAccumulation_sa(complete_berg_exc_path, os.path.join(scratch_folder, "distance_raster.tif")).getOutput(0)

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
    print(f"Generating buffered berg excavation for tile {suffix}")
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
final_result.save(os.path.join(output_folder, "final_result_raster.tif"))
print("final result complete.")

#TODO FIX CLEANUP ROUTINE



# print("cleaning up")
# for f in clipped_berg_tiles + clipped_model_tiles + clipped_filtered_tiles:
#     f_xml = f + ".xml"
#     if os.path.exists(f):
#         os.remove(f)
#     if os.path.exists(f_xml):
#         os.remove(f_xml)

