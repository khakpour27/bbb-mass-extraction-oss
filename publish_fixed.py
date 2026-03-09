"""
publish_fixed.py — Publishes fixed pipeline results to AGOL for comparison.

Publishes to folder "Parametrisk masseuttak (fixed)" with _fixed suffix.
Does NOT update the existing web scene or delete any existing items.
"""
import os
import shutil
import arcpy
import arcgis
from arcgis.gis import ItemTypeEnum, ItemProperties
import keyring
import datetime
import zipfile
import time

def zip_gdb(input_gdb_path, out_zip_path):
    gdb_name = os.path.basename(input_gdb_path)
    with zipfile.ZipFile(out_zip_path, mode='w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zipf:
        for f in os.listdir(input_gdb_path):
            if not f.endswith(".lock"):
                zipf.write(os.path.join(input_gdb_path, f), os.path.join(gdb_name, os.path.basename(f)))

arcpy.CheckOutExtension("3D")
arcpy.CheckOutExtension("Spatial")
CELL_SIZE = 0.2
AGOL_URL = "https://bybanen.maps.arcgis.com/"
AGOL_USER = "ADM_COWI"
AGOL_PW = keyring.get_password("bybanen_agol", AGOL_USER)
CORAV_GROUP_ID = "c3c1802614a24b94901cf0198ac65767"

AGOL_FOLDER = "Parametrisk masseuttak (fixed)"
ITEM_SUFFIX = "_fixed"

latest_output_fld = os.listdir('output')[-1]
latest_output_fld_path = os.path.join('output', latest_output_fld)
target_gdb = os.path.join('output', latest_output_fld, f"{latest_output_fld.title()}.gdb")

print(f"Publishing fixed results from: {latest_output_fld_path}")
print(f"  AGOL folder: {AGOL_FOLDER}")
print(f"  Item suffix: {ITEM_SUFFIX}")

arcpy.env.overwriteOutput = True
date = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M")

if not os.path.exists(os.path.join(latest_output_fld_path, "publish.gdb")):
    arcpy.management.CreateFileGDB(latest_output_fld_path, "publish")

publish_gdb = os.path.join(latest_output_fld_path, "publish.gdb")

model_raster = arcpy.Raster(os.path.join(latest_output_fld_path, 'FINAL_RESULT_RASTER.tif'))
terrain_raster = arcpy.Raster(os.path.join(latest_output_fld_path, 'TERRAIN_MERGED_RASTER.tif'))

print("Creating footprint of excavation area.")
exc_footprint = arcpy.ddd.RasterDomain(
        in_raster=model_raster,
        out_feature_class=os.path.join(publish_gdb, f"Excavation_footprint"),
        out_geometry_type="POLYGON"
    )

print("Simplifying original footprint")
simple_footprint = arcpy.cartography.SimplifyPolygon(
        in_features=exc_footprint,
        out_feature_class=os.path.join(publish_gdb, f"Excavation_footprint_smp"),
        algorithm="BEND_SIMPLIFY",
        tolerance="1 Meters",
        minimum_area="2 SquareMeters",
        error_option="NO_CHECK",
        collapsed_point_option="NO_KEEP",
        in_barriers=None
    )

print("Deleting original footprint")
arcpy.management.Delete(os.path.join(publish_gdb, "Excavation_footprint"))

print("Zipping publishing gdb")
zip_path = os.path.join(latest_output_fld_path, f"exc_footprint{ITEM_SUFFIX}_{date}.zip")
zip_gdb(publish_gdb, zip_path)

print("Merging excavation result and terrain model...")
agol_elevation = arcpy.ia.Merge([model_raster, terrain_raster], resolve_overlap='MIN')
agol_elevation.save(os.path.join(latest_output_fld_path, 'agol_elevation.tif'))

print("Creating tile cache")
elev_tile_cache = arcpy.management.ManageTileCache(
    in_cache_location=latest_output_fld_path,
    manage_mode="RECREATE_ALL_TILES",
    in_cache_name="BB5_Elevation_Layer",
    in_datasource=agol_elevation,
    tiling_scheme="IMPORT_SCHEME",
    import_tiling_scheme=os.path.join('SCRIPT_HELP_FILES','bb5_tile_scheme.xml'),
    scales="40000;20000;10000;5000;2500;1250;625",
    min_cached_scale="40000",
    max_cached_scale="625",
    ready_to_serve_format="NON_READY_TO_SERVE_FORMAT"
)

print("Packaging tile cache as tpkx")
elev_tpkx = arcpy.management.ExportTileCache(
    in_cache_source=elev_tile_cache,
    in_target_cache_folder=latest_output_fld_path,
    in_target_cache_name=f"BB5_elevation_tiles{ITEM_SUFFIX}_{date}",
    export_cache_type="TILE_PACKAGE_TPKX",
    storage_format_type="COMPACT_V2",
    scales="40000;20000;10000;5000;2500;1250;625"
)

print("Packaging models to .slpk")
slpk = arcpy.management.Create3DObjectSceneLayerPackage(
    in_dataset=os.path.join(target_gdb, "MERGED_MODEL_RASTER"),
    out_slpk=os.path.join(latest_output_fld_path, f"model_scene_layer{ITEM_SUFFIX}_{date}.slpk"),
    out_coor_system=arcpy.SpatialReference(25832),
    texture_optimization="NONE",
)

tunnel_slpk = arcpy.management.Create3DObjectSceneLayerPackage(
    in_dataset=os.path.join(target_gdb, "MERGED_TUNNEL_RASTER"),
    out_slpk=os.path.join(latest_output_fld_path, f"tunnel_scene_layer{ITEM_SUFFIX}_{date}.slpk"),
    out_coor_system=arcpy.SpatialReference(25832),
    texture_optimization="NONE",
)

berg_slpk = arcpy.management.Create3DObjectSceneLayerPackage(
    in_dataset=os.path.join(target_gdb, "MERGED_BERG_RASTER"),
    out_slpk=os.path.join(latest_output_fld_path, f"berg_scene_layer{ITEM_SUFFIX}_{date}.slpk"),
    out_coor_system=arcpy.SpatialReference(25832),
    texture_optimization="NONE",
)

print("Signing in to Bybanen AGOL")
agol_token = arcpy.SignInToPortal(AGOL_URL, AGOL_USER, AGOL_PW)
gis = arcgis.GIS(url=AGOL_URL, username=AGOL_USER, password=AGOL_PW)

print(f"Publishing to AGOL folder: {AGOL_FOLDER}")
folders = gis.content.folders
dest_folder = folders.get(folder=AGOL_FOLDER)
if dest_folder is None:
    print(f"  Creating new folder: {AGOL_FOLDER}")
    dest_folder = folders.create(folder=AGOL_FOLDER)

corav = gis.groups.get(CORAV_GROUP_ID)

# --- Publish footprint ---
print("Sharing footprint to AGOL")
footprint_props = ItemProperties(
    title=f"excavation_footprint{ITEM_SUFFIX}_{date}",
    description="Fotavtrykk for gravemodellen (fixed pipeline: deep model filter + Strategy E)",
    item_type=ItemTypeEnum.FILE_GEODATABASE,
    tags=["BB5", "Parametrisk masseuttak", "fixed"]
)

footprint_upload_job = dest_folder.add(
    item_properties=footprint_props,
    file=zip_path
)

while not footprint_upload_job.done():
    print("Publishing in progress...")
    time.sleep(5)

gdb_item = footprint_upload_job.result()
footprint_item = gdb_item.publish()
footprint_item.sharing.groups.add(group=corav)
print(f"Footprint item published, id: {footprint_item.id}")

# --- Publish elevation ---
print("Sharing elevation package to AGOL")
elev_result, elev_package_item, elev_publishing_result = arcpy.management.SharePackage(
    in_package=os.path.join(latest_output_fld_path, f"BB5_elevation_tiles{ITEM_SUFFIX}_{date}.tpkx"),
    username="",
    password="",
    summary=f"Gravemodell (fixed pipeline: deep model filter + Strategy E)",
    tags=f"Parametrisk masseuttak, gravemodell, kkp, fixed",
    credits="SCBM, COWI NORGE AS",
    public="MYGROUPS",
    groups="CORAV",
    organization="MYORGANIZATION",
    publish_web_layer="TRUE",
    portal_folder=AGOL_FOLDER
    )

print("Publishing elevation layer complete")
print(f"Details: \n{elev_publishing_result}")

# --- Publish scene layer packages ---
print("Sharing scene layer packages to AGOL")
slpk_result, slpk_package_item, slpk_publishing_result = arcpy.management.SharePackage(
    in_package=os.path.join(latest_output_fld_path, f"model_scene_layer{ITEM_SUFFIX}_{date}.slpk"),
    username="",
    password="",
    summary=f"IFC-modeller fra BB5 (fixed pipeline: deep model filter + Strategy E)",
    tags=f"Parametrisk masseuttak, gravemodell, kkp, fixed",
    credits="SCBM, COWI NORGE AS",
    public="MYGROUPS",
    groups="CORAV",
    organization="MYORGANIZATION",
    publish_web_layer="TRUE",
    portal_folder=AGOL_FOLDER
    )

tunnel_result, tunnel_package_item, tunnel_publishing_result = arcpy.management.SharePackage(
    in_package=os.path.join(latest_output_fld_path, f"tunnel_scene_layer{ITEM_SUFFIX}_{date}.slpk"),
    username="",
    password="",
    summary=f"IFC-modeller fra BB5 tunnel (fixed pipeline: deep model filter + Strategy E)",
    tags=f"Parametrisk masseuttak, gravemodell, kkp, fixed",
    credits="SCBM, COWI NORGE AS",
    public="MYGROUPS",
    groups="CORAV",
    organization="MYORGANIZATION",
    publish_web_layer="TRUE",
    portal_folder=AGOL_FOLDER
    )

berg_result, berg_package_item, berg_publishing_result = arcpy.management.SharePackage(
    in_package=os.path.join(latest_output_fld_path, f"berg_scene_layer{ITEM_SUFFIX}_{date}.slpk"),
    username="",
    password="",
    summary=f"IFC-modeller fra BB5 berg (fixed pipeline: deep model filter + Strategy E)",
    tags=f"Parametrisk masseuttak, gravemodell, kkp, fixed",
    credits="SCBM, COWI NORGE AS",
    public="MYGROUPS",
    groups="CORAV",
    organization="MYORGANIZATION",
    publish_web_layer="TRUE",
    portal_folder=AGOL_FOLDER
    )

print("Publishing scene layer packages complete")
print(f"Item IDs: \n  model: {slpk_publishing_result}\n  tunnel: {tunnel_publishing_result}\n  berg: {berg_publishing_result}")

print("")
print("=" * 60)
print("PUBLISH SUMMARY (fixed — deep model filter + Strategy E)")
print("=" * 60)
print(f"  AGOL folder:  {AGOL_FOLDER}")
print(f"  Timestamp:    {date}")
print(f"  Items published:")
print(f"    Footprint:  excavation_footprint{ITEM_SUFFIX}_{date}")
print(f"    Elevation:  BB5_elevation_tiles{ITEM_SUFFIX}_{date}")
print(f"    Model SLPK: model_scene_layer{ITEM_SUFFIX}_{date}")
print(f"    Tunnel SLPK: tunnel_scene_layer{ITEM_SUFFIX}_{date}")
print(f"    Berg SLPK:  berg_scene_layer{ITEM_SUFFIX}_{date}")
print(f"  Web scene:    NOT MODIFIED (production scene untouched)")
print(f"  Old items:    NOT DELETED (production folder untouched)")
print("=" * 60)
print("")
print("To clean up fixed items later, delete the folder")
print(f"  '{AGOL_FOLDER}' from AGOL content.")

arcpy.CheckInExtension("3D")
arcpy.CheckInExtension("Spatial")
