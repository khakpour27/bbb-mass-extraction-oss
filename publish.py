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
AGOL_PW = keyring.get_password("bybanen_agol", AGOL_USER) #this is stored in Windows Credential Manager. Update there if AGOL user password changes.
SCENE_ID = "aef0f7fb6ca646b48e0c274200daec07"
CORAV_GROUP_ID = "c3c1802614a24b94901cf0198ac65767"
RESULT_DEST_PATH = r"C:\Users\scbm\OneDrive - COWI\A240636-EX02- Bybanen byggetrinn 5 CORAV fellesområde - Automatisert mengdehøsting\Resultatfiler\Tverrfaglig masseuttak"
LOG_DEST_PATH = r"C:\Users\scbm\OneDrive - COWI\A240636-EX02- Bybanen byggetrinn 5 CORAV fellesområde - Automatisert mengdehøsting\Loggfiler\Tverrfaglig masseuttak"

latest_output_fld = os.listdir('output')[-1]
latest_output_fld_path = os.path.join('output', latest_output_fld)
target_gdb = os.path.join('output', latest_output_fld, f"{latest_output_fld.title()}.gdb")

print("Copying result files and logs to sharepoint")

excel_path = os.path.join(latest_output_fld_path, "masseuttak_bb5.xlsx")
log_path = os.path.join(latest_output_fld_path, "results.log")

shutil.copy2(src=excel_path, dst=RESULT_DEST_PATH)
shutil.copy2(src=log_path, dst=LOG_DEST_PATH)

print("Files copied to sharepoint")

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

print("simplifying original footprint")
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

print("zipping publishing gdb")
zip_path = os.path.join(latest_output_fld_path, f"exc_footprint_{date}.zip")
zip_gdb(publish_gdb, zip_path)

print("Merging excavation result and terrain model...")
agol_elevation = arcpy.ia.Merge([model_raster, terrain_raster], resolve_overlap='MIN')
agol_elevation.save(os.path.join(latest_output_fld_path, 'agol_elevation.tif'))

print("creating tile cache")
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

print("packaging tile cache as tpkx")
elev_tpkx = arcpy.management.ExportTileCache(
    in_cache_source=elev_tile_cache,
    in_target_cache_folder=latest_output_fld_path,
    in_target_cache_name=f"BB5_elevation_tiles_{date}",
    export_cache_type="TILE_PACKAGE_TPKX",
    storage_format_type="COMPACT_V2",
    scales="40000;20000;10000;5000;2500;1250;625"   
)

print("Packaging models to .slpk")
slpk = arcpy.management.Create3DObjectSceneLayerPackage(
    in_dataset=os.path.join(target_gdb, "MERGED_MODEL_RASTER"),
    out_slpk=os.path.join(latest_output_fld_path, f"model_scene_layer_{date}.slpk"),
    out_coor_system=arcpy.SpatialReference(25832),
    texture_optimization="NONE",

)

tunnel_slpk = arcpy.management.Create3DObjectSceneLayerPackage(
    in_dataset=os.path.join(target_gdb, "MERGED_TUNNEL_RASTER"),
    out_slpk=os.path.join(latest_output_fld_path, f"tunnel_scene_layer_{date}.slpk"),
    out_coor_system=arcpy.SpatialReference(25832),
    texture_optimization="NONE",

)

berg_slpk = arcpy.management.Create3DObjectSceneLayerPackage(
    in_dataset=os.path.join(target_gdb, "MERGED_BERG_RASTER"),
    out_slpk=os.path.join(latest_output_fld_path, f"berg_scene_layer_{date}.slpk"),
    out_coor_system=arcpy.SpatialReference(25832),
    texture_optimization="NONE",

)

print("Signing in to Bybanen AGOL")
agol_token = arcpy.SignInToPortal(AGOL_URL, AGOL_USER, AGOL_PW) #bybanen agol
gis = arcgis.GIS(url=AGOL_URL, username=AGOL_USER, password=AGOL_PW)

print("Registering existing items...")
scene_item = gis.content.get(SCENE_ID)
scene_data = scene_item.get_data()
existing_scene_items = {}
for lyr in scene_data["operationalLayers"]:
    if lyr["title"] == "Antatt bergoverflate":
        existing_scene_items["berg"] = {"id": lyr["itemId"], "url": lyr["url"]}
    if lyr["title"] == "IFC Disiplinmodeller":
        existing_scene_items["models"] = {"id": lyr["itemId"], "url": lyr["url"]}
    if lyr["title"] == "Tunneler":
        existing_scene_items["tunnels"] = {"id": lyr["itemId"], "url": lyr["url"]}
    if lyr["title"] == "Området for masseberegning":
        existing_scene_items["footprint"] = {"id": lyr["itemId"], "url": lyr["url"]}

for glyr in scene_data["ground"]["layers"]:
    existing_scene_items["elevation"] = {"id": glyr["itemId"], "url": glyr["url"]}

print("Sharing footprint to AGOL")
folders = gis.content.folders
dest_folder = folders.get(folder="Parametrisk masseuttak")

footprint_props = ItemProperties(
    title= f"excavation_footprint_{date}",
    description= "Fotavtrykk for gravemodellen for parametrisk masseuttak",
    item_type= ItemTypeEnum.FILE_GEODATABASE, 
    tags= ["BB5", "Parametrisk masseuttak"]
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
corav = gis.groups.get(CORAV_GROUP_ID)
sharing_mgr = footprint_item.sharing.groups
sharing_mgr.add(group=corav)
print(f"Footprint item published, id: {footprint_item.id}")


print("Sharing elevation package to AGOL")
elev_result, elev_package_item, elev_publishing_result = arcpy.management.SharePackage(
    in_package=os.path.join(latest_output_fld_path, f"BB5_elevation_tiles_{date}.tpkx"),
    username="",
    password="", #username and password should be passed from SignInToPortal for python session...according to docs.
    summary="Gravemodell produsert av masseuttaksscript",
    tags="Parametrisk masseuttak, gravemodell, kkp",
    credits="SCBM, COWI NORGE AS",
    public="MYGROUPS",
    groups="CORAV",
    organization="MYORGANIZATION",
    publish_web_layer="TRUE",
    portal_folder="Parametrisk masseuttak"
    )

print("Publishing elevation layer complete")
print(f"Details: \n{elev_publishing_result} ")

print("Sharing scene layer packages to AGOL")
slpk_result, slpk_package_item, slpk_publishing_result = arcpy.management.SharePackage(
    in_package=os.path.join(latest_output_fld_path, f"model_scene_layer_{date}.slpk"),
    username="",
    password="", #username and password should be passed from SignInToPortal for python session...according to docs.
    summary="IFC-modeller fra BB5 som inngår i gravemodell",
    tags="Parametrisk masseuttak, gravemodell, kkp",
    credits="SCBM, COWI NORGE AS",
    public="MYGROUPS",
    groups="CORAV",
    organization="MYORGANIZATION",
    publish_web_layer="TRUE",
    portal_folder="Parametrisk masseuttak"
    )

tunnel_result, tunnel_package_item, tunnel_publishing_result = arcpy.management.SharePackage(
    in_package=os.path.join(latest_output_fld_path, f"tunnel_scene_layer_{date}.slpk"),
    username="",
    password="", #username and password should be passed from SignInToPortal for python session...according to docs.
    summary="IFC-modeller fra BB5 for tunnel",
    tags="Parametrisk masseuttak, gravemodell, kkp",
    credits="SCBM, COWI NORGE AS",
    public="MYGROUPS",
    groups="CORAV",
    organization="MYORGANIZATION",
    publish_web_layer="TRUE",
    portal_folder="Parametrisk masseuttak"
    )

berg_result, berg_package_item, berg_publishing_result = arcpy.management.SharePackage(
    in_package=os.path.join(latest_output_fld_path, f"berg_scene_layer_{date}.slpk"),
    username="",
    password="", #username and password should be passed from SignInToPortal for python session...according to docs.
    summary="IFC-modeller fra BB5 for berg",
    tags="Parametrisk masseuttak, gravemodell, kkp",
    credits="SCBM, COWI NORGE AS",
    public="MYGROUPS",
    groups="CORAV",
    organization="MYORGANIZATION",
    publish_web_layer="TRUE",
    portal_folder="Parametrisk masseuttak"
    )

print("Publishing scene layer package complete")
print(f"itemids: \n{slpk_publishing_result}, {tunnel_publishing_result}")

print("Updating references in webscene")

new_content = gis.content.search(f"{date}")
update_items = {}
for ni in new_content:
    if ni.type in ["Image Service", "Scene Service", "Feature Service"]:
        if "model" in ni.title:
            update_items["models"] = {"id": ni.id, "url": ni.url}
        if "berg" in ni.title:
            update_items["berg"] = {"id": ni.id, "url": ni.url}
        if "tunnel" in ni.title:
            update_items["tunnels"] = {"id": ni.id, "url": ni.url}
        if "footprint" in ni.title:
            update_items["footprint"] = {"id": ni.id, "url": ni.url}
        if "elevation" in ni.title:
            update_items["elevation"] = {"id": ni.id, "url": ni.url} 

swaps = {}
for key in existing_scene_items:
    swaps[existing_scene_items[key]["id"]] = update_items[key]["id"]
    swaps[existing_scene_items[key]["url"]] = update_items[key]["url"]

scene_item.remap_data(item_mapping=swaps, force= True)
print("Web scene references updated.")

print("Deleting old data and unused packages")

folder_items = dest_folder.list()
for i in folder_items:
    if i.type in ["Compact Tile Package", "Scene Package", "File Geodatabase"]:
        i.delete()
        continue
    if date not in i.title and i.type != "Web Scene":
        i.delete()

arcpy.CheckInExtension("3D")
arcpy.CheckInExtension("Spatial")
