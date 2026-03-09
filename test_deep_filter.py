"""
Quick test of the deep model filter against actual IFC data.
Imports all models + tunnels and runs the filter to verify it works.
"""
import arcpy
import os
import glob
import logging
from multiprocessing import Pool, cpu_count

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ACC_PATH = r"C:\Users\MHKK\DC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)"
IFC_PATH = os.path.join(ACC_PATH, "Discipline models")


def clean_file_name(filename):
    name = filename.replace(".ifc", "")
    if name[0] in ["_", "0","1","2","3","4","5","6","7","8","9"]:
        name = "x_" + name
    illegals = ["-", ".", "(", ")", "[", "]", ":", " "]
    return "".join([c if c not in illegals else "_" for c in name])


def import_ifc_worker(args):
    ifc_path, out_dir, spatial_ref = args
    name = clean_file_name(os.path.basename(ifc_path).replace(".ifc", ""))
    temp_gdb = arcpy.management.CreateFileGDB(out_dir, f"{name}.gdb")
    result = arcpy.conversion.BIMFileToGeodatabase(
        ifc_path, temp_gdb, name, spatial_ref, include_floorplan='EXCLUDE_FLOORPLAN')
    return result.getOutput(0)


def main():
    arcpy.CheckOutExtension("3D")
    arcpy.env.overwriteOutput = True
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(25832)

    # Create test workspace
    test_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_filter_temp")
    if os.path.exists(test_dir):
        import shutil
        shutil.rmtree(test_dir, ignore_errors=True)
    os.makedirs(test_dir, exist_ok=True)

    # Find relevant IFCs
    all_ifcs = glob.glob(os.path.join(IFC_PATH, "*.ifc"))

    substrings = ["fm_Veg", "fm_VA", "fm_FVG", "fm_Ele", "fm_Spo_Sporsystem"]
    model_ifcs = [f for f in all_ifcs if any(sub in f for sub in substrings) and "_alt" not in f]
    tunnel_ifcs = [f for f in all_ifcs if f.endswith("sprengning.ifc") and "fm_Geo" in f]

    print(f"Found {len(model_ifcs)} model IFCs, {len(tunnel_ifcs)} tunnel IFCs")

    # Import all (parallel)
    print("Importing model IFCs...")
    model_args = [(p, test_dir, arcpy.env.outputCoordinateSystem) for p in model_ifcs]
    with Pool(min(12, cpu_count())) as pool:
        bim_mps = pool.map(import_ifc_worker, model_args)

    print("Importing tunnel IFCs...")
    tunnel_args = [(p, test_dir, arcpy.env.outputCoordinateSystem) for p in tunnel_ifcs]
    with Pool(min(12, cpu_count())) as pool:
        tunnel_mps = pool.map(import_ifc_worker, tunnel_args)

    print(f"\nImported {len(bim_mps)} models, {len(tunnel_mps)} tunnels")

    # Run the filter
    from deep_model_filter import filter_deep_orphan_models
    print("\n" + "="*60)
    print("Running deep model filter...")
    print("="*60)

    excluded = filter_deep_orphan_models(bim_mps, tunnel_mps, depth_threshold=5.0)

    print(f"\nFilter complete. {len(excluded)} models excluded, {len(bim_mps)} models remaining.")

    # Cleanup
    print("\nCleaning up test workspace...")
    import shutil
    shutil.rmtree(test_dir, ignore_errors=True)
    arcpy.CheckInExtension("3D")


if __name__ == "__main__":
    main()
