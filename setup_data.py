"""One-time setup script: copy SCRIPT_HELP_FILES and convert AOI.gdb to GeoPackage."""

import os
import shutil

SRC_DIR = os.path.join(
    os.path.dirname(__file__),
    "..",
    "bbb_mass_extraction",
    "SCRIPT_HELP_FILES",
)
DST_DIR = os.path.join(os.path.dirname(__file__), "SCRIPT_HELP_FILES")


def copy_help_files() -> None:
    """Copy munkebotn_mask.tif and AOI.gdb from the original project."""
    src = os.path.normpath(SRC_DIR)
    dst = os.path.normpath(DST_DIR)

    if not os.path.isdir(src):
        raise FileNotFoundError(f"Source directory not found: {src}")

    os.makedirs(dst, exist_ok=True)

    # Copy munkebotn mask
    mask_src = os.path.join(src, "munkebotn_mask.tif")
    mask_dst = os.path.join(dst, "munkebotn_mask.tif")
    if os.path.isfile(mask_src) and not os.path.isfile(mask_dst):
        shutil.copy2(mask_src, mask_dst)
        print(f"Copied {mask_src} -> {mask_dst}")

    # Copy AOI.gdb (needed for conversion)
    gdb_src = os.path.join(src, "AOI.gdb")
    gdb_dst = os.path.join(dst, "AOI.gdb")
    if os.path.isdir(gdb_src) and not os.path.isdir(gdb_dst):
        shutil.copytree(gdb_src, gdb_dst)
        print(f"Copied {gdb_src} -> {gdb_dst}")


def convert_gdb_to_gpkg() -> None:
    """Convert AOI.gdb/INDEX_GRID_200_overlap to a GeoPackage."""
    import geopandas as gpd

    gdb_path = os.path.join(DST_DIR, "AOI.gdb")
    gpkg_path = os.path.join(DST_DIR, "grid_index.gpkg")

    if os.path.isfile(gpkg_path):
        print(f"GeoPackage already exists: {gpkg_path}")
        return

    if not os.path.isdir(gdb_path):
        raise FileNotFoundError(
            f"AOI.gdb not found at {gdb_path}. Run copy_help_files() first."
        )

    gdf = gpd.read_file(gdb_path, layer="INDEX_GRID_200_overlap")
    gdf.to_file(gpkg_path, driver="GPKG")
    print(f"Converted {gdb_path} -> {gpkg_path}")


if __name__ == "__main__":
    copy_help_files()
    convert_gdb_to_gpkg()
    print("Setup complete.")
