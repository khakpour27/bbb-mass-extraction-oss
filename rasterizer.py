"""3D mesh to 2D raster via Open3D raycasting, plus GeoTIFF I/O helpers.

Optimized for:
- Tiled + compressed GeoTIFF output for faster NVMe I/O
- GPU-accelerated raycasting when CUDA Open3D is available
- Configurable GDAL cache for large rasters
"""

import logging
import os

import numpy as np
import open3d as o3d
import rasterio
from rasterio.transform import from_origin
import trimesh

logger = logging.getLogger(__name__)

# Set GDAL cache to ~4 GB if not already configured (plenty of RAM on target machine)
if "GDAL_CACHEMAX" not in os.environ:
    os.environ["GDAL_CACHEMAX"] = "4096"

# Check for CUDA availability
_HAS_CUDA = False
try:
    if o3d.core.cuda.is_available():
        _HAS_CUDA = True
        logger.info("Open3D CUDA available — GPU raycasting enabled")
except (AttributeError, RuntimeError):
    pass


def mesh_to_raster(
    mesh: trimesh.Trimesh,
    cell_size: float,
    method: str = "MINIMUM_HEIGHT",
    use_gpu: bool = True,
) -> tuple[np.ndarray, "rasterio.transform.Affine"]:
    """Rasterize a trimesh mesh to a 2D height grid via raycasting.

    Parameters
    ----------
    mesh : trimesh.Trimesh
    cell_size : raster cell size in map units
    method : "MINIMUM_HEIGHT" (rays up from below) or "MAXIMUM_HEIGHT" (rays down from above)
    use_gpu : attempt GPU raycasting if CUDA Open3D available

    Returns
    -------
    (raster_array, transform)
    """
    bounds = mesh.bounds  # [[xmin,ymin,zmin],[xmax,ymax,zmax]]
    xmin, ymin, zmin = bounds[0]
    xmax, ymax, zmax = bounds[1]

    # Select device
    device = o3d.core.Device("CPU:0")
    if use_gpu and _HAS_CUDA:
        try:
            device = o3d.core.Device("CUDA:0")
        except RuntimeError:
            device = o3d.core.Device("CPU:0")

    # Build Open3D raycasting scene
    o3d_mesh = o3d.t.geometry.TriangleMesh(device)
    o3d_mesh.vertex.positions = o3d.core.Tensor(
        mesh.vertices.astype(np.float32), device=device
    )
    o3d_mesh.triangle.indices = o3d.core.Tensor(
        mesh.faces.astype(np.int32), device=device
    )
    scene = o3d.t.geometry.RaycastingScene()
    scene.add_triangles(o3d_mesh)

    # Build ray grid
    x = np.arange(xmin, xmax, cell_size)
    y = np.arange(ymax, ymin, -cell_size)  # top→bottom
    xx, yy = np.meshgrid(x, y)
    n_rays = xx.size

    if method == "MINIMUM_HEIGHT":
        z_start = zmin - 10.0
        origins = np.column_stack([xx.ravel(), yy.ravel(), np.full(n_rays, z_start)])
        directions = np.tile([0.0, 0.0, 1.0], (n_rays, 1))
    else:  # MAXIMUM_HEIGHT
        z_start = zmax + 10.0
        origins = np.column_stack([xx.ravel(), yy.ravel(), np.full(n_rays, z_start)])
        directions = np.tile([0.0, 0.0, -1.0], (n_rays, 1))

    rays = np.hstack([origins, directions]).astype(np.float32)
    result = scene.cast_rays(o3d.core.Tensor(rays))
    hit_dist = result["t_hit"].numpy()

    if method == "MINIMUM_HEIGHT":
        z_values = z_start + hit_dist
    else:
        z_values = z_start - hit_dist

    z_values[np.isinf(hit_dist)] = np.nan
    raster = z_values.reshape(xx.shape).astype(np.float32)

    transform = from_origin(xmin, ymax, cell_size, cell_size)
    return raster, transform


def _estimate_ray_grid_bytes(meshes: list[trimesh.Trimesh], cell_size: float) -> int:
    """Estimate memory needed for combined raycast (6 floats per ray * 4 bytes)."""
    combined = trimesh.util.concatenate(meshes)
    bounds = combined.bounds
    nx = int((bounds[1, 0] - bounds[0, 0]) / cell_size) + 1
    ny = int((bounds[1, 1] - bounds[0, 1]) / cell_size) + 1
    # rays array (n_rays * 6 * 4) + origins + directions + result
    return nx * ny * 6 * 4 * 3  # ~3x for working buffers


def meshes_to_merged_raster(
    meshes: list[trimesh.Trimesh],
    cell_size: float,
    method: str = "MINIMUM_HEIGHT",
    scratch_dir: str | None = None,
) -> tuple[np.ndarray, "rasterio.transform.Affine"]:
    """Rasterize multiple meshes and merge.

    For small combined bounding boxes, concatenates and raycasts in one pass.
    For large ones (>8 GB ray grid), rasterizes each mesh individually and
    merges via rasterio to avoid OOM.
    """
    if not meshes:
        raise ValueError("No meshes to rasterize")

    # Estimate memory for combined approach
    MAX_RAY_BYTES = 8 * 1024**3  # 8 GB threshold
    try:
        est_bytes = _estimate_ray_grid_bytes(meshes, cell_size)
    except Exception:
        est_bytes = MAX_RAY_BYTES + 1  # force safe path on error

    if len(meshes) == 1 or est_bytes < MAX_RAY_BYTES:
        # Fast path: combine and raycast in one pass
        combined = trimesh.util.concatenate(meshes)
        logger.info(
            "Combined %d meshes → %d vertices, %d faces (est %.1f GB rays)",
            len(meshes), len(combined.vertices), len(combined.faces),
            est_bytes / 1024**3,
        )
        return mesh_to_raster(combined, cell_size, method)

    # Memory-safe path: rasterize each mesh individually, merge via rasterio
    logger.info(
        "Ray grid too large (%.1f GB) for %d meshes — rasterizing individually and merging",
        est_bytes / 1024**3, len(meshes),
    )
    import tempfile
    import gc
    from rasterio.merge import merge as rasterio_merge

    temp_dir = scratch_dir or tempfile.gettempdir()
    temp_paths = []

    for i, mesh in enumerate(meshes):
        logger.info("  Rasterizing mesh %d/%d (%d verts)...", i + 1, len(meshes), len(mesh.vertices))
        arr, tf = mesh_to_raster(mesh, cell_size, method)
        tmp_path = os.path.join(temp_dir, f"_mesh_raster_{i}.tif")
        write_geotiff(arr, tf, "EPSG:25832", tmp_path)
        temp_paths.append(tmp_path)
        del arr
        gc.collect()

    # Merge all individual rasters
    logger.info("Merging %d individual rasters...", len(temp_paths))
    datasets = [rasterio.open(p) for p in temp_paths]
    try:
        mosaic, mosaic_transform = rasterio_merge(datasets, method="min")
    finally:
        for ds in datasets:
            ds.close()

    # Clean up temp files
    for p in temp_paths:
        try:
            os.remove(p)
        except OSError:
            pass

    result = mosaic[0]  # merge returns (bands, rows, cols), take band 0
    transform = rasterio.transform.Affine(
        mosaic_transform.a, mosaic_transform.b, mosaic_transform.c,
        mosaic_transform.d, mosaic_transform.e, mosaic_transform.f,
    )
    logger.info("Merged raster: %dx%d", result.shape[1], result.shape[0])
    return result, transform


# ── GeoTIFF I/O ──────────────────────────────────────────────────────────────

def write_geotiff(
    array: np.ndarray,
    transform: "rasterio.transform.Affine",
    crs: str,
    output_path: str,
    tiled: bool = True,
    compress: str = "deflate",
) -> None:
    """Write a 2D numpy array as a single-band GeoTIFF.

    Optimized with tiling and compression for fast NVMe I/O.
    """
    profile = {
        "driver": "GTiff",
        "height": array.shape[0],
        "width": array.shape[1],
        "count": 1,
        "dtype": "float32",
        "crs": crs,
        "transform": transform,
        "nodata": np.nan,
    }

    # Use tiled + compressed output for large rasters
    if tiled and min(array.shape) > 256:
        profile["tiled"] = True
        profile["blockxsize"] = 256
        profile["blockysize"] = 256
        if compress:
            profile["compress"] = compress

    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(array.astype(np.float32), 1)
    logger.info("Wrote GeoTIFF: %s (%s)", output_path, array.shape)


def read_geotiff(path: str) -> tuple[np.ndarray, "rasterio.transform.Affine", str]:
    """Read a single-band GeoTIFF. Returns (array, transform, crs_string)."""
    with rasterio.open(path) as src:
        array = src.read(1).astype(np.float32)
        # Replace nodata with NaN
        if src.nodata is not None:
            array[array == src.nodata] = np.nan
        return array, src.transform, str(src.crs)


def snap_transform(
    transform: "rasterio.transform.Affine",
    ref_transform: "rasterio.transform.Affine",
    cell_size: float,
) -> "rasterio.transform.Affine":
    """Snap a transform's origin to align with a reference grid.

    Adjusts the origin (upper-left corner) so that the pixel grid aligns with
    *ref_transform*'s grid at the given *cell_size*.
    """
    import math
    ref_x = ref_transform.c  # upper-left X
    ref_y = ref_transform.f  # upper-left Y
    src_x = transform.c
    src_y = transform.f

    # Snap to nearest cell boundary
    dx = src_x - ref_x
    dy = src_y - ref_y
    snapped_x = ref_x + math.floor(dx / cell_size) * cell_size
    snapped_y = ref_y + math.ceil(dy / cell_size) * cell_size

    return from_origin(snapped_x, snapped_y, cell_size, cell_size)
