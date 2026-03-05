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


def meshes_to_merged_raster(
    meshes: list[trimesh.Trimesh],
    cell_size: float,
    method: str = "MINIMUM_HEIGHT",
) -> tuple[np.ndarray, "rasterio.transform.Affine"]:
    """Combine multiple meshes into one, then rasterize.

    Meshes are concatenated into a single trimesh before raycasting, which is
    faster and ensures a single consistent grid.
    """
    if not meshes:
        raise ValueError("No meshes to rasterize")

    combined = trimesh.util.concatenate(meshes)
    logger.info(
        "Combined %d meshes → %d vertices, %d faces",
        len(meshes), len(combined.vertices), len(combined.faces),
    )
    return mesh_to_raster(combined, cell_size, method)


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
