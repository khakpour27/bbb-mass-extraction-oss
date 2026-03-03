"""Tile-based spatial processing: clipping, filtering, merging, masking."""

import logging
import os

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import shapes as rasterio_shapes
from rasterio.merge import merge as rasterio_merge
from rasterio.transform import from_origin
from rasterio.windows import from_bounds
from shapely.geometry import box, shape as shapely_shape
from shapely.ops import unary_union

from config import CELL_SIZE, MAX_TILE_DIMENSION

logger = logging.getLogger(__name__)


# ── Grid index ────────────────────────────────────────────────────────────────

def load_grid_index(gpkg_path: str) -> gpd.GeoDataFrame:
    """Load the grid index GeoPackage."""
    gdf = gpd.read_file(gpkg_path)
    logger.info("Loaded grid index: %d cells", len(gdf))
    return gdf


def get_model_footprint(raster_path: str) -> "shapely.geometry.base.BaseGeometry":
    """Get a polygon footprint of non-NaN cells in a raster."""
    with rasterio.open(raster_path) as src:
        arr = src.read(1)
        # Create a mask: 1 where data exists, 0 where NoData
        if src.nodata is not None:
            mask = ((arr != src.nodata) & ~np.isnan(arr)).astype(np.uint8)
        else:
            mask = (~np.isnan(arr)).astype(np.uint8)

        polys = []
        for geom, val in rasterio_shapes(mask, transform=src.transform):
            if val == 1:
                polys.append(shapely_shape(geom))

    if not polys:
        # Fallback: use raster bounds
        with rasterio.open(raster_path) as src:
            return box(*src.bounds)

    footprint = unary_union(polys)
    logger.info("Model footprint area: %.1f m²", footprint.area)
    return footprint


def get_intersecting_tiles(
    grid_gdf: gpd.GeoDataFrame,
    footprint: "shapely.geometry.base.BaseGeometry",
) -> gpd.GeoDataFrame:
    """Return grid cells that intersect the model footprint."""
    intersecting = grid_gdf[grid_gdf.intersects(footprint)]
    logger.info("Intersecting tiles: %d / %d", len(intersecting), len(grid_gdf))
    return intersecting


# ── Raster clipping ───────────────────────────────────────────────────────────

def clip_raster_to_bounds(
    raster_path: str,
    bounds: tuple[float, float, float, float],
) -> tuple[np.ndarray, "rasterio.transform.Affine"]:
    """Clip a raster to a bounding box. Returns (array, transform).

    *bounds*: (xmin, ymin, xmax, ymax).
    """
    with rasterio.open(raster_path) as src:
        window = from_bounds(*bounds, transform=src.transform)
        # Clamp window to raster extent
        window = window.intersection(rasterio.windows.Window(0, 0, src.width, src.height))
        data = src.read(1, window=window).astype(np.float32)
        transform = src.window_transform(window)

        # Replace nodata with NaN
        if src.nodata is not None:
            data[data == src.nodata] = np.nan

    return data, transform


def clip_raster_to_file(
    raster_path: str,
    bounds: tuple[float, float, float, float],
    output_path: str,
    crs: str,
) -> str:
    """Clip a raster and write to a GeoTIFF file. Returns output path."""
    data, transform = clip_raster_to_bounds(raster_path, bounds)
    with rasterio.open(
        output_path, "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=np.nan,
    ) as dst:
        dst.write(data.astype(np.float32), 1)
    return output_path


def validate_tile_dimensions(
    array: np.ndarray,
    max_dim: float = MAX_TILE_DIMENSION,
    cell_size: float = CELL_SIZE,
) -> bool:
    """Return True if tile is within expected dimensions (not an artefact)."""
    threshold_pixels = (max_dim / cell_size) ** 2
    pixel_count = array.shape[0] * array.shape[1]
    if pixel_count > threshold_pixels:
        logger.warning(
            "Tile too large: %d×%d = %d pixels > threshold %d",
            array.shape[0], array.shape[1], pixel_count, int(threshold_pixels),
        )
        return False
    return True


# ── Raster merging ────────────────────────────────────────────────────────────

def merge_rasters_min(raster_paths: list[str], output_path: str) -> str:
    """Merge multiple GeoTIFF rasters taking the minimum value. Returns output path."""
    datasets = [rasterio.open(p) for p in raster_paths]
    try:
        mosaic, out_transform = rasterio_merge(datasets, method="min")
        profile = datasets[0].profile.copy()
        profile.update(
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            transform=out_transform,
            nodata=np.nan,
        )
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(mosaic)
    finally:
        for ds in datasets:
            ds.close()

    logger.info("Merged %d rasters → %s", len(raster_paths), output_path)
    return output_path


# ── Cell-by-cell array operations (vectorised numpy) ─────────────────────────

def filter_model_under_berg(
    model_arr: np.ndarray,
    berg_arr: np.ndarray,
) -> np.ndarray:
    """Keep only model cells that are at or below the berg surface.

    Vectorised replacement for the original for-loop (lines 169–172).
    """
    rows = min(model_arr.shape[0], berg_arr.shape[0])
    cols = min(model_arr.shape[1], berg_arr.shape[1])
    m = model_arr[:rows, :cols]
    b = berg_arr[:rows, :cols]

    mask = ~np.isnan(b) & ~np.isnan(m) & (m <= b)
    output = np.full((rows, cols), np.nan, dtype=np.float32)
    output[mask] = m[mask]
    return output


def merge_berg_with_models(
    berg_exc_arr: np.ndarray,
    model_arr: np.ndarray,
) -> np.ndarray:
    """Merge berg excavation into model raster, taking the lower elevation.

    Vectorised replacement for ``merge_berg_with_existing_models()``
    (original lines 318–328).
    """
    rows = min(model_arr.shape[0], berg_exc_arr.shape[0])
    cols = min(model_arr.shape[1], berg_exc_arr.shape[1])
    m = model_arr[:rows, :cols]
    b = berg_exc_arr[:rows, :cols]

    output = np.copy(m)

    # Where model is empty but berg has data → fill with berg
    fill_mask = np.isnan(m) & ~np.isnan(b)
    output[fill_mask] = b[fill_mask]

    # Where both exist and berg is lower → use berg
    lower_mask = ~np.isnan(m) & ~np.isnan(b) & (b < m)
    output[lower_mask] = b[lower_mask]

    return output


def merge_buffer_with_berg(
    berg_flate_arr: np.ndarray,
    berg_exc_arr: np.ndarray,
    buffer_arr: np.ndarray,
    cell_size: float = CELL_SIZE,
) -> np.ndarray:
    """Merge buffer zone with berg excavation.

    Replicates original ``merge_buffer_with_berg()`` (lines 127–154):
    - Where buffer == 0 (i.e. buffer zone) AND berg surface exists → use berg
      surface elevation
    - Where berg excavation has data → use berg excavation elevation

    *buffer_arr*: integer array from the Expand operation. In our case, a
    boolean array where True = buffer zone.
    """
    rows = min(berg_flate_arr.shape[0], berg_exc_arr.shape[0], buffer_arr.shape[0])
    cols = min(berg_flate_arr.shape[1], berg_exc_arr.shape[1], buffer_arr.shape[1])

    bf = berg_flate_arr[:rows, :cols]
    be = berg_exc_arr[:rows, :cols]
    bu = buffer_arr[:rows, :cols]

    output = np.full((rows, cols), np.nan, dtype=np.float32)

    # Buffer zone cells with berg surface → use berg surface elevation
    if bu.dtype == bool:
        buf_mask = bu & ~np.isnan(bf)
    else:
        buf_mask = (bu == 0) & ~np.isnan(bf)
    output[buf_mask] = bf[buf_mask]

    # Existing berg excavation overrides
    exc_mask = ~np.isnan(be)
    output[exc_mask] = be[exc_mask]

    return output


# ── Exclusion masking ─────────────────────────────────────────────────────────

def create_exclusion_mask(
    tunnel_raster_path: str | None,
    munkebotn_path: str | None,
    output_path: str,
    crs: str,
) -> str | None:
    """Merge tunnel raster and munkebotn mask into a single exclusion mask.

    Returns the path to the merged mask GeoTIFF, or None if no masks exist.
    """
    paths = [p for p in [tunnel_raster_path, munkebotn_path] if p and os.path.isfile(p)]
    if not paths:
        return None

    if len(paths) == 1:
        return paths[0]

    # Merge with "first" strategy — any data in either mask counts
    datasets = [rasterio.open(p) for p in paths]
    try:
        mosaic, out_transform = rasterio_merge(datasets, method="first")
        profile = datasets[0].profile.copy()
        profile.update(
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            transform=out_transform,
            crs=crs,
            nodata=np.nan,
        )
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(mosaic)
    finally:
        for ds in datasets:
            ds.close()

    logger.info("Created exclusion mask: %s", output_path)
    return output_path


def apply_exclusion_mask(
    model_raster_path: str,
    mask_raster_path: str,
    output_path: str,
    crs: str,
) -> str:
    """Remove model cells that overlap with the exclusion mask.

    Where the mask has data, the model cells are set to NaN.
    Returns the output file path.
    """
    with rasterio.open(model_raster_path) as model_src:
        model_data = model_src.read(1).astype(np.float32)
        model_transform = model_src.transform
        model_profile = model_src.profile.copy()

        if model_src.nodata is not None:
            model_data[model_data == model_src.nodata] = np.nan

    with rasterio.open(mask_raster_path) as mask_src:
        # Read mask aligned to model grid
        mask_bounds = rasterio.transform.array_bounds(
            model_data.shape[0], model_data.shape[1], model_transform,
        )
        mask_window = from_bounds(*mask_bounds, transform=mask_src.transform)
        # Clamp to mask extent
        mask_window = mask_window.intersection(
            rasterio.windows.Window(0, 0, mask_src.width, mask_src.height)
        )

        if mask_window.width > 0 and mask_window.height > 0:
            mask_data = mask_src.read(
                1,
                window=mask_window,
                out_shape=model_data.shape,
                resampling=rasterio.enums.Resampling.nearest,
            ).astype(np.float32)

            if mask_src.nodata is not None:
                mask_has_data = (mask_data != mask_src.nodata) & ~np.isnan(mask_data)
            else:
                mask_has_data = ~np.isnan(mask_data)

            # Erase model cells where mask has data
            model_data[mask_has_data] = np.nan

    model_profile.update(nodata=np.nan, dtype="float32")
    with rasterio.open(output_path, "w", **model_profile) as dst:
        dst.write(model_data.astype(np.float32), 1)

    logger.info("Applied exclusion mask → %s", output_path)
    return output_path
