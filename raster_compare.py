"""Raster comparison utilities for benchmarking OSS vs Legacy pipeline output.

Provides cell-by-cell comparison, difference statistics, and diff GeoTIFF
generation for visual inspection in GIS software.
"""

import logging

import numpy as np
import rasterio
from rasterio.transform import from_origin

logger = logging.getLogger(__name__)


def compare_rasters(path_a: str, path_b: str) -> dict:
    """Compare two GeoTIFF rasters cell-by-cell.

    Aligns the two rasters to their intersection extent, then computes
    difference statistics.

    Returns
    -------
    dict with keys:
        mean_abs_m : mean absolute difference in meters
        max_abs_m : maximum absolute difference
        pct_within_1cm : percentage of cells within 0.01m
        pct_within_10cm : percentage of cells within 0.10m
        pct_within_1m : percentage of cells within 1.0m
        valid_cells : number of cells compared
        rms_m : root mean square difference
    """
    arr_a, tf_a, arr_b, tf_b = _align_rasters(path_a, path_b)

    # Only compare where both have data
    valid = ~np.isnan(arr_a) & ~np.isnan(arr_b)
    if not np.any(valid):
        return {
            "mean_abs_m": float("nan"),
            "max_abs_m": float("nan"),
            "pct_within_1cm": 0.0,
            "pct_within_10cm": 0.0,
            "pct_within_1m": 0.0,
            "valid_cells": 0,
            "rms_m": float("nan"),
        }

    diff = arr_a[valid] - arr_b[valid]
    abs_diff = np.abs(diff)
    n = len(diff)

    return {
        "mean_abs_m": float(np.mean(abs_diff)),
        "max_abs_m": float(np.max(abs_diff)),
        "pct_within_1cm": float(np.sum(abs_diff <= 0.01) / n * 100),
        "pct_within_10cm": float(np.sum(abs_diff <= 0.10) / n * 100),
        "pct_within_1m": float(np.sum(abs_diff <= 1.0) / n * 100),
        "valid_cells": int(n),
        "rms_m": float(np.sqrt(np.mean(diff ** 2))),
    }


def generate_diff_raster(path_a: str, path_b: str, output_path: str) -> str:
    """Write a difference GeoTIFF (A - B) for visual inspection.

    Returns the output path.
    """
    arr_a, tf_a, arr_b, tf_b = _align_rasters(path_a, path_b)
    diff = arr_a - arr_b  # NaN where either is NaN

    # Use the intersection transform
    rows = min(arr_a.shape[0], arr_b.shape[0])
    cols = min(arr_a.shape[1], arr_b.shape[1])
    diff = diff[:rows, :cols]

    with rasterio.open(
        output_path, "w",
        driver="GTiff",
        height=rows,
        width=cols,
        count=1,
        dtype="float32",
        crs=_read_crs(path_a),
        transform=tf_a,
        nodata=np.nan,
    ) as dst:
        dst.write(diff.astype(np.float32), 1)

    logger.info("Wrote difference raster: %s (%d×%d)", output_path, cols, rows)
    return output_path


def compare_volumes(oss_volumes: dict, legacy_volumes: dict) -> dict:
    """Compare volume metrics between OSS and legacy pipelines.

    Returns a dict of {metric: {oss, legacy, diff, pct_diff}}.
    """
    results = {}
    all_keys = set(oss_volumes.keys()) | set(legacy_volumes.keys())

    for key in sorted(all_keys):
        oss_val = oss_volumes.get(key, 0.0)
        leg_val = legacy_volumes.get(key, 0.0)
        diff = oss_val - leg_val

        if leg_val != 0:
            pct_diff = abs(diff / leg_val) * 100
        elif oss_val != 0:
            pct_diff = 100.0
        else:
            pct_diff = 0.0

        results[key] = {
            "oss": oss_val,
            "legacy": leg_val,
            "diff": diff,
            "pct_diff": pct_diff,
        }

    return results


def _align_rasters(path_a: str, path_b: str):
    """Read and align two rasters to their common extent.

    Returns (arr_a, transform_a, arr_b, transform_b) cropped to intersection.
    """
    with rasterio.open(path_a) as src_a:
        arr_a = src_a.read(1).astype(np.float64)
        tf_a = src_a.transform
        bounds_a = src_a.bounds
        if src_a.nodata is not None:
            arr_a[arr_a == src_a.nodata] = np.nan

    with rasterio.open(path_b) as src_b:
        arr_b = src_b.read(1).astype(np.float64)
        tf_b = src_b.transform
        bounds_b = src_b.bounds
        if src_b.nodata is not None:
            arr_b[arr_b == src_b.nodata] = np.nan

    # Align to common extent (minimum intersection)
    rows = min(arr_a.shape[0], arr_b.shape[0])
    cols = min(arr_a.shape[1], arr_b.shape[1])
    arr_a = arr_a[:rows, :cols]
    arr_b = arr_b[:rows, :cols]

    return arr_a, tf_a, arr_b, tf_b


def _read_crs(path: str) -> str:
    """Read CRS from a raster file."""
    with rasterio.open(path) as src:
        return str(src.crs)
