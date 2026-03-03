"""Cut/fill volume calculations using numpy array subtraction."""

import logging

import numpy as np
import rasterio

from config import CELL_SIZE, ROCK_DENSITY, SEDIMENT_DIESEL_FACTOR

logger = logging.getLogger(__name__)


def calculate_cut_volume(
    before_path: str,
    after_path: str,
    cell_size: float = CELL_SIZE,
) -> float:
    """Calculate excavation (cut) volume between two co-registered rasters.

    Volume = sum of (before - after) where positive, multiplied by cell area.
    Positive values indicate material removed.
    """
    with rasterio.open(before_path) as src:
        before = src.read(1).astype(np.float64)
        if src.nodata is not None:
            before[before == src.nodata] = np.nan

    with rasterio.open(after_path) as src:
        after = src.read(1).astype(np.float64)
        if src.nodata is not None:
            after[after == src.nodata] = np.nan

    # Align shapes (take minimum extent)
    rows = min(before.shape[0], after.shape[0])
    cols = min(before.shape[1], after.shape[1])
    before = before[:rows, :cols]
    after = after[:rows, :cols]

    diff = before - after  # positive where cut (material removed)
    cell_area = cell_size ** 2

    cut_volume = float(np.nansum(diff[diff > 0]) * cell_area)
    logger.info("Cut volume: %.2f m³", cut_volume)
    return cut_volume


def calculate_all_volumes(
    terrain_path: str,
    berg_path: str,
    final_path: str,
    cell_size: float = CELL_SIZE,
) -> dict[str, float]:
    """Calculate all excavation volumes and derived quantities.

    Returns a dict matching the Excel output columns.
    """
    terrain_vol = calculate_cut_volume(terrain_path, final_path, cell_size)
    berg_vol = calculate_cut_volume(berg_path, final_path, cell_size)
    sediment_vol = terrain_vol - berg_vol

    volumes = {
        "VOL_BERG_DAGSONE_m3": berg_vol,
        "VEKT_BERG_DAGSONE_kg": berg_vol * ROCK_DENSITY,
        "VOL_SEDIMENT_m3": sediment_vol,
        "VOL_SEDIMENT_DIESEL_LITER": sediment_vol * SEDIMENT_DIESEL_FACTOR,
    }

    logger.info("Volume results: %s", volumes)
    return volumes
