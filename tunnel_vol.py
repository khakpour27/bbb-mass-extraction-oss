"""Tunnel volume calculation using min/max height raycasting."""

import logging

import numpy as np
import trimesh

from config import CELL_SIZE, TUNNEL_ROCK_DENSITY
from rasterizer import mesh_to_raster, meshes_to_merged_raster

logger = logging.getLogger(__name__)


def calculate_tunnel_volume(
    tunnel_meshes: list[trimesh.Trimesh],
    cell_size: float = CELL_SIZE,
) -> tuple[float, float]:
    """Calculate tunnel excavation volume from meshes.

    Rasterises the combined tunnel mesh twice (MIN_HEIGHT and MAX_HEIGHT),
    then computes volume = sum of (max - min) per cell.

    Returns (volume_m3, weight_kg).
    """
    if not tunnel_meshes:
        logger.warning("No tunnel meshes provided")
        return 0.0, 0.0

    combined = trimesh.util.concatenate(tunnel_meshes)

    raster_lo, _ = mesh_to_raster(combined, cell_size, "MINIMUM_HEIGHT")
    raster_hi, _ = mesh_to_raster(combined, cell_size, "MAXIMUM_HEIGHT")

    diff = raster_hi - raster_lo
    cell_area = cell_size ** 2
    tunnel_vol = float(np.nansum(diff[diff > 0]) * cell_area)
    tunnel_weight = tunnel_vol * TUNNEL_ROCK_DENSITY

    logger.info("Tunnel volume: %.2f m³, weight: %.2f kg", tunnel_vol, tunnel_weight)
    return tunnel_vol, tunnel_weight
