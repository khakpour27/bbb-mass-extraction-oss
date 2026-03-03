"""BFS slope expansion algorithms for rock and soil excavation.

Ported from the original arcpy-based mass_calc.py (lines 182–305),
with the berg.height→berg.width bug on line 187 fixed.
"""

import logging
from collections import deque
from math import sqrt

import numpy as np
from scipy.ndimage import binary_dilation, generate_binary_structure

from config import BUFFER_DISTANCE, CELL_SIZE, ROCK_SLOPE_FACTOR, SOIL_SLOPE_DIVISOR

logger = logging.getLogger(__name__)

# 8-connected neighbour offsets: (row_delta, col_delta, distance)
def _make_neighbours(cell_size: float) -> list[tuple[int, int, float]]:
    d = cell_size
    dd = cell_size * sqrt(2)
    return [
        (-1,  0, d),   # up
        ( 1,  0, d),   # down
        ( 0, -1, d),   # left
        ( 0,  1, d),   # right
        (-1, -1, dd),  # up-left
        (-1,  1, dd),  # up-right
        ( 1, -1, dd),  # down-left
        ( 1,  1, dd),  # down-right
    ]


def propagate_rock_slope(
    model_arr: np.ndarray,
    berg_arr: np.ndarray,
    cell_size: float = CELL_SIZE,
    slope_factor: float = ROCK_SLOPE_FACTOR,
) -> np.ndarray:
    """BFS slope propagation for rock excavation (10:1 slope).

    Starting from model cells that are below the berg surface, propagates
    excavation elevation outward through rock, rising by
    ``distance * slope_factor`` per cell step.

    Corresponds to ``generate_berg_excavation()`` in the original.
    """
    rows, cols = model_arr.shape
    output = np.copy(model_arr)
    in_queue = np.zeros(output.shape, dtype=bool)
    queue: deque[tuple[int, int]] = deque()
    neighbours = _make_neighbours(cell_size)

    # Seed with all non-NaN model cells
    for r in range(rows):
        for c in range(cols):
            if not np.isnan(output[r, c]):
                queue.append((r, c))

    while queue:
        r, c = queue.popleft()
        in_queue[r, c] = False
        current_elev = output[r, c]

        for dr, dc, dist in neighbours:
            nr, nc = r + dr, c + dc
            if 0 <= nr < rows and 0 <= nc < cols:
                berg_rise = dist * slope_factor
                tent_elev = current_elev + berg_rise
                n_berg = berg_arr[nr, nc]
                n_current = output[nr, nc]

                if (
                    not np.isnan(n_berg)
                    and (np.isnan(n_current) or tent_elev < n_current)
                    and n_berg > tent_elev
                ):
                    output[nr, nc] = tent_elev
                    if not in_queue[nr, nc]:
                        queue.append((nr, nc))
                        in_queue[nr, nc] = True

    logger.info("Rock slope propagation complete (%d×%d)", rows, cols)
    return output


def propagate_soil_slope(
    model_arr: np.ndarray,
    berg_arr: np.ndarray,
    terrain_arr: np.ndarray,
    cell_size: float = CELL_SIZE,
    slope_divisor: float = SOIL_SLOPE_DIVISOR,
) -> np.ndarray:
    """BFS slope propagation for soil/loam excavation (1:1.5 slope).

    Propagates from model cells into soil areas (where berg is NaN), rising by
    ``distance / slope_divisor`` per cell step, constrained by the terrain
    surface elevation.

    Corresponds to ``generate_final_excavation()`` in the original.
    """
    rows, cols = model_arr.shape
    output = np.copy(model_arr)
    in_queue = np.zeros(output.shape, dtype=bool)
    queue: deque[tuple[int, int]] = deque()
    neighbours = _make_neighbours(cell_size)

    # Seed with all non-NaN model cells
    for r in range(rows):
        for c in range(cols):
            if not np.isnan(output[r, c]):
                queue.append((r, c))

    while queue:
        r, c = queue.popleft()
        in_queue[r, c] = False
        current_elev = output[r, c]

        for dr, dc, dist in neighbours:
            nr, nc = r + dr, c + dc
            if 0 <= nr < rows and 0 <= nc < cols:
                rise = dist / slope_divisor
                tent_elev = current_elev + rise
                n_current = output[nr, nc]
                n_berg = berg_arr[nr, nc]
                n_terrain = terrain_arr[nr, nc]

                if (
                    (np.isnan(n_current) or tent_elev < n_current)
                    and np.isnan(n_berg)
                    and tent_elev < n_terrain
                ):
                    output[nr, nc] = tent_elev
                    if not in_queue[nr, nc]:
                        queue.append((nr, nc))
                        in_queue[nr, nc] = True

    logger.info("Soil slope propagation complete (%d×%d)", rows, cols)
    return output


def buffer_excavation(
    excavation_arr: np.ndarray,
    buffer_cells: int | None = None,
    cell_size: float = CELL_SIZE,
    buffer_distance: float = BUFFER_DISTANCE,
) -> np.ndarray:
    """Expand the NaN (empty) region around excavation data by *buffer_cells*.

    Replicates the original's IsNull → Expand(zone=0, cells=5) pattern:
    the "zone 0" (NaN cells) is grown INTO the data area by the buffer,
    which effectively expands the excavation boundary.

    Returns a boolean array where True = buffer zone (was NaN, now within
    buffer distance of data cells).
    """
    if buffer_cells is None:
        buffer_cells = int(buffer_distance / cell_size)

    data_mask = ~np.isnan(excavation_arr)

    # Dilate the data region outward
    struct = generate_binary_structure(2, 2)  # 8-connected
    dilated = binary_dilation(data_mask, structure=struct, iterations=buffer_cells)

    # Buffer zone = newly added cells (not in original data)
    buffer_zone = dilated & ~data_mask
    logger.info(
        "Buffer expansion: %d buffer cells (%d new cells)",
        buffer_cells, int(buffer_zone.sum()),
    )
    return buffer_zone
