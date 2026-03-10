"""BFS slope expansion algorithms for rock and soil excavation.

Ported from the original arcpy-based mass_calc.py (lines 182-305),
with the berg.height->berg.width bug on line 187 fixed.

Includes numba JIT-compiled versions for 50-100x speedup on large rasters,
with automatic fallback to pure-Python if numba is not available.
"""

import logging
from collections import deque
from math import sqrt

import numpy as np
from scipy.ndimage import binary_dilation, generate_binary_structure

from config import BUFFER_DISTANCE, CELL_SIZE, ROCK_SLOPE_FACTOR, SOIL_SLOPE_DIVISOR

logger = logging.getLogger(__name__)

# Try to import numba for JIT compilation
try:
    import numba
    from numba import njit, int32, float32, float64, boolean
    from numba.typed import List as NumbaList
    HAS_NUMBA = True
    logger.info("numba available — using JIT-compiled BFS")
except ImportError:
    HAS_NUMBA = False
    logger.info("numba not available — using pure-Python BFS (install numba for 50-100x speedup)")


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


# ── numba JIT implementations ────────────────────────────────────────────────

if HAS_NUMBA:
    @njit(cache=True)
    def _rock_slope_numba(model_arr, berg_arr, tunnel_protected, cell_size, slope_factor):
        """JIT-compiled BFS for rock slope propagation with tunnel protection."""
        rows, cols = model_arr.shape
        output = model_arr.copy()
        in_queue = np.zeros((rows, cols), dtype=numba.boolean)

        d = cell_size
        dd = cell_size * 1.4142135623730951  # sqrt(2)

        # Neighbour offsets
        dr = np.array([-1, 1, 0, 0, -1, -1, 1, 1], dtype=np.int32)
        dc = np.array([0, 0, -1, 1, -1, 1, -1, 1], dtype=np.int32)
        dist = np.array([d, d, d, d, dd, dd, dd, dd], dtype=np.float64)

        # Seed queue — allocate 4x capacity because cells can be re-added
        # after being popped (in_queue reset to False enables re-queuing)
        queue_cap = rows * cols * 4
        queue = np.empty(queue_cap, dtype=np.int64)
        head = 0
        tail = 0

        for r in range(rows):
            for c in range(cols):
                if not np.isnan(output[r, c]):
                    queue[tail] = r * cols + c
                    tail += 1

        while head < tail:
            idx = queue[head]
            head += 1
            r = idx // cols
            c = idx % cols
            in_queue[r, c] = False
            current_elev = output[r, c]

            for k in range(8):
                nr = r + dr[k]
                nc = c + dc[k]
                if 0 <= nr < rows and 0 <= nc < cols:
                    if tunnel_protected[nr, nc]:
                        continue  # skip tunnel zone
                    berg_rise = dist[k] * slope_factor
                    tent_elev = current_elev + berg_rise
                    n_berg = berg_arr[nr, nc]
                    n_current = output[nr, nc]

                    if (not np.isnan(n_berg)
                            and (np.isnan(n_current) or tent_elev < n_current)
                            and n_berg > tent_elev):
                        output[nr, nc] = tent_elev
                        if not in_queue[nr, nc] and tail < queue_cap:
                            queue[tail] = nr * cols + nc
                            tail += 1
                            in_queue[nr, nc] = True

        return output

    @njit(cache=True)
    def _soil_slope_numba(model_arr, berg_arr, terrain_arr, tunnel_protected, cell_size, slope_divisor):
        """JIT-compiled BFS for soil slope propagation with tunnel protection."""
        rows, cols = model_arr.shape
        output = model_arr.copy()
        in_queue = np.zeros((rows, cols), dtype=numba.boolean)

        d = cell_size
        dd = cell_size * 1.4142135623730951

        dr = np.array([-1, 1, 0, 0, -1, -1, 1, 1], dtype=np.int32)
        dc = np.array([0, 0, -1, 1, -1, 1, -1, 1], dtype=np.int32)
        dist = np.array([d, d, d, d, dd, dd, dd, dd], dtype=np.float64)

        # Allocate 4x capacity — cells can be re-added after popping
        queue_cap = rows * cols * 4
        queue = np.empty(queue_cap, dtype=np.int64)
        head = 0
        tail = 0

        for r in range(rows):
            for c in range(cols):
                if not np.isnan(output[r, c]):
                    queue[tail] = r * cols + c
                    tail += 1

        while head < tail:
            idx = queue[head]
            head += 1
            r = idx // cols
            c = idx % cols
            in_queue[r, c] = False
            current_elev = output[r, c]

            for k in range(8):
                nr = r + dr[k]
                nc = c + dc[k]
                if 0 <= nr < rows and 0 <= nc < cols:
                    if tunnel_protected[nr, nc]:
                        continue  # skip tunnel zone
                    rise = dist[k] / slope_divisor
                    tent_elev = current_elev + rise
                    n_current = output[nr, nc]
                    n_berg = berg_arr[nr, nc]
                    n_terrain = terrain_arr[nr, nc]

                    if ((np.isnan(n_current) or tent_elev < n_current)
                            and np.isnan(n_berg)
                            and tent_elev < n_terrain):
                        output[nr, nc] = tent_elev
                        if not in_queue[nr, nc] and tail < queue_cap:
                            queue[tail] = nr * cols + nc
                            tail += 1
                            in_queue[nr, nc] = True

        return output


# ── Pure-Python fallback implementations ─────────────────────────────────────

def _rock_slope_python(model_arr, berg_arr, tunnel_protected, cell_size, slope_factor):
    """Pure-Python BFS for rock slope propagation with tunnel protection."""
    rows, cols = model_arr.shape
    output = np.copy(model_arr)
    in_queue = np.zeros(output.shape, dtype=bool)
    queue: deque[tuple[int, int]] = deque()
    neighbours = _make_neighbours(cell_size)

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
                if tunnel_protected[nr, nc]:
                    continue  # skip tunnel zone
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

    return output


def _soil_slope_python(model_arr, berg_arr, terrain_arr, tunnel_protected, cell_size, slope_divisor):
    """Pure-Python BFS for soil slope propagation with tunnel protection."""
    rows, cols = model_arr.shape
    output = np.copy(model_arr)
    in_queue = np.zeros(output.shape, dtype=bool)
    queue: deque[tuple[int, int]] = deque()
    neighbours = _make_neighbours(cell_size)

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
                if tunnel_protected[nr, nc]:
                    continue  # skip tunnel zone
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

    return output


# ── Public API (auto-selects numba or Python) ────────────────────────────────

def _build_tunnel_protection(tunnel_mask: np.ndarray | None, rows: int, cols: int) -> np.ndarray:
    """Build boolean tunnel protection array, handling shape mismatches."""
    if tunnel_mask is None:
        return np.zeros((rows, cols), dtype=np.bool_)
    # Tunnel mask may differ slightly in shape from model — clip to match
    tr = min(tunnel_mask.shape[0], rows)
    tc = min(tunnel_mask.shape[1], cols)
    protected = np.zeros((rows, cols), dtype=np.bool_)
    protected[:tr, :tc] = ~np.isnan(tunnel_mask[:tr, :tc])
    return protected


def propagate_rock_slope(
    model_arr: np.ndarray,
    berg_arr: np.ndarray,
    cell_size: float = CELL_SIZE,
    slope_factor: float = ROCK_SLOPE_FACTOR,
    tunnel_mask: np.ndarray | None = None,
) -> np.ndarray:
    """BFS slope propagation for rock excavation (10:1 slope).

    Starting from model cells that are below the berg surface, propagates
    excavation elevation outward through rock, rising by
    ``distance * slope_factor`` per cell step.

    Parameters
    ----------
    tunnel_mask : optional float array where non-NaN = tunnel zone (BFS blocked).
        Prevents excavation from flooding into areas above tunnels.

    Uses numba JIT if available, otherwise falls back to pure Python.
    """
    rows, cols = model_arr.shape
    tunnel_protected = _build_tunnel_protection(tunnel_mask, rows, cols)
    n_protected = int(tunnel_protected.sum())
    if n_protected > 0:
        logger.info("Tunnel protection mask: %d cells blocked from rock BFS", n_protected)

    if HAS_NUMBA:
        result = _rock_slope_numba(
            model_arr.astype(np.float64),
            berg_arr.astype(np.float64),
            tunnel_protected,
            float(cell_size),
            float(slope_factor),
        )
    else:
        result = _rock_slope_python(model_arr, berg_arr, tunnel_protected, cell_size, slope_factor)

    logger.info("Rock slope propagation complete (%d×%d)", model_arr.shape[0], model_arr.shape[1])
    return result.astype(np.float32)


def propagate_soil_slope(
    model_arr: np.ndarray,
    berg_arr: np.ndarray,
    terrain_arr: np.ndarray,
    cell_size: float = CELL_SIZE,
    slope_divisor: float = SOIL_SLOPE_DIVISOR,
    tunnel_mask: np.ndarray | None = None,
) -> np.ndarray:
    """BFS slope propagation for soil/loam excavation (1:1.5 slope).

    Propagates from model cells into soil areas (where berg is NaN), rising by
    ``distance / slope_divisor`` per cell step, constrained by the terrain
    surface elevation.

    Parameters
    ----------
    tunnel_mask : optional float array where non-NaN = tunnel zone (BFS blocked).
        Prevents excavation from flooding into areas above tunnels.

    Uses numba JIT if available, otherwise falls back to pure Python.
    """
    rows, cols = model_arr.shape
    tunnel_protected = _build_tunnel_protection(tunnel_mask, rows, cols)
    n_protected = int(tunnel_protected.sum())
    if n_protected > 0:
        logger.info("Tunnel protection mask: %d cells blocked from soil BFS", n_protected)

    if HAS_NUMBA:
        result = _soil_slope_numba(
            model_arr.astype(np.float64),
            berg_arr.astype(np.float64),
            terrain_arr.astype(np.float64),
            tunnel_protected,
            float(cell_size),
            float(slope_divisor),
        )
    else:
        result = _soil_slope_python(model_arr, berg_arr, terrain_arr, tunnel_protected, cell_size, slope_divisor)

    logger.info("Soil slope propagation complete (%d×%d)", model_arr.shape[0], model_arr.shape[1])
    return result.astype(np.float32)


def buffer_excavation(
    excavation_arr: np.ndarray,
    buffer_cells: int | None = None,
    cell_size: float = CELL_SIZE,
    buffer_distance: float = BUFFER_DISTANCE,
) -> np.ndarray:
    """Expand the NaN (empty) region around excavation data by *buffer_cells*.

    Replicates the original's IsNull -> Expand(zone=0, cells=5) pattern:
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
