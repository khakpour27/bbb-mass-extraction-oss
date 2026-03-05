"""IFC file listing, filtering, mesh extraction, and geographic validation.

Uses IfcOpenShell + trimesh for mesh extraction, with caching support
and size-sorted parallel scheduling for optimal resource utilization.
"""

import logging
import os
from multiprocessing import Pool, cpu_count

import ifcopenshell
import ifcopenshell.geom
import numpy as np
import trimesh

from config import (
    EXTENT_X_MAX,
    EXTENT_X_MIN,
    EXTENT_Y_MAX,
    EXTENT_Y_MIN,
    MAX_CORES,
    SPORSYSTEM_Z_OFFSET,
)
from utils import clean_file_name, list_files_by_ext

logger = logging.getLogger(__name__)

# ── File filtering ────────────────────────────────────────────────────────────

MODEL_SUBSTRINGS = ["fm_Veg", "fm_VA", "fm_FVG", "fm_Ele", "fm_Spo_Sporsystem"]


def list_model_ifcs(ifc_list: list[str]) -> list[str]:
    """Return IFC paths that are discipline models (exclude _alt variants)."""
    return [
        f for f in ifc_list
        if any(sub in f for sub in MODEL_SUBSTRINGS) and "_alt" not in f
    ]


def list_tunnel_ifcs(ifc_list: list[str]) -> list[str]:
    """Return IFC paths for tunnel blasting models."""
    return [f for f in ifc_list if f.endswith("sprengning.ifc") and "fm_Geo" in f]


def list_berg_ifcs(berg_path: str) -> list[str]:
    """Return IFC paths for assumed rock-surface models."""
    return [
        os.path.join(berg_path, f)
        for f in os.listdir(berg_path)
        if "Antatt-bergoverflate" in f and f.endswith(".ifc")
    ]


# ── Geographic extent validation ──────────────────────────────────────────────

def validate_mesh_extent(
    mesh: trimesh.Trimesh,
    filepath: str,
    x_min: float = EXTENT_X_MIN,
    x_max: float = EXTENT_X_MAX,
    y_min: float = EXTENT_Y_MIN,
    y_max: float = EXTENT_Y_MAX,
) -> bool:
    """Check if mesh bounding box falls within UTM32N Bergen bounds.

    Matches legacy mass_calc.py:136-144 validation logic.
    Returns True if valid, False if out of bounds.
    """
    bounds = mesh.bounds  # [[xmin,ymin,zmin],[xmax,ymax,zmax]]
    mesh_xmin, mesh_ymin = bounds[0][0], bounds[0][1]
    mesh_xmax, mesh_ymax = bounds[1][0], bounds[1][1]

    if (mesh_xmax < x_min or mesh_xmin > x_max or
            mesh_ymax < y_min or mesh_ymin > y_max):
        logger.warning(
            "Mesh from %s outside Bergen bounds: X=[%.0f,%.0f] Y=[%.0f,%.0f]",
            os.path.basename(filepath), mesh_xmin, mesh_xmax, mesh_ymin, mesh_ymax,
        )
        return False
    return True


# ── IFC → trimesh ────────────────────────────────────────────────────────────

def parse_ifc(ifc_path: str) -> list[trimesh.Trimesh]:
    """Parse an IFC file and return a list of trimesh meshes for all elements."""
    ifc_file = ifcopenshell.open(ifc_path)
    settings = ifcopenshell.geom.settings()
    settings.set(settings.USE_WORLD_COORDS, True)

    meshes: list[trimesh.Trimesh] = []
    iterator = ifcopenshell.geom.iterator(settings, ifc_file)

    if not iterator.initialize():
        logger.warning("No geometry found in %s", ifc_path)
        return meshes

    while True:
        shape = iterator.get()
        try:
            geom = shape.geometry
            verts = np.array(geom.verts).reshape(-1, 3)
            faces = np.array(geom.faces).reshape(-1, 3)
            if len(verts) > 0 and len(faces) > 0:
                mesh = trimesh.Trimesh(vertices=verts, faces=faces, process=False)
                meshes.append(mesh)
        except Exception:
            pass  # skip elements without usable geometry

        if not iterator.next():
            break

    logger.info("Parsed %d meshes from %s", len(meshes), os.path.basename(ifc_path))
    return meshes


def _parse_ifc_worker(ifc_path: str) -> tuple[str, list[tuple[np.ndarray, np.ndarray]], bool]:
    """Worker function for multiprocessing — returns serialisable arrays + validation status."""
    import time as _time
    t0 = _time.time()
    meshes = parse_ifc(ifc_path)

    # Validate geographic extent for each mesh
    valid_arrays = []
    skipped_count = 0
    for m in meshes:
        if validate_mesh_extent(m, ifc_path):
            valid_arrays.append((m.vertices.copy(), m.faces.copy()))
        else:
            skipped_count += 1

    all_valid = skipped_count == 0
    if skipped_count > 0:
        logger.warning(
            "Skipped %d meshes with bad extent in %s",
            skipped_count, os.path.basename(ifc_path),
        )

    elapsed = _time.time() - t0
    logger.info(
        "Parsed %s in %.1fs (%d meshes, %d valid)",
        os.path.basename(ifc_path), elapsed, len(meshes), len(valid_arrays),
    )

    # Cache the results
    try:
        from ifc_cache import store_cached
        store_cached(ifc_path, valid_arrays)
    except Exception:
        pass  # caching is best-effort

    return ifc_path, valid_arrays, all_valid


def _sort_by_size_desc(paths: list[str]) -> list[str]:
    """Sort IFC files by size descending (longest-job-first scheduling)."""
    sized = []
    for p in paths:
        try:
            sz = os.path.getsize(p)
        except OSError:
            sz = 0
        sized.append((sz, p))
    sized.sort(key=lambda x: -x[0])
    return [p for _, p in sized]


def import_ifcs_parallel(
    ifc_paths: list[str],
    num_cores: int = MAX_CORES,
    on_file_done=None,
) -> list[trimesh.Trimesh]:
    """Parse multiple IFC files in parallel, return combined list of trimesh meshes.

    Features:
    - Checks IFC cache first, only parses uncached files
    - Sorts uncached files by size descending (longest-job-first)
    - Returns validation status per file via on_file_done callback

    Parameters
    ----------
    on_file_done : callable(path, mesh_count, done_index, total), optional
        Called from the main process when each file finishes parsing.
    """
    total = len(ifc_paths)

    # Check cache first
    try:
        from ifc_cache import partition_cached_uncached
        cached_dict, uncached_paths = partition_cached_uncached(ifc_paths)
    except ImportError:
        cached_dict = {}
        uncached_paths = ifc_paths

    all_meshes: list[trimesh.Trimesh] = []
    done_count = 0

    # Add cached meshes
    for path, arrays in cached_dict.items():
        for verts, faces in arrays:
            all_meshes.append(trimesh.Trimesh(vertices=verts, faces=faces, process=False))
        done_count += 1
        if on_file_done:
            on_file_done(path, len(arrays), done_count, total)
        logger.info("Loaded %d meshes from cache: %s", len(arrays), os.path.basename(path))

    if not uncached_paths:
        return all_meshes

    # Sort uncached by size descending for optimal scheduling
    uncached_paths = _sort_by_size_desc(uncached_paths)

    n = min(num_cores, cpu_count(), len(uncached_paths))
    logger.info("Parsing %d IFC files with %d cores (%d from cache)...",
                len(uncached_paths), n, len(cached_dict))

    if n <= 1 or len(uncached_paths) <= 1:
        for path in uncached_paths:
            _, arrays, _ = _parse_ifc_worker(path)
            for verts, faces in arrays:
                all_meshes.append(trimesh.Trimesh(vertices=verts, faces=faces, process=False))
            done_count += 1
            if on_file_done:
                on_file_done(path, len(arrays), done_count, total)
        return all_meshes

    with Pool(n) as pool:
        for ifc_path, arrays, all_valid in pool.imap_unordered(_parse_ifc_worker, uncached_paths):
            for verts, faces in arrays:
                all_meshes.append(trimesh.Trimesh(vertices=verts, faces=faces, process=False))
            logger.info("Loaded %d meshes from %s", len(arrays), os.path.basename(ifc_path))
            done_count += 1
            if on_file_done:
                on_file_done(ifc_path, len(arrays), done_count, total)

    return all_meshes


def adjust_sporsystem_z(meshes: list[trimesh.Trimesh], offset: float = SPORSYSTEM_Z_OFFSET) -> None:
    """Lower all mesh Z-coordinates in-place by *offset* (default -0.9 m)."""
    for mesh in meshes:
        mesh.vertices[:, 2] += offset
    logger.info("Adjusted Z by %.2f m on %d meshes", offset, len(meshes))


# ── Helpers ───────────────────────────────────────────────────────────────────

def separate_sporsystem_meshes(
    ifc_paths: list[str],
    all_meshes_per_file: dict[str, list[trimesh.Trimesh]],
) -> tuple[list[trimesh.Trimesh], list[trimesh.Trimesh]]:
    """Split parsed meshes into (sporsystem_meshes, other_meshes).

    *all_meshes_per_file* maps IFC path → list of trimesh meshes.
    """
    spor = []
    other = []
    for path in ifc_paths:
        target = spor if "Sporsystem" in path else other
        target.extend(all_meshes_per_file.get(path, []))
    return spor, other
