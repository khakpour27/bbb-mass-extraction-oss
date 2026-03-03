"""IFC file listing, filtering, and mesh extraction using IfcOpenShell + trimesh."""

import logging
import os
from multiprocessing import Pool, cpu_count

import ifcopenshell
import ifcopenshell.geom
import numpy as np
import trimesh

from config import MAX_CORES, SPORSYSTEM_Z_OFFSET
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


def _parse_ifc_worker(ifc_path: str) -> tuple[str, list[tuple[np.ndarray, np.ndarray]]]:
    """Worker function for multiprocessing — returns serialisable arrays."""
    meshes = parse_ifc(ifc_path)
    # Return raw arrays for pickling (trimesh objects don't pickle well)
    arrays = [(m.vertices.copy(), m.faces.copy()) for m in meshes]
    return ifc_path, arrays


def import_ifcs_parallel(
    ifc_paths: list[str],
    num_cores: int = MAX_CORES,
    on_file_done=None,
) -> list[trimesh.Trimesh]:
    """Parse multiple IFC files in parallel, return combined list of trimesh meshes.

    Parameters
    ----------
    on_file_done : callable(path, mesh_count, done_index, total), optional
        Called from the main process when each file finishes parsing.
    """
    total = len(ifc_paths)
    n = min(num_cores, cpu_count(), total)
    logger.info("Parsing %d IFC files with %d cores...", total, n)

    all_meshes: list[trimesh.Trimesh] = []

    if n <= 1 or total <= 1:
        for i, path in enumerate(ifc_paths):
            meshes = parse_ifc(path)
            all_meshes.extend(meshes)
            if on_file_done:
                on_file_done(path, len(meshes), i + 1, total)
        return all_meshes

    with Pool(n) as pool:
        for i, (ifc_path, arrays) in enumerate(pool.imap_unordered(_parse_ifc_worker, ifc_paths)):
            for verts, faces in arrays:
                all_meshes.append(trimesh.Trimesh(vertices=verts, faces=faces, process=False))
            logger.info("Loaded %d meshes from %s", len(arrays), os.path.basename(ifc_path))
            if on_file_done:
                on_file_done(ifc_path, len(arrays), i + 1, total)

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
