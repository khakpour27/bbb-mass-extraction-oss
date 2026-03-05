"""IFC parse caching — stores parsed mesh data as .npz files keyed by path + mtime.

Ported from legacy mass_calc.py:52-121 but adapted for numpy/trimesh serialization.
Avoids re-parsing unchanged IFC files across pipeline runs.
"""

import hashlib
import json
import logging
import os
import time

import numpy as np

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "ifc_cache")
MANIFEST_PATH = os.path.join(CACHE_DIR, "manifest.json")


def _cache_key(ifc_path: str) -> str:
    """Derive a filesystem-safe cache key from the IFC path."""
    # Use basename + stable hash of full path (hash() is randomized per process)
    basename = os.path.basename(ifc_path).replace(".ifc", "")
    path_hash = hashlib.md5(os.path.normpath(ifc_path).encode()).hexdigest()[:12]
    return f"{basename}_{path_hash}"


def _load_manifest() -> dict:
    """Load the cache manifest (maps cache key -> {path, mtime, num_meshes})."""
    if os.path.isfile(MANIFEST_PATH):
        try:
            with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_manifest(manifest: dict) -> None:
    """Persist the cache manifest."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def get_cached(ifc_path: str) -> list[tuple[np.ndarray, np.ndarray]] | None:
    """Return cached (vertices, faces) arrays for an IFC file, or None if stale/missing."""
    key = _cache_key(ifc_path)
    manifest = _load_manifest()
    entry = manifest.get(key)
    if entry is None:
        return None

    try:
        current_mtime = os.path.getmtime(ifc_path)
    except OSError:
        return None

    if abs(entry.get("mtime", 0) - current_mtime) > 0.01:
        return None  # file changed since caching

    npz_path = os.path.join(CACHE_DIR, f"{key}.npz")
    if not os.path.isfile(npz_path):
        return None

    try:
        data = np.load(npz_path, allow_pickle=False)
        num_meshes = entry.get("num_meshes", 0)
        arrays = []
        for i in range(num_meshes):
            verts = data[f"v_{i}"]
            faces = data[f"f_{i}"]
            arrays.append((verts, faces))
        return arrays
    except Exception as e:
        logger.warning("Cache read failed for %s: %s", os.path.basename(ifc_path), e)
        return None


def store_cached(
    ifc_path: str,
    arrays: list[tuple[np.ndarray, np.ndarray]],
) -> None:
    """Store parsed mesh arrays in cache."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    key = _cache_key(ifc_path)

    try:
        mtime = os.path.getmtime(ifc_path)
    except OSError:
        return

    # Save arrays as compressed npz
    npz_path = os.path.join(CACHE_DIR, f"{key}.npz")
    save_dict = {}
    for i, (verts, faces) in enumerate(arrays):
        save_dict[f"v_{i}"] = verts
        save_dict[f"f_{i}"] = faces

    try:
        np.savez_compressed(npz_path, **save_dict)
    except Exception as e:
        logger.warning("Cache write failed for %s: %s", os.path.basename(ifc_path), e)
        return

    # Update manifest
    manifest = _load_manifest()
    manifest[key] = {
        "path": os.path.normpath(ifc_path),
        "mtime": mtime,
        "num_meshes": len(arrays),
        "cached_at": time.time(),
    }
    _save_manifest(manifest)
    logger.debug("Cached %d meshes for %s", len(arrays), os.path.basename(ifc_path))


def partition_cached_uncached(
    ifc_paths: list[str],
) -> tuple[dict[str, list[tuple[np.ndarray, np.ndarray]]], list[str]]:
    """Split file list into cached (with data) and uncached (need parsing).

    Returns (cached_dict, uncached_list) where cached_dict maps path -> arrays.
    """
    cached: dict[str, list[tuple[np.ndarray, np.ndarray]]] = {}
    uncached: list[str] = []

    for path in ifc_paths:
        result = get_cached(path)
        if result is not None:
            cached[path] = result
        else:
            uncached.append(path)

    if cached:
        logger.info(
            "IFC cache: %d cached, %d need parsing",
            len(cached), len(uncached),
        )

    return cached, uncached


def clear_cache() -> None:
    """Remove all cached data."""
    import shutil
    if os.path.isdir(CACHE_DIR):
        shutil.rmtree(CACHE_DIR)
        logger.info("IFC cache cleared")
