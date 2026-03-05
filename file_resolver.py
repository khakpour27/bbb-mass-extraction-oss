"""Centralized file discovery, filtering, validation, and test-mode selection.

Both OSS and legacy pipelines consume the same file manifest, guaranteeing
identical inputs for benchmark comparisons.
"""

import logging
import os
import re

from ifc_parser import list_berg_ifcs, list_model_ifcs, list_tunnel_ifcs
from terrain_parser import list_land_xmls
from utils import list_files_by_ext

logger = logging.getLogger(__name__)

# Section code pattern in filenames: e.g. F03_011, E03_011
_SECTION_PATTERN = re.compile(r"[EF]03_(\d{3})")

# Domain categories recognised in IFC filenames
_DOMAIN_CATEGORIES = {
    "VA": "fm_VA",
    "Veg": "fm_Veg",
    "FVG": "fm_FVG",
    "Ele": "fm_Ele",
    "Spo": "fm_Spo",
    "KONS": "fm_KONS",
    "Geo": "fm_Geo",
}


def _extract_section_code(filepath: str) -> str | None:
    """Extract the 3-digit section code from a filename (e.g. '011' from 'F03_011_...')."""
    m = _SECTION_PATTERN.search(os.path.basename(filepath))
    return m.group(1) if m else None


def _group_by_section(paths: list[str]) -> dict[str, list[str]]:
    """Group files by their section code."""
    groups: dict[str, list[str]] = {}
    for p in paths:
        code = _extract_section_code(p)
        if code:
            groups.setdefault(code, []).append(p)
    return groups


def _group_by_domain(paths: list[str]) -> tuple[dict[str, list[str]], list[str]]:
    """Group file paths by domain category extracted from filename."""
    groups: dict[str, list[str]] = {}
    ungrouped: list[str] = []
    for p in paths:
        basename = os.path.basename(p)
        matched = False
        for cat_name, cat_substr in _DOMAIN_CATEGORIES.items():
            if cat_substr in basename:
                groups.setdefault(cat_name, []).append(p)
                matched = True
                break
        if not matched:
            ungrouped.append(p)
    return groups, ungrouped


def _select_test_files(
    paths: list[str],
    max_files: int,
    label: str,
    test_area_prefix: str = "",
) -> list[str]:
    """Select a coherent subset of files for test mode.

    If test_area_prefix is set, filter to that section directly.
    Otherwise, auto-detect the most populated section code.
    Falls back to domain-based selection if no section codes found.
    """
    if len(paths) <= max_files:
        return paths

    # Try section-code based selection
    if test_area_prefix:
        # Filter to files matching the prefix section
        matching = [p for p in paths if test_area_prefix in os.path.basename(p)]
        if matching:
            selected = sorted(matching)[:max_files]
            logger.info(
                "Selected %d %s files matching prefix '%s'",
                len(selected), label, test_area_prefix,
            )
            return selected

    section_groups = _group_by_section(paths)
    if section_groups:
        # Pick the most populated section
        best_section = max(section_groups, key=lambda k: len(section_groups[k]))
        section_files = sorted(section_groups[best_section])
        selected = section_files[:max_files]
        logger.info(
            "Selected %d %s files from section %s (%d available in section)",
            len(selected), label, best_section, len(section_files),
        )
        # Log section distribution
        logger.info("Section groups for %s:", label)
        for code in sorted(section_groups):
            logger.info("  Section %s: %d files", code, len(section_groups[code]))
        return selected

    # Fallback: domain-based selection (original behavior)
    domain_groups, ungrouped = _group_by_domain(paths)
    if domain_groups:
        largest_cat = max(domain_groups, key=lambda k: len(domain_groups[k]))
        cat_files = sorted(domain_groups[largest_cat])
        selected = cat_files[:max_files]
        logger.info(
            "Selected %d %s files from category '%s' (sorted by name)",
            len(selected), label, largest_cat,
        )
        return selected

    # Last resort: alphabetical
    selected = sorted(paths)[:max_files]
    logger.info("No groups found for %s — picking first %d alphabetically", label, len(selected))
    return selected


def _file_info(path: str) -> dict:
    """Get file metadata for manifest."""
    try:
        stat = os.stat(path)
        return {"path": path, "size": stat.st_size, "mtime": stat.st_mtime}
    except OSError:
        return {"path": path, "size": 0, "mtime": 0}


def resolve_files(config: dict) -> dict:
    """Discover, filter, validate, and select input files.

    Returns a file manifest dict:
        {
            "model_files": [{"path": ..., "size": ..., "mtime": ...}, ...],
            "tunnel_files": [...],
            "berg_files": [...],
            "terrain_files": [...],
            "skipped_files": [{"path": ..., "reason": ...}, ...],
        }
    """
    logger.info("Scanning for IFC files in: %s", config["MODEL_FOLDER_PATH"])
    ifc_list = list_files_by_ext(config["MODEL_FOLDER_PATH"], "*.ifc")
    logger.info("Found %d total IFC files", len(ifc_list))

    model_list = list_model_ifcs(ifc_list)
    tunnel_list = list_tunnel_ifcs(ifc_list)
    berg_list = list_berg_ifcs(config["BERG_PATH"])
    terrain_list = list_land_xmls(config["TERRAIN_PATH"])

    logger.info("Model IFCs: %d, Tunnel: %d, Berg: %d, Terrain: %d",
                len(model_list), len(tunnel_list), len(berg_list), len(terrain_list))

    # Apply file limit if set
    max_files = int(config.get("MAX_MODEL_FILES", 0))
    test_area_prefix = config.get("TEST_AREA_PREFIX", "")
    if max_files > 0:
        logger.info("MAX_MODEL_FILES=%d — selecting subsets", max_files)
        model_list = _select_test_files(model_list, max_files, "model", test_area_prefix)
        tunnel_list = _select_test_files(tunnel_list, max_files, "tunnel", test_area_prefix)
        berg_list = _select_test_files(berg_list, max_files, "berg", test_area_prefix)
        terrain_list = _select_test_files(terrain_list, max_files, "terrain", test_area_prefix)

    # Verify readability (catch ACC cloud stubs early)
    skipped: list[dict] = []
    all_files = model_list + tunnel_list + berg_list + terrain_list
    for p in all_files:
        try:
            with open(p, "rb") as fh:
                header = fh.read(16)
            if len(header) == 0:
                skipped.append({"path": p, "reason": "empty file (0 bytes) — ACC cloud stub"})
        except OSError as e:
            skipped.append({"path": p, "reason": str(e)})

    if skipped:
        logger.warning("%d file(s) cannot be read:", len(skipped))
        for entry in skipped:
            logger.warning("  SKIPPED: %s — %s", os.path.basename(entry["path"]), entry["reason"])

        bad_paths = {e["path"] for e in skipped}
        model_list = [p for p in model_list if p not in bad_paths]
        tunnel_list = [p for p in tunnel_list if p not in bad_paths]
        berg_list = [p for p in berg_list if p not in bad_paths]
        terrain_list = [p for p in terrain_list if p not in bad_paths]

    # Log selected files with sizes
    for label, paths in [("Model", model_list), ("Tunnel", tunnel_list),
                         ("Berg", berg_list), ("Terrain", terrain_list)]:
        for p in paths:
            try:
                sz = os.path.getsize(p) / (1024 * 1024)
            except OSError:
                sz = 0
            logger.info("  %s: %.1f MB  %s", label, sz, os.path.basename(p))

    return {
        "model_files": [_file_info(p) for p in model_list],
        "tunnel_files": [_file_info(p) for p in tunnel_list],
        "berg_files": [_file_info(p) for p in berg_list],
        "terrain_files": [_file_info(p) for p in terrain_list],
        "skipped_files": skipped,
    }


def manifest_paths(manifest: dict, key: str) -> list[str]:
    """Extract just the file paths from a manifest category."""
    return [entry["path"] for entry in manifest.get(key, [])]
