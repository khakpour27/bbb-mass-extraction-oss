"""
Detect and exclude model feature datasets that go deep underground without
corresponding tunnel coverage.

Any infrastructure model (Sporsystem, Vegsystem, etc.) that extends significantly
below terrain without a tunnel enclosing it is likely a data mismatch — e.g., a
tunnel was moved but the infrastructure model wasn't updated. Including these in
the analysis creates false excavation.

Usage:
    excluded = filter_deep_orphan_models(bim_mps, tunnel_mps, depth_threshold=5.0)
    # bim_mps is modified in-place (orphans removed), excluded contains the removed paths
"""

import os
import logging
import arcpy


def get_multipatch_z_range(feature_dataset_path):
    """Get the Z range (min, max) of all multipatch features in a feature dataset."""
    z_min = float('inf')
    z_max = float('-inf')
    count = 0

    try:
        desc = arcpy.Describe(feature_dataset_path)
        for child in desc.children:
            if child.shapeType == "MultiPatch":
                fc_desc = arcpy.Describe(child.catalogPath)
                ext = fc_desc.extent
                if hasattr(ext, 'ZMin') and ext.ZMin is not None and ext.ZMin < z_min:
                    z_min = ext.ZMin
                if hasattr(ext, 'ZMax') and ext.ZMax is not None and ext.ZMax > z_max:
                    z_max = ext.ZMax
                count += int(arcpy.management.GetCount(child.catalogPath).getOutput(0))
    except Exception as e:
        logging.warning("Could not read Z range for %s: %s", feature_dataset_path, e)
        return None, None, 0

    if z_min == float('inf'):
        return None, None, 0
    return z_min, z_max, count


def get_deep_features_extent(feature_dataset_path, z_threshold):
    """Get the XY extent of only the features whose Z goes below z_threshold.

    Instead of checking the whole model's bounding box, this iterates individual
    features and collects the extent of only those that are actually deep.
    Returns an arcpy.Extent or None if no deep features found.
    """
    x_min = float('inf')
    y_min = float('inf')
    x_max = float('-inf')
    y_max = float('-inf')
    deep_count = 0

    try:
        desc = arcpy.Describe(feature_dataset_path)
        for child in desc.children:
            if child.shapeType == "MultiPatch":
                with arcpy.da.SearchCursor(child.catalogPath, ["SHAPE@"]) as cursor:
                    for row in cursor:
                        shape = row[0]
                        if shape is None:
                            continue
                        ext = shape.extent
                        if ext.ZMin is not None and ext.ZMin < z_threshold:
                            deep_count += 1
                            if ext.XMin < x_min: x_min = ext.XMin
                            if ext.YMin < y_min: y_min = ext.YMin
                            if ext.XMax > x_max: x_max = ext.XMax
                            if ext.YMax > y_max: y_max = ext.YMax
    except Exception as e:
        logging.warning("Could not get deep features extent for %s: %s",
                        feature_dataset_path, e)
        return None, 0

    if x_min == float('inf'):
        return None, 0
    return arcpy.Extent(x_min, y_min, x_max, y_max), deep_count


def get_tunnel_geometries(tunnel_mps):
    """Build a list of (tunnel_name, union_polygon) for all tunnel feature datasets.

    Uses actual multipatch feature footprints (convex hulls) unioned per tunnel,
    NOT bounding boxes. This captures the real tunnel path geometry.
    """
    tunnels = []
    for tunnel_fd in tunnel_mps:
        name = os.path.basename(tunnel_fd)
        try:
            desc = arcpy.Describe(tunnel_fd)
            for child in desc.children:
                if child.shapeType == "MultiPatch":
                    # Get actual 2D footprint of the tunnel multipatch
                    footprint_fc = f"memory/tunnel_fp_{len(tunnels)}"
                    try:
                        arcpy.ddd.MultiPatchFootprint(
                            child.catalogPath, footprint_fc
                        )
                        footprints = []
                        with arcpy.da.SearchCursor(footprint_fc, ["SHAPE@"]) as cur:
                            for row in cur:
                                if row[0] is not None:
                                    footprints.append(row[0])
                        if footprints:
                            union = footprints[0]
                            for f in footprints[1:]:
                                union = union.union(f)
                            tunnels.append((name, union))
                            logging.info("[DeepFilter] Tunnel %s: 2D footprint from %d polygons",
                                         name, len(footprints))
                        arcpy.management.Delete(footprint_fc)
                    except Exception as fp_err:
                        logging.warning("[DeepFilter] MultiPatchFootprint failed for %s: %s. "
                                        "Falling back to feature extent polygons.", name, fp_err)
                        # Fallback: union individual feature extent polygons
                        extents = []
                        with arcpy.da.SearchCursor(child.catalogPath, ["SHAPE@"]) as cursor:
                            for row in cursor:
                                if row[0] is not None:
                                    extents.append(row[0].extent.polygon)
                        if extents:
                            union = extents[0]
                            for e in extents[1:]:
                                union = union.union(e)
                            tunnels.append((name, union))
                            logging.info("[DeepFilter] Tunnel %s: %d feature extents unioned (fallback)",
                                         name, len(extents))
        except Exception as e:
            logging.warning("Could not read tunnel geometry for %s: %s", tunnel_fd, e)
    return tunnels


def check_deep_features_tunnel_coverage(deep_features_path, z_threshold, tunnel_geometries):
    """Check if the individual deep features are covered by any tunnel geometry.

    Iterates each deep feature and checks if its footprint intersects any tunnel.
    Returns (has_coverage, tunnel_name, covered_count, total_deep).
    """
    covered = 0
    uncovered = 0

    try:
        desc = arcpy.Describe(deep_features_path)
        for child in desc.children:
            if child.shapeType == "MultiPatch":
                with arcpy.da.SearchCursor(child.catalogPath, ["SHAPE@"]) as cursor:
                    for row in cursor:
                        shape = row[0]
                        if shape is None:
                            continue
                        ext = shape.extent
                        if ext.ZMin is None or ext.ZMin >= z_threshold:
                            continue  # Not a deep feature
                        # Check this specific feature's centroid against tunnels
                        feat_centroid = shape.centroid
                        feat_point = arcpy.PointGeometry(feat_centroid, shape.spatialReference)
                        found = False
                        for tunnel_name, tunnel_geom in tunnel_geometries:
                            if feat_point.within(tunnel_geom):
                                covered += 1
                                found = True
                                break
                        if not found:
                            uncovered += 1
    except Exception as e:
        logging.warning("Could not check deep features for %s: %s",
                        deep_features_path, e)
        return False, None, 0, 0

    total = covered + uncovered
    if total == 0:
        return False, None, 0, 0

    # If ANY deep feature is uncovered, flag the model
    if uncovered > 0:
        return False, None, covered, total
    return True, "all covered", covered, total


def filter_deep_orphan_models(bim_mps, tunnel_mps, depth_threshold=5.0):
    """Detect and remove model feature datasets that go deep without tunnel coverage.

    Only checks infrastructure types that require a tunnel when going deep:
    road (Veg, FVG) and rail (Spo). Water/sewer pipes (VA) go underground
    without tunnels and are skipped.

    Args:
        bim_mps: List of model feature dataset paths (modified in-place)
        tunnel_mps: List of tunnel feature dataset paths
        depth_threshold: How deep below Z=0 a model must go to be considered
                         "deep" (meters). Models shallower than this are always kept.

    Returns:
        List of (path, reason_dict) tuples for excluded models.
    """
    # Only these infrastructure types require a tunnel when going deep
    TUNNEL_REQUIRED_TYPES = ["fm_Veg", "fm_FVG", "fm_Spo"]

    excluded = []
    z_threshold = -depth_threshold

    # First pass: identify all models and their Z ranges
    model_info = []
    for fd_path in bim_mps:
        name = os.path.basename(fd_path.split(".gdb")[0]) if ".gdb" in fd_path else os.path.basename(fd_path)

        # Skip model types that don't require tunnels (e.g. VA pipes)
        requires_tunnel = any(t in name for t in TUNNEL_REQUIRED_TYPES)
        if not requires_tunnel:
            logging.info("[DeepFilter] %s: skipped (type does not require tunnel)", name)
            continue
        z_min, z_max, count = get_multipatch_z_range(fd_path)
        model_info.append({
            "path": fd_path,
            "name": name,
            "z_min": z_min,
            "z_max": z_max,
            "count": count,
        })
        if z_min is not None:
            logging.info("[DeepFilter] %s: Z range %.1f to %.1f (%d objects)",
                         name, z_min, z_max, count)

    # Pre-build tunnel geometries once
    tunnel_geometries = get_tunnel_geometries(tunnel_mps)
    logging.info("[DeepFilter] Loaded %d tunnel geometries for coverage checks",
                 len(tunnel_geometries))

    # Second pass: check deep models for tunnel coverage
    for info in model_info:
        if info["z_min"] is None:
            continue

        if info["z_min"] >= z_threshold:
            continue  # Shallow model, always keep

        # Deep model — check each deep feature against actual tunnel geometry
        has_tunnel, tunnel_name, covered, total = check_deep_features_tunnel_coverage(
            info["path"], z_threshold, tunnel_geometries)
        logging.info("[DeepFilter] %s: %d deep features, %d covered by tunnel, %d uncovered",
                     info["name"], total, covered, total - covered)

        if not has_tunnel:
            reason = {
                "z_min": info["z_min"],
                "z_max": info["z_max"],
                "count": info["count"],
                "deep_count": total,
                "uncovered_count": total - covered,
                "reason": "Deep model (Z=%.1f, %d/%d deep features uncovered by tunnel)" % (
                    info["z_min"], total - covered, total),
            }
            excluded.append((info["path"], reason))
            logging.warning(
                "[DeepFilter] EXCLUDING %s: Z_min=%.1f, %d/%d deep features not covered by any tunnel.",
                info["name"], info["z_min"], total - covered, total
            )
        else:
            logging.info("[DeepFilter] KEEPING %s: Z_min=%.1f, all %d deep features covered by tunnels",
                         info["name"], info["z_min"], total)

    # Remove excluded models from bim_mps
    excluded_paths = {e[0] for e in excluded}
    for path in excluded_paths:
        bim_mps.remove(path)

    if excluded:
        print(f"\n{'='*60}")
        print(f"WARNING: {len(excluded)} model(s) excluded (deep without tunnel):")
        for path, reason in excluded:
            name = os.path.basename(path.split(".gdb")[0]) if ".gdb" in path else os.path.basename(path)
            print(f"  {name}: {reason['reason']}")
        print(f"{'='*60}\n")
    else:
        logging.info("[DeepFilter] All models passed — none excluded")

    return excluded
