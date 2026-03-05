"""LandXML parsing and TIN-to-DEM conversion using lxml + direct triangle rasterization.

Optimizations:
- Direct triangle rasterization from LandXML TIN faces (no Delaunay recomputation)
- Per-file rasterization to own bounding box (avoids huge global grid)
- Terrain raster caching (GeoTIFF per file, keyed by filename+mtime)
- Accepts optional bounds to clip terrain to model footprint
"""

import hashlib
import logging
import os
import time

import numpy as np
import rasterio
from lxml import etree
from rasterio.transform import from_origin

logger = logging.getLogger(__name__)

# Both LandXML 1.1 and 1.2 namespaces
_NAMESPACES = [
    {"lx": "http://www.landxml.org/schema/LandXML-1.2"},
    {"lx": "http://www.landxml.org/schema/LandXML-1.1"},
    {},  # fallback: no namespace
]

TERRAIN_CACHE_DIR = os.path.join("scratch", "terrain_cache")


def list_land_xmls(xml_folder: str) -> list[str]:
    """Return paths to LandXML terrain surface files."""
    return [
        os.path.join(xml_folder, f)
        for f in os.listdir(xml_folder)
        if f.endswith(".xml") and "Terrengoverflate" in f
    ]


def parse_landxml(xml_path: str) -> tuple[dict[int, list[float]], list[list[int]]]:
    """Parse a LandXML file and return (points_dict, faces_list).

    *points_dict*: {id: [easting, northing, elevation]}
    *faces_list*: [[pid1, pid2, pid3], ...]

    LandXML stores coordinates as northing, easting, elevation — we swap to
    easting (X), northing (Y), elevation (Z).
    """
    # Read file into memory first — works around ACC cloud filesystem issues
    try:
        with open(xml_path, "rb") as fh:
            raw = fh.read()
    except OSError as e:
        raise OSError(f"Cannot read {xml_path} — file may be an ACC cloud stub "
                      f"that needs to be pinned/downloaded first: {e}") from e

    if len(raw) == 0:
        raise ValueError(f"File is empty (0 bytes) — likely an ACC cloud stub "
                         f"not synced locally: {xml_path}")

    parser = etree.XMLParser(huge_tree=True)
    tree = etree.fromstring(raw, parser)
    tree = tree.getroottree()

    points: dict[int, list[float]] = {}
    faces: list[list[int]] = []

    for ns in _NAMESPACES:
        prefix = "lx:" if ns else ""

        p_elements = tree.xpath(f"//{prefix}P", namespaces=ns or None)
        if not p_elements:
            continue

        for p in p_elements:
            pid = int(p.attrib["id"])
            coords = list(map(float, p.text.split()))
            # LandXML order: northing, easting, elevation → swap to x, y, z
            points[pid] = [coords[1], coords[0], coords[2]]

        f_elements = tree.xpath(f"//{prefix}F", namespaces=ns or None)
        for f in f_elements:
            face_ids = list(map(int, f.text.split()))
            faces.append(face_ids)

        break  # found a working namespace

    logger.info(
        "Parsed LandXML %s: %d points, %d faces",
        os.path.basename(xml_path), len(points), len(faces),
    )
    return points, faces


# ── Terrain raster cache ─────────────────────────────────────────────────────

def _cache_key(xml_path: str, cell_size: float) -> str:
    """Generate a cache key from file path, mtime, and cell_size."""
    basename = os.path.basename(xml_path)
    mtime = os.path.getmtime(xml_path)
    raw = f"{xml_path}|{mtime}|{cell_size}"
    h = hashlib.md5(raw.encode()).hexdigest()[:10]
    name = os.path.splitext(basename)[0]
    return f"{name}_{h}"


def _get_cached_terrain(xml_path: str, cell_size: float) -> tuple[np.ndarray, "rasterio.transform.Affine"] | None:
    """Try to load a cached terrain raster for this file+cell_size. Returns None if not found."""
    key = _cache_key(xml_path, cell_size)
    tif_path = os.path.join(TERRAIN_CACHE_DIR, f"{key}.tif")
    if not os.path.isfile(tif_path):
        return None
    try:
        with rasterio.open(tif_path) as src:
            arr = src.read(1).astype(np.float32)
            tf = src.transform
        logger.info("  Cache HIT: %s", os.path.basename(xml_path))
        return arr, tf
    except Exception as e:
        logger.warning("  Cache read failed for %s: %s", os.path.basename(xml_path), e)
        return None


def _store_cached_terrain(
    xml_path: str, cell_size: float, arr: np.ndarray, tf: "rasterio.transform.Affine",
) -> None:
    """Store a terrain raster to cache."""
    os.makedirs(TERRAIN_CACHE_DIR, exist_ok=True)
    key = _cache_key(xml_path, cell_size)
    tif_path = os.path.join(TERRAIN_CACHE_DIR, f"{key}.tif")
    try:
        with rasterio.open(
            tif_path, "w", driver="GTiff",
            height=arr.shape[0], width=arr.shape[1],
            count=1, dtype="float32",
            transform=tf, nodata=float("nan"),
            compress="deflate", tiled=True, blockxsize=256, blockysize=256,
        ) as dst:
            dst.write(arr.astype(np.float32), 1)
    except Exception as e:
        logger.warning("  Cache write failed: %s", e)


# ── Per-file rasterization ───────────────────────────────────────────────────

try:
    import numba

    @numba.njit(cache=True)
    def _rasterize_tin_faces_jit(
        verts: np.ndarray,       # (n_pts, 3) float64 — x, y, z
        tri_indices: np.ndarray, # (n_faces, 3) int32 — 0-based vertex indices
        cell_size: float,
        xmin: float, ymin: float, xmax: float, ymax: float,
    ) -> np.ndarray:
        """JIT-compiled triangle rasterization via barycentric interpolation."""
        ncols = int(np.ceil((xmax - xmin) / cell_size))
        nrows = int(np.ceil((ymax - ymin) / cell_size))
        dem = np.full((nrows, ncols), np.nan, dtype=np.float32)

        n_faces = tri_indices.shape[0]
        half_cell = cell_size * 0.5

        for fi in range(n_faces):
            i0 = tri_indices[fi, 0]
            i1 = tri_indices[fi, 1]
            i2 = tri_indices[fi, 2]

            x0, y0, z0 = verts[i0, 0], verts[i0, 1], verts[i0, 2]
            x1, y1, z1 = verts[i1, 0], verts[i1, 1], verts[i1, 2]
            x2, y2, z2 = verts[i2, 0], verts[i2, 1], verts[i2, 2]

            # Triangle bounding box → grid indices
            tri_xmin = min(x0, x1, x2)
            tri_xmax = max(x0, x1, x2)
            tri_ymin = min(y0, y1, y2)
            tri_ymax = max(y0, y1, y2)

            col_s = int((tri_xmin - xmin) / cell_size)
            col_e = int(np.ceil((tri_xmax - xmin) / cell_size))
            row_s = int((ymax - tri_ymax) / cell_size)
            row_e = int(np.ceil((ymax - tri_ymin) / cell_size))

            if col_s < 0:
                col_s = 0
            if row_s < 0:
                row_s = 0
            if col_e > ncols:
                col_e = ncols
            if row_e > nrows:
                row_e = nrows
            if col_s >= col_e or row_s >= row_e:
                continue

            denom = (y1 - y2) * (x0 - x2) + (x2 - x1) * (y0 - y2)
            if abs(denom) < 1e-10:
                continue

            inv_denom = 1.0 / denom

            for r in range(row_s, row_e):
                gy = ymax - (r + 0.5) * cell_size
                for c in range(col_s, col_e):
                    gx = xmin + (c + 0.5) * cell_size

                    w0 = ((y1 - y2) * (gx - x2) + (x2 - x1) * (gy - y2)) * inv_denom
                    w1 = ((y2 - y0) * (gx - x2) + (x0 - x2) * (gy - y2)) * inv_denom
                    w2 = 1.0 - w0 - w1

                    if w0 >= -1e-6 and w1 >= -1e-6 and w2 >= -1e-6:
                        z_val = np.float32(w0 * z0 + w1 * z1 + w2 * z2)
                        cur = dem[r, c]
                        if np.isnan(cur) or z_val < cur:
                            dem[r, c] = z_val

        return dem

    _HAS_NUMBA = True
except Exception:
    _HAS_NUMBA = False


def _rasterize_tin_faces(
    pts: dict[int, list[float]],
    faces: list[list[int]],
    cell_size: float,
    xmin: float, ymin: float, xmax: float, ymax: float,
) -> np.ndarray:
    """Rasterize TIN faces to a grid using barycentric interpolation.

    Uses numba JIT if available (50-100x faster), otherwise pure Python fallback.
    """
    # Build 0-based arrays from the point dict
    pids = sorted(pts.keys())
    pid_to_idx = {}
    for i, pid in enumerate(pids):
        pid_to_idx[pid] = i

    verts = np.empty((len(pids), 3), dtype=np.float64)
    for i, pid in enumerate(pids):
        verts[i, 0] = pts[pid][0]
        verts[i, 1] = pts[pid][1]
        verts[i, 2] = pts[pid][2]

    # Convert faces to 0-based indices, skipping any with missing point IDs
    tri_list = []
    for face in faces:
        if face[0] in pid_to_idx and face[1] in pid_to_idx and face[2] in pid_to_idx:
            tri_list.append([pid_to_idx[face[0]], pid_to_idx[face[1]], pid_to_idx[face[2]]])
    tri_indices = np.array(tri_list, dtype=np.int32)

    if _HAS_NUMBA:
        return _rasterize_tin_faces_jit(verts, tri_indices, cell_size, xmin, ymin, xmax, ymax)

    # Pure Python fallback
    ncols = int(np.ceil((xmax - xmin) / cell_size))
    nrows = int(np.ceil((ymax - ymin) / cell_size))
    dem = np.full((nrows, ncols), np.nan, dtype=np.float32)

    for fi in range(len(tri_indices)):
        i0, i1, i2 = tri_indices[fi]
        x0, y0, z0 = verts[i0]
        x1, y1, z1 = verts[i1]
        x2, y2, z2 = verts[i2]

        tri_xmin = min(x0, x1, x2)
        tri_xmax = max(x0, x1, x2)
        tri_ymin = min(y0, y1, y2)
        tri_ymax = max(y0, y1, y2)

        col_s = max(0, int((tri_xmin - xmin) / cell_size))
        col_e = min(ncols, int(np.ceil((tri_xmax - xmin) / cell_size)))
        row_s = max(0, int((ymax - tri_ymax) / cell_size))
        row_e = min(nrows, int(np.ceil((ymax - tri_ymin) / cell_size)))

        if col_s >= col_e or row_s >= row_e:
            continue

        denom = (y1 - y2) * (x0 - x2) + (x2 - x1) * (y0 - y2)
        if abs(denom) < 1e-10:
            continue
        inv_denom = 1.0 / denom

        cx = xmin + (np.arange(col_s, col_e) + 0.5) * cell_size
        cy = ymax - (np.arange(row_s, row_e) + 0.5) * cell_size
        gx, gy = np.meshgrid(cx, cy)

        w0 = ((y1 - y2) * (gx - x2) + (x2 - x1) * (gy - y2)) * inv_denom
        w1 = ((y2 - y0) * (gx - x2) + (x0 - x2) * (gy - y2)) * inv_denom
        w2 = 1.0 - w0 - w1
        inside = (w0 >= -1e-6) & (w1 >= -1e-6) & (w2 >= -1e-6)

        if not np.any(inside):
            continue

        z_interp = (w0 * z0 + w1 * z1 + w2 * z2).astype(np.float32)
        patch = dem[row_s:row_e, col_s:col_e]
        update = np.where(inside, z_interp, np.nan)
        dem[row_s:row_e, col_s:col_e] = np.fmin(patch, update)

    return dem


def _rasterize_single_terrain(
    xml_path: str,
    pts: dict[int, list[float]],
    faces: list[list[int]],
    cell_size: float,
) -> tuple[np.ndarray, "rasterio.transform.Affine", tuple[float, float, float, float]]:
    """Rasterize a single terrain file to its own bounding box.

    Uses existing TIN faces from LandXML for interpolation (no Delaunay).
    Returns (dem_array, transform, (xmin, ymin, xmax, ymax)).
    """
    # Check cache first
    cached = _get_cached_terrain(xml_path, cell_size)
    if cached is not None:
        arr, tf = cached
        xmin = tf.c
        ymax = tf.f
        xmax = xmin + arr.shape[1] * cell_size
        ymin = ymax - arr.shape[0] * cell_size
        return arr, tf, (xmin, ymin, xmax, ymax)

    t0 = time.time()
    arr = np.array(list(pts.values()))
    xy = arr[:, :2]

    xmin, ymin = xy.min(axis=0)
    xmax, ymax = xy.max(axis=0)

    # Direct triangle rasterization using existing TIN faces (no Delaunay!)
    dem = _rasterize_tin_faces(pts, faces, cell_size, xmin, ymin, xmax, ymax)
    tf = from_origin(xmin, ymax, cell_size, cell_size)

    logger.info(
        "  Rasterized %s: %dx%d (%.1fs, %d pts, %d faces)",
        os.path.basename(xml_path), dem.shape[1], dem.shape[0],
        time.time() - t0, len(pts), len(faces),
    )

    # Cache for next run
    _store_cached_terrain(xml_path, cell_size, dem, tf)

    return dem, tf, (xmin, ymin, xmax, ymax)


def tin_to_raster(
    points: dict[int, list[float]],
    cell_size: float,
    bounds: tuple[float, float, float, float] | None = None,
    faces: list[list[int]] | None = None,
) -> tuple[np.ndarray, "rasterio.transform.Affine"]:
    """Interpolate TIN points to a regular grid using linear interpolation.

    Parameters
    ----------
    points : dict mapping point id to [x, y, z]
    cell_size : raster cell size in map units
    bounds : optional (xmin, ymin, xmax, ymax); derived from data if None
    faces : optional triangle faces from LandXML (avoids Delaunay recomputation)

    Returns
    -------
    (dem_array, transform) where transform is a rasterio Affine.
    """
    pts = np.array(list(points.values()))
    xy = pts[:, :2]

    if bounds is None:
        xmin, ymin = xy.min(axis=0)
        xmax, ymax = xy.max(axis=0)
    else:
        xmin, ymin, xmax, ymax = bounds

    if faces:
        dem = _rasterize_tin_faces(points, faces, cell_size, xmin, ymin, xmax, ymax)
    else:
        from scipy.interpolate import LinearNDInterpolator
        interp = LinearNDInterpolator(xy, pts[:, 2])
        x_grid = np.arange(xmin, xmax, cell_size)
        y_grid = np.arange(ymax, ymin, -cell_size)
        xx, yy = np.meshgrid(x_grid, y_grid)
        dem = interp(xx, yy).astype(np.float32)

    transform = from_origin(xmin, ymax, cell_size, cell_size)
    logger.info(
        "TIN→raster: shape=%s, bounds=(%.1f, %.1f, %.1f, %.1f)",
        dem.shape, xmin, ymin, xmax, ymax,
    )
    return dem, transform


def parse_and_rasterize_terrain(
    xml_paths: list[str],
    cell_size: float,
    bounds: tuple[float, float, float, float] | None = None,
) -> tuple[np.ndarray, "rasterio.transform.Affine"]:
    """Parse multiple LandXML files and produce a single merged terrain raster.

    Each file is rasterized to its own bounding box (fast), then pasted into
    the output grid at the correct position. Merges by taking the minimum
    elevation where surfaces overlap.

    Parameters
    ----------
    xml_paths : list of paths to LandXML files
    cell_size : raster cell size in map units
    bounds : optional (xmin, ymin, xmax, ymax) to clip the output grid
             (e.g. model footprint with padding). If None, uses the union
             of all terrain file extents.
    """
    # First pass: parse all files, collect per-file bounds
    parsed_files: list[tuple[str, dict[int, list[float]], list[list[int]], tuple]] = []
    global_xmin = global_ymin = float("inf")
    global_xmax = global_ymax = float("-inf")

    for xml_path in xml_paths:
        try:
            pts, faces = parse_landxml(xml_path)
        except Exception as e:
            logger.error("Failed to parse %s: %s — skipping", os.path.basename(xml_path), e)
            continue
        if not pts:
            continue
        arr = np.array(list(pts.values()))
        xmin_f, ymin_f = arr[:, :2].min(axis=0)
        xmax_f, ymax_f = arr[:, :2].max(axis=0)
        global_xmin = min(global_xmin, xmin_f)
        global_ymin = min(global_ymin, ymin_f)
        global_xmax = max(global_xmax, xmax_f)
        global_ymax = max(global_ymax, ymax_f)
        parsed_files.append((xml_path, pts, faces, (xmin_f, ymin_f, xmax_f, ymax_f)))

    if not parsed_files:
        raise ValueError(
            f"No points found in any of {len(xml_paths)} LandXML file(s). "
            "Files may be ACC cloud stubs — pin them in Autodesk Desktop Connector."
        )

    # Determine output grid bounds (model footprint clip or global)
    if bounds is not None:
        out_xmin, out_ymin, out_xmax, out_ymax = bounds
        # Expand slightly to cover model + some buffer for slope propagation
        pad = 200.0  # 200m buffer around model footprint
        out_xmin = max(global_xmin, out_xmin - pad)
        out_ymin = max(global_ymin, out_ymin - pad)
        out_xmax = min(global_xmax, out_xmax + pad)
        out_ymax = min(global_ymax, out_ymax + pad)
    else:
        out_xmin, out_ymin = global_xmin, global_ymin
        out_xmax, out_ymax = global_xmax, global_ymax

    # Snap bounds to cell grid
    out_xmin = np.floor(out_xmin / cell_size) * cell_size
    out_ymin = np.floor(out_ymin / cell_size) * cell_size
    out_xmax = np.ceil(out_xmax / cell_size) * cell_size
    out_ymax = np.ceil(out_ymax / cell_size) * cell_size

    # Create the output grid
    ncols = int(np.ceil((out_xmax - out_xmin) / cell_size))
    nrows = int(np.ceil((out_ymax - out_ymin) / cell_size))
    logger.info(
        "Output terrain grid: %dx%d (%.1f x %.1f m), cell_size=%.2f",
        ncols, nrows,
        out_xmax - out_xmin, out_ymax - out_ymin,
        cell_size,
    )
    merged = np.full((nrows, ncols), np.nan, dtype=np.float32)

    # Second pass: rasterize each file to its own bounds, paste into output
    for i, (xml_path, pts, faces, file_bounds) in enumerate(parsed_files, 1):
        logger.info(
            "Rasterizing terrain %d/%d: %s (%d points, %d faces)...",
            i, len(parsed_files), os.path.basename(xml_path), len(pts), len(faces),
        )

        # Rasterize to this file's own bounding box (uses existing TIN faces)
        dem, file_tf, (fxmin, fymin, fxmax, fymax) = _rasterize_single_terrain(
            xml_path, pts, faces, cell_size,
        )

        # Compute where this file's grid overlaps with the output grid
        # Overlap region in map coordinates
        ox_min = max(out_xmin, fxmin)
        oy_min = max(out_ymin, fymin)
        ox_max = min(out_xmax, fxmax)
        oy_max = min(out_ymax, fymax)

        if ox_min >= ox_max or oy_min >= oy_max:
            logger.info("  No overlap with output grid — skipping")
            continue

        # Indices into the output array
        out_c0 = int(round((ox_min - out_xmin) / cell_size))
        out_r0 = int(round((out_ymax - oy_max) / cell_size))
        out_c1 = int(round((ox_max - out_xmin) / cell_size))
        out_r1 = int(round((out_ymax - oy_min) / cell_size))

        # Indices into the file's array
        src_c0 = int(round((ox_min - fxmin) / cell_size))
        src_r0 = int(round((fymax - oy_max) / cell_size))
        src_c1 = int(round((ox_max - fxmin) / cell_size))
        src_r1 = int(round((fymax - oy_min) / cell_size))

        # Clamp to actual array dimensions
        h_out = min(out_r1 - out_r0, merged.shape[0] - out_r0)
        w_out = min(out_c1 - out_c0, merged.shape[1] - out_c0)
        h_src = min(src_r1 - src_r0, dem.shape[0] - src_r0)
        w_src = min(src_c1 - src_c0, dem.shape[1] - src_c0)
        h = min(h_out, h_src)
        w = min(w_out, w_src)

        if h <= 0 or w <= 0:
            logger.info("  Zero-size overlap — skipping")
            continue

        src_patch = dem[src_r0:src_r0 + h, src_c0:src_c0 + w]
        out_patch = merged[out_r0:out_r0 + h, out_c0:out_c0 + w]

        # np.fmin ignores NaN
        merged[out_r0:out_r0 + h, out_c0:out_c0 + w] = np.fmin(out_patch, src_patch)

        logger.info(
            "  Pasted %dx%d patch at output[%d:%d, %d:%d]",
            w, h, out_r0, out_r0 + h, out_c0, out_c0 + w,
        )

    transform = from_origin(out_xmin, out_ymax, cell_size, cell_size)
    logger.info(
        "Merged terrain raster: shape=%s, bounds=(%.1f, %.1f, %.1f, %.1f)",
        merged.shape, out_xmin, out_ymin, out_xmax, out_ymax,
    )
    return merged, transform
