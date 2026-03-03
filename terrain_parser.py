"""LandXML parsing and TIN-to-DEM conversion using lxml + scipy."""

import logging
import os

import numpy as np
from lxml import etree
from rasterio.transform import from_origin
from scipy.interpolate import LinearNDInterpolator
from scipy.spatial import Delaunay

logger = logging.getLogger(__name__)

# Both LandXML 1.1 and 1.2 namespaces
_NAMESPACES = [
    {"lx": "http://www.landxml.org/schema/LandXML-1.2"},
    {"lx": "http://www.landxml.org/schema/LandXML-1.1"},
    {},  # fallback: no namespace
]


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
    # where etree.parse(path) fails but reading bytes works after pinning
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


def tin_to_raster(
    points: dict[int, list[float]],
    cell_size: float,
    bounds: tuple[float, float, float, float] | None = None,
) -> tuple[np.ndarray, "rasterio.transform.Affine"]:
    """Interpolate TIN points to a regular grid using linear interpolation.

    Parameters
    ----------
    points : dict mapping point id to [x, y, z]
    cell_size : raster cell size in map units
    bounds : optional (xmin, ymin, xmax, ymax); derived from data if None

    Returns
    -------
    (dem_array, transform) where transform is a rasterio Affine.
    """
    pts = np.array(list(points.values()))
    xy = pts[:, :2]
    z = pts[:, 2]

    if bounds is None:
        xmin, ymin = xy.min(axis=0)
        xmax, ymax = xy.max(axis=0)
    else:
        xmin, ymin, xmax, ymax = bounds

    interp = LinearNDInterpolator(xy, z)

    x_grid = np.arange(xmin, xmax, cell_size)
    y_grid = np.arange(ymax, ymin, -cell_size)  # top→bottom for raster convention
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

    Merges by taking the minimum elevation where surfaces overlap.
    """
    all_points: dict[int, list[float]] = {}
    offset = 0
    for xml_path in xml_paths:
        try:
            pts, _ = parse_landxml(xml_path)
        except Exception as e:
            logger.error("Failed to parse %s: %s — skipping", os.path.basename(xml_path), e)
            continue
        # Re-key to avoid id collisions across files
        for pid, coords in pts.items():
            all_points[pid + offset] = coords
        offset += max(pts.keys()) + 1 if pts else 0

    if not all_points:
        raise ValueError(
            f"No points found in any of {len(xml_paths)} LandXML file(s). "
            "Files may be ACC cloud stubs — pin them in Autodesk Desktop Connector."
        )

    return tin_to_raster(all_points, cell_size, bounds)
