# BBB Mass Extraction - Open Source Rewrite

## Project Overview

This project is an open-source rewrite of the Bergen Bybane BB5 mass extraction pipeline, replacing the proprietary ArcGIS Pro (arcpy) dependency with open-source Python libraries. The pipeline calculates excavation volumes from BIM/IFC discipline models, terrain data (LandXML), and geological surfaces (rock/berg), then outputs results to Excel.

**Original project:** `C:\Users\MHKK\bbb_mass_extraction` (arcpy-based, reference implementation)

## Goal

Run the full mass extraction calculation on any machine (server, Docker, CI/CD) without an ArcGIS Pro license. Produce identical Excel output (`masseuttak_bb5.xlsx`) with the same volume calculations.

---

## Architecture

### Pipeline Stages

```
1. INPUT PARSING
   IFC files ──> IfcOpenShell ──> trimesh meshes
   LandXML   ──> lxml         ──> scipy TIN ──> numpy DEM array
   Berg IFCs ──> IfcOpenShell ──> trimesh meshes

2. RASTERIZATION
   trimesh meshes ──> Open3D raycasting ──> numpy 2D height arrays ──> GeoTIFF (rasterio)

3. GRID-BASED PROCESSING (per 200m tile)
   - Clip model/berg/terrain rasters to tile
   - Filter model cells below berg elevation
   - BFS slope propagation (rock 10:1, soil 1:1.5)
   - Buffer berg excavation by 1m
   - Merge excavation surfaces

4. VOLUME CALCULATION
   - CutFill = numpy array subtraction + cell area multiplication
   - Terrain vol, berg vol, sediment vol = terrain_vol - berg_vol

5. OUTPUT
   - Excel via pandas + openpyxl
   - GeoTIFF rasters via rasterio
```

### Directory Structure

```
bbb_mass_extraction_oss/
├── CLAUDE.md                  # This file
├── requirements.txt           # Python dependencies
├── runner.py                  # Entry point / orchestrator (CLI + web-callable)
├── config.py                  # All configurable paths and constants
├── ifc_parser.py              # IFC file listing, filtering, and mesh extraction
├── terrain_parser.py          # LandXML parsing and TIN-to-DEM conversion
├── rasterizer.py              # 3D mesh to 2D raster (raycasting)
├── grid_processor.py          # Tile-based spatial processing
├── slope_propagation.py       # BFS slope expansion algorithms
├── volume_calc.py             # Cut/fill volume calculations
├── tunnel_vol.py              # Tunnel volume calculation
├── output_writer.py           # Excel and CSV output
├── utils.py                   # Shared utilities (file listing, name cleaning)
├── web_ui.py                  # FastAPI web dashboard server (port 8502)
├── pipeline_worker.py         # Subprocess-based pipeline runner for web UI
├── templates/
│   └── index.html             # Single-page dashboard (config, logs, progress)
├── config_overrides.json      # User config overrides (auto-created, gitignored)
└── SCRIPT_HELP_FILES/         # Copied from original project
    ├── AOI.gdb/               # Grid index (convert to GeoPackage or GeoJSON)
    ├── grid_index.gpkg        # Converted grid index (GeoPackage)
    ├── munkebotn_mask.tif      # Exclusion mask raster
    └── Expand.rft.xml          # Not needed (replaced by scipy dilation)
```

---

## Web UI

A local FastAPI web dashboard for configuring, running, and monitoring the pipeline.

### Quick Start

```bash
python web_ui.py
# Opens http://localhost:8502
```

### Architecture

- **`web_ui.py`** — FastAPI server with REST API + SSE log streaming
- **`pipeline_worker.py`** — Runs the pipeline in a **separate process** (not thread) via `multiprocessing.Process` so CPU-heavy work doesn't block the web server
- **`templates/index.html`** — Two-column SPA: config form (left), run controls + live logs + results (right)

### API Endpoints

```
GET  /                        → Dashboard HTML
GET  /api/config              → Current config as JSON
POST /api/config              → Update config (saved to config_overrides.json)
POST /api/config/preset/{name} → Apply preset ("test" or "prod")
POST /api/config/reset        → Reset to defaults
POST /api/run                 → Start pipeline in background subprocess
POST /api/stop                → Cancel pipeline (checked between steps)
GET  /api/status              → {running, step, progress_pct, error}
GET  /api/logs                → SSE stream of log lines + progress events
GET  /api/results             → List completed output folders
GET  /api/results/{name}/{file} → Download output files
```

### Config Management

- Defaults from `config.py` module attributes
- User edits saved to `config_overrides.json` (persists across restarts)
- Presets: **test** (CELL_SIZE=1.0, MAX_CORES=2, GRID_CELL_SIZE=200, MAX_MODEL_FILES=10) and **prod** (defaults)

### Test Mode File Selection

When `MAX_MODEL_FILES > 0`, the pipeline selects files intelligently:
- Groups model IFCs by domain category (VA, Veg, FVG, Ele, Spo, KONS, Geo)
- Picks from the largest category group
- Sorts by filename for geographical proximity (section numbers in filenames correlate with location)
- Berg/tunnel/terrain files fall back to alphabetical sorting

### Key Design Decisions

- **Subprocess, not thread**: The pipeline uses `multiprocessing.Process` (non-daemon, so it can spawn `Pool` children for parallel IFC parsing). A `multiprocessing.Queue` carries log entries and progress updates back to the web server. This keeps the FastAPI event loop responsive during heavy terrain parsing / raycasting.
- **SSE for real-time logs**: `sse-starlette` EventSourceResponse streams log lines, progress events, and status heartbeats every 300ms.
- **Elapsed timer + heartbeat**: The UI shows a running clock and a pulsing dot that turns red if no data arrives for 10s.

### ACC Cloud File Handling

Input files may be Autodesk Construction Cloud (ACC) placeholders synced via Desktop Connector. The pipeline:
1. Checks all input files for readability upfront
2. Detects ACC cloud stubs (empty files / `RECALL_ON_DATA_ACCESS` attribute)
3. Logs clear instructions: start Desktop Connector, wait for sync, re-run
4. Skips unreadable files and continues with the rest

---

## Tech Stack

| Library | Version | Replaces | Purpose |
|---------|---------|----------|---------|
| ifcopenshell | >=0.8.0 | BIMFileToGeodatabase | IFC parsing, 3D geometry extraction |
| trimesh | >=4.0 | Multipatch feature classes | Mesh representation, operations |
| open3d | >=0.19.0 | MultipatchToRaster | Raycasting meshes to height grids |
| rasterio | >=1.4.0 | arcpy.Raster, Clip, MosaicToNewRaster | Raster I/O (GeoTIFF read/write) |
| numpy | >=1.26 | arcpy.RasterToNumPyArray | All array operations |
| scipy | >=1.12 | LandXMLToTin, TinRaster, Expand | TIN interpolation, morphological ops |
| geopandas | >=1.0 | SearchCursor, PairwiseIntersect | Vector spatial operations |
| shapely | >=2.0 | Geometry objects | Geometry operations |
| pyproj | >=3.6 | arcpy.SpatialReference | CRS handling (EPSG:25832) |
| fiona | >=1.9 | GDB access | Vector file I/O (GeoPackage) |
| lxml | >=5.0 | LandXMLToTin (parsing) | XML parsing for LandXML |
| pandas | >=2.2 | pandas (same) | DataFrame operations |
| openpyxl | >=3.1 | Excel engine | Excel file writing |

### Install

```bash
pip install ifcopenshell trimesh open3d rasterio numpy scipy geopandas shapely pyproj fiona lxml pandas openpyxl
```

---

## Configuration (config.py)

All hardcoded paths and constants extracted to one file:

```python
# Input data paths
MODEL_FOLDER_PATH = r"C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Discipline models"
TERRAIN_PATH = r"C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Existing condition models (CORAV)\Terrengflater"
BERG_PATH = r"C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Existing condition models (CORAV)"

# Processing parameters
CELL_SIZE = 0.2              # 20cm raster resolution
CRS = "EPSG:25832"           # ETRS 1989 UTM Zone 32N
GRID_CELL_SIZE = 200         # 200m processing tiles
SPORSYSTEM_Z_OFFSET = -0.9   # Track system sinks 900mm

# Slope parameters
ROCK_SLOPE_FACTOR = 10.0     # Rock slope: rise = distance * 10.0
SOIL_SLOPE_DIVISOR = 1.5     # Soil slope: rise = distance / 1.5
BUFFER_DISTANCE = 1.0        # 1m buffer around berg excavation

# Density/conversion factors
ROCK_DENSITY = 0.7           # kg per m3 (loose rock)
SEDIMENT_DIESEL_FACTOR = 1.98 # liters diesel per m3 sediment
TUNNEL_ROCK_DENSITY = 1.8    # kg per m3 (tunnel rock)

# Clipping artifact threshold
MAX_TILE_DIMENSION = 250     # meters - tiles larger than this are artifacts

# Grid index file (convert AOI.gdb to GeoPackage first)
GRID_PATH = "SCRIPT_HELP_FILES/grid_index.gpkg"
MUNKEBOTN_MASK = "SCRIPT_HELP_FILES/munkebotn_mask.tif"
```

---

## Detailed Function Mapping: arcpy -> Open Source

### 1. IFC Import (`ifc_parser.py`)

**Original:** `arcpy.conversion.BIMFileToGeodatabase(ifc, gdb, name, sr, include_floorplan='EXCLUDE_FLOORPLAN')`

**Replacement:**
```python
import ifcopenshell
import ifcopenshell.geom
import ifcopenshell.util.shape
import trimesh
import numpy as np

def parse_ifc(ifc_path):
    """Parse IFC file and return list of trimesh meshes."""
    ifc_file = ifcopenshell.open(ifc_path)
    settings = ifcopenshell.geom.settings()
    meshes = []
    for element in ifc_file.by_type("IfcBuildingElement"):
        try:
            shape = ifcopenshell.geom.create_shape(settings, element)
            verts = ifcopenshell.util.shape.get_vertices(shape.geometry)
            faces = ifcopenshell.util.shape.get_faces(shape.geometry)
            mesh = trimesh.Trimesh(vertices=verts, faces=faces)
            meshes.append(mesh)
        except Exception:
            continue  # skip elements without geometry
    return meshes
```

**IFC file filtering logic** (from original `list_model_ifcs`):
- Include files containing: `fm_Veg`, `fm_VA`, `fm_FVG`, `fm_Ele`, `fm_Spo_Sporsystem`
- Exclude files containing: `_alt` (alternative versions)
- Tunnel files: contain `fm_Geo` and `sprengning.ifc`
- Berg files: contain `Antatt-bergoverflate`

**Multiprocessing:** Use `multiprocessing.Pool` with up to 12 cores, same as original. IfcOpenShell is process-safe.

**Sporsystem Z-adjustment:** After parsing track IFCs, subtract 0.9m from all Z vertices:
```python
mesh.vertices[:, 2] -= 0.9  # lower by 900mm
```

### 2. LandXML to Raster (`terrain_parser.py`)

**Original:** `arcpy.ddd.LandXMLToTin()` then `arcpy.ddd.TinRaster(method="LINEAR")`

**Replacement:**
```python
from lxml import etree
from scipy.interpolate import LinearNDInterpolator
import numpy as np

def parse_landxml(xml_path):
    """Parse LandXML and return vertices array (N,3) with x,y,z."""
    tree = etree.parse(xml_path)
    ns = {"lx": "http://www.landxml.org/schema/LandXML-1.2"}
    # Try both 1.2 and 1.1 namespaces
    points = {}
    for p in tree.findall(".//lx:P", ns):
        pid = int(p.attrib["id"])
        coords = list(map(float, p.text.split()))
        # LandXML uses northing, easting, elevation order
        points[pid] = [coords[1], coords[0], coords[2]]  # x=easting, y=northing, z=elev

    faces = []
    for f in tree.findall(".//lx:F", ns):
        face_ids = list(map(int, f.text.split()))
        faces.append(face_ids)

    return points, faces

def tin_to_raster(points, cell_size, bounds=None):
    """Interpolate TIN points to regular grid using linear interpolation."""
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
    y_grid = np.arange(ymax, ymin, -cell_size)  # top to bottom for raster convention
    xx, yy = np.meshgrid(x_grid, y_grid)
    dem = interp(xx, yy)
    return dem, (xmin, ymax)  # return array and upper-left origin
```

**LandXML filtering:** Files matching `*Terrengoverflate.xml` pattern.

### 3. Mesh to Raster - Raycasting (`rasterizer.py`)

**Original:** `arcpy.conversion.MultipatchToRaster(mp, out, cell_size, "MINIMUM_HEIGHT")`

**Replacement using Open3D raycasting:**
```python
import open3d as o3d
import numpy as np
import rasterio
from rasterio.transform import from_origin

def mesh_to_raster(mesh, cell_size, method="MINIMUM_HEIGHT"):
    """Rasterize trimesh mesh to 2D height grid via raycasting.

    method: "MINIMUM_HEIGHT" or "MAXIMUM_HEIGHT"
    - MINIMUM_HEIGHT: cast rays upward from below, get lowest hit
    - MAXIMUM_HEIGHT: cast rays downward from above, get highest hit
    """
    bounds = mesh.bounds  # [[xmin,ymin,zmin],[xmax,ymax,zmax]]
    xmin, ymin, zmin = bounds[0]
    xmax, ymax, zmax = bounds[1]

    # Create Open3D raycasting scene
    o3d_mesh = o3d.t.geometry.TriangleMesh()
    o3d_mesh.vertex.positions = o3d.core.Tensor(mesh.vertices.astype(np.float32))
    o3d_mesh.triangle.indices = o3d.core.Tensor(mesh.faces.astype(np.int32))
    scene = o3d.t.geometry.RaycastingScene()
    scene.add_triangles(o3d_mesh)

    # Build ray grid
    x = np.arange(xmin, xmax, cell_size)
    y = np.arange(ymax, ymin, -cell_size)
    xx, yy = np.meshgrid(x, y)

    if method == "MINIMUM_HEIGHT":
        # Rays shoot upward from below
        z_start = zmin - 10
        origins = np.column_stack([xx.ravel(), yy.ravel(), np.full(xx.size, z_start)])
        directions = np.tile([0, 0, 1], (xx.size, 1))
        rays = np.hstack([origins, directions]).astype(np.float32)
        result = scene.cast_rays(o3d.core.Tensor(rays))
        hit_dist = result['t_hit'].numpy()
        z_values = z_start + hit_dist
    else:  # MAXIMUM_HEIGHT
        z_start = zmax + 10
        origins = np.column_stack([xx.ravel(), yy.ravel(), np.full(xx.size, z_start)])
        directions = np.tile([0, 0, -1], (xx.size, 1))
        rays = np.hstack([origins, directions]).astype(np.float32)
        result = scene.cast_rays(o3d.core.Tensor(rays))
        hit_dist = result['t_hit'].numpy()
        z_values = z_start - hit_dist

    z_values[np.isinf(hit_dist)] = np.nan
    raster = z_values.reshape(xx.shape)
    return raster, (xmin, ymax)  # array + upper-left origin
```

**Writing GeoTIFF output:**
```python
def write_geotiff(array, origin, cell_size, crs, output_path):
    transform = from_origin(origin[0], origin[1], cell_size, cell_size)
    with rasterio.open(output_path, "w", driver="GTiff",
                       height=array.shape[0], width=array.shape[1],
                       count=1, dtype="float32", crs=crs,
                       transform=transform, nodata=np.nan) as dst:
        dst.write(array.astype(np.float32), 1)
```

### 4. Raster Clipping (`grid_processor.py`)

**Original:** `arcpy.management.Clip(raster, rectangle, out, nodata)`

**Replacement:**
```python
import rasterio
from rasterio.windows import from_bounds

def clip_raster(raster_path, bounds, output_path):
    """Clip raster to bounding box (xmin, ymin, xmax, ymax)."""
    with rasterio.open(raster_path) as src:
        window = from_bounds(*bounds, transform=src.transform)
        data = src.read(1, window=window)
        transform = src.window_transform(window)
        profile = src.profile.copy()
        profile.update(height=data.shape[0], width=data.shape[1], transform=transform)
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(data, 1)
    return data, transform
```

### 5. Grid Index (`grid_processor.py`)

**Original:** INDEX_GRID_200_overlap from AOI.gdb, accessed via `arcpy.da.SearchCursor`

**Pre-processing step:** Convert AOI.gdb to GeoPackage (one-time):
```python
# One-time conversion (or use ogr2ogr CLI)
import geopandas as gpd
gdf = gpd.read_file("SCRIPT_HELP_FILES/AOI.gdb", layer="INDEX_GRID_200_overlap")
gdf.to_file("SCRIPT_HELP_FILES/grid_index.gpkg", driver="GPKG")
```

**Usage:**
```python
import geopandas as gpd
from shapely.geometry import box

def get_intersecting_tiles(grid_path, model_bounds):
    """Return grid cells that intersect the model extent."""
    grid = gpd.read_file(grid_path)
    model_box = box(*model_bounds)
    intersecting = grid[grid.intersects(model_box)]
    return intersecting  # iterate rows, use .geometry.bounds for each tile
```

### 6. Slope Propagation BFS (`slope_propagation.py`)

**Original:** Custom BFS in `generate_berg_excavation()` and `generate_final_excavation()`

**This is pure numpy + collections.deque. Port directly from original** with minimal changes:

```python
from collections import deque
import numpy as np
from math import sqrt

def propagate_rock_slope(model_array, berg_array, cell_size):
    """BFS slope propagation for rock excavation (10:1 slope).

    Propagates excavation elevation from model cells outward through rock,
    rising by distance * 10.0 per cell step.
    """
    rows, cols = model_array.shape
    output = np.copy(model_array)
    in_queue = np.zeros(output.shape, dtype=bool)
    queue = deque()

    # Seed queue with all model cells (non-NaN)
    for r in range(rows):
        for c in range(cols):
            if not np.isnan(model_array[r, c]):
                queue.append((r, c))
                in_queue[r, c] = True

    neighbors = [
        (-1, 0, cell_size), (1, 0, cell_size),
        (0, -1, cell_size), (0, 1, cell_size),
        (-1, -1, cell_size * sqrt(2)), (-1, 1, cell_size * sqrt(2)),
        (1, -1, cell_size * sqrt(2)), (1, 1, cell_size * sqrt(2)),
    ]

    while queue:
        r, c = queue.popleft()
        current_elev = output[r, c]

        for dr, dc, dist in neighbors:
            nr, nc = r + dr, c + dc
            if 0 <= nr < rows and 0 <= nc < cols:
                berg_rise = dist * 10.0
                tent_elev = current_elev + berg_rise
                n_berg = berg_array[nr, nc]
                n_current = output[nr, nc]

                if (not np.isnan(n_berg) and
                    (np.isnan(n_current) or tent_elev < n_current) and
                    n_berg > tent_elev):
                    output[nr, nc] = tent_elev
                    if not in_queue[nr, nc]:
                        queue.append((nr, nc))
                        in_queue[nr, nc] = True

    return output

def propagate_soil_slope(model_array, berg_array, terrain_array, cell_size):
    """BFS slope propagation for soil/loam excavation (1:1.5 slope).

    Propagates from model cells into soil areas (where berg is NaN),
    rising by distance / 1.5 per cell step. Constrained by terrain elevation.
    """
    rows, cols = model_array.shape
    output = np.copy(model_array)
    in_queue = np.zeros(output.shape, dtype=bool)
    queue = deque()

    for r in range(rows):
        for c in range(cols):
            if not np.isnan(model_array[r, c]):
                queue.append((r, c))
                in_queue[r, c] = True

    neighbors = [
        (-1, 0, cell_size), (1, 0, cell_size),
        (0, -1, cell_size), (0, 1, cell_size),
        (-1, -1, cell_size * sqrt(2)), (-1, 1, cell_size * sqrt(2)),
        (1, -1, cell_size * sqrt(2)), (1, 1, cell_size * sqrt(2)),
    ]

    while queue:
        r, c = queue.popleft()
        current_elev = output[r, c]

        for dr, dc, dist in neighbors:
            nr, nc = r + dr, c + dc
            if 0 <= nr < rows and 0 <= nc < cols:
                rise = dist / 1.5
                tent_elev = current_elev + rise
                n_current = output[nr, nc]
                n_berg = berg_array[nr, nc]
                n_terrain = terrain_array[nr, nc]

                if ((np.isnan(n_current) or tent_elev < n_current) and
                    np.isnan(n_berg) and
                    tent_elev < n_terrain):
                    output[nr, nc] = tent_elev
                    if not in_queue[nr, nc]:
                        queue.append((nr, nc))
                        in_queue[nr, nc] = True

    return output
```

### 7. Buffer Expansion (`slope_propagation.py`)

**Original:** `arcpy.ia.Apply()` with Expand.rft.xml (1m buffer)

**Replacement:**
```python
from scipy.ndimage import binary_dilation, generate_binary_structure

def buffer_raster(raster_array, buffer_cells):
    """Expand non-NaN region by buffer_cells using morphological dilation."""
    mask = ~np.isnan(raster_array)
    struct = generate_binary_structure(2, 2)  # 8-connected
    dilated = binary_dilation(mask, structure=struct, iterations=buffer_cells)
    return dilated  # boolean mask of expanded region
```

Buffer distance = 1.0m, so `buffer_cells = int(1.0 / CELL_SIZE)` = 5 cells.

### 8. Raster Merging (`grid_processor.py`)

**Original:** `arcpy.ia.Merge(rasters, "MIN")` or `arcpy.management.MosaicToNewRaster()`

**Replacement:**
```python
from rasterio.merge import merge as rasterio_merge

def merge_rasters(raster_paths, output_path, method="min"):
    """Merge multiple rasters. method: 'first', 'last', 'min', 'max'."""
    import rasterio
    datasets = [rasterio.open(p) for p in raster_paths]

    if method == "min":
        mosaic, transform = rasterio_merge(datasets, method="min")
    else:
        mosaic, transform = rasterio_merge(datasets, method=method)

    profile = datasets[0].profile.copy()
    profile.update(height=mosaic.shape[1], width=mosaic.shape[2], transform=transform)

    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(mosaic)

    for ds in datasets:
        ds.close()
```

### 9. Volume Calculation (`volume_calc.py`)

**Original:** `arcpy.ddd.CutFill()` then summing RAT['VOLUME']

**Replacement:**
```python
import numpy as np
import rasterio

def calculate_volumes(before_path, after_path, cell_size):
    """Calculate cut/fill volumes between two co-registered rasters.

    Returns dict with volume values (positive = excavation/cut).
    """
    with rasterio.open(before_path) as src:
        before = src.read(1).astype(np.float64)
    with rasterio.open(after_path) as src:
        after = src.read(1).astype(np.float64)

    diff = before - after  # positive where material is removed (cut)
    cell_area = cell_size ** 2

    cut_volume = float(np.nansum(diff[diff > 0]) * cell_area)
    return cut_volume
```

**Volume outputs (matching original Excel columns):**
```python
terrain_vol = calculate_volumes(terrain_raster, final_result_raster, CELL_SIZE)
berg_vol = calculate_volumes(berg_raster, final_result_raster, CELL_SIZE)
sediment_vol = terrain_vol - berg_vol

results = {
    "VOL_BERG_DAGSONE_m3": terrain_vol,       # was confusingly named in original
    "VEKT_BERG_DAGSONE_kg": berg_vol * 0.7,
    "VOL_SEDIMENT_m3": sediment_vol,
    "VOL_SEDIMENT_DIESEL_LITER": sediment_vol * 1.98,
}
```

### 10. Tunnel Volume (`tunnel_vol.py`)

**Original:** Two MultipatchToRaster calls (MIN_HEIGHT, MAX_HEIGHT) then CutFill

**Replacement:**
```python
def calculate_tunnel_volume(tunnel_mesh, cell_size):
    """Calculate tunnel volume from mesh using min/max height difference."""
    raster_lo, origin = mesh_to_raster(tunnel_mesh, cell_size, "MINIMUM_HEIGHT")
    raster_hi, _ = mesh_to_raster(tunnel_mesh, cell_size, "MAXIMUM_HEIGHT")

    diff = raster_hi - raster_lo
    cell_area = cell_size ** 2
    tunnel_vol = float(np.nansum(diff[diff > 0]) * cell_area)
    tunnel_weight = tunnel_vol * 1.8  # rock density for tunnels
    return tunnel_vol, tunnel_weight
```

---

## Pre-processing: Convert AOI.gdb

The grid index is stored in an Esri File Geodatabase. Convert once using GDAL:

```bash
ogr2ogr -f GPKG SCRIPT_HELP_FILES/grid_index.gpkg SCRIPT_HELP_FILES/AOI.gdb INDEX_GRID_200_overlap
```

Or in Python with geopandas (requires GDAL with FileGDB driver):
```python
import geopandas as gpd
gdf = gpd.read_file("SCRIPT_HELP_FILES/AOI.gdb", layer="INDEX_GRID_200_overlap")
gdf.to_file("SCRIPT_HELP_FILES/grid_index.gpkg", driver="GPKG")
```

---

## Processing Flow (Complete)

```
runner.py
  │
  ├── 1. Parse inputs
  │   ├── List & filter IFC files (model, tunnel, berg)
  │   ├── Parse model IFCs → trimesh meshes (parallel, 12 cores)
  │   ├── Adjust Sporsystem Z-values: -0.9m
  │   ├── Parse tunnel IFCs → trimesh meshes
  │   ├── Parse berg IFCs → trimesh meshes
  │   └── Parse LandXML terrain files → TIN points
  │
  ├── 2. Rasterize
  │   ├── Model meshes → merged model raster (MINIMUM_HEIGHT, 0.2m)
  │   ├── Tunnel meshes → tunnel raster (for exclusion mask)
  │   ├── Berg meshes → berg raster (MINIMUM_HEIGHT, 0.2m)
  │   ├── Terrain TIN → terrain raster (LINEAR interpolation, 0.2m)
  │   └── Apply tunnel + munkebotn exclusion mask to model raster
  │
  ├── 3. Grid-based processing (per 200m tile)
  │   ├── Load grid index (GeoPackage)
  │   ├── Get model footprint polygon
  │   ├── Select intersecting grid cells
  │   └── For each tile:
  │       ├── Clip model, berg, terrain rasters
  │       ├── Validate tile dimensions (skip artifacts > 250m)
  │       ├── Filter model cells below berg elevation
  │       ├── Rock slope propagation (BFS, factor=10.0)
  │       ├── Buffer berg excavation (+1m / 5 cells)
  │       ├── Merge buffer with berg excavation
  │       ├── Merge with model raster
  │       └── Soil slope propagation (BFS, divisor=1.5)
  │
  ├── 4. Merge & calculate volumes
  │   ├── Merge all tiles → FINAL_RESULT_RASTER.tif
  │   ├── CutFill: terrain vs final → total volume
  │   ├── CutFill: berg vs final → rock volume
  │   ├── Sediment volume = total - rock
  │   └── Apply density/conversion factors
  │
  ├── 5. Tunnel volumes
  │   ├── Rasterize tunnel mesh (MIN + MAX height)
  │   ├── Volume = sum of (max - min) per cell
  │   └── Weight = volume * 1.8
  │
  └── 6. Output
      ├── Write volumes.csv
      ├── Write masseuttak_bb5.xlsx (sheet: "Mengder")
      ├── Save FINAL_RESULT_RASTER.tif
      ├── Save TERRAIN_MERGED_RASTER.tif
      └── Save BERG_MERGED_RASTER.tif
```

---

## Key Implementation Notes

### Raster Alignment
All rasters MUST be pixel-aligned. After creating the model raster, derive the grid origin and snap all subsequent rasters to it:
```python
# Use model raster transform as reference
with rasterio.open("model_raster.tif") as ref:
    ref_transform = ref.transform
    ref_crs = ref.crs
# All subsequent rasters must use compatible origin + cell_size
```

### NoData Handling
- Original uses `np.nan` for floating-point NoData and `3.4e+38` as raster NoData sentinel
- In open-source version: use `np.nan` consistently, set `nodata=np.nan` in rasterio profiles

### Coordinate System
- EPSG:25832 (ETRS 1989 UTM Zone 32N)
- LandXML may use northing/easting order (Y, X, Z) — swap when parsing
- IFC files may have local coordinates — check and transform with pyproj if needed

### Performance Considerations
- IFC parsing is CPU-bound: use multiprocessing.Pool (12 cores max)
- BFS slope propagation is the bottleneck for large areas: consider Cython or numba JIT
- Open3D raycasting uses BVH trees internally — very fast
- For very large rasters, process in tiles (already implemented via grid system)

### Differences from Original
- No File Geodatabase (.gdb) — use GeoTIFF for rasters, GeoPackage for vectors
- No ArcGIS extensions needed
- No AGOL publishing (separate concern, could use CesiumJS + py3dtiles later)
- Raster snapping must be handled explicitly (arcpy does it via env.snapRaster)

---

## Excel Output Format

Sheet name: **"Mengder"**

| Column | Description | Unit |
|--------|-------------|------|
| VOL_BERG_DAGSONE_m3 | Rock excavation volume (daylight/surface) | m3 |
| VEKT_BERG_DAGSONE_kg | Rock weight (vol * 0.7) | kg |
| VOL_SEDIMENT_m3 | Sediment/soil volume | m3 |
| VOL_SEDIMENT_DIESEL_LITER | Sediment diesel equivalent (vol * 1.98) | liters |
| VOL_BERG_TUNNEL_m3 | Tunnel rock volume | m3 |
| VEKT_BERG_TUNNEL_kg | Tunnel rock weight (vol * 1.8) | kg |

---

## Testing Strategy

1. **Unit tests:** Test each module independently with small synthetic data
2. **Integration test:** Run full pipeline on a small subset (1-2 grid tiles)
3. **Validation:** Compare output volumes against original arcpy pipeline results
4. **Regression:** Store reference Excel output for automated comparison

---

## Future: Web Publishing (Phase 2)

If AGOL replacement is needed later:
- **CesiumJS** for 3D web visualization
- **py3dtiles** / **py3dtilers** to convert meshes to OGC 3D Tiles format
- **GeoServer** or **Martin** (Rust) for serving tile layers
- Self-hosted or cloud-deployed (Docker)
