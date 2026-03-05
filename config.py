"""Configuration constants and paths for the BBB mass extraction pipeline."""

import os

# ── Input data paths ──────────────────────────────────────────────────────────
MODEL_FOLDER_PATH = (
    r"C:\Users\MHKK\DC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03"
    r"\Project Files\03_Shared (non-contractual)\Discipline models"
)
TERRAIN_PATH = (
    r"C:\Users\MHKK\DC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03"
    r"\Project Files\03_Shared (non-contractual)"
    r"\Existing condition models (CORAV)\Terrengflater"
)
BERG_PATH = (
    r"C:\Users\MHKK\DC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03"
    r"\Project Files\03_Shared (non-contractual)"
    r"\Existing condition models (CORAV)"
)

# ── Processing parameters ─────────────────────────────────────────────────────
CELL_SIZE = 0.2              # 20 cm raster resolution
CRS = "EPSG:25832"           # ETRS 1989 UTM Zone 32N
GRID_CELL_SIZE = 200         # 200 m processing tiles
SPORSYSTEM_Z_OFFSET = -0.9   # Track system sinks 900 mm

# ── Slope parameters ──────────────────────────────────────────────────────────
ROCK_SLOPE_FACTOR = 10.0     # Rock slope: rise = distance * 10.0
SOIL_SLOPE_DIVISOR = 1.5     # Soil slope: rise = distance / 1.5
BUFFER_DISTANCE = 1.0        # 1 m buffer around berg excavation

# ── Density / conversion factors ──────────────────────────────────────────────
ROCK_DENSITY = 0.7           # kg per m³ (loose rock)
SEDIMENT_DIESEL_FACTOR = 1.98  # litres diesel per m³ sediment
TUNNEL_ROCK_DENSITY = 1.8   # kg per m³ (tunnel rock)

# ── Clipping artefact threshold ───────────────────────────────────────────────
MAX_TILE_DIMENSION = 250     # metres — tiles larger than this are artefacts

# ── Support files ─────────────────────────────────────────────────────────────
SCRIPT_HELP_DIR = os.path.join(os.path.dirname(__file__), "SCRIPT_HELP_FILES")
GRID_PATH = os.path.join(SCRIPT_HELP_DIR, "grid_index.gpkg")
MUNKEBOTN_MASK = os.path.join(SCRIPT_HELP_DIR, "munkebotn_mask.tif")

# ── Multiprocessing ───────────────────────────────────────────────────────────
MAX_CORES = 24               # Use all available threads (i9-12900: 16C/24T)

# ── File limits (0 = no limit) ───────────────────────────────────────────────
MAX_MODEL_FILES = 0          # Limit number of model IFC files processed

# ── Test mode file selection ─────────────────────────────────────────────────
TEST_AREA_PREFIX = ""        # e.g. "E03_011"; empty = auto-detect best section

# ── Benchmark / legacy pipeline ──────────────────────────────────────────────
LEGACY_PYTHON_PATH = (
    r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
)

# ── Geographic extent validation (UTM32N Bergen bounds) ──────────────────────
EXTENT_X_MIN = 200_000
EXTENT_X_MAX = 400_000
EXTENT_Y_MIN = 6_600_000
EXTENT_Y_MAX = 6_800_000
