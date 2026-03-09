# Continue: BB5 Mass Extraction — v3 Pipeline + Server

## Current State (March 9, 2026)

### What was done
- Created `mass_calc_v2.py`, `runner_v2.py`, `publish_v2.py` — optimized pipeline (3.3x faster)
- Fixed **Sporsystem Z-offset bug**: original code applied `Adjust3DZ(-0.9)` to cached GDBs, causing cumulative -0.9m per run. v2+ copies to scratch first.
- Created **deep model filter** (`deep_model_filter.py`) — detects and excludes infrastructure models that go deep underground without corresponding tunnel coverage
- Created `mass_calc_fixed.py` — scbm's legacy code + deep model filter integration
- Created `tunnel_fix_strategies.py` — pluggable tunnel fix strategies (A, B, D, E). Strategy E on hold (overcorrects).
- Published results to 3 separate AGOL comparison folders (non-destructive)
- **Created `mass_calc_v3.py` / `runner_v3.py`** — combines v2 parallelism + deep model filter + performance tiers
- **Created pipeline server** (`server/`) — SharePoint-triggered pipeline execution via Graph API
- **Improved logging** — model-by-model listing, volume discrepancy warnings, tile outlier filtering
- **Extended trigger schema** — pipeline variant, publish target, test mode, full help text
- **Set up Windows Scheduled Tasks** (March 9):
  - **BB5 Pipeline Server**: runs at startup, polls SharePoint for trigger.json
  - **BB5 Weekly Pipeline Run**: Saturdays 02:00, `runner_v3.py --moderate --publish --publish-target production`
  - **Disabled scbm's `bb5_masseberegning`** task to avoid conflicts (was Saturdays 02:23)
  - Tasks registered under `PC04355\Administrator`, installer at `server\install_task.ps1`

### mass_calc_v3.py — Production Pipeline
Key changes vs v2:
1. **Deep model filter integrated**: Tunnel IFCs imported BEFORE model rasterization. Filter runs between import and merge, modifying `bim_mps` in-place.
2. **Performance tiers**: `--moderate` (12/12/8 workers), `--aggressive` (24/20/16), `--sequential`
3. **Split worker counts**: `NUM_WORKERS_IFC`, `NUM_WORKERS_GRID`, `NUM_WORKERS_VALIDATE`
4. **`--no-filter` flag**: Skip deep model filter for comparison runs
5. **Configurable publish target**: `--publish-target` (optimized/legacy/fixed/production)

Usage:
```bash
python runner_v3.py                                          # Moderate (default)
python runner_v3.py --aggressive                             # Max parallelism
python runner_v3.py --publish                                # Publish to optimized folder
python runner_v3.py --publish --publish-target production     # Publish to production
python runner_v3.py --no-filter                              # Skip filter for comparison
```

### Volume Comparison
| Run | Berg (m3) | Sediment (m3) | Notes |
|-----|-----------|---------------|-------|
| scbm baseline | 329,936 | 2,045,014 | Reference |
| Legacy unfixed | 1,362,175 | — | False excavation above tunnels |
| Fixed + Strategy E | 12,070 | 1,766,950 | Strategy E overcorrects — ON HOLD |
| **Fixed, filter only** | **245,079** | **1,231,379** | Deep model filter only |
| **v3 moderate + publish** | **290,259** | **1,284,315** | v3, 37.1 min |
| v3 no-filter (server) | 1,450,282 | 6,510,563 | Confirms filter importance |

## What needs to be done

1. **Investigate remaining volume gap**: v3 berg (290K) is 12% below scbm (330K). Possible causes:
   - Excluding entire Sporsystem DS1 removes some legitimate surface geometry
   - Could partially exclude only the deep features instead of the whole model

2. **Strategy E refinement** (on hold): Overcorrects — may be fixed by adjusting depth threshold.

3. **Power Automate / Power Apps**: User will build trigger UI and monitoring dashboards.

## Key context
- Hostname: PC04355, user: MHKK
- ArcGIS Pro 3.6, CRS EPSG:25832, cell size 0.2m, 143 grid tiles
- AGOL credentials: `keyring.set_password("bybanen_agol", "ADM_COWI", "Inger Bang Lunds vei 4")`
- Run via: `propy.bat` or `"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"`
- Pipeline server: `"C:\Program Files\Python311\python.exe"` (standard Python, not ArcGIS)
- SharePoint folder: `60-WIP/20-Arbeidsomrade disipliner/10-21 GIS/masseuttak_ikke_slett/pipeline_runs`
