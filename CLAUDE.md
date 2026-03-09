# BB5 Mass Extraction — Project Instructions

## Do NOT modify
- `mass_calc.py` and `publish.py` — these are scbm's original production scripts. Never edit them.

## Production pipeline
- **`mass_calc_v3.py` / `runner_v3.py`** is the recommended pipeline (v2 parallelism + deep model filter + performance tiers)
- Run via `propy.bat` (ArcGIS Python), not standard Python
- Weekly automated run: Saturdays 02:00 via Windows Scheduled Task

## Key conventions
- CRS: EPSG:25832, cell size 0.2m
- ArcGIS Pro 3.6 with Spatial + 3D Analyst licenses
- Pipeline server (`server/`) uses standard Python 3.11, not ArcGIS Python
- AGOL credentials stored via `keyring` in Windows Credential Manager
- `server/config.json` is gitignored (contains Azure app secrets)

## File organization
- `*_legacy.py` = scbm's code with updated paths
- `*_fixed.py` = scbm's code + deep model filter
- `*_v2.py` = parallelized pipeline
- `*_v3.py` = v2 + deep model filter (production)
- `server/` = SharePoint-triggered pipeline server
- `deep_model_filter.py` = standalone filter module
- `tunnel_fix_strategies.py` = experimental strategies (Strategy E on hold)

## Testing
- Use `--test` flag to process only 10 IFC files
- Use `--sequential` for single-threaded debugging
- Use `--no-filter` to compare with/without deep model filter
- Publish to `optimized`/`legacy`/`fixed` folders for comparison; only `production` overwrites live data
