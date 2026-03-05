# Continue: BB5 Mass Extraction v2 Validation

## What was done
- Created `mass_calc_v2.py`, `runner_v2.py`, `publish_v2.py` — optimized pipeline (3.3x faster)
- Fixed **Sporsystem Z-offset bug**: original code applied `Adjust3DZ(-0.9)` to cached GDBs, causing cumulative -0.9m per run. v2 copies to scratch first.
- Added `--prefix` flag to filter IFC files by prefix (e.g. `--prefix F03`)
- Deleted corrupted Sporsystem cache GDBs from `ifc_cache/` and cleaned `manifest.json`
- Published v2 results to AGOL folder "Parametrisk masseuttak (optimized)" for comparison

## What needs to be done NOW

1. **Verify ACC file connector** has all F03_ berg files synced (the previous PC was missing F03_020, F03_023-027 berg files). Check:
   ```
   dir "C:\ADC\ACCDocs\COWI ACC EU\A240636 - Bergen Bybane BT5 E03\Project Files\03_Shared (non-contractual)\Existing condition models (CORAV)\F03_*berg*"
   ```
   Also check `C:\Users\MHKK\DC\ACCDocs\...` if the first path doesn't have them. The data path in `mass_calc_v2.py` may need updating.

2. **Run comparison with scbm's exact file scope** (F03-only):
   ```
   "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" mass_calc_v2.py --prefix F03 --workers 12
   "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" tunnel_vol.py
   ```

3. **Compare volumes** with scbm's baseline (his Feb 21 run, 51 F03_ models):
   - Berg: 329,936 m3
   - Sediment: 2,045,014 m3
   - Tunnel: 599,056 m3

4. **Compare visuals** — publish with `publish_v2.py` and compare scene layers with scbm's: `https://bybanen.maps.arcgis.com/home/item.html?id=863993e4bc1d4329a8944a28fa7ecb7e`

5. **After validation passes**, consider removing `fm_Ele` from model filter (line with `substrings = [...]` in mass_calc_v2.py) to match the project spec.

## Key context
- AGOL credentials: `keyring.set_password("bybanen_agol", "ADM_COWI", "Inger Bang Lunds vei 4")`
- CRS: EPSG:25832, cell size 0.2m, 143 grid tiles
- Repo: `git clone https://github.com/khakpour27/bbb-mass-extraction-oss.git` branch `dev/v2-optimized`
- Memory notes: `~/.claude/projects/C--Users-MHKK/memory/bbb_mass_extraction.md`
