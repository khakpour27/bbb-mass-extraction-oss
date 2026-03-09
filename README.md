# BB5 Masseberegning

Beregning av masseuttak basert på laveste IFC-høyde for disiplinmodeller i Bergen Bybanen BT5. Graveskråningene har helning bestemt av massetype (berg 10:1, løsmasse 2:3), styrt av IFC-modeller for antatt bergoverflate.

## Hurtigstart

```bash
# Kjør v3-pipeline (anbefalt produksjon)
propy runner_v3.py                                           # Moderate (standard)
propy runner_v3.py --aggressive                              # Maks parallellisering
propy runner_v3.py --publish --publish-target production     # Kjør + publiser til produksjon
propy runner_v3.py --test                                    # Testmodus (10 IFC-filer)
propy runner_v3.py --no-filter                               # Uten deep model filter
propy runner_v3.py --sequential                              # Enkeltrådet (debug)
```

## Pipeline-varianter

| Fil | Beskrivelse |
|-----|-------------|
| `mass_calc.py` / `publish.py` | Produksjonskode (scbm). **Ikke modifiser.** |
| `mass_calc_legacy.py` / `publish_legacy.py` | scbm's kode med oppdaterte filstier |
| `mass_calc_fixed.py` / `publish_fixed.py` | scbm's kode + deep model filter |
| `mass_calc_v2.py` / `publish_v2.py` | Optimalisert pipeline (parallellisert, NumPy) |
| **`mass_calc_v3.py` / `runner_v3.py`** | **Produksjonsanbefalt**: v2 + deep model filter + ytelsesnivå |

### Støttemoduler

| Fil | Beskrivelse |
|-----|-------------|
| `deep_model_filter.py` | Detekterer infrastrukturmodeller som går dypt uten tunnel |
| `tunnel_fix_strategies.py` | Pluggbare tunnel-fix strategier (A, B, D, E). Strategy E on hold. |
| `tunnel_vol.py` | Tunnelvolumberegning (kjøres av runner etter mass_calc) |

## Deep Model Filter

`deep_model_filter.py` oppdager og ekskluderer infrastrukturmodeller (Veg, FVG, Spo) som går dypt under bakken uten tilhørende tunnel. VA-modeller (vann/avløp) sjekkes ikke. Filteret bruker `MultiPatchFootprint` for faktisk tunnel-geometri og sentroid punkt-i-polygon for dype features.

```python
from deep_model_filter import filter_deep_orphan_models
excluded = filter_deep_orphan_models(bim_mps, tunnel_mps, depth_threshold=5.0)
```

## Ytelsesnivå (v3)

| Nivå | IFC workers | Grid workers | Validate workers | Typisk tid |
|------|-------------|-------------|------------------|------------|
| `--moderate` (standard) | 12 | 12 | 8 | ~37 min |
| `--aggressive` | 24 | 20 | 16 | ~25 min |
| `--sequential` | 1 | 1 | 1 | ~120 min |

## Volumsammenligning

| Kjøring | Berg (m³) | Sediment (m³) | Merknad |
|---------|-----------|---------------|---------|
| scbm baseline | 329 936 | 2 045 014 | Referanse |
| Legacy ufixed | 1 362 175 | — | Falsk gravning over tunneler |
| Fixed + Strategy E | 12 070 | 1 766 950 | Overkorrigerer |
| **Fixed, kun filter** | **245 079** | **1 231 379** | Nærmest scbm |
| **v3 moderate** | **290 259** | **1 284 315** | Produksjonskjøring |
| v3 uten filter | 1 450 282 | 6 510 563 | Bekrefter filterbehov |

## Automatiserte kjøringer (Scheduled Tasks)

Tre Windows Scheduled Tasks er satt opp på PC04355:

| Task | Trigger | Status | Beskrivelse |
|------|---------|--------|-------------|
| **BB5 Pipeline Server** | Ved oppstart | Aktiv | Poller SharePoint for `trigger.json` hvert 15. sek |
| **BB5 Weekly Pipeline Run** | Lørdag 02:00 | Aktiv | `runner_v3.py --moderate --publish --publish-target production` |
| ~~bb5_masseberegning~~ | Lørdag 02:23 | Deaktivert | scbm's originale task (deaktivert 9. mars 2026) |

Oppsett av tasks krever admin-rettigheter:
```powershell
# Fra en forhøyet (Run as Administrator) PowerShell:
powershell -ExecutionPolicy Bypass -File "server\install_task.ps1"
```

Etter passordendring på Administrator-konto må tasks re-registreres med nytt passord.

## Pipeline Server (SharePoint-triggered)

Mappen `server/` inneholder en pollingbasert server som overvåker SharePoint via Microsoft Graph API.

### Komponenter

| Fil | Beskrivelse |
|-----|-------------|
| `server/pipeline_server.py` | Hovedserver: poller trigger.json, oppretter kjøringsmappe, strømmer manifest + logg |
| `server/graph_client.py` | Graph API-klient (MSAL client credentials flow) |
| `server/config.json` | Azure-app + SharePoint-konfigurasjon (gitignored) |
| `server/start_server.bat` | Starter serveren som bakgrunnsprosess |
| `server/install_task.ps1` | Registrerer begge Scheduled Tasks (krever admin) |
| `server/templates/` | Maler for trigger.json og manifest.json med hjelpetekst |

### SharePoint-mappestruktur

```
pipeline_runs/
    templates/
        trigger.json                <- Mal med alle parametere og hjelpetekst
        manifest.json               <- Mal for manifest med alle felter
    trigger.json                    <- Power Automate oppretter denne for å starte kjøring
    RUN_20260308_143000/
        manifest.json               <- Status, fase, fremdrift, volumer, advarsler (live)
        log.txt                     <- Full pipeline-output (live)
```

### Trigger-parametere

```json
{
  "pipeline": "v3",
  "tier": "moderate",
  "publish": true,
  "publish_target": "production",
  "no_filter": false,
  "test": false
}
```

| Parameter | Verdier | Standard | Beskrivelse |
|-----------|---------|----------|-------------|
| `pipeline` | `v3`, `legacy`, `fixed` | `v3` | Pipeline-variant |
| `tier` | `moderate`, `aggressive`, `sequential` | `moderate` | Parallelliseringsnivå |
| `publish` | `true`, `false` | `false` | Publiser til AGOL etter kjøring |
| `publish_target` | `auto`, `optimized`, `legacy`, `fixed`, `production` | `auto` | AGOL-mappe |
| `no_filter` | `true`, `false` | `false` | Hopp over deep model filter |
| `test` | `true`, `false` | `false` | Testmodus (kun 10 IFC-filer) |

### AGOL Publish targets

| Target | Publisher | AGOL-mappe |
|--------|----------|------------|
| `optimized` | `publish_v2.py` | Parametrisk masseuttak (optimized) |
| `legacy` | `publish_legacy.py` | Parametrisk masseuttak (legacy) |
| `fixed` | `publish_fixed.py` | Parametrisk masseuttak (fixed) |
| `production` | `publish.py` | Parametrisk masseuttak (produksjon) |

### Logging (v3)

- Alle modeller listes med `[1/51] filnavn (størrelse)` under lasting
- Advarsler om negative Z-verdier (dype modeller uten tunnel)
- Volumavvik-varsler mot baseline (scbm)
- Kun tile-outliers logges (>2x avg eller tregest 5), ikke alle 134 tiles
- Manifest oppdateres hvert 5. sek, logg hvert 10. sek

## Avhengigheter og krav

- **ArcGIS Pro 3.6** med Spatial Analyst og 3D Analyst lisenser
- Python-miljø: `arcgispro-py3` via `propy.bat`
- Pipeline-server: Standard Python 3.11 med `msal` og `requests`
- AGOL-passord via `keyring`:
  ```python
  import keyring
  keyring.set_password("bybanen_agol", "ADM_COWI", "<passord>")
  ```

## Kjente begrensninger

- **Volumgap**: v3 berg (290K) er ~12% under scbm baseline (330K). Sannsynlig årsak: ekskludering av hele Sporsystem DS1 fjerner også noe legitim overflategeometri.
- **Strategy E**: On hold — overkorrigerer tunnelvolumer.

## Endringer som vil påvirke skriptene

- Endringer til filstier i SharePoint og ACC
- Endringer til brukernavn/passord for Bybanen AGOL
- Lisensendringer til ArcGIS Pro
- Oppdateringer av `arcpy` og `arcgis` Python-pakker
- Passordendring på Administrator-konto (krever re-registrering av Scheduled Tasks)
