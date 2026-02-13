# BB5 Masseberegning

Denne skripten beregner masseuttak basert på laveste IFC høyde for disiplinmodeller i Bergen bybanen. Graveskråningene har en helning bestemt av massetype, styrt av IFC modeller for antatt bergoverflate. Områder i berg har en brattere helning på 10:1 (rise:run) og områder utenfor bergoverflater antas å være løsmasse med en helning på 2:3. 

Prosesseringen i filen `mass_calc.py` er rasterbasert og skripten skriver både rasterfiler og multipatchresultater som output. I tillegg er statistikk på massene skrevet ut som .xlsx og kopiert til Sharepoint. Output inkluderer historikk for de 2 siste kjøringene. Eldre output blir slettet automatisk. 

Volumer for tunnel beregnes i filen `tunnel_vol.py` og resultatet blir lagt til `bb5_masseuttak.xlsx` som ligger i output mappen for gjeldende kjøring. 

Resultatene publiseres automatisk videre til en 3D webscene på Bergen Bybanens sin ArcGIS Online. Denne prosesseringen skjer i filen `publish.py` Oppdateringer til Web Scenen skjer automatisk og eldre versjoner av publiserte filer blir slettet.

# Avhengigheter og krav

Skripten er laget til å kjøre på ArcGIS Pro sin default pythonmiljø `arcgispro-py3` ved Pro versjon `3.6.0`. Denne finner du på maskiner der ArcGIS Pro er installert under filstien `C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe`

`publish.py` benytter `keyring` til å lagre passordinformasjon i Windows Credential Manager. Ved nytt oppsett på nye maskiner må man passe på å legge til passordinfo med keyring. Dette gjøres ved å kjøre følgende pythonkode i `arcpypro-py3` interactive terminal. 
```
 import keyring 

 keyring.set_password("bybanen_agol", DESIRED_USERNAME_HERE, DESIRED_PASSORD_HERE)
```

Rutinen er egnet til å kjøre som batchfil. Dette settes opp som Windows Scheduled Task. Pass på at brukeren som kjører rutinen har rettigheter til å kjøre Scheduled Tasks uten login. 

Videre endringer som vil påvirke skriptet:
* Endringer til filstier i Sharepoint og ACC. Disse må oppdateres i alle skript dersom det forekommer endringer i mappestruktur
* Endringer til brukernavn og passord for Bybanen AGOL
* Evt. lisensendringer til ArcGIS Pro. Skriptet trenger både Spatial og 3D analyst til processering med `arcpy`
* Evt. endringer til `arcpy` og `arcgis` ved oppdateringer av ArcGIS Pro. 




