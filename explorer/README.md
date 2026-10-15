---
title: Bayanat Arab Trade Exposure
emoji: 🌊
colorFrom: green
colorTo: yellow
sdk: docker
app_port: 7860
license: mit
---

Read-only explorer for [Bayanat Arab Trade Exposure](https://huggingface.co/datasets/Bayanat-Labs/arab-trade-exposure), derived independently from CEPII BACI HS92 V202601.

This app loads one selected annual flow file and its small annual derived tables, cached locally. It uses the pinned dataset tag `v0.1.0`. No API key, arbitrary SQL, external tracking, customer uploads or model inference.

Local preview: `python app.py --local-data /absolute/path/to/release --port 7860`.

Values are CEPII reconciled bilateral trade estimates. Source `v` is thousands of current USD; source `q` is metric tonnes. Display rounding does not change the downloadable exact strings. Concentration describes recorded positive trade flows. It does not establish disruption risk, import dependence relative to domestic consumption, retail prices, or forecasts. Absent flows are not proven zero. The latest year may be revised.

The geographic collection comprises 22 modern Arab League members and source code 736 for former Sudan as a separate historical area. Original BACI source identities remain separate. Product names are the original source labels; this app does not invent official Arabic product nomenclature. Legacy non-WCO product code 9999AA remains flagged.

Source: CEPII, BACI HS92 V202601, updated 22 January 2026. [Documentation](https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html). Data and lookup reuse follows [Etalab Open Licence 2.0](https://raw.githubusercontent.com/etalab/licence-ouverte/master/open-licence.txt), with source attribution and modifications disclosed. App code: MIT. CEPII does not endorse Bayanat or this app.
