---
title: Bayanat Arab Trade Exposure
emoji: 🌊
colorFrom: green
colorTo: yellow
sdk: static
app_file: index.html
license: mit
---

Read-only explorer for [Bayanat Arab Trade Exposure](https://huggingface.co/datasets/Bayanat-Labs/arab-trade-exposure), derived independently from CEPII BACI HS92 V202601.

This static app runs DuckDB-Wasm in your browser and downloads only the selected annual flow file and its two annual derived tables. The 2024 files total about 79 MB, plus the one-time browser engine download. Files from the previous year are evicted when the year changes. It uses the pinned dataset tag `v0.1.0`. No API key, arbitrary SQL, external tracking, customer uploads or model inference.

Static local preview: `python -m http.server 8773 --directory .`. The bundled Python backend remains an optional local reference, not the hosted application.

Values are CEPII reconciled bilateral trade estimates. Source `v` is thousands of current USD; source `q` is metric tonnes. Display rounding does not change the downloadable exact strings. Concentration describes recorded positive trade flows. It does not establish disruption risk, import dependence relative to domestic consumption, retail prices, or forecasts. Absent flows are not proven zero. The latest year may be revised.

The geographic collection comprises 22 modern Arab League members and source code 736 for former Sudan as a separate historical area. Original BACI source identities remain separate. Product names are the original source labels; this app does not invent official Arabic product nomenclature. Legacy non-WCO product code 9999AA remains flagged.

Source: CEPII, BACI HS92 V202601, updated 22 January 2026. [Documentation](https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html). Data and lookup reuse follows [Etalab Open Licence 2.0](https://raw.githubusercontent.com/etalab/licence-ouverte/master/open-licence.txt), with source attribution and modifications disclosed. App code: MIT. CEPII does not endorse Bayanat or this app.

Browser engine: official `@duckdb/duckdb-wasm@1.32.0`, bundled with `apache-arrow@17.0.0` using `esbuild@0.28.2`; matched single-threaded EH worker/Wasm files are vendored locally. Third-party license notices are in `assets/vendor/`. No server compute or paid hardware is required.

Rebuild the pinned browser bundle with the lockfile and script in `vendor-build/`; see its README. `assets/vendor/provenance.json` records official npm integrity and generated file SHA-256 hashes.
