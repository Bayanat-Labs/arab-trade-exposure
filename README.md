# Bayanat Arab Trade Exposure

**33,517,452 Arab-region trade flow records · 1995–2024 · 3,770,939 country/product/year exposure observations.**

Analyze which countries supply a product to an Arab market, where Arab exports go, how concentrated those relationships are, and how recorded trade links change over time. Values retain exact decimal precision and every flow resolves to its original source archive member and data-row ordinal.

English is the default language for this release, its research documentation and the explorer. Arabic is available as an optional interface language.

[Open the explorer](https://huggingface.co/spaces/Bayanat-Labs/arab-trade-exposure) · [Source and pipeline](https://github.com/Bayanat-Labs/arab-trade-exposure) · [Worked examples](docs/USE_CASES.md) · [Methodology](docs/METHODOLOGY.md) · [Validation](reports/validation.json)

## What is included

The pipeline scanned **269,894,500 worldwide source rows** from one fixed CEPII BACI vintage and retained flows with an endpoint in the declared Arab-region collection. CEPII performed the original trade reconciliation. Bayanat adds a reproducible regional analytical layer, complete observed-partner denominators, conservative missingness and geographic handling, and practical SQL queries.

| Table / Hugging Face config | Rows | Parquet size |
|---|---:|---:|
| `flows` | 33,517,452 | 1,729.8 MiB |
| `exposures` | 3,770,939 | 310.4 MiB |
| `global_context` | 146,967 | 15.1 MiB |
| `link_observation_changes` | 21,630,858 | 154.4 MiB |
| `countries` | 238 | 0.0 MiB |
| `products` | 5,022 | 0.1 MiB |

`train` is a Hub loading split for a descriptive collection; this release has no ML train/test split or benchmark labels. Flow rows, derived market observations and recorded-link changes have different grains and must not be added as independent purchases. No synthetic trade records were generated.

The scope contains the **22 modern Arab League member geographies plus former Sudan (BACI736) as separately labeled historical context**. Modern Sudan 729 and former Sudan 736 retain separate identities. The collection does not reconstruct historical legal membership. Data coverage differs across source areas and years; an absent record is not confirmed zero trade.

## What people can use it for

- Research import-source concentration for a particular country and HS92 product, with all recorded partners in the denominator.
- Review export-destination concentration and intra-region trade shares.
- Compare national import-source concentration with worldwide exporter concentration for the same product/year.
- Study persistent concentration and newly/no-longer observed bilateral links without inventing zeros or market-entry claims.
- Build trade dashboards, analytical tools and data-engineering examples using year-partitioned Parquet and auditable source traces.

The measures describe recorded merchandise trade. They do **not** measure domestic import dependence, shortage probability, producer concentration, spare capacity, supplier replaceability, company transactions or retail prices. Trade hubs and re-exports matter. Annual values are nominal, reconciled estimates and can be revised upstream.

## Load one year

```python
from datasets import load_dataset

flows = load_dataset(
    "Bayanat-Labs/arab-trade-exposure", "flows", split="train", streaming=True,
    data_files={"train": "data/flows/year=2024/*.parquet"},
    revision="v0.1.0",
)
row = next(iter(flows))
print(row["hs6"], row["trade_value_1000usd"])  # Decimal; source value in thousands USD
```

For a precise local query without downloading all years:

```python
import duckdb
from huggingface_hub import hf_hub_download

p = hf_hub_download("Bayanat-Labs/arab-trade-exposure", "data/exposures/year=2024/part-00000.parquet",
                    repo_type="dataset", revision="v0.1.0")
con = duckdb.connect()
result = con.execute("SELECT hs6, total_value_usd, top1_share, hhi FROM read_parquet(?) "
                     "WHERE country_code=818 AND direction='import' ORDER BY total_value_usd DESC LIMIT 20", [p]).fetchall()
```

Six executable [SQL examples](queries/) cover market coverage, partner evidence, persistent concentration, world context, regional mix and recorded-link changes. They use explicit country, direction, product and year boundaries. Source product descriptions are English; curated Arabic display labels cover regional source areas. These labels are not official Arabic HS translations.

## Precision, provenance and quality

`v_raw` and `q_raw` retain parsed original CSV strings. `trade_value_1000usd` and `quantity_tonnes` are lossless DECIMAL(38,18); quantities marked NA/blank remain missing and reported zero quantities remain zero. No currency conversion or imputation is applied. Dollar totals multiply the native thousands-USD value by exactly 1000. Shares and HHI use double precision with a documented validation tolerance.

Exact storage preserves the publisher's numeric representation, including any upstream precision artifacts. It does not imply that economic measurement is accurate to 18 decimal places.

Original BACI country identities are authoritative; they are not interchangeable with numeric ISO codes. HS product strings retain leading zeros. The source-listed legacy non-WCO code `9999AA` remains flagged `product_is_standard_hs6=false`; it is not forced into a numeric HS hierarchy.

That flag checks source-listed six-digit numeric code shape, not external WCO certification. The product lookup separately flags unspecified catalog entries `999999` and `9999AA`. Neither occurs in the regional flow records of this vintage.

The country lookup supplies four explicitly documented display-label encoding repairs (including `TÃ¼rkiye`→`Türkiye`). Original source country labels remain unchanged in flow/exposure records. [Repair map](metadata/country_label_repairs.csv).

Every source ZIP member is fully read with CRC verification. Per-year completion reports bind source archive SHA-256, scope/code versions and output checksums. Invalid critical values, unknown source identities, duplicate native keys and incomplete inputs block a complete-year release. The full validation recomputes market denominators, worldwide totals, arithmetic bounds and link-presence changes independently. It also checks 150 original CSV source traces and 300 Python Decimal market calculations across 30 years. Those are technical prefix checks, not random expert verification of the source statistics.

All 18 semantic, failure-path and validator-corruption tests pass. The release audit also independently recomputes every local market and world-product concentration from the complete data.

For source drill-down, use `pipeline/trace.py` with a flow's `source_member`, `source_record_ordinal` and `source_archive_sha256`. The raw 2.42 GB archive is publicly downloadable from CEPII and is not duplicated here. [Machine-readable source metadata](metadata/source.json) · [Field schemas](metadata/field_schemas.json) · [Derived-field dictionary](metadata/derived_data_dictionary.json) · [Yearly source coverage](reports/source_coverage_by_year.json).

## Reproduce

Clone the [pipeline repository](https://github.com/Bayanat-Labs/arab-trade-exposure), install `requirements.txt`, then run:

```bash
python pipeline/download.py --output /path/to/build
python pipeline/ingest.py --archive /path/to/build/private/BACI_HS92_V202601.zip --output /path/to/build --country-scope country_scope.csv --archive-sha256 43259580059fa83e17fcdb6866e7b7f39aaf0dd50277cfe494a1ed05a4844faf
python pipeline/derive.py --output /path/to/build --years 1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024
python pipeline/validate.py --output /path/to/build --archive /path/to/build/private/BACI_HS92_V202601.zip
```

Processing extracts one annual CSV at a time and uses bounded DuckDB memory/disk spill. A completed source year can resume only when its bound inputs and output checksums still match. A rebuilt Parquet may have a different row order/byte hash; native record identities and numerical content are the reproducibility contract.

## Attribution and license

Source: CEPII, BACI database, HS92, version 202601; archive release date 22 January 2026. Downloaded 2026-10-15. Reused under Etalab Open Licence 2.0. Bayanat Labs selected source areas, added display metadata and computed analytical tables. CEPII does not endorse this derived release.

[BACI documentation](https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html) · [Full data license](DATA_LICENSE.txt). The presentation page's 30 January 2026 update date is separately preserved; it is not the observation year or our download date. Software is [MIT licensed](CODE_LICENSE.txt).

Please cite Gaulier, G. and Zignago, S. (2010), *BACI: International Trade Database at the Product-Level. The 1994–2007 Version*, CEPII Working Paper No. 2010-23, and this Bayanat derived release v0.1.0. Existing tools such as OEC, CEPII GeoDep and the Arab Development Portal already provide trade data/analysis. This release claims a portable, audited regional data product, not a new trade-concentration method or first-ever trade corpus.

<details><summary>Arabic overview (optional)</summary>

مجموعة بيانات لتحليل التجارة السلعية للدول العربية خلال 1995–2024، مبنية على إصدار ثابت من قاعدة BACI التابعة لمركز CEPII. تتضمن التدفقات التجارية ومؤشرات تركز مصادر الاستيراد ووجهات التصدير، مع الحفاظ على القيم الأصلية وإمكانية تتبع كل سجل إلى مصدره. تُحفظ بيانات السودان السابق منفصلة عن السودان الحالي. غياب سجل لا يعني بالضرورة انعدام التجارة، ولا تُعد مؤشرات التركز تقديرًا لاحتمال النقص أو الاعتماد على الاستيراد لتلبية الاستهلاك المحلي.

</details>
