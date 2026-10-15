# Three things you can do with Arab Trade Exposure

These examples use actual query results from the completed BACI HS92 V202601 partitions. They show how to answer an analytical question, inspect its denominator and trace the evidence. They are not forecasts or a new trade-concentration method. All monetary amounts below are nominal reconciled merchandise-trade values. The source cutoff is 2024.

Run the SQL from the downloaded release root with DuckDB. The three JSON files in `examples/` contain the exact SQL, complete result fields, source version/archive hash and interpretation limits. Exact decimal results are serialized as strings so JSON consumers do not silently round them.

## 1. Answer a sourcing question—and know when the data cannot answer a quantity question

**Task:** For Egypt's 2024 imports in HS92 `100110`, identify the recorded source-country distribution and decide whether an overall tonnage or price calculation is supported.

The official source product label is **“Cereals: durum wheat.”** These results describe that HS92 source category; they are not a current product-specification audit or a measure of Egyptian domestic consumption.

| Actual result | 2024 value |
|---|---:|
| Recorded import value, all 15 origin areas | **$4,402,072,938** |
| Largest recorded origin | Russian Federation, BACI **643** |
| Largest origin's value | **$3,057,343,802** |
| Largest origin's share of the complete recorded market | **69.4524%** |
| Three largest origins: 643, 804, 642 | **88.8932%** |
| Import-origin HHI | **0.507531** |
| Rows with positive recorded quantity | **3 of 15** |
| Value attached to those quantity-positive rows | **$1,422,023** |
| Share of import value with positive recorded quantity | **0.0323035%** |

The value concentration is calculable using all recorded origins. An overall tonnage or price answer is not supported by this quantity subset: the other 12 rows, including the three largest, have missing quantity. Dropping them would change the population being described. Missing quantity does not make their monetary value disappear.

```sql
SELECT year,country_code,hs6,total_value_usd,partner_count,
       top1_partner_code,top1_share,top3_share,hhi,
       positive_quantity_rows,missing_quantity_rows,
       quantity_covered_value_1000usd,quantity_value_coverage_share
FROM read_parquet('data/exposures/year=2024/*.parquet',
                  hive_partitioning=false)
WHERE country_code=818 AND direction='import' AND hs6='100110';
```

Trace the largest-origin statement to its source record:

```sql
SELECT year,hs6,exporter_code,importer_code,v_raw,q_raw,
       trade_value_1000usd,quantity_tonnes,
       source_member,source_record_ordinal,
       source_version,source_archive_sha256
FROM read_parquet('data/flows/year=2024/*.parquet',
                  hive_partitioning=false)
WHERE exporter_code=643 AND importer_code=818 AND hs6='100110';
```

The record is in `BACI_HS92_Y2024_V202601.csv`, **parsed source-record ordinal 7,279,045** after the CSV header. It has `v_raw="3057343.802"` in thousands of USD and `q_raw=""`, with typed quantity null. Its original archive SHA-256 is:

```text
43259580059fa83e17fcdb6866e7b7f39aaf0dd50277cfe494a1ed05a4844faf
```

**Why this is useful:** a trade analyst gets a source-country breakdown without losing missing-quantity flows. A grounded data-analysis assistant can return the value answer with a verifiable record pointer and explicitly decline the unsupported quantity/price answer. The dataset supports research on faithful tool execution and numerical abstention without inventing benchmark questions or human gold labels.

Machine-readable results, including all 15 partners and the full market/world query: [01_egypt_100110_imports_2024.json](../examples/01_egypt_100110_imports_2024.json).

## 2. Separate an exporter's destination spread from its position in world trade

**Task:** Describe Morocco's 2024 exports of HS92 `310530` and compare the destination distribution with the complete world product-trade denominator.

The official source label is **“Fertilizers, mineral or chemical: diammonium hydrogenorthophosphate (diammonium phosphate)”**.

| Actual result | 2024 value |
|---|---:|
| Morocco, BACI **504**, recorded export value | **$2,955,248,931** |
| Recorded destination areas | **71** |
| Largest destination: India, BACI **699** | **$842,678,751; 28.5146%** |
| Top-three destination share | **52.0644%** |
| Destination HHI | **0.130660** |
| Recorded world product-trade value | **$10,251,259,607** |
| Morocco's share of that world value | **28.8282%** |
| World exporter-area HHI | **0.197137** |

The 71-destination distribution and the world exporter distribution are different objects. The first describes where Morocco's recorded exports went; the second describes how world product exports were distributed across exporter areas. World context includes flows whose two endpoints are outside the Arab scope, so it cannot be recovered by summing the regional extract alone.

```sql
SELECT e.country_code,e.hs6,e.total_value_usd,e.partner_count,
       e.top1_partner_code,e.top1_share,e.top3_share,e.hhi,
       g.global_value_usd,g.global_supplier_hhi,
       cast(e.total_value_1000usd AS DOUBLE)
         /cast(g.global_value_1000usd AS DOUBLE) AS world_value_share
FROM read_parquet('data/exposures/year=2024/*.parquet',
                  hive_partitioning=false) e
JOIN read_parquet('data/global_context/year=2024/*.parquet',
                  hive_partitioning=false) g USING(year,hs6)
WHERE e.country_code=504 AND e.direction='export' AND e.hs6='310530';
```

India's supporting flow is source key `(2024,504,699,310530)`, member `BACI_HS92_Y2024_V202601.csv`, **ordinal 5,945,812**, with `v_raw="842678.751"`. This is BACI code **699**, not an assumed ISO numeric country code. The top-five destination evidence in the JSON keeps all 71 destinations in its share denominator; it does not renormalize the shortlist to 100%.

**Why this is useful:** an analyst can distinguish geographic destination spread from a country's share of world recorded exports. An analytical assistant must select the correct denominator for each question. Neither result establishes production capacity, firm concentration, replaceability, product quality or a profitable market-entry opportunity.

Machine-readable market, world context and top-five source evidence: [02_morocco_310530_exports_2024.json](../examples/02_morocco_310530_exports_2024.json). Country names in evidence preserve the source labels; source code 792 has an upstream text-encoding artifact, so source codes remain the identity keys.

## 3. Catch a historical-identity error before publishing a growth comparison

**Task:** Check whether the historical Sudan source area can be treated as the same territory as current Sudan, and verify what the 2024 regional scope actually contains.

The curated longitudinal scope has **23 source areas: 22 modern member areas plus former Sudan**, explicitly marked historical. In the actual 2024 flows, the query finds **22 modern member areas and no historical-predecessor area**. This is not a claim of 23 modern countries.

| Source area / year | Recorded import product-market rows | Recorded import value |
|---|---:|---:|
| Former Sudan, BACI **736**, 2010 | 4,024 | **$14,196,976,601** |
| Current Sudan, BACI **729**, 2012 | 3,934 | **$9,697,063,505** |
| Current Sudan, BACI **729**, 2024 | 2,730 | **$3,700,901,735** |

Both source areas can carry an `SDN` label, but the historical territory is different. Joining on that label would silently stitch different source identities. These rows therefore do **not** support a fixed-border 2010–2024 growth rate. The example also shows absent source-area/year combinations explicitly: their observed-row count is zero and their recorded value remains **null**, rather than an invented zero-trade amount.

```sql
SELECT year,country_code,min(country_name) AS source_name,
       min(country_scope_status) AS scope_status,
       min(country_iso3) AS source_iso3,
       count(*) AS observed_product_markets,
       sum(total_value_usd) AS recorded_import_value_usd
FROM read_parquet('data/exposures/year=*/*.parquet',
                  hive_partitioning=false)
WHERE year IN (2010,2012,2024)
  AND country_code IN (729,736) AND direction='import'
GROUP BY year,country_code
ORDER BY year,country_code;
```

**Why this is useful:** a researcher can audit the geography before interpreting a long-run chart. A data-analysis assistant can preserve original identities, return “no recorded source rows” separately from zero trade, and flag an unsupported comparison. This same discipline applies to historical predecessor areas, source-code changes and regional totals with changing territorial coverage.

Machine-readable 2024 scope audit plus all requested import/export presence combinations for 2010, 2012 and 2024: [03_scope_and_sudan_history.json](../examples/03_scope_and_sudan_history.json).

The examples illustrate trade exposure and data support. Source-country concentration is not domestic import dependence or a disruption-risk estimate. See [METHODOLOGY.md](METHODOLOGY.md) for source coverage, reconciliation, quantity limitations and temporal absence semantics.
