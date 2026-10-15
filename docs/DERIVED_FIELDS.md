# Derived field dictionary

All data grains retain original source-area codes and the exact source product code in `hs6`. `product_is_standard_hs6` separates standard six-digit codes from retained source-listed legacy codes. Decimal values are expressed in thousands of current USD unless a field ends `_usd`; dollar columns multiply by exactly 1,000. Shares/HHI are doubles in `[0,1]`, not percentages. Null denominator results stay null.

## Exposures

Path: `data/exposures/year=YYYY/part-00000.parquet`. Unique key: `(year,country_code,direction,hs6)`. `direction` is `import` or `export`; all recorded counterparties are included in the denominator.

| Fields | Meaning |
|---|---|
| `country_code`, `country_iso3`, `country_name` | Source BACI area identity and source labels. ISO3 is not the primary key. |
| `country_scope_status`, `country_current_member`, `country_historical_predecessor` | Curated cohort metadata. Former Sudan 736 is separate from current Sudan 729. “Current” is the modern membership cohort, not the trade year. |
| `total_value_1000usd`, `total_value_usd` | Exact market sum over all recorded partners. The denominator for all local shares. |
| `observed_partner_count`, `partner_count` | Recorded counterparties; positive-value counterparties. Production ingestion requires positive values, so these coincide in the release. |
| `top1_partner_code`, `top1_value_1000usd`, `top1_share` | Largest recorded positive-value counterparty, its exact value and share of total market value. |
| `top3_partner_codes`, `top3_value_1000usd`, `top3_share` | Up to three largest positive-value counterparties, their exact summed value and share. Code lists are descending value then ascending source code. |
| `hhi`, `effective_partner_count` | `sum((partner_value/total_value)²)` and `1/hhi`. Geographic concentration statistics, not firm counts or risk scores. |
| `intra_scope_value_1000usd`, `intra_scope_share` | Counterparty value in the complete documented scope, including historical predecessors; divide by the full market total. |
| `intra_current_member_value_1000usd`, `intra_current_member_share` | Counterparty value in the modern membership cohort; excludes historical predecessors; divide by full market total. |
| `positive_quantity_rows`, `missing_quantity_rows`, `nonpositive_quantity_rows` | Recorded quantity `>0`, null, or `<=0`. They partition the market's recorded bilateral rows. |
| `quantity_covered_value_1000usd`, `quantity_value_coverage_share` | Exact value with recorded quantity `>0` and its share of total market value. Missing quantities stay in the monetary denominator. |
| `source_version`, `source_archive_sha256` | Frozen upstream release and archive hash. Calculation definitions/version are in metadata and build reports. |

## Global context

Path: `data/global_context/year=YYYY/part-00000.parquet`. Unique key: `(year,hs6)`. Computed from all worldwide source rows, not just the published regional extract.

| Fields | Meaning |
|---|---|
| `global_value_1000usd`, `global_value_usd` | Exact world product-trade total. |
| `observed_supplier_count`, `supplier_count` | Recorded exporter areas and positive-value exporter areas. These are countries/areas, not firms or factories. |
| `global_supplier_hhi`, `global_effective_supplier_count` | HHI of exporters' world product values and its inverse. A different distribution from one country's import-origin HHI. |
| `top1_exporter_code`, `top1_exporter_value_1000usd`, `top1_exporter_share` | Largest exporter area's worldwide product value and its share. |
| `top3_exporter_codes`, `top3_exporter_value_1000usd`, `top3_exporter_share` | Up to three largest worldwide exporter areas, exact combined value and share. |
| `intra_scope_exporter_value_1000usd`, `intra_scope_exporter_share` | World export value attributed to the full regional source-area scope and its share. Does not measure regional production. |
| `current_member_exporter_value_1000usd`, `current_member_exporter_share` | Same measure restricted to the modern membership cohort. |
| `product_is_standard_hs6`, `source_version`, `source_archive_sha256` | Product-classification flag and frozen source provenance. |

## Link observation changes

Path: `data/link_observation_changes/year=YYYY/part-00000.parquet`, where YYYY is the later year. Unique key: `(previous_year,year,exporter_code,importer_code,hs6)`. Only changed physical source-record presence is output; unchanged present links are omitted.

| Fields | Meaning |
|---|---|
| `previous_year`, `year` | Adjacent complete source years; never bridged across a missing year. |
| `exporter_code`, `importer_code`, `exporter_iso3`, `importer_iso3`, `exporter_name`, `importer_name` | Original directional source-area identity/labels. |
| `exporter_scope_status`, `importer_scope_status`, `exporter_current_member`, `importer_current_member` | Source-area cohort metadata. Historical identities remain distinct. |
| `present_previous`, `present_current` | True when the physical source flow record exists in that year. Exactly one is true in a change row. |
| `observation_change` | `newly_observed` or `no_longer_observed`. These are not verified market entry/exit or disruption labels. |
| `previous_value_1000usd`, `current_value_1000usd` | Exact values from the present source records; absent sides stay null. |
| `hs6`, `product_is_standard_hs6`, `source_version`, `source_archive_sha256` | Exact source product, standard-code flag and frozen source provenance. |

Quantities do not become prices; source-country concentration does not establish domestic import dependence or substitutability. See [METHODOLOGY.md](METHODOLOGY.md) for denominators, missingness and scope limitations.
