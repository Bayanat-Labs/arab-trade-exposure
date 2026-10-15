# Source fields and lookup tables

English is the default documentation and explorer language. Arabic labels are optional display metadata for regional source areas; English product descriptions remain the original publisher labels.

## Flows

One row is a recorded CEPII-reconciled exporter–importer–product–year estimate. A row is not a shipment, company, invoice or unit purchase.

| Field | Meaning |
|---|---|
| `year` | Observation year of the source annual CSV, 1995–2024. It is distinct from source publication, download and release dates. |
| `hs6` | Original HS92 source product string. Leading zeros survive. The source catalog also contains unspecified/legacy entries. |
| `product_is_standard_hs6` | Source-listed six-digit numeric code-shape flag. The name does not certify external WCO validity or detailed product comparability. |
| `exporter_code`, `importer_code` | Original numeric BACI source-area identities. Use them as keys; numeric ISO codes and ISO3 labels are not interchangeable identities. |
| `exporter_name`, `importer_name` | Original source country names, including four documented encoding defects retained for evidence. |
| `exporter_iso3`, `importer_iso3` | Publisher ISO3 display/lookup labels. Historical and present areas can share a label. |
| `exporter_name_ar`, `importer_name_ar` | Bayanat Arabic display labels for selected regional areas, with source-English fallback elsewhere. Not official translations or new identity keys. |
| `exporter_in_scope`, `importer_in_scope` | Endpoint is one of the explicit 23 source-area codes: 22 modern Arab League geographies plus former Sudan 736 separately. |
| `exporter_current_member`, `importer_current_member` | Endpoint belongs to the modern 22-member geographic collection. These are not trade-year legal membership flags. |
| `exporter_scope_status`, `importer_scope_status` | `current_member`, `historical_predecessor`, or null outside the collection. |
| `v_raw` | Original parsed CSV value string, native unit thousands of current USD. CSV parsing removes quoting; archive provenance recovers original bytes. |
| `q_raw` | Original parsed CSV quantity string, native unit metric tonnes. Blank and `NA` tokens remain visible. |
| `trade_value_1000usd` | Lossless DECIMAL(38,18) value parsed directly from `v_raw`. Source trade values must be positive. |
| `quantity_tonnes` | Lossless DECIMAL(38,18) parsed from `q_raw`, null only for observed missing tokens. Recorded zero remains zero. No minimum quantity is imputed. |
| `quantity_missing`, `quantity_positive` | Independent missingness and positive-recorded-quantity flags. Missing/zero quantity never removes the positive monetary value. |
| `source_archive_sha256` | SHA-256 of the entire fixed publisher ZIP. |
| `source_version` | `202601`; historical observations use this one release vintage throughout. |
| `source_member` | Original annual CSV filename inside the ZIP. |
| `source_member_crc32` | Source ZIP member CRC, checked by complete decompression during ingestion. CRC is not a substitute for archive SHA-256. |
| `source_record_ordinal` | One-based parsed CSV record number after the header. |
| `source_record_id` | SHA-256 of the compact JSON array `[archive_sha256, member_name, str(ordinal)]`. This identifies an immutable source position, not a product or company. |

Source `v` may contain many decimals or tiny upstream floating-point representation artifacts. Preserving them exactly does not assert measurement accuracy at that precision. No inflation/FX adjustment, retail-price inference or quantity imputation is applied.

## Countries

`countries` contains the complete publisher lookup, including nonregional partners. `country_code`, `country_name` and `country_iso3` retain the publisher identity and labels. `in_scope`, `current_member` and `scope_status` apply the explicit collection policy. `name_ar` supplies regional Arabic display labels with English fallback.

`country_name_display` and `country_label_repaired` are Bayanat display additions. Four reversible Latin-1/UTF-8 encoding repairs are recorded in `metadata/country_label_repairs.csv`; the source name remains available unchanged. Former Sudan 736 and modern Sudan 729 stay separate even though both carry SDN. Belgium 56 and former Belgium–Luxembourg 58 also share BEL and must not be merged on that label.

## Products

`products` preserves all 5,022 source catalog entries and their original English descriptions, including entries unused in the regional flow data. `product_is_unspecified` flags `999999` and legacy non-WCO `9999AA`. `product_has_classified_detail` requires a source-listed six-digit numeric code and excludes those unspecified entries; it is a practical filter, not an external classification certification. Neither unspecified code occurs in the released regional flow records of this vintage.

## Other tables

See [derived field definitions](DERIVED_FIELDS.md), [methodology](METHODOLOGY.md), [worked examples](USE_CASES.md) and the machine-readable schema/dictionary in `metadata/`. The dataset has no human-authored outcome labels or prepackaged ML train/test split.
