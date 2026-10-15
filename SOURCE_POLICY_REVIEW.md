# Independent BACI source-policy and semantic release review

Reviewed 2026-10-15. This memo distinguishes publisher statements, verified local metadata and our conservative downstream policies. The full archive and resulting analytics have not yet been audited at the time of writing.

## Source rights and precise attribution

The [CEPII BACI presentation](https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37) and its [detailed documentation](https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html) declare **Etalab Open Licence 2.0 / Licence Ouverte 2.0**. The official detailed page was retrieved successfully using a normal browser user agent; the web search renderer intermittently received the publisher's 403 page. This is a documentation access difference, not evidence that the data lacks reuse rights.

The [official Etalab license text](https://raw.githubusercontent.com/etalab/licence-ouverte/master/open-licence.txt) permits commercial and noncommercial reproduction, adaptation, redistribution and integration into products. Reuse must acknowledge the grantor/source and the reused information's most recent update date. Linking the source can satisfy acknowledgement. Attribution must not imply official endorsement, and the reuse must not mislead about source, content or update date. The license does not guarantee accuracy or uninterrupted future supply.

Preserve the actual Etalab license with the dataset and cite CEPII/BACI on the card, downloads, demo and documentation. Keep our software license separate. Do not label the source data CC0. Compatibility with another attribution license is not a reason to conceal the source license.

Recommended attribution, with retrieval date filled from the real manifest:

> Source: CEPII, BACI database, HS92, version 202601; archive release date 22 January 2026. Reused under Etalab Open Licence 2.0. Downloaded [actual retrieval date]. Bayanat Labs selected the Arab-region source areas, added display metadata and computed the published analytical tables. CEPII does not endorse this derived release. [BACI documentation](https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html) · [Etalab license](https://raw.githubusercontent.com/etalab/licence-ouverte/master/open-licence.txt).

Also provide the archive's requested methodological citation:

Gaulier, G. and Zignago, S. (2010), *BACI: International Trade Database at the Product-Level. The 1994–2007 Version*. CEPII Working Paper No. 2010-23. [Paper](https://www.cepii.fr/pdf_pub/wp/2010/wp2010-23.pdf).

Date precision matters: `Readme.txt` and detailed BACI documentation give **22 January 2026**; CEPII's top-level presentation says last update **30 January 2026**. Keep these separately labeled if both are recorded. The archive version is V202601, not an October 2026 data update. Download date is not source update date, and trade year 2024 is not publication year 2026.

## What the current publisher documentation actually establishes

The inspected documentation defines the native observation as exporter–importer–HS6 product–year. Values are **thousands of current USD** and quantities are **metric tons**. Country codes are inherited from UN Comtrade and can differ from standard numeric ISO codes; the archive's own lookup is authoritative for this release. Leading zeros in product identifiers matter. Only positive trade flows are normally stored. The latest year can be incomplete and substantially revised, and historical flows can also change between releases; do not mix vintages.

The current detailed page did **not** establish a specific `q=NA` string convention, decimal rounding precision, or an exact mass interpretation of a displayed zero. Those details must come from the actual CSVs and their empirical audit; do not present our parsing policy as a publisher guarantee. In particular:

- Preserve `v_raw` and `q_raw` plus losslessly representable numeric values.
- Treat observed NA/blank quantity tokens as missing, retain the value-bearing flow, and count quantity coverage separately.
- Preserve a source numeric quantity of zero as numeric zero, distinct from null. It may reflect representation/rounding; it is not proof of literally zero shipped mass. Do not impute a positive minimum quantity.
- A zero or invalid trade value is a source anomaly relative to the documented positive-flow convention. Do not silently remove it from a world denominator and then claim that denominator is complete.
- No published unit-price field is needed. An HS6 bilateral annual value/weight ratio would be a trade unit value, affected by composition, quality and aggregation, not a retail or procurement price.

The [2010 methodological paper](https://www.cepii.fr/pdf_pub/wp/2010/wp2010-23.pdf) discusses reporting coverage and auxiliary information for distinguishing zeros from missing flows. It does not justify treating every absent current-release row as proven zero trade. Keep observed-row appearance/disappearance semantics explicit; that is weaker and more defensible than asserting a product market was born or ceased.

## Country-scope verification

Independently checked all 23 rows in `country_scope.csv` against `source_metadata/country_codes_V202601.csv` on source code, publisher country name and ISO3. **All 23 match; source codes are unique; 22 rows are current members; one row is historical context.** The sole duplicate ISO3 in this selected scope is SDN, attached to both 729 and 736.

The current-member code set is:

`12,48,174,262,275,368,400,414,422,434,478,504,512,634,682,706,729,760,784,788,818,887`.

The additional source area is **736, Sudan (...2011)**. Modern Sudan is **729**. Both are labeled SDN in the official lookup, but are different source territories. Former Sudan includes present-day South Sudan; **728, South Sudan**, remains outside the cohort unless its counterpart is in scope. Do not stitch 736 and 729 or interpret their difference as an economic change on fixed territory.

This is a present-day geographic collection with separately labeled historical context, not a historical Arab League legal-membership panel. Do not infer membership from language or country names. Report **22 modern countries/territories plus former-Sudan context**, not 23 countries. All calculations must keep BACI numeric identity even when display names or ISO3 labels coincide. Official partner metadata also contains historical Belgium–Luxembourg 58 and Belgium 56 with the same BEL ISO3, so this requirement applies beyond Sudan.

Useful exact lookup checks include Palestine 275, Comoros 174, Djibouti 262, Somalia 706, Yemen 887, and France **251** rather than the commonly assumed standard ISO numeric 250.

The official HS92 product lookup also explicitly contains **`9999AA`**, described as commodities not specified according to kind and marked **LEGACY NON-WCO CODE**. Retain it as an original source product identity with `product_is_standard_hs6=false`. Do not silently discard it, cast it to a number, call it standard HS6, or assign an HS2 chapter from its prefix. Unknown codes absent from the official source lookup remain validation failures. Six-digit product strings retain leading zeros.

The Boolean's implemented meaning is the numeric six-character shape of a source-listed code; it is **not external WCO certification**. The catalog also contains numeric `999999`, likewise described as unspecified commodities. A separate product-catalog `product_is_unspecified` flag identifies both special entries. Independent checks found neither code in any regional flow or global-context row across this release's 30 years. Future hierarchy analyses should explicitly exclude unspecified categories rather than assume every six-digit string is an ordinary detailed commodity.

## Analytical invariants to test independently

1. **World denominator completeness:** global supplier shares must use all recorded worldwide exports for the same product/year, including flows with neither endpoint in the regional collection. A denominator built from Arab-linked flows answers a different question. Detect quarantined invalid-value rows before declaring a global denominator complete.
2. **Regional direction:** imports are grouped by regional importer and all its source partners; exports by regional exporter and all its destinations. An intra-regional flow is one source observation but legitimately appears in both country perspectives. Never sum those two perspectives as distinct regional trade.
3. **Partner shares and HHI:** aggregate partner values before ranking/squaring. Use total positive observed value for the declared country/product/year/direction, not only the top partners or quantities. HHI uses fractional shares, ranges from zero to one for nonempty positive groups, and effective partner count is its reciprocal. A single partner gives one; two equal partners give HHI 0.5 and effective count two. Keep deterministic tie handling. Zero-value partners, if a derived fixture contains them, must not inflate positive partner count or change concentration.
4. **Quantity coverage:** missing/zero quantity must not delete positive trade value. Coverage is value associated with strictly positive reported quantity divided by all relevant positive observed value. A sum of known quantities cannot be labeled complete physical quantity when other rows lack it.
5. **Temporal absence:** compare source keys only between consecutive available years of the same vintage. Distinguish row absent from row present with a zero field. Nullable values on absent sides must remain null. Never fill years or quantity with zero to manufacture continuity. Report newly/no-longer observed rows, not proven zero-to-positive trade transitions.
6. **Precision and identity:** preserve six-character HS codes; avoid binary-float loss in money and quantities; reject values beyond the exact chosen DECIMAL representation rather than round silently. Source record IDs must bind archive hash, member and physical record ordinal. Two equal payloads on different source lines are different source positions, although duplicate native keys are a quality failure to resolve.
7. **Historical geography:** no grouping on ISO3 alone; 729 and 736 stay separate in flow, exposure, transition and country metadata tables. Regional membership/partner flags must follow the explicit source-code policy.

## Archive and publication integrity

For the production archive, record URL, ETag/Last-Modified where available, actual bytes, SHA-256 and the entire ZIP member inventory. Validate the expected 30 annual HS92/V202601 members for 1995–2024 plus known metadata. Reject duplicate annual member names, unexpected versions, unsafe paths, malformed headers, rows whose year disagrees with their file and duplicate native keys. The archive size and local SHA-256 alone do not prove the publisher intended those exact bytes unless the publisher supplies a checksum; say what was actually checked.

Every member read to completion should receive ZIP CRC verification. A central-directory CRC value merely being recorded is not the same as verified decompression. Ensure corrupted/truncated streams cannot leave an apparently complete Parquet or complete ingest report. Atomic output commit and resumable per-year reports should bind their input archive hash.

Before release, compare global and regional source counts/value totals, input/output keys, exact sums and quantity missingness. Review the released manifests for private local paths. Verify anonymous remote loading and preserve an independent validation report. Tests must exercise real derivation code on deliberately confounding fixtures, not only restate the formulas in a second implementation.

## Claims that the data can and cannot support

Useful descriptions include observed goods-trade flows, import-source/export-destination concentration, recorded intra-collection trade share and changes in observed partner/product portfolios. Supply sources are **countries/territories**, not individual companies. Imports do not reveal total domestic supply because domestic production, consumption, inventory and re-exports are not modeled here. Concentration is not a vulnerability or causal risk score. Monetary changes are nominal; inflation, exchange rates, classification and composition affect them. No domestic-dependence, shortage probability, price competitiveness or corruption claim follows from the tables alone.

The valuable contribution is a portable, carefully scoped regional analytical/evidence layer over CEPII's substantial original harmonization work. Do not claim a new underlying trade dataset, first-ever trade concentration measure or guaranteed decision usefulness without actual user evaluation.

## Independent fixture test result

All **18 independent tests passed** on 2026-10-15: 12 source/analytical semantic tests and six adversarial full-validator tests. The real ingestion and derivation functions are exercised. Review resolved two pipeline issues: incomplete resume manifests could previously omit the worldwide denominator output; invalid-source flag reporting previously used an unsupported same-level `UNNEST` aggregation. The fixture tests verify conservative failure and repair behavior, source-record lineage, exact decimal boundaries, quantity states, hand-calculated concentrations and rankings, globally complete supplier context, legacy products, historical source identities, and record-presence transitions.

The full-partition validator was strengthened to independently regroup all market and global product distributions, reject duplicate keys and mismatched transition key sets, use NULL-safe comparison, check code/scope/source bindings and remove stale passed approval after a failed rerun. Adversarial tests show that annual-total-preserving corruption of individual product totals, required null fields and duplicate/replaced keys are rejected. A production 2024 current-year arithmetic/reference/prefix probe passed with a 1,800 MB DuckDB limit; it intentionally did not certify transitions before their files were available. Tested code and metadata hashes are recorded in `tests/semantic_test_report.json`.

This fixture result does not certify the full production corpus. The separately maintained full-corpus validator must still succeed before publication.
