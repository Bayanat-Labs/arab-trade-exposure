# Methodology

This is a derived analytical dataset built from CEPII BACI HS92 V202601. BACI reconciles bilateral merchandise-trade declarations into one directed flow for each exporter, importer, source product code and year. We preserve its estimates and add tested regional panels. These are annual merchandise flows, not customs transactions, individual suppliers, physical shipping routes or current market prices. Source data: [CEPII BACI](https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37); source methods: [Gaulier and Zignago (2010)](https://www.cepii.fr/pdf_pub/wp/2010/wp2010-23.pdf).

## Source, scope and accounting

The source archive supplies thirty annual CSV members, 1995–2024. The pipeline checks archive SHA-256, ZIP-member CRC/length, lookups, row keys and exact numerical representability. Each year is processed separately with a memory limit and disk spill. Invalid identifiers, malformed values, duplicate source keys or critical quarantine records stop a partition from being marked complete. A resume accepts only outputs bound to the source, mapping, code and file checksums.

The regional flow table retains a source row when either endpoint belongs to the documented country scope. An internal regional flow is stored **once**. The scope contains 22 modern Arab League member source areas and a separately labeled historical predecessor, former Sudan (BACI 736). Current Sudan (729) stays separate, even though labels can share an ISO3 abbreviation. Source BACI country codes are the primary keys; they are not assumed to be ISO numeric codes. Historical-predecessor and modern-member flags remain visible. Regional aggregates across time do not represent a fixed territorial boundary.

All source-listed product codes are retained. Most are six-digit HS92 codes; the official lookup also contains legacy `9999AA`, flagged `product_is_standard_hs6=false`. Legacy codes must not enter HS2/HS4 hierarchy summaries. Product definitions use this single HS revision throughout; codes must not be joined to another revision by matching digits. Official product names are preserved, without generated translations presented as source labels.

## Values and quantities

BACI `v` is **thousands of current US dollars**; `q` is metric tonnes. Typed values and sums use exact decimal arithmetic; USD columns multiply by exactly 1,000. Shares and concentration indices use floating-point arithmetic and are checked with absolute tolerance `1e-12`. Monetary values are reconciled FOB estimates and are not inflation-adjusted. Reconciliation and quantity estimation happen upstream; precise arithmetic does not imply precise underlying customs observations.

Quantity missingness and recorded numeric zero remain distinct; a zero can reflect source representation or rounding. Quantity coverage reports both row counts and the share of value attached to strictly positive recorded quantities. Missing/zero quantity does not remove a valid monetary flow from a value denominator. No transaction price, retail price, average price index or conversion into physical production is published from `value/quantity`.

## Exposure panels and global context

An import panel market is an in-scope destination × source product code × year. Its denominator is the exact sum from **all observed exporter areas**, including areas outside the region. An export panel market is the corresponding exporter × product × year, across all observed destinations. One internal flow supports one import market and one export market; summing both directions together therefore double-counts internal regional trade and should not be described as distinct trade value.

For counterparty values `v_i`, total `V=sum(v_i)` and shares `s_i=v_i/V`, the pipeline reports partner count, top-one/top-three share and unnormalized Herfindahl–Hirschman index `HHI=sum(s_i²)`. Effective partner count is `1/HHI`. Ties are ordered by source code for deterministic display. A source-country area can be a trading hub; it is not necessarily the producer or an independent firm. No arbitrary high/medium/low risk labels are assigned.

`intra_scope_share` uses the full documented regional scope, including historical predecessors. `intra_current_member_share` uses the modern member cohort and excludes historical predecessors. Both divide by the market's total observed value, with numerator and denominator exported. The modern cohort is not a reconstruction of political membership for every past year.

Global product supplier context is computed during ingestion from the **full worldwide source stream**, including non-Arab-to-non-Arab flows. Each exporter's world total is divided by the worldwide product total to calculate supplier HHI. The regional extract alone cannot reconstruct that denominator. Local import-source HHI and global exporter HHI answer different questions; neither is a measure of domestic import dependence, inventories, spare capacity, firm ownership, resilience or supplier replaceability.

## Time and record absence

Observation-change rows are created only for adjacent, fully ingested annual members of the same source archive/version. `newly_observed` means the bilateral source key appears this year but not the preceding member; `no_longer_observed` is the reverse. Absent values remain null and presence booleans are retained. An absent record is not a verified zero, and these labels do not prove real market entry/exit or disruption. First and last years are censored; a missing annual partition must not be bridged.

“Complete” refers to local file processing, not complete international customs reporting. Changes can reflect reporting, reconciliations, geography, classification or source revisions. The data cutoff is 2024, not the current month. All years belong to one frozen BACI release; comparison with another release requires a separate revision audit.

## Runnable SQL examples

The six files in `queries/` each run independently with DuckDB from the downloaded release root, whose `data/` directory holds the Parquet files. Edit the opening `SET VARIABLE` parameters, including the relative data path, source country code, direction, year and product. The default product `100190` is an HS92 code. These SQL files perform no network access.

They cover annual coverage/missingness, partner evidence with source member/record ordinal, persistent concentration across observed annual markets, local-vs-world concentration, modern/historical regional partner mix and adjacent-year record changes. Three-year persistence uses the **mean of annual HHIs**, explicitly not HHI computed from pooled multi-year values. Counts and full denominators accompany summaries. A shortlist is evidence for research, not a forecast or a recommendation to replace a supplier.

For a Python environment with `duckdb` installed:

```python
from pathlib import Path
import duckdb

con = duckdb.connect()
sql = Path("queries/01_market_coverage.sql").read_text()
result = con.execute(sql).fetchdf()
print(result.head())
```

Detailed fields are documented in [DERIVED_FIELDS.md](DERIVED_FIELDS.md) and generated `metadata/derived_data_dictionary.json`. Source manifests and trace tools connect derived markets to the bilateral rows and original archive/member record ordinals. Dataset counts must distinguish original flow rows from derived panel and observation-change rows.
