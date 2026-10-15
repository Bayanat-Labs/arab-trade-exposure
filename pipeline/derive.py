#!/usr/bin/env python3
"""Derive auditable trade-exposure panels from complete BACI ingestion.

One annual partition is processed at a time. Values retain DuckDB DECIMAL sums;
shares and concentration indices use DOUBLE. No absent flow is filled with zero.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import tempfile

DERIVATION_VERSION = "1.0.0"

EXPOSURE_DEFINITIONS = {
    "grain": "year × source BACI country_code × direction × HS92 hs6",
    "direction": "import: all observed origins to an in-scope importer; export: all observed destinations of an in-scope exporter",
    "country_code": "Original BACI source-area identity. Former Sudan 736 and current Sudan 729 are separate even if ISO3 labels match.",
    "hs6": "Exact source-listed HS92 code; includes flagged legacy nonstandard codes such as 9999AA.",
    "product_is_standard_hs6": "True only for six ASCII digits. False source-listed legacy codes are retained separately and must not enter HS2/HS4 hierarchy aggregates.",
    "country_scope_status": "Versioned source-area cohort label from the country mapping; historical_predecessor is not a modern member.",
    "country_current_member": "Modern Arab League membership cohort flag; not membership reconstructed for the trade year.",
    "total_value_1000usd": "Exact sum of recorded bilateral values in thousands of current USD, all observed partners.",
    "total_value_usd": "Exact total_value_1000usd × 1000. Reconciled FOB merchandise value; not inflation-adjusted.",
    "observed_partner_count": "Distinct counterparties with an input row, including a zero if supplied to the derivation API.",
    "partner_count": "Distinct counterparties with positive value. Production BACI ingestion requires positive source values.",
    "top1_partner_code": "Largest positive-value source/destination BACI code; ties broken by ascending source code, without implying a meaningful difference.",
    "top1_value_1000usd": "Exact value of top1 counterparty.",
    "top1_share": "top1 value divided by all observed market value; null for zero denominator.",
    "top3_partner_codes": "Up to three positive-value counterparties, descending value then ascending source code.",
    "top3_value_1000usd": "Exact sum for up to three largest positive-value counterparties.",
    "top3_share": "top3 value divided by all observed market value; null for zero denominator.",
    "hhi": "Sum of squared counterparty value shares in [0,1], unnormalized; null for zero denominator.",
    "effective_partner_count": "1/hhi, a concentration statistic, not a count of independent firms or physical producers.",
    "intra_scope_value_1000usd": "Exact counterparty value whose source-area in_scope flag is true, including explicitly retained historical predecessors.",
    "intra_scope_share": "intra_scope value / all observed value. Historical scope is not a fixed-territory regional time series.",
    "intra_current_member_value_1000usd": "Exact counterparty value for the modern membership cohort; historical predecessor is excluded.",
    "intra_current_member_share": "intra_current_member value / all observed value; modern cohort, not contemporaneous membership.",
    "positive_quantity_rows": "Bilateral rows with recorded quantity > 0; quantities may be reconciled/estimated upstream.",
    "missing_quantity_rows": "Bilateral rows whose typed quantity is null; distinct from recorded zero.",
    "nonpositive_quantity_rows": "Bilateral rows with a non-null quantity <= 0 (production ingest permits rounded zero).",
    "quantity_covered_value_1000usd": "Exact value of rows with quantity > 0.",
    "quantity_value_coverage_share": "quantity-covered value / total market value. Not quantity accuracy or a transaction-price guarantee.",
    "source_version": "Single BACI release version shared by inputs.",
    "source_archive_sha256": "SHA-256 of the original source ZIP shared by inputs.",
    "limitations": "Only recorded reconciled merchandise trade. Source-country concentration is not domestic import dependence, disruption risk, production origin, spare capacity, supplier replaceability or firm concentration.",
}
GLOBAL_CONTEXT_DEFINITIONS = {
    "grain": "year × HS92 hs6",
    "global_value_1000usd": "Exact sum across the full worldwide product-supplier aggregate, including non-Arab-to-non-Arab trade.",
    "global_value_usd": "Exact global_value_1000usd × 1000.",
    "observed_supplier_count": "Source BACI exporter areas present in the global aggregate.",
    "supplier_count": "Positive-value source BACI exporter areas.",
    "global_supplier_hhi": "Unnormalized HHI of each source area's total world exports of this product. Re-exporters/hubs are not necessarily producers.",
    "global_effective_supplier_count": "1/global_supplier_hhi; not a count of production firms.",
    "top1_exporter_code": "Largest exporter area by full worldwide value; ascending BACI code breaks ties.",
    "top1_exporter_value_1000usd": "Exact total world exports by the top exporter area.",
    "top1_exporter_share": "Top exporter's value / global product value.",
    "top3_exporter_codes": "Up to three positive-value exporters, descending value then ascending BACI code.",
    "top3_exporter_value_1000usd": "Exact total for top three exporter areas.",
    "top3_exporter_share": "Top three exporters' value / global product value.",
    "intra_scope_exporter_share": "Share of world product export value attributed to in-scope exporter areas; not their global production share.",
    "current_member_exporter_share": "Share of world product export value attributed to the modern membership cohort.",
}
TRANSITION_DEFINITIONS = {
    "grain": "previous_year × year × exporter_code × importer_code × hs6; only links whose physical record presence changed",
    "observation_change": "newly_observed or no_longer_observed. Record absence is not a verified trade zero, market entry/exit or disruption.",
    "present_previous": "True only if a source flow record exists in previous_year.",
    "present_current": "True only if a source flow record exists in year.",
    "previous_value_1000usd": "Exact previous recorded value, null if absent; a numeric zero remains a present record.",
    "current_value_1000usd": "Exact current recorded value, null if absent.",
    "window_rule": "Only adjacent, parse-complete annual members within the same source archive/version. First/last source years are censored; no transition bridges a missing year.",
}


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_view(conn, name: str, path: str | Path) -> dict[str, str]:
    conn.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM read_parquet({sql_string(path)}, hive_partitioning=false)")
    return {row[0]: row[1] for row in conn.execute(f"DESCRIBE {name}").fetchall()}


def _require(columns: dict[str, str], names: tuple[str, ...], table: str) -> None:
    missing = sorted(set(names) - set(columns))
    if missing:
        raise ValueError(f"{table}: missing required fields {missing}")


def _bad_count(conn, table: str, predicate: str, message: str) -> None:
    count = conn.execute(f"SELECT count(*) FROM {table} WHERE {predicate}").fetchone()[0]
    if count:
        raise ValueError(f"{message}: {count} rows")


def _validate_flows(conn, table: str, columns: dict[str, str], year: int) -> None:
    _require(columns, ("year", "hs6", "product_is_standard_hs6", "exporter_code", "importer_code", "exporter_in_scope", "importer_in_scope", "exporter_current_member", "importer_current_member", "exporter_scope_status", "importer_scope_status", "exporter_name", "importer_name", "exporter_iso3", "importer_iso3", "trade_value_1000usd", "quantity_tonnes", "source_version", "source_archive_sha256"), table)
    for field in ("trade_value_1000usd", "quantity_tonnes"):
        if not columns[field].startswith("DECIMAL("):
            raise ValueError(f"{table}.{field} must be DECIMAL, got {columns[field]}")
    _bad_count(conn, table, f"year IS NULL OR year <> {int(year)}", "Flow partition has wrong year")
    _bad_count(conn, table, "hs6 IS NULL OR hs6='' OR product_is_standard_hs6 IS NULL OR product_is_standard_hs6 <> regexp_full_match(hs6, '[0-9]{6}') OR exporter_code IS NULL OR importer_code IS NULL", "Invalid source identity/product flag")
    _bad_count(conn, table, "trade_value_1000usd IS NULL OR trade_value_1000usd < 0", "Missing/negative trade value")
    _bad_count(conn, table, "exporter_in_scope IS NULL OR importer_in_scope IS NULL OR exporter_current_member IS NULL OR importer_current_member IS NULL OR NOT (exporter_in_scope OR importer_in_scope)", "Invalid regional scope flags")
    _bad_count(conn, table, "(exporter_current_member AND NOT exporter_in_scope) OR (importer_current_member AND NOT importer_in_scope)", "Modern membership must be in scope")
    duplicates = conn.execute(f"SELECT count(*) FROM (SELECT year,hs6,exporter_code,importer_code FROM {table} GROUP BY ALL HAVING count(*)>1)").fetchone()[0]
    if duplicates:
        raise ValueError(f"Duplicate bilateral flow keys: {duplicates}")


def _provenance(conn, table: str) -> tuple[str | None, str | None]:
    row = conn.execute(f"SELECT count(DISTINCT source_version), count(DISTINCT source_archive_sha256), min(source_version), min(source_archive_sha256), count(*) FILTER(WHERE source_version IS NULL OR source_archive_sha256 IS NULL) FROM {table}").fetchone()
    if row[0] > 1 or row[1] > 1 or row[4]:
        raise ValueError(f"{table}: mixed or missing source archive/version provenance")
    return row[2], row[3]


def _atomic_copy(conn, query: str, path: str | Path) -> dict:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".incomplete")
    temporary.unlink(missing_ok=True)
    try:
        conn.execute(f"COPY ({query}) TO {sql_string(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)")
        count = conn.execute(f"SELECT count(*) FROM read_parquet({sql_string(temporary)}, hive_partitioning=false)").fetchone()[0]
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    return {"path": str(output), "rows": count, "sha256": sha256(output), "bytes": output.stat().st_size}


def derive_year(conn, flows_path: str | Path, global_suppliers_path: str | Path,
                year: int, exposure_output: str | Path,
                context_output: str | Path) -> dict:
    """Create import/export exposure and full-world supplier context partitions."""
    columns = _load_view(conn, "_derived_flows", flows_path)
    _validate_flows(conn, "_derived_flows", columns, year)
    flow_provenance = _provenance(conn, "_derived_flows")
    global_columns = _load_view(conn, "_derived_suppliers", global_suppliers_path)
    _require(global_columns, ("year", "hs6", "product_is_standard_hs6", "exporter_code", "exporter_in_scope", "exporter_current_member", "trade_value_1000usd", "source_version", "source_archive_sha256"), "global suppliers")
    if not global_columns["trade_value_1000usd"].startswith("DECIMAL("):
        raise ValueError("Global supplier trade values must be DECIMAL")
    _bad_count(conn, "_derived_suppliers", f"year IS NULL OR year <> {int(year)} OR hs6 IS NULL OR hs6='' OR product_is_standard_hs6 IS NULL OR product_is_standard_hs6 <> regexp_full_match(hs6, '[0-9]{{6}}') OR exporter_code IS NULL OR trade_value_1000usd IS NULL OR trade_value_1000usd < 0 OR exporter_in_scope IS NULL OR exporter_current_member IS NULL", "Invalid global supplier aggregate")
    duplicates = conn.execute("SELECT count(*) FROM (SELECT year,hs6,exporter_code FROM _derived_suppliers GROUP BY ALL HAVING count(*)>1)").fetchone()[0]
    if duplicates:
        raise ValueError(f"Duplicate global supplier keys: {duplicates}")
    global_provenance = _provenance(conn, "_derived_suppliers")
    if flow_provenance != (None, None) and flow_provenance != global_provenance:
        raise ValueError("Flow and global context provenance differ")

    # Scope each direction before grouping. An intra-scope bilateral row supports
    # one import market and one export market; it remains one physical flow row.
    conn.execute("""
        CREATE OR REPLACE TEMP VIEW _derived_markets AS
        SELECT year,hs6,product_is_standard_hs6,importer_code AS country_code,'import' AS direction,
          importer_iso3 AS country_iso3,importer_name AS country_name,
          importer_scope_status AS country_scope_status,
          importer_current_member AS country_current_member,
          exporter_code AS partner_code,exporter_in_scope AS partner_in_scope,
          exporter_current_member AS partner_current_member,
          trade_value_1000usd,quantity_tonnes,source_version,source_archive_sha256
        FROM _derived_flows WHERE importer_in_scope
        UNION ALL
        SELECT year,hs6,product_is_standard_hs6,exporter_code,'export',exporter_iso3,exporter_name,
          exporter_scope_status,exporter_current_member,importer_code,
          importer_in_scope,importer_current_member,trade_value_1000usd,
          quantity_tonnes,source_version,source_archive_sha256
        FROM _derived_flows WHERE exporter_in_scope
    """)
    # Invalid source mapping must not split the same country/product market.
    mapping_conflicts = conn.execute("SELECT count(*) FROM (SELECT year,hs6,country_code,direction FROM _derived_markets GROUP BY ALL HAVING count(DISTINCT country_scope_status)>1 OR count(DISTINCT country_current_member)>1 OR count(DISTINCT country_name)>1)").fetchone()[0]
    if mapping_conflicts:
        raise ValueError(f"Conflicting country mapping inside markets: {mapping_conflicts}")
    exposure_query = """
      WITH ranked AS (
        SELECT *, SUM(trade_value_1000usd) OVER market AS denominator,
          ROW_NUMBER() OVER(PARTITION BY year,hs6,country_code,direction
            ORDER BY trade_value_1000usd DESC,partner_code ASC) AS rn
        FROM _derived_markets
        WINDOW market AS(PARTITION BY year,hs6,country_code,direction)
      ), aggregated AS (
        SELECT year,hs6,country_code,direction,
          bool_and(product_is_standard_hs6) AS product_is_standard_hs6,
          min(country_iso3) AS country_iso3,min(country_name) AS country_name,
          min(country_scope_status) AS country_scope_status,
          bool_and(country_current_member) AS country_current_member,
          max(denominator) AS total_value_1000usd,
          count(*) AS observed_partner_count,
          count(*) FILTER(WHERE trade_value_1000usd>0) AS partner_count,
          max(partner_code) FILTER(WHERE rn=1 AND trade_value_1000usd>0) AS top1_partner_code,
          sum(trade_value_1000usd) FILTER(WHERE rn=1) AS top1_value_1000usd,
          coalesce(list(partner_code ORDER BY rn) FILTER(WHERE rn<=3 AND trade_value_1000usd>0), []::INTEGER[]) AS top3_partner_codes,
          sum(trade_value_1000usd) FILTER(WHERE rn<=3) AS top3_value_1000usd,
          sum(power(cast(trade_value_1000usd AS DOUBLE)/nullif(cast(denominator AS DOUBLE),0),2)) AS hhi,
          sum(CASE WHEN partner_in_scope THEN trade_value_1000usd ELSE 0 END) AS intra_scope_value_1000usd,
          sum(CASE WHEN partner_current_member THEN trade_value_1000usd ELSE 0 END) AS intra_current_member_value_1000usd,
          count(*) FILTER(WHERE quantity_tonnes>0) AS positive_quantity_rows,
          count(*) FILTER(WHERE quantity_tonnes IS NULL) AS missing_quantity_rows,
          count(*) FILTER(WHERE quantity_tonnes<=0) AS nonpositive_quantity_rows,
          sum(CASE WHEN quantity_tonnes>0 THEN trade_value_1000usd ELSE 0 END) AS quantity_covered_value_1000usd,
          min(source_version) AS source_version,min(source_archive_sha256) AS source_archive_sha256
        FROM ranked GROUP BY year,hs6,country_code,direction
      )
      SELECT *,country_scope_status='historical_predecessor' AS country_historical_predecessor,
        total_value_1000usd*1000 AS total_value_usd,
        cast(top1_value_1000usd AS DOUBLE)/nullif(cast(total_value_1000usd AS DOUBLE),0) AS top1_share,
        cast(top3_value_1000usd AS DOUBLE)/nullif(cast(total_value_1000usd AS DOUBLE),0) AS top3_share,
        1.0/nullif(hhi,0) AS effective_partner_count,
        cast(intra_scope_value_1000usd AS DOUBLE)/nullif(cast(total_value_1000usd AS DOUBLE),0) AS intra_scope_share,
        cast(intra_current_member_value_1000usd AS DOUBLE)/nullif(cast(total_value_1000usd AS DOUBLE),0) AS intra_current_member_share,
        cast(quantity_covered_value_1000usd AS DOUBLE)/nullif(cast(total_value_1000usd AS DOUBLE),0) AS quantity_value_coverage_share
      FROM aggregated ORDER BY year,country_code,direction,hs6
    """
    context_query = """
      WITH ranked AS (
        SELECT *,SUM(trade_value_1000usd) OVER(PARTITION BY year,hs6) AS denominator,
          ROW_NUMBER() OVER(PARTITION BY year,hs6 ORDER BY trade_value_1000usd DESC,exporter_code ASC) AS rn
        FROM _derived_suppliers
      ), aggregated AS (
        SELECT year,hs6,bool_and(product_is_standard_hs6) AS product_is_standard_hs6,
          max(denominator) AS global_value_1000usd,
          count(*) AS observed_supplier_count,
          count(*) FILTER(WHERE trade_value_1000usd>0) AS supplier_count,
          max(exporter_code) FILTER(WHERE rn=1 AND trade_value_1000usd>0) AS top1_exporter_code,
          sum(trade_value_1000usd) FILTER(WHERE rn=1) AS top1_exporter_value_1000usd,
          coalesce(list(exporter_code ORDER BY rn) FILTER(WHERE rn<=3 AND trade_value_1000usd>0), []::INTEGER[]) AS top3_exporter_codes,
          sum(trade_value_1000usd) FILTER(WHERE rn<=3) AS top3_exporter_value_1000usd,
          sum(power(cast(trade_value_1000usd AS DOUBLE)/nullif(cast(denominator AS DOUBLE),0),2)) AS global_supplier_hhi,
          sum(CASE WHEN exporter_in_scope THEN trade_value_1000usd ELSE 0 END) AS intra_scope_exporter_value_1000usd,
          sum(CASE WHEN exporter_current_member THEN trade_value_1000usd ELSE 0 END) AS current_member_exporter_value_1000usd,
          min(source_version) AS source_version,min(source_archive_sha256) AS source_archive_sha256
        FROM ranked GROUP BY year,hs6
      )
      SELECT *,global_value_1000usd*1000 AS global_value_usd,
        1.0/nullif(global_supplier_hhi,0) AS global_effective_supplier_count,
        cast(top1_exporter_value_1000usd AS DOUBLE)/nullif(cast(global_value_1000usd AS DOUBLE),0) AS top1_exporter_share,
        cast(top3_exporter_value_1000usd AS DOUBLE)/nullif(cast(global_value_1000usd AS DOUBLE),0) AS top3_exporter_share,
        cast(intra_scope_exporter_value_1000usd AS DOUBLE)/nullif(cast(global_value_1000usd AS DOUBLE),0) AS intra_scope_exporter_share,
        cast(current_member_exporter_value_1000usd AS DOUBLE)/nullif(cast(global_value_1000usd AS DOUBLE),0) AS current_member_exporter_share
      FROM aggregated ORDER BY year,hs6
    """
    exposure_result = _atomic_copy(conn, exposure_query, exposure_output)
    context_result = _atomic_copy(conn, context_query, context_output)
    return {"year": year, "status": "complete", "derivation_version": DERIVATION_VERSION,
            "source_version": global_provenance[0], "source_archive_sha256": global_provenance[1],
            "exposures": exposure_result, "global_context": context_result}


def derive_transition(conn, previous_flows_path: str | Path,
                      current_flows_path: str | Path, previous_year: int,
                      current_year: int, output_path: str | Path) -> dict:
    """Record source-row appearances/absences; never infer true zero trade."""
    if current_year != previous_year + 1:
        raise ValueError("Observation changes require adjacent calendar years")
    previous_columns = _load_view(conn, "_transition_previous", previous_flows_path)
    current_columns = _load_view(conn, "_transition_current", current_flows_path)
    _validate_flows(conn, "_transition_previous", previous_columns, previous_year)
    _validate_flows(conn, "_transition_current", current_columns, current_year)
    previous_provenance = _provenance(conn, "_transition_previous")
    current_provenance = _provenance(conn, "_transition_current")
    if previous_provenance != current_provenance and previous_provenance != (None, None) and current_provenance != (None, None):
        raise ValueError("Adjacent source years have different archive/version provenance")
    query = f"""
      SELECT {int(previous_year)}::SMALLINT AS previous_year,
        {int(current_year)}::SMALLINT AS year,
        coalesce(p.hs6,c.hs6) AS hs6,
        coalesce(p.product_is_standard_hs6,c.product_is_standard_hs6) AS product_is_standard_hs6,
        coalesce(p.exporter_code,c.exporter_code) AS exporter_code,
        coalesce(p.importer_code,c.importer_code) AS importer_code,
        coalesce(p.exporter_iso3,c.exporter_iso3) AS exporter_iso3,
        coalesce(p.importer_iso3,c.importer_iso3) AS importer_iso3,
        coalesce(p.exporter_name,c.exporter_name) AS exporter_name,
        coalesce(p.importer_name,c.importer_name) AS importer_name,
        coalesce(p.exporter_scope_status,c.exporter_scope_status) AS exporter_scope_status,
        coalesce(p.importer_scope_status,c.importer_scope_status) AS importer_scope_status,
        coalesce(p.exporter_current_member,c.exporter_current_member) AS exporter_current_member,
        coalesce(p.importer_current_member,c.importer_current_member) AS importer_current_member,
        p.exporter_code IS NOT NULL AS present_previous,
        c.exporter_code IS NOT NULL AS present_current,
        CASE WHEN p.exporter_code IS NULL THEN 'newly_observed' ELSE 'no_longer_observed' END AS observation_change,
        p.trade_value_1000usd AS previous_value_1000usd,
        c.trade_value_1000usd AS current_value_1000usd,
        coalesce(p.source_version,c.source_version) AS source_version,
        coalesce(p.source_archive_sha256,c.source_archive_sha256) AS source_archive_sha256
      FROM _transition_previous p FULL OUTER JOIN _transition_current c
        ON p.hs6=c.hs6 AND p.exporter_code=c.exporter_code AND p.importer_code=c.importer_code
      WHERE p.exporter_code IS NULL OR c.exporter_code IS NULL
      ORDER BY exporter_code,importer_code,hs6
    """
    result = _atomic_copy(conn, query, output_path)
    return {"previous_year": previous_year, "year": current_year,
            "status": "complete", "derivation_version": DERIVATION_VERSION,
            "semantics": "Recorded-link presence changes only; absent is not verified zero trade.",
            "transitions": result}


def _check_complete_report(report_dir: Path, year: int) -> dict:
    path = report_dir / f"ingest-{year}.json"
    report = json.loads(path.read_text())
    if report.get("year") != year or report.get("status") != "complete" or report.get("denominator_complete") is not True:
        raise ValueError(f"Year {year} has no complete denominator report")
    if report.get("quarantined_rows") != 0 or report.get("source_rows") != report.get("global_rows"):
        raise ValueError(f"Year {year} source accounting is incomplete")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Build root containing release/data and private/ingest-reports")
    parser.add_argument("--years", required=True, help="Comma-separated years; only adjacent requested complete years produce transitions")
    parser.add_argument("--memory-limit", default="1500MB")
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--no-transitions", action="store_true")
    args = parser.parse_args()
    import duckdb
    years = sorted(set(int(x.strip()) for x in args.years.split(",") if x.strip()))
    if not years:
        raise ValueError("At least one year is required")
    report_dir = args.output / "private" / "ingest-reports"
    derived_reports = args.output / "private" / "derive-reports"
    derived_reports.mkdir(parents=True, exist_ok=True)
    release = args.output / "release"
    reports = {}
    for year in years:
        reports[year] = _check_complete_report(report_dir, year)
    archive_keys = {(r.get("source_archive_sha256"), r.get("source_version")) for r in reports.values()}
    if len({x[0] for x in archive_keys}) > 1:
        raise ValueError("All requested years must share one source archive")
    temp_root = args.output / "private" / "duckdb-derive-spill"
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="derive-", dir=temp_root) as spill:
        conn = duckdb.connect()
        conn.execute(f"SET memory_limit={sql_string(args.memory_limit)}")
        conn.execute(f"SET threads={int(args.threads)}")
        conn.execute(f"SET temp_directory={sql_string(spill)}")
        conn.execute("SET preserve_insertion_order=false")
        try:
            for year in years:
                flows = release / "data" / "flows" / f"year={year}" / "part-00000.parquet"
                suppliers = args.output / "private" / "global_product_suppliers" / f"year={year}" / "part-00000.parquet"
                # Match actual inputs to the successful ingestion's file hashes.
                file_reports = {(Path(x["path"]) if Path(x["path"]).is_absolute() else args.output / x["path"]).resolve(): x for x in reports[year].get("output_files", [])}
                for source in (flows, suppliers):
                    info = file_reports.get(source.resolve())
                    if info is None or sha256(source) != info.get("sha256"):
                        raise ValueError(f"Input absent from completion report or checksum changed: {source}")
                result = derive_year(conn, flows, suppliers, year,
                    release / "data" / "exposures" / f"year={year}" / "part-00000.parquet",
                    release / "data" / "global_context" / f"year={year}" / "part-00000.parquet")
                (derived_reports / f"derive-{year}.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
                print(json.dumps({"year": year, "exposure_rows": result["exposures"]["rows"], "global_context_rows": result["global_context"]["rows"]}), flush=True)
                if not args.no_transitions and year - 1 in reports:
                    transition = derive_transition(conn,
                        release / "data" / "flows" / f"year={year-1}" / "part-00000.parquet",
                        flows, year - 1, year,
                        release / "data" / "link_observation_changes" / f"year={year}" / "part-00000.parquet")
                    (derived_reports / f"transitions-{year-1}-{year}.json").write_text(json.dumps(transition, ensure_ascii=False, indent=2) + "\n")
                    print(json.dumps({"year": year, "observation_change_rows": transition["transitions"]["rows"]}), flush=True)
        finally:
            conn.close()
    metadata = release / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    dictionary = {"derivation_version": DERIVATION_VERSION,
        "arithmetic": {"sums": "Exact DuckDB DECIMAL", "shares_hhi": "IEEE754 DOUBLE; validation absolute tolerance 1e-12", "value_unit": "thousands of nominal USD; dollar columns multiply by exactly 1000"},
        "exposures": EXPOSURE_DEFINITIONS, "global_context": GLOBAL_CONTEXT_DEFINITIONS,
        "link_observation_changes": TRANSITION_DEFINITIONS}
    (metadata / "derived_data_dictionary.json").write_text(json.dumps(dictionary, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
