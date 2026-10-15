"""Ingest CEPII BACI HS92 V202601, preserving source grain and exact decimals.

The release contains reconciled bilateral trade estimates, not transactions or
retail prices. One source row is one directed exporter/importer/product/year.
Only the Arab-scope subset is published as flows; global supplier aggregates use
every accepted source row, including flows with neither endpoint in scope.

CLI: --archive ZIP --output OUTPUT_ROOT --country-scope CSV
     [--archive-sha256 EXPECTED] [--years 1995,...,2024]
OUTPUT_ROOT contains release/data/... and private/ingest-reports/ingest-YEAR.json.
CSV ordinals start at 1 after the header and refer to parsed source records.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
from pathlib import Path
import re
import shutil
import sys
import time
import zipfile

import duckdb
import pyarrow as pa

SOURCE_VERSION = "202601"
SOURCE_REVISION = "HS92"
YEARS = tuple(range(1995, 2025))
DECIMAL_TYPE = "DECIMAL(38,18)"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def decimal_exact_sql(column: str) -> str:
    """SQL predicate proving a string fits DECIMAL(38,18) without rounding.

    Accept fixed/scientific ASCII decimal notation, including signed zero.
    Compute the necessary scale and integer digits from the original string;
    never round through a float or trust TRY_CAST's permissive rounding alone.
    """
    s = f"trim({column})"
    mantissa = f"split_part(lower(regexp_replace({s}, '^[+-]', '')), 'e', 1)"
    exponent = f"CASE WHEN contains(lower({s}), 'e') THEN try_cast(split_part(lower({s}), 'e', 2) AS INTEGER) ELSE 0 END"
    fraction = f"split_part({mantissa}, '.', 2)"
    meaningful_fraction = f"regexp_replace({fraction}, '0+$', '')"
    digits = f"replace({mantissa}, '.', '')"
    all_zero = f"length(ltrim({digits}, '0')) = 0"
    integer_digits = f"greatest(length(ltrim({digits}, '0')) - length({fraction}) + ({exponent}), 0)"
    scale = f"greatest(length({meaningful_fraction}) - ({exponent}), 0)"
    syntax = f"regexp_full_match({s}, '[+-]?[0-9]+(\\.[0-9]*)?([eE][+-]?[0-9]+)?')"
    return (
        f"coalesce(({syntax}) AND try_cast({s} AS {DECIMAL_TYPE}) IS NOT NULL "
        f"AND (({all_zero}) OR (({exponent}) IS NOT NULL "
        f"AND ({integer_digits}) <= 20 AND ({scale}) <= 18)), false)"
    )


def write_json_atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def load_lookups(archive: zipfile.ZipFile, scope_path: Path) -> tuple[pa.Table, pa.Table]:
    country_member = f"country_codes_V{SOURCE_VERSION}.csv"
    product_member = f"product_codes_{SOURCE_REVISION}_V{SOURCE_VERSION}.csv"
    with archive.open(country_member) as stream:
        countries = list(csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")))
    with archive.open(product_member) as stream:
        products = list(csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")))
    with scope_path.open(encoding="utf-8-sig", newline="") as stream:
        scope_rows = list(csv.DictReader(stream))
    if not scope_rows or "baci_code" not in scope_rows[0]:
        raise ValueError("Country scope CSV must contain baci_code")
    scope = {}
    for row in scope_rows:
        code = int(row["baci_code"])
        if code in scope:
            raise ValueError(f"Duplicate country-scope BACI code: {code}")
        member = row.get("current_member", "true").strip().lower()
        if member not in ("true", "false", "1", "0"):
            raise ValueError(f"Invalid current_member for code {code}")
        row["_current_member"] = member in ("true", "1")
        scope[code] = row
    known_codes = {int(row["country_code"]) for row in countries}
    if not set(scope).issubset(known_codes):
        raise ValueError(f"Scope code absent from source country lookup: {set(scope) - known_codes}")
    output = []
    for country in countries:
        code = int(country["country_code"])
        row = scope.get(code)
        if row and row.get("country_iso3") and row["country_iso3"] != country["country_iso3"]:
            raise ValueError(f"Curated/source ISO3 mismatch for BACI code {code}")
        output.append({
            "country_code": code, "country_name": country["country_name"],
            "country_iso3": country["country_iso3"] or None,
            "name_ar": (row.get("display_name_ar") if row else None) or country["country_name"],
            "in_scope": row is not None,
            "current_member": row["_current_member"] if row else False,
            "scope_status": row.get("scope_status") if row else None,
        })
    if len(known_codes) != len(countries):
        raise ValueError("Source country metadata has duplicate source codes")
    product_rows = [{"hs6": str(row["code"]), "product_name": row["description"],
                     "product_is_standard_hs6": bool(re.fullmatch(r"[0-9]{6}", str(row["code"])))}
                    for row in products]
    if len({row["hs6"] for row in product_rows}) != len(product_rows):
        raise ValueError("Source product metadata has duplicate HS codes")
    # The official HS92 lookup explicitly includes legacy non-WCO code9999AA.
    # Retain source-listed six-character codes, but never label that code HS6.
    if any(not re.fullmatch(r"[0-9A-Z]{6}", row["hs6"]) for row in product_rows):
        raise ValueError("Source product metadata contains malformed product codes")
    country_schema = pa.schema([
        ("country_code", pa.int32()), ("country_name", pa.string()), ("country_iso3", pa.string()),
        ("name_ar", pa.string()), ("in_scope", pa.bool_()), ("current_member", pa.bool_()),
        ("scope_status", pa.string()),
    ])
    return pa.Table.from_pylist(output, schema=country_schema), pa.Table.from_pylist(product_rows)


def completed_report_valid(report_path: Path, binding: dict, output_root: Path) -> dict | None:
    if not report_path.is_file():
        return None
    try:
        report = json.loads(report_path.read_text())
    except (OSError, ValueError):
        return None
    if (report.get("status") != "complete" or report.get("binding") != binding
            or report.get("denominator_complete") is not True
            or report.get("quarantined_rows") != 0
            or report.get("duplicate_keys") != 0 or report.get("duplicate_extra_rows") != 0):
        return None
    year = report.get("year")
    if year not in YEARS or binding.get("source_member") != f"BACI_HS92_Y{year}_V202601.csv":
        return None
    source_rows, global_rows, regional_rows = (report.get(key) for key in ("source_rows", "global_rows", "regional_rows"))
    if (not all(isinstance(n, int) and not isinstance(n, bool) and n >= 0 for n in (source_rows, global_rows, regional_rows))
            or source_rows != global_rows or regional_rows > global_rows):
        return None
    expected = {
        "flows": f"release/data/flows/year={year}/part-00000.parquet",
        "global_product_suppliers": f"private/global_product_suppliers/year={year}/part-00000.parquet",
    }
    items = report.get("output_files", [])
    if (not isinstance(items, list) or len(items) != 2 or not all(isinstance(item, dict) for item in items)
            or {item.get("table") for item in items} != set(expected)):
        return None
    for item in items:
        if (item.get("path") != expected[item["table"]]
                or not re.fullmatch(r"[0-9a-f]{64}", str(item.get("sha256", "")))):
            return None
        if item["table"] == "flows" and item.get("rows") != regional_rows:
            return None
        if item["table"] == "global_product_suppliers" and not (isinstance(item.get("rows"), int) and 0 <= item["rows"] <= global_rows):
            return None
        path = output_root / item["path"]
        if not path.is_file() or sha256_file(path) != item["sha256"]:
            return None
    return report


def ingest_year(
    archive: zipfile.ZipFile, year: int, output_root: Path, archive_sha: str,
    scope_sha: str, code_sha: str, country_lookup: pa.Table, product_lookup: pa.Table,
) -> dict:
    """Process one annual source member. Invalid rows fail with a private quarantine."""
    member = f"BACI_{SOURCE_REVISION}_Y{year}_V{SOURCE_VERSION}.csv"
    info = archive.getinfo(member)
    binding = {
        "archive_sha256": archive_sha, "source_member": member, "source_member_crc32": f"{info.CRC:08x}",
        "source_member_uncompressed_bytes": info.file_size, "country_scope_sha256": scope_sha,
        "ingest_code_sha256": code_sha, "duckdb_version": duckdb.__version__,
    }
    reports_dir = output_root / "private" / "ingest-reports"
    report_path = reports_dir / f"ingest-{year}.json"
    previous = completed_report_valid(report_path, binding, output_root)
    if previous:
        print(json.dumps({"year": year, "status": "already_complete", "regional_rows": previous["regional_rows"]}), flush=True)
        return previous
    started = time.monotonic()
    temporary_dir = output_root / "private" / "annual-work" / str(year)
    temporary_dir.mkdir(parents=True, exist_ok=True)
    csv_path = temporary_dir / member
    # Reading the entire member through ZipExtFile verifies its ZIP CRC. Only
    # this task-created annual extraction is removed after successful processing.
    with archive.open(member) as source, csv_path.open("wb") as target:
        shutil.copyfileobj(source, target, length=4 * 1024 * 1024)
    if csv_path.stat().st_size != info.file_size:
        raise ValueError(f"Extracted member length mismatch: {member}")
    temporary_db = temporary_dir / "annual.duckdb"
    spill_dir = output_root / "private" / "duckdb" / str(year)
    spill_dir.mkdir(parents=True, exist_ok=True)
    # A prior interrupted attempt has no completion manifest; rebuild its own DB.
    for path in (temporary_db, temporary_db.with_suffix(".duckdb.wal")):
        if path.exists():
            path.unlink()
    connection = duckdb.connect(str(temporary_db))
    report = {
        "year": year, "status": "running", "source_member": member,
        "source_archive_sha256": archive_sha, "binding": binding,
        "source_version": SOURCE_VERSION, "source_revision": SOURCE_REVISION,
        "source_member_crc32": f"{info.CRC:08x}", "zip_crc_verified": True,
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "value_unit": "thousands of current USD", "quantity_unit": "metric tonnes",
        "observation_type": "CEPII reconciled directed bilateral trade estimate",
        "ordinal_semantics": "1-based parsed CSV record after header; serial CSV reader WITH ORDINALITY",
    }
    write_json_atomic(report_path, report)
    try:
        connection.execute("SET memory_limit='2GB'")
        connection.execute("SET threads=4")
        connection.execute("SET preserve_insertion_order=true")
        connection.execute(f"SET temp_directory={sql_string(spill_dir)}")
        connection.register("country_lookup_arrow", country_lookup)
        connection.register("product_lookup_arrow", product_lookup)
        connection.execute("CREATE TEMP TABLE country_lookup AS SELECT * FROM country_lookup_arrow")
        connection.execute("CREATE TEMP TABLE product_lookup AS SELECT * FROM product_lookup_arrow")
        connection.execute(f"""
            CREATE TABLE raw_rows AS
            SELECT t AS t_raw, i AS i_raw, j AS j_raw, k AS k_raw,
                   v AS v_raw, q AS q_raw, ordinality::BIGINT AS source_record_ordinal
            FROM read_csv({sql_string(csv_path)},
                columns={{'t':'VARCHAR','i':'VARCHAR','j':'VARCHAR','k':'VARCHAR','v':'VARCHAR','q':'VARCHAR'}},
                header=true, auto_detect=false, parallel=false, strict_mode=true,
                nullstr='__BAYANAT_NEVER_AN_IMPLICIT_NULL__', encoding='utf-8') WITH ORDINALITY
        """)
        # String parsing goes straight to DECIMAL. Invalid/rounded source scalars
        # remain in the private quarantine and block a complete-year manifest.
        connection.execute(f"""
            CREATE TABLE parsed_rows AS
            SELECT *, try_cast(trim(t_raw) AS SMALLINT) AS year,
                   try_cast(trim(i_raw) AS INTEGER) AS exporter_code,
                   try_cast(trim(j_raw) AS INTEGER) AS importer_code,
                   k_raw AS hs6,
                   try_cast(trim(v_raw) AS {DECIMAL_TYPE}) AS trade_value_1000usd,
                   try_cast(trim(q_raw) AS {DECIMAL_TYPE}) AS quantity_tonnes,
                   coalesce(trim(q_raw) IN ('NA', ''), true) AS quantity_missing,
                   {decimal_exact_sql('v_raw')} AS value_exact,
                   {decimal_exact_sql('q_raw')} AS quantity_exact
            FROM raw_rows
        """)
        connection.execute(f"""
            CREATE TABLE checked_rows AS
            SELECT r.*, p.product_is_standard_hs6,
                   list_filter([
                       CASE WHEN r.year IS NULL OR r.year <> {year} OR NOT regexp_full_match(trim(t_raw), '[0-9]{{4}}') THEN 'invalid_source_year' END,
                       CASE WHEN ec.country_code IS NULL OR NOT regexp_full_match(trim(i_raw), '[0-9]+') THEN 'unknown_or_malformed_exporter' END,
                       CASE WHEN ic.country_code IS NULL OR NOT regexp_full_match(trim(j_raw), '[0-9]+') THEN 'unknown_or_malformed_importer' END,
                       CASE WHEN p.hs6 IS NULL OR NOT regexp_full_match(k_raw, '[0-9A-Z]{{6}}') THEN 'unknown_or_malformed_product_code' END,
                       CASE WHEN NOT value_exact THEN 'invalid_or_inexact_trade_value' END,
                       CASE WHEN trade_value_1000usd IS NULL OR trade_value_1000usd <= 0 THEN 'nonpositive_or_missing_trade_value' END,
                       CASE WHEN NOT quantity_missing AND NOT quantity_exact THEN 'invalid_or_inexact_quantity' END,
                       CASE WHEN NOT quantity_missing AND quantity_tonnes < 0 THEN 'negative_quantity' END
                   ], flag -> flag IS NOT NULL) AS validation_flags
            FROM parsed_rows r
            LEFT JOIN country_lookup ec ON r.exporter_code=ec.country_code
            LEFT JOIN country_lookup ic ON r.importer_code=ic.country_code
            LEFT JOIN product_lookup p ON r.hs6=p.hs6
        """)
        source_rows = connection.execute("SELECT count(*) FROM checked_rows").fetchone()[0]
        invalid = connection.execute("SELECT count(*) FROM checked_rows WHERE len(validation_flags)>0").fetchone()[0]
        duplicate_stats = connection.execute("""
            SELECT count(*), coalesce(sum(n-1),0) FROM (
              SELECT year, exporter_code, importer_code, hs6, count(*) n
              FROM checked_rows GROUP BY ALL HAVING count(*)>1)
        """).fetchone()
        report.update({"source_rows": source_rows, "quarantined_rows": invalid,
                       "duplicate_keys": int(duplicate_stats[0]), "duplicate_extra_rows": int(duplicate_stats[1])})
        if invalid or duplicate_stats[0]:
            quarantine_dir = output_root / "private" / "quarantine"
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            quarantine = quarantine_dir / f"invalid-{year}.parquet"
            connection.execute(f"COPY (SELECT * FROM checked_rows WHERE len(validation_flags)>0) TO {sql_string(quarantine)} (FORMAT PARQUET, COMPRESSION ZSTD)")
            if duplicate_stats[0]:
                duplicate_path = quarantine_dir / f"duplicates-{year}.parquet"
                connection.execute(f"COPY (SELECT year,exporter_code,importer_code,hs6,count(*) occurrence_count FROM checked_rows GROUP BY ALL HAVING count(*)>1) TO {sql_string(duplicate_path)} (FORMAT PARQUET, COMPRESSION ZSTD)")
            flags = connection.execute("SELECT flag, count(*) n FROM (SELECT unnest(validation_flags) flag FROM checked_rows) GROUP BY flag").fetchall()
            report["validation_flag_counts"] = dict(flags)
            raise ValueError(f"{year}: {invalid} invalid source rows; {duplicate_stats[0]} duplicate source keys. Private quarantine saved; no complete-year release.")
        connection.execute("""
            CREATE VIEW enriched AS
            SELECT r.*,
                   ec.country_name exporter_name, ec.country_iso3 exporter_iso3,
                   ec.name_ar exporter_name_ar, ec.in_scope exporter_in_scope,
                   ec.current_member exporter_current_member, ec.scope_status exporter_scope_status,
                   ic.country_name importer_name, ic.country_iso3 importer_iso3,
                   ic.name_ar importer_name_ar, ic.in_scope importer_in_scope,
                   ic.current_member importer_current_member, ic.scope_status importer_scope_status
            FROM checked_rows r
            JOIN country_lookup ec ON r.exporter_code=ec.country_code
            JOIN country_lookup ic ON r.importer_code=ic.country_code
        """)
        constants = (
            f"{sql_string(archive_sha)}::VARCHAR source_archive_sha256, "
            f"{sql_string(SOURCE_VERSION)}::VARCHAR source_version, "
            f"{sql_string(member)}::VARCHAR source_member, "
            f"{sql_string(f'{info.CRC:08x}')}::VARCHAR source_member_crc32"
        )
        flow_query = f"""
            SELECT year,hs6,product_is_standard_hs6,exporter_code,importer_code,
                   exporter_iso3,importer_iso3,exporter_name,importer_name,
                   exporter_name_ar,importer_name_ar,exporter_in_scope,importer_in_scope,
                   exporter_current_member,importer_current_member,exporter_scope_status,importer_scope_status,
                   v_raw,q_raw,trade_value_1000usd,
                   CASE WHEN quantity_missing THEN NULL::{DECIMAL_TYPE} ELSE quantity_tonnes END quantity_tonnes,
                   quantity_missing,coalesce(NOT quantity_missing AND quantity_tonnes>0,false) quantity_positive,
                   source_record_ordinal,
                   sha256(to_json(list_value({sql_string(archive_sha)},{sql_string(member)},source_record_ordinal::VARCHAR))) source_record_id,
                   {constants}
            FROM enriched WHERE exporter_in_scope OR importer_in_scope
        """
        global_query = f"""
            SELECT year,hs6,product_is_standard_hs6,exporter_code,exporter_iso3,exporter_name,exporter_name_ar,
                   exporter_in_scope,exporter_current_member,exporter_scope_status,
                   sum(trade_value_1000usd)::{DECIMAL_TYPE} trade_value_1000usd,
                   count(*)::BIGINT partner_count,count(*)::BIGINT observed_partner_count,
                   {constants}
            FROM enriched GROUP BY year,hs6,product_is_standard_hs6,exporter_code,exporter_iso3,exporter_name,
                 exporter_name_ar,exporter_in_scope,exporter_current_member,exporter_scope_status
        """
        outputs = []
        for table, query in (("flows", flow_query), ("global_product_suppliers", global_query)):
            base = output_root / "release" / "data" if table == "flows" else output_root / "private"
            folder = base / table / f"year={year}"
            folder.mkdir(parents=True, exist_ok=True)
            target = folder / "part-00000.parquet"
            partial = folder / "part-00000.parquet.partial"
            connection.execute(f"COPY ({query}) TO {sql_string(partial)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)")
            partial.replace(target)
            rows = connection.execute(f"SELECT count(*) FROM read_parquet({sql_string(target)}, hive_partitioning=false)").fetchone()[0]
            outputs.append({"table": table, "path": str(target.relative_to(output_root)), "rows": rows,
                            "bytes": target.stat().st_size, "sha256": sha256_file(target)})
        coverage = connection.execute("""
            SELECT count(*) FILTER(WHERE exporter_in_scope OR importer_in_scope),
              count(*) FILTER(WHERE exporter_in_scope AND importer_in_scope),
              count(*) FILTER(WHERE quantity_missing),
              count(*) FILTER(WHERE NOT quantity_missing AND quantity_tonnes=0),
              sum(trade_value_1000usd),
              sum(trade_value_1000usd) FILTER(WHERE exporter_in_scope OR importer_in_scope),
              count(*) FILTER(WHERE NOT product_is_standard_hs6),
              count(*) FILTER(WHERE NOT product_is_standard_hs6 AND (exporter_in_scope OR importer_in_scope))
            FROM enriched
        """).fetchone()
        report.update({"status": "complete", "global_rows": source_rows, "regional_rows": coverage[0],
            "intra_scope_rows": coverage[1], "global_quantity_missing_rows": coverage[2],
            "global_quantity_zero_rows": coverage[3], "global_value_1000usd": str(coverage[4]),
            "regional_value_1000usd": str(coverage[5]), "denominator_complete": True,
            "global_nonstandard_product_rows": coverage[6], "regional_nonstandard_product_rows": coverage[7],
            "validation_flag_counts": {}, "output_files": outputs,
            "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "elapsed_seconds": round(time.monotonic()-started, 3)})
        write_json_atomic(report_path, report)
    except Exception as error:
        report.update({"status": "failed", "denominator_complete": False,
                       "error": f"{type(error).__name__}: {error}"})
        write_json_atomic(report_path, report)
        raise
    finally:
        connection.close()
    # Completion is committed before cleanup. Only this year's own work is removed.
    csv_path.unlink(missing_ok=True)
    temporary_db.unlink(missing_ok=True)
    temporary_db.with_suffix(".duckdb.wal").unlink(missing_ok=True)
    if spill_dir.exists():
        shutil.rmtree(spill_dir)
    print(json.dumps({key: report[key] for key in ("year", "status", "global_rows", "regional_rows", "elapsed_seconds")}), flush=True)
    return report


def run(archive_path: Path, output_root: Path, scope_path: Path, years: tuple[int, ...], expected_sha: str | None = None) -> dict:
    archive_path, output_root, scope_path = archive_path.resolve(), output_root.resolve(), scope_path.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    archive_sha = sha256_file(archive_path)
    if expected_sha and archive_sha != expected_sha:
        raise ValueError("Archive SHA256 differs from expected source binding")
    scope_sha, code_sha = sha256_file(scope_path), sha256_file(Path(__file__))
    with zipfile.ZipFile(archive_path) as archive:
        annual_members = sorted(name for name in archive.namelist() if re.fullmatch(r"BACI_HS92_Y[0-9]{4}_V202601\.csv", name))
        expected = sorted(f"BACI_HS92_Y{year}_V202601.csv" for year in YEARS)
        if annual_members != expected:
            raise ValueError("Archive must contain exactly the thirty HS92 V202601 annual members, 1995–2024")
        if len(archive.namelist()) != len(set(archive.namelist())):
            raise ValueError("Archive contains duplicate member names")
        countries, products = load_lookups(archive, scope_path)
        reports = [ingest_year(archive, year, output_root, archive_sha, scope_sha, code_sha, countries, products) for year in years]
    summary = {"status": "complete", "years": list(years), "source_archive_sha256": archive_sha,
        "country_scope_sha256": scope_sha, "ingest_code_sha256": code_sha,
        "global_rows": sum(r["global_rows"] for r in reports),
        "regional_rows": sum(r["regional_rows"] for r in reports),
        "source_revision": SOURCE_REVISION, "source_version": SOURCE_VERSION,
        "reports": [f"ingest-{year}.json" for year in years]}
    write_json_atomic(output_root / "private" / "ingest-reports" / "ingest-summary.json", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--country-scope", type=Path, required=True)
    parser.add_argument("--archive-sha256")
    parser.add_argument("--years", default=",".join(map(str, YEARS)))
    args = parser.parse_args()
    years = tuple(int(value) for value in args.years.split(","))
    if not years or len(years) != len(set(years)) or not set(years).issubset(YEARS):
        parser.error("--years must be unique years within 1995–2024")
    run(args.archive, args.output, args.country_scope, years, args.archive_sha256)


if __name__ == "__main__":
    main()
