"""Independent full-partition arithmetic, identity and source-pointer audit.

All markets and global product distributions are regrouped from their inputs.
Original-CSV prefix traces test alignment, not underlying reporting accuracy.
"""
from __future__ import annotations
import argparse
import csv
from decimal import Decimal, localcontext
import hashlib
import io
import json
from pathlib import Path
import re
import tempfile
import zipfile
import duckdb

SOURCE = Path(__file__).resolve().parents[1]
YEARS = list(range(1995, 2025))
TOLERANCE = 1e-12


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def no_bad(conn, table, predicate, message):
    bad = conn.execute(f"SELECT count(*) FROM {table} WHERE {predicate}").fetchone()[0]
    require(bad == 0, f"{message}: {bad}")


def unique(conn, table, fields):
    bad = conn.execute(f"SELECT count(*) FROM (SELECT {','.join(fields)} FROM {table} GROUP BY ALL HAVING count(*)>1)").fetchone()[0]
    require(bad == 0, f"{table}: duplicate keys: {bad}")


def nonnull(conn, table, fields):
    no_bad(conn, table, " OR ".join(f"{field} IS NULL" for field in fields), f"{table}: required null fields")


def decimal_columns(conn, table, fields):
    columns = dict((row[0], row[1]) for row in conn.execute(f"DESCRIBE {table}").fetchall())
    for field in fields:
        require(columns.get(field, "").startswith("DECIMAL("), f"{table}.{field}: exact decimal type required")


def initialize_references(conn, archive, scope_path):
    """Independent small references; no ingestion helper/constants imported."""
    with scope_path.open(encoding="utf-8-sig", newline="") as stream:
        scope_rows = list(csv.DictReader(stream))
    scope = {int(row["baci_code"]): row for row in scope_rows}
    require(len(scope) == len(scope_rows) == 23, "Scope identities must be 23 distinct source areas")
    require(sum(row["current_member"].lower() == "true" for row in scope_rows) == 22, "Modern scope must contain 22 source areas")
    with archive.open("country_codes_V202601.csv") as stream:
        countries = list(csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")))
    rows = []
    for row in countries:
        code = int(row["country_code"])
        selected = scope.get(code)
        if selected:
            require(selected["country_iso3"] == row["country_iso3"] and selected["country_name"] == row["country_name"], f"Scope/source country mismatch: {code}")
        rows.append((code, row["country_name"], row["country_iso3"] or None,
                     (selected.get("display_name_ar") if selected else None) or row["country_name"],
                     selected is not None, selected["current_member"].lower() == "true" if selected else False,
                     selected["scope_status"] if selected else None))
    conn.execute("CREATE OR REPLACE TEMP TABLE reference_countries(code INTEGER PRIMARY KEY,name VARCHAR,iso3 VARCHAR,name_ar VARCHAR,in_scope BOOLEAN,current_member BOOLEAN,scope_status VARCHAR)")
    conn.executemany("INSERT INTO reference_countries VALUES (?,?,?,?,?,?,?)", rows)
    require(set(scope).issubset({row[0] for row in rows}), "Scope contains unknown source areas")
    with archive.open("product_codes_HS92_V202601.csv") as stream:
        products = list(csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")))
    conn.execute("CREATE OR REPLACE TEMP TABLE reference_products(hs6 VARCHAR PRIMARY KEY,standard BOOLEAN)")
    conn.executemany("INSERT INTO reference_products VALUES (?,?)", [(r["code"], bool(re.fullmatch(r"[0-9]{6}", r["code"]))) for r in products])


def compare_grouped(conn, expected, actual, keys, exact_fields, approximate_fields, year):
    key_predicate = " AND ".join(f"x.{key}=a.{key}" for key in keys)
    mismatch = [f"x.{keys[0]} IS NULL", f"a.{keys[0]} IS NULL"]
    mismatch += [f"x.{field} IS DISTINCT FROM a.{field}" for field in exact_fields]
    for field in approximate_fields:
        # Shares/HHIs are <=1, so this is an absolute tolerance there. The
        # reciprocal effective count uses relative tolerance to avoid magnifying
        # harmless floating-point rounding in an independently computed HHI.
        mismatch += [f"a.{field} IS NULL", f"NOT isfinite(a.{field})", f"abs(x.{field}-a.{field})>{TOLERANCE}*greatest(1.0,abs(x.{field}))"]
    no_bad(conn, f"{expected} x FULL OUTER JOIN {actual} a ON {key_predicate}", " OR ".join(mismatch), f"{year}: {actual} differs from independent grouped values")


def check_year(conn, release, report, archive, archive_sha):
    year = report["year"]
    def view(name, path):
        require(path.is_file(), f"Missing {path}")
        conn.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM read_parquet({literal(path)},hive_partitioning=false)")
    for name, table in (("f", "flows"), ("e", "exposures"), ("g", "global_context")):
        view(name, release / "data" / table / f"year={year}" / "part-00000.parquet")
    view("s", release.parent / "private/global_product_suppliers" / f"year={year}" / "part-00000.parquet")
    nonnull(conn, "f", ("year", "hs6", "product_is_standard_hs6", "exporter_code", "importer_code", "exporter_in_scope", "importer_in_scope", "exporter_current_member", "importer_current_member", "v_raw", "trade_value_1000usd", "quantity_missing", "quantity_positive", "source_record_ordinal", "source_record_id", "source_member", "source_member_crc32", "source_archive_sha256", "source_version"))
    decimal_columns(conn, "f", ("trade_value_1000usd", "quantity_tonnes"))
    unique(conn, "f", ("year", "exporter_code", "importer_code", "hs6"))
    unique(conn, "f", ("source_record_ordinal",))
    info = archive.getinfo(report["source_member"])
    bad_base = f"""year IS DISTINCT FROM {year}
      OR source_archive_sha256 IS DISTINCT FROM {literal(archive_sha)}
      OR source_version IS DISTINCT FROM '202601'
      OR source_member IS DISTINCT FROM {literal(info.filename)}
      OR source_member_crc32 IS DISTINCT FROM {literal(f'{info.CRC:08x}')}
      OR trade_value_1000usd<=0 OR CAST(v_raw AS DECIMAL(38,18)) IS DISTINCT FROM trade_value_1000usd
      OR quantity_missing IS DISTINCT FROM coalesce(trim(q_raw) IN ('NA',''),true)
      OR (quantity_missing AND quantity_tonnes IS NOT NULL)
      OR (NOT quantity_missing AND (quantity_tonnes IS NULL OR quantity_tonnes<0 OR CAST(q_raw AS DECIMAL(38,18)) IS DISTINCT FROM quantity_tonnes))
      OR quantity_positive IS DISTINCT FROM coalesce(NOT quantity_missing AND quantity_tonnes>0,false)
      OR source_record_ordinal<1 OR source_record_ordinal>{report['source_rows']}
      OR source_record_id IS DISTINCT FROM sha256(to_json(list_value({literal(archive_sha)},{literal(info.filename)},source_record_ordinal::VARCHAR)))"""
    no_bad(conn, "f", bad_base, f"{year}: base source/scalar invariant")
    country_predicates = []
    for side, reference in (("exporter", "ec"), ("importer", "ic")):
        country_predicates.append(f"{reference}.code IS NULL")
        for field, ref_field in (("name", "name"), ("iso3", "iso3"), ("name_ar", "name_ar"), ("in_scope", "in_scope"), ("current_member", "current_member"), ("scope_status", "scope_status")):
            country_predicates.append(f"f.{side}_{field} IS DISTINCT FROM {reference}.{ref_field}")
    country_predicates += ["p.hs6 IS NULL", "f.product_is_standard_hs6 IS DISTINCT FROM p.standard", "NOT(f.exporter_in_scope OR f.importer_in_scope)"]
    no_bad(conn, "f LEFT JOIN reference_countries ec ON exporter_code=ec.code LEFT JOIN reference_countries ic ON importer_code=ic.code LEFT JOIN reference_products p ON f.hs6=p.hs6", " OR ".join(country_predicates), f"{year}: country/product reference mismatch")
    count = conn.execute("SELECT count(*),sum(trade_value_1000usd) FROM f").fetchone()
    require(count[0] == report["regional_rows"], f"{year}: regional row count mismatch")
    require(count[1] == Decimal(report["regional_value_1000usd"]), f"{year}: exact regional sum mismatch")
    require(conn.execute("SELECT count(*) FROM f WHERE NOT product_is_standard_hs6").fetchone()[0] == report["regional_nonstandard_product_rows"], f"{year}: legacy product count mismatch")

    # Independent of derive.py's row-share window: sum(v*v)/sum(v)^2.
    conn.execute("""CREATE OR REPLACE TEMP VIEW market_inputs AS
      SELECT importer_code country_code,'import' direction,hs6,exporter_code partner_code,
        exporter_in_scope partner_in_scope,exporter_current_member partner_current_member,
        trade_value_1000usd v,quantity_tonnes q FROM f WHERE importer_in_scope
      UNION ALL
      SELECT exporter_code,'export',hs6,importer_code,importer_in_scope,importer_current_member,
        trade_value_1000usd,quantity_tonnes FROM f WHERE exporter_in_scope""")
    conn.execute("""CREATE OR REPLACE TEMP TABLE expected_market_groups AS
      SELECT country_code,direction,hs6,sum(v) total_value_1000usd,count(*) observed_partner_count,count(*) partner_count,
        max(v) top1_value_1000usd,arg_min(partner_code,struct_pack(value := -v,code := partner_code)) top1_partner_code,
        arg_min(partner_code,struct_pack(value := -v,code := partner_code),3) top3_partner_codes,
        list_sum(arg_min(v,struct_pack(value := -v,code := partner_code),3)) top3_value_1000usd,
        sum(pow(cast(v AS DOUBLE),2))/pow(cast(sum(v) AS DOUBLE),2) hhi,
        sum(CASE WHEN partner_in_scope THEN v ELSE 0 END) intra_scope_value_1000usd,
        sum(CASE WHEN partner_current_member THEN v ELSE 0 END) intra_current_member_value_1000usd,
        count(*) FILTER(WHERE q>0) positive_quantity_rows,count(*) FILTER(WHERE q IS NULL) missing_quantity_rows,
        count(*) FILTER(WHERE q<=0) nonpositive_quantity_rows,
        sum(CASE WHEN q>0 THEN v ELSE 0 END) quantity_covered_value_1000usd
      FROM market_inputs GROUP BY country_code,direction,hs6""")
    conn.execute("""CREATE OR REPLACE TEMP VIEW expected_markets AS SELECT *,total_value_1000usd*1000 total_value_usd,
      cast(top1_value_1000usd AS DOUBLE)/cast(total_value_1000usd AS DOUBLE) top1_share,
      cast(top3_value_1000usd AS DOUBLE)/cast(total_value_1000usd AS DOUBLE) top3_share,1.0/hhi effective_partner_count,
      cast(intra_scope_value_1000usd AS DOUBLE)/cast(total_value_1000usd AS DOUBLE) intra_scope_share,
      cast(intra_current_member_value_1000usd AS DOUBLE)/cast(total_value_1000usd AS DOUBLE) intra_current_member_share,
      cast(quantity_covered_value_1000usd AS DOUBLE)/cast(total_value_1000usd AS DOUBLE) quantity_value_coverage_share
      FROM expected_market_groups""")
    unique(conn, "e", ("year", "country_code", "direction", "hs6"))
    nonnull(conn, "e", ("year", "country_code", "direction", "hs6", "product_is_standard_hs6", "source_version", "source_archive_sha256"))
    exposure_exact = ("total_value_1000usd", "total_value_usd", "observed_partner_count", "partner_count", "top1_value_1000usd", "top1_partner_code", "top3_partner_codes", "top3_value_1000usd", "intra_scope_value_1000usd", "intra_current_member_value_1000usd", "positive_quantity_rows", "missing_quantity_rows", "nonpositive_quantity_rows", "quantity_covered_value_1000usd")
    decimal_columns(conn, "e", ("total_value_1000usd", "total_value_usd", "top1_value_1000usd", "top3_value_1000usd", "intra_scope_value_1000usd", "intra_current_member_value_1000usd", "quantity_covered_value_1000usd"))
    compare_grouped(conn, "expected_markets", "e", ("country_code", "direction", "hs6"), exposure_exact, ("hhi", "effective_partner_count", "top1_share", "top3_share", "intra_scope_share", "intra_current_member_share", "quantity_value_coverage_share"), year)
    no_bad(conn, "e LEFT JOIN reference_countries c ON country_code=c.code LEFT JOIN reference_products p USING(hs6)", f"e.year IS DISTINCT FROM {year} OR c.code IS NULL OR NOT c.in_scope OR e.country_iso3 IS DISTINCT FROM c.iso3 OR e.country_name IS DISTINCT FROM c.name OR e.country_scope_status IS DISTINCT FROM c.scope_status OR e.country_current_member IS DISTINCT FROM c.current_member OR e.country_historical_predecessor IS DISTINCT FROM (c.scope_status='historical_predecessor') OR e.product_is_standard_hs6 IS DISTINCT FROM p.standard OR e.source_version IS DISTINCT FROM '202601' OR e.source_archive_sha256 IS DISTINCT FROM {literal(archive_sha)}", f"{year}: exposure identity/provenance")

    nonnull(conn, "s", ("year", "hs6", "product_is_standard_hs6", "exporter_code", "exporter_in_scope", "exporter_current_member", "trade_value_1000usd", "partner_count", "observed_partner_count", "source_version", "source_archive_sha256"))
    decimal_columns(conn, "s", ("trade_value_1000usd",))
    unique(conn, "s", ("year", "hs6", "exporter_code"))
    no_bad(conn, "s LEFT JOIN reference_countries c ON exporter_code=c.code LEFT JOIN reference_products p USING(hs6)", f"s.year IS DISTINCT FROM {year} OR c.code IS NULL OR p.hs6 IS NULL OR s.product_is_standard_hs6 IS DISTINCT FROM p.standard OR s.exporter_in_scope IS DISTINCT FROM c.in_scope OR s.exporter_current_member IS DISTINCT FROM c.current_member OR s.trade_value_1000usd<=0 OR s.partner_count<=0 OR s.observed_partner_count IS DISTINCT FROM s.partner_count OR s.source_version IS DISTINCT FROM '202601' OR s.source_archive_sha256 IS DISTINCT FROM {literal(archive_sha)}", f"{year}: global input identity/scalar mismatch")
    supplier_accounting = conn.execute("SELECT sum(trade_value_1000usd),sum(partner_count) FROM s").fetchone()
    require(supplier_accounting == (Decimal(report["global_value_1000usd"]), report["source_rows"]), f"{year}: worldwide input accounting mismatch")
    conn.execute("""CREATE OR REPLACE TEMP TABLE expected_world_groups AS
      SELECT hs6,sum(trade_value_1000usd) global_value_1000usd,count(*) observed_supplier_count,count(*) supplier_count,
        max(trade_value_1000usd) top1_exporter_value_1000usd,
        arg_min(exporter_code,struct_pack(value := -trade_value_1000usd,code := exporter_code)) top1_exporter_code,
        arg_min(exporter_code,struct_pack(value := -trade_value_1000usd,code := exporter_code),3) top3_exporter_codes,
        list_sum(arg_min(trade_value_1000usd,struct_pack(value := -trade_value_1000usd,code := exporter_code),3)) top3_exporter_value_1000usd,
        sum(pow(cast(trade_value_1000usd AS DOUBLE),2))/pow(cast(sum(trade_value_1000usd) AS DOUBLE),2) global_supplier_hhi,
        sum(CASE WHEN exporter_in_scope THEN trade_value_1000usd ELSE 0 END) intra_scope_exporter_value_1000usd,
        sum(CASE WHEN exporter_current_member THEN trade_value_1000usd ELSE 0 END) current_member_exporter_value_1000usd
      FROM s GROUP BY hs6""")
    conn.execute("""CREATE OR REPLACE TEMP VIEW expected_world AS SELECT *,global_value_1000usd*1000 global_value_usd,
      1.0/global_supplier_hhi global_effective_supplier_count,
      cast(top1_exporter_value_1000usd AS DOUBLE)/cast(global_value_1000usd AS DOUBLE) top1_exporter_share,
      cast(top3_exporter_value_1000usd AS DOUBLE)/cast(global_value_1000usd AS DOUBLE) top3_exporter_share,
      cast(intra_scope_exporter_value_1000usd AS DOUBLE)/cast(global_value_1000usd AS DOUBLE) intra_scope_exporter_share,
      cast(current_member_exporter_value_1000usd AS DOUBLE)/cast(global_value_1000usd AS DOUBLE) current_member_exporter_share FROM expected_world_groups""")
    unique(conn, "g", ("year", "hs6"))
    nonnull(conn, "g", ("year", "hs6", "product_is_standard_hs6", "source_version", "source_archive_sha256"))
    global_exact = ("global_value_1000usd", "global_value_usd", "observed_supplier_count", "supplier_count", "top1_exporter_value_1000usd", "top1_exporter_code", "top3_exporter_codes", "top3_exporter_value_1000usd", "intra_scope_exporter_value_1000usd", "current_member_exporter_value_1000usd")
    decimal_columns(conn, "g", ("global_value_1000usd", "global_value_usd", "top1_exporter_value_1000usd", "top3_exporter_value_1000usd", "intra_scope_exporter_value_1000usd", "current_member_exporter_value_1000usd"))
    compare_grouped(conn, "expected_world", "g", ("hs6",), global_exact, ("global_supplier_hhi", "global_effective_supplier_count", "top1_exporter_share", "top3_exporter_share", "intra_scope_exporter_share", "current_member_exporter_share"), year)
    no_bad(conn, "g LEFT JOIN reference_products p USING(hs6)", f"g.year IS DISTINCT FROM {year} OR p.hs6 IS NULL OR g.product_is_standard_hs6 IS DISTINCT FROM p.standard OR g.source_version IS DISTINCT FROM '202601' OR g.source_archive_sha256 IS DISTINCT FROM {literal(archive_sha)}", f"{year}: global context identity/provenance")

    # Independent Python Decimal prefix examples supplement full SQL checks.
    examples = conn.execute("SELECT country_code,direction,hs6,total_value_1000usd,hhi,top1_share FROM e ORDER BY hs6,country_code,direction LIMIT 10").fetchall()
    with localcontext() as context:
        context.prec = 60
        for code, direction, hs6, total, hhi, top1 in examples:
            field = "importer_code" if direction == "import" else "exporter_code"
            values = [r[0] for r in conn.execute(f"SELECT trade_value_1000usd FROM f WHERE {field}=? AND hs6=?", [code, hs6]).fetchall()]
            require(sum(values, Decimal(0)) == total, f"{year}: Python decimal market sum mismatch")
            require(abs(float(sum((v / total) ** 2 for v in values)) - hhi) < TOLERANCE, f"{year}: Python HHI mismatch")
            require(abs(float(max(values) / total) - top1) < TOLERANCE, f"{year}: Python top1 mismatch")
    selected = conn.execute("SELECT source_record_ordinal,year,exporter_code,importer_code,hs6,v_raw,q_raw FROM f ORDER BY source_record_ordinal LIMIT 5").fetchall()
    targets = {r[0]: r for r in selected}
    seen = 0
    with archive.open(info) as raw:
        for ordinal, row in enumerate(csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")), 1):
            if ordinal in targets:
                require(tuple(targets[ordinal][1:]) == (int(row["t"]), int(row["i"]), int(row["j"]), row["k"], row["v"], row["q"]), f"{year}: original CSV pointer mismatch at {ordinal}")
                seen += 1
            if seen == len(targets):
                break
    require(seen == len(targets), f"{year}: unresolved source trace")
    transition_rows = 0
    if year > 1995:
        view("previous_flows", release / "data/flows" / f"year={year-1}" / "part-00000.parquet")
        view("t", release / "data/link_observation_changes" / f"year={year}" / "part-00000.parquet")
        unique(conn, "t", ("previous_year", "year", "exporter_code", "importer_code", "hs6"))
        nonnull(conn, "t", ("previous_year", "year", "exporter_code", "importer_code", "hs6", "product_is_standard_hs6", "present_previous", "present_current", "observation_change", "source_version", "source_archive_sha256"))
        decimal_columns(conn, "t", ("previous_value_1000usd", "current_value_1000usd"))
        conn.execute("""CREATE OR REPLACE TEMP VIEW expected_change_keys AS
          SELECT coalesce(p.exporter_code,c.exporter_code) exporter_code,coalesce(p.importer_code,c.importer_code) importer_code,coalesce(p.hs6,c.hs6) hs6
          FROM previous_flows p FULL OUTER JOIN f c ON p.exporter_code=c.exporter_code AND p.importer_code=c.importer_code AND p.hs6=c.hs6
          WHERE p.exporter_code IS NULL OR c.exporter_code IS NULL""")
        for left, right in (("t", "expected_change_keys"), ("expected_change_keys", "t")):
            missing = conn.execute(f"SELECT count(*) FROM (SELECT exporter_code,importer_code,hs6 FROM {left} EXCEPT SELECT exporter_code,importer_code,hs6 FROM {right})").fetchone()[0]
            require(missing == 0, f"{year}: transition key-set mismatch {left} vs {right}: {missing}")
        predicates = [f"t.previous_year IS DISTINCT FROM {year-1}", f"t.year IS DISTINCT FROM {year}", "t.present_previous IS DISTINCT FROM (p.exporter_code IS NOT NULL)", "t.present_current IS DISTINCT FROM (c.exporter_code IS NOT NULL)", "t.present_previous=t.present_current", "t.previous_value_1000usd IS DISTINCT FROM p.trade_value_1000usd", "t.current_value_1000usd IS DISTINCT FROM c.trade_value_1000usd", "t.observation_change IS DISTINCT FROM CASE WHEN p.exporter_code IS NULL THEN 'newly_observed' ELSE 'no_longer_observed' END", "t.source_version IS DISTINCT FROM '202601'", f"t.source_archive_sha256 IS DISTINCT FROM {literal(archive_sha)}"]
        for field in ("product_is_standard_hs6", "exporter_iso3", "importer_iso3", "exporter_name", "importer_name", "exporter_scope_status", "importer_scope_status", "exporter_current_member", "importer_current_member"):
            predicates.append(f"t.{field} IS DISTINCT FROM coalesce(p.{field},c.{field})")
        no_bad(conn, "t LEFT JOIN previous_flows p USING(exporter_code,importer_code,hs6) LEFT JOIN f c USING(exporter_code,importer_code,hs6)", " OR ".join(predicates), f"{year}: link-change semantics/provenance")
        transition_rows = conn.execute("SELECT count(*) FROM t").fetchone()[0]
    return {"year": year, "flow_rows": count[0], "exposure_rows": conn.execute("SELECT count(*) FROM e").fetchone()[0], "global_context_rows": conn.execute("SELECT count(*) FROM g").fetchone()[0], "link_change_rows": transition_rows, "csv_prefix_round_trips": seen, "python_market_recomputations": len(examples), "exact_regional_value_1000usd": str(count[1]), "exact_global_value_1000usd": str(supplier_accounting[0]), "all_market_concentrations_recomputed": True, "all_global_product_distributions_recomputed": True, "passed": True}


def run(output, archive_path):
    output = Path(output).resolve()
    release = output / "release"
    validation_path = release / "reports/validation.json"
    validation_path.unlink(missing_ok=True)  # Failed reruns cannot leave stale approval.
    summary = json.loads((output / "private/ingest-reports/ingest-summary.json").read_text())
    require(summary.get("status") == "complete" and summary.get("years") == YEARS, "Full 30-year corpus has not completed ingestion")
    archive_sha = file_sha(archive_path)
    scope_sha = file_sha(SOURCE / "country_scope.csv")
    ingest_sha = file_sha(SOURCE / "pipeline/ingest.py")
    require(archive_sha == summary["source_archive_sha256"], "Archive hash changed")
    require(scope_sha == summary["country_scope_sha256"] and ingest_sha == summary["ingest_code_sha256"], "Current scope/ingest code differs from processed binding")
    results, checked_files = [], []
    with tempfile.TemporaryDirectory(prefix="validation-", dir=output / "private") as tmp, zipfile.ZipFile(archive_path) as archive:
        names = archive.namelist()
        expected_names = {f"BACI_HS92_Y{year}_V202601.csv" for year in YEARS} | {"country_codes_V202601.csv", "product_codes_HS92_V202601.csv", "Readme.txt"}
        require(len(names) == len(set(names)) == 33 and set(names) == expected_names, "Archive member inventory differs from the fixed release")
        conn = duckdb.connect()
        conn.execute("SET memory_limit='1800MB'")
        conn.execute("SET threads=2")
        conn.execute("SET preserve_insertion_order=false")
        conn.execute(f"SET temp_directory={literal(tmp)}")
        initialize_references(conn, archive, SOURCE / "country_scope.csv")
        total_source_rows = 0
        for year in YEARS:
            source_report = json.loads((output / f"private/ingest-reports/ingest-{year}.json").read_text())
            require(source_report.get("year") == year and source_report.get("status") == "complete" and source_report.get("zip_crc_verified") is True and source_report.get("denominator_complete") is True and source_report.get("quarantined_rows") == 0 and source_report.get("duplicate_keys") == 0 and source_report.get("duplicate_extra_rows") == 0 and source_report.get("global_rows") == source_report.get("source_rows"), f"{year}: incomplete/anomalous source")
            require(source_report.get("source_archive_sha256") == archive_sha and source_report.get("source_version") == "202601" and source_report.get("source_revision") == "HS92", f"{year}: source report provenance changed")
            info = archive.getinfo(f"BACI_HS92_Y{year}_V202601.csv")
            expected_binding = {"archive_sha256": archive_sha, "source_member": info.filename, "source_member_crc32": f"{info.CRC:08x}", "source_member_uncompressed_bytes": info.file_size, "country_scope_sha256": scope_sha, "ingest_code_sha256": ingest_sha, "duckdb_version": duckdb.__version__}
            require(source_report["binding"] == expected_binding and source_report["source_member"] == info.filename, f"{year}: source/code/scope binding mismatch")
            output_files = source_report["output_files"]
            require(len(output_files) == 2 and {item["table"] for item in output_files} == {"flows", "global_product_suppliers"}, f"{year}: incomplete output manifest")
            for item in output_files:
                base = "release/data" if item["table"] == "flows" else "private"
                expected_path = f"{base}/{item['table']}/year={year}/part-00000.parquet"
                require(item["path"] == expected_path and file_sha(output / expected_path) == item["sha256"], f"{year}: output path/hash changed")
            result = check_year(conn, release, source_report, archive, archive_sha)
            results.append(result)
            total_source_rows += source_report["source_rows"]
            for table in ("flows", "exposures", "global_context") + (("link_observation_changes",) if year > 1995 else ()):
                path = release / "data" / table / f"year={year}/part-00000.parquet"
                checked_files.append({"path": str(path.relative_to(release)), "sha256": file_sha(path)})
            print(json.dumps(result), flush=True)
        conn.close()
    require(total_source_rows == summary["global_rows"] and sum(r["flow_rows"] for r in results) == summary["regional_rows"], "Full-corpus summary accounting mismatch")
    report = {"passed": True, "source_archive_sha256": archive_sha, "country_scope_sha256": scope_sha, "ingest_code_sha256": ingest_sha, "validator_code_sha256": file_sha(Path(__file__)), "years": results, "global_source_rows_processed": total_source_rows, "released_flow_rows": sum(x["flow_rows"] for x in results), "exposure_rows": sum(x["exposure_rows"] for x in results), "global_context_rows": sum(x["global_context_rows"] for x in results), "link_change_rows": sum(x["link_change_rows"] for x in results), "csv_source_round_trip_count": sum(x["csv_prefix_round_trips"] for x in results), "python_market_recomputation_count": sum(x["python_market_recomputations"] for x in results), "sample_design": "First five released source positions and first ten sorted markets per annual member. Technical prefix checks; not a random expert validation sample. All market and global product concentrations are additionally regrouped from exact input values in SQL.", "arithmetic_tolerance_hhi": TOLERANCE, "checked_data_files": checked_files, "limitations": "Software/integrity audit only. Underlying source reporting accuracy, economic causality, supplier replaceability and market forecasts are not validated."}
    validation_path.parent.mkdir(exist_ok=True)
    temporary = validation_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(validation_path)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--archive", required=True)
    args = parser.parse_args()
    run(args.output, args.archive)
