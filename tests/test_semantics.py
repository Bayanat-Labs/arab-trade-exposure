"""Independent semantic checks for the Arab Trade Exposure release.

The expected examples in this file are deliberately hand calculated. They do
not import implementation constants for country membership or expected scores.
Run with: python -m unittest discover -s tests -p 'test_semantics.py' -v
"""
from __future__ import annotations

import csv
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import tempfile
import unittest
import zipfile
from decimal import Decimal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


PROJECT = Path(__file__).resolve().parents[1]
EXPECTED_CURRENT_CODES = {
    12, 48, 174, 262, 275, 368, 400, 414, 422, 434, 478,
    504, 512, 634, 682, 706, 729, 760, 784, 788, 818, 887,
}


def load_pipeline(name):
    spec = importlib.util.spec_from_file_location(name, PROJECT / "pipeline" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_archive(path, records_by_year, *, compression=zipfile.ZIP_DEFLATED):
    """Complete tiny archive, using authentic official country/product lookups."""
    with zipfile.ZipFile(path, "w", compression=compression) as archive:
        for name in ("country_codes_V202601.csv", "product_codes_HS92_V202601.csv"):
            archive.writestr(name, (PROJECT / "source_metadata" / name).read_bytes())
        archive.writestr("Readme.txt", (PROJECT / "source_metadata/Readme.txt").read_bytes())
        for year in range(1995, 2025):
            stream = io.StringIO(newline="")
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(("t", "i", "j", "k", "v", "q"))
            writer.writerows(records_by_year.get(year, []))
            archive.writestr(f"BACI_HS92_Y{year}_V202601.csv", stream.getvalue())


def rows_at(path):
    # ParquetFile avoids implicit year partition inference colliding with file year.
    return pq.ParquetFile(path).read().to_pylist()


class CountryIdentitySemantics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with (PROJECT / "country_scope.csv").open(newline="", encoding="utf-8-sig") as handle:
            cls.scope = list(csv.DictReader(handle))
        with (PROJECT / "source_metadata/country_codes_V202601.csv").open(
            newline="", encoding="utf-8-sig"
        ) as handle:
            cls.official = {
                int(row["country_code"]): row for row in csv.DictReader(handle)
            }

    def test_22_modern_identities_and_one_separate_historical_area(self):
        current = {
            int(row["baci_code"])
            for row in self.scope
            if row["current_member"].strip().lower() == "true"
        }
        all_codes = [int(row["baci_code"]) for row in self.scope]
        self.assertEqual(current, EXPECTED_CURRENT_CODES)
        self.assertEqual(set(all_codes), EXPECTED_CURRENT_CODES | {736})
        self.assertEqual(len(all_codes), 23)
        self.assertEqual(len(all_codes), len(set(all_codes)))

    def test_every_selected_identity_matches_official_archive_lookup(self):
        for row in self.scope:
            with self.subTest(code=row["baci_code"]):
                official = self.official[int(row["baci_code"])]
                self.assertEqual(row["country_iso3"], official["country_iso3"])
                self.assertEqual(row["country_name"], official["country_name"])

    def test_sudan_iso3_is_not_a_unique_source_identity(self):
        sudan = [row for row in self.scope if row["country_iso3"] == "SDN"]
        self.assertEqual({int(row["baci_code"]) for row in sudan}, {729, 736})
        by_code = {int(row["baci_code"]): row for row in sudan}
        self.assertEqual(by_code[729]["scope_status"], "current_member")
        self.assertEqual(by_code[736]["scope_status"], "historical_predecessor")
        self.assertEqual(by_code[736]["current_member"].lower(), "false")
        self.assertNotEqual(by_code[729]["country_name"], by_code[736]["country_name"])

    def test_south_sudan_is_not_an_independently_selected_country(self):
        self.assertEqual(self.official[728]["country_name"], "South Sudan")
        self.assertNotIn(728, {int(row["baci_code"]) for row in self.scope})

    def test_known_baci_codes_are_not_assumed_numeric_iso_codes(self):
        examples = {
            174: "Comoros", 262: "Djibouti", 275: "State of Palestine",
            706: "Somalia", 887: "Yemen", 251: "France",
        }
        for code, name in examples.items():
            with self.subTest(code=code):
                self.assertEqual(self.official[code]["country_name"], name)
        # Duplicate ISO identities also occur among nonregional partners.
        self.assertEqual(self.official[56]["country_iso3"], "BEL")
        self.assertEqual(self.official[58]["country_iso3"], "BEL")
        self.assertNotEqual(self.official[56]["country_name"], self.official[58]["country_name"])

    def test_scope_policy_records_historical_context(self):
        policy = json.loads((PROJECT / "country_scope_policy.json").read_text())
        self.assertEqual(policy["current_members"], 22)
        self.assertEqual(policy["source_areas"], 23)
        self.assertEqual(policy["historical_source_code"], 736)


class AnalyticalSemantics(unittest.TestCase):
    """Real ingestion and derivation, checked against hand-computed examples."""

    @classmethod
    def setUpClass(cls):
        cls.ingest = load_pipeline("ingest")
        cls.derive = load_pipeline("derive")
        cls.temporary = tempfile.TemporaryDirectory(prefix="arab-trade-independent-tests-")
        cls.root = Path(cls.temporary.name)
        cls.archive = cls.root / "fixture.zip"
        cls.output = cls.root / "build"
        cls.records = {
            2023: [
                (2023, 76, 818, "010111", "60", "NA"),
                (2023, 643, 818, "010111", "30", "0.000"),
                (2023, 12, 818, "010111", "10", "2"),
                # Entirely outside the collection; still essential to world context.
                (2023, 156, 842, "010111", "900", "10"),
                (2023, 818, 156, "010111", "20", "1"),
                (2023, 818, 12, "010111", "20", ""),
                (2023, 736, 818, "010119", "4", "NA"),
                (2023, 729, 818, "010119", "6", "2"),
                (2023, 728, 156, "010119", "100", "3"),
                (2023, 818, 76, "010119", "0.123000000000000001", "0.000000000000000001"),
            ],
            2024: [
                (2024, 643, 818, "010111", "50", "1"),
                (2024, 12, 818, "010111", "20", "1"),
                (2024, 842, 818, "010111", "30", "NA"),
                (2024, 156, 842, "010111", "900", "10"),
                (2024, 818, 156, "010111", "20", "1"),
                (2024, 818, 12, "010111", "20", "1"),
                (2024, 729, 818, "010119", "8", "2"),
                (2024, 818, 76, "010119", "0.123000000000000001", "NA"),
                (2024, 76, 818, "9999AA", "7", "NA"),
            ],
        }
        write_archive(cls.archive, cls.records)
        cls.archive_sha = hashlib.sha256(cls.archive.read_bytes()).hexdigest()
        cls.ingest.run(cls.archive, cls.output, PROJECT / "country_scope.csv", (2023, 2024), cls.archive_sha)
        cls.flows = {}
        cls.exposures = {}
        cls.context = {}
        cls.conn = duckdb.connect()
        for year in (2023, 2024):
            flows_path = cls.flow_path(year)
            exposure_path = cls.root / f"exposure-{year}.parquet"
            context_path = cls.root / f"context-{year}.parquet"
            cls.derive.derive_year(cls.conn, flows_path, cls.supplier_path(year), year, exposure_path, context_path)
            cls.flows[year] = rows_at(flows_path)
            cls.exposures[year] = rows_at(exposure_path)
            cls.context[year] = rows_at(context_path)
        transition_path = cls.root / "transitions.parquet"
        cls.derive.derive_transition(cls.conn, cls.flow_path(2023), cls.flow_path(2024), 2023, 2024, transition_path)
        cls.transitions = rows_at(transition_path)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        cls.temporary.cleanup()

    @classmethod
    def flow_path(cls, year):
        return cls.output / f"release/data/flows/year={year}/part-00000.parquet"

    @classmethod
    def supplier_path(cls, year):
        return cls.output / f"private/global_product_suppliers/year={year}/part-00000.parquet"

    def market(self, year, country, direction, product):
        rows = [r for r in self.exposures[year] if (r["country_code"], r["direction"], r["hs6"]) == (country, direction, product)]
        self.assertEqual(len(rows), 1)
        return rows[0]

    def test_full_world_denominator_and_directional_rankings(self):
        imports = self.market(2023, 818, "import", "010111")
        self.assertEqual(imports["total_value_1000usd"], Decimal(100))
        self.assertEqual(imports["total_value_usd"], Decimal(100000))
        self.assertEqual(imports["partner_count"], 3)
        self.assertEqual(imports["top1_partner_code"], 76)
        self.assertEqual(imports["top3_partner_codes"], [76, 643, 12])
        self.assertAlmostEqual(imports["top1_share"], 0.6)
        self.assertAlmostEqual(imports["hhi"], 0.46)
        self.assertAlmostEqual(imports["effective_partner_count"], 1 / 0.46)
        self.assertAlmostEqual(imports["intra_current_member_share"], 0.1)
        exports = self.market(2023, 818, "export", "010111")
        self.assertEqual(exports["total_value_1000usd"], Decimal(40))
        self.assertEqual(exports["top1_partner_code"], 12)  # ties: BACI code ascending
        self.assertEqual(exports["top3_partner_codes"], [12, 156])
        self.assertAlmostEqual(exports["hhi"], 0.5)
        world = next(r for r in self.context[2023] if r["hs6"] == "010111")
        self.assertEqual(world["global_value_1000usd"], Decimal(1040))
        self.assertEqual(world["supplier_count"], 5)
        self.assertEqual(world["top1_exporter_code"], 156)
        self.assertEqual(world["top3_exporter_codes"], [156, 76, 818])
        self.assertEqual(world["top3_exporter_value_1000usd"], Decimal(1000))
        self.assertAlmostEqual(world["top1_exporter_share"], 900 / 1040)
        self.assertAlmostEqual(world["global_supplier_hhi"], 816200 / 1081600)
        # Nonregional flow must never leak into published regional flow rows.
        self.assertFalse(any(r["exporter_code"] == 156 and r["importer_code"] == 842 for r in self.flows[2023]))
        self.assertFalse(any(r["exporter_code"] == 728 for r in self.flows[2023]))

    def test_exact_scalars_quantity_states_and_record_lineage(self):
        by_key = {(r["exporter_code"], r["importer_code"], r["hs6"]): r for r in self.flows[2023]}
        self.assertIsNone(by_key[(76, 818, "010111")]["quantity_tonnes"])
        self.assertTrue(by_key[(76, 818, "010111")]["quantity_missing"])
        zero = by_key[(643, 818, "010111")]
        self.assertEqual(zero["quantity_tonnes"], Decimal(0))
        self.assertEqual(zero["q_raw"], "0.000")
        self.assertFalse(zero["quantity_missing"])
        self.assertFalse(zero["quantity_positive"])
        self.assertIsNone(by_key[(818, 12, "010111")]["quantity_tonnes"])
        exact = by_key[(818, 76, "010119")]
        self.assertEqual(exact["trade_value_1000usd"], Decimal("0.123000000000000001"))
        self.assertEqual(exact["quantity_tonnes"], Decimal("0.000000000000000001"))
        market = self.market(2023, 818, "import", "010111")
        self.assertEqual((market["positive_quantity_rows"], market["missing_quantity_rows"], market["nonpositive_quantity_rows"]), (1, 1, 1))
        self.assertEqual(market["quantity_covered_value_1000usd"], Decimal(10))
        self.assertAlmostEqual(market["quantity_value_coverage_share"], 0.1)
        for row in self.flows[2023]:
            ordinal = row["source_record_ordinal"]
            raw = self.records[2023][ordinal - 1]
            self.assertEqual((row["exporter_code"], row["importer_code"], row["hs6"], row["v_raw"], row["q_raw"]), raw[1:])
            self.assertEqual(row["source_archive_sha256"], self.archive_sha)
            expected_id = hashlib.sha256(json.dumps([self.archive_sha, "BACI_HS92_Y2023_V202601.csv", str(ordinal)], separators=(",", ":")).encode()).hexdigest()
            self.assertEqual(row["source_record_id"], expected_id)
        self.assertEqual(len({r["source_record_id"] for r in self.flows[2023]}), 8)

    def test_legacy_product_and_shared_iso_identities_stay_separate(self):
        legacy = next(r for r in self.flows[2024] if r["hs6"] == "9999AA")
        self.assertFalse(legacy["product_is_standard_hs6"])
        self.assertFalse(self.market(2024, 818, "import", "9999AA")["product_is_standard_hs6"])
        self.assertFalse(next(r for r in self.context[2024] if r["hs6"] == "9999AA")["product_is_standard_hs6"])
        sudan = [r for r in self.flows[2023] if r["exporter_code"] in (729, 736)]
        self.assertEqual({r["exporter_iso3"] for r in sudan}, {"SDN"})
        self.assertEqual({r["exporter_code"] for r in sudan}, {729, 736})
        historical = self.market(2023, 736, "export", "010119")
        self.assertTrue(historical["country_historical_predecessor"])
        self.assertFalse(historical["country_current_member"])
        imports = self.market(2023, 818, "import", "010119")
        self.assertEqual(imports["partner_count"], 2)
        self.assertEqual(imports["top3_partner_codes"], [729, 736])
        self.assertAlmostEqual(imports["hhi"], 0.52)
        self.assertEqual(imports["intra_scope_value_1000usd"], Decimal(10))
        self.assertEqual(imports["intra_current_member_value_1000usd"], Decimal(6))

    def test_transitions_use_record_presence_and_same_vintage_adjacent_years(self):
        observed = {(r["exporter_code"], r["importer_code"], r["hs6"], r["observation_change"]) for r in self.transitions}
        self.assertEqual(observed, {(76, 818, "010111", "no_longer_observed"), (736, 818, "010119", "no_longer_observed"), (842, 818, "010111", "newly_observed"), (76, 818, "9999AA", "newly_observed")})
        for row in self.transitions:
            added = row["observation_change"] == "newly_observed"
            self.assertEqual(row["present_previous"], not added)
            self.assertEqual(row["present_current"], added)
            self.assertIsNone(row["previous_value_1000usd"] if added else row["current_value_1000usd"])
        # Derivation also distinguishes a hypothetical present zero from absence;
        # production ingestion correctly disallows zero-valued source flows.
        schema = pq.ParquetFile(self.flow_path(2023)).schema_arrow
        previous = [dict(r, trade_value_1000usd=Decimal(0)) for r in self.flows[2023][:2]]
        current = [dict(previous[0], year=2024, trade_value_1000usd=Decimal(5))]
        old_path, new_path = self.root / "zero-old.parquet", self.root / "zero-new.parquet"
        pq.write_table(pa.Table.from_pylist(previous, schema=schema), old_path)
        pq.write_table(pa.Table.from_pylist(current, schema=schema), new_path)
        target = self.root / "zero-transitions.parquet"
        self.derive.derive_transition(self.conn, old_path, new_path, 2023, 2024, target)
        changes = rows_at(target)
        self.assertEqual(len(changes), 1)
        self.assertTrue(changes[0]["present_previous"])
        self.assertEqual(changes[0]["previous_value_1000usd"], Decimal(0))
        self.assertIsNone(changes[0]["current_value_1000usd"])
        with self.assertRaisesRegex(ValueError, "adjacent"):
            self.derive.derive_transition(self.conn, old_path, new_path, 2022, 2024, target)
        current[0]["source_archive_sha256"] = "f" * 64
        pq.write_table(pa.Table.from_pylist(current, schema=schema), new_path)
        with self.assertRaisesRegex(ValueError, "provenance"):
            self.derive.derive_transition(self.conn, old_path, new_path, 2023, 2024, target)

    def test_decimal_limits_unknown_products_and_duplicates_fail_closed(self):
        valid = ["1", "0.000000000000000001", "0.123000000000000001", "99999999999999999999.999999999999999999", "1e-18", "1.0000000000000000000", "10000000000000000000", "0e-999"]
        invalid = ["0.0000000000000000001", "1.1230000000000000001", "100000000000000000000", "1e-19", "nan", "inf", "1e+wat", "1e9999"]
        for scalar in valid + invalid:
            with self.subTest(scalar=scalar):
                accepted = self.conn.execute(f"SELECT {self.ingest.decimal_exact_sql('scalar')} FROM (SELECT ?::VARCHAR AS scalar)", [scalar]).fetchone()[0]
                self.assertEqual(accepted, scalar in valid)
        cases = {
            "unknown-product": [(2023, 76, 818, "000000", "1", "NA")],
            "duplicate": [(2023, 76, 818, "010111", "1", "NA")] * 2,
            "inexact": [(2023, 76, 818, "010111", "1.0000000000000000001", "1")],
            "zero-value": [(2023, 76, 818, "010111", "0", "1")],
        }
        for name, records in cases.items():
            with self.subTest(case=name):
                archive, output = self.root / f"bad-{name}.zip", self.root / f"bad-{name}"
                write_archive(archive, {2023: records})
                with self.assertRaisesRegex(ValueError, "quarantine"):
                    self.ingest.run(archive, output, PROJECT / "country_scope.csv", (2023,))
                report = json.loads((output / "private/ingest-reports/ingest-2023.json").read_text())
                self.assertEqual(report["status"], "failed")
                self.assertFalse(report["denominator_complete"])
                self.assertFalse((output / "release/data/flows/year=2023/part-00000.parquet").exists())

    def test_archive_integrity_and_hash_verified_idempotent_recovery(self):
        report_path = self.output / "private/ingest-reports/ingest-2023.json"
        previous = json.loads(report_path.read_text())
        flow_mtime = self.flow_path(2023).stat().st_mtime_ns
        self.ingest.run(self.archive, self.output, PROJECT / "country_scope.csv", (2023,), self.archive_sha)
        self.assertEqual(self.flow_path(2023).stat().st_mtime_ns, flow_mtime)
        # A changed payload cannot be accepted merely because a completion file exists.
        self.supplier_path(2023).write_bytes(b"broken parquet payload")
        self.ingest.run(self.archive, self.output, PROJECT / "country_scope.csv", (2023,), self.archive_sha)
        repaired = json.loads(report_path.read_text())
        self.assertEqual(hashlib.sha256(self.supplier_path(2023).read_bytes()).hexdigest(), next(f["sha256"] for f in repaired["output_files"] if f["table"] == "global_product_suppliers"))
        self.assertEqual(sorted((r["hs6"], r["exporter_code"], r["trade_value_1000usd"]) for r in rows_at(self.supplier_path(2023))), sorted([
            ("010111", 12, Decimal(10)), ("010111", 76, Decimal(60)),
            ("010111", 156, Decimal(900)), ("010111", 643, Decimal(30)),
            ("010111", 818, Decimal(40)), ("010119", 728, Decimal(100)),
            ("010119", 729, Decimal(6)), ("010119", 736, Decimal(4)),
            ("010119", 818, Decimal("0.123000000000000001")),
        ]))
        # An incomplete output manifest must force reprocessing, too.
        truncated = json.loads(report_path.read_text())
        truncated["output_files"] = [f for f in truncated["output_files"] if f["table"] == "flows"]
        report_path.write_text(json.dumps(truncated))
        self.supplier_path(2023).unlink()
        self.ingest.run(self.archive, self.output, PROJECT / "country_scope.csv", (2023,), self.archive_sha)
        self.assertTrue(self.supplier_path(2023).is_file())
        with self.assertRaisesRegex(ValueError, "SHA256"):
            self.ingest.run(self.archive, self.output, PROJECT / "country_scope.csv", (2023,), "0" * 64)
        corrupt = self.root / "corrupt-crc.zip"
        write_archive(corrupt, {2023: self.records[2023]}, compression=zipfile.ZIP_STORED)
        with zipfile.ZipFile(corrupt) as archive:
            info = archive.getinfo("BACI_HS92_Y2023_V202601.csv")
        payload = bytearray(corrupt.read_bytes())
        name_length, extra_length = struct.unpack_from("<HH", payload, info.header_offset + 26)
        data_start = info.header_offset + 30 + name_length + extra_length
        payload[data_start + info.file_size - 3] ^= 1
        corrupt.write_bytes(payload)
        with self.assertRaises(zipfile.BadZipFile):
            self.ingest.run(corrupt, self.root / "corrupt-build", PROJECT / "country_scope.csv", (2023,))


if __name__ == "__main__":
    unittest.main(verbosity=2)
