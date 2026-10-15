"""Adversarial checks of the full-partition validator using real tiny outputs."""
from __future__ import annotations

from decimal import Decimal
import json
import shutil
import unittest
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

import test_semantics as semantics


class PartitionValidatorSemantics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = semantics.AnalyticalSemantics
        cls.fixture.setUpClass()
        cls.validator = semantics.load_pipeline("validate")
        cls.release = cls.fixture.output / "release"
        cls.paths = {}
        for year in (2023, 2024):
            for table, name in (("exposures", f"exposure-{year}.parquet"), ("global_context", f"context-{year}.parquet")):
                destination = cls.release / f"data/{table}/year={year}/part-00000.parquet"
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(cls.fixture.root / name, destination)
                if year == 2024:
                    cls.paths[table] = destination
        transition = cls.release / "data/link_observation_changes/year=2024/part-00000.parquet"
        transition.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cls.fixture.root / "transitions.parquet", transition)
        cls.paths["transitions"] = transition
        cls.paths["flows"] = cls.fixture.flow_path(2024)
        cls.original = {key: path.read_bytes() for key, path in cls.paths.items()}
        cls.report = json.loads((cls.fixture.output / "private/ingest-reports/ingest-2024.json").read_text())
        cls.archive = zipfile.ZipFile(cls.fixture.archive)
        cls.validator.initialize_references(cls.fixture.conn, cls.archive, semantics.PROJECT / "country_scope.csv")

    @classmethod
    def tearDownClass(cls):
        cls.archive.close()
        cls.fixture.tearDownClass()

    def tearDown(self):
        for key, path in self.paths.items():
            path.write_bytes(self.original[key])

    def check(self):
        return self.validator.check_year(self.fixture.conn, self.release, self.report, self.archive, self.fixture.archive_sha)

    def replace(self, table, transform):
        path = self.paths[table]
        schema = pq.ParquetFile(path).schema_arrow
        rows = semantics.rows_at(path)
        transform(rows)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)

    def test_real_fixture_passes_all_distribution_checks(self):
        result = self.check()
        self.assertTrue(result["passed"])
        self.assertTrue(result["all_market_concentrations_recomputed"])
        self.assertTrue(result["all_global_product_distributions_recomputed"])
        self.assertEqual(result["exact_global_value_1000usd"], "1055.123000000000000001")

    def test_redistributed_world_product_totals_fail_even_if_annual_sum_matches(self):
        def redistribute(rows):
            rows[0]["global_value_1000usd"] += Decimal(1)
            rows[1]["global_value_1000usd"] -= Decimal(1)
            rows[0]["global_value_usd"] += Decimal(1000)
            rows[1]["global_value_usd"] -= Decimal(1000)
        self.replace("global_context", redistribute)
        with self.assertRaisesRegex(AssertionError, "independent grouped"):
            self.check()

    def test_duplicate_market_context_and_change_keys_are_rejected(self):
        for table in ("exposures", "global_context", "transitions"):
            with self.subTest(table=table):
                self.replace(table, lambda rows: rows.append(dict(rows[0])))
                with self.assertRaisesRegex(AssertionError, "duplicate keys"):
                    self.check()
                self.paths[table].write_bytes(self.original[table])

    def test_nulls_wrong_scalars_flags_and_provenance_cannot_bypass_sql(self):
        cases = (
            ("exposures", "quantity_value_coverage_share", None),
            ("exposures", "total_value_1000usd", None),
            ("global_context", "global_supplier_hhi", None),
            ("exposures", "source_archive_sha256", None),
            ("flows", "exporter_in_scope", None),
            ("flows", "source_record_id", "bad-source-record-hash"),
        )
        for table, field, value in cases:
            with self.subTest(table=table, field=field):
                self.replace(table, lambda rows: rows[0].__setitem__(field, value))
                with self.assertRaises(AssertionError):
                    self.check()
                self.paths[table].write_bytes(self.original[table])

    def test_equal_count_wrong_transition_key_set_is_rejected(self):
        self.replace("transitions", lambda rows: rows[0].__setitem__("exporter_code", 48))
        with self.assertRaisesRegex(AssertionError, "transition key-set"):
            self.check()

    def test_failed_rerun_clears_stale_validation_approval(self):
        path = self.release / "reports/validation.json"
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps({"passed": True}))
        with self.assertRaisesRegex(AssertionError, "Full 30-year"):
            self.validator.run(self.fixture.output, self.fixture.archive)
        self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
