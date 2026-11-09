from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import polars as pl

from pipelines.fundamentals_query import (
    FORMAT_FILTERS,
    _identity_key,
    build_compustat_query_tables,
)
from pipelines.fundamentals_storage import persist_mapping_snapshot
from pipelines.ibkr_fundamentals_lookup import lookup_compustat_metric_by_barrid
from pipelines.utils.enums import DatabaseName
from pipelines.utils.fundamentals_schema import ANNUAL_FUNDAMENTAL_FIELDS, QUARTERLY_FUNDAMENTAL_FIELDS
from pipelines.utils.tables import Database


RUN_ID = "run_query_test"
MAPPING_DATE = date(2026, 8, 31)
ANNUAL_DATE = date(2024, 12, 31)
QUARTERLY_DATE = date(2025, 3, 31)
GVKEYS = ["000001", "000002", "000003", "000004", "000005"]


class FundamentalsQueryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.compustat_root = self.base / "archive" / "compustat"
        self.run_dir = self.compustat_root / "source_extracts" / RUN_ID
        self.run_dir.mkdir(parents=True)
        self._write_mapping()
        self._write_run()
        self._write_report("annual", ANNUAL_DATE, fyear=2023, metric="sale", value_a=100.0, value_b=111.0)
        self._write_report("quarterly", QUARTERLY_DATE, fyearq=2024, fqtr=1, metric="oancfy", value_a=1.0, value_b=2.5)

    def tearDown(self):
        self.temp.cleanup()

    def _write_mapping(self):
        rows = []
        def add(cusip, iid, gvkey, barrid, status="mapped_current_cik_metadata", barra_status="mapped"):
            rows.append({
                "membership_date": MAPPING_DATE, "cusip9": cusip,
                "russell_1000": True, "russell_2000": False,
                "gvkey": gvkey, "iid": iid, "cik": "0000000001",
                "compustat_efffrom": date(2020, 1, 1), "compustat_effthru": None,
                "mapping_source": "comp.sec_idhist:item=CUSIP; exact CUSIP9",
                "cik_source": "comp.company.cik current metadata",
                "mapping_status": status, "barrid": barrid,
                "issuerid": "issuer", "rootid": barrid, "instrument": "CS",
                "barra_start_date": date(2020, 1, 1), "barra_end_date": None,
                "barra_mapping_status": barra_status,
            })
        add("000000001", "01", "000001", "USAAA01")
        add("000000001", "02", "000001", "USAAA01")  # Same Barrid/report; must deduplicate.
        add("000000002", "01", "000001", "USAAA02")  # Another security for one company.
        add("000000003", "01", "000002", "USAAA03", status="ambiguous_gvkey")
        add("000000004", "01", "000003", None, barra_status="unmatched")
        add("000000005", "01", "000004", "USAAA05")
        add("000000006", "01", "000005", "USAAA05")  # Barrid conflicts across GVKEYs.
        self.mapping_path = persist_mapping_snapshot(
            self.compustat_root, pl.DataFrame(rows), MAPPING_DATE
        )

    def _write_run(self):
        common = {"indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD"}
        annual = pl.DataFrame({
            "gvkey": ["000001"], "datadate": [ANNUAL_DATE], "fyear": [2023], "fyr": [12],
            "fyearq": [None], "fqtr": [None], **{key: [value] for key, value in common.items()},
        })
        quarterly = pl.DataFrame({
            "gvkey": ["000001"], "datadate": [QUARTERLY_DATE], "fyear": [None], "fyr": [12],
            "fyearq": [2024], "fqtr": [1], **{key: [value] for key, value in common.items()},
        })
        annual.write_parquet(self.run_dir / "annual.parquet")
        quarterly.write_parquet(self.run_dir / "quarterly.parquet")
        (self.run_dir / "run.json").write_text(json.dumps({
            "run_id": RUN_ID,
            "selected_gvkeys": GVKEYS,
            "selected_gvkey_count": len(GVKEYS),
            "membership_snapshot_date": MAPPING_DATE.isoformat(),
            "mapping_snapshot": str(self.mapping_path),
            "filters": FORMAT_FILTERS,
            "start_date": "2024-01-01", "end_date": "2026-10-29",
            "excluded_format_counts": {"annual": [{"indfmt": "FS", "count": 1}], "quarterly": []},
        }))
        (self.run_dir / "coverage_report.json").write_text(json.dumps({
            "frequencies": {
                "annual": {"source_row_count": 1, "duplicate_report_identity_rows": 0},
                "quarterly": {"source_row_count": 1, "duplicate_report_identity_rows": 0},
            },
        }))
        (self.run_dir / "completion_report.json").write_text(json.dumps({
            "run_id": RUN_ID, "complete": True, "collector_exit_code": 0,
            "collector_process_exited": True,
            "missing_reports_count": 0, "missing_artifacts_count": 0,
            "identity_mismatches_count": 0, "sample_value_mismatches_count": 0,
            "source_row_counts": {"annual": 1, "quarterly": 1},
        }))

    def _write_report(self, frequency, datadate, *, metric, value_a, value_b, fyear=None, fyearq=None, fqtr=None):
        name = f"{frequency}_{datadate.isoformat()}_INDL_STD"
        period = self.compustat_root / str(datadate.year) / "gvkey_000001" / name
        version_ids = ("version-a", "version-b")
        for version_id, value in zip(version_ids, (value_a, value_b), strict=True):
            version_dir = period / "snapshots" / version_id
            version_dir.mkdir(parents=True, exist_ok=True)
            fields = ANNUAL_FUNDAMENTAL_FIELDS if frequency == "annual" else QUARTERLY_FUNDAMENTAL_FIELDS
            for statement, filename in (
                ("balance_sheet", "balance_sheet.parquet"),
                ("income_statement", "income_statement.parquet"),
                ("cash_flow", "cash_flow.parquet"),
                ("capital_metrics", "capital_metrics.parquet"),
            ):
                group = {
                    "balance_sheet": fields[:11],
                    "income_statement": fields[11:24],
                    "cash_flow": fields[24:38],
                    "capital_metrics": fields[38:],
                }[statement]
                data = {
                    "gvkey": ["000001"], "datadate": [datadate],
                    "fyear": [fyear], "fyearq": [fyearq], "fqtr": [fqtr], "fyr": [12],
                    "currency": ["USD"], "rdq": [date(2025, 2, 1)],
                    "indfmt": ["INDL"], "consol": ["C"], "popsrc": ["D"], "datafmt": ["STD"],
                }
                for field in group:
                    data[field] = [None]
                if metric in group:
                    data[metric] = [value]
                if metric == "sale" and "dvpd" in group:
                    data["dvpd"] = [None]
                pl.DataFrame(data).write_parquet(version_dir / filename)
            definitions = {
                metric: {
                    "label": metric,
                    "unit": "Millions of reported currency; fiscal year-to-date duration"
                    if frequency == "quarterly" and metric.endswith("y")
                    else "Millions of reported currency; period duration",
                    "period_semantics": "fiscal year-to-date duration"
                    if frequency == "quarterly" and metric.endswith("y")
                    else "period duration",
                }
            }
            report = {
                "period_frequency": frequency, "gvkey": "000001", "datadate": datadate.isoformat(),
                "indfmt": "INDL", "datafmt": "STD", "source_snapshot_id": version_id,
                "format_filters": FORMAT_FILTERS, "source_field_definitions": definitions,
                "fiscal_year": fyear if frequency == "annual" else fyearq,
                "fiscal_quarter": fqtr,
            }
            (version_dir / "report.json").write_text(json.dumps(report))
        (period / "latest_snapshot.json").write_text(json.dumps({
            "snapshot_id": "version-b", "snapshot_path": "snapshots/version-b",
        }))
        (period / "report.json").write_text((period / "snapshots/version-b/report.json").read_text())

    def _build(self, version_policy="latest", *, target=DatabaseName.DEVELOPMENT, rebuild_scope=False):
        with patch("pipelines.utils.tables.load_dotenv", return_value=None), patch.dict(
            os.environ, {"ROOT": str(self.base / "query-root")}, clear=False
        ):
            return build_compustat_query_tables(
                self.compustat_root, [RUN_ID], self.mapping_path, MAPPING_DATE,
                version_policy, target_database=target, rebuild_scope=rebuild_scope,
            )

    def _database(self):
        with patch("pipelines.utils.tables.load_dotenv", return_value=None), patch.dict(
            os.environ, {"ROOT": str(self.base / "query-root")}, clear=False
        ):
            return Database(DatabaseName.DEVELOPMENT)

    def _read_table(self, frequency):
        with patch("pipelines.utils.tables.load_dotenv", return_value=None), patch.dict(
            os.environ, {"ROOT": str(self.base / "query-root")}, clear=False
        ):
            db = Database(DatabaseName.DEVELOPMENT)
            table = db.fundamentals_annual_table if frequency == "annual" else db.fundamentals_quarterly_table
            return table.read()

    def test_tables_are_lazy_and_annual_quarterly_rows_keep_fiscal_dates_and_nulls(self):
        result = self._build()
        annual_lazy = self._read_table("annual")
        quarterly_lazy = self._read_table("quarterly")
        self.assertIsInstance(annual_lazy, pl.LazyFrame)
        self.assertIsInstance(quarterly_lazy, pl.LazyFrame)
        annual, quarterly = annual_lazy.collect(), quarterly_lazy.collect()
        self.assertEqual(annual.height, 2)
        self.assertEqual(quarterly.height, 2)
        self.assertEqual(set(annual.get_column("barrid")), {"USAAA01", "USAAA02"})
        self.assertEqual(annual.get_column("gvkey").unique().to_list(), ["000001"])
        self.assertEqual(annual.get_column("fyear").unique().to_list(), [2023])
        self.assertEqual(annual.get_column("datadate").unique().to_list(), [ANNUAL_DATE])
        self.assertEqual(annual.get_column("sale").unique().to_list(), [111.0])
        self.assertEqual(annual.get_column("dvpd").null_count(), 2)
        self.assertEqual(quarterly.get_column("fyearq").unique().to_list(), [2024])
        self.assertEqual(quarterly.get_column("fqtr").unique().to_list(), [1])
        self.assertEqual(quarterly.get_column("datadate").unique().to_list(), [QUARTERLY_DATE])
        self.assertEqual(quarterly.get_column("oancfy").unique().to_list(), [2.5])
        definitions = json.loads(quarterly.get_column("source_field_definitions_json")[0])
        self.assertEqual(definitions["oancfy"]["period_semantics"], "fiscal year-to-date duration")
        self.assertIn("/snapshots/version-b/", quarterly.get_column("source_cash_flow_path")[0])
        self.assertIn("/snapshots/version-b/report.json", quarterly.get_column("source_report_path")[0])
        self.assertEqual(result["row_counts"], {"annual": 2, "quarterly": 2})
        self.assertEqual(Path(result["annual_paths"][0]).name, "fundamentals_annual_2024.parquet")
        self.assertEqual(Path(result["quarterly_paths"][0]).name, "fundamentals_quarterly_2025.parquet")

    def test_ambiguous_and_missing_mappings_are_reported_without_rows(self):
        result = self._build()
        manifest = json.loads(Path(result["manifest_path"]).read_text())
        exclusions = manifest["mapping_exclusions"]
        self.assertEqual(exclusions["ambiguous_company_mapping_count"], 1)
        self.assertGreaterEqual(exclusions["missing_barra_mapping_count"], 1)
        self.assertEqual(exclusions["ambiguous_barrid_mapping_count"], 1)
        annual = self._read_table("annual").collect()
        self.assertNotIn("000002", annual.get_column("gvkey").to_list())
        self.assertNotIn("000004", annual.get_column("gvkey").to_list())
        self.assertNotIn("000005", annual.get_column("gvkey").to_list())
        self.assertEqual(manifest["format_exclusions"]["annual"]["reported_by_collection"][0]["counts"][0]["indfmt"], "FS")

    def test_specific_version_policy_uses_metadata_and_values_from_one_snapshot(self):
        key = _identity_key("annual", {
            "gvkey": "000001", "datadate": ANNUAL_DATE,
            "indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD",
        })
        versions = {
            key: "version-a",
            _identity_key("quarterly", {
                "gvkey": "000001", "datadate": QUARTERLY_DATE,
                "indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD",
            }): "version-b",
        }
        result = self._build(versions)
        annual = self._read_table("annual").collect()
        self.assertEqual(annual.get_column("sale").unique().to_list(), [100.0])
        self.assertEqual(annual.get_column("source_snapshot_id").unique().to_list(), ["version-a"])
        self.assertIn("/snapshots/version-a/", annual.get_column("source_income_statement_path")[0])
        manifest = json.loads(Path(result["manifest_path"]).read_text())
        self.assertEqual(manifest["extraction_version_policy"], versions)

    def test_repeatable_write_and_scope_conflicts_require_explicit_rebuild(self):
        first = self._build()
        first_rows = self._read_table("annual").collect()
        second = self._build()
        second_rows = self._read_table("annual").collect()
        self.assertEqual(first["materialization_id"], second["materialization_id"])
        self.assertTrue(first_rows.equals(second_rows))
        key = _identity_key("annual", {
            "gvkey": "000001", "datadate": ANNUAL_DATE,
            "indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD",
        })
        with self.assertRaisesRegex(ValueError, "different run/mapping/version scope"):
            self._build({key: "version-a", _identity_key("quarterly", {
                "gvkey": "000001", "datadate": QUARTERLY_DATE,
                "indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD",
            }): "version-a"})
        rebuilt = self._build({key: "version-a", _identity_key("quarterly", {
            "gvkey": "000001", "datadate": QUARTERLY_DATE,
            "indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD",
        }): "version-a"}, rebuild_scope=True)
        self.assertNotEqual(first["materialization_id"], rebuilt["materialization_id"])
        annual = self._read_table("annual").collect()
        self.assertEqual(annual.get_column("source_snapshot_id").unique().to_list(), ["version-a"])

    def test_quarterly_same_date_distinct_fiscal_periods_are_retained(self):
        source = pl.read_parquet(self.run_dir / "quarterly.parquet")
        second_source = source.with_columns(
            pl.lit(2).cast(source.schema["fqtr"]).alias("fqtr"),
            pl.lit(2025).cast(source.schema["fyearq"]).alias("fyearq"),
        )
        pl.concat([source, source.clone(), second_source]).write_parquet(self.run_dir / "quarterly.parquet")
        coverage_path = self.run_dir / "coverage_report.json"
        coverage = json.loads(coverage_path.read_text())
        coverage["frequencies"]["quarterly"].update({"source_row_count": 3, "duplicate_report_identity_rows": 2})
        coverage_path.write_text(json.dumps(coverage))
        completion_path = self.run_dir / "completion_report.json"
        completion = json.loads(completion_path.read_text())
        completion["source_row_counts"]["quarterly"] = 3
        completion["unique_report_identity_counts"] = {"annual": 1, "quarterly": 1}
        completion_path.write_text(json.dumps(completion))

        period = self.compustat_root / "2025/gvkey_000001/quarterly_2025-03-31_INDL_STD/snapshots/version-b"
        for filename in ("balance_sheet.parquet", "income_statement.parquet", "cash_flow.parquet", "capital_metrics.parquet"):
            path = period / filename
            data = pl.read_parquet(path)
            if "saleq" in data.columns:
                data = data.with_columns(pl.col("saleq").cast(pl.Float64))
            if "oancfy" in data.columns:
                data = data.with_columns(pl.col("oancfy").cast(pl.Float64))
            second = data.with_columns(
                pl.lit(2).cast(data.schema["fqtr"]).alias("fqtr"),
                pl.lit(2025).cast(data.schema["fyearq"]).alias("fyearq"),
            )
            if "saleq" in data.columns:
                second = second.with_columns(pl.lit(222.0).cast(pl.Float64).alias("saleq"))
            if "oancfy" in data.columns:
                second = second.with_columns(pl.lit(3.0).cast(pl.Float64).alias("oancfy"))
            pl.concat([data, data.clone(), second]).write_parquet(path)
        report_path = period / "report.json"
        report = json.loads(report_path.read_text())
        report["fiscal_year"] = [2024, 2025]
        report["fiscal_quarter"] = [1, 2]
        report_path.write_text(json.dumps(report))
        archive_report = self.compustat_root / "2025/gvkey_000001/quarterly_2025-03-31_INDL_STD/report.json"
        archive_report.write_text(json.dumps(report))

        result = self._build()
        quarterly = self._read_table("quarterly").collect()
        self.assertEqual(quarterly.height, 4)  # Two valid Barrids times two fiscal periods.
        self.assertEqual(set(quarterly.get_column("fqtr")), {1, 2})
        self.assertEqual(set(quarterly.get_column("fyearq")), {2024, 2025})
        self.assertEqual(set(quarterly.get_column("oancfy")), {2.5, 3.0})
        manifest = json.loads(Path(result["manifest_path"]).read_text())
        self.assertEqual(manifest["build_status"], "complete")
        self.assertEqual(manifest["quarantined_report_identity_count"], 0)
        self.assertEqual(manifest["duplicate_resolution"][RUN_ID]["quarterly"]["exact_duplicate_rows_removed_in_derived_build"], 1)
        self.assertIn("fyearq", manifest["query_key_fields"]["quarterly"])
        self.assertIn("fqtr", manifest["query_key_fields"]["quarterly"])

    def test_unresolved_conflict_for_same_fiscal_identity_is_quarantined_and_partial(self):
        source = pl.read_parquet(self.run_dir / "quarterly.parquet").with_columns(pl.lit(100.0).alias("saleq"))
        conflict = source.with_columns(pl.lit(101.0).alias("saleq"))
        pl.concat([source, conflict]).write_parquet(self.run_dir / "quarterly.parquet")
        coverage_path = self.run_dir / "coverage_report.json"
        coverage = json.loads(coverage_path.read_text())
        coverage["frequencies"]["quarterly"].update({"source_row_count": 2, "duplicate_report_identity_rows": 1})
        coverage_path.write_text(json.dumps(coverage))
        completion_path = self.run_dir / "completion_report.json"
        completion = json.loads(completion_path.read_text())
        completion["source_row_counts"]["quarterly"] = 2
        completion["unique_report_identity_counts"] = {"annual": 1, "quarterly": 1}
        completion_path.write_text(json.dumps(completion))

        result = self._build()
        manifest = json.loads(Path(result["manifest_path"]).read_text())
        self.assertEqual(manifest["row_counts"]["quarterly"], 0)
        self.assertEqual(manifest["build_status"], "partial")
        self.assertEqual(manifest["quarantined_report_identity_count"], 1)
        self.assertEqual(
            manifest["quarantined_report_identities"][0]["reason"],
            "conflicting_values_for_same_fiscal_identity",
        )

    def test_incomplete_run_is_rejected_before_materialization(self):
        (self.run_dir / "completion_report.json").unlink()
        with self.assertRaisesRegex(ValueError, "Cannot read valid JSON metadata"):
            self._build()

    def test_existing_barrid_lookup_agrees_with_materialized_metric(self):
        self._build()
        materialized = self._read_table("annual").filter(
            pl.col("barrid") == "USAAA01"
        ).select("sale").collect().item()
        lookup = lookup_compustat_metric_by_barrid(
            self.compustat_root, "USAAA01", MAPPING_DATE, 2023, "annual", "sale",
            "income_statement", mapping_snapshot=pl.read_parquet(self.mapping_path), source_snapshot_id="latest",
        )
        self.assertEqual(lookup["status"], "ok")
        self.assertEqual(materialized, lookup["value"])


if __name__ == "__main__":
    unittest.main()
