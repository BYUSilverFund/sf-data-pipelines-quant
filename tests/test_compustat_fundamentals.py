from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import polars as pl
from click.testing import CliRunner

from pipelines.compustat_fundamentals import (
    build_coverage_report,
    _store_periods,
    compustat_sql,
    eligible_company_mappings,
    field_definitions,
    select_gvkey_cohort,
)
from pipelines.__main__ import cli


class CompustatFundamentalsTests(unittest.TestCase):
    def test_coverage_report_records_nulls_missing_companies_and_duplicate_identities(self):
        annual = pl.DataFrame({
            "gvkey": ["000001", "000001"],
            "datadate": [date(2024, 12, 31), date(2024, 12, 31)],
            "fyear": [2024, 2024],
            "indfmt": ["INDL", "INDL"],
            "datafmt": ["STD", "STD"],
            "sale": [100.0, None],
        })
        report = build_coverage_report(
            ["000001", "000002"], annual, pl.DataFrame(), {
                "run_id": "run_test",
                "membership_snapshot_date": date(2026, 8, 31),
                "start_date": date(2024, 1, 1), "end_date": date(2026, 10, 28),
                "filters": {"indfmt": "INDL", "datafmt": "STD"},
                "report_identity_fields": ["period_frequency", "gvkey", "datadate", "indfmt", "datafmt"],
                "eligible_gvkey_count": 2,
                "excluded_format_counts": {"annual": [], "quarterly": []},
                "fields_missing_from_schema": {"annual": [], "quarterly": []},
            },
        )
        annual_summary = report["frequencies"]["annual"]
        self.assertEqual(annual_summary["duplicate_report_identity_rows"], 1)
        self.assertEqual(annual_summary["field_coverage"]["sale"], {"non_null_rows": 1, "null_rows": 1})
        self.assertEqual(annual_summary["companies_without_reports"], 1)
        self.assertEqual(report["company_coverage"][1]["status"], "no_reports_in_date_range_reason_unverified")

    def test_mapping_eligibility_keeps_missing_cik_and_excludes_conflicts(self):
        mapping = pl.DataFrame({
            "gvkey": ["000001", "000001", "000002", "000003", None],
            "mapping_status": ["mapped_current_cik_metadata", "missing_cik", "ambiguous_gvkey", "ambiguous_cik", "no_gvkey_match"],
        })
        self.assertEqual(eligible_company_mappings(mapping).get_column("gvkey").to_list(), ["000001"])

    def test_explicit_gvkey_cohort_must_be_unique_and_eligible(self):
        self.assertEqual(
            select_gvkey_cohort(["000003", "000001", "000002"], selected_gvkeys=["000002", "000001"]),
            ["000002", "000001"],
        )
        self.assertEqual(select_gvkey_cohort(["000003", "000001"], limit=1), ["000001"])
        for kwargs in (
            {"selected_gvkeys": ["000001", "000001"]},
            {"selected_gvkeys": ["000004"]},
            {"limit": 1, "selected_gvkeys": ["000001"]},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                select_gvkey_cohort(["000001", "000002"], **kwargs)

    def test_sql_uses_compustat_quarter_field_names_and_standard_filters(self):
        sql = compustat_sql("fundq", ["000001"], date(2024, 1, 1), date(2026, 10, 28))
        self.assertIn("atq", sql)
        self.assertIn("saleq", sql)
        self.assertIn("oancfy", sql)
        self.assertIn("capxy", sql)
        self.assertIn("cshoq", sql)
        self.assertIn("curcdq", sql)
        self.assertIn("indfmt = 'INDL'", sql)
        self.assertIn("consol = 'C'", sql)
        self.assertIn("popsrc = 'D'", sql)
        self.assertIn("datafmt = 'STD'", sql)
        self.assertNotIn(", at,", sql)
        annual_sql = compustat_sql("funda", ["000001"], date(2024, 1, 1), date(2026, 10, 28))
        self.assertIn("at,", annual_sql)
        self.assertIn("sale,", annual_sql)
        self.assertIn("oancf,", annual_sql)
        self.assertIn("dvpd", annual_sql)
        self.assertIn("curcd,", annual_sql)
        self.assertNotIn("atq", annual_sql)
        self.assertNotIn("rdq", annual_sql)

    def test_period_folder_identity_nulls_and_source_versioning(self):
        base = {
            "gvkey": "000001", "datadate": date(2026, 3, 31), "fyearq": 2026,
            "fqtr": 1, "fyr": 12, "currency": "USD", "rdq": None,
            "indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD",
            "atq": 120.0, "saleq": None,
        }
        frame = pl.DataFrame([base])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            count, _ = _store_periods(root, frame, "quarterly", {"000001": {"snapshot": "security_mappings/map.parquet"}}, "2026-10-28T12:00:00+00:00", {}, [])
            self.assertEqual(count, 1)
            report_dir = root / "2026/gvkey_000001/quarterly_2026-03-31_INDL_STD"
            report = json.loads((report_dir / "report.json").read_text())
            self.assertEqual(report["source_table"], "comp.fundq")
            self.assertEqual((report["indfmt"], report["datafmt"]), ("INDL", "STD"))
            self.assertEqual(report["fiscal_quarter"], 1)
            self.assertIn("currency", report["units"])
            self.assertIn("year-to-date", report["cash_flow_values_are"])
            self.assertEqual(report["source_field_units"]["oancfy"], "Millions of reported currency; fiscal year-to-date duration")
            self.assertEqual(report["source_field_units"]["cshoq"], "Millions of shares; period-end instant")
            self.assertIn("aggregate cash dividends paid", [item["item"] for item in report["requested_items_unavailable"]])
            self.assertEqual(pl.read_parquet(report_dir / "income_statement.parquet")["saleq"][0], None)
            self.assertEqual(pl.read_parquet(report_dir / "balance_sheet.parquet")["atq"][0], 120.0)
            cash_flow = pl.read_parquet(report_dir / "cash_flow.parquet")
            capital = pl.read_parquet(report_dir / "capital_metrics.parquet")
            self.assertIsNone(cash_flow["oancfy"][0])
            self.assertIn("oancfy", cash_flow.columns)
            self.assertIn("cshoq", capital.columns)
            self.assertNotIn("oancfy", capital.columns)

            annual = pl.DataFrame([{
                "gvkey": "000001", "datadate": date(2026, 3, 31), "fyear": 2026,
                "fyearq": None, "fqtr": None, "fyr": 12, "currency": "USD", "rdq": None, "indfmt": "INDL",
                "consol": "C", "popsrc": "D", "datafmt": "STD", "at": 120.0,
                "sale": 42.0,
            }])
            _store_periods(root, annual, "annual", {}, "2026-10-28T12:00:00+00:00", {}, [])
            self.assertTrue((root / "2026/gvkey_000001/annual_2026-03-31_INDL_STD/report.json").exists())
            self.assertTrue((root / "2026/gvkey_000001/quarterly_2026-03-31_INDL_STD/report.json").exists())

            _store_periods(root, frame, "quarterly", {"000001": {"snapshot": "security_mappings/map.parquet"}}, "2026-10-29T12:00:00+00:00", {}, [])
            snapshots = list((report_dir / "snapshots").iterdir())
            self.assertEqual(len(snapshots), 1)
            changed = pl.DataFrame([{**base, "saleq": 88.0}])
            _store_periods(root, changed, "quarterly", {}, "2026-10-30T12:00:00+00:00", {}, [])
            self.assertEqual(len(list((report_dir / "snapshots").iterdir())), 2)
            self.assertEqual(pl.read_parquet(report_dir / "income_statement.parquet")["saleq"][0], 88.0)

    def test_unchanged_period_skips_canonical_shared_filesystem_copies(self):
        frame = pl.DataFrame([{
            "gvkey": "000001", "datadate": date(2026, 3, 31), "fyearq": 2026,
            "fqtr": 1, "fyr": 12, "currency": "USD", "rdq": None,
            "indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD",
            "atq": 120.0, "saleq": 42.0,
        }])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = (root, frame, "quarterly", {}, "2026-10-28T12:00:00+00:00", {}, [])
            _store_periods(*args)
            with patch("pipelines.compustat_fundamentals.shutil.copyfile", side_effect=AssertionError):
                count, _ = _store_periods(*args)
            self.assertEqual(count, 1)

    def test_report_identity_separates_format_rows_with_the_same_period_key(self):
        frame = pl.DataFrame([
            {
                "gvkey": "000001", "datadate": date(2026, 3, 31), "fyearq": 2026,
                "fqtr": 1, "fyr": 12, "currency": "USD", "rdq": None,
                "indfmt": indfmt, "consol": "C", "popsrc": "D", "datafmt": datafmt,
                "atq": value, "saleq": value,
            }
            for indfmt, datafmt, value in (("INDL", "STD", 100.0), ("FS", "STD", 900.0))
        ])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            count, _ = _store_periods(root, frame, "quarterly", {}, "2026-10-28T12:00:00+00:00", {}, [])
            self.assertEqual(count, 2)
            industrial = root / "2026/gvkey_000001/quarterly_2026-03-31_INDL_STD"
            financial = root / "2026/gvkey_000001/quarterly_2026-03-31_FS_STD"
            self.assertTrue((industrial / "report.json").is_file())
            self.assertTrue((financial / "report.json").is_file())
            self.assertEqual(pl.read_parquet(industrial / "balance_sheet.parquet")["atq"][0], 100.0)
            self.assertEqual(pl.read_parquet(financial / "balance_sheet.parquet")["atq"][0], 900.0)

    def test_verified_units_keep_cash_paid_and_declared_dividends_distinct(self):
        annual = field_definitions("funda")
        quarterly = field_definitions("fundq")
        self.assertEqual(annual["dvpd"]["label"], "Cash Dividends Paid")
        self.assertIn("declared/provided", annual["dvt"]["unit"])
        self.assertEqual(annual["csho"]["unit"], "Millions of shares; period-end instant")
        self.assertIn("weighted average", annual["cshpri"]["unit"])
        self.assertIn("fiscal year-to-date", quarterly["oancfy"]["unit"])
        self.assertIn("quarterly duration", quarterly["piq"]["unit"])

    def test_compustat_cli_does_not_route_to_sec_collector(self):
        called = {}

        def fake_compustat(source, limit, start, end, output_root):
            called["args"] = (source, limit, start, end, output_root)

        from pipelines import __main__ as entrypoint
        original = entrypoint.run_compustat_fundamentals
        entrypoint.run_compustat_fundamentals = fake_compustat
        try:
            result = CliRunner().invoke(cli, ["compustat", "--start", "2024-01-01", "--end", "2026-10-28", "--limit", "5"])
        finally:
            entrypoint.run_compustat_fundamentals = original
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(called["args"][1], 5)
        self.assertEqual(called["args"][0], "development")


if __name__ == "__main__":
    unittest.main()
