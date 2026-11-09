from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import polars as pl

from pipelines.ibkr_fundamentals_lookup import lookup_compustat_metric_by_barrid


def security_mapping() -> pl.DataFrame:
    return pl.DataFrame([
        {
            "membership_date": date(2026, 8, 31),
            "gvkey": "001000", "cusip9": "000000001", "iid": "01", "cik": "0000000100",
            "barrid": "USAA001", "issuerid": "A00001", "rootid": "USAA001",
            "compustat_efffrom": date(2020, 1, 1), "compustat_effthru": None,
            "barra_start_date": date(2020, 1, 1), "barra_end_date": None,
            "mapping_status": "mapped_current_cik_metadata", "barra_mapping_status": "mapped",
        },
        {
            "membership_date": date(2026, 8, 31),
            "gvkey": "001000", "cusip9": "000000002", "iid": "02", "cik": "0000000100",
            "barrid": "USAA002", "issuerid": "A00001", "rootid": "USAA002",
            "compustat_efffrom": date(2020, 1, 1), "compustat_effthru": None,
            "barra_start_date": date(2020, 1, 1), "barra_end_date": None,
            "mapping_status": "mapped_current_cik_metadata", "barra_mapping_status": "mapped",
        },
        {
            "membership_date": date(2026, 8, 31),
            "gvkey": "002000", "cusip9": "000000003", "iid": "01", "cik": "0000000200",
            "barrid": "USAA003", "issuerid": "A00002", "rootid": "USAA003",
            "compustat_efffrom": date(2020, 1, 1), "compustat_effthru": None,
            "barra_start_date": date(2020, 1, 1), "barra_end_date": None,
            "mapping_status": "ambiguous_gvkey", "barra_mapping_status": "mapped",
        },
        {
            "membership_date": date(2026, 8, 31),
            "gvkey": "003000", "cusip9": "000000003", "iid": "02", "cik": "0000000300",
            "barrid": "USAA004", "issuerid": "A00003", "rootid": "USAA004",
            "compustat_efffrom": date(2020, 1, 1), "compustat_effthru": None,
            "barra_start_date": date(2020, 1, 1), "barra_end_date": None,
            "mapping_status": "ambiguous_gvkey", "barra_mapping_status": "mapped",
        },
    ])


class CompustatBarridLookupTests(unittest.TestCase):
    def write_metric_report(self, root: Path, *, year: int = 2024, quarter: int = 1,
                            datadate: date = date(2024, 3, 31),
                            snapshots: tuple[str, ...] = ("version-a",),
                            value: float | None = 12.0, unavailable: tuple[str, ...] = (),
                            format_identity: tuple[str, str] | None = None,
                            format_keyed_path: bool = False) -> Path:
        suffix = f"_{format_identity[0]}_{format_identity[1]}" if format_identity and format_keyed_path else ""
        period = root / str(datadate.year) / "gvkey_001000" / f"quarterly_{datadate.isoformat()}{suffix}"
        for snapshot_id in snapshots:
            version = period / "snapshots" / snapshot_id
            version.mkdir(parents=True, exist_ok=True)
            report = {
                "gvkey": "001000", "datadate": datadate.isoformat(),
                "period_frequency": "quarterly",
                "fiscal_year": year, "fiscal_quarter": quarter,
                "source_table": "comp.fundq", "retrieved_at": "2026-10-28T00:00:00+00:00",
                "source_snapshot_id": snapshot_id,
                "source_fields_missing_from_schema": list(unavailable),
                "source_field_labels": {"oancfy": "Operating Activities - Net Cash Flow", "gpq": "Gross profit"},
                "source_field_units": {"oancfy": "Millions of reported currency; fiscal year-to-date duration", "gpq": "Millions of reported currency; period duration"},
                "source_field_definitions": {
                    "oancfy": {"label": "Operating Activities - Net Cash Flow", "unit": "Millions of reported currency; fiscal year-to-date duration", "period_semantics": "fiscal year-to-date duration"},
                    "gpq": {"label": "Gross profit", "unit": "Millions of reported currency; period duration", "period_semantics": "quarterly duration"},
                },
                "field_groups": {"cash_flow": ["oancfy"], "income_statement": ["gpq"]},
            }
            if format_identity:
                report["indfmt"], report["datafmt"] = format_identity
                report["format_filters"] = {"indfmt": format_identity[0], "datafmt": format_identity[1]}
            (version / "report.json").write_text(json.dumps(report))
            pl.DataFrame({"gvkey": ["001000"], "datadate": [datadate], "currency": ["USD"], "rdq": [date(2024, 4, 25)], "oancfy": [value]}).write_parquet(version / "cash_flow.parquet")
            pl.DataFrame({"gvkey": ["001000"], "datadate": [datadate], "currency": ["USD"], "rdq": [date(2024, 4, 25)], "gpq": [None]}).write_parquet(version / "income_statement.parquet")
        (period / "report.json").write_text((period / "snapshots" / snapshots[-1] / "report.json").read_text())
        (period / "latest_snapshot.json").write_text(json.dumps({"snapshot_id": snapshots[-1]}))
        return period

    def test_barrid_metric_lookup_requires_exact_mapping_date_and_reports_temporal_caveat(self):
        mapping = security_mapping().filter(pl.col("barrid") == "USAA001")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            period = self.write_metric_report(root)
            default_result = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping,
            )
            self.assertEqual(default_result["status"], "ok")
            self.assertEqual(default_result["value"], 12.0)
            self.assertEqual(default_result["resolved_gvkey"], "001000")
            self.assertEqual(default_result["period_semantics"], "Compustat fiscal year-to-date value as reported; no standalone quarter derivation.")
            self.assertEqual(default_result["mapping_temporal_status"], "saved_effective_intervals_cover_period; snapshot_is_not_a_historical_vintage")
            self.assertFalse(default_result["point_in_time_vintage"])
            self.assertTrue(default_result["source_file_path"].endswith("version-a/cash_flow.parquet"))
            mismatch = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 30), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping,
            )
            self.assertEqual(mismatch["status"], "no_valid_mapping")
            self.assertIn("does not have the requested membership date", mismatch["reason"])

    def test_barrid_metric_lookup_never_falls_back_to_a_different_latest_mapping_date(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mapping_dir = root / "security_mappings"
            mapping_dir.mkdir()
            security_mapping().write_parquet(
                mapping_dir / "membership_2026-08-31_example.parquet"
            )
            result = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2025, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1,
            )
        self.assertEqual(result["status"], "no_valid_mapping")
        self.assertIsNone(result["resolved_gvkey"])

    def test_barrid_metric_lookup_does_not_choose_between_versions_and_supports_explicit_latest(self):
        mapping = security_mapping().filter(pl.col("barrid") == "USAA001")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_metric_report(root, snapshots=("version-a", "version-b"))
            ambiguous = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping,
            )
            self.assertEqual(ambiguous["status"], "multiple_extraction_versions")
            selected = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping, source_snapshot_id="latest",
            )
            self.assertEqual(selected["status"], "ok")
            self.assertEqual(selected["source_snapshot_id"], "version-b")
            self.assertTrue(selected["source_file_path"].endswith("version-b/cash_flow.parquet"))

    def test_barrid_metric_lookup_uses_metadata_from_the_selected_extraction_version(self):
        mapping = security_mapping().filter(pl.col("barrid") == "USAA001")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            period = self.write_metric_report(root, snapshots=("version-a", "version-b"))
            old_version = period / "snapshots/version-a"
            old_report_path = old_version / "report.json"
            old_report = json.loads(old_report_path.read_text())
            old_report["retrieved_at"] = "2026-01-02T00:00:00+00:00"
            old_report["source_field_labels"]["oancfy"] = "Old version label"
            old_report["source_field_units"]["oancfy"] = "Old version units"
            old_report["source_field_definitions"]["oancfy"]["label"] = "Old version label"
            old_report["source_field_definitions"]["oancfy"]["unit"] = "Old version units"
            old_report_path.write_text(json.dumps(old_report))
            old_cash_flow = pl.read_parquet(old_version / "cash_flow.parquet").with_columns(
                pl.lit(7.0).alias("oancfy")
            )
            old_cash_flow.write_parquet(old_version / "cash_flow.parquet")
            selected = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping, source_snapshot_id="version-a",
            )
        self.assertEqual(selected["status"], "ok")
        self.assertEqual(selected["value"], 7.0)
        self.assertEqual(selected["units"], "Old version units")
        self.assertEqual(selected["original_field_label"], "Old version label")
        self.assertEqual(selected["extracted_at"], "2026-01-02T00:00:00+00:00")
        self.assertTrue(selected["source_report_metadata_path"].endswith("version-a/report.json"))

    def test_format_identity_separates_reports_and_lookup_can_select_format(self):
        mapping = security_mapping().filter(pl.col("barrid") == "USAA001")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_metric_report(root, format_identity=("INDL", "STD"), format_keyed_path=True, value=12.0)
            self.write_metric_report(root, format_identity=("FS", "STD"), format_keyed_path=True, value=99.0)
            ambiguous = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping,
            )
            selected = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping, indfmt="fs", datafmt="std",
            )
        self.assertEqual(ambiguous["status"], "multiple_matching_reports")
        self.assertEqual({(r["indfmt"], r["datafmt"]) for r in ambiguous["candidate_reports"]}, {("INDL", "STD"), ("FS", "STD")})
        self.assertEqual(selected["status"], "ok")
        self.assertEqual(selected["value"], 99.0)
        self.assertEqual((selected["indfmt"], selected["datafmt"]), ("FS", "STD"))

    def test_format_keyed_report_supersedes_legacy_path_without_deleting_it(self):
        mapping = security_mapping().filter(pl.col("barrid") == "USAA001")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = self.write_metric_report(root, format_identity=("INDL", "STD"))
            format_keyed = legacy.with_name(f"{legacy.name}_INDL_STD")
            shutil.copytree(legacy, format_keyed)
            selected = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping,
            )
            legacy_preserved = legacy.exists()
        self.assertTrue(legacy_preserved)
        self.assertEqual(selected["status"], "ok")
        self.assertIn("_INDL_STD/", selected["source_file_path"])

    def test_barrid_metric_lookup_keeps_older_versions_readable_when_artifact_is_absent(self):
        mapping = security_mapping().filter(pl.col("barrid") == "USAA001")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            period = self.write_metric_report(root, snapshots=("legacy-version", "current-version"))
            legacy = period / "snapshots/legacy-version"
            (legacy / "cash_flow.parquet").unlink()
            result = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping, source_snapshot_id="legacy-version",
            )
        self.assertEqual(result["status"], "field_unavailable")
        self.assertEqual(result["source_snapshot_id"], "legacy-version")
        self.assertEqual(result["extracted_at"], "2026-10-28T00:00:00+00:00")
        self.assertTrue(result["source_file_path"].endswith("legacy-version/cash_flow.parquet"))

    def test_fiscal_year_lookup_does_not_relabel_from_datadate_calendar_year(self):
        mapping = security_mapping().filter(pl.col("barrid") == "USAA001")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            period = root / "2025/gvkey_001000/annual_2025-05-31"
            version = period / "snapshots/annual-version"
            version.mkdir(parents=True)
            report = {
                "gvkey": "001000", "datadate": "2025-05-31", "period_frequency": "annual",
                "fiscal_year": 2024, "fiscal_quarter": None, "source_table": "comp.funda",
                "retrieved_at": "2026-10-28T00:00:00+00:00", "source_snapshot_id": "annual-version",
                "source_field_labels": {"gp": "Gross profit"},
                "source_field_units": {"gp": "Millions of reported currency; annual duration"},
                "source_field_definitions": {"gp": {"label": "Gross profit", "unit": "Millions of reported currency; annual duration", "period_semantics": "annual duration"}},
                "field_groups": {"income_statement": ["gp"]},
                "source_fields_missing_from_schema": [],
            }
            (version / "report.json").write_text(json.dumps(report))
            pl.DataFrame({
                "gvkey": ["001000"], "datadate": [date(2025, 5, 31)], "fyear": [2024],
                "fyr": [5], "currency": ["USD"], "gp": [583.1],
            }).write_parquet(version / "income_statement.parquet")
            (period / "report.json").write_text(json.dumps(report))
            (period / "latest_snapshot.json").write_text(json.dumps({"snapshot_id": "annual-version"}))
            by_fiscal_year = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "annual", "gp", "income_statement",
                mapping_snapshot=mapping, source_snapshot_id="latest",
            )
            by_calendar_year = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2025, "annual", "gp", "income_statement",
                mapping_snapshot=mapping, source_snapshot_id="latest",
            )
        self.assertEqual(by_fiscal_year["status"], "ok")
        self.assertEqual(by_fiscal_year["value"], 583.1)
        self.assertEqual(by_fiscal_year["fiscal_year"], 2024)
        self.assertEqual(by_fiscal_year["source_fyear"], 2024)
        self.assertEqual(by_fiscal_year["fiscal_year_end_month"], 5)
        self.assertEqual(by_fiscal_year["reporting_date"], date(2025, 5, 31))
        self.assertEqual(by_calendar_year["status"], "report_not_collected")

    def test_barrid_metric_lookup_distinguishes_unavailable_null_uncollected_and_ambiguous_mapping(self):
        mapping = security_mapping().filter(pl.col("barrid") == "USAA001")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_metric_report(root, value=None, unavailable=("gpq",))
            null_value = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping,
            )
            unavailable = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "gpq", "income_statement",
                fiscal_quarter=1, mapping_snapshot=mapping,
            )
            uncollected = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2019, "annual", "gp", "income_statement",
                mapping_snapshot=mapping,
            )
        self.assertEqual(null_value["status"], "field_present_null")
        self.assertIsNone(null_value["value"])
        self.assertEqual(null_value["currency"], "USD")
        self.assertEqual(unavailable["status"], "field_unavailable")
        self.assertEqual(unavailable["currency"], "USD")
        self.assertEqual(uncollected["status"], "report_not_collected")
        self.assertEqual(uncollected["resolved_gvkey"], "001000")

        ambiguous_mapping = pl.concat([
            mapping.with_columns(pl.lit("002000").alias("gvkey")),
            mapping.with_columns(pl.lit("003000").alias("gvkey")),
        ])
        with tempfile.TemporaryDirectory() as directory:
            ambiguous = lookup_compustat_metric_by_barrid(
                directory, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=ambiguous_mapping,
            )
        self.assertEqual(ambiguous["status"], "ambiguous_mapping")
        self.assertEqual(ambiguous["candidate_gvkeys"], ["002000", "003000"])

    def test_barrid_metric_lookup_reports_multiple_matching_periods(self):
        mapping = security_mapping().filter(pl.col("barrid") == "USAA001")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_metric_report(root, datadate=date(2024, 3, 31), snapshots=("a",))
            self.write_metric_report(root, datadate=date(2024, 4, 30), snapshots=("b",))
            result = lookup_compustat_metric_by_barrid(
                root, "USAA001", date(2026, 8, 31), 2024, "quarterly", "oancfy", "cash_flow",
                fiscal_quarter=1, mapping_snapshot=mapping,
            )
        self.assertEqual(result["status"], "multiple_matching_reports")
        self.assertEqual(len(result["candidate_reports"]), 2)


if __name__ == "__main__":
    unittest.main()
