from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import tempfile
import unittest

import polars as pl

from scripts.finalize_compustat_run import ARTIFACTS, audit


class CompustatFinalizerTests(unittest.TestCase):
    def _save_report(self, root: Path, frequency: str, date_value: date,
                     fields_by_file: dict[str, dict[str, float | None]]) -> None:
        gvkey, indfmt, datafmt = "000001", "INDL", "STD"
        identity = root / str(date_value.year) / f"gvkey_{gvkey}" / (
            f"{frequency}_{date_value.isoformat()}_{indfmt}_{datafmt}"
        )
        version = identity / "snapshots" / f"snapshot_{frequency}"
        version.mkdir(parents=True)
        report = {
            "period_frequency": frequency,
            "gvkey": gvkey,
            "datadate": date_value.isoformat(),
            "indfmt": indfmt,
            "datafmt": datafmt,
        }
        (version / "report.json").write_text(json.dumps(report))
        for name in ARTIFACTS[1:]:
            values = fields_by_file.get(name, {})
            frame = pl.DataFrame({
                "gvkey": [gvkey],
                "datadate": [date_value],
                **{field: [value] for field, value in values.items()},
            })
            frame.write_parquet(version / name)
            frame.write_parquet(identity / name)
        (identity / "report.json").write_text(json.dumps(report))
        (identity / "latest_snapshot.json").write_text(json.dumps({
            "snapshot_id": f"snapshot_{frequency}",
            "snapshot_path": f"snapshots/snapshot_{frequency}",
        }))

    def test_audit_checks_composite_identity_artifacts_schemas_and_sample_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            root = base / "reports"
            run_dir = base / "run"
            run_dir.mkdir()
            annual = pl.DataFrame({
                "gvkey": ["000001"], "datadate": [date(2024, 12, 31)],
                "indfmt": ["INDL"], "datafmt": ["STD"],
                "at": [20.0], "sale": [10.0], "oancf": [3.0],
                "csho": [4.0], "dvpd": [None],
            })
            quarterly = pl.DataFrame({
                "gvkey": ["000001"], "datadate": [date(2024, 3, 31)],
                "indfmt": ["INDL"], "datafmt": ["STD"],
                "atq": [6.0], "saleq": [5.0], "oancfy": [1.0], "cshoq": [7.0],
            })
            annual.write_parquet(run_dir / "annual.parquet")
            quarterly.write_parquet(run_dir / "quarterly.parquet")
            self._save_report(root, "annual", date(2024, 12, 31), {
                "balance_sheet.parquet": {"at": 20.0},
                "income_statement.parquet": {"sale": 10.0},
                "cash_flow.parquet": {"oancf": 3.0},
                "capital_metrics.parquet": {"csho": 4.0, "dvpd": None},
            })
            self._save_report(root, "quarterly", date(2024, 3, 31), {
                "balance_sheet.parquet": {"atq": 6.0},
                "income_statement.parquet": {"saleq": 5.0},
                "cash_flow.parquet": {"oancfy": 1.0},
                "capital_metrics.parquet": {"cshoq": 7.0},
            })
            (run_dir / "run.json").write_text(json.dumps({
                "run_id": "run_test", "membership_snapshot_date": "2026-08-31",
                "start_date": "2024-01-01", "end_date": "2026-10-29",
                "filters": {"indfmt": "INDL", "datafmt": "STD"},
                "report_identity_fields": ["period_frequency", "gvkey", "datadate", "indfmt", "datafmt"],
                "selected_gvkey_count": 1,
            }))
            frequencies = {
                name: {
                    "source_row_count": 1, "reporting_company_count": 1,
                    "companies_without_reports": 0, "duplicate_report_identity_rows": 0,
                    "field_coverage": {},
                }
                for name in ("annual", "quarterly")
            }
            (run_dir / "coverage_report.json").write_text(json.dumps({"frequencies": frequencies}))

            report = audit(run_dir, root, 999999999, 0)
            self.assertTrue(report["complete"])
            self.assertEqual(report["unique_report_identity_counts"], {"annual": 1, "quarterly": 1})
            self.assertEqual(report["sample_value_mismatches_count"], 0)
            self.assertEqual(report["missing_artifacts_count"], 0)
            self.assertIn("cash_flow.parquet", report["sample_output_schemas"]["quarterly"])
            self.assertTrue(all(row["matches"] for rows in report["sampled_value_checks"].values() for row in rows))


if __name__ == "__main__":
    unittest.main()
