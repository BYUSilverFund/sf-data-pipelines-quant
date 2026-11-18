"""Tests for scripts/check_date_coverage.py (issue #29)."""

import datetime as dt
import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database

_spec = importlib.util.spec_from_file_location(
    "check_date_coverage",
    Path(__file__).resolve().parents[1] / "scripts" / "check_date_coverage.py",
)
coverage = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(coverage)


def _write(table, dates):
    table.create_if_not_exists(2025)
    table.overwrite_year(2025, pl.DataFrame({"date": dates}))


class DateCoverageTests(unittest.TestCase):
    def test_reports_missing_dates_and_missing_files(self):
        all_dates = [dt.date(2025, 7, d) for d in (7, 8, 9)]
        with tempfile.TemporaryDirectory() as root, patch(
            "pipelines.utils.tables.load_dotenv"
        ), patch.dict(os.environ, {"ROOT": root}):
            db = Database(DatabaseName.DEVELOPMENT)
            _write(db.assets_table, all_dates)
            for name in ["covariances_table", "barra_risk_table", "signals_table",
                         "scores_table", "alpha_table"]:
                _write(getattr(db, name), all_dates)
            _write(db.exposures_table, all_dates[:1])

            missing = coverage.missing_dates(db, 2025)
            missing_no_file = coverage.missing_dates(db, 2024)

        self.assertEqual(missing["exposures"], all_dates[1:])
        self.assertEqual(missing["signals"], [])
        self.assertEqual(missing["alpha"], [])
        # No assets file for 2024 means nothing to compare against.
        self.assertTrue(all(v == [] for v in missing_no_file.values()))


if __name__ == "__main__":
    unittest.main()
