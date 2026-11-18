"""Regression tests for issue #33.

The legacy CRSP backfills created a year's parquet file before checking the
query result, so years with no source data (2025+, after WRDS stopped updating
legacy CRSP) were silently written as empty files.
"""

import datetime as dt
import unittest
from unittest.mock import MagicMock, patch

import polars as pl

from pipelines import crsp_daily_flow, crsp_events_flow, crsp_monthly_flow


def _rows(year: int) -> pl.DataFrame:
    return pl.DataFrame({"date": [dt.date(year, 6, 30)], "permno": [10001]})


class LegacyCrspEmptyYearTests(unittest.TestCase):
    def _assert_only_2024_written(self, db, table_name):
        table = getattr(db, table_name)
        table.create_if_not_exists.assert_called_once_with(2024)
        self.assertEqual(table.upsert.call_count, 1)
        self.assertEqual(table.upsert.call_args.args[0], 2024)

    def test_daily_skips_empty_year_with_warning(self):
        db = MagicMock()
        by_year = {2024: _rows(2024), 2025: _rows(2024).clear()}
        with patch.object(
            crsp_daily_flow,
            "load_crsp_daily_df",
            side_effect=lambda start_date, end_date, user: by_year[start_date.year],
        ), self.assertWarnsRegex(UserWarning, "no rows for 2025"):
            crsp_daily_flow.crsp_daily_backfill_flow(
                dt.date(2024, 1, 1), dt.date(2025, 12, 31), db, "user"
            )
        self._assert_only_2024_written(db, "crsp_daily_table")

    def test_monthly_skips_empty_year_with_warning(self):
        db = MagicMock()
        with patch.object(
            crsp_monthly_flow, "load_crsp_monthly_df", return_value=_rows(2024)
        ), self.assertWarnsRegex(UserWarning, "no rows for 2025"):
            crsp_monthly_flow.crsp_monthly_backfill_flow(
                dt.date(2024, 1, 1), dt.date(2025, 12, 31), db, "user"
            )
        self._assert_only_2024_written(db, "crsp_monthly_table")

    def test_events_skips_empty_year_with_warning(self):
        db = MagicMock()
        with patch.object(
            crsp_events_flow, "load_crsp_events_df", return_value=_rows(2024)
        ), self.assertWarnsRegex(UserWarning, "no rows for 2025"):
            crsp_events_flow.crsp_events_backfill_flow(
                dt.date(2024, 1, 1), dt.date(2025, 12, 31), db, "user"
            )
        self._assert_only_2024_written(db, "crsp_events_table")


if __name__ == "__main__":
    unittest.main()
