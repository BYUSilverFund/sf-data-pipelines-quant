import datetime as dt
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import polars as pl

from pipelines.crsp_v2_daily_flow import (
    _deduplicate_crsp_v2_daily_rows,
    load_crsp_v2_daily_df,
)
from pipelines.utils.tables import Table


class Issue34CrspV2DailyTests(unittest.TestCase):
    def test_daily_query_maps_distinct_high_low_and_close(self):
        result_frame = pd.DataFrame(
            {
                "date": [dt.date(2024, 1, 2)],
                "permno": [14593],
                "cusip": ["03783310"],
                "ticker": ["AAPL"],
                "ret": [-0.035787],
                "retx": [-0.035787],
                "prc": [185.64],
                "vol": [81752737],
                "open": [187.15],
                "high": [188.44],
                "low": [183.885],
                "close": [185.64],
                "shrout": [15508763],
                "primaryexch": ["N"],
                "securitytype": ["EQTY"],
            }
        )

        class FakeConnection:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def raw_sql(self, query):
                self.query = query
                return result_frame

        with patch.dict(os.environ, {"WRDS_PASSWORD": "test-password"}):
            with patch(
                "pipelines.crsp_v2_daily_flow.wrds.Connection", FakeConnection
            ):
                result = load_crsp_v2_daily_df(
                    dt.date(2024, 1, 2), dt.date(2024, 1, 2), "wrds-user"
                )

        self.assertEqual(result.select("high", "low", "close").row(0), (188.44, 183.885, 185.64))

    def test_identical_duplicate_rows_are_collapsed(self):
        frame = pl.DataFrame(
            {
                "date": [dt.date(2024, 1, 2), dt.date(2024, 1, 2)],
                "permno": [14593, 14593],
                "high": [188.44, 188.44],
                "low": [183.885, 183.885],
                "close": [185.64, 185.64],
            }
        )

        result = _deduplicate_crsp_v2_daily_rows(frame, 2024)

        self.assertEqual(result.height, 1)
        self.assertEqual(result.select("high", "low", "close").row(0), (188.44, 183.885, 185.64))

    def test_conflicting_duplicate_keys_are_rejected(self):
        frame = pl.DataFrame(
            {
                "date": [dt.date(2024, 1, 2), dt.date(2024, 1, 2)],
                "permno": [14593, 14593],
                "high": [188.44, 188.44],
                "low": [183.885, 183.90],
                "close": [185.64, 185.64],
            }
        )

        with self.assertRaisesRegex(ValueError, "conflicting rows"):
            _deduplicate_crsp_v2_daily_rows(frame, 2024)

    def test_overwrite_year_replaces_existing_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            table_dir = Path(temp_dir) / "crsp_v2_daily"
            table_dir.mkdir()
            table = Table.__new__(Table)
            table._base_path = temp_dir
            table._name = "crsp_v2_daily"

            old = pl.DataFrame({"date": [dt.date(2024, 1, 2)], "permno": [1]})
            new = pl.DataFrame({"date": [dt.date(2024, 1, 3)], "permno": [2]})
            old.write_parquet(table_dir / "crsp_v2_daily_2024.parquet")

            table.overwrite_year(2024, new)

            saved = pl.read_parquet(table_dir / "crsp_v2_daily_2024.parquet")
            self.assertTrue(saved.equals(new))
            self.assertEqual(list(table_dir.glob("*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
