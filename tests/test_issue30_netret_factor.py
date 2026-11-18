"""Regression test for issue #30.

USSLOWL_NETRET (Internet & Direct Marketing Retail, removed from GICS in March
2023) was commented out of the factor list. Table.upsert keeps only columns in
the file's schema, so NETRET exposures were silently dropped on load and
historical industry exposures for internet retailers did not sum to 1.
"""

import os
import tempfile
import unittest
from unittest.mock import patch

import polars as pl

from pipelines.barra_exposures_flow import clean_barra_df
from pipelines.utils.enums import DatabaseName
from pipelines.utils.factors import factors
from pipelines.utils.tables import Database

INDUSTRY = "USSLOWL_NETRET"
OTHER_INDUSTRY = "USSLOWL_SPLTYRET"


class NetretFactorTests(unittest.TestCase):
    def test_netret_in_factor_list(self):
        self.assertIn(INDUSTRY, factors)

    def test_netret_exposure_survives_load(self):
        # Raw-style rows (after CSV parse) for one Amazon-like stock split
        # between two industries, plus a style factor.
        raw = pl.DataFrame(
            {
                "!Barrid": ["USAAAA1"] * 3,
                "Factor": [INDUSTRY, OTHER_INDUSTRY, "USSLOWL_SIZE"],
                "Exposure": [0.85, 0.15, 1.2],
                "DataDate": ["20221230"] * 3,
            }
        )
        clean = clean_barra_df(raw)

        with tempfile.TemporaryDirectory() as root, patch(
            "pipelines.utils.tables.load_dotenv"
        ), patch.dict(os.environ, {"ROOT": root}):
            table = Database(DatabaseName.DEVELOPMENT).exposures_table
            table.create_if_not_exists(2022)
            table.upsert(2022, clean)
            stored = table.read(2022).collect()

        self.assertIn(INDUSTRY, stored.columns)
        row = stored.row(0, named=True)
        self.assertAlmostEqual(row[INDUSTRY], 0.85)
        self.assertAlmostEqual(row[INDUSTRY] + row[OTHER_INDUSTRY], 1.0)

    def test_risk_tables_include_netret(self):
        with tempfile.TemporaryDirectory() as root, patch(
            "pipelines.utils.tables.load_dotenv"
        ), patch.dict(os.environ, {"ROOT": root}):
            db = Database(DatabaseName.DEVELOPMENT)
            for table in (db.exposures_table, db.covariances_table, db.factors_table):
                self.assertIn(INDUSTRY, table._schema)


if __name__ == "__main__":
    unittest.main()
