"""Regression test for issue #31.

The pipeline wrote Fama-French factors to fama_french/fama_french.parquet while
sf_quant.data.load_fama_french() reads ff5_factors/ff5_factors*.parquet, so the
consumer could never find what the producer wrote.
"""

import datetime as dt
import os
import tempfile
import unittest
from unittest.mock import patch

import polars as pl
import sf_quant.data as sfd
from sf_quant.data import _config as sfq_config

from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database


class FamaFrenchPathContractTests(unittest.TestCase):
    def test_sf_quant_loads_what_pipeline_writes(self):
        factors = pl.DataFrame(
            {
                "date": [dt.date(2025, 1, 2), dt.date(2025, 1, 3)],
                "mkt_rf": [0.001, -0.002],
                "smb": [0.0005, 0.0],
                "hml": [-0.001, 0.0015],
                "rmw": [0.0002, -0.0003],
                "cma": [0.0, 0.0004],
                "rf": [0.0002, 0.0002],
            }
        )

        with tempfile.TemporaryDirectory() as root, patch(
            "pipelines.utils.tables.load_dotenv"
        ), patch.dict(os.environ, {"ROOT": root}), patch.dict(
            sfq_config._config, {"root": root, "database": "development"}
        ):
            Database(DatabaseName.DEVELOPMENT).fama_french_table.overwrite(factors)

            loaded = sfd.load_fama_french(
                start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 31)
            )

        self.assertTrue(loaded.sort("date").equals(factors))


if __name__ == "__main__":
    unittest.main()
