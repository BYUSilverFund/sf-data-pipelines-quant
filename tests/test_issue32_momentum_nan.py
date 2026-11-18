"""Regression tests for issue #32.

A single stock with a return <= -100% (LandAmerica, Nov 2008) produced NaN or
-inf from log1p. The value passed the flow's is_not_null() filter and made the
per-date mean/std NaN, so every barra_momentum score and alpha was NaN.
"""

import datetime as dt
import math
import unittest

import polars as pl

from pipelines.signals_flow import signals_flow


class _FakeTable:
    def __init__(self):
        self.frames = []

    def create_if_not_exists(self, year):
        pass

    def upsert(self, year, df):
        self.frames.append(df)

    def collected(self) -> pl.DataFrame:
        return pl.concat(self.frames)


class _FakeAssetsTable:
    def __init__(self, df):
        self.df = df

    def read(self):
        return self.df.lazy()


class _FakeDatabase:
    def __init__(self, assets):
        self.assets_table = _FakeAssetsTable(assets)
        self.signals_table = _FakeTable()
        self.scores_table = _FakeTable()
        self.alpha_table = _FakeTable()


def _assets(bad_return_pct: float, n_days: int = 300) -> pl.DataFrame:
    """Three stocks in percent units (as stored); BAD has one invalid return."""
    start = dt.date(2008, 1, 1)
    rows = []
    for k, barrid in enumerate(["GOOD1", "GOOD2", "BAD"]):
        for i in range(n_days):
            r = 0.1 * k + math.sin(i + k)  # percent
            if barrid == "BAD" and i == 100:
                r = bad_return_pct
            rows.append(
                {
                    "date": start + dt.timedelta(days=i),
                    "barrid": barrid,
                    "ticker": barrid,
                    "price": 10.0,
                    "return": r,
                    "specific_return": r,
                    "specific_risk": 30.0 + k + 0.01 * i,
                    "predicted_beta": 1.0 + 0.1 * k,
                    "daily_volume": 1000.0,
                    "in_universe": True,
                }
            )
    return pl.DataFrame(rows)


class InvalidReturnTests(unittest.TestCase):
    def _check(self, bad_return_pct: float):
        db = _FakeDatabase(_assets(bad_return_pct))
        signals_flow(db)

        signals = db.signals_table.collected()
        scores = db.scores_table.collected()
        alphas = db.alpha_table.collected()

        # Nothing non-finite is written for any signal.
        self.assertTrue(signals["signal_value"].is_finite().all())
        self.assertTrue(scores["score"].is_finite().all())
        self.assertTrue(alphas["alpha"].is_finite().all())

        # Valid peers still get barra_momentum scores on every date where the
        # invalid stock is in its 230-day window.
        mom = scores.filter(pl.col("signal_name") == "barra_momentum")
        peers = mom.filter(pl.col("barrid") != "BAD")
        bad = mom.filter(pl.col("barrid") == "BAD")
        self.assertGreater(peers.height, 0)
        excluded_dates = set(peers["date"]) - set(bad["date"])
        self.assertGreater(len(excluded_dates), 0)

    def test_return_below_minus_100pct(self):
        self._check(-103.7)

    def test_return_exactly_minus_100pct(self):
        self._check(-100.0)


if __name__ == "__main__":
    unittest.main()
