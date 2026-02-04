from pipelines.signals.base import BaseSignal
import polars as pl

class Beta(BaseSignal):
    def __init__(self, lookback: int = 0):
        self.lookback = lookback

    @property
    def name(self) -> str:
        return "beta"

    @property
    def lookback_days(self) -> int:
        return self.lookback + 100

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.sort("date")
            .with_columns(
                pl.col("predicted_beta")
                .mul(-1)
                .over("barrid")
                .alias("signal_value")
            )
            .select(["date", "barrid", "ticker", "signal_value", "specific_risk"])
        )