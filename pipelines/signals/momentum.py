from pipelines.signals.base import BaseSignal
from components.types import AssetsDF
import polars as pl


class Momentum(BaseSignal):
    def __init__(self, window: int = 230, shift: int = 22):
        self.window = window
        self.shift = shift

    @property
    def name(self) -> str:
        return "momentum"

    @property
    def lookback_days(self) -> int:
        return self.window + 20

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.sort("date")
            .with_columns(
                pl.col("return")
                .log1p()
                .rolling_sum(window_size=self.window)
                .shift(self.shift)
                .over("barrid")
                .alias("signal_value")
            )
            .select(["date", "barrid", "ticker", "signal_value", "specific_risk"])
        )