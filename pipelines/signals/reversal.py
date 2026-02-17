from pipelines.signals.base import BaseSignal
import polars as pl


class Reversal(BaseSignal):

    required_cols = {"return", "barrid"}

    def __init__(self, window: int = 22):
        self.window = window

    @property
    def name(self) -> str:
        return "reversal"

    @property
    def lookback_days(self) -> int:
        return self.window + 20

    @property
    def expr(self) -> pl.Expr:
        return (
            pl.col("return")
            .log1p()
            .rolling_sum(window_size=self.window)
            .mul(-1)
            .over("barrid")
            .alias(self.name)
        )
        

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.sort("date")
            .with_columns(
                pl.col("return")
                .log1p()
                .rolling_sum(window_size=self.window)
                .mul(-1)
                .over("barrid")
                .alias("signal_value")
            )
            .select(["date", "barrid", "ticker", "signal_value", "specific_risk"])
        )