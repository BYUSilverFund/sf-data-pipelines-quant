import datetime as dt
import polars as pl
from pipelines.utils.tables import Database
from components.types import SignalsDf, Signals

from components.types import (
    AlphasDf, Alphas,
    
)

class SignalsDao:
    def __init__(self, database: Database):
        self.database = database

    def get_current_signals(self, tickers: list[str]) -> SignalsDf:
        """
        Gets the signals for the given tickers on the day specified in the config.
        """
    def write_signals(self, signals: SignalsDf) -> None:
        """
        Writes the given signals to the location specified in the config.
        """
        start_date = signals.get_column(pl.col("date")).min()
        end_date = signals.get_column(pl.col("date")).max()
        
        signals_dummy = signals.with_columns(
            pl.col("date").dt.truncate("1y").alias("year")
        )
        for year in range(start_date.year, end_date.year + 1):

            chunk = signals_dummy.filter(pl.col("year") == year).drop("year")
            # upsert into database
            self.database.signals_table.create_if_not_exists(year)
            self.database.signals_table.upsert(year, rows=chunk)