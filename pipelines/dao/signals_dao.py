import sf_quant.data as sfd
import polars as pl

from components.types import (
    AlphasDf, Alphas,
    
)

class SignalsDao:
    def __init__(self):
        pass

    def get_signals(self, tickers: list[str]) -> pl.DataFrame:
        """
        Gets the signals for the given tickers on the day specified in the config.
        """
    def write_signals(self, signals: pl.DataFrame) -> None:
        """
        Writes the given signals to the location specified in the config.
        """