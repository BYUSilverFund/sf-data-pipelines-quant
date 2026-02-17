from abc import ABC, abstractmethod
import polars as pl

class BaseSignal(ABC):
    @property
    @abstractmethod
    def name(self) -> str: pass

    @property
    @abstractmethod
    def lookback_days(self) -> int: pass

    @property
    @abstractmethod
    def compute(self, df: pl.DataFrame) -> pl.DataFrame: 
        """Must return a polars DataFrame with the computed signal column added."""
        pass

    # required_columns: list[str] = ["date", "barrid", "ticker", "specific_risk", "in_universe"]
