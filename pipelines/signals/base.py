from abc import ABC, abstractmethod
from components.models import AssetsDF
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
    def compute(self, df: AssetsDF) -> pl.DataFrame: 
        """Must return a polars DataFrame with the computed signal column added."""
        pass

