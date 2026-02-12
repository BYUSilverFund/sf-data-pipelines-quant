import polars as pl
import numpy as np

from pipelines.utils.functions import _config

from pipelines.dao.dao import DAO
from components.types import (
    AssetsDf, Assets,
    PricesDf, Prices,
    WeightsDf, Weights,
    BetasDf, Betas,
    AlphasDf, Alphas,
    DollarsDf, Dollars,
    SharesDf, Shares,
    OrdersDf, Orders,
)


class PortfolioService:
    def __init__(self):
        self.dao = DAO.PortfolioDAO()

    def get_tradable_universe(self, prices: PricesDf) -> list[str]:
        return (
            prices.filter(
                pl.col("price").ge(5),
            )["ticker"]
            .unique()
            .sort()
            .to_list()
        )
    
    def get_alphas(self, assets: AssetsDf) -> AlphasDf:
        
        #TODO: This is wrong, based off of the old signals model
        
        signals = _config.signals
        signal_combinator = _config.signal_combinator
        ic = _config.ic
        data_date = _config.data_date

        alphas = (
            assets.sort("ticker", "date")
            # Compute signals
            .with_columns([signal.expr for signal in signals])
            # Compute scores
            .with_columns(
                [
                    pl.col(signal.name)
                    .sub(pl.col(signal.name).mean())
                    .truediv(pl.col(signal.name).std())
                    for signal in signals
                ]
            )
            # Compute alphas
            .with_columns(
                [
                    pl.col(signal.name).mul(pl.lit(ic)).mul(pl.col("specific_risk"))
                    for signal in signals
                ]
            )
            # Fill null alphas with 0
            .with_columns(pl.col(signal.name).fill_null(0) for signal in signals)
            # Combine alphas
            .with_columns(signal_combinator.combine_fn([signal.name for signal in signals]))
            # Get trade date
            .filter(pl.col("date").eq(data_date))
            .select("ticker", "alpha")
            .sort("ticker")
        )

        return Alphas.validate(alphas)
    
    def compute_risk(self, weights: np.ndarray, covariance_matrix: np.ndarray) -> float:
        return np.sqrt(weights @ covariance_matrix @ weights.T)
    
    
