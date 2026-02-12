import polars as pl
from pipelines.dao.dao import DAO

from pipelines.utils.functions import _config

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

class TradingService:
    def __init__(self, dao: DAO):
        self.dao = dao

    def get_optimal_shares(
        self, weights: WeightsDf, prices: PricesDf, account_value: float
    ) -> SharesDf:
        optimal_shares = (
            weights.join(prices, on="ticker", how="left")
            .with_columns(pl.lit(account_value).mul(pl.col("weight")).alias("dollars"))
            .with_columns(
                pl.col("dollars").truediv(pl.col("price")).floor().alias("shares")
            )
            .select(
                "ticker",
                "shares",
            )
        )

        return Shares.validate(optimal_shares)
    
    def get_order_deltas(
        self,
        prices: PricesDf,
        current_shares: SharesDf,
        optimal_shares: SharesDf,
    ) -> OrdersDf:
        # Prep shares dataframes for join
        current_shares = current_shares.rename({"shares": "current_shares"})
        optimal_shares = optimal_shares.rename({"shares": "optimal_shares"})

        orders = (
            prices
            # Joins
            .join(current_shares, on="ticker", how="left")
            .join(optimal_shares, on="ticker", how="left")
            # Fill nulls with 0
            .with_columns(pl.col("current_shares", "optimal_shares").fill_null(0))
            # Compute share differential
            .with_columns(pl.col("optimal_shares").sub("current_shares").alias("shares"))
            # Compute order side
            .with_columns(
                pl.when(pl.col("shares").gt(0))
                .then(pl.lit("BUY"))
                .when(pl.col("shares").lt(0))
                .then(pl.lit("SELL"))
                .otherwise(pl.lit("HOLD"))
                .alias("action")
            )
            # Absolute value the shares
            .with_columns(pl.col("shares").abs())
            # Select
            .select("ticker", "price", "shares", "action")
            # Filter
            .filter(
                pl.col("ticker")
                .is_in(_config.ignore_tickers)
                .not_(),  # Ignore problematic tickers
                pl.col("shares").ne(0),  # Remove 0 share trades
                pl.col("action").ne("HOLD"),  # Remove HOLDs
                pl.col("price").is_not_null(),  # Remove unknown prices
            )
            # Sort
            .sort("ticker")
        )

        return Orders.validate(orders)
    
    def get_dollars(
        self, shares: SharesDf, prices: PricesDf
    ) -> DollarsDf:
        dollars = (
            shares.join(prices, on="ticker", how="left")
            .with_columns(pl.col("shares").mul("price").alias("dollars"))
            .select("ticker", "dollars")
        )

        return Dollars.validate(dollars)