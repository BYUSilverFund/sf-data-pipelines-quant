from typing import TypeAlias
import dataframely as dy

from models import Alphas, Assets, Prices, Dollars, Shares, Weights, Betas, Orders, Signals

AssetsDf: TypeAlias = dy.DataFrame[Assets]
PricesDf: TypeAlias = dy.DataFrame[Prices]
DollarsDf: TypeAlias = dy.DataFrame[Dollars]
SharesDf: TypeAlias = dy.DataFrame[Shares]
WeightsDf: TypeAlias = dy.DataFrame[Weights]
AlphasDf: TypeAlias = dy.DataFrame[Alphas]
BetasDf: TypeAlias = dy.DataFrame[Betas]
OrdersDf: TypeAlias = dy.DataFrame[Orders]
SignalsDf: TypeAlias = dy.DataFrame[Signals]


# TODO: Add from dataframely import validate
