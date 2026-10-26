import datetime as dt
import glob
import os

import cvxpy as cp
import polars as pl
import sf_quant.data as sfd
import sf_quant.optimizer as sfo
from tqdm import tqdm

from pipelines.utils.tables import Database

# Portfolio construction settings. These match the live sf-trader config.
CONFIG = {
    "constraints": [
        sfo.FullInvestment(),
        sfo.LongOnly(),
        sfo.NoBuyingOnMargin(),
        sfo.UnitBeta(),
    ],
    "gamma": 1_000_000,
    "min_price": 5,
}


def optimal_weights_flow(start: dt.date, end: dt.date, database: Database) -> None:
    """
    Compute mean-variance optimal portfolio weights from composite alphas.

    Each date is a separate optimization, so only dates that have composite
    alphas but no optimal weights yet are computed. Existing dates are kept.

    Strategy:
    1. Find composite alpha dates in [start, end] missing from optimal_weights
    2. Per year: load the universe (in_universe, price >= min_price, has
       beta, specific risk, and Barra exposures) with composite alphas
    3. Per date: build the factor model and run the optimizer
    4. Upsert the year's weights into optimal_weights table
    """
    # Point sf_quant's loaders at the same database as this run
    sfd.env(root=os.getenv("ROOT"), database=database._database_name.value)

    if not glob.glob(database.composite_alphas_table._file_path()):
        print("No composite alphas found, skipping optimal weights.", flush=True)
        return

    dates = (
        database.composite_alphas_table.read()
        .filter(pl.col("date").is_between(start, end))
        .select("date")
        .unique()
        .collect()["date"]
        .to_list()
    )

    years = sorted({date_.year for date_ in dates})

    for year in years:
        database.optimal_weights_table.create_if_not_exists(year)

        existing_dates = set(
            database.optimal_weights_table.read(year)
            .select("date")
            .unique()
            .collect()["date"]
            .to_list()
        )
        year_dates = sorted(
            date_ for date_ in dates
            if date_.year == year and date_ not in existing_dates
        )

        if not year_dates:
            continue

        universe = load_universe(year, year_dates, database)

        weights_list = []
        for date_ in tqdm(year_dates, desc=f"Optimal Weights {year}"):
            weights = optimize(date_, universe.filter(pl.col("date").eq(date_)))
            if weights is not None:
                weights_list.append(weights)

        if weights_list:
            database.optimal_weights_table.upsert(year, pl.concat(weights_list))


def load_universe(year: int, dates: list[dt.date], database: Database) -> pl.DataFrame:
    """Load optimizer inputs for the given dates within a single year."""
    assets = (
        database.assets_table.read(year)
        .filter(
            pl.col("date").is_in(dates),
            pl.col("in_universe"),
            pl.col("price").ge(CONFIG["min_price"]),
            pl.col("predicted_beta").is_not_null(),
            pl.col("specific_risk").is_not_null(),
        )
        .select("date", "barrid", "predicted_beta")
    )

    # Assets without exposures would misalign the factor model components
    exposures = (
        database.exposures_table.read(year)
        .filter(pl.col("date").is_in(dates))
        .select("date", "barrid")
        .unique()
    )

    alphas = (
        database.composite_alphas_table.read(year)
        .filter(pl.col("date").is_in(dates))
        .select("date", "barrid", "alpha")
    )

    return (
        assets
        .join(exposures, on=["date", "barrid"], how="inner")
        .join(alphas, on=["date", "barrid"], how="left")
        .with_columns(pl.col("alpha").fill_null(0).fill_nan(0))
        .sort("date", "barrid")
        .collect()
    )


def optimize(date_: dt.date, universe: pl.DataFrame) -> pl.DataFrame | None:
    """Run the optimizer for one date. Returns None if it cannot be solved."""
    if universe.is_empty():
        print(f"Skipping {date_}: empty universe.", flush=True)
        return None

    barrids = universe["barrid"].to_list()

    factor_exposures, factor_covariance, specific_risk = (
        sfd.construct_factor_model_components(date_, barrids)
    )

    if factor_exposures.shape[0] != len(barrids):
        raise ValueError(
            f"{date_}: {factor_exposures.shape[0]} exposure rows for {len(barrids)} assets."
        )

    try:
        weights = sfo.mve_optimizer(
            ids=barrids,
            alphas=universe["alpha"].to_numpy(),
            factor_exposures=factor_exposures,
            factor_covariance=factor_covariance,
            specific_risk=specific_risk,
            constraints=CONFIG["constraints"],
            gamma=CONFIG["gamma"],
            betas=universe["predicted_beta"].to_numpy(),
        )
    except cp.error.SolverError as e:
        print(f"Skipping {date_}: optimizer failed ({e}).", flush=True)
        return None

    if weights["weight"].null_count() > 0:
        print(f"Skipping {date_}: optimizer returned no solution.", flush=True)
        return None

    return weights.select(
        pl.lit(date_).alias("date"),
        "barrid",
        pl.col("weight").cast(pl.Float64),
    )


if __name__ == "__main__":
    from pipelines.utils.enums import DatabaseName
    db = Database(DatabaseName.DEVELOPMENT)
    start = dt.date(2026, 1, 1)
    end = dt.date.today()
    optimal_weights_flow(start, end, db)
