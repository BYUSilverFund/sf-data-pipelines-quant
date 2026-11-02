import datetime as dt
import os

import polars as pl
import sf_quant.backtester as sfb
import sf_quant.optimizer as sfo

from pipelines.utils.tables import Database

# Fully invested, long only, and same beta as the benchmark
CONSTRAINTS = [sfo.ZeroInvestment(), sfo.LongOnly(), sfo.ZeroBeta()]
TARGET_ACTIVE_RISK = 0.05
MIN_PRICE = 5

# Use the CPUs Slurm gives the job, not every CPU on the node
N_CPUS = int(os.getenv("SLURM_CPUS_PER_TASK", os.cpu_count()))


def optimal_weights_flow(start: dt.date, end: dt.date, database: Database) -> None:
    """
    Compute optimal portfolio weights from composite alphas.

    Steps:
    1. Find dates that have composite alphas but no weights yet
    2. For each year, load the stocks the optimizer can hold on those dates
    3. Run sf_quant's optimizer on every date in parallel
    4. Save that year's weights to the optimal_weights table
    """
    # sf_quant runs each date in a separate Ray worker, and the workers
    # find the database through this environment variable
    os.environ["DATABASE"] = database._database_name.value

    missing_dates = get_missing_dates(start, end, database)

    for year in sorted({date_.year for date_ in missing_dates}):
        year_dates = [date_ for date_ in missing_dates if date_.year == year]
        universe = load_universe(year, year_dates, database)

        weights = sfb.dynamic_backtest_parallel(
            data=universe,
            constraints=CONSTRAINTS,
            target_active_risk=TARGET_ACTIVE_RISK,
            n_cpus=N_CPUS,
            active_weights=False,
        )

        database.optimal_weights_table.upsert(year, weights)


def get_missing_dates(start: dt.date, end: dt.date, database: Database) -> list[dt.date]:
    """Find dates in [start, end] that have composite alphas but no weights yet."""
    alpha_dates = (
        database.composite_alphas_table.read()
        .filter(pl.col("date").is_between(start, end))
        .select("date")
        .unique()
    )

    for year in range(start.year, end.year + 1):
        database.optimal_weights_table.create_if_not_exists(year)

    weight_dates = database.optimal_weights_table.read().select("date").unique()

    return (
        alpha_dates
        .join(weight_dates, on="date", how="anti")
        .sort("date")
        .collect()["date"]
        .to_list()
    )


def load_universe(year: int, dates: list[dt.date], database: Database) -> pl.DataFrame:
    """Load the stocks the optimizer can hold, with their alphas and benchmark weights."""
    assets = (
        database.assets_table.read(year)
        .filter(
            pl.col("date").is_in(dates),
            pl.col("in_universe"),
            pl.col("price").ge(MIN_PRICE),
            pl.col("predicted_beta").is_not_null(),
            pl.col("specific_risk").is_not_null(),
            pl.col("market_cap").is_not_null(),
        )
        .select("date", "barrid", "predicted_beta", "market_cap")
    )

    # Drop stocks without Barra exposures, since sf_quant can't build their risk
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
        .with_columns(
            # Stocks without an alpha stay in with an alpha of 0
            pl.col("alpha").fill_null(0),
            # Market-cap weights, so the benchmark adds up to 1 each day
            pl.col("market_cap")
            .truediv(pl.col("market_cap").sum().over("date"))
            .alias("benchmark_weight"),
        )
        .drop("market_cap")
        .collect()
    )


if __name__ == "__main__":
    from pipelines.utils.enums import DatabaseName
    db = Database(DatabaseName.DEVELOPMENT)
    start = dt.date(2026, 1, 1)
    end = dt.date.today()
    optimal_weights_flow(start, end, db)
