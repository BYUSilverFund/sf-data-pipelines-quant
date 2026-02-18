import datetime as dt
import polars as pl
from tqdm import tqdm

from pipelines.utils.tables import Database
from pipelines.utils.enums import DatabaseName
from pipelines.utils.functions import get_optimal_weights
from pipelines.utils.data import get_betas, get_covariance_matrix
import sf_quant.optimizer as sfo


def weights_backfill_flow(
    *,
    database: Database,
    start_date: dt.date,
    end_date: dt.date
) -> None:
    """"""
    combined_alpha_table = database.combined_alpha_table
    weights_table = database.weights_table

    dates = pl.date_range(start_date, end_date, "1d").to_list()

    for d in tqdm(dates, desc="Weights Backfill"):
        year = d.year

        combined_alpha = pl.read_parquet(f"/Users/nathanpreslar/groups/grp_quant/database/production/combined_alpha/combined_alpha_{year}.parquet")
        combined_alpha = combined_alpha.filter(pl.col("date") == d)
        # combined_alpha = combined_alpha_table.read(year, start_date=year_start, end_date=year_end)

        if combined_alpha is None or combined_alpha.height == 0:
            continue

        tickers = combined_alpha["ticker"].unique().to_list()
        weights = get_optimal_weights(
            tickers=tickers,
            alphas=combined_alpha,
            betas=tickers,
            covariance_matrix=get_covariance_matrix(tickers)
        )

        if weights.height == 0:
            continue

        weights_table.create_if_not_exists(year)
        weights_table.upsert(year, weights)


def weights_daily_flow(
    *,
    database: Database,
    dates: list[dt.date] = None
) -> None:
    if not dates:
        dates = [(dt.date.today() - dt.timedelta(days=i)) for i in range(3)]

    weights_backfill_flow(
        database=database,
        start_date=min(dates),
        end_date=max(dates)
    )