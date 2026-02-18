from datetime import date, timedelta
from tqdm import tqdm
import polars as pl

from pipelines.utils.tables import Database
from pipelines.utils.enums import DatabaseName
from pipelines.utils.computations import compute_scores
from pipelines.utils.functions import get_tradable_universe
from pipelines.utils.data import get_prices_range


def scores_backfill_flow(
    *,
    database: Database,
    start_date: date,
    end_date: date,
) -> None:
    signals_table = database.signals_table
    scores_table = database.scores_table

    years = list(range(start_date.year, end_date.year + 1))
    for year in tqdm(years, desc="Scores Backfill"):
        year_start = max(start_date, date(year, 1, 1))
        year_end = min(end_date, date(year, 12, 31))

        sig = pl.read_parquet(f"/Users/nathanpreslar/groups/grp_quant/database/production/signals/signals_{year}.parquet")
        # sig = signals_table.read(year, start_date=year_start, end_date=year_end)

        if sig is None or sig.height == 0:
            continue

        prices = get_prices_range(year_start, year_end)

        sig = (
            sig.join(prices, on=["date", "ticker"], how="left")
            .with_columns(
                (
                    pl.col("in_universe")
                    & pl.col("price").is_not_null()
                    & pl.col("price").ge(5)
                ).alias("is_tradable")
            )
        )

        scored = compute_scores(sig)

        scores_table.create_if_not_exists(year)
        scores_table.upsert(year, scored)


def scores_daily_flow(
    *,
    database: Database,
    dates: list[date],
) -> None:
    """
    Daily update: just a small-window backfill.
    You likely already have a helper that returns last N market dates like in barra flow.
    """
    if not dates:
        dates = [(date.today() - timedelta(days=i)) for i in range(3)]

    scores_backfill_flow(
        database=database,
        start_date=min(dates),
        end_date=max(dates),
    )


if __name__ == "__main__":
    database_name = DatabaseName("production")
    database_instance = Database(database_name)

    scores_backfill_flow(
        database=database_instance,
        start_date=date(2020, 1, 1),
        end_date=date(2025, 12, 31),
    )