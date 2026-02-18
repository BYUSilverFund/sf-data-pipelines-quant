import datetime as dt
from tqdm import tqdm
import polars as pl

from pipelines.utils.tables import Database
from pipelines.utils.enums import DatabaseName
from pipelines.utils.computations import (
    compute_signal_alphas,
    compute_combined_alpha
)

def signal_alphas_backfill_flow(
    *,
    database: Database,
    start_date: date,
    end_date: date,
    ic: float = 0.5,
) -> None:
    """
    Reads scores_table, writes signal_alphas_table (per-signal alphas).
    """
    scores_table = database.scores_table
    signal_alphas_table = database.signal_alphas_table

    years = list(range(start_date.year, end_date.year + 1))
    for year in tqdm(years, desc="Signal Alphas Backfill"):
        year_start = max(start_date, dt.date(year, 1, 1))
        year_end = min(end_date, dt.date(year, 12, 31))

        scores = pl.read_parquet(f"/Users/nathanpreslar/groups/grp_quant/database/production/scores/scores_{year}.parquet")
        # scores = scores_table.read(year, start_date=year_start, end_date=year_end)

        if scores is None or scores.height == 0:
            continue

        signal_alphas = compute_signal_alphas(scores, ic=ic)

        signal_alphas_table.create_if_not_exists(year)
        signal_alphas_table.upsert(year, signal_alphas)


def combined_alpha_backfill_flow(
    *,
    database: Database,
    start_date: date,
    end_date: date,
    signal_combinator,
) -> None:
    """
    Reads signal_alphas_table, writes combined_alpha_table.
    """
    signal_alphas_table = database.signal_alphas_table
    combined_alpha_table = database.combined_alpha_table

    years = list(range(start_date.year, end_date.year + 1))
    for year in tqdm(years, desc="Combined Alpha Backfill"):
        year_start = max(start_date, dt.date(year, 1, 1))
        year_end = min(end_date, dt.date(year, 12, 31))

        signal_alphas = pl.read_parquet(f"/Users/nathanpreslar/groups/grp_quant/database/production/signal_alphas/signal_alphas_{year}.parquet")
        # signal_alphas = signal_alphas_table.read(year, start_date=year_start, end_date=year_end)

        if signal_alphas is None or signal_alphas.height == 0:
            continue

        combined_alpha = compute_combined_alpha(signal_alphas, signal_combinator=signal_combinator)

        combined_alpha_table.create_if_not_exists(year)
        combined_alpha_table.upsert(year, combined_alpha)


def alphas_daily_flow(
    *,
    database: Database,
    dates: list[date],
    ic: float = 0.5,
    signal_combinator=None,
) -> None:
    """
    Daily update window:
      1) compute signal_alphas
      2) compute combined alpha (if combinator provided)
    """
    if not dates:
        dates = [(dt.date.today() - dt.timedelta(days=i)) for i in range(3)]

    start_date = min(dates)
    end_date = max(dates)

    signal_alphas_backfill_flow(
        database=database,
        start_date=start_date,
        end_date=end_date,
        ic=ic,
    )

    if signal_combinator is not None:
        combined_alpha_backfill_flow(
            database=database,
            start_date=start_date,
            end_date=end_date,
            signal_combinator=signal_combinator,
        )