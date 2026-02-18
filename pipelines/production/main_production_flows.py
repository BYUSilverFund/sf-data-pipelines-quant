import datetime as dt

from pipelines.utils.tables import Database
from pipelines.utils.enums import DatabaseName

from pipelines.production.signal_flows import signals_backfill_flow, signals_daily_flow
from pipelines.production.score_flows import scores_backfill_flow, scores_daily_flow
from pipelines.production.alpha_flows import (
    signal_alphas_backfill_flow, 
    combined_alpha_backfill_flow, 
    signal_alphas_daily_flow, 
    combined_alpha_daily_flow
)
from pipelines.production.weight_flows import weights_backfill_flow, weights_daily_flow


def production_backfill_flow(
    *,
    database: Database,
    start_date: dt.date,
    end_date: dt.date,
    load_assets_fn,
    signal_combinator,
) -> None:
    """End to end backfill across all components."""

    # signals 
    signals_backfill_flow(
        database=database,
        start_date=start_date,
        end_date=end_date,
        load_assets_fn=load_assets_fn,
    )

    # scores
    scores_backfill_flow(
        database=database,
        start_date=start_date,
        end_date=end_date,
    )

    # signal alphas
    signal_alphas_backfill_flow(
        database=database,
        start_date=start_date,
        end_date=end_date,
    )

    # combined alpha
    combined_alpha_backfill_flow(
        database=database,
        start_date=start_date,
        end_date=end_date,
        signal_combinator=signal_combinator,
    )

    # weights
    weights_backfill_flow(
        database=database,
        start_date=start_date,
        end_date=end_date,
    )


def production_daily_flow(
    *,
    database: Database,
    dates: list[dt.date],
    load_assets_fn,
    signal_combinator
) -> None:
    """Daily flow across all components."""

    if not dates:
        dates = [(dt.date.today() - dt.timedelta(days=i)) for i in range(3)]

    # signals
    signals_daily_flow(
        database=database,
        dates=dates,
        load_assets_fn=load_assets_fn,
    )

    # scores
    scores_daily_flow(
        database=database,
        dates=dates,
    )

    # signal alphas
    signal_alphas_daily_flow(
        database=database,
        dates=dates,
    )

    # combined alpha
    combined_alpha_daily_flow(
        database=database,
        dates=dates,
        signal_combinator=signal_combinator,
    )

    # weights
    weights_daily_flow(
        database=database,
        dates=dates,
    )