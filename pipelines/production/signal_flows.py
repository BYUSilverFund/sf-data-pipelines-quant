import polars as pl
from datetime import date

from pipelines.signals import discover_signals
from pipelines.utils.tables import Database


def compute_signals_long(assets: pl.DataFrame, signals) -> pl.DataFrame:
    """
    assets must include:
      date, barrid, ticker, specific_risk, in_universe
    signals are BaseSignal instances with .name and .expr producing a column named signal.name (or aliased).
    """
    # Compute wide signal columns
    wide = (
        assets.sort(["ticker", "date"])
        .with_columns([s.expr for s in signals])
    )

    id_cols = ["date", "barrid", "ticker", "specific_risk", "in_universe"]
    signal_cols = [s.name for s in signals]

    # Convert wide -> long to match signals_table schema
    long = (
        wide
        .select(id_cols + signal_cols)
        .melt(
            id_vars=id_cols,
            value_vars=signal_cols,
            variable_name="signal_name",
            value_name="signal_value",
        )
        .drop_nulls(["signal_value"])
    )

    # Replace with dataframely check
    return long.with_columns([
        pl.col("date").cast(pl.Date),
        pl.col("barrid").cast(pl.String),
        pl.col("ticker").cast(pl.String),
        pl.col("signal_name").cast(pl.String),
        pl.col("signal_value").cast(pl.Float64),
        pl.col("specific_risk").cast(pl.Float64),
        pl.col("in_universe").cast(pl.Boolean),
    ])


def signals_backfill_flow(
    *,
    database: Database,
    start_date: date,
    end_date: date,
    load_assets_fn,  # inject your existing loader
) -> None:
    """
    Backfill across a range. Saves parquets partitioned by year via signals_table.
    """
    signals = discover_signals()
    table = database.signals_table

    years = list(range(start_date.year, end_date.year + 1))
    for year in tqdm(years, desc="Signals Backfill"):
        year_start = max(start_date, date(year, 1, 1))
        year_end = min(end_date, date(year, 12, 31))

        # Load the raw inputs needed by signals
        assets = load_assets_fn(year_start, year_end).select([
            "date", "barrid", "ticker", "specific_risk", "in_universe",
            # plus any other columns signals require (returns, fundamentals, etc.)
        ])

        out = compute_signals_long(assets, signals)

        table.create_if_not_exists(year)
        table.upsert(year, out)


def signals_daily_flow(
    *,
    database: Database,
    dates: list[date],     # e.g. last 3 market dates
    load_assets_fn,
) -> None:
    """
    Daily update flow. Just calls backfill over a small window.
    """
    if not dates:
        return
    signals_backfill_flow(
        database=database,
        start_date=min(dates),
        end_date=max(dates),
        load_assets_fn=load_assets_fn,
    )