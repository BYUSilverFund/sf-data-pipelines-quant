import datetime as dt
import polars as pl
from pipelines.signals.base import BaseSignal
from pipelines.utils.tables import Database
from pipelines.signals.reversal_flow import get_assets_signal


def add_metadata_filter(df: pl.DataFrame, signal_name: str, year: int) -> pl.DataFrame:
    return (
        df
        .with_columns(
            pl.lit(signal_name).alias("signal_name")
        )
        .filter(pl.col("date").dt.year() == year)
    )


def run_single_backfill(signal: BaseSignal, start_date: dt.date, end_date: dt.date, database: Database) -> None:
    """Executes the pipeline for one signal instance across a date range."""

    for year in range(start_date.year, end_date.year + 1):
        # get raw data
        df = get_assets_signal(
            start_date=dt.date(year, 1, 1),
            end_date=dt.date(year, 12, 31),
            lookback=signal.lookback_days
        )

        # compute the signal
        results = signal.compute(df)

        # add metadata and filter to year
        final_df = add_metadata_filter(results, signal.name, year)

        # upsert into database
        database.signals_table.create_if_not_exists(year)
        database.signals_table.upsert(year, rows=final_df)