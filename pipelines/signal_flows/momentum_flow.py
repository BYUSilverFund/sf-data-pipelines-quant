import datetime as dt
from pipelines.utils.tables import Database
import polars as pl
from pipelines.signal_flows.reversal_flow import get_assets_signal
from pipelines.utils.enums import DatabaseName
from pipelines.components.signals import get_signal

def momentum_backfill_flow(
    start_date: dt.date, 
    end_date: dt.date, 
    database: Database
) -> None:
    # get signal
    signal = get_signal("momentum")

    for year in range(start_date.year, end_date.year + 1):
        # get asset data to compute signal for a given year
        year_data = get_assets_signal(
            start_date=(dt.date(year, 1, 1) - dt.timedelta(days=signal.lookback_days)),
            end_date=dt.date(year, 12, 31),
            signal_name=signal.name
        )
        print(f"\nAsset data for {year}:\n")

        # compute signal, filter to year, select relevant columns
        year_data = (
            year_data
            .with_columns(signal.expr)
            .with_columns(
                pl.lit(signal.name).alias("signal_name")
            )
            .select(["date", "barrid", "ticker", "signal_name", f"{signal.name}"])
            .rename({f"{signal.name}": "signal_value"})
        )
        signals_df_year = year_data.filter(
            (pl.col("date").dt.year() == year)
        )
        print(f"\nSignals dataframe:\n{signals_df_year}")

        # upsert into database
        database.signals_table.create_if_not_exists(year)
        database.signals_table.upsert(year, rows=signals_df_year)


if __name__ == "__main__":
    database_name = DatabaseName("production")
    database_instance = Database(database_name)

    momentum_backfill_flow(
        start_date=dt.date(2023, 1, 1),
        end_date=dt.date(2025, 12, 31),
        database=database_instance
    )