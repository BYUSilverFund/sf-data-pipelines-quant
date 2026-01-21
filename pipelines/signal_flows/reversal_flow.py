import datetime as dt
from pipelines.utils.tables import Database
import polars as pl
from pipelines.components.signals import get_signal
import sf_quant.data as sfd
from pipelines.utils.enums import DatabaseName
import dataframely as dy


def get_assets_signal(
    start_date: dt.date,
    end_date: dt.date,
    signal_name: str
) -> pl.DataFrame:
    """"""
    signal = get_signal(signal_name)
    lookback_days = signal.lookback_days

    columns = [
        "date",
        "barrid",
        "ticker",
        "return",
        "predicted_beta",
        "specific_risk",
    ]

    asset_data = (
        sfd.load_assets(
            start=(start_date - dt.timedelta(days=lookback_days * 2)),
            end=end_date, 
            columns=columns, 
            in_universe=True
        )
        .with_columns(pl.col("return").truediv(100))
        .sort("ticker", "date")
    )
    return asset_data

    # return Assets.validate(asset_data)


def reversal_backfill_flow(
    start_date: dt.date, 
    end_date: dt.date, 
    database: Database
) -> None:
    # get signal
    signal = get_signal("reversal")
    
    for year in range(start_date.year, end_date.year + 1):
        # get asset data to compute signal for a given year
        year_data = get_assets_signal(
            start_date=dt.date(year, 1, 1),
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
        print(f"\nyear data after computing signal:\n{year_data}")
        signals_df_year = year_data.filter(
            (pl.col("date").dt.year() == year)
        )
        print(f"\nSignals DataFrame for {year}:\n{signals_df_year}")
        
        # upsert into database
        database.signals_table.create_if_not_exists(year)
        database.signals_table.upsert(year, rows=signals_df_year)



# def reversal_signal_daily_flow(
#     database: Database
# ) -> None:
#     # get signal
#     signal = get_signal("reversal")

#     # get the last day of the signal
#     last_signal_date = database.signals_table.get_last_date(signal.name)
#     return
    


if __name__ == "__main__":
    database_name = DatabaseName("production")
    database_instance = Database(database_name)

    reversal_backfill_flow(
        start_date=dt.date(2023, 1, 1),
        end_date=dt.date(2025, 12, 31),
        database=database_instance
    )