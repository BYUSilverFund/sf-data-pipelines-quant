import datetime as dt
from pipelines.utils.tables import Database
import polars as pl
from pipelines.components.signals import get_signal
import sf_quant.data as sfd
from pipelines.utils.enums import DatabaseName
# import dataframely as dy

def get_assets_signal(
    start_date: dt.date,
    end_date: dt.date,
    signal_name: str
) -> pl.DataFrame:
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
            start=start_date - dt.timedelta(days=lookback_days),
            end=end_date, 
            columns=columns, 
            in_universe=True
        )
        .with_columns(pl.col("return").truediv(100))
        # .filter(pl.col("ticker").is_in(tickers))
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
    
    # iteratively get data, compute signal, save to database
    for year in range(start_date.year, end_date.year + 1):
        year_data = get_assets_signal(
            start_date=start_date,
            end_date=end_date,
            signal_name=signal.name
        )

        year_data = year_data.with_columns(
            signal.expr.alias(signal.name),
            pl.lit(signal.name).alias("signal_name")
        )

        signals_df_year = year_data.filter(
            (pl.col("date").dt.year == year)
        )

        database.signals_table.create_if_not_exists(year)
        database.signals_table.upsert(year, rows=signals_df_year)

if __name__ == "__main__":
    database_name = DatabaseName("production")
    database_instance = Database(database_name)

    reversal_backfill_flow(
        start_date=dt.date(2023, 1, 1),
        end_date=dt.date(2024, 1, 1),
        database=database_instance
    )