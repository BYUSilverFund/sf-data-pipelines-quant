""""""

import polars as pl
import sf_quant.data as sfd
import datetime as dt




def get_assets_signal(
    start_date: dt.date,
    end_date: dt.date,
    lookback: int
) -> pl.DataFrame:
    """"""

    columns = [
        "date",
        "barrid",
        "ticker",
        "return",
        "predicted_beta",
        "specific_risk",
        "in_universe"
    ]

    asset_data = (
        sfd.load_assets(
            start=(start_date - dt.timedelta(days=lookback * 2)),
            end=end_date, 
            columns=columns, 
            in_universe=True
        )
        .with_columns(pl.col("return").truediv(100))
        .sort("ticker", "date")
    )
    return asset_data


def add_metadata_filter(df: pl.DataFrame, signal_name: str, year: int) -> pl.DataFrame:
    return (
        df
        .with_columns(
            pl.lit(signal_name).alias("signal_name")
        )
        .filter(pl.col("date").dt.year() == year)
    )