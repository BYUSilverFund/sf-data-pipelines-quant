from datetime import date
import zipfile
import polars as pl
from io import BytesIO
from pipelines.utils import barra_schema, barra_columns
import os
from tqdm import tqdm
from pipelines.utils import get_last_market_date
from pipelines.utils.barra_datasets import barra_ids
from pipelines.utils.tables import Database
import datetime as dt


def load_current_barra_files() -> pl.DataFrame:
    dates = get_last_market_date(n_days=60)

    for date_ in reversed(dates):
        zip_folder_path = barra_ids.daily_zip_folder_path(date_)
        file_path = barra_ids.file_name(date_)

        if os.path.exists(zip_folder_path):
            with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
                return pl.read_csv(
                    BytesIO(zip_folder.read(file_path)),
                    skip_rows=1,
                    separator="|",
                    schema_overrides=barra_schema,
                    try_parse_dates=True,
                )

    return pl.DataFrame()


def clean_barra_df(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(barra_columns, strict=False)
        .with_columns(pl.col("start_date", "end_date").str.strptime(pl.Date, "%Y%m%d"))
        .filter(
            pl.col("barrid").ne("[End of File]"),
            pl.col("asset_id_type").eq("CUSIP"),
        )
        .drop('asset_id_type')
        .rename({"assetid": "cusip"})
        .sort(["barrid", "start_date", "end_date"])
    )


def barra_cusips_daily_flow(database: Database) -> None:
    raw_df = load_current_barra_files()
    clean_df = clean_barra_df(raw_df)

    min_date = clean_df['start_date'].min()
    max_date = min(clean_df['end_date'].max(), dt.date.today())

    years = list(range(min_date.year, max_date.year + 1))

    for year in tqdm(years, desc="Barra Cusips"):
        if database.assets_table.exists(year):
            database.assets_table.update_asof(
                year=year,
                right_df=clean_df,
                left_on='date',
                right_on='start_date',
                by='barrid',
                strategy='backward',
                drop_right_cols=['start_date', 'end_date']
            )
