import zipfile
import polars as pl
from io import BytesIO
from pipelines.utils import barra_schema, barra_columns
import os
from tqdm import tqdm
from pipelines.utils import get_last_market_date
from pipelines.utils.barra_datasets import barra_factors
from pipelines.utils.tables import Database


def load_current_barra_files() -> pl.DataFrame:
    dates = get_last_market_date(n_days=60)

    for date_ in reversed(dates):
        zip_folder_path = barra_factors.daily_zip_folder_path(date_)
        file_path = barra_factors.file_name(date_)

        if os.path.exists(zip_folder_path):
            with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
                return pl.read_csv(
                    BytesIO(zip_folder.read(file_path)),
                    skip_rows=2,
                    separator="|",
                    schema_overrides=barra_schema,
                    try_parse_dates=True,
                )

    return pl.DataFrame()


def clean_barra_df(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.rename(barra_columns, strict=False)
        .with_columns(pl.col("date").str.strptime(pl.Date, "%Y%m%d"))
        .with_columns(pl.col("return").mul(100))
        .filter(pl.col("factor").ne("[End of File]"))
        .pivot(index="date", on="factor", values="return")
        .sort("date")
    )

    return df


def barra_factors_daily_flow(database: Database) -> None:
    raw_df = load_current_barra_files()
    clean_df = clean_barra_df(raw_df)

    years = clean_df.select(pl.col("date").dt.year().unique().sort().alias("year"))[
        "year"
    ]

    for year in tqdm(years, desc="Daily Barra Factors"):
        year_df = clean_df.filter(pl.col("date").dt.year().eq(year))

        database.factors_table.create_if_not_exists(year)
        database.factors_table.upsert(year, year_df)
