from datetime import date
from pipelines.utils import crsp_schema
import polars as pl
import wrds
from tqdm import tqdm
from pipelines.utils.tables import Database


def load_crsp_monthly_df(start_date: date, end_date: date) -> pl.DataFrame:
    wrds_db = wrds.Connection(wrds_username="amh1124")

    df = wrds_db.raw_sql(
        f"""
        SELECT
            date,
            permno,
            cusip,
            ret,
            retx,
            prc,
            vol,
            shrout
        FROM crsp_m_stock.msf a
        WHERE a.date BETWEEN '{start_date}' AND '{end_date}'
        ;
        """
    )
    df = pl.from_pandas(df, schema_overrides=crsp_schema)

    return df


def crsp_monthly_backfill_flow(
    start_date: date, end_date: date, database: Database
) -> None:
    years = list(range(start_date.year, end_date.year + 1))

    df = load_crsp_monthly_df(start_date, end_date)

    for year in tqdm(years, desc="CRSP Monthly"):
        year_df = df.filter(pl.col("date").dt.year().eq(year))

        database.crsp_monthly_table.create_if_not_exists(year)
        database.crsp_monthly_table.upsert(year, year_df)
