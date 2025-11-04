from datetime import date
from pipelines.utils import crsp_schema
import polars as pl
import wrds
from tqdm import tqdm
from pipelines.utils.tables import Database


def load_crsp_daily_df(start_date: date, end_date: date) -> pl.DataFrame:
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
                openprc,
                askhi,
                bidlo,
                shrout
            FROM crsp_m_stock.dsf a
            WHERE a.date BETWEEN '{start_date}' AND '{end_date}'
            ;
            """
    )
    df = pl.from_pandas(df, schema_overrides=crsp_schema)

    return df


def crsp_daily_backfill_flow(
    start_date: date, end_date: date, database: Database
) -> None:
    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="CRSP Daily"):
        df = load_crsp_daily_df(
            start_date=date(year, 1, 1), end_date=date(year, 12, 31)
        )

        database.crsp_daily_table.create_if_not_exists(year)
        database.crsp_daily_table.upsert(year, df)
