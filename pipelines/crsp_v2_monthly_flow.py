from datetime import date
from pipelines.utils import crsp_v2_schema
import polars as pl
import wrds
from tqdm import tqdm
from pipelines.utils.tables import Database


def load_crsp_v2_monthly_df(start_date: date, end_date: date) -> pl.DataFrame:
    wrds_db = wrds.Connection(wrds_username="amh1124")

    df = wrds_db.raw_sql(
        f"""
            SELECT
                mthcaldt AS date,
                permno,
                cusip,
                ticker,
                mthret AS ret,
                mthretx AS retx,
                mthprc AS prc,
                mthvol AS vol,
                shrout,
                primaryexch,
                securitytype
            FROM crsp_m_stock.wrds_msfv2_query a
            WHERE a.mthcaldt BETWEEN '{start_date}' AND '{end_date}'
            ;
        """
    )
    df = pl.from_pandas(df, schema_overrides=crsp_v2_schema)

    return df


def crsp_v2_monthly_backfill_flow(
    start_date: date, end_date: date, database: Database
) -> None:
    years = list(range(start_date.year, end_date.year + 1))

    df = load_crsp_v2_monthly_df(start_date, end_date)

    for year in tqdm(years, desc="CRSP Monthly"):
        year_df = df.filter(pl.col("date").dt.year().eq(year))

        database.crsp_v2_monthly_table.create_if_not_exists(year)
        database.crsp_v2_monthly_table.upsert(year, year_df)
