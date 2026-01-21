from datetime import date
from pipelines.utils import crsp_v2_schema
import polars as pl
import wrds
from tqdm import tqdm
from pipelines.utils.tables import Database


def load_crsp_v2_daily_df(start_date: date, end_date: date, user: str) -> pl.DataFrame:
    wrds_db = wrds.Connection(wrds_username=user)

    df = wrds_db.raw_sql(
        f"""
            SELECT
                dlycaldt AS date,
                permno,
                cusip,
                ticker,
                dlyret AS ret,
                dlyretx AS retx,
                dlyprc AS prc,
                dlyvol AS vol,
                dlyopen AS open,
                dlyhigh AS high,
                dlyhigh AS low,
                dlyhigh AS close,
                shrout,
                primaryexch,
                securitytype
            FROM crsp_m_stock.wrds_dsfv2_query a
            WHERE a.dlycaldt BETWEEN '{start_date}' AND '{end_date}'
            ;
            """
    )
    df = pl.from_pandas(df, schema_overrides=crsp_v2_schema)

    return df


def crsp_v2_daily_backfill_flow(
    start_date: date, end_date: date, database: Database, user: str
) -> None:
    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="CRSP Daily"):
        df = load_crsp_v2_daily_df(
            start_date=date(year, 1, 1), end_date=date(year, 12, 31), user=user
        )

        database.crsp_v2_daily_table.create_if_not_exists(year)
        database.crsp_v2_daily_table.upsert(year, df)
