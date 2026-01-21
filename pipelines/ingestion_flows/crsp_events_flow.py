from datetime import date
from pipelines.utils import crsp_schema
import polars as pl
import wrds
from tqdm import tqdm
from pipelines.utils.tables import Database


def load_crsp_events_df(start_date: date, end_date: date, user: str) -> pl.DataFrame:
    wrds_db = wrds.Connection(wrds_username=user)

    df = wrds_db.raw_sql(
        f"""
            SELECT
                date,
                permno,
                ticker,
                shrcd,
                exchcd
            FROM crsp_m_stock.dse a
            WHERE a.date BETWEEN '{start_date}' AND '{end_date}'
                AND event = 'NAMES'
            ;
            """
    )
    df = pl.from_pandas(df, schema_overrides=crsp_schema)

    return df


def crsp_events_backfill_flow(
    start_date: date, end_date: date, database: Database, user: str
) -> None:
    years = list(range(start_date.year, end_date.year + 1))

    df = load_crsp_events_df(start_date, end_date, user)

    for year in tqdm(years, desc="CRSP Events"):
        year_df = df.filter(pl.col("date").dt.year().eq(year))

        database.crsp_events_table.create_if_not_exists(year)
        database.crsp_events_table.upsert(year, year_df)
