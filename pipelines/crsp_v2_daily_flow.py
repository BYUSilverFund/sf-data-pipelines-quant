from datetime import date
import os
from pipelines.utils import crsp_v2_schema
import polars as pl
import wrds
from tqdm import tqdm
from pipelines.utils.tables import Database


def load_crsp_v2_daily_df(start_date: date, end_date: date, user: str) -> pl.DataFrame:
    wrds_db = wrds.Connection(
        wrds_username=user, wrds_password=os.getenv("WRDS_PASSWORD")
    )

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
                dlylow AS low,
                dlyclose AS close,
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


def _deduplicate_crsp_v2_daily_rows(df: pl.DataFrame, year: int) -> pl.DataFrame:
    """Drop identical WRDS rows and reject conflicting date/PERMNO records."""
    unique_rows = df.unique(maintain_order=True)
    duplicate_rows = df.height - unique_rows.height

    conflicting_keys = (
        unique_rows.group_by(["date", "permno"])
        .len()
        .filter(pl.col("len") > 1)
    )
    if conflicting_keys.height:
        sample = conflicting_keys.head(5).to_dicts()
        raise ValueError(
            f"CRSP v2 returned conflicting rows for {conflicting_keys.height} "
            f"date/PERMNO keys in {year}; refusing to replace that year's file. "
            f"Sample: {sample}"
        )

    if duplicate_rows:
        print(f"Removed {duplicate_rows} identical CRSP v2 rows for {year}.")

    return unique_rows


def crsp_v2_daily_backfill_flow(
    start_date: date, end_date: date, database: Database, user: str
) -> None:
    years = list(range(start_date.year, end_date.year + 1))
    table = database.crsp_v2_daily_table

    for year in tqdm(years, desc="CRSP Daily"):
        df = load_crsp_v2_daily_df(
            start_date=date(year, 1, 1), end_date=date(year, 12, 31), user=user
        )
        df = _deduplicate_crsp_v2_daily_rows(df, year)
        first_date = df.get_column("date").min() if df.height else None
        last_date = df.get_column("date").max() if df.height else None
        print(
            f"Rebuilding CRSP v2 daily {year}: {df.height} rows, "
            f"date coverage {first_date} through {last_date}."
        )
        table.overwrite_year(year, df)
