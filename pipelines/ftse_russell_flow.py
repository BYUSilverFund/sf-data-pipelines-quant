from datetime import date
from pipelines.utils import russell_schema, russell_columns
import polars as pl
import wrds
from tqdm import tqdm
from pipelines.utils.tables import Database
import os


def load_ftse_russell_df(start_date: date, end_date: date, user: str) -> pl.DataFrame:
    """Load FTSE Russell data from WRDS for the given date range."""
    wrds_db = wrds.Connection(wrds_username=user)

    df = wrds_db.raw_sql(
        f"""
            SELECT
                a.date,
                a.cusip,
                a.russell2000,
                a.russell1000,
                b.cik
            FROM ftse_russell_us.idx_holdings_us a
            LEFT JOIN (
                SELECT
                    names.cusip AS cusip,
                    LPAD(CAST(company.cik AS varchar), 10, '0') AS cik
                FROM comp.names AS names
                INNER JOIN comp.company AS company
                    ON names.gvkey = company.gvkey
                WHERE names.cusip IS NOT NULL
                    AND company.cik IS NOT NULL
            ) b
                -- FTSE and Compustat both store full CUSIPs, but the stable issuer
                -- match for downstream 10-K work is the first 8 characters.
                ON LEFT(a.cusip, 8) = LEFT(b.cusip, 8)
            WHERE a.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY a.cusip, a.date
        """
    )
    return pl.from_pandas(df, schema_overrides=russell_schema)


def clean(df: pl.DataFrame) -> pl.DataFrame:
    """Clean and standardize FTSE Russell dataframe."""
    return df.rename(russell_columns, strict=False).with_columns(
        pl.col("russell_2000", "russell_1000").eq("Y"),
        # Keep CIK as a zero-padded string so EDGAR lookups do not depend on
        # integer formatting later in the 10-K flow.
        pl.col("cik").cast(pl.String).str.strip_chars(),
    )


def ftse_russell_backfill_flow(
    start_date: date, end_date: date, database: Database
) -> None:
    """
    Flow for orchestrating FTSE Russell backfill.

    Loads FTSE data, cleans it, and writes to the ftse_russell table by year.
    """
    user = os.getenv("WRDS_USER")
    raw_df = load_ftse_russell_df(start_date=start_date, end_date=end_date, user=user)
    clean_df = clean(raw_df)

    years = list(range(start_date.year, end_date.year + 1))

    for year in tqdm(years, desc="FTSE Russell"):
        year_data = clean_df.filter(pl.col("date").dt.year() == year)

        database.ftse_russell_table.create_if_not_exists(year)
        database.ftse_russell_table.upsert(year, year_data)

if __name__ == '__main__':
    from utils.enums import DatabaseName
    start = date(1995, 1, 1)
    end = date(2025, 12, 31)
    db = Database(DatabaseName.DEVELOPMENT)
    ftse_russell_backfill_flow(start, end, db)
