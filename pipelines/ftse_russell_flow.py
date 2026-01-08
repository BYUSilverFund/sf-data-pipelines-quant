from datetime import date
from pipelines.utils import russell_schema, russell_columns
import polars as pl
import wrds
from tqdm import tqdm
from pipelines.utils.tables import Database


def load_ftse_russell_df(start_date: date, end_date: date, user: str) -> pl.DataFrame:
    """Load FTSE Russell data from WRDS for the given date range."""
    wrds_db = wrds.Connection(wrds_username=user)

    df = wrds_db.raw_sql(
        f"""
            SELECT 
                date, 
                cusip, 
                russell2000,
                russell1000
            FROM ftse_russell_us.idx_holdings_us
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY cusip, date
        """
    )
    return pl.from_pandas(df, schema_overrides=russell_schema)


def clean(df: pl.DataFrame) -> pl.DataFrame:
    """Clean and standardize FTSE Russell dataframe."""
    return df.rename(russell_columns, strict=False).with_columns(
        pl.col("russell_2000", "russell_1000").eq("Y")
    )


def get_universe(database: Database) -> pl.LazyFrame:
    """Get the base universe of assets."""
    return (
        database.assets_table.read()
        .filter(pl.col("barrid").eq(pl.col("rootid")))
        .filter(pl.col("iso_country_code").eq("USA"))
        .select('date', 'barrid', 'cusip')
    )


def get_russell_rebalance_dates(universe: pl.LazyFrame, df_ftse: pl.DataFrame) -> pl.LazyFrame:
    """Identify Russell rebalance dates."""
    return (
        universe
        .join(df_ftse.lazy(), on=['date', 'cusip'], how='left')
        .filter(pl.col("russell_1000") | pl.col("russell_2000"))
        .select("date", pl.lit(True).alias("russell_rebalance"))
        .unique()
    )


def get_in_universe_fields(database: Database, df_ftse: pl.DataFrame) -> pl.DataFrame:
    """Compute in_universe fields by joining FTSE data with the universe."""
    universe = get_universe(database)
    russell_rebalance_dates = get_russell_rebalance_dates(universe, df_ftse)

    return (
        universe
        .join(df_ftse.lazy(), on=['date', 'cusip'], how='left')
        .join(russell_rebalance_dates, on="date", how="left")
        .with_columns(
            pl.when(pl.col("russell_rebalance")).then(
                pl.col("russell_1000", "russell_2000").fill_null(False)
            )
        )
        .sort(["barrid", "date"])
        .with_columns(
            pl.col("russell_1000", "russell_2000")
            .fill_null(strategy="forward")
            .over("barrid")
        )
        .with_columns(
            pl.col('russell_1000').or_(pl.col('russell_2000')).alias('in_universe')
        )
        .sort("barrid", "date")
        .select('date', 'barrid', 'russell_1000', 'russell_2000', 'in_universe')
        .collect()
    )


def ftse_russell_backfill_flow(
    start_date: date, end_date: date, database: Database, user: str
) -> None:
    """
    Flow for orchestrating FTSE Russell backfill.
    
    Loads all FTSE data at once, computes in_universe fields,
    then updates files by year.
    """
    # Load all FTSE data for the entire date range
    raw_df = load_ftse_russell_df(start_date=start_date, end_date=end_date, user=user)
    clean_df = clean(raw_df)
    
    # Compute in_universe fields once for all data
    in_universe_fields = get_in_universe_fields(database, clean_df)
    
    # Update files by year
    years = list(range(start_date.year, end_date.year + 1))
    
    for year in tqdm(years, desc="Updating FTSE Russell by year"):
        # Filter in_universe_fields for the current year
        year_data = in_universe_fields.filter(
            (pl.col("date").dt.year() == year)
        )
        
        if database.assets_table.exists(year):
            database.assets_table.update(year, year_data, on=["date", "barrid"])