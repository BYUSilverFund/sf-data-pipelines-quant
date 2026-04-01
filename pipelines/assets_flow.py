from datetime import date
import polars as pl
from pipelines.utils.tables import Database
from tqdm import tqdm

def assets_backfill_flow(start_date: date, end_date: date, database: Database) -> None:
    """
    Materialize assets table by combining data from multiple sources.

    Strategy:
    1. Lazy scan all sources at once
    2. Chain all lazy joins on all data
    3. Collect once on all data
    4. Per year: filter, finalize, and write to parquet
    """
    years = list(range(start_date.year, end_date.year + 1))

    # Step 1: Lazy scan all sources
    returns_lazy = database.barra_returns_table.read().filter(pl.col('date').is_between(start_date, end_date))
    specific_returns_lazy = database.barra_specific_returns_table.read().filter(pl.col('date').is_between(start_date, end_date))
    risk_lazy = database.barra_risk_table.read().filter(pl.col('date').is_between(start_date, end_date))
    volume_lazy = database.barra_volume_table.read().filter(pl.col('date').is_between(start_date, end_date))
    asset_ids_lazy = database.asset_ids_table.read_id_file()
    barra_ids_lazy = database.barra_ids_table.read_id_file()
    ftse_russell_lazy = database.ftse_russell_table.read()

    # Step 2: Create secondary lazy frames
    barra_tickers = (
        barra_ids_lazy
        .filter(
            pl.col("asset_id_type").eq("LOCALID"),
            pl.col("barrid").str.starts_with('US')
        )
        .drop('asset_id_type')
        .rename({"asset_id": "ticker"})
        .with_columns(pl.col("ticker").str.replace("US", "")) # Only works for US securities
    )

    barra_cusips = (
        barra_ids_lazy
        .filter(
            pl.col("asset_id_type").eq("CUSIP"),
            pl.col("barrid").str.starts_with('US')
        )
        .drop('asset_id_type')
        .rename({"asset_id": "cusip"})
    )

    russell_rebalance_dates = (
        ftse_russell_lazy
        .select("date", pl.lit(True).alias("russell_rebalance"))
        .unique()
        .sort('date')
    )
    
    # Step 3: Perform simple lazy joins
    combined = (
        returns_lazy
        .join(
            specific_returns_lazy,
            on=["date", "barrid"],
            how="left"
        )
        .join(
            risk_lazy,
            on=["date", "barrid"],
            how="left"
        )
        .join(
            volume_lazy,
            on=["date", "barrid"],
            how="left"
        )
    )

    # Step 4: Perform complex lazy join_asofs
    combined = (
        combined
        # Join asset ids table
        .sort('date', 'barrid')
        .join_asof(
            other=asset_ids_lazy.sort('barrid', 'start_date'),
            left_on="date",
            right_on="start_date",
            by="barrid",
            strategy="backward"
        )
        .drop('start_date', 'end_date')
        # Join tickers
        .sort('date', 'barrid')
        .join_asof(
            other=barra_tickers.sort('barrid', 'start_date'),
            left_on="date",
            right_on="start_date",
            by="barrid",
            strategy="backward"
        )
        .drop('start_date', 'end_date')
        # Join cusips
        .sort('date', 'barrid')
        .join_asof(
            other=barra_cusips.sort('barrid', 'start_date'),
            left_on="date",
            right_on="start_date",
            by="barrid",
            strategy="backward"
        )
    )

    # Step 5: Join raw ftse_russell and perform rebalance aware constituency forward fill
    combined = (
        combined
        .drop('start_date', 'end_date')
        # Join ftse russell data
        .sort('date', 'barrid')
        .join(
            other=ftse_russell_lazy,
            on=['date', 'cusip'],
            how='left'
        )
        # Join russell rebalance dates
        .sort('date', 'barrid')
        .join(russell_rebalance_dates, on="date", how="left")
        # Forward fill russell constituency
        .with_columns(
            pl.when(pl.col("russell_rebalance")).then(
                pl.col("russell_1000", "russell_2000").fill_null(False)
            )
        )
        .sort("barrid", "date")
        .with_columns(
            pl.col("russell_1000", "russell_2000")
            .fill_null(strategy="forward")
            .over("barrid")
        )
        # Create in_universe column
        .with_columns(
            pl.col("russell_1000").or_(pl.col("russell_2000")).alias("in_universe")
        )
        .drop("russell_rebalance")
    )

    combined_eager = combined.collect()

    # Step 6: Per year, filter and write to parquet
    for year in tqdm(years, desc="Assets Backfill"):
        database.assets_table.delete(year)
        year_data = combined_eager.filter(pl.col("date").dt.year().eq(year))
        year_data.write_parquet(database.assets_table._file_path(year))

if __name__ == '__main__':
    from pipelines.utils.enums import DatabaseName
    start = date(1995, 1, 1)
    # end = date(2025, 12, 31)
    # start = date(2010, 1, 1)
    end = date(2026, 12, 31)
    db = Database(DatabaseName.PRODUCTION)
    assets_backfill_flow(start, end, db)
