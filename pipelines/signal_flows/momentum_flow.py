import datetime as dt
from pipelines.utils.tables import Database
import polars as pl


def momentum_backfill_flow(start_date: dt.date, end_date: dt.date, database: Database) -> None:
    # This is all just nonsense - Andrew
    # get data
    # - get factor exposures
    # - run pca
    # compute signal
    for year in range(start_date.year, end_date.year + 1):
        year_df = pl.DataFrame().filter(pl.col('date').year.eq(year))

        database.signals_table.create_if_not_exists(year)
        database.signals_table.upsert(year, rows=year_df)


    pass