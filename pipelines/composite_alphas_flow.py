import polars as pl
import datetime as dt
from pipelines.utils.tables import Database
from tqdm import tqdm

SIGNALS = ['barra_momentum', 'barra_reversal', 'ivol', 'beta']

def composite_alphas_flow(start: dt.date, end: dt.date, database: Database):
    """
    Average the signal alphas into one composite alpha per stock.

    Steps:
    1. Read the alphas for SIGNALS between start and end
    2. For each date and stock, add up the alphas and divide by the number
       of signals, so a missing alpha counts as 0
    3. Save each year to the composite_alphas table
    """
    composite_alphas = (
        database.alpha_table.read()
        .filter(
            pl.col('signal_name').is_in(SIGNALS),
            pl.col('date').is_between(start, end)
        )
        .group_by('date', 'barrid')
        .agg(
            pl.col('alpha').fill_nan(0).sum().truediv(len(SIGNALS))
        )
        .sort('date', 'barrid')
        .collect()
    )

    years = composite_alphas['date'].dt.year().unique().sort().to_list()

    for year in tqdm(years, desc="Composite Alphas"):
        year_df = composite_alphas.filter(pl.col("date").dt.year().eq(year))

        database.composite_alphas_table.create_if_not_exists(year)
        database.composite_alphas_table.upsert(year, year_df)

if __name__ == '__main__':
    from pipelines.utils.enums import DatabaseName
    db = Database(DatabaseName.DEVELOPMENT)
    start = dt.date(2000, 1, 1)
    end = dt.date(2024, 12, 31)
    composite_alphas_flow(start, end, db)
