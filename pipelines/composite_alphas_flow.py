import polars as pl
import datetime as dt
from pipelines.utils.tables import Database
from tqdm import tqdm

# Signals averaged into the composite alpha. These match the live sf-trader
# config (vol_scaled_barra_momentum, barra_reversal_volume_clipped, ivol, beta).
SIGNALS = ['barra_momentum', 'barra_reversal', 'ivol', 'beta']

def composite_alphas_flow(start: dt.date, end: dt.date, database: Database):
    """
    Combine per-signal alphas into one composite alpha per asset.

    Matches sf-trader's mean combinator: a missing signal alpha counts as 0,
    so the composite is the sum of available alphas divided by the number
    of signals.

    Strategy:
    1. Lazy scan alphas table, filtered to SIGNALS and [start, end]
    2. Per (date, barrid): sum alphas and divide by len(SIGNALS)
    3. Write parquet files partitioned by year
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
