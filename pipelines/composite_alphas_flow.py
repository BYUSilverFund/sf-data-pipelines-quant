import polars as pl
import datetime as dt
from utils.tables import Database
from tqdm import tqdm

SIGNALS = ['barra_reveral', 'beta', 'ivol']

def composite_alphas_flow(start: dt.date, end: dt.date, database: Database):
    signal_weights = (
        database.signal_weights_table.read()
        .filter(
            pl.col('date').is_between(start, end),
            pl.col('signal_name').is_in(SIGNALS)
        )
        .cast({'weight': pl.Float64})
        .drop_nans()
        .sort('date', 'signal_name')
        .collect()
    )

    alphas = (
        database.alpha_table.read()
        .filter(
            pl.col('signal_name').is_in(SIGNALS),
            pl.col('date').is_between(start, end)
        )
        .sort('date', 'barrid', 'signal_name')
        .collect()
    )

    composite_alphas = (
        alphas
        .join(signal_weights, on=['date', 'signal_name'], how='inner') # It would be better if this was "left" and we knew how to deal with weird values.
        .group_by('date', 'barrid')
        .agg(
            pl.col('alpha').mul(pl.col('weight')).sum()
        )
        .sort('date', 'barrid')
    )

    years = composite_alphas['date'].dt.year().unique().sort().to_list()

    for year in tqdm(years, desc="Signal Returns"):
        year_df = composite_alphas.filter(pl.col("date").dt.year().eq(year))

        database.composite_alphas_table.create_if_not_exists(year)
        database.composite_alphas_table.upsert(year, year_df)

if __name__ == '__main__':
    from utils.tables import DatabaseName
    db = Database(DatabaseName.DEVELOPMENT)
    start = dt.date(2000, 1, 1)
    end = dt.date(2024, 12, 31)
    composite_alphas_flow(start, end, db)