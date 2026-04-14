import polars as pl
import sf_quant.data as sfd
import datetime as dt
from utils.tables import Database
from tqdm import tqdm
from utils.tables import Database

def signal_returns_flow(signal_names: list[str], database: Database, active_risk: float = 0.05):
    all_signal_returns_list = []
    for signal_name in signal_names:        
        weights_path = f"weights/{signal_name}/{active_risk}/*.parquet"
        weights = pl.read_parquet(weights_path)

        start = dt.date(1995, 6, 30)
        end = dt.date.today()
        returns = (database.assets_table.read()
            .filter(pl.col('date').is_between(start, end))
            .select('date', 'barrid', 'return')
            .sort('date', 'barrid')
            .with_columns(
                pl.col('return')
                .truediv(100)
                .over('barrid')
            )
        )

        signal_returns = (
            weights
            .sort('date', 'barrid')
            .with_columns(
                pl.col('weight').shift(1).over('barrid')
            )
            .join(returns, on=['date', 'barrid'], how='left')
            .group_by('date')
            .agg(
                pl.col('return').mul(pl.col('weight')).sum()
            )
            .with_columns(
                pl.lit(signal_name).alias('signal_name')
            )
            .sort('date')
        )
        all_signal_returns_list.append(signal_returns)


    all_signal_returns = pl.concat(all_signal_returns_list)
    years = all_signal_returns['date'].dt.year().unique().sort().to_list()

    for year in tqdm(years, desc="Signal Returns"):
        year_df = all_signal_returns.filter(pl.col("date").dt.year().eq(year))

        database.signal_returns_table.create_if_not_exists(year)
        database.signal_returns_table.upsert(year, year_df)

if __name__ == '__main__':
    from utils.tables import DatabaseName
    db = Database(DatabaseName.DEVELOPMENT)
    signal_names = ['reversal', 'momentum', 'beta', 'barra_reversal', 'barra_momentum', 'ivol']
    signal_returns_flow(signal_names, db)