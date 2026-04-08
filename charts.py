import datetime as dt
import polars as pl
import sf_quant.data as sfd
import seaborn as sns
import matplotlib.pyplot as plt

weights_path = "weights/composite_active/0.05/*.parquet"
weights = pl.read_parquet(weights_path)

start = dt.date(2005, 1, 7)
end = dt.date(2024, 12, 31)
returns = (
    sfd.load_assets(
        start=start,
        end=end,
        columns=['date', 'barrid', 'market_cap', 'return'],
        in_universe=True
    )
    .sort('date', 'barrid')
    .with_columns(
        pl.col('return')
        .truediv(100)
        .shift(-1)
        .over('barrid')
    )
)

portfolio_returns = (
    weights
    .sort('date', 'barrid')
    .join(returns, on=['date', 'barrid'], how='left')
    .group_by('date')
    .agg(
        pl.col('return').mul(pl.col('weight')).sum()
    )
    .sort('date')
)

benchmark_returns = (
    returns
    .with_columns(
        pl.col('market_cap')
        .truediv(pl.col('market_cap').sum())
        .over('date')
        .alias('weight')
    )
    .sort('date', 'barrid')
    .group_by('date')
    .agg(
        pl.col('return')
        .mul(pl.col('weight'))
        .sum()
    )
    .with_columns(
        pl.col('return')
        .log1p()
        .cum_sum()
        .mul(100)
        .alias('cumulative_return')
    )
    .with_columns(
        pl.lit('Benchmark').alias('portfolio')
    )
)

cumulative_returns = (
    portfolio_returns
    .sort('date')
    .with_columns(
        pl.col('return')
        .log1p()
        .cum_sum()
        .mul(100)
        .alias('cumulative_return')
    )
    .with_columns(
        pl.lit('Silver Fund').alias('portfolio')
    )
)

all_returns = (
    # pl.concat([cumulative_returns, benchmark_returns])
    cumulative_returns
    # .filter(pl.col('date').ge(dt.date(2022, 1, 1)))
)

summary = (
    all_returns
    # .pivot(index='date', on='portfolio', values='return')
    # .with_columns(
    #     pl.col('Silver Fund').sub(pl.col('Benchmark')).alias('Active')
    # )
    # .unpivot(index='date', variable_name='portfolio', value_name='return')
    .group_by('portfolio')
    .agg(
        pl.col('return').mean().mul(252 * 100).alias('mean_return'),
        pl.col('return').std().mul(pl.lit(252).sqrt() * 100).alias('volatility'),
    )
    .with_columns(
        pl.col('mean_return').truediv(pl.col('volatility')).alias('sharpe')
    )
)
print(summary)

sns.lineplot(all_returns, x='date', y='cumulative_return', hue='portfolio')
plt.title("Composite Alpha Active Backtest")

plt.legend(title='Portfolio')
plt.xlabel(None)
plt.ylabel("Cumulative Log Returns (%)")

plt.savefig("composite_active_backtest.png")