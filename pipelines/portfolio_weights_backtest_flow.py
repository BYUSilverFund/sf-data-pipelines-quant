import datetime as dt
from pipelines.signals import SIGNALS
import polars as pl
from utils.tables import Database
from sf_backtester import BacktestDynamicConfig, BacktestDynamicRunner, SlurmConfig
import os

def portfolio_weights_backtest_flow(start: dt.date, end: dt.date, database: Database) -> None:
    # Load necessary data
    assets =(
        database.assets_table.read()
            .filter(
                pl.col('date').is_between(start, end),
                pl.col('in_universe')
                )
            .select(
                "date",
                "barrid",
                "predicted_beta",
                pl.col("market_cap")
                .truediv(pl.col("market_cap").sum())
                .over("date")
                .alias("benchmark_weight"),
            )
            .sort(["barrid", "date"])
        )
    alphas = database.composite_alphas_table.read().filter(pl.col('date').is_between(start, end)).sort(["barrid", "date"])

    # Combine data
    data = (
        assets
        .join(alphas, on=['date', 'barrid'], how='left')
        .with_columns(pl.col('alpha').fill_null(0))
        .sort('date', 'barrid')
    )

    # Save data to temporary file
    os.makedirs("data", exist_ok=True)
    data_path = f"data/composite_{start}_{end}.parquet"
    data.write_parquet(data_path)

    # Slurm config
    slurm_config = SlurmConfig(
        n_cpus=8,
        mem="32G",
        time="03:00:00",
        mail_type="BEGIN,END,FAIL",
        max_concurrent_jobs=30
    )

    # Backtester config
    config = BacktestDynamicConfig(
        signal_name='composite_active',
        data_path=data_path,
        initial_gamma=100,
        target_active_risk=0.05,
        active_weights=True,
        project_root=os.getenv("PROJECT_ROOT"),
        byu_email=os.getenv("BYU_EMAIL"),
        constraints=['ZeroInvestment', 'ZeroBeta'],
        slurm=slurm_config
    )

    # Run backtest
    runner = BacktestDynamicRunner(config)
    runner.submit()
    

if __name__ == '__main__':
    start = dt.date(2005, 1, 7)
    end = dt.date(2024, 12, 31)
    db = Database(DatabaseName.DEVELOPMENT)
    portfolio_weights_backtest_flow(start, end, db)
