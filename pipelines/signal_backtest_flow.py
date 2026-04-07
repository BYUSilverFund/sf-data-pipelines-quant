import datetime as dt
from pipelines.signals import SIGNALS
import polars as pl
import sf_quant.data as sfd
from sf_backtester import BacktestDynamicConfig, BacktestDynamicRunner, SlurmConfig
import os

def signal_returns_backfill_flow(start: dt.date, end: dt.date, signal_name: str) -> None:
    # Get signal config
    signal_config = SIGNALS[signal_name]

    # Load necessary data
    assets = sfd.load_assets(start, end, columns=['date', 'barrid', 'predicted_beta'], in_universe=True)
    benchmark_weights = sfd.load_benchmark(start, end).rename({'weight': 'benchmark_weight'})
    alphas = sfd.load_alphas(start, end, names=[signal_name]).drop('signal_name')
    
    # Combine data
    data = (
        assets
        .join(benchmark_weights, on=['date', 'barrid'], how='left')
        .join(alphas, on=['date', 'barrid'], how='left')
        .with_columns(pl.col('alpha').fill_null(0))
        .sort('date', 'barrid')
    )

    # Save data to temporary file
    os.makedirs("data", exist_ok=True)
    data_path = f"data/{signal_name}_{start}_{end}.parquet"
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
        signal_name=signal_name,
        data_path=data_path,
        initial_gamma=100,
        target_active_risk=0.05,
        active_weights=True,
        project_root=os.getenv("PROJECT_ROOT"),
        byu_email=os.getenv("BYU_EMAIL"),
        constraints=signal_config['constraints'],
        slurm=slurm_config
    )

    # Run backtest
    runner = BacktestDynamicRunner(config)
    runner.submit()
    

if __name__ == '__main__':
    start = dt.date(1995, 1, 1)
    end = dt.date(2025, 12, 31)
    signal_name = 'reversal'
    signal_returns_backfill_flow(start, end, signal_name)
