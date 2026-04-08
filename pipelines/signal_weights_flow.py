import polars as pl
from utils.tables import Database
import numpy as np
import datetime as dt
from tqdm import tqdm

CONFIG = {
    'signals': ['beta', 'barra_reversal', 'ivol'],
    'prior_sharpes': [0.5, 0.5, 0.5],
    'N_0': 252 * 5,  # Number of prior pseudo-observations
    'N': 252 * 5,  # Number of likelihood observations
    'DAILY_VAR': 1e-5
}

def signal_weights_flow(start: dt.date, end: dt.date, database: Database):
    # Parse config
    signal_names = CONFIG['signals']
    prior_sharpes = CONFIG['prior_sharpes']
    N_0 = CONFIG['N_0']
    N = CONFIG['N']
    DAILY_VAR = CONFIG['DAILY_VAR']
    K = len(signal_names)

    # Prior hyperparameters (do not change these)
    mu_0 = np.array(prior_sharpes) * np.sqrt(DAILY_VAR) / np.sqrt(252) # Daily returns
    Sigma_0 = np.eye(K) * DAILY_VAR  # Diagonal daily variance

    # Load signal returns
    signal_returns = (
        database.signal_returns_table.read()
        .filter(
            pl.col('date').is_between(start, end),
            pl.col('signal_name').is_in(signal_names)
        )
        .with_columns(pl.col('forward_return').cast(pl.Float64))
        .sort('date', 'signal_name')
        .collect()
    )

    # Pivot to wide format
    signal_returns = signal_returns.pivot(index='date', on='signal_name', values='forward_return').sort('date')
    print(signal_returns)
    print(signal_returns.filter(pl.any_horizontal(pl.exclude('date').is_null())))
    dates = signal_returns['date'].to_list()
    returns_np = signal_returns.drop('date').to_numpy()

    rows = []
    for t in range(N, len(returns_np)):
        X = returns_np[t - N : t]
        x_bar = X.mean(axis=0)
        Sigma = np.cov(X, rowvar=False)

        # Posterior parameters (multivariate normal conjugate update)
        Sigma_0_inv = np.linalg.inv(Sigma_0)
        Sigma_inv = np.linalg.inv(Sigma)
        Sigma_n = np.linalg.inv(N_0 * Sigma_0_inv + N * Sigma_inv)
        mu_n = Sigma_n @ (N_0 * Sigma_0_inv @ mu_0 + N * Sigma_inv @ x_bar)

        weights_raw = np.linalg.solve(Sigma_n, mu_n)

        # Normalize
        weights_normalized = weights_raw / np.abs(weights_raw).sum()

        # Softmax
        exp_w = np.exp(weights_normalized - weights_normalized.max())
        weights_softmax = exp_w / exp_w.sum()

        rows.append(
            {'date': dates[t]} |
            {f"w_raw_{name}": mu_n[i] for i, name in enumerate(signal_names)} |
            {f"w_norm_{name}": weights_normalized[i] for i, name in enumerate(signal_names)} |
            {f'w_{name}': weights_softmax[i] for i, name in enumerate(signal_names)}
        )

    # Combine and smooth weights
    weight_columns = [f'w_{name}' for name in signal_names]
    signal_weights = pl.DataFrame(rows)
    signal_weights = (
        signal_weights
        .sort('date')
        .with_columns(
            pl.col(weight_columns).ewm_mean(span=252)
        )
        .sort('date')
    )

    signal_weights_long = (
        signal_weights
        .unpivot(index='date', on=weight_columns, variable_name='signal_name', value_name='weight')
        .with_columns(
            pl.col('signal_name').str.replace("w_", "")
        )
        .sort('date', 'signal_name')
    )

    print(signal_weights)
    print(signal_weights_long)

    years = signal_weights_long['date'].dt.year().unique().sort().to_list()

    for year in tqdm(years, desc="Signal Returns"):
        year_df = signal_weights_long.filter(pl.col("date").dt.year().eq(year))

        database.signal_weights_table.create_if_not_exists(year)
        database.signal_weights_table.upsert(year, year_df)

if __name__ == '__main__':
    from utils.tables import DatabaseName
    db = Database(DatabaseName.DEVELOPMENT)
    start = dt.date(2000, 1, 1)
    end = dt.date(2024, 12, 31)
    signal_weights_flow(start, end, db)