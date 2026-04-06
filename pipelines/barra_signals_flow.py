from datetime import date
import polars as pl
from tqdm import tqdm
from pipelines.utils.tables import Database
from pipelines.signals import BARRA_SIGNALS
from pipelines.signal_output_utils import load_signal_assets_df, write_signal_subset_outputs


def barra_signals_flow(start_date: date, end_date: date, database: Database) -> None:
    """
    Compute signals, scores, and alphas from assets table.

    Since signals have lookback windows (up to 252 days), the entire history
    must be recomputed on every run.

    Strategy:
    1. Lazy scan assets_table (all years needed for rolling window lookback)
    2. Filter to in_universe=True and select needed columns
    3. Collect eagerly (rolling windows require full sorted history)
    4. For each signal: compute signal_value, score (z-score), and alpha
    5. Write single parquet file per table (all years)
    """

    assets_df = load_signal_assets_df(database, start_date, end_date)

    # Lists to accumulate results for each table
    signals_rows = []
    scores_rows = []
    alphas_rows = []

    # Compute each signal
    for signal_name, signal_config in tqdm(BARRA_SIGNALS.items(), desc="Signals", total=len(BARRA_SIGNALS)):
        signal_df = (
            assets_df
            .with_columns(signal_config["expr"])
            .with_columns(pl.col(signal_name).alias("signal_value"))
        ).filter(
                pl.col(signal_name).is_not_null(),
                pl.col("predicted_beta").is_not_null(),
                pl.col("specific_risk").is_not_null(),
            )
        signals_rows.append(signal_df.select([
            "date",
            "barrid",
            pl.lit(signal_name).alias("signal_name"),
            "signal_value"
        ]).unique(subset=["date", "barrid", "signal_name"], keep="last"))
        
        # Compute score using signal-specific scorer
        scorer = signal_config["scorer"]
        score_df = scorer(signal_df)
        scores_rows.append(score_df.select([
            "date",
            "barrid",
            pl.lit(signal_name).alias("signal_name"),
            "score"
        ]).unique(subset=["date", "barrid", "signal_name"], keep="last"))

        # Compute alpha using signal-specific alphatizer
        alphatizer = signal_config["alphatizer"]
        alpha_df = alphatizer(score_df)
        alphas_rows.append(alpha_df.select([
            "date",
            "barrid",
            pl.lit(signal_name).alias("signal_name"),
            "alpha"
        ]).unique(subset=["date", "barrid", "signal_name"], keep="last"))

    # Concatenate all rows for each table
    signals_df = pl.concat(signals_rows)
    scores_df = pl.concat(scores_rows)
    alphas_df = pl.concat(alphas_rows)

    write_signal_subset_outputs(
        database,
        signal_names=list(BARRA_SIGNALS.keys()),
        signals_df=signals_df,
        scores_df=scores_df,
        alphas_df=alphas_df,
    )


if __name__ == "__main__":
    from pipelines.utils.enums import DatabaseName
    start = date(1995, 1, 1)
    end = date(2025, 12, 31)
    db = Database(DatabaseName.DEVELOPMENT)
    barra_signals_flow(start, end, db)
