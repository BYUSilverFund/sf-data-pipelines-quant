from datetime import date
import polars as pl
from pipelines.utils.tables import Database
from pipelines.signals import SIGNALS, add_dollar_volume_stats

MIN_PRICE = 5.0


def signals_flow(database: Database) -> None:
    """
    Compute signals, scores, and alphas from assets table.

    Since signals have lookback windows (up to 252 days), the entire history
    must be recomputed on every run.

    Strategy:
    1. Lazy scan assets_table (all years needed for rolling window lookback)
    2. Select needed columns for every barrid that was ever in the universe
    3. Collect eagerly (rolling windows require full sorted history)
    4. For each signal: compute signal_value on the full history, restrict to
       in_universe names with a prior close above MIN_PRICE, then compute score
       (z-score) and alpha
    5. Write parquet files partitioned by year for each table
    """

    # Lazy scan assets_table
    assets_lazy = database.assets_table.read()

    # Filter and select needed columns
    needed_cols = (
        assets_lazy
        .select([
            "date",
            "barrid",
            "ticker",
            "price",
            "return",
            "specific_return",
            "specific_risk",
            "predicted_beta",
            "daily_volume",
            "in_universe"
        ])
        # Keep each constituent's full history so rolling windows have no holes
        .filter(
            pl.col("in_universe").any().over("barrid")
        )
        .with_columns(
            pl.col("return").truediv(100),
            pl.col("specific_return").truediv(100),
            pl.col("specific_risk").truediv(100),
        )
    )

    # Collect eagerly (rolling windows need full history in memory)
    assets_df = add_dollar_volume_stats(needed_cols.collect().sort(["barrid", "date"]))

    # Lists to accumulate results for each table
    signals_rows = []
    scores_rows = []
    alphas_rows = []

    # Compute each signal
    for signal_name, signal_config in SIGNALS.items():
        signal_df = (
            assets_df
            .with_columns(signal_config["expr"])
            .with_columns(pl.col(signal_name).alias("signal_value"))
        ).filter(
                # Tradeable cross-section as of the prior close, before scoring
                pl.col("in_universe"),
                pl.col("price").shift(1).over("barrid").gt(MIN_PRICE),
                pl.col(signal_name).is_not_null(),
                pl.col("predicted_beta").is_not_null(),
                pl.col("specific_risk").is_not_null(),
            )
        signals_rows.append(signal_df.select([
            "date",
            "barrid",
            pl.lit(signal_name).alias("signal_name"),
            "signal_value"
        ]))
        
        # Compute score using signal-specific scorer
        scorer = signal_config["scorer"]
        score_df = scorer(signal_df)
        scores_rows.append(score_df.select([
            "date",
            "barrid",
            pl.lit(signal_name).alias("signal_name"),
            "score"
        ]))

        # Compute alpha using signal-specific alphatizer
        alphatizer = signal_config["alphatizer"]
        alpha_df = alphatizer(score_df)
        alphas_rows.append(alpha_df.select([
            "date",
            "barrid",
            pl.lit(signal_name).alias("signal_name"),
            "alpha"
        ]))

    # Concatenate all rows for each table
    signals_df = pl.concat(signals_rows)
    scores_df = pl.concat(scores_rows)
    alphas_df = pl.concat(alphas_rows)

    # Write by year
    years = signals_df.select(pl.col("date").dt.year().unique().sort().alias("year"))["year"]

    for year in years:
        year_filter = pl.col("date").dt.year().eq(year)

        # The full history is recomputed, so replace each year. An upsert would
        # keep rows for names that have since left the cross-section.
        database.signals_table.delete(year)
        database.signals_table.create_if_not_exists(year)
        database.signals_table.upsert(year, signals_df.filter(year_filter))

        database.scores_table.delete(year)
        database.scores_table.create_if_not_exists(year)
        database.scores_table.upsert(year, scores_df.filter(year_filter))

        database.alpha_table.delete(year)
        database.alpha_table.create_if_not_exists(year)
        database.alpha_table.upsert(year, alphas_df.filter(year_filter))


if __name__ == "__main__":
    from pipelines.utils.enums import DatabaseName
    db = Database(DatabaseName.DEVELOPMENT)
    signals_flow(db)
