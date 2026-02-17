from datetime import date, timedelta
from tqdm import tqdm
import polars as pl

from pipelines.utils.tables import Database
from pipelines.utils.enums import DatabaseName
# from pipelines.production.steps.scoring import compute_scores_long


def compute_scores_long(
    signals_long: pl.DataFrame,
    *,
    value_col: str = "signal_value",
    score_col: str = "score",
    mask_col: str = "in_universe",
) -> pl.DataFrame:
    """
    signals_long schema expected:
      date, barrid, ticker, signal_name, signal_value, specific_risk, in_universe

    Computes cross-sectional zscore per (date, signal_name).
    Mean/std are computed only on rows where mask_col==True (if present).
    """
    df = signals_long

    # compute mean/std on masked subset but keep all rows
    x = pl.col(value_col)

    if mask_col in df.columns:
        x_masked = pl.when(pl.col(mask_col)).then(x).otherwise(None)
    else:
        x_masked = x

    mu = x_masked.mean().over(["date", "signal_name"])
    sd = x_masked.std().over(["date", "signal_name"])

    # avoid divide-by-zero / null std
    score_expr = (
        pl.when(sd.is_null() | (sd == 0))
          .then(None)
          .otherwise((x - mu) / sd)
          .alias(score_col)
    )

    out = (
        df
        .with_columns(score_expr)
        .select([
            "date", "barrid", "ticker", "signal_name",
            value_col, score_col,
            "specific_risk",
            *(["in_universe"] if "in_universe" in df.columns else []),
        ])
        .with_columns([
            pl.col("date").cast(pl.Date),
            pl.col("barrid").cast(pl.String),
            pl.col("ticker").cast(pl.String),
            pl.col("signal_name").cast(pl.String),
            pl.col(value_col).cast(pl.Float64),
            pl.col(score_col).cast(pl.Float64),
            pl.col("specific_risk").cast(pl.Float64),
            *( [pl.col("in_universe").cast(pl.Boolean)] if "in_universe" in df.columns else [] ),
        ])
    )

    # Scores.validate(out)
    return out



def scores_backfill_flow(
    *,
    database: Database,
    start_date: date,
    end_date: date,
) -> None:
    signals_table = database.signals_table
    scores_table = database.scores_table

    years = list(range(start_date.year, end_date.year + 1))
    for year in tqdm(years, desc="Scores Backfill"):
        year_start = max(start_date, date(year, 1, 1))
        year_end = min(end_date, date(year, 12, 31))

        # Ensure output partition exists
        scores_table.create_if_not_exists(year)

        sig = pl.read_parquet(f"/Users/nathanpreslar/groups/grp_quant/database/production/signals/signals_{year}.parquet")
        # sig = signals_table.read(year, start_date=year_start, end_date=year_end)

        if sig is None or sig.height == 0:
            continue

        scored = compute_scores_long(sig)

        scores_table.upsert(year, scored)


def scores_daily_flow(
    *,
    database: Database,
    dates: list[date],
) -> None:
    """
    Daily update: just a small-window backfill.
    You likely already have a helper that returns last N market dates like in barra flow.
    """
    if not dates:
        dates = [(date.today() - timedelta(days=i)) for i in range(3)]

    scores_backfill_flow(
        database=database,
        start_date=min(dates),
        end_date=max(dates),
    )


if __name__ == "__main__":
    database_name = DatabaseName("production")
    database_instance = Database(database_name)

    scores_backfill_flow(
        database=database_instance,
        start_date=date(2023, 1, 1),
        end_date=date(2025, 12, 31),
    )