from __future__ import annotations
import polars as pl
from pipelines.config.alpha_config import AlphaConfig


def compute_signal_alphas_long(
    scores_long: pl.DataFrame,
    *,
    alpha_config: AlphaConfig,
    score_col: str = "score",
    alpha_col: str = "alpha",
) -> pl.DataFrame:
    """
    Input (scores_long) expected columns:
      date, barrid, ticker, signal_name, score, specific_risk, in_universe

    Output:
      date, barrid, ticker, signal_name, score, ic, specific_risk, alpha, in_universe

    Alpha definition:
      alpha = score * ic(signal_name) * specific_risk
    """
    df = scores_long

    # Build an expression mapping signal_name -> ic
    ic_expr = (
        pl.col("signal_name")
        .replace(alpha_config.ic_by_signal, default=alpha_config.default_ic)
        .cast(pl.Float64)
        .alias("ic")
    )

    out = (
        df
        .with_columns(ic_expr)
        .with_columns(
            (pl.col(score_col) * pl.col("ic") * pl.col("specific_risk"))
            .fill_null(0.0)
            .alias(alpha_col)
        )
        .select([
            "date", "barrid", "ticker",
            "signal_name",
            score_col,
            "ic",
            "specific_risk",
            alpha_col,
            *(["in_universe"] if "in_universe" in df.columns else []),
        ])
        .with_columns([
            pl.col("date").cast(pl.Date),
            pl.col("barrid").cast(pl.String),
            pl.col("ticker").cast(pl.String),
            pl.col("signal_name").cast(pl.String),
            pl.col(score_col).cast(pl.Float64),
            pl.col("ic").cast(pl.Float64),
            pl.col("specific_risk").cast(pl.Float64),
            pl.col(alpha_col).cast(pl.Float64),
            *( [pl.col("in_universe").cast(pl.Boolean)] if "in_universe" in df.columns else [] ),
        ])
    )

    return out
