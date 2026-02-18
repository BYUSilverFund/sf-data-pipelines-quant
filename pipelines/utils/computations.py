import polars as pl


def compute_signals(assets: pl.DataFrame, signals) -> pl.DataFrame:
    """
    assets must include:
      date, barrid, ticker, specific_risk, in_universe
    signals are BaseSignal instances with .name and .expr producing a column named signal.name (or aliased).
    """
    # Compute wide signal columns
    wide = (
        assets.sort(["ticker", "date"])
        .with_columns([s.expr for s in signals])
    )

    id_cols = ["date", "barrid", "ticker", "specific_risk", "in_universe"]
    signal_cols = [s.name for s in signals]

    # Convert wide -> long to match signals_table schema
    long = (
        wide
        .select(id_cols + signal_cols)
        .melt(
            id_vars=id_cols,
            value_vars=signal_cols,
            variable_name="signal_name",
            value_name="signal_value",
        )
        .drop_nulls(["signal_value"])
    )

    return long.with_columns([
        pl.col("date").cast(pl.Date),
        pl.col("barrid").cast(pl.String),
        pl.col("ticker").cast(pl.String),
        pl.col("signal_name").cast(pl.String),
        pl.col("signal_value").cast(pl.Float64),
        pl.col("specific_risk").cast(pl.Float64),
        pl.col("in_universe").cast(pl.Boolean),
    ])



def compute_scores(
    signals: pl.DataFrame,
    *,
    mask_col: str = "is_tradable"
) -> pl.DataFrame:
    """
    signals_long schema expected:
      date, barrid, ticker, signal_name, signal_value, specific_risk, in_universe

    Computes cross-sectional zscore per (date, signal_name).
    Mean/std are computed only on rows where mask_col==True (if present).
    """

    score_cols = [
        "date", "barrid", "ticker", "signal_name", "signal_value", "score", "specific_risk", "is_tradable"
    ]

    x = pl.col("signal_value")
    x_masked = pl.when(pl.col(mask_col)).then(x).otherwise(None)
    mu = x_masked.mean().over(["date", "signal_name"])
    sd = x_masked.std().over(["date", "signal_name"])

    score_expr = (
        pl.when(sd.is_null() | (sd == 0))
          .then(None)
          .otherwise((x - mu) / sd)
          .alias("score")
    )

    final_df = (
        signals
        .with_columns(score_expr)
        .select(score_cols)
        .with_columns([
            pl.col("date").cast(pl.Date),
            pl.col("barrid").cast(pl.String),
            pl.col("ticker").cast(pl.String),
            pl.col("signal_name").cast(pl.String),
            pl.col("signal_value").cast(pl.Float64),
            pl.col("score").cast(pl.Float64),
            pl.col("specific_risk").cast(pl.Float64),
            pl.col("is_tradable").cast(pl.Boolean)
        ])
    )

    return final_df


def compute_signal_alphas(
    scores: pl.DataFrame,
    *,
    ic: float = 0.5,
    score_col: str = "score",
) -> pl.DataFrame:
    """
    Input (scores_long) expected columns:
      date, barrid, ticker, signal_name, score, specific_risk
      (optionally in_universe / is_tradable)

    Output (long):
      date, barrid, ticker, signal_name, signal_alpha, specific_risk, (optional masks)
    """
    return (
        scores
        .with_columns(
            (pl.col(score_col) * pl.lit(ic) * pl.col("specific_risk"))
            .fill_null(0.0)  
            .alias("signal_alpha")
        )
        .select([
            "date", "barrid", "ticker", "signal_name",
            "signal_alpha", "specific_risk",
            *(["in_universe"] if "in_universe" in scores.columns else []),
            *(["is_tradable"] if "is_tradable" in scores.columns else []),
        ])
        .with_columns([
            pl.col("date").cast(pl.Date),
            pl.col("barrid").cast(pl.String),
            pl.col("ticker").cast(pl.String),
            pl.col("signal_name").cast(pl.String),
            pl.col("signal_alpha").cast(pl.Float64),
            pl.col("specific_risk").cast(pl.Float64),
            *( [pl.col("in_universe").cast(pl.Boolean)] if "in_universe" in scores_long.columns else [] ),
            *( [pl.col("is_tradable").cast(pl.Boolean)] if "is_tradable" in scores_long.columns else [] ),
        ])
    )


def compute_combined_alpha(
    signal_alphas_long: pl.DataFrame,
    *,
    signal_combinator,
    output_col: str = "alpha",
) -> pl.DataFrame:
    """
    Mirrors trader:
      - fill null alphas with 0
      - combine using signal_combinator.combine_fn(signal_cols)

    Input (long):
      date, barrid, ticker, signal_name, signal_alpha

    Output:
      date, barrid, ticker, alpha
    """
    wide = (
        signal_alphas_long
        .pivot(
            index=["date", "barrid", "ticker"],
            columns="signal_name",
            values="signal_alpha",
            aggregate_function="first",
        )
        .fill_null(0.0)
    )

    id_cols = {"date", "barrid", "ticker"}
    signal_cols = [c for c in wide.columns if c not in id_cols]

    return (
        wide
        .with_columns(signal_combinator.combine_fn(signal_cols).alias(output_col))
        .select(["date", "barrid", "ticker", output_col])
        .with_columns([
            pl.col("date").cast(pl.Date),
            pl.col("barrid").cast(pl.String),
            pl.col("ticker").cast(pl.String),
            pl.col(output_col).cast(pl.Float64),
        ])
    )