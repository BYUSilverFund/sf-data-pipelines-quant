from datetime import date

import polars as pl

from pipelines.utils.tables import Database, Table


def load_signal_assets_df(
    database: Database, start_date: date | None = None, end_date: date | None = None
) -> pl.DataFrame:
    """
    Shared asset-panel loader for the split signal flows.

    Why this exists:
    - Barra-only signals and Ten-K-only signals both need the same base asset
      panel from `assets_table`.
    - Before the split, that logic lived in one signals flow. After the split,
      duplicating the same load/filter/normalize block in multiple files would
      make the code harder to maintain and easier to accidentally drift apart.

    What this does:
    - reads the common asset history
    - keeps only in-universe names
    - optionally narrows to a requested date range
    - converts percent-style return/risk fields to decimals
    - returns a sorted DataFrame that downstream signal code can work with
    """
    assets_lazy = database.assets_table.read()

    filter_exprs = [pl.col("in_universe") == True]
    if start_date is not None and end_date is not None:
        filter_exprs.append(pl.col("date").is_between(start_date, end_date))

    needed_cols = (
        assets_lazy
        .select([
            "date",
            "barrid",
            "cusip",
            "ticker",
            "price",
            "return",
            "specific_return",
            "specific_risk",
            "predicted_beta",
            "daily_volume",
            "in_universe",
        ])
        .filter(*filter_exprs)
        .with_columns(
            pl.col("return").truediv(100),
            pl.col("specific_return").truediv(100),
            pl.col("specific_risk").truediv(100),
        )
    )

    return needed_cols.collect().sort(["barrid", "date"])


def _load_existing_table_df(table: Table) -> pl.DataFrame:
    """
    Read the current single-parquet signal output if it exists.

    Signals/scores/alphas are stored as one file each rather than yearly files.
    When we run only one subset of signals (for example Barra-only or Ten-K-only),
    we need to preserve the rows for the other subsets that already exist in the
    file. This helper gives us the current contents if present, otherwise an
    empty DataFrame with the right schema.
    """
    try:
        return table.read_id_file().collect()
    except Exception:
        return pl.DataFrame(schema=table._schema)


def _merge_signal_subset(
    table: Table, new_df: pl.DataFrame, signal_names: list[str]
) -> pl.DataFrame:
    """
    Replace only a named subset of signals inside an existing output table.

    Why this exists:
    - After splitting the signal pipeline, `barra-signals` should be able to run
      without deleting previously written Ten-K rows.
    - Likewise, `ten-k-signals` should be able to update just the filing-based
      signal without wiping out the Barra signals.

    Merge strategy:
    1. Load the existing table.
    2. Drop rows whose `signal_name` belongs to the subset we are updating.
    3. Append the newly computed rows for that subset.

    This gives us "replace just these signals, preserve everything else"
    semantics while still writing back one parquet file per output table.
    """
    existing_df = _load_existing_table_df(table)

    if existing_df.is_empty():
        return new_df

    # Keep the rows for all signal families we are not updating in this run.
    keep_existing_df = existing_df.filter(~pl.col("signal_name").is_in(signal_names))

    if new_df.is_empty():
        return keep_existing_df

    # Append the refreshed rows for the target signal subset.
    return pl.concat([keep_existing_df, new_df], how="vertical_relaxed")


def write_signal_subset_outputs(
    database: Database,
    signal_names: list[str],
    signals_df: pl.DataFrame,
    scores_df: pl.DataFrame,
    alphas_df: pl.DataFrame,
) -> None:
    """
    Write one signal family back into the shared signals/scores/alphas tables.

    Even though the computation has been split into separate flows, the repo
    still keeps one shared output file for:
    - `signals`
    - `scores`
    - `alphas`

    This helper is the glue that makes that architecture work. A partial flow
    can call this with only its own `signal_names`, and the helper will:
    - preserve unrelated signal rows already on disk
    - replace only the targeted subset
    - overwrite the shared parquet files with the merged result
    """
    merged_signals_df = _merge_signal_subset(database.signals_table, signals_df, signal_names)
    merged_scores_df = _merge_signal_subset(database.scores_table, scores_df, signal_names)
    merged_alphas_df = _merge_signal_subset(database.alpha_table, alphas_df, signal_names)

    database.signals_table.overwrite(merged_signals_df)
    database.scores_table.overwrite(merged_scores_df)
    database.alpha_table.overwrite(merged_alphas_df)
