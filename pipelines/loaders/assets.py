from datetime import date, timedelta
from typing import Iterable, Sequence, Optional
from pipelines.signals.base import BaseSignal

import polars as pl
import sf_quant.data as sfd


def load_assets_fn(
    start: date,
    end: date,
    *,
    signals: list[BaseSignal] = None,
    extra_columns: Optional[Iterable[str]] = None,
    in_universe: bool = True,
    apply_lookback_buffer: bool = True,
) -> pl.DataFrame:
    """
    Load raw asset panel needed for signal computation.

    - start/end: the target output range you want persisted
    - signals: optional list of signal objects that have lookback_days and (optionally) required_columns
    - apply_lookback_buffer: loads additional history but you can later filter back to [start, end]
    """

    base_cols = {
        "date",
        "barrid",
        "ticker",
        "specific_risk",
        "in_universe",
        "return",
        "predicted_beta",
        "price",
        "market_cap",
    }

    # Pull required columns from signals 
    required_cols = set(base_cols)
    if signals is not None:
        for s in signals:
            req = getattr(s, "required_columns", None)
            if req:
                required_cols |= set(req)

    if extra_columns:
        required_cols |= set(extra_columns)

    # Determine lookback buffer
    buffered_start = start
    if apply_lookback_buffer and signals is not None and len(signals) > 0:
        lookback_days = max(int(getattr(s, "lookback_days", 0) or 0) for s in signals)
        buffered_start = start - timedelta(days=lookback_days)

    df = (
        sfd.load_assets(
            start=buffered_start,
            end=end,
            columns=sorted(required_cols),
            in_universe=in_universe,
        )
        # your existing convention: returns are % in vendor -> convert to decimal
        .with_columns(
            pl.col("return").truediv(100)
        )
        .sort(["ticker", "date"])
    )

    return df