"""Look up ticker to Barra ID mappings in saved asset snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database


@dataclass(frozen=True)
class BarraIdLookupResult:
    """Matches for one ticker in the selected saved asset snapshot."""

    ticker: str
    mapping_date: date | None
    matches: pl.DataFrame

    @property
    def found(self) -> bool:
        return not self.matches.is_empty()


def _parse_as_of(as_of: date | str | None) -> date | None:
    if as_of is None:
        return None
    if isinstance(as_of, datetime):
        return as_of.date()
    if isinstance(as_of, date):
        return as_of
    try:
        return date.fromisoformat(as_of.strip())
    except (AttributeError, ValueError) as exc:
        raise ValueError("as_of must be a date or ISO date string (YYYY-MM-DD)") from exc


def find_barra_id(
    ticker: str,
    database: DatabaseName | str = DatabaseName.DEVELOPMENT,
    as_of: date | str | None = None,
) -> BarraIdLookupResult:
    """Find all saved Barra IDs associated with ``ticker``.

    The lookup uses the repository's saved ``assets`` table, which contains
    ticker and Barrid values per mapping date. By default it selects the
    latest available snapshot. With ``as_of``, it selects the latest saved
    snapshot on or before that date. No network or vendor service is used.

    ``matches`` is a Polars DataFrame with ``ticker``, ``barrid``, ``name``,
    and ``mapping_date`` columns. An empty frame means the ticker had no match
    in the selected snapshot; ``mapping_date`` on the result still identifies
    the snapshot used.
    """
    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string")
    normalized_ticker = ticker.strip().upper()
    as_of_date = _parse_as_of(as_of)
    database_name = database if isinstance(database, DatabaseName) else DatabaseName(database.lower())

    assets = Database(database_name).assets_table.read()
    schema = assets.collect_schema()
    required = {"date", "ticker", "barrid"}
    missing = required - set(schema.names())
    if missing:
        raise ValueError(f"Saved assets table is missing required columns: {sorted(missing)}")

    snapshot_dates = assets.select(pl.col("date").max().alias("mapping_date"))
    if as_of_date is not None:
        snapshot_dates = assets.filter(pl.col("date") <= as_of_date).select(
            pl.col("date").max().alias("mapping_date")
        )
    mapping_date = snapshot_dates.collect().item()

    result_schema = {
        "ticker": pl.String,
        "barrid": pl.String,
        "name": pl.String,
        "mapping_date": pl.Date,
    }
    if mapping_date is None:
        return BarraIdLookupResult(
            ticker=normalized_ticker,
            mapping_date=None,
            matches=pl.DataFrame(schema=result_schema),
        )

    name_expr = pl.col("name").cast(pl.String) if "name" in schema.names() else pl.lit(None, dtype=pl.String)
    matches = (
        assets.filter(
            (pl.col("date") == mapping_date)
            & (pl.col("ticker").cast(pl.String).str.strip_chars().str.to_uppercase() == normalized_ticker)
            & pl.col("barrid").is_not_null()
        )
        .select(
            pl.col("ticker").cast(pl.String).str.strip_chars().str.to_uppercase().alias("ticker"),
            pl.col("barrid").cast(pl.String),
            name_expr.alias("name"),
            pl.col("date").alias("mapping_date"),
        )
        .unique()
        .sort(["barrid", "name"])
        .collect()
    )
    return BarraIdLookupResult(
        ticker=normalized_ticker,
        mapping_date=mapping_date,
        matches=matches,
    )
