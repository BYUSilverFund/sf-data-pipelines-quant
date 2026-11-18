"""Report dates present in assets but missing from downstream tables.

Usage:
    python -m scripts.check_date_coverage --database production --start-year 2025 --end-year 2025

Exits 1 if any table is missing dates (or a year file), 0 otherwise.
"""

from __future__ import annotations

import argparse
import sys

import polars as pl

from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database

DOWNSTREAM_TABLES = [
    "exposures_table",
    "covariances_table",
    "barra_risk_table",
    "signals_table",
    "scores_table",
    "alpha_table",
]


def _dates(table, year: int) -> set:
    if not table.exists(year):
        return set()
    return set(table.read(year).select(pl.col("date").unique()).collect()["date"])


def missing_dates(database: Database, year: int) -> dict[str, list]:
    """Map each downstream table name to the sorted assets dates it lacks."""
    asset_dates = _dates(database.assets_table, year)
    return {
        name.removesuffix("_table"): sorted(
            asset_dates - _dates(getattr(database, name), year)
        )
        for name in DOWNSTREAM_TABLES
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--database", required=True, choices=[d.value for d in DatabaseName]
    )
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    args = parser.parse_args(argv)

    database = Database(DatabaseName(args.database))
    found_gap = False

    for year in range(args.start_year, args.end_year + 1):
        n_assets = len(_dates(database.assets_table, year))
        print(f"{year}: {n_assets} asset dates")
        for table, dates in missing_dates(database, year).items():
            if dates:
                found_gap = True
                print(f"  {table}: missing {len(dates)} dates ({dates[0]} to {dates[-1]})")

    print("Gaps found." if found_gap else "No gaps.")
    return 1 if found_gap else 0


if __name__ == "__main__":
    sys.exit(main())
