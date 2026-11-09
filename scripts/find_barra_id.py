"""Command-line ticker to Barra ID lookup using saved database snapshots."""

from __future__ import annotations

import argparse

from pipelines.barra_lookup import find_barra_id
from pipelines.utils.enums import DatabaseName


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find all saved Barra IDs for a ticker in an asset snapshot."
    )
    parser.add_argument("ticker", help="Ticker symbol, matched without case or surrounding whitespace")
    parser.add_argument(
        "--database",
        choices=[name.value for name in (DatabaseName.DEVELOPMENT, DatabaseName.RESEARCH, DatabaseName.PRODUCTION)],
        default=DatabaseName.DEVELOPMENT.value,
        help="Saved database environment to read (default: development)",
    )
    parser.add_argument(
        "--as-of",
        help="Use the latest saved mapping snapshot on or before YYYY-MM-DD",
    )
    args = parser.parse_args()

    result = find_barra_id(args.ticker, database=args.database, as_of=args.as_of)
    if result.mapping_date is None:
        print(f"No saved asset snapshot is available for {args.database} at or before {args.as_of or 'the latest date'}.")
        return

    print(f"Ticker: {result.ticker}")
    print(f"Database: {args.database}")
    print(f"Mapping snapshot date: {result.mapping_date}")
    if not result.found:
        print("No match found in the selected snapshot.")
        return

    print(result.matches)


if __name__ == "__main__":
    main()
