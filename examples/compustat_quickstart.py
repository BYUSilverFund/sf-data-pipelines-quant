"""Read the saved development Compustat tables for current-data research."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

import polars as pl

# Allow ``python examples/compustat_quickstart.py`` from the repository root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database


EXPECTED_FORMAT = ("INDL", "C", "D", "STD")
FORMAT_COLUMNS = ["indfmt", "consol", "popsrc", "datafmt"]
ANNUAL_PERIOD_KEY = ["barrid", "gvkey", "fyear", "fyr", *FORMAT_COLUMNS]
QUARTERLY_PERIOD_KEY = ["barrid", "gvkey", "fyearq", "fqtr", "fyr", *FORMAT_COLUMNS]


def _validate_format(frame: pl.DataFrame, label: str) -> None:
    formats = frame.select(FORMAT_COLUMNS).unique().rows()
    if formats != [EXPECTED_FORMAT]:
        raise ValueError(
            f"{label} must contain only INDL/C/D/STD rows; found {formats!r}"
        )


def _validate_unique(frame: pl.DataFrame, keys: list[str], label: str) -> None:
    duplicates = frame.group_by(keys).len().filter(pl.col("len") > 1).head(5)
    if duplicates.height:
        raise ValueError(
            f"{label} contains conflicting rows for a fiscal identity:\n{duplicates}"
        )


def annual_revenue_growth(annual: pl.DataFrame) -> pl.DataFrame:
    """Calculate sales growth only where the previous fiscal year is present."""
    _validate_format(annual, "Annual table")
    _validate_unique(annual, ANNUAL_PERIOD_KEY, "Annual table")
    sort_keys = ["barrid", "gvkey", *FORMAT_COLUMNS, "fyr", "fyear", "datadate"]
    group_keys = ["barrid", "gvkey", *FORMAT_COLUMNS, "fyr"]
    rows = annual.sort(sort_keys).with_columns(
        pl.col("fyear").shift(1).over(group_keys).alias("prior_fyear"),
        pl.col("sale").shift(1).over(group_keys).alias("prior_sale"),
        pl.col("source_snapshot_id")
        .shift(1)
        .over(group_keys)
        .alias("prior_source_snapshot_id"),
        pl.col("source_income_statement_path")
        .shift(1)
        .over(group_keys)
        .alias("prior_source_income_statement_path"),
    )
    return (
        rows.filter(pl.col("fyear") == pl.col("prior_fyear") + 1)
        .with_columns(
            pl.when(pl.col("prior_sale") > 0)
            .then(pl.col("sale") / pl.col("prior_sale") - 1)
            .otherwise(None)
            .alias("revenue_growth")
        )
    )


def latest_annual_profitability(annual: pl.DataFrame, asset_date: date) -> pl.DataFrame:
    """Return latest saved annual operating income / assets per Barrid."""
    _validate_format(annual, "Annual table")
    _validate_unique(annual, ANNUAL_PERIOD_KEY, "Annual table")
    _validate_unique(annual, ["barrid", "gvkey", "fyear"], "Annual fiscal year")
    multi_company = (
        annual.group_by("barrid")
        .agg(pl.col("gvkey").n_unique().alias("gvkey_count"))
        .filter(pl.col("gvkey_count") > 1)
    )
    if multi_company.height:
        raise ValueError(f"Barrids map to multiple GVKEYs:\n{multi_company.head(5)}")

    return (
        annual.filter(pl.col("datadate") <= asset_date)
        .sort(["barrid", "fyear", "datadate"])
        .group_by("barrid", maintain_order=True)
        .last()
        .filter(
            pl.col("at").is_not_null()
            & (pl.col("at") > 0)
            & pl.col("oiadp").is_not_null()
        )
        .with_columns((pl.col("oiadp") / pl.col("at")).alias("operating_roa"))
    )


def join_asset_snapshot(
    assets: pl.DataFrame, profitability: pl.DataFrame, asset_date: date
) -> pl.DataFrame:
    """Join one asset snapshot row to one annual result per Barrid."""
    snapshot = assets.filter(pl.col("date") == asset_date)
    if snapshot.is_empty():
        raise ValueError(f"No saved asset rows for {asset_date}")
    _validate_unique(snapshot, ["barrid"], "Asset snapshot")
    _validate_unique(profitability, ["barrid"], "Profitability result")
    return snapshot.join(
        profitability,
        on="barrid",
        how="inner",
        validate="1:1",
        suffix="_fundamentals",
    )


def _asset_snapshot_date(database: Database, requested_date: date) -> date:
    table = database.assets_table
    for year in range(requested_date.year, 1994, -1):
        if not table.exists(year):
            continue
        selected = (
            table.read(year)
            .filter(pl.col("date") <= requested_date)
            .select(pl.col("date").max())
            .collect()
            .item()
        )
        if selected is not None:
            return selected
    raise ValueError(f"No saved asset date on or before {requested_date}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--asset-date",
        type=date.fromisoformat,
        default=date.today(),
        help="Use the latest saved assets snapshot on or before this date (default: today).",
    )
    args = parser.parse_args()

    database = Database(DatabaseName.DEVELOPMENT)
    asset_date = _asset_snapshot_date(database, args.asset_date)
    annual = database.fundamentals_annual_table.read().collect()
    quarterly = database.fundamentals_quarterly_table.read().collect()
    _validate_format(annual, "Annual table")
    _validate_format(quarterly, "Quarterly table")
    _validate_unique(quarterly, QUARTERLY_PERIOD_KEY, "Quarterly table")

    print(f"Development Database; selected assets snapshot: {asset_date}")
    print(
        "Current Compustat data is not point-in-time. A report datadate on or "
        "before an asset date does not establish that the report was available then."
    )
    print(
        "Rows are Barrid-level; a GVKEY with multiple Barrids appears once for "
        "each mapped security/share class."
    )
    multi_barrid_gvkeys = (
        annual.group_by("gvkey")
        .agg(pl.col("barrid").n_unique().alias("barrid_count"))
        .filter(pl.col("barrid_count") > 1)
        .height
    )
    print(f"GVKEYs mapped to multiple Barrids in this materialization: {multi_barrid_gvkeys}")

    profitability = latest_annual_profitability(annual, asset_date)
    print("\nAnnual operating profitability (oiadp / at; reported-currency units cancel):")
    print(
        profitability.sort("operating_roa", descending=True)
        .select(
            "barrid",
            "gvkey",
            "datadate",
            "fyear",
            "fyr",
            "currency",
            "operating_roa",
            "source_snapshot_id",
            "source_income_statement_path",
            "source_balance_sheet_path",
        )
        .head(5)
    )

    growth = annual_revenue_growth(annual).filter(
        pl.col("revenue_growth").is_finite()
    )
    print("\nRecent annual revenue growth (sale / prior-year sale - 1; consecutive fyear only):")
    print(
        growth.sort(
            ["datadate", "fyear", "revenue_growth"],
            descending=[True, True, True],
        )
        .select(
            "barrid",
            "gvkey",
            "datadate",
            "fyear",
            "prior_fyear",
            "fyr",
            "prior_sale",
            "sale",
            "currency",
            "revenue_growth",
            "prior_source_snapshot_id",
            "source_snapshot_id",
            "prior_source_income_statement_path",
            "source_income_statement_path",
        )
        .head(5)
    )

    print("\nQuarterly operating cash flow: oancfy is fiscal-year-to-date, not standalone quarter.")
    print(
        quarterly.filter(pl.col("oancfy").is_not_null())
        .sort(["datadate", "fyearq", "fqtr"], descending=[True, True, True])
        .select(
            "barrid",
            "gvkey",
            "datadate",
            "fyearq",
            "fqtr",
            "fyr",
            "currency",
            "oancfy",
            "source_snapshot_id",
            "source_cash_flow_path",
        )
        .head(5)
    )

    assets = (
        database.assets_table.read(asset_date.year)
        .filter(pl.col("date") == asset_date)
        .select("date", "barrid", "ticker", "name", "market_cap")
        .collect()
    )
    joined = join_asset_snapshot(assets, profitability, asset_date)
    print(
        f"\nBarrid join: {assets.height:,} asset rows, "
        f"{profitability.height:,} profitability rows, {joined.height:,} matched rows; "
        "one-to-one validation passed."
    )
    print(
        joined.sort("operating_roa", descending=True)
        .select(
            "date",
            "barrid",
            "ticker",
            "market_cap",
            "gvkey",
            "datadate",
            "fyear",
            "currency",
            "operating_roa",
            "source_snapshot_id",
            "source_income_statement_path",
            "source_balance_sheet_path",
        )
        .head(5)
    )


if __name__ == "__main__":
    main()
