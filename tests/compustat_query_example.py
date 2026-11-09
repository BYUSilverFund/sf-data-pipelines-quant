from pathlib import Path
import sys

# The VS Code Play button runs this file by path, so add the repository root
# for imports from ``pipelines``.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database


def annual_reports(barrid: str) -> pl.DataFrame:
    """Return all annual report rows and columns for a Barrid."""
    table = Database(DatabaseName.DEVELOPMENT).fundamentals_annual_table.read()
    rows = table.filter(pl.col("barrid") == barrid)
    rows = rows.sort("datadate", "fyear", "fyr")
    return rows.collect()


def quarterly_reports(barrid: str) -> pl.DataFrame:
    """Return all quarterly report rows and columns for a Barrid."""
    table = Database(DatabaseName.DEVELOPMENT).fundamentals_quarterly_table.read()
    rows = table.filter(pl.col("barrid") == barrid)
    rows = rows.sort("datadate", "fyearq", "fqtr", "fyr")
    return rows.collect()


def top_ten_gross_margin_2026(database) -> pl.DataFrame:
    """Return top gross margins with the latest available Barrid ticker."""
    result = (
        database.fundamentals_annual_table.read()
        .filter(
            (pl.col("datadate").dt.year() == 2026)
            & (pl.col("sale") > 0)
        )
        .with_columns(
            ((pl.col("sale") - pl.col("cogs")) / pl.col("sale"))
            .alias("gross_margin")
        )
        .filter(pl.col("gross_margin").is_finite())
        .sort("gross_margin", descending=True)
        .unique(subset="gvkey", keep="first", maintain_order=True)
        .select("barrid", "datadate", "gross_margin")
        .head(10)
        .collect()
    )
    tickers = (
        database.assets_table.read()
        .filter(pl.col("barrid").is_in(result.get_column("barrid").to_list()))
        .filter(pl.col("ticker").is_not_null())
        .sort("date", descending=True)
        .unique(subset="barrid", keep="first", maintain_order=True)
        .select(
            "barrid",
            "ticker",
            pl.col("date").alias("ticker_date"),
        )
        .collect()
    )
    return result.join(tickers, on="barrid", how="left")


if __name__ == "__main__":
    database = Database(DatabaseName.DEVELOPMENT)
    barrid = "USAA191"
    print("Annual reports:")
    print(annual_reports(barrid))
    print("Quarterly reports:")
    print(quarterly_reports(barrid))
    print("Top 10 gross margins for 2026:")
    print(top_ten_gross_margin_2026(database))
