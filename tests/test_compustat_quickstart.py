from datetime import date

import polars as pl
import pytest

from examples.compustat_quickstart import (
    annual_revenue_growth,
    join_asset_snapshot,
    latest_annual_profitability,
)


def annual_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "barrid": ["B1", "B1", "B1"],
            "gvkey": ["G1", "G1", "G1"],
            "datadate": [date(2022, 12, 31), date(2023, 12, 31), date(2025, 12, 31)],
            "fyear": [2022, 2023, 2025],
            "fyr": [12, 12, 12],
            "currency": ["USD", "USD", "USD"],
            "sale": [100.0, 125.0, 150.0],
            "at": [500.0, 600.0, 750.0],
            "oiadp": [50.0, 60.0, 75.0],
            "rdq": [None, None, None],
            "indfmt": ["INDL"] * 3,
            "consol": ["C"] * 3,
            "popsrc": ["D"] * 3,
            "datafmt": ["STD"] * 3,
            "source_snapshot_id": ["s1", "s2", "s3"],
            "source_report_path": ["r1", "r2", "r3"],
            "source_income_statement_path": ["i1", "i2", "i3"],
            "source_balance_sheet_path": ["b1", "b2", "b3"],
        }
    )


def test_revenue_growth_requires_consecutive_fiscal_years_and_keeps_provenance():
    result = annual_revenue_growth(annual_rows())

    assert result.select("fyear", "prior_fyear", "revenue_growth").rows() == [
        (2023, 2022, 0.25),
    ]
    assert result["prior_source_snapshot_id"].to_list() == ["s1"]
    assert result["source_snapshot_id"].to_list() == ["s2"]
    assert result["prior_source_income_statement_path"].to_list() == ["i1"]


def test_profitability_and_asset_join_are_one_to_one_by_barrid():
    annual = annual_rows()
    profitability = latest_annual_profitability(annual, date(2025, 12, 31))
    assets = pl.DataFrame(
        {
            "date": [date(2026, 1, 2)],
            "barrid": ["B1"],
            "ticker": ["ONE"],
            "market_cap": [1000.0],
        }
    )

    joined = join_asset_snapshot(assets, profitability, date(2026, 1, 2))
    assert joined.height == 1
    assert joined["operating_roa"][0] == 0.1
    assert joined["source_snapshot_id"][0] == "s3"


def test_mixed_formats_and_conflicting_fiscal_identities_fail_closed():
    mixed = pl.concat(
        [annual_rows(), annual_rows().with_columns(pl.lit("SUMM_STD").alias("datafmt"))]
    )
    with pytest.raises(ValueError, match="only INDL/C/D/STD"):
        annual_revenue_growth(mixed)

    duplicate = pl.concat([annual_rows(), annual_rows().tail(1)])
    with pytest.raises(ValueError, match="conflicting rows"):
        annual_revenue_growth(duplicate)


def test_asset_duplicates_are_rejected_instead_of_multiplying_results():
    profitability = latest_annual_profitability(annual_rows(), date(2025, 12, 31))
    assets = pl.DataFrame(
        {
            "date": [date(2026, 1, 2), date(2026, 1, 2)],
            "barrid": ["B1", "B1"],
        }
    )
    with pytest.raises(ValueError, match="Asset snapshot contains conflicting rows"):
        join_asset_snapshot(assets, profitability, date(2026, 1, 2))
