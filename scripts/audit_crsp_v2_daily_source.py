"""Compare rebuilt CRSP v2 daily files with WRDS for a range of years."""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import polars as pl
import wrds
from dotenv import load_dotenv


VALUE_COLUMNS = ["open", "high", "low", "close", "prc", "ret", "retx", "vol"]
KEY_COLUMNS = ["date", "permno"]


def main(start_year: int = 1925, end_year: int = 2025) -> int:
    load_dotenv(override=True)
    username = os.getenv("WRDS_USER")
    password = os.getenv("WRDS_PASSWORD")
    root = os.getenv("ROOT")
    if not username or not password or not root:
        raise EnvironmentError("WRDS_USER, WRDS_PASSWORD, and ROOT are required.")

    output_dir = (
        Path(root)
        / "groups/grp_quant/database/research/crsp_v2_daily"
    )
    connection = wrds.Connection(wrds_username=username, wrds_password=password)
    failed_years: list[int] = []
    try:
        for year in range(start_year, end_year + 1):
            start = date(year, 1, 1)
            end = date(year, 12, 31)
            source_pandas = connection.raw_sql(
                f"""
                SELECT dlycaldt AS date, permno, dlyopen AS open,
                       dlyhigh AS high, dlylow AS low, dlyclose AS close,
                       dlyprc AS prc, dlyret AS ret, dlyretx AS retx,
                       dlyvol AS vol
                FROM crsp_m_stock.wrds_dsfv2_query
                WHERE dlycaldt BETWEEN '{start}' AND '{end}'
                """
            )
            source = pl.from_pandas(source_pandas).with_columns(
                pl.col("date").cast(pl.Date),
                *[
                    pl.col(column).cast(pl.Float64, strict=False)
                    for column in VALUE_COLUMNS
                    if column != "vol"
                ],
                pl.col("vol")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int64, strict=False),
            )
            path = output_dir / f"crsp_v2_daily_{year}.parquet"
            if not path.exists():
                print(f"{year}: MISSING_OUTPUT {path}", flush=True)
                failed_years.append(year)
                continue

            output = pl.read_parquet(path).select(KEY_COLUMNS + VALUE_COLUMNS)
            source_unique = source.unique()
            source_duplicate_extra_rows = source.height - source_unique.height
            source_conflict_groups = (
                source_unique.group_by(KEY_COLUMNS)
                .len()
                .filter(pl.col("len") > 1)
                .height
            )
            output_duplicate_groups = (
                output.group_by(KEY_COLUMNS)
                .len()
                .filter(pl.col("len") > 1)
                .height
            )
            if source_conflict_groups:
                print(
                    f"{year}: SOURCE_CONFLICTS {source_conflict_groups}",
                    flush=True,
                )
                failed_years.append(year)
                continue

            source_keys = source_unique.select(KEY_COLUMNS)
            output_keys = output.select(KEY_COLUMNS)
            source_only_keys = source_keys.join(
                output_keys, on=KEY_COLUMNS, how="anti"
            ).height
            output_only_keys = output_keys.join(
                source_keys, on=KEY_COLUMNS, how="anti"
            ).height
            joined = output.join(
                source_unique, on=KEY_COLUMNS, how="inner", suffix="_source"
            )
            mismatches = {
                column: joined.filter(
                    ~pl.col(column).eq_missing(pl.col(f"{column}_source"))
                ).height
                for column in VALUE_COLUMNS
            }
            passed = (
                source_only_keys == 0
                and output_only_keys == 0
                and output_duplicate_groups == 0
                and not any(mismatches.values())
            )
            if not passed:
                failed_years.append(year)
            print(
                f"{year}: {'PASS' if passed else 'FAIL'} "
                f"source_rows={source.height} "
                f"source_exact_duplicates={source_duplicate_extra_rows} "
                f"output_rows={output.height} "
                f"source_only_keys={source_only_keys} "
                f"output_only_keys={output_only_keys} "
                f"output_duplicate_groups={output_duplicate_groups} "
                f"value_mismatches={mismatches}",
                flush=True,
            )
    finally:
        connection.close()

    print(
        f"AUDIT_RESULT={'PASS' if not failed_years else 'FAIL'} "
        f"years={start_year}-{end_year} failed_years={failed_years}",
        flush=True,
    )
    return int(bool(failed_years))


if __name__ == "__main__":
    first_year = int(sys.argv[1]) if len(sys.argv) > 1 else 1925
    last_year = int(sys.argv[2]) if len(sys.argv) > 2 else 2025
    raise SystemExit(main(first_year, last_year))
