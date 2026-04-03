from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import os
import time

from edgar import Company, set_identity
import polars as pl
from tqdm import tqdm

from pipelines.utils import get_last_market_date
from pipelines.utils.tables import Database

MAX_WORKERS = 4
SLEEP_BETWEEN = 0.2
TEN_K_RECHECK_TRADING_DAYS = 245


def _normalize_cik(cik: str | None) -> str | None:
    if cik is None:
        return None

    cik = str(cik).strip()
    return cik.zfill(10) if cik else None


def _safe_attr(obj, *names: str):
    for name in names:
        value = getattr(obj, name, None)
        if value is not None:
            return value

    return None


def _safe_item(obj, key: str) -> str | None:
    try:
        value = obj[key]
    except Exception:
        return None

    if value is None:
        return None

    value = str(value).strip()
    return value or None


def _normalize_date(value) -> date | None:
    if value is None:
        return None

    if isinstance(value, date):
        return value

    value = str(value).strip()
    if not value:
        return None

    return date.fromisoformat(value[:10])


def _normalize_datetime(value) -> str | None:
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    value = str(value).strip()
    return value or None


def _latest_filing(company: Company, start_date: date, end_date: date):
    filings = company.get_filings(form="10-K")

    if filings is None:
        return None

    try:
        filings = filings.filter(date=f"{start_date}:{end_date}")
    except Exception:
        pass

    try:
        if len(filings) == 0:
            return None
    except TypeError:
        pass

    latest = None

    try:
        latest = filings[0]
    except Exception:
        try:
            latest = filings.latest()
        except Exception:
            return None

    if latest is None:
        return None

    try:
        if not hasattr(latest, "form") and len(latest) > 0:
            latest = latest[0]
    except Exception:
        pass

    return latest


def load_ftse_cik_df(
    database: Database, start_date: date, end_date: date
) -> pl.DataFrame:
    # The 10-K universe is sourced from FTSE Russell membership rather than the
    # final assets table so we can reuse the same benchmark-driven CUSIP/CIK map
    # across the filing pipeline.
    return (
        database.ftse_russell_table.read()
        .filter(
            pl.col("date").is_between(start_date, end_date),
            pl.col("russell_1000").fill_null(False) | pl.col("russell_2000").fill_null(False),
        )
        .collect()
    )


def load_existing_ten_k_filings_df(database: Database, year: int) -> pl.DataFrame:
    try:
        return database.ten_k_filings_table.read(year).collect()
    except Exception:
        # Backfills are incremental by year, so missing parquet files should
        # behave like an empty historical result set rather than raising.
        return pl.DataFrame(
            schema={
                "year": pl.Int64,
                "cusip": pl.String,
                "cik": pl.String,
                "form": pl.String,
                "filing_date": pl.Date,
                "acceptance_datetime": pl.String,
                "report_date": pl.Date,
                "accession_number": pl.String,
                "filing_url": pl.String,
                "item_1a": pl.String,
            }
        )


def load_all_existing_ten_k_filings_df(database: Database) -> pl.DataFrame:
    try:
        return database.ten_k_filings_table.read().collect()
    except Exception:
        return pl.DataFrame(
            schema={
                "year": pl.Int64,
                "cusip": pl.String,
                "cik": pl.String,
                "form": pl.String,
                "filing_date": pl.Date,
                "acceptance_datetime": pl.String,
                "report_date": pl.Date,
                "accession_number": pl.String,
                "filing_url": pl.String,
                "item_1a": pl.String,
            }
        )


def _trading_day_cutoff(current_date: date, lookback_days: int) -> date:
    previous_market_dates = [
        d for d in get_last_market_date(current_date=current_date, n_days=lookback_days) if d is not None
    ]

    if not previous_market_dates:
        return current_date

    return previous_market_dates[0]


def load_today_ftse_cik_df(database: Database, current_date: date) -> pl.DataFrame:
    latest_ftse_date = (
        database.ftse_russell_table.read()
        .filter(pl.col("date").le(current_date))
        .select(pl.col("date").max().alias("latest_date"))
        .collect()
        .item()
    )

    if latest_ftse_date is None:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "cusip": pl.String,
                "russell_2000": pl.Boolean,
                "russell_1000": pl.Boolean,
                "cik": pl.String,
            }
        )

    # Daily update mode uses the newest FTSE Russell holdings snapshot available
    # on or before today so the candidate universe reflects the latest benchmark
    # membership without requiring an exact-date FTSE file.
    return (
        database.ftse_russell_table.read()
        .filter(
            pl.col("date").eq(latest_ftse_date),
            pl.col("russell_1000").fill_null(False) | pl.col("russell_2000").fill_null(False),
        )
        .collect()
    )


def _fetch_latest_ten_k_filing(
    row: dict[str, str | None], start_date: date, end_date: date, year: int | None = None
) -> dict[str, object]:
    # Keep the output schema stable even when EDGAR returns nothing for a name.
    result = {
        "year": year,
        "cusip": row["cusip"],
        "cik": row["cik"],
        "form": None,
        "filing_date": None,
        "acceptance_datetime": None,
        "report_date": None,
        "accession_number": None,
        "filing_url": None,
        "item_1a": None,
    }

    cik = _normalize_cik(row["cik"])
    if cik is None:
        return result

    try:
        time.sleep(SLEEP_BETWEEN)
        company = Company(cik)
        filing = _latest_filing(company, start_date, end_date)

        if filing is None:
            return result

        filing_obj = None
        try:
            filing_obj = filing.obj()
        except Exception:
            filing_obj = None

        result.update(
            {
                "form": _safe_attr(filing, "form"),
                "filing_date": _normalize_date(_safe_attr(filing, "filing_date")),
                "acceptance_datetime": _normalize_datetime(
                    _safe_attr(filing, "acceptance_datetime")
                ),
                "report_date": _normalize_date(_safe_attr(filing, "report_date")),
                "accession_number": _safe_attr(
                    filing,
                    "accession_number",
                    "accession_no",
                ),
                "filing_url": _safe_attr(
                    filing,
                    "filing_url",
                    "homepage_url",
                    "url",
                    "link",
                ),
                "item_1a": _safe_item(filing_obj, "Item 1A") if filing_obj is not None else None,
            }
        )
        if result["year"] is None and result["filing_date"] is not None:
            result["year"] = result["filing_date"].year
    except Exception:
        return result

    return result


def _year_bounds(year: int, start_date: date, end_date: date) -> tuple[date, date]:
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)

    return max(start_date, year_start), min(end_date, year_end)


def _fetch_filings_for_rows(
    rows: list[dict[str, object]],
    start_date: date,
    end_date: date,
    year: int | None = None,
    desc: str | None = None,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(_fetch_latest_ten_k_filing, row, start_date, end_date, year)
            for row in rows
        ]

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc=desc,
        ):
            results.append(future.result())

    return results


def _daily_update_candidate_df(database: Database, current_date: date) -> pl.DataFrame:
    cutoff_date = _trading_day_cutoff(current_date, TEN_K_RECHECK_TRADING_DAYS)

    cik_df = (
        load_today_ftse_cik_df(database, current_date)
        .with_columns(pl.col("cik").map_elements(_normalize_cik, return_dtype=pl.String))
        .filter(pl.col("cik").is_not_null())
        .sort(["cusip", "date"])
        .unique(subset=["cusip", "cik"], keep="last")
        .select("cusip", "cik")
        .sort(["cusip", "cik"])
    )

    if cik_df.is_empty():
        return cik_df

    latest_existing_df = (
        load_all_existing_ten_k_filings_df(database)
        .filter(pl.col("filing_date").is_not_null())
        .sort(["cusip", "cik", "filing_date"])
        .unique(subset=["cusip", "cik"], keep="last")
        .select(["cusip", "cik", "filing_date"])
        .rename({"filing_date": "last_filing_date"})
    )

    # Only re-query companies whose most recent saved 10-K is at least
    # 245 trading days old, or that have never been stored at all.
    return (
        cik_df
        .join(latest_existing_df, on=["cusip", "cik"], how="left")
        .filter(
            pl.col("last_filing_date").is_null()
            | pl.col("last_filing_date").le(cutoff_date)
        )
    )


def ten_k_filings_today_flow(current_date: date, database: Database) -> None:
    candidate_df = _daily_update_candidate_df(database, current_date)

    if candidate_df.is_empty():
        return

    lookback_start = date(current_date.year - 2, 1, 1)
    rows = candidate_df.select("cusip", "cik").to_dicts()
    results = _fetch_filings_for_rows(
        rows,
        start_date=lookback_start,
        end_date=current_date,
        year=None,
        desc=f"10-K Filings Daily Update {current_date}",
    )

    filings_df = (
        pl.from_dicts(results, infer_schema_length=10000)
        .filter(pl.col("filing_date").is_not_null())
        .sort(["year", "cusip", "cik"])
    )

    if filings_df.is_empty():
        return

    for year_df in filings_df.partition_by("year", maintain_order=True):
        year = int(year_df["year"][0])
        database.ten_k_filings_table.create_if_not_exists(year)
        database.ten_k_filings_table.upsert(year, year_df.sort(["cusip", "cik"]))


def ten_k_filings_flow(
    start_date: date, end_date: date, database: Database, today_mode: bool = False
) -> None:
    identity = os.getenv("SEC_IDENTITY")
    if identity is None:
        raise EnvironmentError(
            "Missing required environment variable: SEC_IDENTITY. "
            "Check your .env file."
        )

    set_identity(identity)

    if today_mode:
        ten_k_filings_today_flow(end_date, database)
        return

    years = list(range(start_date.year, end_date.year + 1))

    for year in years:
        year_start, year_end = _year_bounds(year, start_date, end_date)

        cik_df = (
            load_ftse_cik_df(database, year_start, year_end)
            .with_columns(pl.col("cik").map_elements(_normalize_cik, return_dtype=pl.String))
            .filter(pl.col("cik").is_not_null())
            .sort(["cusip", "date"])
            # Deduplicate to the latest Russell membership observation for each
            # CUSIP/CIK pair in the year before querying EDGAR.
            .unique(subset=["cusip", "cik"], keep="last")
            .select("cusip", "cik")
            .sort(["cusip", "cik"])
        )

        if cik_df.is_empty():
            continue

        existing_df = (
            load_existing_ten_k_filings_df(database, year)
            .filter(pl.col("filing_date").is_not_null())
            .select("cusip", "cik")
            .unique()
        )

        # Re-runs only query names that do not already have a filing saved for
        # the target year, which keeps daily/yearly refreshes lightweight.
        cik_df = cik_df.join(existing_df, on=["cusip", "cik"], how="anti")

        if cik_df.is_empty():
            continue

        rows = cik_df.to_dicts()
        results = _fetch_filings_for_rows(
            rows,
            start_date=year_start,
            end_date=year_end,
            year=year,
            desc=f"10-K Filings {year}",
        )

        filings_df = pl.from_dicts(results, infer_schema_length=10000).sort(["cusip", "cik"])

        database.ten_k_filings_table.create_if_not_exists(year)
        database.ten_k_filings_table.upsert(year, filings_df)
