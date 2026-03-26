from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import os
import time

from edgar import Company, set_identity
import polars as pl
from tqdm import tqdm

from pipelines.utils.tables import Database

MAX_WORKERS = 4
SLEEP_BETWEEN = 0.2


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


def _fetch_latest_ten_k_filing(
    row: dict[str, str | None], year: int, start_date: date, end_date: date
) -> dict[str, object]:
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
    except Exception:
        return result

    return result


def _year_bounds(year: int, start_date: date, end_date: date) -> tuple[date, date]:
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)

    return max(start_date, year_start), min(end_date, year_end)


def ten_k_filings_flow(
    start_date: date, end_date: date, database: Database
) -> None:
    identity = os.getenv("SEC_IDENTITY")
    if identity is None:
        raise EnvironmentError(
            "Missing required environment variable: SEC_IDENTITY. "
            "Check your .env file."
        )

    set_identity(identity)

    years = list(range(start_date.year, end_date.year + 1))

    for year in years:
        year_start, year_end = _year_bounds(year, start_date, end_date)

        cik_df = (
            load_ftse_cik_df(database, year_start, year_end)
            .with_columns(pl.col("cik").map_elements(_normalize_cik, return_dtype=pl.String))
            .filter(pl.col("cik").is_not_null())
            .sort(["cusip", "date"])
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

        cik_df = cik_df.join(existing_df, on=["cusip", "cik"], how="anti")

        if cik_df.is_empty():
            continue

        rows = cik_df.to_dicts()
        results: list[dict[str, object]] = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(_fetch_latest_ten_k_filing, row, year, year_start, year_end)
                for row in rows
            ]

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"10-K Filings {year}",
            ):
                results.append(future.result())

        filings_df = pl.from_dicts(results, infer_schema_length=10000).sort(["cusip", "cik"])

        database.ten_k_filings_table.create_if_not_exists(year)
        database.ten_k_filings_table.upsert(year, filings_df)
