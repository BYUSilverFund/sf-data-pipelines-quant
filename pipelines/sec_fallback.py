"""Bounded, provenance-preserving SEC fallback pilot.

This first pilot is intentionally limited to one AAT observation. It reads a
previously saved Company Facts response and filing metadata; it never makes
network requests or writes Compustat query tables.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import re
from typing import Any

import polars as pl


PILOT = {
    "ticker": "AAT",
    "cik": "0001500217",
    "gvkey": "185549",
    "barrid": "USAC121",
    "membership_date": date(2026, 8, 31),
    "mapping_snapshot_id": "5b1f0a0bbf5450a79c5594c61416d9d096e1b82e8e1c4a1a4e5b08675cdaf132",
    "mapping_snapshot_path": (
        "/home/gunnjake/groups/grp_quant/database/fundamentals/sec_filings/"
        "security_mappings/membership_2026-08-31_"
        "5b1f0a0bbf5450a79c5594c61416d9d096e1b82e8e1c4a1a4e5b08675cdaf132.parquet"
    ),
    "mapping_source": "comp.sec_idhist:item=CUSIP; exact CUSIP9; inclusive effective interval",
    "cik_source": "comp.company.cik current metadata; not historically point-in-time",
    "metric": "operating_cash_flow_ytd",
    "taxonomy": "us-gaap",
    "tag": "NetCashProvidedByUsedInOperatingActivities",
    "unit": "USD",
    "form": "10-Q",
    "accession": "0001500217-26-000052",
    "filing_date": date(2026, 10, 30),
    "acceptance_timestamp": "2026-10-30T19:25:22.000Z",
    "report_period": date(2026, 9, 30),
    "period_start": date(2026, 1, 1),
    "fiscal_year": 2026,
    "fiscal_period": "Q3",
}

FALLBACK_SCHEMA = {
    "ticker": pl.String,
    "cik": pl.String,
    "gvkey": pl.String,
    "barrid": pl.String,
    "membership_date": pl.Date,
    "mapping_snapshot_id": pl.String,
    "mapping_snapshot_path": pl.String,
    "mapping_source": pl.String,
    "cik_source": pl.String,
    "metric": pl.String,
    "taxonomy": pl.String,
    "tag": pl.String,
    "unit": pl.String,
    "value_text": pl.String,
    "value": pl.Float64,
    "period_start": pl.Date,
    "period_end": pl.Date,
    "period_semantics": pl.String,
    "fiscal_year": pl.Int32,
    "fiscal_period": pl.String,
    "form": pl.String,
    "accession": pl.String,
    "filing_date": pl.Date,
    "acceptance_timestamp": pl.String,
    "source_url": pl.String,
    "source_api": pl.String,
    "source_response_sha256": pl.String,
    "submission_response_sha256": pl.String,
    "extracted_at": pl.String,
    "extraction_version": pl.String,
}


class SecFallbackValidationError(ValueError):
    """Raised when the saved SEC evidence does not fit the documented pilot."""


def _iso_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def _valid_sha256(value: str) -> bool:
    return re.fullmatch(r"[0-9a-f]{64}", value or "") is not None


def extract_aat_q3_operating_cash_flow_ytd(
    facts_payload: dict[str, Any],
    filing: dict[str, Any],
    *,
    source_response_sha256: str,
    submission_response_sha256: str,
    extracted_at: str,
) -> pl.DataFrame:
    """Extract only the AAT FY2026 Q3 YTD operating-cash-flow observation.

    Values remain in the SEC-reported USD units (not Compustat USD millions).
    No period subtraction, currency conversion, or quant-table substitution is
    performed here.
    """
    if not _valid_sha256(source_response_sha256):
        raise SecFallbackValidationError("source_response_sha256 must be a lowercase SHA-256")
    if not _valid_sha256(submission_response_sha256):
        raise SecFallbackValidationError("submission_response_sha256 must be a lowercase SHA-256")
    try:
        extracted_datetime = datetime.fromisoformat(extracted_at.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        raise SecFallbackValidationError("extracted_at must be an ISO-8601 timestamp") from None
    if extracted_datetime.tzinfo is None or extracted_datetime.utcoffset() is None:
        raise SecFallbackValidationError("extracted_at must include a timezone")

    actual = {
        "cik": str(facts_payload.get("cik", "")).zfill(10),
        "form": filing.get("form"),
        "accession": filing.get("accessionNumber"),
        "filing_date": _iso_date(filing.get("filingDate")),
        "acceptance_timestamp": filing.get("acceptanceDateTime"),
        "report_period": _iso_date(filing.get("reportDate")),
    }
    expected = {key: PILOT[key] for key in actual}
    if actual != expected:
        raise SecFallbackValidationError(
            f"SEC entity/filing metadata is outside the documented AAT pilot: {actual}"
        )

    concepts = (
        facts_payload.get("facts", {})
        .get(PILOT["taxonomy"], {})
        .get(PILOT["tag"], {})
        .get("units", {})
        .get(PILOT["unit"], [])
    )
    matches = [
        obs
        for obs in concepts
        if obs.get("accn") == PILOT["accession"]
        and _iso_date(obs.get("start")) == PILOT["period_start"]
        and _iso_date(obs.get("end")) == PILOT["report_period"]
        and obs.get("fy") == PILOT["fiscal_year"]
        and obs.get("fp") == PILOT["fiscal_period"]
        and obs.get("form") == PILOT["form"]
    ]
    if len(matches) != 1:
        raise SecFallbackValidationError(
            "Expected exactly one exact-accession, exact-unit, exact-YTD-period "
            f"fact; found {len(matches)}"
        )

    observation = matches[0]
    raw_value = observation.get("val")
    try:
        numeric_value = float(Decimal(str(raw_value)))
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        raise SecFallbackValidationError("SEC fact val is not a finite numeric value") from None
    if numeric_value != numeric_value or numeric_value in (float("inf"), float("-inf")):
        raise SecFallbackValidationError("SEC fact val is not a finite numeric value")

    primary_document = str(filing.get("primaryDocument") or "").strip("/")
    if not primary_document or ".." in primary_document.split("/"):
        raise SecFallbackValidationError("Filing metadata must include a safe primaryDocument")
    source_url = (
        "https://www.sec.gov/Archives/edgar/data/1500217/"
        "000150021726000052/" + primary_document
    )
    row = {
        "ticker": PILOT["ticker"],
        "cik": PILOT["cik"],
        "gvkey": PILOT["gvkey"],
        "barrid": PILOT["barrid"],
        "membership_date": PILOT["membership_date"],
        "mapping_snapshot_id": PILOT["mapping_snapshot_id"],
        "mapping_snapshot_path": PILOT["mapping_snapshot_path"],
        "mapping_source": PILOT["mapping_source"],
        "cik_source": PILOT["cik_source"],
        "metric": PILOT["metric"],
        "taxonomy": PILOT["taxonomy"],
        "tag": PILOT["tag"],
        "unit": PILOT["unit"],
        "value_text": str(raw_value),
        "value": numeric_value,
        "period_start": PILOT["period_start"],
        "period_end": PILOT["report_period"],
        "period_semantics": "fiscal_year_to_date",
        "fiscal_year": PILOT["fiscal_year"],
        "fiscal_period": PILOT["fiscal_period"],
        "form": PILOT["form"],
        "accession": PILOT["accession"],
        "filing_date": PILOT["filing_date"],
        "acceptance_timestamp": filing.get("acceptanceDateTime"),
        "source_url": source_url,
        "source_api": "sec_companyfacts",
        "source_response_sha256": source_response_sha256,
        "submission_response_sha256": submission_response_sha256,
        "extracted_at": extracted_at,
        "extraction_version": "aat_q3_2026_operating_cash_flow_ytd_v1",
    }
    return pl.DataFrame([row], schema=FALLBACK_SCHEMA)


def sha256_file(path: str) -> str:
    """Hash a saved SEC JSON response for extraction provenance."""
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
