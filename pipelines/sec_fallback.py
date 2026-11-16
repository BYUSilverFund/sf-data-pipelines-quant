"""Reusable, configuration-driven SEC operating-cash-flow YTD extraction.

Extraction reads saved Company Facts and filing metadata. It does not fetch
SEC data or write Compustat query tables; acceptance and resolution are
separate reviewed steps.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import re
from typing import Any

import polars as pl


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


def extract_ocf_ytd_observation(
    facts_payload: dict[str, Any],
    filing: dict[str, Any],
    *,
    config: dict[str, Any],
    source_response_sha256: str,
    submission_response_sha256: str,
    extracted_at: str,
) -> pl.DataFrame:
    """Extract one configured operating-cash-flow fiscal-YTD observation.

    ``config`` is a candidate specification, not evidence of acceptance. It
    must identify the issuer, period, fiscal labels, metric definition, unit,
    and verified mapping. Discovery code may produce configurations for
    review; only configurations with ``decision='accept'`` and affirmative
    identity and scope evidence can be extracted for resolution.
    """
    for digest, label in ((source_response_sha256, "source_response_sha256"),
                          (submission_response_sha256, "submission_response_sha256")):
        if not _valid_sha256(digest):
            raise SecFallbackValidationError(f"{label} must be a lowercase SHA-256")
    try:
        extracted_datetime = datetime.fromisoformat(extracted_at.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        raise SecFallbackValidationError("extracted_at must be an ISO-8601 timestamp") from None
    if extracted_datetime.tzinfo is None or extracted_datetime.utcoffset() is None:
        raise SecFallbackValidationError("extracted_at must include a timezone")

    required = {
        "ticker", "cik", "gvkey", "barrid", "membership_date",
        "mapping_snapshot_id", "mapping_snapshot_path", "mapping_source",
        "cik_source", "decision", "identity_verified", "scope_verified",
        "taxonomy", "tag", "unit", "accession", "form", "report_period",
        "period_start", "fiscal_year", "fiscal_period",
    }
    missing = required - set(config)
    if missing:
        raise SecFallbackValidationError(f"candidate config missing fields: {sorted(missing)}")
    if config["decision"] != "accept":
        raise SecFallbackValidationError("candidate decision is not accept")
    if config["identity_verified"] is not True or config["scope_verified"] is not True:
        raise SecFallbackValidationError("accounting entity and consolidation scope are not verified")

    cik = str(config["cik"]).zfill(10)
    accession = str(config["accession"])
    expected = {
        "cik": cik,
        "form": config["form"],
        "accession": accession,
        "filing_date": _iso_date(filing.get("filingDate")),
        "acceptance_timestamp": filing.get("acceptanceDateTime"),
        "report_period": _iso_date(filing.get("reportDate")),
    }
    actual = {
        "cik": str(facts_payload.get("cik", "")).zfill(10),
        "form": filing.get("form"),
        "accession": filing.get("accessionNumber"),
        "filing_date": _iso_date(filing.get("filingDate")),
        "acceptance_timestamp": filing.get("acceptanceDateTime"),
        "report_period": _iso_date(filing.get("reportDate")),
    }
    if actual != expected:
        raise SecFallbackValidationError(f"SEC entity/filing metadata differs from candidate config: {actual}")

    facts = facts_payload.get("facts", {}).get(config["taxonomy"], {}).get(config["tag"], {})
    observations = facts.get("units", {}).get(config["unit"], [])
    matches = [
        obs for obs in observations
        if obs.get("accn") == accession
        and _iso_date(obs.get("start")) == _iso_date(config["period_start"])
        and _iso_date(obs.get("end")) == _iso_date(config["report_period"])
        and obs.get("fy") == config["fiscal_year"]
        and obs.get("fp") == config["fiscal_period"]
        and obs.get("form") == config["form"]
    ]
    if len(matches) != 1:
        raise SecFallbackValidationError(f"expected one exact accession/unit/YTD fact; found {len(matches)}")
    observation = matches[0]
    try:
        numeric_value = float(Decimal(str(observation.get("val"))))
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        raise SecFallbackValidationError("SEC fact val is not a finite numeric value") from None
    if not numeric_value == numeric_value or numeric_value in (float("inf"), float("-inf")):
        raise SecFallbackValidationError("SEC fact val is not a finite numeric value")
    primary_document = str(filing.get("primaryDocument") or "").strip("/")
    if not primary_document or ".." in primary_document.split("/"):
        raise SecFallbackValidationError("filing metadata must include a safe primaryDocument")

    report_period = _iso_date(config["report_period"])
    period_start = _iso_date(config["period_start"])
    membership_date = _iso_date(config["membership_date"])
    if None in (report_period, period_start, membership_date):
        raise SecFallbackValidationError("candidate dates must be ISO calendar dates")
    cik_int = str(int(cik))
    accession_compact = accession.replace("-", "")
    row = {
        "ticker": config["ticker"], "cik": cik, "gvkey": str(config["gvkey"]),
        "barrid": config["barrid"], "membership_date": membership_date,
        "mapping_snapshot_id": config["mapping_snapshot_id"],
        "mapping_snapshot_path": config["mapping_snapshot_path"],
        "mapping_source": config["mapping_source"], "cik_source": config["cik_source"],
        "metric": config.get("metric", "operating_cash_flow_ytd"),
        "taxonomy": config["taxonomy"], "tag": config["tag"], "unit": config["unit"],
        "value_text": str(observation.get("val")), "value": numeric_value,
        "period_start": period_start, "period_end": report_period,
        "period_semantics": "fiscal_year_to_date", "fiscal_year": int(config["fiscal_year"]),
        "fiscal_period": config["fiscal_period"], "form": filing["form"],
        "accession": accession, "filing_date": expected["filing_date"],
        "acceptance_timestamp": filing["acceptanceDateTime"],
        "source_url": f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_compact}/{primary_document}",
        "source_api": "sec_companyfacts", "source_response_sha256": source_response_sha256,
        "submission_response_sha256": submission_response_sha256,
        "extracted_at": extracted_at,
        "extraction_version": config.get("extraction_version", "operating_cash_flow_ytd_v1"),
    }
    return pl.DataFrame([row], schema=FALLBACK_SCHEMA)


def sha256_file(path: str) -> str:
    """Hash a saved SEC JSON response for extraction provenance."""
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
