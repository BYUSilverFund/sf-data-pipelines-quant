"""Build content-addressed OCF-YTD reviews from explicitly selected archived gaps."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import calendar
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any

import polars as pl
from dotenv import load_dotenv

from pipelines.fundamentals_flow import _fetch_and_archive_json, _make_sec_session
from pipelines.fundamentals_storage import filing_source_url

from pipelines.sec_ocf_cohort import (
    AFTER_CUTOFF,
    INCOMPATIBLE,
    NO_DISCLOSURE,
    OCF_TAG,
    OCF_TAXONOMY,
    OCF_UNIT,
    UNRESOLVED,
    VERIFIED_CANDIDATE,
    classify_ocf_gap,
    collect_ocf_ytd_observations,
    inline_ocf_facts,
)


NO_HISTORICAL_CUTOFF = None

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _latest_json(directory: Path) -> Path:
    files = sorted(directory.glob("*.json"))
    expected_key = "facts" if directory.name == "companyfacts" else "filings"
    for path in reversed(files):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(payload, dict) and expected_key in payload:
            return path
    raise FileNotFoundError(f"No valid saved SEC {directory.name} response under {directory}")


def _retrieved_at(path: Path) -> str | None:
    match = re.match(r"(\d{8}T\d{12}Z)_", path.name)
    if not match:
        return None
    return match.group(1)


def _filings_for_period(submissions: dict[str, Any], report_date: str) -> list[dict[str, Any]]:
    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    result = []
    for index, form in enumerate(forms):
        if form not in {"10-Q", "10-Q/A"} or recent.get("reportDate", [None] * len(forms))[index] != report_date:
            continue
        result.append(
            {
                key: recent.get(key, [None] * len(forms))[index]
                for key in (
                    "accessionNumber",
                    "filingDate",
                    "reportDate",
                    "acceptanceDateTime",
                    "form",
                    "primaryDocument",
                )
            }
        )
    return sorted(result, key=lambda row: (row["filingDate"], row["accessionNumber"]))


def _source_document(directory: Path, cik: str, accession: str) -> Path | None:
    accession_key = accession.replace("-", "")
    files = sorted(directory.glob(f"CIK{cik}_{accession_key}_*.html"))
    return files[-1] if files else None


def _fiscal_ytd_start(case: dict[str, Any], end: date) -> date | None:
    """Return the start of the row's fiscal year, checking its FYR/FYEARQ labels."""
    fyr = case.get("fyr")
    fyearq = case.get("fyearq")
    if fyr is None or fyearq is None:
        return date(end.year, 1, 1)
    try:
        fyr = int(fyr)
        fyearq = int(fyearq)
    except (TypeError, ValueError):
        return None
    if not 1 <= fyr <= 12:
        return None
    fiscal_end_year = end.year if end.month <= fyr else end.year + 1
    if fyearq != fiscal_end_year:
        return None
    prior_fiscal_end = date(
        fiscal_end_year - 1,
        fyr,
        calendar.monthrange(fiscal_end_year - 1, fyr)[1],
    )
    return date.fromordinal(prior_fiscal_end.toordinal() + 1)


def _mapping_relationship_is_valid_at_period(link: dict[str, Any], end: date) -> bool:
    """Require both Compustat and Barra security intervals to cover the report date."""
    for start_field, end_field in (
        ("compustat_efffrom", "compustat_effthru"),
        ("barra_start_date", "barra_end_date"),
    ):
        start, finish = link.get(start_field), link.get(end_field)
        if isinstance(start, str):
            start = date.fromisoformat(start)
        if isinstance(finish, str):
            finish = date.fromisoformat(finish)
        if start is not None and start > end:
            return False
        if finish is not None and end > finish:
            return False
    return True


def _plain_inline(fact: dict[str, Any]) -> dict[str, Any]:
    return {
        key: (str(value) if isinstance(value, Decimal) else value)
        for key, value in fact.items()
        if key not in {"text_parts"}
    }


def _case_result(
    case: dict[str, Any],
    *,
    mapping: pl.DataFrame,
    quarterly: pl.DataFrame,
    facts_path: Path,
    submissions_path: Path,
    filing_directory: Path,
    mapping_hash: str,
    mapping_snapshot_path: Path,
    compustat_run_id: str,
    quarterly_hash: str,
    collection_cutoff: date,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    end = date.fromisoformat(case["datadate"])
    start = _fiscal_ytd_start(case, end)
    fiscal_period_labels_valid = start is not None
    if start is None:
        # Keep extraction deterministic, but never classify/accept a row whose
        # fiscal-year labels contradict the report date.
        start = date(end.year, 1, 1)
    mapping_fields = [
        column for column in (
            "cik", "barrid", "cik_source", "mapping_status", "cusip9", "iid",
            "compustat_efffrom", "compustat_effthru", "barra_start_date", "barra_end_date",
        ) if column in mapping.columns
    ]
    mapping_rows = mapping.filter(pl.col("gvkey") == case["gvkey"]).select(mapping_fields).unique()
    ciks = set(mapping_rows.get_column("cik").drop_nulls().to_list())
    matching_links = mapping_rows.filter(
        (pl.col("cik") == case["cik"])
        & (pl.col("barrid") == case["barrid"])
        & (pl.col("mapping_status") == "mapped_current_cik_metadata")
    )
    mapping_relationship_valid = any(
        _mapping_relationship_is_valid_at_period(link, end)
        for link in matching_links.iter_rows(named=True)
    )
    mapping_verified = ciks == {case["cik"]} and mapping_relationship_valid
    comp_filter = (pl.col("gvkey") == case["gvkey"]) & (pl.col("datadate") == end)
    for field in ("fyearq", "fqtr", "fyr"):
        if case.get(field) is not None and field in quarterly.columns:
            comp_filter &= pl.col(field) == case[field]
    case_currency = case.get("currency")
    currency_field = "curcdq" if "curcdq" in quarterly.columns else "currency"
    if case_currency is not None and currency_field in quarterly.columns:
        comp_filter &= pl.col(currency_field) == case_currency
    comp_rows = quarterly.filter(comp_filter)
    comp_verified = comp_rows.height == 1 and comp_rows["oancfy"][0] is None
    company_row = comp_rows.row(0, named=True) if comp_rows.height == 1 else {}

    facts_payload = json.loads(facts_path.read_text(encoding="utf-8"))
    submissions = json.loads(submissions_path.read_text(encoding="utf-8"))
    sec_name = str(submissions.get("name", ""))
    facts_name = str(facts_payload.get("entityName", ""))
    name_normalize = lambda value: re.sub(r"[^a-z0-9]", "", value.lower())
    names_match = bool(sec_name and facts_name and name_normalize(sec_name) == name_normalize(facts_name))
    entity_verified = (
        str(facts_payload.get("cik", "")).zfill(10) == case["cik"]
        and bool(sec_name)
        and bool(facts_name)
    )
    taxonomy_names = set(facts_payload.get("facts", {}))
    target_filings = _filings_for_period(submissions, end.isoformat())
    target_accessions = {row["accessionNumber"] for row in target_filings}
    observations = collect_ocf_ytd_observations(
        facts_payload, period_start=start, period_end=end
    )
    exact_observations = [
        row for row in observations if row.get("accn") in target_accessions
    ]

    inline_evidence: list[dict[str, Any]] = []
    filing_evidence: list[dict[str, Any]] = []
    recent = submissions.get("filings", {}).get("recent", {})
    submission_by_accession: dict[str, dict[str, Any]] = {}
    for index, accession in enumerate(recent.get("accessionNumber", [])):
        submission_by_accession[accession] = {
            key: recent.get(key, [None] * len(recent.get("accessionNumber", [])))[index]
            for key in (
                "accessionNumber",
                "filingDate",
                "reportDate",
                "acceptanceDateTime",
                "form",
                "primaryDocument",
            )
        }
    for filing in target_filings:
        path = _source_document(filing_directory, case["cik"], filing["accessionNumber"])
        if path is None:
            continue
        raw_html = path.read_bytes()
        parsed = inline_ocf_facts(raw_html, period_start=start, period_end=end)
        inline_evidence.extend(parsed)
        filing_evidence.append(
            {
                "accession": filing["accessionNumber"],
                "source_url": (
                    f"https://www.sec.gov/Archives/edgar/data/{int(case['cik'])}/"
                    f"{filing['accessionNumber'].replace('-', '')}/{filing['primaryDocument']}"
                ),
                "archive_path": str(path),
                "sha256": _sha256(raw_html),
                "bytes": len(raw_html),
                "inline_ocf_ytd_facts": [_plain_inline(fact) for fact in parsed],
            }
        )

    related_filing_evidence = []
    for observation in observations:
        accession = str(observation.get("accn", ""))
        if accession in target_accessions:
            continue
        filing = submission_by_accession.get(accession)
        if filing is None:
            continue
        path = _source_document(filing_directory, case["cik"], accession)
        if path is None:
            continue
        raw_html = path.read_bytes()
        parsed = inline_ocf_facts(raw_html, period_start=start, period_end=end)
        related_filing_evidence.append(
            {
                "accession": accession,
                "form": filing["form"],
                "report_date": filing["reportDate"],
                "filing_date": filing["filingDate"],
                "acceptance_timestamp": filing["acceptanceDateTime"],
                "source_url": (
                    f"https://www.sec.gov/Archives/edgar/data/{int(case['cik'])}/"
                    f"{accession.replace('-', '')}/{filing['primaryDocument']}"
                ),
                "archive_path": str(path),
                "sha256": _sha256(raw_html),
                "bytes": len(raw_html),
                "inline_ocf_ytd_facts": [_plain_inline(fact) for fact in parsed],
                "inline_values_match_companyfacts": any(
                    fact.get("scaled_value") == Decimal(str(observation["val"]))
                    for fact in parsed
                ),
            }
        )

    classification, reason = classify_ocf_gap(
        mapping_verified=mapping_verified and comp_verified and fiscal_period_labels_valid,
        entity_verified=bool(entity_verified),
        taxonomy_names=taxonomy_names,
        target_filings=target_filings,
        exact_accession_observations=exact_observations,
        all_period_observations=observations,
        inline_facts=inline_evidence,
        expected_period_start=start,
        expected_period_end=end,
        collection_cutoff=collection_cutoff,
    )

    if not comp_verified:
        classification, reason = UNRESOLVED, "The saved Compustat extract did not contain exactly one null oancfy row for the selected fiscal-period key."
    if not fiscal_period_labels_valid:
        classification, reason = UNRESOLVED, "Compustat fiscal-year-end and fiscal-year labels do not agree with the report date."
    if mapping_verified and not entity_verified:
        classification, reason = UNRESOLVED, "The CIK or SEC entity names did not agree across mapping, submissions, and Company Facts."

    accepted_accessions = {
        filing["accessionNumber"]
        for filing in target_filings
        if date.fromisoformat(filing["filingDate"]) <= collection_cutoff
    }
    all_observation_rows = []
    for obs in observations:
        fact_accession = str(obs.get("accn", ""))
        fact_filing = submission_by_accession.get(fact_accession, {})
        fact_document = _source_document(filing_directory, case["cik"], fact_accession)
        fact_document_sha256 = (
            _sha256(fact_document.read_bytes()) if fact_document is not None else None
        )
        fact_source_url = None
        if fact_filing.get("primaryDocument"):
            fact_source_url = (
                f"https://www.sec.gov/Archives/edgar/data/{int(case['cik'])}/"
                f"{fact_accession.replace('-', '')}/{fact_filing['primaryDocument']}"
            )
        all_observation_rows.append(
            {
                "cik": case["cik"],
                "gvkey": case["gvkey"],
                "barrid": case["barrid"],
                "membership_date": "2026-08-31",
                "mapping_snapshot_path": str(mapping_snapshot_path),
                "mapping_snapshot_sha256": mapping_hash,
                "mapping_source": "comp.sec_idhist CUSIP relationship and current comp.company.cik metadata",
                "cik_source_is_point_in_time": False,
                "datadate": end.isoformat(),
                "metric": "operating_cash_flow_ytd",
                "taxonomy": OCF_TAXONOMY,
                "tag": OCF_TAG,
                "unit": obs["unit"],
                "value_text": str(obs["val"]),
                "value": float(Decimal(str(obs["val"]))),
                "period_start": obs["start"],
                "period_end": obs["end"],
                "period_semantics": "fiscal_year_to_date",
                "fiscal_year": obs.get("fy"),
                "fiscal_period": obs.get("fp"),
                "form": obs.get("form"),
                "accession": obs.get("accn"),
                "filing_date": obs.get("filed"),
                "acceptance_timestamp": fact_filing.get("acceptanceDateTime"),
                "source_url": fact_source_url,
                "source_filing_html_sha256": fact_document_sha256,
                "is_target_report_accession": obs.get("accn") in target_accessions,
                "is_amended_form": obs.get("form") == "10-Q/A",
                "accession_role": (
                    "target_report"
                    if obs.get("accn") in target_accessions
                    else "comparative_or_later_accession"
                ),
                "source_response_sha256": _sha256(facts_path.read_bytes()),
                "source_response_retrieved_at": _retrieved_at(facts_path),
                "submission_response_sha256": _sha256(submissions_path.read_bytes()),
                "submission_response_retrieved_at": _retrieved_at(submissions_path),
                "classification": classification,
                "extraction_version": "sec_ocf_ytd_cohort_v1",
            }
        )

    result = {
        **case,
        "company_name": sec_name,
        "companyfacts_entity_name": facts_name,
        "sec_and_companyfacts_names_match": names_match,
        "mapping_verified": mapping_verified,
        "mapping_relationship_valid_at_period": mapping_relationship_valid,
        "company_identity_verified": bool(entity_verified),
        "mapping_cik_source": mapping_rows["cik_source"][0] if mapping_rows.height else None,
        "mapping_status": mapping_rows["mapping_status"][0] if mapping_rows.height else None,
        "mapping_snapshot_path": str(mapping_snapshot_path),
        "mapping_snapshot_sha256": mapping_hash,
        "compustat_row_count": comp_rows.height,
        "compustat_oancfy": company_row.get("oancfy"),
        "compustat_rdq": str(company_row.get("rdq")) if company_row.get("rdq") is not None else None,
        "compustat_currency": company_row.get("curcdq"),
        "compustat_extract_sha256": quarterly_hash,
        "compustat_run_id": compustat_run_id,
        "fiscal_quarter": company_row.get("fqtr"),
        "fiscal_year_quarter": company_row.get("fyearq"),
        "fiscal_year_end_month": company_row.get("fyr"),
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "collection_cutoff": collection_cutoff.isoformat(),
        "collection_cutoff_basis": f"explicit collection cutoff {collection_cutoff.isoformat()}",
        "historical_investment_cutoff": NO_HISTORICAL_CUTOFF,
        "historical_availability_claimed": False,
        "taxonomy_names": sorted(taxonomy_names),
        "classification": classification,
        "reason": reason,
        "matching_period_filings": target_filings,
        "exact_accession_observation_count": len(exact_observations),
        "all_matching_period_observation_count": len(observations),
        "all_matching_period_observations": observations,
        "filing_evidence": filing_evidence,
        "related_filing_evidence": related_filing_evidence,
        "companyfacts_response_path": str(facts_path),
        "companyfacts_response_retrieved_at": _retrieved_at(facts_path),
        "companyfacts_response_sha256": _sha256(facts_path.read_bytes()),
        "submissions_response_path": str(submissions_path),
        "submissions_response_retrieved_at": _retrieved_at(submissions_path),
        "submissions_response_sha256": _sha256(submissions_path.read_bytes()),
    }
    return result, all_observation_rows


def _write_content_addressed(directory: Path, stem: str, suffix: str, payload: bytes) -> Path:
    digest = _sha256(payload)
    path = directory / f"{stem}_{digest}{suffix}"
    directory.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if _sha256(path.read_bytes()) != digest:
            raise RuntimeError(f"Content-address collision at {path}")
    else:
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_bytes(payload)
        os.replace(temporary, path)
    return path


def sec_fetch_progress_path(archive_root: Path, cases: tuple[dict[str, Any], ...]) -> Path:
    candidate_keys = sorted({
        (str(case["cik"]).zfill(10), str(case["datadate"])) for case in cases
    })
    progress_id = _sha256(json.dumps(candidate_keys, separators=(",", ":")).encode())
    return (archive_root / "fallback_pilots" / "ocf_ytd_cohort_2026-11"
            / f"sec_fetch_progress_{progress_id}.json")


def refresh_archived_sec_evidence(
    *, archive_root: Path, cases: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    """Resume SEC evidence collection from valid archives, recording per-CIK progress."""
    load_dotenv(override=True)
    user_agent = os.environ.get("SEC_USER_AGENT", "")
    if not user_agent or re.search(r"Your Organization|contact@example\.com", user_agent, re.I):
        raise ValueError("Set SEC_USER_AGENT to an identifying organization/app name and contact email")
    session = _make_sec_session(user_agent)
    filing_directory = archive_root / "fallback_pilots" / "ocf_ytd_cohort_2026-11" / "source_documents"
    progress_dir = archive_root / "fallback_pilots" / "ocf_ytd_cohort_2026-11"
    progress_dir.mkdir(parents=True, exist_ok=True)
    candidate_keys = sorted({
        (str(case["cik"]).zfill(10), str(case["datadate"])) for case in cases
    })
    progress_path = sec_fetch_progress_path(archive_root, cases)
    progress_id = progress_path.stem.removeprefix("sec_fetch_progress_")
    progress: dict[str, Any] = {
        "schema_version": 1, "progress_id": progress_id,
        "candidate_count": len(candidate_keys), "ciks": {},
    }
    if progress_path.exists():
        try:
            saved_progress = json.loads(progress_path.read_text())
            if saved_progress.get("progress_id") == progress_id:
                progress = saved_progress
        except (OSError, json.JSONDecodeError):
            pass
    by_cik: dict[str, list[dict[str, Any]]] = {}
    for case in cases:
        by_cik.setdefault(str(case["cik"]).zfill(10), []).append(case)
    result: dict[str, Any] = {
        "candidate_ciks": len(by_cik), "cached_ciks": 0, "fetched_ciks": 0,
        "failed_ciks": 0, "filing_documents_cached": 0,
        "filing_documents_fetched": 0, "failed_requests": [],
        "coverage_complete": False, "progress_manifest": str(progress_path),
    }

    def cached_endpoint(cik: str, endpoint: str) -> dict[str, Any] | None:
        directory = archive_root / "api_responses" / f"CIK{cik}" / endpoint
        files = sorted(directory.glob("*.json"))
        for path in reversed(files):
            try:
                raw = path.read_bytes()
                payload = json.loads(raw)
            except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                continue
            valid = (
                isinstance(payload, dict)
                and ("facts" in payload if endpoint == "companyfacts" else "filings" in payload)
            )
            if valid:
                return {"data": payload, "path": path, "sha256": _sha256(raw), "cached": True}
        return None

    def save_progress() -> None:
        temp = progress_path.with_name(f".{progress_path.name}.{os.getpid()}.tmp")
        temp.write_text(json.dumps(progress, sort_keys=True, indent=2) + "\n")
        os.replace(temp, progress_path)

    try:
        for index, cik in enumerate(sorted(by_cik), start=1):
            entry: dict[str, Any] = {"periods": sorted(k[1] for k in candidate_keys if k[0] == cik)}
            errors: list[str] = []
            endpoint_data: dict[str, dict[str, Any]] = {}
            fetched_any = False
            for endpoint in ("companyfacts", "submissions"):
                cached = cached_endpoint(cik, endpoint)
                if cached is not None:
                    endpoint_data[endpoint] = cached
                    entry[endpoint] = {"status": "cached", "path": str(cached["path"]), "sha256": cached["sha256"]}
                    continue
                url = (f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
                       if endpoint == "companyfacts" else f"https://data.sec.gov/submissions/CIK{cik}.json")
                try:
                    response = _fetch_and_archive_json(session, url, archive_root, cik, endpoint)
                    if response.get("data") is None:
                        raise ValueError(f"HTTP {response.get('status_code')} returned no usable {endpoint} payload")
                    endpoint_data[endpoint] = {**response, "cached": False}
                    fetched_any = True
                    entry[endpoint] = {
                        "status": "fetched", "path": str(response["path"]),
                        "sha256": response["sha256"], "retrieved_at": str(response["retrieved_at"]),
                    }
                except Exception as exc:  # per-endpoint failure is persisted and resumed later
                    message = f"{endpoint}: {type(exc).__name__}: {exc}"
                    errors.append(message)
                    entry[endpoint] = {"status": "failed", "error": message}

            required_documents: dict[str, dict[str, str]] = {}
            if len(endpoint_data) == 2:
                facts = endpoint_data["companyfacts"]["data"]
                submissions = endpoint_data["submissions"]["data"]
                accessions: set[str] = set()
                for case in by_cik[cik]:
                    end = date.fromisoformat(str(case["datadate"]))
                    start = date(end.year, 1, 1)
                    observations = collect_ocf_ytd_observations(facts, period_start=start, period_end=end)
                    accessions.update(
                        str(row.get("accn")) for row in observations
                        if str(row.get("start")) == start.isoformat()
                        and str(row.get("end")) == end.isoformat() and row.get("accn")
                    )
                    target_filings = _filings_for_period(submissions, end.isoformat())
                    accessions.update(str(row["accessionNumber"]) for row in target_filings)
                recent = submissions.get("filings", {}).get("recent", {})
                for accession in accessions:
                    for idx, item in enumerate(recent.get("accessionNumber", [])):
                        if str(item) == accession:
                            filing = {
                                key: recent.get(key, [None] * len(recent.get("accessionNumber", [])))[idx]
                                for key in ("primaryDocument", "form", "reportDate")
                            }
                            if filing.get("primaryDocument"):
                                required_documents[accession] = filing
                            break
            document_records: dict[str, Any] = {}
            for accession, filing in sorted(required_documents.items()):
                existing = _source_document(filing_directory, cik, accession)
                if existing is not None:
                    raw = existing.read_bytes()
                    document_records[accession] = {"status": "cached", "path": str(existing), "sha256": _sha256(raw)}
                    result["filing_documents_cached"] += 1
                    continue
                url = filing_source_url(cik, accession, str(filing["primaryDocument"]))
                try:
                    response = session.get(url, timeout=(10, 60))
                    response.raise_for_status()
                    path = _write_content_addressed(
                        filing_directory, f"CIK{cik}_{accession.replace('-', '')}", ".html", response.content,
                    )
                    digest = _sha256(response.content)
                    document_records[accession] = {"status": "fetched", "path": str(path), "sha256": digest}
                    result["filing_documents_fetched"] += 1
                    time.sleep(0.12)
                except Exception as exc:
                    message = f"filing {accession}: {type(exc).__name__}: {exc}"
                    errors.append(message)
                    document_records[accession] = {"status": "failed", "error": message}
            entry["filings"] = document_records
            complete = len(endpoint_data) == 2 and not errors
            entry["status"] = "complete" if complete else "partial"
            entry["errors"] = errors
            entry["completed_at"] = datetime.now(timezone.utc).isoformat()
            progress.setdefault("ciks", {})[cik] = entry
            save_progress()
            if errors:
                result["failed_requests"].extend({"cik": cik, "error": error} for error in errors)
            if complete:
                if fetched_any:
                    result["fetched_ciks"] += 1
                else:
                    result["cached_ciks"] += 1
            else:
                result["failed_ciks"] += 1
            print(f"SEC evidence {index}/{len(by_cik)} CIK{cik}: {entry['status']}", flush=True)
    finally:
        session.close()
    completed_ciks = sum(entry.get("status") == "complete" for entry in progress.get("ciks", {}).values())
    result["complete_ciks"] = completed_ciks
    result["coverage_complete"] = completed_ciks == result["candidate_ciks"]
    result["progress_manifest"] = str(progress_path)
    save_progress()
    return result


def discover_additional_null_gaps(
    mapping: pl.DataFrame, quarterly: pl.DataFrame, *, limit: int | None = 10,
    known_cases: tuple[dict[str, Any], ...] = (),
) -> tuple[dict[str, Any], ...]:
    """Choose deterministic null company-period gaps, optionally across the full universe."""
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")
    if limit == 0:
        return ()
    required_mapping = {"gvkey", "barrid", "cik", "mapping_status"}
    if not required_mapping.issubset(mapping.columns):
        raise ValueError(f"mapping snapshot is missing columns: {sorted(required_mapping - set(mapping.columns))}")
    required_quarterly = {"gvkey", "datadate", "fyearq", "oancfy"}
    if not required_quarterly.issubset(quarterly.columns):
        raise ValueError(f"quarterly extract is missing columns: {sorted(required_quarterly - set(quarterly.columns))}")
    known = {(str(case["gvkey"]), str(case["datadate"])) for case in known_cases}
    period_columns = ["gvkey", "datadate", "fyearq"]
    period_columns.extend(
        column for column in ("fqtr", "fyr", "curcdq", "currency")
        if column in quarterly.columns and column not in period_columns
    )
    unique_quarterly = quarterly.group_by(period_columns).agg(
        pl.len().alias("row_count"), pl.col("oancfy").first().alias("oancfy"),
        *(
            [pl.col("rdq").first().alias("rdq")] if "rdq" in quarterly.columns else []
        ),
    )
    verified_mapping = mapping.filter(pl.col("mapping_status") == "mapped_current_cik_metadata")
    mapping_by_gvkey: dict[str, tuple[set[str], list[str]]] = {}
    for gvkey, links in verified_mapping.group_by("gvkey", maintain_order=True):
        if isinstance(gvkey, tuple):
            gvkey = gvkey[0]
        cik_values = {str(value).zfill(10) for value in links["cik"].drop_nulls().to_list()}
        barrids = sorted(str(value) for value in links["barrid"].drop_nulls().unique().to_list())
        mapping_by_gvkey[str(gvkey)] = (cik_values, barrids)
    gaps = (
        unique_quarterly.filter(pl.col("oancfy").is_null() & (pl.col("row_count") == 1))
        .sort(["datadate", "gvkey", "fyearq"], descending=[True, False, False])
    )
    candidates: list[dict[str, Any]] = []
    for row in gaps.iter_rows(named=True):
        if row["fyearq"] is None:
            continue
        gvkey = str(row["gvkey"])
        datadate = row["datadate"].isoformat()
        if (gvkey, datadate) in known:
            continue
        ciks, barrids = mapping_by_gvkey.get(gvkey, (set(), []))
        if len(ciks) != 1 or not barrids:
            continue
        case = {
            "cik": next(iter(ciks)), "gvkey": gvkey, "barrid": barrids[0],
            "ticker": None, "datadate": datadate, "fyearq": int(row["fyearq"]),
            "additional_barrids": barrids,
        }
        for field in ("fqtr", "fyr", "curcdq", "currency", "rdq"):
            value = row.get(field)
            if value is not None:
                case[field] = value.isoformat() if isinstance(value, (date, datetime)) else value
        currency_value = row.get("curcdq") or row.get("currency")
        if currency_value is not None:
            case["currency"] = currency_value
        candidates.append(case)
        relationship_fields = [
            column for column in (
                "cusip9", "iid", "cik", "barrid", "issuerid", "rootid",
                "mapping_source", "cik_source", "mapping_status",
                "compustat_efffrom", "compustat_effthru",
                "barra_start_date", "barra_end_date", "barra_mapping_status",
            ) if column in mapping.columns
        ]
        relationship_rows = (
            mapping.filter((pl.col("gvkey").cast(pl.Utf8) == gvkey)
                           & (pl.col("cik").cast(pl.Utf8).str.zfill(10) == case["cik"]))
            .select(relationship_fields).unique().to_dicts()
        )
        case["mapping_relationships"] = [
            {
                field: value.isoformat() if isinstance(value, (date, datetime)) else value
                for field, value in relationship.items()
            }
            for relationship in relationship_rows
        ]
        if limit is not None and len(candidates) >= limit:
            break
    return tuple(candidates)


def build(
    *,
    compustat_root: Path,
    archive_root: Path,
    run_id: str,
    mapping_path: Path,
    cases: tuple[dict[str, Any], ...],
    collection_cutoff: date,
) -> tuple[Path, Path, list[dict[str, Any]]]:
    selected_cases = cases
    quarterly_path = compustat_root / "source_extracts" / run_id / "quarterly.parquet"
    mapping = pl.read_parquet(mapping_path)
    quarterly = pl.read_parquet(quarterly_path)
    mapping_hash = _sha256(mapping_path.read_bytes())
    quarterly_hash = _sha256(quarterly_path.read_bytes())
    filing_directory = archive_root / "fallback_pilots" / "ocf_ytd_cohort_2026-11" / "source_documents"
    cases: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    api_root = archive_root / "api_responses"
    for case in selected_cases:
        cik_root = api_root / f"CIK{case['cik']}"
        submissions_path = _latest_json(cik_root / "submissions")
        facts_path = _latest_json(cik_root / "companyfacts")
        case_result, case_observations = _case_result(
            case,
            mapping=mapping,
            quarterly=quarterly,
            facts_path=facts_path,
            submissions_path=submissions_path,
            filing_directory=filing_directory,
            mapping_hash=mapping_hash,
            mapping_snapshot_path=mapping_path,
            compustat_run_id=run_id,
            quarterly_hash=quarterly_hash,
            collection_cutoff=collection_cutoff,
        )
        cases.append(case_result)
        observations.extend(case_observations)

    case_bytes = json.dumps(
        {
            "schema_version": "sec_ocf_ytd_cohort_v1",
            "metric": "operating_cash_flow_ytd",
            "tag": f"{OCF_TAXONOMY}:{OCF_TAG}",
            "collection_cutoff": collection_cutoff.isoformat(),
            "historical_investment_cutoff": None,
            "historical_availability_policy": "Not evaluated: no historical investment cutoff was supplied. SEC filing/acceptance timestamps are retained per accession; datadate and current Compustat extracts are not treated as historical availability proof.",
            "amended_and_conflicting_observations": "retained by accession as separate rows",
            "cases": cases,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8") + b"\n"
    case_dir = archive_root / "fallback_pilots" / "ocf_ytd_cohort_2026-11"
    manifest_path = _write_content_addressed(case_dir, "cohort_decisions", ".json", case_bytes)

    observation_frame = pl.DataFrame(observations) if observations else pl.DataFrame()
    parquet_tmp = case_dir / f".ocf_ytd_observations.{os.getpid()}.tmp.parquet"
    observation_frame.write_parquet(parquet_tmp, compression="zstd")
    observation_bytes = parquet_tmp.read_bytes()
    parquet_tmp.unlink()
    observations_path = _write_content_addressed(case_dir, "ocf_ytd_observations", ".parquet", observation_bytes)
    return manifest_path, observations_path, cases
