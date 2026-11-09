"""Build a content-addressed, offline audit for ten mapped OCF-YTD gaps."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any

import polars as pl

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


ARCHIVE_ROOT = Path("/home/gunnjake/groups/grp_quant/database/fundamentals/sec_filings")
COMPUSTAT_ROOT = Path("/home/gunnjake/groups/grp_quant/database/fundamentals/compustat")
RUN_ID = "run_bd766d2869a3a83fce895048"
MAPPING_NAME = (
    "membership_2026-08-31_"
    "5b1f0a0bbf5450a79c5594c61416d9d096e1b82e8e1c4a1a4e5b08675cdaf132.parquet"
)
COLLECTION_CUTOFF = date(2026, 10, 29)
NO_HISTORICAL_CUTOFF = None

COHORT = (
    {"cik": "0001518715", "gvkey": "018948", "barrid": "USAHMH1", "ticker": None, "datadate": "2024-09-30", "fyearq": 2024},
    {"cik": "0001500217", "gvkey": "185549", "barrid": "USAC121", "ticker": "AAT", "datadate": "2026-09-30", "fyearq": 2026},
    {"cik": "0001557860", "gvkey": "018626", "barrid": "USBCBI1", "ticker": None, "datadate": "2024-09-30", "fyearq": 2024},
    {"cik": "0000920427", "gvkey": "062243", "barrid": "USAVIJ1", "ticker": None, "datadate": "2026-09-30", "fyearq": 2026},
    {"cik": "0000831001", "gvkey": "003243", "barrid": "USAL5N1", "ticker": None, "datadate": "2026-09-30", "fyearq": 2026},
    {"cik": "0000886982", "gvkey": "114628", "barrid": "USA2ND1", "ticker": None, "datadate": "2026-09-30", "fyearq": 2026},
    {"cik": "0000072971", "gvkey": "008007", "barrid": "USAK1N1", "ticker": None, "datadate": "2026-09-30", "fyearq": 2026},
    {"cik": "0000019617", "gvkey": "002968", "barrid": "USACTE1", "ticker": None, "datadate": "2026-09-30", "fyearq": 2026},
    {"cik": "0001649749", "gvkey": "027871", "barrid": "USBDG41", "ticker": None, "datadate": "2026-09-30", "fyearq": 2026},
    {"cik": "0001331520", "gvkey": "164633", "barrid": "USA2961", "ticker": None, "datadate": "2026-09-30", "fyearq": 2026},
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _latest_json(directory: Path) -> Path:
    files = sorted(directory.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No saved SEC response under {directory}")
    return files[-1]


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
    quarterly_hash: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    end = date.fromisoformat(case["datadate"])
    start = date(end.year, 1, 1)
    mapping_rows = mapping.filter(pl.col("gvkey") == case["gvkey"]).select(
        "cik", "barrid", "cik_source", "mapping_status"
    ).unique()
    ciks = set(mapping_rows.get_column("cik").drop_nulls().to_list())
    mapping_verified = (
        ciks == {case["cik"]}
        and case["barrid"] in mapping_rows.get_column("barrid").drop_nulls().to_list()
        and mapping_rows.filter(
            (pl.col("cik") == case["cik"])
            & (pl.col("mapping_status") == "mapped_current_cik_metadata")
        ).height > 0
    )
    comp_rows = quarterly.filter(
        (pl.col("gvkey") == case["gvkey"]) & (pl.col("datadate") == end)
    )
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
        mapping_verified=mapping_verified and comp_verified,
        entity_verified=bool(entity_verified),
        taxonomy_names=taxonomy_names,
        target_filings=target_filings,
        exact_accession_observations=exact_observations,
        all_period_observations=observations,
        inline_facts=inline_evidence,
        expected_period_start=start,
        expected_period_end=end,
        collection_cutoff=COLLECTION_CUTOFF,
    )

    if not comp_verified:
        classification, reason = UNRESOLVED, "The saved Compustat extract did not contain exactly one null oancfy row for the selected company-period."
    if mapping_verified and not entity_verified:
        classification, reason = UNRESOLVED, "The CIK or SEC entity names did not agree across mapping, submissions, and Company Facts."

    accepted_accessions = {
        filing["accessionNumber"]
        for filing in target_filings
        if date.fromisoformat(filing["filingDate"]) <= COLLECTION_CUTOFF
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
                "mapping_snapshot_path": str(mapping_path_for_result(mapping)),
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
        "company_identity_verified": bool(entity_verified),
        "mapping_cik_source": mapping_rows["cik_source"][0] if mapping_rows.height else None,
        "mapping_status": mapping_rows["mapping_status"][0] if mapping_rows.height else None,
        "mapping_snapshot_path": str(mapping_path_for_result(mapping)),
        "mapping_snapshot_sha256": mapping_hash,
        "compustat_row_count": comp_rows.height,
        "compustat_oancfy": company_row.get("oancfy"),
        "compustat_rdq": str(company_row.get("rdq")) if company_row.get("rdq") is not None else None,
        "compustat_currency": company_row.get("curcdq"),
        "compustat_extract_sha256": quarterly_hash,
        "compustat_run_id": RUN_ID,
        "fiscal_quarter": company_row.get("fqtr"),
        "fiscal_year_quarter": company_row.get("fyearq"),
        "fiscal_year_end_month": company_row.get("fyr"),
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "collection_cutoff": COLLECTION_CUTOFF.isoformat(),
        "collection_cutoff_basis": "completed Compustat run ended 2026-10-29",
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


def mapping_path_for_result(_: pl.DataFrame) -> Path:
    return COMPUSTAT_ROOT.parent / "sec_filings" / "security_mappings" / MAPPING_NAME


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


def build() -> tuple[Path, Path, list[dict[str, Any]]]:
    mapping_path = COMPUSTAT_ROOT.parent / "sec_filings" / "security_mappings" / MAPPING_NAME
    quarterly_path = COMPUSTAT_ROOT / "source_extracts" / RUN_ID / "quarterly.parquet"
    mapping = pl.read_parquet(mapping_path)
    quarterly = pl.read_parquet(quarterly_path)
    mapping_hash = _sha256(mapping_path.read_bytes())
    quarterly_hash = _sha256(quarterly_path.read_bytes())
    filing_directory = ARCHIVE_ROOT / "fallback_pilots" / "ocf_ytd_cohort_2026-11" / "source_documents"
    cases: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    api_root = ARCHIVE_ROOT / "api_responses"
    for case in COHORT:
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
            quarterly_hash=quarterly_hash,
        )
        cases.append(case_result)
        observations.extend(case_observations)

    case_bytes = json.dumps(
        {
            "schema_version": "sec_ocf_ytd_cohort_v1",
            "metric": "operating_cash_flow_ytd",
            "tag": f"{OCF_TAXONOMY}:{OCF_TAG}",
            "collection_cutoff": COLLECTION_CUTOFF.isoformat(),
            "historical_investment_cutoff": None,
            "historical_availability_policy": "Not evaluated: no historical investment cutoff was supplied. SEC filing/acceptance timestamps are retained per accession; datadate and current Compustat extracts are not treated as historical availability proof.",
            "amended_and_conflicting_observations": "retained by accession as separate rows",
            "supersedes_preliminary_mechanics_assessment": {
                "prior_manifest": str(ARCHIVE_ROOT / "fallback_pilots" / "cohort_2026-11" / "cohort_validation.json"),
                "prior_classification": "timely_compustat_source_gap_sec_standard_fact_present",
                "current_disposition": "Unresolved",
                "reason": "The expanded accession scan found a later 2025-11-17 comparative filing with +217358000 USD for the same 2024 YTD period, while the original filing reported -47042000 USD.",
            },
            "cases": cases,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8") + b"\n"
    case_dir = ARCHIVE_ROOT / "fallback_pilots" / "ocf_ytd_cohort_2026-11"
    manifest_path = _write_content_addressed(case_dir, "cohort_decisions", ".json", case_bytes)

    observation_frame = pl.DataFrame(observations) if observations else pl.DataFrame()
    parquet_tmp = case_dir / f".ocf_ytd_observations.{os.getpid()}.tmp.parquet"
    observation_frame.write_parquet(parquet_tmp, compression="zstd")
    observation_bytes = parquet_tmp.read_bytes()
    parquet_tmp.unlink()
    observations_path = _write_content_addressed(case_dir, "ocf_ytd_observations", ".parquet", observation_bytes)
    return manifest_path, observations_path, cases


def main() -> None:
    manifest_path, observations_path, cases = build()
    print(f"cases={len(cases)} observations={sum(row['all_matching_period_observation_count'] for row in cases)}")
    for case in cases:
        print(case["cik"], case["gvkey"], case["datadate"], case["classification"], case["reason"])
    print(f"manifest={manifest_path}")
    print(f"observations={observations_path}")


if __name__ == "__main__":
    main()
