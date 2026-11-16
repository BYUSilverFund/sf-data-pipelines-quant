"""Classify only complete archived SEC evidence for the full null OCF-YTD universe."""

from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from scripts.build_sec_ocf_ytd_cohort import (
    _fiscal_ytd_start,
    _latest_json,
    _source_document,
    _write_content_addressed,
    _filings_for_period,
    build as build_cohort,
    collect_ocf_ytd_observations,
    sec_fetch_progress_path,
)
from scripts.sec_full_universe import load_or_create_discovery


def run(run_id: str) -> dict:
    load_dotenv(dotenv_path=Path.cwd() / ".env", override=True)
    root = Path(os.environ["ROOT"]).expanduser().resolve()
    compustat_root = root / "groups/grp_quant/database/fundamentals/compustat"
    archive_root = root / "groups/grp_quant/database/fundamentals/sec_filings"
    run_dir = compustat_root / "source_extracts" / run_id
    metadata = json.loads((run_dir / "run.json").read_text())
    completion = json.loads((run_dir / "completion_report.json").read_text())
    if metadata.get("run_id") != run_id or completion.get("run_id") != run_id:
        raise ValueError("source run metadata identity mismatch")
    if not (completion.get("complete") is True and completion.get("collector_exit_code") == 0):
        raise ValueError("source run is not complete and successful")
    mapping_path = Path(metadata["mapping_snapshot"]).resolve()
    if not mapping_path.is_file():
        raise FileNotFoundError(mapping_path)

    candidates, discovery_path, discovery = load_or_create_discovery(
        run_id=run_id, compustat_root=compustat_root, archive_root=archive_root,
        mapping_path=mapping_path,
    )
    progress_path = sec_fetch_progress_path(archive_root, candidates)
    if progress_path.exists():
        progress = json.loads(progress_path.read_text())
    else:
        progress = {"ciks": {}}
    per_cik = progress.get("ciks", {})
    grouped: dict[str, list[dict]] = {}
    for case in candidates:
        grouped.setdefault(case["cik"], []).append(case)
    evidence_status: dict[str, str] = {}
    ready_ciks = set()
    api_root = archive_root / "api_responses"
    filing_dir = archive_root / "fallback_pilots" / "ocf_ytd_cohort_2026-11" / "source_documents"
    for cik, cik_cases in grouped.items():
        if per_cik.get(cik, {}).get("status") == "complete":
            ready_ciks.add(cik)
            evidence_status[cik] = "complete_from_fetch_manifest"
            continue
        try:
            facts_path = _latest_json(api_root / f"CIK{cik}" / "companyfacts")
            submissions_path = _latest_json(api_root / f"CIK{cik}" / "submissions")
            facts = json.loads(facts_path.read_text())
            submissions = json.loads(submissions_path.read_text())
            if not isinstance(facts.get("facts"), dict) or not isinstance(submissions.get("filings"), dict):
                raise ValueError("cached SEC payload has an unexpected shape")
            required_accessions = set()
            for case in cik_cases:
                end = date.fromisoformat(case["datadate"])
                start = _fiscal_ytd_start(case, end)
                if start is None:
                    continue
                required_accessions.update(
                    str(row["accn"]) for row in collect_ocf_ytd_observations(
                        facts, period_start=start, period_end=end
                    ) if row.get("accn")
                )
                required_accessions.update(
                    str(row["accessionNumber"]) for row in _filings_for_period(submissions, end.isoformat())
                )
            if all(_source_document(filing_dir, cik, accession) is not None for accession in required_accessions):
                ready_ciks.add(cik)
                evidence_status[cik] = "complete_from_valid_cached_responses"
            else:
                evidence_status[cik] = "partial_cached_missing_filing_document"
        except (OSError, ValueError, json.JSONDecodeError):
            evidence_status[cik] = per_cik.get(cik, {}).get("status", "not_fetched")
    ready = tuple(case for case in candidates if case["cik"] in ready_ciks)
    pending = tuple(case for case in candidates if case["cik"] not in ready_ciks)

    coverage = {
        "schema_version": 1,
        "source_run_id": run_id,
        "candidate_periods": len(candidates),
        "discovery_input": str(discovery_path),
        "discovery_id": discovery["discovery_id"],
        "candidate_ciks": len({case["cik"] for case in candidates}),
        "complete_ciks": len(ready_ciks),
        "partial_or_missing_ciks": len({case["cik"] for case in pending}),
        "complete_periods": len(ready),
        "partial_or_missing_periods": len(pending),
        "coverage_complete": not pending,
        "fetch_progress_manifest": str(progress_path),
        "pending_periods": [
            {"cik": case["cik"], "gvkey": case["gvkey"], "datadate": case["datadate"],
             "evidence_status": evidence_status.get(case["cik"], "not_fetched"),
             "reason": "Evidence cache is incomplete; this is not evidence of no SEC disclosure."}
            for case in pending
        ],
    }
    case_dir = archive_root / "fallback_pilots" / "ocf_ytd_cohort_2026-11"
    coverage_bytes = json.dumps(coverage, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    coverage_path = _write_content_addressed(case_dir, "full_universe_evidence_coverage", ".json", coverage_bytes)

    decisions_path = observations_path = None
    classifications: dict[str, int] = {}
    if ready:
        decisions_path, observations_path, classified = build_cohort(
            compustat_root=compustat_root,
            archive_root=archive_root,
            run_id=run_id,
            mapping_path=mapping_path,
            cases=ready,
        )
        for case in classified:
            status = str(case.get("classification", "requiring identity review"))
            classifications[status] = classifications.get(status, 0) + 1

    result = {
        **coverage,
        "coverage_manifest": str(coverage_path),
        "classified_periods": len(ready),
        "classifications": classifications,
        "decisions_manifest": str(decisions_path) if decisions_path else None,
        "observations_parquet": str(observations_path) if observations_path else None,
        "sec_requests_made": 0,
    }
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run(args.run_id)


if __name__ == "__main__":
    main()
