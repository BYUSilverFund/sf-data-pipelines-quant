"""Build an immutable policy-scoped resolved quarterly view from saved artifacts."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.resolved_fundamentals import (
    MECHANICS,
    POLICY_VERSION,
    ResolutionError,
    SEC_OCF_CANDIDATE_DECISIONS,
    resolve_quarterly_candidates,
    scope_id,
    sha256_file,
    verify_mechanics_discrepancy,
    verify_wells_fargo_evidence,
    wells_fargo_accepted_candidate,
    write_scoped_table,
)
from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database


DEFAULT_ROOT = Path("/home/gunnjake/groups/grp_quant/database")
DEFAULT_ARCHIVE = DEFAULT_ROOT / "fundamentals/sec_filings"
DEFAULT_COHORT = DEFAULT_ARCHIVE / "fallback_pilots/ocf_ytd_cohort_2026-11"
DEFAULT_DECISIONS = DEFAULT_COHORT / "cohort_decisions_17225816d80fbbd8bfd274505fd24384b8625ce51799a9f527d7441e7d250785.json"


def build(*, cutoff: str, database_root: Path = DEFAULT_ROOT, archive: Path = DEFAULT_ARCHIVE,
          decisions_path: Path = DEFAULT_DECISIONS, observations_path: Path | None = None,
          source_run_id: str | None = None,
          output_root: Path | None = None,
          full_universe_cases: list[dict] | None = None) -> tuple[Path, str, pl.DataFrame]:
    manifest = json.loads(decisions_path.read_text())
    source_run_id = source_run_id or manifest.get("compustat_run_id")
    if not source_run_id:
        raise ResolutionError("decision manifest does not identify its Compustat source run")
    cases = full_universe_cases if full_universe_cases is not None else manifest.get("cases", [])
    matching = [row for row in cases if row.get("gvkey") == "008007" and row.get("datadate") == "2026-09-30"]
    if not matching or full_universe_cases is not None:
        # Full-universe decisions are intentionally generic. A missing WFC or
        # Mechanics case is normal; only complete explicit accepted reviews
        # are eligible for the underlying policy resolver.
        db = Database(DatabaseName.DEVELOPMENT)
        original = db.fundamentals_quarterly_table.read().collect()
        mapping_path = next((Path(row["mapping_snapshot_path"]) for row in cases
                             if row.get("mapping_snapshot_path")), None)
        if mapping_path is None or not mapping_path.is_file():
            raise ResolutionError("full-universe decisions lack a readable mapping snapshot")
        mapping = pl.read_parquet(mapping_path)
        required_review = {
            "decision", "gvkey", "barrid", "datadate", "period_start", "period_end",
            "fiscal_year", "fiscal_quarter", "fiscal_year_end_month", "currency",
            "identity_verified", "consolidation_scope_verified", "identity_evidence",
            "scope_evidence", "conflict_review", "taxonomy", "tag", "definition",
            "unit", "sec_value_raw", "sec_to_compustat_multiplier", "inline_value_raw",
            "accession", "form", "filing_time", "acceptance_time", "source_url",
            "filing_archive_path", "companyfacts_sha256", "submissions_sha256",
            "filing_html_sha256", "mapping_snapshot_path", "mapping_snapshot_sha256",
            "mapping_provenance", "extraction_version", "decision_reason",
        }
        eligible = []
        for case in cases:
            if case.get("decision") != "accepted" or not required_review.issubset(case):
                continue
            if case.get("identity_verified") is not True or case.get("consolidation_scope_verified") is not True:
                continue
            if case.get("conflict_review") != "no_conflict_or_verified_amendment":
                continue
            eligible.append(case)
        expanded = []
        for case in eligible:
            ids = mapping.filter(
                (pl.col("gvkey") == str(case["gvkey"]))
                & (pl.col("cik").cast(pl.String).str.zfill(10) == str(case.get("cik", "")).zfill(10))
                & (pl.col("barra_mapping_status") == "mapped")
            )["barrid"].drop_nulls().unique().to_list()
            expanded.extend({**case, "barrid": barrid} for barrid in ids)
        input_hashes = {str(decisions_path): sha256_file(decisions_path),
                        str(mapping_path): sha256_file(mapping_path),
                        "resolved_builder_contract": "full-universe-v4-reviewed-candidates"}
        for review_path in sorted({str(x["review_artifact_path"]) for x in eligible
                                   if x.get("review_artifact_path")}):
            path = Path(review_path)
            if not path.is_file():
                raise ResolutionError(f"review artifact is missing: {path}")
            input_hashes[str(path)] = sha256_file(path)
        source_path = database_root / "fundamentals/compustat/source_extracts" / source_run_id / "quarterly.parquet"
        if source_path.is_file():
            input_hashes[str(source_path)] = sha256_file(source_path)
        if observations_path and observations_path.is_file():
            input_hashes[str(observations_path)] = sha256_file(observations_path)
        scope = scope_id(input_hashes, cutoff)
        frame = resolve_quarterly_candidates(original, expanded, cutoff=cutoff, extraction_id=scope)
        keys = db.fundamentals_quarterly_table._ids
        if (frame.height != original.height
                or frame.select(keys).unique().height != frame.height
                or original.select(keys).unique().height != original.height
                or not frame.sort(keys).select(keys).equals(original.sort(keys).select(keys))):
            raise ResolutionError("generic resolved layer changed source row count or keys")
        if not original.sort(db.fundamentals_quarterly_table._ids).equals(
            frame.sort(db.fundamentals_quarterly_table._ids).select(original.columns)
        ):
            raise ResolutionError("generic resolved layer changed original Compustat columns")
        fills = frame.filter(pl.col("selected_source") == "sec")
        if fills.height and not expanded:
            raise ResolutionError("SEC fills exist without explicit accepted reviews")
        out = output_root or database_root / "development"
        path = write_scoped_table(frame, output_root=out, scope=scope, manifest={
            "schema_version": 1, "policy_version": POLICY_VERSION, "policy_cutoff": cutoff,
            "source_run_id": source_run_id, "input_hashes": input_hashes,
            "source_row_count": original.height, "resolved_row_count": frame.height,
            "accepted_company_periods": len({(x["gvkey"], x["datadate"]) for x in expanded}),
            "accepted_sec_rows": fills.height, "classified_decisions": len(cases),
            "classified_periods": sum(x.get("classification") != "Pending incomplete evidence" for x in cases),
            "incomplete_evidence_pending": sum(x.get("classification") == "Pending incomplete evidence" for x in cases),
            "excluded_or_unaccepted": len(cases) - len(eligible),
            "unresolved_oancfy_rows_after_resolution": frame.filter(pl.col("resolved_oancfy").is_null()).height,
            "mechanics_supplemented_rows": 0,
        })
        return path, scope, frame
    if len(matching) != 1:
        raise ResolutionError(f"expected one Wells Fargo decision case, found {len(matching)}")
    case = matching[0]
    facts_path = Path(case["companyfacts_response_path"])
    submissions_path = Path(case["submissions_response_path"])
    filing_evidence = case.get("filing_evidence", [])
    if len(filing_evidence) != 1:
        raise ResolutionError("expected one saved Wells Fargo filing evidence document")
    filing_path = Path(filing_evidence[0]["archive_path"])
    mapping_path = Path(case["mapping_snapshot_path"])
    for path in (facts_path, submissions_path, filing_path, mapping_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    facts_hash, submissions_hash = sha256_file(facts_path), sha256_file(submissions_path)
    filing_hash, mapping_hash = sha256_file(filing_path), sha256_file(mapping_path)
    if facts_hash != case.get("companyfacts_response_sha256"):
        raise ResolutionError("Company Facts archive checksum does not match the saved decision")
    if submissions_hash != case.get("submissions_response_sha256"):
        raise ResolutionError("submissions archive checksum does not match the saved decision")
    if filing_hash != filing_evidence[0].get("sha256"):
        raise ResolutionError("filing archive checksum does not match the saved decision")

    submissions_payload = json.loads(submissions_path.read_text())
    facts_payload = json.loads(facts_path.read_text())
    documents = {}
    recent = submissions_payload.get("filings", {}).get("recent", {})
    for index, accession in enumerate(recent.get("accessionNumber", [])):
        form = recent.get("form", [None] * len(recent["accessionNumber"]))[index]
        report_date = recent.get("reportDate", [None] * len(recent["accessionNumber"]))[index]
        if report_date != "2026-09-30" or form not in {"10-Q", "10-Q/A"}:
            continue
        if accession == case["matching_period_filings"][0]["accessionNumber"]:
            candidate_path = filing_path
        else:
            accession_key = accession.replace("-", "")
            matches = sorted(filing_path.parent.glob(f"CIK0000072971_{accession_key}_*.html"))
            if not matches:
                continue
            candidate_path = matches[0]
        documents[accession] = candidate_path.read_bytes()
    if not documents:
        raise ResolutionError("no saved matching filing document is available")
    evidence = verify_wells_fargo_evidence(
        facts_payload,
        submissions_payload,
        documents,
        cutoff=cutoff,
        companyfacts_sha256=facts_hash,
        submissions_sha256=submissions_hash,
        filing_html_sha256=filing_hash,
        mapping_snapshot_path=str(mapping_path),
        mapping_snapshot_sha256=mapping_hash,
        decision_case=case,
    )

    mechanics_case_matches = [row for row in cases if row.get("gvkey") == "018948" and row.get("datadate") == "2024-09-30"]
    if len(mechanics_case_matches) != 1:
        raise ResolutionError(f"expected one Mechanics discrepancy case, found {len(mechanics_case_matches)}")
    mechanics_case = mechanics_case_matches[0]
    mechanics_facts_path = Path(mechanics_case["companyfacts_response_path"])
    mechanics_submissions_path = Path(mechanics_case["submissions_response_path"])
    related = mechanics_case.get("related_filing_evidence", [])
    comparative_rows = [row for row in related if row.get("accession") == "0001518715-25-000181"]
    original_rows = [row for row in mechanics_case.get("filing_evidence", []) if row.get("accession") == "0001518715-24-000181"]
    if len(comparative_rows) != 1 or len(original_rows) != 1:
        raise ResolutionError("saved cohort does not contain both Mechanics filing documents")
    mechanics_original_path = Path(original_rows[0]["archive_path"])
    mechanics_comparative_path = Path(comparative_rows[0]["archive_path"])
    mechanics_hashes = {
        "companyfacts": sha256_file(mechanics_facts_path),
        "submissions": sha256_file(mechanics_submissions_path),
        "original": sha256_file(mechanics_original_path),
        "comparative": sha256_file(mechanics_comparative_path),
    }
    for actual, expected, label in (
        (mechanics_hashes["companyfacts"], mechanics_case.get("companyfacts_response_sha256"), "Mechanics Company Facts"),
        (mechanics_hashes["submissions"], mechanics_case.get("submissions_response_sha256"), "Mechanics submissions"),
        (mechanics_hashes["original"], original_rows[0].get("sha256"), "Mechanics original filing"),
        (mechanics_hashes["comparative"], comparative_rows[0].get("sha256"), "Mechanics comparative filing"),
    ):
        if actual != expected:
            raise ResolutionError(f"{label} checksum does not match the saved cohort evidence")
    mechanics_evidence = verify_mechanics_discrepancy(
        json.loads(mechanics_facts_path.read_text()),
        json.loads(mechanics_submissions_path.read_text()),
        mechanics_original_path.read_bytes(),
        mechanics_comparative_path.read_bytes(),
        cutoff=cutoff,
        companyfacts_sha256=mechanics_hashes["companyfacts"],
        submissions_sha256=mechanics_hashes["submissions"],
        original_html_sha256=mechanics_hashes["original"],
        comparative_html_sha256=mechanics_hashes["comparative"],
    )

    db = Database(DatabaseName.DEVELOPMENT)
    original = db.fundamentals_quarterly_table.read().collect()
    input_paths = sorted((database_root / "development/fundamentals_quarterly").glob("*.parquet"))
    if not input_paths:
        raise ResolutionError("no saved yearly development quarterly tables found")
    input_hashes = {str(path): sha256_file(path) for path in input_paths}
    source_run_path = database_root / "fundamentals/compustat/source_extracts" / source_run_id / "quarterly.parquet"
    input_hashes[str(source_run_path)] = sha256_file(source_run_path)
    if observations_path is None:
        observation_candidates = sorted(decisions_path.parent.glob("ocf_ytd_observations_*.parquet"))
        if not observation_candidates:
            raise ResolutionError(f"no accession observations found beside {decisions_path}")
        observations_path = observation_candidates[-1]
    for path in (decisions_path, facts_path, submissions_path, mapping_path, observations_path,
                 mechanics_facts_path, mechanics_submissions_path):
        input_hashes[str(path)] = sha256_file(path)
    for accession in documents:
        if accession == case["matching_period_filings"][0]["accessionNumber"]:
            doc_path = filing_path
        else:
            doc_path = next(filing_path.parent.glob(f"CIK0000072971_{accession.replace('-', '')}_*.html"))
        input_hashes[str(doc_path)] = sha256_file(doc_path)
    for path in (mechanics_original_path, mechanics_comparative_path):
        input_hashes[str(path)] = sha256_file(path)
    scope = scope_id(input_hashes, cutoff)
    current_wfc_review_is_eligible = case.get("classification") == "Verified SEC fallback candidate"
    accepted_candidates = []
    if current_wfc_review_is_eligible:
        mapped = pl.read_parquet(mapping_path).filter(
            (pl.col("gvkey") == str(case["gvkey"]))
            & (pl.col("cik").cast(pl.String).str.zfill(10) == str(case["cik"]).zfill(10))
            & (pl.col("barra_mapping_status") == "mapped")
        )["barrid"].drop_nulls().unique().to_list()
        if not mapped:
            raise ResolutionError("accepted SEC review has no mapped Barra securities in its saved snapshot")
        accepted_candidates = [
            {**wells_fargo_accepted_candidate(evidence), "barrid": barrid}
            for barrid in mapped
        ]
    frame = resolve_quarterly_candidates(
        original, accepted_candidates, cutoff=cutoff,
        extraction_id=scope, policy_version=POLICY_VERSION,
    )
    mechanics_decision = SEC_OCF_CANDIDATE_DECISIONS[
        f"{MECHANICS['gvkey']}|{MECHANICS['period_end'].isoformat()}"
    ]
    if mechanics_decision["classification"] != "requiring_identity_review":
        raise ResolutionError("Mechanics decision configuration must require identity review")
    mechanics_target = (
        (pl.col("gvkey") == MECHANICS["gvkey"])
        & (pl.col("barrid") == MECHANICS["barrid"])
        & (pl.col("datadate") == MECHANICS["period_end"])
        & (pl.col("fyearq") == 2024) & (pl.col("fqtr") == 3)
        & (pl.col("fyr") == 12) & (pl.col("currency") == "USD")
    )
    mechanics_rows = frame.filter(mechanics_target)
    if mechanics_rows.height != 1 or mechanics_rows["original_oancfy"][0] is not None:
        raise ResolutionError("Mechanics review decision does not identify one null source row")
    mechanics_evidence_json = json.dumps(mechanics_evidence, sort_keys=True, separators=(",", ":"))
    frame = frame.with_columns(
        pl.when(mechanics_target).then(pl.lit(mechanics_evidence["decision_reason"]))
        .otherwise(pl.col("selection_reason")).alias("selection_reason"),
        pl.when(mechanics_target).then(pl.lit(mechanics_evidence_json))
        .otherwise(pl.col("candidate_evidence_json")).alias("candidate_evidence_json"),
    )
    if frame.height != original.height:
        raise ResolutionError("resolved materialization changed original row count")
    if frame.select(db.fundamentals_quarterly_table._ids).unique().height != original.select(db.fundamentals_quarterly_table._ids).unique().height:
        raise ResolutionError("resolved materialization changed logical row keys")
    sec_rows = frame.filter(pl.col("selected_source") == "sec")
    accepted_case = next(
        row for row in cases
        if row.get("gvkey") == "008007" and row.get("datadate") == "2026-09-30"
    )
    accepted_decision = SEC_OCF_CANDIDATE_DECISIONS["008007|2026-09-30"]
    eligible_barrids = set(
        pl.read_parquet(mapping_path).filter(
            (pl.col("gvkey") == str(accepted_case["gvkey"]))
            & (pl.col("cik").cast(pl.String).str.zfill(10) == str(accepted_case["cik"]).zfill(10))
            & (pl.col("barra_mapping_status") == "mapped")
        )["barrid"].drop_nulls().unique().to_list()
    )
    expected_sec_keys = set()
    if (accepted_decision.get("classification") == "accepted"
            and accepted_case.get("classification") == "Verified SEC fallback candidate"):
        expected_sec_keys = {
            ("008007", barrid, date(2026, 9, 30))
            for barrid in eligible_barrids
            if original.filter(
                (pl.col("gvkey") == "008007") & (pl.col("barrid") == barrid)
                & (pl.col("datadate") == date(2026, 9, 30)) & pl.col("oancfy").is_null()
            ).height == 1
        }
    actual_sec_keys = {
        (row["gvkey"], row["barrid"], row["datadate"])
        for row in sec_rows.select("gvkey", "barrid", "datadate").iter_rows(named=True)
    }
    if not actual_sec_keys.issubset(expected_sec_keys):
        raise ResolutionError("SEC-supplemented rows do not match an eligible reviewed decision and mapped security")
    if expected_sec_keys and not actual_sec_keys:
        raise ResolutionError("eligible null Wells Fargo rows were not resolved")
    if frame.filter(pl.col("gvkey") == "018948").filter(pl.col("selected_source") == "sec").height:
        raise ResolutionError("Mechanics comparative must remain excluded under the saved evidence")
    if frame.filter(pl.col("original_oancfy").is_not_null() & (pl.col("original_oancfy") != pl.col("resolved_oancfy"))).height:
        raise ResolutionError("a non-null Compustat oancfy value changed")

    selected_cutoff = evidence["cutoff"].isoformat()
    target_root = output_root or database_root / "development"
    build_manifest = {
        "schema_version": 1,
        "policy_version": POLICY_VERSION,
        "policy_cutoff": selected_cutoff,
        "policy_cutoff_basis": "supplied comparison-policy cutoff; archived retrieval timestamps are not original collection time",
        "accepted_sec_rows": sec_rows.height,
        "excluded_candidate_rows": max(0, len(cases) - sec_rows.height),
        "mechanics_decision": mechanics_evidence,
        "mechanics_supplemented_rows": 0,
        "mechanics_unresolved_barrids": ["USAHMH1"],
        "unresolved_oancfy_rows_after_resolution": frame.filter(pl.col("resolved_oancfy").is_null()).height,
        "source_row_count": original.height,
        "resolved_row_count": frame.height,
        "supplemented_barra_ids": sorted(sec_rows["barrid"].unique().to_list()),
        "input_hashes": input_hashes,
        "input_scope_description": f"current development quarterly table plus archived SEC cohort evidence from {source_run_id}",
    }
    target = write_scoped_table(frame, output_root=target_root, scope=scope, manifest=build_manifest)
    return target, scope, frame


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cutoff", required=True, help="Explicit ISO-8601 timezone-aware policy cutoff")
    parser.add_argument("--database-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--decisions", type=Path, default=DEFAULT_DECISIONS)
    parser.add_argument("--observations", type=Path, default=None)
    parser.add_argument("--source-run-id", default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    args = parser.parse_args()
    try:
        target, scope, frame = build(cutoff=args.cutoff, database_root=args.database_root,
                                     archive=args.archive, decisions_path=args.decisions,
                                     observations_path=args.observations, source_run_id=args.source_run_id,
                                     output_root=args.output_root)
    except (ResolutionError, ValueError, FileNotFoundError) as exc:
        parser.error(str(exc))
    print(f"output={target}")
    print(f"scope_id={scope}")
    print(f"rows={frame.height}")
    print(frame.filter(pl.col("selected_source") == "sec").select(
        "barrid", "gvkey", "datadate", "fyearq", "fqtr", "fyr", "original_oancfy",
        "resolved_oancfy", "sec_value_raw", "sec_unit", "sec_accession", "policy_cutoff"
    ))


if __name__ == "__main__":
    main()
