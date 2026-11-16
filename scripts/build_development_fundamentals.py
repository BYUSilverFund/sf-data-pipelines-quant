"""Validate and build the development Compustat plus reviewed SEC OCF view."""

from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sys

import polars as pl
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipelines.fundamentals_query import build_compustat_query_tables
from pipelines.fundamentals_query import FORMAT_FILTERS
from pipelines.resolved_fundamentals import ResolutionError
from pipelines.utils.enums import DatabaseName
from pipelines.utils.fundamentals_schema import fundamentals_table_keys
from pipelines.utils.tables import Database
from scripts.build_resolved_fundamentals import build as build_resolved
from scripts.build_sec_ocf_ytd_cohort import (
    discover_additional_null_gaps,
)


DEFAULT_CUTOFF = "2026-10-29T23:59:59+00:00"
DEFAULT_REVIEW = Path(__file__).resolve().parents[1] / "docs/sec_ocf_full_universe_candidate_review_v1.json"
FULL_UNIVERSE_JOB_LOG = Path(__file__).resolve().parents[1] / "logs/slurm-sec-ocf-archive-process-15268690.out"


def _full_universe_inputs(log_path: Path, *, run_id: str, mapping_path: Path, cutoff: str) -> tuple[Path, Path, list[dict]]:
    raw = log_path.read_text()
    start, end = raw.find("{\n"), raw.rfind("\n}")
    if start < 0 or end < start:
        raise ResolutionError(f"full-universe job log has no result manifest: {log_path}")
    result = json.loads(raw[start:end + 2])
    if result.get("source_run_id") != run_id:
        raise ResolutionError("full-universe source run does not match the selected Compustat run")
    keys = ("discovery_input", "coverage_manifest", "decisions_manifest", "observations_parquet")
    artifacts = {key: Path(result.get(key, "")) for key in keys}
    for key, path in artifacts.items():
        if not str(path) or not path.is_file():
            raise ResolutionError(f"full-universe {key} is missing: {path}")
    discovery = _read_json(artifacts["discovery_input"])
    coverage = _read_json(artifacts["coverage_manifest"])
    decisions = _read_json(artifacts["decisions_manifest"])
    discovery_id = result.get("discovery_id")
    if discovery_id != discovery.get("discovery_id") or discovery_id != coverage.get("discovery_id"):
        raise ResolutionError("full-universe discovery ID mismatch")
    canonical = {key: value for key, value in discovery.items() if key != "discovery_id"}
    digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
    if digest != discovery_id:
        raise ResolutionError("full-universe discovery content hash mismatch")
    mapping_hash = hashlib.sha256(mapping_path.read_bytes()).hexdigest()
    if Path(discovery.get("mapping_path", "")).resolve() != mapping_path.resolve() or discovery.get("mapping_sha256") != mapping_hash:
        raise ResolutionError("full-universe mapping path/hash mismatch")
    if discovery.get("source_run_id") != run_id or coverage.get("source_run_id") != run_id:
        raise ResolutionError("full-universe source run mismatch")
    if decisions.get("collection_cutoff") != date.fromisoformat(cutoff[:10]).isoformat():
        raise ResolutionError("full-universe decision cutoff mismatch")
    cases = decisions.get("cases", [])
    if not cases:
        raise ResolutionError("full-universe decisions contain no cases")
    for case in cases:
        if case.get("compustat_run_id") != run_id or case.get("mapping_snapshot_sha256") != mapping_hash:
            raise ResolutionError("full-universe case source run or mapping evidence hash mismatch")
    pending = coverage.get("pending_periods", [])
    discovered = coverage.get("candidate_periods")
    classified = coverage.get("complete_periods")
    pending_count = coverage.get("partial_or_missing_periods")
    if not all(isinstance(x, int) for x in (discovered, classified, pending_count)) or pending_count != len(pending):
        raise ResolutionError("full-universe coverage counts are invalid")
    _validate_full_universe_reconciliation(discovered, classified, pending, cases)
    cases = list(cases) + [{**item, "classification": "Pending incomplete evidence",
                            "decision": "unresolved", "compustat_run_id": run_id,
                            "mapping_snapshot_sha256": mapping_hash} for item in pending]
    return artifacts["decisions_manifest"], artifacts["observations_parquet"], cases


def _validate_full_universe_reconciliation(discovered: int, classified: int,
                                           pending: list[dict], cases: list[dict]) -> None:
    if discovered != classified + len(pending) or classified != len(cases):
        raise ResolutionError(
            "full-universe accounting mismatch: discovered periods must equal classified plus pending"
        )
    keys = [(str(item.get("gvkey", "")), str(item.get("datadate", ""))) for item in pending]
    if any(not all(key) for key in keys) or len(keys) != len(set(keys)):
        raise ResolutionError("full-universe pending periods must have unique company-period keys")


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read JSON at {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return value


def _apply_review_artifact(cases: list[dict], decisions_path: Path, review_path: Path,
                           *, run_id: str, cutoff: str, mapping_path: Path) -> list[dict]:
    """Verify the signed-by-content review inputs and adapt accepted reviews to resolver fields."""
    import hashlib
    from pipelines.resolved_fundamentals import parse_aware_timestamp

    review = _read_json(review_path)
    if review.get("schema_version") != "sec_ocf_candidate_review_v1":
        raise ResolutionError("unsupported candidate review artifact schema")
    if review.get("source_run_id") != run_id:
        raise ResolutionError("candidate review source run does not match full-universe run")
    if review.get("source_manifest_sha256") != hashlib.sha256(decisions_path.read_bytes()).hexdigest():
        raise ResolutionError("candidate review source manifest hash mismatch")
    cutoff_dt = parse_aware_timestamp(cutoff)
    if parse_aware_timestamp(review.get("comparison_cutoff", "")) != cutoff_dt:
        raise ResolutionError("candidate review cutoff does not match resolver cutoff")
    mapping_hash = hashlib.sha256(mapping_path.read_bytes()).hexdigest()
    index = {(str(c.get("gvkey")), str(c.get("datadate"))): c for c in cases}
    reviewed = []
    for item in review.get("cases", []):
        key = (str(item.get("gvkey")), str(item.get("datadate")))
        source = index.get(key)
        if source is None:
            raise ResolutionError(f"review period is absent from full-universe decisions: {key}")
        ev, rv = item.get("evidence", {}), item.get("review", {})
        if item.get("decision") != "accepted":
            continue
        if any(rv.get(name) is not True for name in (
            "identity_verified", "consolidation_scope_verified", "period_verified",
            "unit_scale_sign_verified", "cutoff_verified",
        )):
            raise ResolutionError(f"accepted review lacks affirmative required checks: {key}")
        if source.get("compustat_oancfy") is not None:
            raise ResolutionError(f"accepted review is no longer null in source Compustat: {key}")
        if source.get("compustat_run_id") != run_id:
            raise ResolutionError(f"review source run mismatch: {key}")
        if source.get("mapping_snapshot_sha256") != mapping_hash:
            raise ResolutionError(f"review mapping hash mismatch: {key}")
        facts_path = Path(source["companyfacts_response_path"])
        submissions_path = Path(source["submissions_response_path"])
        filing_path = Path(ev.get("filing_path", ""))
        for path, expected, label in (
            (facts_path, ev.get("companyfacts_sha256"), "Company Facts"),
            (submissions_path, ev.get("submissions_sha256"), "submissions"),
            (filing_path, ev.get("filing_sha256"), "filing HTML"),
        ):
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise ResolutionError(f"{label} evidence hash mismatch for {key}")
        if source.get("cik") != item.get("cik") or source.get("barrid") != item.get("barrid"):
            raise ResolutionError(f"review issuer/security mapping differs from source case: {key}")
        acceptance = rv.get("acceptance_time")
        if parse_aware_timestamp(acceptance) > cutoff_dt:
            raise ResolutionError(f"accepted review is after cutoff: {key}")
        raw = format(Decimal(str(rv["normalized_compustat_value"])) * Decimal(1_000_000), "f")
        observation = next((x for x in source.get("all_matching_period_observations", [])
                            if x.get("accn") == rv.get("accession")), None)
        if observation is None or float(observation.get("val")) != float(raw):
            raise ResolutionError(f"review value does not match exact Company Facts observation: {key}")
        filing = next((f for f in source.get("matching_period_filings", [])
                       if f.get("accessionNumber") == rv.get("accession")), None)
        if not filing or filing.get("acceptanceDateTime") != acceptance:
            raise ResolutionError(f"review filing acceptance does not match submissions: {key}")
        facts = ev.get("inline_facts", [])
        if not facts or not any(
            f.get("scaled_value") is not None and Decimal(str(f["scaled_value"])) == Decimal(raw)
            for f in facts
        ):
            raise ResolutionError(f"review amount lacks inline XBRL corroboration: {key}")
        period_end = item["period_end"]
        relationship = item.get("mapping_relationships", [])
        reviewed.append({
            **source, "decision": "accepted", "classification": "accepted",
            "gvkey": key[0], "barrid": item["barrid"],
            "cik": item["cik"], "period_end": period_end, "period_start": item["period_start"],
            "fiscal_year": int(item["fyearq"]), "fiscal_quarter": int(item["fqtr"]),
            "fiscal_year_end_month": int(item["fyr"]), "currency": item["compustat_currency"],
            "identity_verified": True, "consolidation_scope_verified": True,
            "identity_evidence": f"Archived Company Facts entity {source['companyfacts_entity_name']}; CIK/GVKEY/Barrid mapping verified.",
            "scope_evidence": rv["reason"],
            "conflict_review": "no_conflict_or_verified_amendment",
            "taxonomy": "us-gaap", "tag": "NetCashProvidedByUsedInOperatingActivities",
            "definition": "Net cash provided by (used in) operating activities",
            "unit": "USD", "sec_value_raw": raw, "sec_to_compustat_multiplier": "0.000001",
            "inline_value_raw": raw, "period_semantics": "fiscal_year_to_date",
            "fiscal_period": f"Q{item['fqtr']}", "accession": rv["accession"], "form": "10-Q",
            "filing_time": filing["filingDate"], "acceptance_time": acceptance,
            "source_url": ev["filing_url"], "filing_archive_path": str(filing_path),
            "companyfacts_sha256": ev["companyfacts_sha256"],
            "submissions_sha256": ev["submissions_sha256"],
            "filing_html_sha256": ev["filing_sha256"],
            "mapping_snapshot_path": str(mapping_path), "mapping_snapshot_sha256": mapping_hash,
            "mapping_provenance": json.dumps(relationship, sort_keys=True),
            "extraction_version": review["schema_version"], "decision_reason": rv["reason"],
            "review_artifact_path": str(review_path.resolve()),
        })
    accepted_count = sum(item.get("decision") == "accepted" for item in review.get("cases", []))
    if len(reviewed) != accepted_count:
        raise ResolutionError("accepted candidate reviews were not all validated")
    return cases + reviewed


def _defaults() -> tuple[Path, Path, Path, list[str], date]:
    load_dotenv(override=True)
    root = Path(os.environ["ROOT"]).expanduser().resolve()
    database_root = root / "groups/grp_quant/database"
    query_manifest_path = database_root / "compustat_query_manifest.json"
    manifest = _read_json(query_manifest_path)
    run_ids = list(manifest.get("input_run_ids", []))
    if not run_ids:
        raise ValueError(f"No input run IDs recorded in {query_manifest_path}")
    mapping_path = Path(str(manifest.get("mapping_snapshot_path", "")))
    if not mapping_path.is_file():
        mapping_path = database_root / "fundamentals/compustat/security_mappings" / mapping_path.name
    if not mapping_path.is_file():
        raise FileNotFoundError(f"Saved mapping snapshot is missing: {mapping_path}")
    mapping_date = date.fromisoformat(manifest["mapping_date"])
    return database_root, query_manifest_path, mapping_path, run_ids, mapping_date


def _validate_resolved_table(database: Database, scope: str, candidate_cases: list[dict]) -> dict:
    source = database.fundamentals_quarterly_table.read().collect()
    table = database.fundamentals_quarterly_resolved_table(scope)
    resolved = table.read().collect()
    keys = fundamentals_table_keys("quarterly")
    if resolved.height != source.height:
        raise ResolutionError(f"resolved row count changed: {source.height} -> {resolved.height}")
    if source.select(keys).unique().height != source.height:
        raise ResolutionError("source quarterly table keys are not unique")
    if resolved.select(keys).unique().height != resolved.height:
        raise ResolutionError("resolved quarterly table keys are not unique")
    if not set(source.columns).issubset(resolved.columns):
        raise ResolutionError("resolved table is missing original source columns")
    if not source.sort(keys).select(source.columns).equals(resolved.sort(keys).select(source.columns)):
        raise ResolutionError("resolved table changed original Compustat columns or values")
    non_null = source.filter(pl.col("oancfy").is_not_null()).height
    if resolved.filter(pl.col("original_oancfy").is_not_null()).height != non_null:
        raise ResolutionError("resolved non-null source oancfy count changed")
    if resolved.filter(
        pl.col("original_oancfy").is_not_null()
        & (pl.col("original_oancfy") != pl.col("resolved_oancfy"))
    ).height:
        raise ResolutionError("a non-null Compustat oancfy value was replaced")

    accepted = resolved.filter(pl.col("selected_source") == "sec")
    reviewed: set[tuple[str, str, date]] = set()
    for case in candidate_cases:
        if not (case.get("decision") == "accepted" or case.get("classification") == "Verified SEC fallback candidate"):
            continue
        period = date.fromisoformat(str(case["datadate"]))
        mapped_ids = {str(case.get("barrid"))} if case.get("barrid") else set()
        mapping_path = case.get("mapping_snapshot_path")
        if mapping_path and Path(mapping_path).is_file() and case.get("cik"):
            mapping = pl.read_parquet(mapping_path).filter(
                (pl.col("gvkey") == str(case["gvkey"]))
                & (pl.col("cik").cast(pl.String).str.zfill(10) == str(case["cik"]).zfill(10))
                & (pl.col("barra_mapping_status") == "mapped")
            )
            mapped_ids.update(str(value) for value in mapping["barrid"].drop_nulls().unique().to_list())
        reviewed.update((str(case["gvkey"]), barrid, period) for barrid in mapped_ids)
    actual = {
        (str(row["gvkey"]), str(row["barrid"]), row["datadate"])
        for row in accepted.select("gvkey", "barrid", "datadate").iter_rows(named=True)
    }
    if not actual.issubset(reviewed):
        raise ResolutionError("resolved SEC rows include a company/security/period without an accepted review")
    for row in accepted.iter_rows(named=True):
        if (row["sec_unit"], row["sec_period_semantics"], row["sec_period_end"]) != (
            "USD", "fiscal_year_to_date", row["datadate"]
        ):
            raise ResolutionError("resolved SEC row has invalid unit or YTD period provenance")
    required_provenance = (
        "candidate_evidence_json", "sec_accession", "sec_acceptance_time",
        "sec_filing_time", "sec_mapping_snapshot_path", "sec_filing_archive_path",
        "sec_companyfacts_sha256", "sec_submissions_sha256", "sec_filing_html_sha256",
        "sec_extraction_version", "selection_reason",
    )
    for row in accepted.iter_rows(named=True):
        if any(row.get(name) in (None, "") for name in required_provenance):
            raise ResolutionError("accepted SEC row is missing required provenance")
    for case in candidate_cases:
        if not case.get("barrid") or not case.get("gvkey") or not case.get("datadate"):
            # Universe-level pending records (such as incomplete SEC cache
            # coverage) have no resolved Compustat key by design.
            continue
        if case.get("compustat_oancfy") is not None:
            continue
        row = resolved.filter(
            (pl.col("gvkey") == str(case["gvkey"]))
            & (pl.col("barrid") == str(case["barrid"]))
            & (pl.col("datadate") == date.fromisoformat(str(case["datadate"])))
        )
        for case_field, column in (("fyearq", "fyearq"), ("fqtr", "fqtr"), ("fyr", "fyr"), ("currency", "currency")):
            if case.get(case_field) is not None and column in resolved.columns:
                row = row.filter(pl.col(column) == case[case_field])
        if row.height != 1:
            raise ResolutionError(
                f"candidate {case['gvkey']} {case['datadate']} does not have exactly one resolved key"
            )
        if row["selected_source"][0] == "sec":
            continue
        if row["resolved_oancfy"][0] is not None:
            raise ResolutionError(
                f"unaccepted candidate {case['gvkey']} {case['datadate']} did not remain null"
            )

    fdic_reason = "FDIC filer; separate source required."
    for gvkey in ("120318", "177009"):
        row = resolved.filter(
            (pl.col("gvkey") == gvkey)
            & (pl.col("datadate") == date(2026, 9, 30))
        )
        if row.height != 1:
            raise ResolutionError(f"expected one FDIC-filer exclusion row for {gvkey}, found {row.height}")
        # FDIC exclusion blocks SEC supplementation. It does not override an
        # existing Compustat observation, which the resolver must preserve.
        source_row = source.filter(
            (pl.col("gvkey") == gvkey) & (pl.col("datadate") == date(2026, 9, 30))
        )
        if source_row.height != 1:
            raise ResolutionError(f"expected one source FDIC-filer row for {gvkey}, found {source_row.height}")
        original_value = source_row["oancfy"][0]
        if row["resolved_oancfy"][0] != original_value:
            raise ResolutionError(f"FDIC-filer exclusion {gvkey} changed the Compustat value")
        if original_value is None and row["selection_reason"][0] != fdic_reason:
            raise ResolutionError(f"null FDIC-filer exclusion {gvkey} lacks the required label")
        if original_value is not None and row["selected_source"][0] != "compustat":
            raise ResolutionError(f"non-null FDIC-filer exclusion {gvkey} lost Compustat source precedence")

    table_path = Path(table._base_path) / table._name
    manifest_path = table_path / "manifest.json"
    manifest = _read_json(manifest_path)
    if manifest.get("scope_id") != scope:
        raise ResolutionError("Database table manifest does not match the selected scope")
    if manifest.get("accepted_sec_rows") != accepted.height:
        raise ResolutionError("resolved manifest acceptance count is inconsistent")
    return {
        "rows": resolved.height,
        "unique_keys": resolved.select(keys).unique().height,
        "original_columns": len(source.columns),
        "preserved_nonnull_oancfy": non_null,
        "accepted_sec_rows": accepted.height,
        "unresolved_oancfy_rows": resolved.filter(pl.col("resolved_oancfy").is_null()).height,
        "manifest_path": str(manifest_path),
        "resolved_table_path": str(table_path),
    }


def validate_sec_filled_keys(actual: set[tuple[str, str, date]], eligible: set[tuple[str, str, date]]) -> None:
    """Require every SEC-filled key to match reviewed, mapped and currently eligible keys."""
    if not actual.issubset(eligible):
        raise ResolutionError("SEC-supplemented rows do not match eligible reviewed decisions and mapped securities")


def _reuse_matching_query_scope(
    database: Database,
    manifest: dict,
    *,
    run_ids: list[str],
    mapping_path: Path,
    mapping_date: date,
) -> dict | None:
    """Reuse a complete query build when its source and mapping scope match."""
    if not (
        manifest.get("build_status") == "complete"
        and manifest.get("target_database") == DatabaseName.DEVELOPMENT.value
        and manifest.get("input_run_ids") == run_ids
        and manifest.get("mapping_date") == mapping_date.isoformat()
        and manifest.get("mapping_snapshot_id")
        == f"{mapping_date.isoformat()}_{mapping_path.stem.split('_')[-1]}"
        and manifest.get("extraction_version_policy") == "latest"
        and manifest.get("filters") == FORMAT_FILTERS
        and manifest.get("duplicate_key_check") == {"annual": "passed", "quarterly": "passed"}
    ):
        return None
    annual = database.fundamentals_annual_table.read().collect()
    quarterly = database.fundamentals_quarterly_table.read().collect()
    expected = manifest.get("row_counts", {})
    if annual.height != expected.get("annual") or quarterly.height != expected.get("quarterly"):
        return None
    for frequency, frame in (("annual", annual), ("quarterly", quarterly)):
        keys = fundamentals_table_keys(frequency)
        if frame.select(keys).unique().height != frame.height:
            return None
    return {
        "materialization_id": manifest["materialization_id"],
        "row_counts": {"annual": annual.height, "quarterly": quarterly.height},
        "manifest_path": str(Path(database.fundamentals_annual_table._base_path).parent / "compustat_query_manifest.json"),
        "reused_existing_scope": True,
    }


def run(*, run_ids: list[str] | None = None, mapping_snapshot: Path | None = None,
        mapping_date: date | None = None, cutoff: str = DEFAULT_CUTOFF,
        rebuild_query_scope: bool = False, refresh_sec: bool = False,
        full_universe_log: Path = FULL_UNIVERSE_JOB_LOG,
        review_artifact: Path = DEFAULT_REVIEW) -> dict:
    database_root, query_manifest_path, default_mapping, default_run_ids, default_date = _defaults()
    compustat_root = database_root / "fundamentals/compustat"
    archive_root = database_root / "fundamentals/sec_filings"
    selected_runs = run_ids or default_run_ids
    selected_mapping = mapping_snapshot or default_mapping
    selected_date = mapping_date or default_date
    if len(selected_runs) != 1:
        raise ValueError("the bounded SEC cohort currently requires exactly one completed source run")

    print("[1/5] Validating completed Compustat source collection", flush=True)
    validated_runs = []
    for run_id in selected_runs:
        run_dir = compustat_root / "source_extracts" / run_id
        metadata = _read_json(run_dir / "run.json")
        completion = _read_json(run_dir / "completion_report.json")
        coverage = _read_json(run_dir / "coverage_report.json")
        if metadata.get("run_id") != run_id or completion.get("run_id") != run_id:
            raise ValueError(f"source run metadata identity mismatch for {run_id}")
        if metadata.get("filters") != FORMAT_FILTERS:
            raise ValueError(f"source run {run_id} does not use the validated INDL/C/D/STD filters")
        if not (
            completion.get("complete") is True
            and completion.get("collector_exit_code") == 0
            and completion.get("collector_process_exited") is True
        ):
            raise ValueError(f"source run {run_id} has no successful completion report")
        if any(completion.get(name, 0) for name in (
            "missing_reports_count", "missing_artifacts_count",
            "identity_mismatches_count", "sample_value_mismatches_count",
        )):
            raise ValueError(f"source run {run_id} completion report contains failed checks")
        if not isinstance(coverage.get("frequencies"), dict):
            raise ValueError(f"source run {run_id} has no valid coverage report")
        for frequency in ("annual", "quarterly"):
            if coverage["frequencies"].get(frequency, {}).get("source_row_count") != completion["source_row_counts"].get(frequency):
                raise ValueError(f"source run {run_id} {frequency} completion and coverage counts disagree")
        validated_runs.append({
            "run_id": run_id, "run_dir": str(run_dir),
            "annual_source_rows": completion["source_row_counts"]["annual"],
            "quarterly_source_rows": completion["source_row_counts"]["quarterly"],
            "completion_status": "complete",
        })
        print(
            f"  {run_id}: annual={completion['source_row_counts']['annual']} "
            f"quarterly={completion['source_row_counts']['quarterly']} complete",
            flush=True,
        )

    print("[2/5] Building development Compustat query tables", flush=True)
    database = Database(DatabaseName.DEVELOPMENT)
    existing_query_manifest = _read_json(query_manifest_path)
    query_result = None
    if not rebuild_query_scope:
        query_result = _reuse_matching_query_scope(
            database, existing_query_manifest, run_ids=selected_runs,
            mapping_path=selected_mapping, mapping_date=selected_date,
        )
    if query_result is None:
        query_result = build_compustat_query_tables(
            compustat_root=compustat_root, run_ids=selected_runs,
            mapping_snapshot_path=selected_mapping, mapping_date=selected_date,
            version_policy="latest", target_database=DatabaseName.DEVELOPMENT,
            rebuild_scope=rebuild_query_scope,
        )
        query_result["reused_existing_scope"] = False
    print(f"  materialization={query_result['materialization_id']} row_counts={query_result['row_counts']}", flush=True)

    if refresh_sec:
        raise ResolutionError("full-universe mode is archive-only; SEC evidence refresh is disabled")
    sec_refresh = None
    print("[3/5] Loading completed full-universe SEC decisions", flush=True)
    decisions_path, observations_path, cases = _full_universe_inputs(
        full_universe_log, run_id=selected_runs[0], mapping_path=selected_mapping, cutoff=cutoff,
    )
    cases = _apply_review_artifact(
        cases, decisions_path, review_artifact, run_id=selected_runs[0],
        cutoff=cutoff, mapping_path=selected_mapping,
    )
    classifications: dict[str, int] = {}
    for case in cases:
        status = str(case.get("classification", "requiring identity review"))
        classifications[status] = classifications.get(status, 0) + 1
    print(f"  candidates={len(cases)} classifications={classifications}", flush=True)

    print("[4/5] Building policy-scoped resolved development table", flush=True)
    output_path, scope, _ = build_resolved(
        cutoff=cutoff, database_root=database_root, archive=archive_root,
        decisions_path=decisions_path, observations_path=observations_path,
        source_run_id=selected_runs[0], output_root=database_root / "development",
        full_universe_cases=cases,
    )
    print(f"  scope_id={scope} output={output_path}", flush=True)

    print("[5/5] Verifying the Database interface and resolved provenance", flush=True)
    database = Database(DatabaseName.DEVELOPMENT)
    resolved_checks = _validate_resolved_table(database, scope, cases)
    return {
        "source_runs": validated_runs,
        "query_tables": query_result,
        "candidate_decisions": str(decisions_path),
        "candidate_observations": str(observations_path),
        "candidate_review_artifact": str(review_artifact.resolve()),
        "sec_evidence_refresh": sec_refresh,
        "candidate_classifications": classifications,
        "additional_candidates": 0,
        "resolved_scope_id": scope,
        "resolved_output": str(output_path),
        "resolved_checks": resolved_checks,
        "query_manifest": str(query_manifest_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", action="append", help="Completed Compustat source run; defaults to current query manifest.")
    parser.add_argument("--mapping-snapshot", type=Path, default=None)
    parser.add_argument("--mapping-date", type=date.fromisoformat, default=None)
    parser.add_argument("--cutoff", default=DEFAULT_CUTOFF, help="Explicit timezone-aware resolution cutoff")
    parser.add_argument("--full-universe-log", type=Path, default=FULL_UNIVERSE_JOB_LOG)
    parser.add_argument("--review-artifact", type=Path, default=DEFAULT_REVIEW)
    parser.add_argument("--rebuild-query-scope", action="store_true", help="Replace derived development query tables if their scope changed.")
    parser.add_argument(
        "--refresh-sec-evidence", action="store_true",
        help="Refresh SEC evidence for the configured cohort plus up to 10 discovered null periods.",
    )
    args = parser.parse_args()
    try:
        result = run(run_ids=args.run_id, mapping_snapshot=args.mapping_snapshot,
                     mapping_date=args.mapping_date, cutoff=args.cutoff,
                     rebuild_query_scope=args.rebuild_query_scope,
                     refresh_sec=args.refresh_sec_evidence,
                     full_universe_log=args.full_universe_log,
                     review_artifact=args.review_artifact)
    except (ValueError, FileNotFoundError, ResolutionError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
