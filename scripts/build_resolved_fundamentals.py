"""Build an immutable resolved quarterly table from explicitly reviewed candidates."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from pipelines.resolved_fundamentals import (
    POLICY_VERSION,
    ResolutionError,
    resolve_quarterly_candidates,
    scope_id,
    sha256_file,
    write_scoped_table,
)
from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database


def build(
    *,
    cutoff: str,
    database_root: Path,
    decisions_path: Path,
    observations_path: Path | None,
    source_run_id: str,
    output_root: Path | None,
    full_universe_cases: list[dict],
) -> tuple[Path, str, pl.DataFrame]:
    """Resolve accepted reviews only; company and period policy lives in review evidence."""
    if not full_universe_cases:
        raise ResolutionError("full-universe decision cases are required")
    db = Database(DatabaseName.DEVELOPMENT)
    original = db.fundamentals_quarterly_table.read().collect()
    mapping_path = next((Path(row["mapping_snapshot_path"]) for row in full_universe_cases
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
    for case in full_universe_cases:
        if case.get("decision") != "accepted":
            continue
        missing = required_review - set(case)
        if missing:
            raise ResolutionError(f"accepted candidate missing review fields: {sorted(missing)}")
        if case["identity_verified"] is not True or case["consolidation_scope_verified"] is not True:
            raise ResolutionError("accepted candidate lacks verified issuer or consolidation scope")
        if case["conflict_review"] != "no_conflict_or_verified_amendment":
            raise ResolutionError("accepted candidate lacks an affirmative conflict review")
        if case["mapping_snapshot_sha256"] != sha256_file(mapping_path):
            raise ResolutionError("accepted candidate mapping snapshot hash mismatch")
        eligible.append(case)

    expanded = []
    for case in eligible:
        ids = mapping.filter(
            (pl.col("gvkey") == str(case["gvkey"]))
            & (pl.col("cik").cast(pl.String).str.zfill(10) == str(case.get("cik", "")).zfill(10))
            & (pl.col("barra_mapping_status") == "mapped")
        )["barrid"].drop_nulls().unique().to_list()
        if not ids:
            raise ResolutionError(f"accepted review has no mapped Barra securities: {case['gvkey']} {case['datadate']}")
        expanded.extend({**case, "barrid": barrid} for barrid in ids)

    input_hashes = {
        str(decisions_path): sha256_file(decisions_path),
        str(mapping_path): sha256_file(mapping_path),
        "resolved_builder_contract": "full-universe-generic-v1",
    }
    for review_path in sorted({str(x["review_artifact_path"]) for x in eligible if x.get("review_artifact_path")}):
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
    frame = resolve_quarterly_candidates(original, expanded, cutoff=cutoff, extraction_id=scope,
                                         policy_version=POLICY_VERSION)
    keys = db.fundamentals_quarterly_table._ids
    if (frame.height != original.height or frame.select(keys).unique().height != frame.height
            or original.select(keys).unique().height != original.height
            or not frame.sort(keys).select(keys).equals(original.sort(keys).select(keys))):
        raise ResolutionError("resolved layer changed source row count or unique keys")
    if not original.sort(keys).equals(frame.sort(keys).select(original.columns)):
        raise ResolutionError("resolved layer changed original Compustat columns or values")
    fills = frame.filter(pl.col("selected_source") == "sec")
    expected = {(x["gvkey"], x["barrid"], date.fromisoformat(str(x["period_end"])[:10])) for x in expanded
                if original.filter((pl.col("gvkey") == str(x["gvkey"]))
                                   & (pl.col("barrid") == str(x["barrid"]))
                                   & (pl.col("datadate") == x["period_end"])
                                   & pl.col("oancfy").is_null()).height == 1}
    actual = {(x["gvkey"], x["barrid"], x["datadate"]) for x in
              fills.select("gvkey", "barrid", "datadate").iter_rows(named=True)}
    if actual != expected:
        raise ResolutionError("SEC fills do not exactly match accepted mapped null rows")
    if frame.filter(pl.col("original_oancfy").is_not_null()
                    & (pl.col("original_oancfy") != pl.col("resolved_oancfy"))).height:
        raise ResolutionError("a non-null Compustat value changed")

    output = output_root or database_root / "development"
    path = write_scoped_table(frame, output_root=output, scope=scope, manifest={
        "schema_version": 1, "policy_version": POLICY_VERSION, "policy_cutoff": cutoff,
        "source_run_id": source_run_id, "input_hashes": input_hashes,
        "source_row_count": original.height, "resolved_row_count": frame.height,
        "accepted_company_periods": len({(x["gvkey"], x["datadate"]) for x in eligible}),
        "accepted_sec_rows": fills.height, "classified_decisions": len(full_universe_cases),
        "classified_periods": sum(x.get("classification") != "Pending incomplete evidence" for x in full_universe_cases),
        "incomplete_evidence_pending": sum(x.get("classification") == "Pending incomplete evidence" for x in full_universe_cases),
        "unresolved_oancfy_rows_after_resolution": frame.filter(pl.col("resolved_oancfy").is_null()).height,
    })
    return path, scope, frame
