"""Policy-gated, provenance-preserving SEC OCF-YTD resolution."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

import polars as pl

from pipelines.utils.fundamentals_schema import fundamentals_table_schema

POLICY_VERSION = "sec-ocf-ytd-reviewed-v6"

RESOLVED_COLUMNS = {
    "original_oancfy": pl.Float64,
    "resolved_oancfy": pl.Float64,
    "selected_source": pl.String,
    "selection_reason": pl.String,
    "candidate_evidence_json": pl.String,
    "sec_value_raw": pl.String,
    "sec_unit": pl.String,
    "sec_currency": pl.String,
    "sec_taxonomy": pl.String,
    "sec_tag": pl.String,
    "sec_definition": pl.String,
    "sec_period_start": pl.Date,
    "sec_period_end": pl.Date,
    "sec_period_semantics": pl.String,
    "sec_fiscal_year": pl.Int32,
    "sec_fiscal_period": pl.String,
    "sec_accession": pl.String,
    "sec_form": pl.String,
    "sec_filing_time": pl.String,
    "sec_acceptance_time": pl.String,
    "sec_source_url": pl.String,
    "sec_filing_archive_path": pl.String,
    "sec_mapping_provenance": pl.String,
    "sec_mapping_snapshot_path": pl.String,
    "sec_mapping_snapshot_sha256": pl.String,
    "sec_companyfacts_sha256": pl.String,
    "sec_submissions_sha256": pl.String,
    "sec_filing_html_sha256": pl.String,
    "sec_extraction_id": pl.String,
    "sec_extraction_version": pl.String,
    "policy_version": pl.String,
    "policy_cutoff": pl.String,
    "policy_cutoff_basis": pl.String,
}


class ResolutionError(ValueError):
    """Raised when archived evidence cannot support a unique fallback."""



def parse_aware_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        raise ResolutionError("cutoff must be an ISO-8601 timezone-aware timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ResolutionError("cutoff must be an ISO-8601 timezone-aware timestamp")
    return parsed.astimezone(timezone.utc)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except (TypeError, ValueError):
        return None




def resolve_quarterly_candidates(
    quarterly: pl.DataFrame,
    candidates: list[dict[str, Any]],
    *,
    cutoff: str,
    extraction_id: str,
    policy_version: str = POLICY_VERSION,
) -> pl.DataFrame:
    """Resolve configured, pre-reviewed OCF-YTD candidates against null rows.

    Candidate discovery and candidate acceptance are intentionally separate.
    Each candidate must carry an explicit accepted decision plus verified issuer,
    reporting-entity, consolidation-scope, conflict, and filing evidence. This
    function applies decisions; it does not infer identity or choose among
    competing accessions.
    """
    cutoff_dt = parse_aware_timestamp(cutoff)
    required_columns = {"barrid", "gvkey", "datadate", "fyearq", "fqtr", "fyr", "currency", "oancfy"}
    if not required_columns.issubset(quarterly.columns):
        raise ResolutionError(f"quarterly input missing columns: {sorted(required_columns - set(quarterly.columns))}")
    if "original_oancfy" in quarterly.columns or "resolved_oancfy" in quarterly.columns:
        raise ResolutionError("input already contains resolved-layer columns")
    if not extraction_id:
        raise ResolutionError("extraction_id is required")

    defaults = {name: None for name in RESOLVED_COLUMNS}
    defaults.update({
        "original_oancfy": None, "resolved_oancfy": None,
        "selected_source": "unresolved", "selection_reason": "no accepted SEC candidate",
        "policy_version": policy_version, "policy_cutoff": cutoff_dt.isoformat(),
        "policy_cutoff_basis": "supplied comparison-policy cutoff; original collection timestamp not recorded",
    })
    frame = quarterly.with_columns(
        pl.col("oancfy").alias("original_oancfy"),
        pl.col("oancfy").alias("resolved_oancfy"),
        pl.when(pl.col("oancfy").is_not_null()).then(pl.lit("compustat"))
        .otherwise(pl.lit("unresolved")).alias("selected_source"),
        pl.when(pl.col("oancfy").is_not_null()).then(pl.lit("preserved non-null Compustat value"))
        .otherwise(pl.lit("no accepted SEC candidate")).alias("selection_reason"),
        *[pl.lit(defaults[name], dtype=dtype).alias(name) for name, dtype in RESOLVED_COLUMNS.items()
          if name not in {"original_oancfy", "resolved_oancfy", "selected_source", "selection_reason"}],
    )
    original_non_null = frame.filter(pl.col("oancfy").is_not_null()).select("oancfy").to_series().to_list()

    for candidate in candidates:
        status = candidate.get("classification") or candidate.get("decision")
        if status != "accepted":
            continue
        required = {
            "gvkey", "barrid", "period_end", "period_start", "fiscal_year",
            "fiscal_quarter", "fiscal_year_end_month", "currency", "identity_verified",
            "consolidation_scope_verified", "identity_evidence", "scope_evidence",
            "conflict_review", "taxonomy", "tag", "definition", "unit",
            "sec_value_raw", "sec_to_compustat_multiplier", "inline_value_raw",
            "accession", "form", "filing_time", "acceptance_time", "source_url",
            "filing_archive_path", "companyfacts_sha256", "submissions_sha256",
            "filing_html_sha256", "mapping_snapshot_path", "mapping_snapshot_sha256",
            "mapping_provenance", "extraction_version", "decision_reason",
        }
        missing = required - set(candidate)
        if missing:
            raise ResolutionError(f"accepted candidate missing fields: {sorted(missing)}")
        if candidate["identity_verified"] is not True or candidate["consolidation_scope_verified"] is not True:
            raise ResolutionError("accepted candidate lacks verified accounting entity or consolidation scope")
        if candidate["conflict_review"] != "no_conflict_or_verified_amendment":
            raise ResolutionError("accepted candidate lacks an affirmative accession/amendment conflict review")
        acceptance = parse_aware_timestamp(str(candidate["acceptance_time"]))
        if acceptance > cutoff_dt:
            raise ResolutionError("accepted candidate filing was after the explicit policy cutoff")
        period_end, period_start = _date(candidate["period_end"]), _date(candidate["period_start"])
        if period_end is None or period_start is None:
            raise ResolutionError("candidate period dates are invalid")
        if candidate.get("period_semantics", "fiscal_year_to_date") != "fiscal_year_to_date":
            raise ResolutionError("candidate is not explicitly a fiscal-year-to-date observation")
        try:
            raw = Decimal(str(candidate["sec_value_raw"]))
            inline = Decimal(str(candidate["inline_value_raw"]))
            multiplier = Decimal(str(candidate["sec_to_compustat_multiplier"]))
        except (InvalidOperation, TypeError, ValueError):
            raise ResolutionError("candidate value or unit conversion is invalid") from None
        if not raw.is_finite() or not multiplier.is_finite() or raw != inline:
            raise ResolutionError("candidate filing value does not corroborate the exact SEC value")
        converted = raw * multiplier
        target = (
            (pl.col("gvkey") == str(candidate["gvkey"]))
            & (pl.col("barrid") == str(candidate["barrid"]))
            & (pl.col("datadate") == period_end)
            & (pl.col("fyearq") == int(candidate["fiscal_year"]))
            & (pl.col("fqtr") == int(candidate["fiscal_quarter"]))
            & (pl.col("fyr") == int(candidate["fiscal_year_end_month"]))
            & (pl.col("currency") == candidate["currency"])
        )
        rows = frame.filter(target)
        if rows.height != 1:
            raise ResolutionError(f"candidate Compustat company-period key must identify one row; found {rows.height}")
        if rows["oancfy"][0] is not None:
            continue
        acceptance_text = acceptance.isoformat()
        evidence_json = json.dumps(candidate, sort_keys=True, separators=(",", ":"), default=str)
        patch = {
            "resolved_oancfy": float(converted), "selected_source": "sec",
            "selection_reason": str(candidate["decision_reason"]),
            "candidate_evidence_json": evidence_json, "sec_value_raw": str(raw),
            "sec_unit": str(candidate["unit"]), "sec_currency": str(candidate["currency"]),
            "sec_taxonomy": str(candidate["taxonomy"]), "sec_tag": str(candidate["tag"]),
            "sec_definition": str(candidate["definition"]), "sec_period_start": period_start,
            "sec_period_end": period_end, "sec_period_semantics": "fiscal_year_to_date",
            "sec_fiscal_year": int(candidate["fiscal_year"]),
            "sec_fiscal_period": str(candidate.get("fiscal_period", f"Q{candidate['fiscal_quarter']}")),
            "sec_accession": str(candidate["accession"]), "sec_form": str(candidate["form"]),
            "sec_filing_time": str(candidate["filing_time"]), "sec_acceptance_time": acceptance_text,
            "sec_source_url": str(candidate["source_url"]),
            "sec_filing_archive_path": str(candidate["filing_archive_path"]),
            "sec_mapping_provenance": str(candidate["mapping_provenance"]),
            "sec_mapping_snapshot_path": str(candidate["mapping_snapshot_path"]),
            "sec_mapping_snapshot_sha256": str(candidate["mapping_snapshot_sha256"]),
            "sec_companyfacts_sha256": str(candidate["companyfacts_sha256"]),
            "sec_submissions_sha256": str(candidate["submissions_sha256"]),
            "sec_filing_html_sha256": str(candidate["filing_html_sha256"]),
            "sec_extraction_id": extraction_id,
            "sec_extraction_version": str(candidate["extraction_version"]),
            "policy_version": policy_version, "policy_cutoff": cutoff_dt.isoformat(),
            "policy_cutoff_basis": "supplied comparison-policy cutoff; original collection timestamp not recorded",
        }
        frame = frame.with_columns(
            *[pl.when(target).then(pl.lit(value, dtype=RESOLVED_COLUMNS[key]))
              .otherwise(pl.col(key)).alias(key) for key, value in patch.items()]
        )
    if frame.filter(pl.col("original_oancfy").is_not_null() & (pl.col("original_oancfy") != pl.col("resolved_oancfy"))).height:
        raise ResolutionError("a non-null Compustat oancfy value changed")
    if frame.filter(pl.col("oancfy").is_not_null()).select("oancfy").to_series().to_list() != original_non_null:
        raise ResolutionError("original Compustat values or their row order changed")
    return frame


def resolved_schema() -> dict[str, pl.DataType]:
    return fundamentals_table_schema("quarterly") | RESOLVED_COLUMNS


def scope_id(input_hashes: dict[str, str], cutoff: str) -> str:
    payload = {"policy": POLICY_VERSION, "cutoff": parse_aware_timestamp(cutoff).isoformat(), "inputs": input_hashes}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def write_scoped_table(
    frame: pl.DataFrame,
    *,
    output_root: Path,
    scope: str,
    manifest: dict[str, Any],
) -> Path:
    """Create immutable, yearly Parquets and manifest; reruns verify existing outputs."""
    if not re.fullmatch(r"[0-9a-f]{64}", scope):
        raise ResolutionError("scope must be a lowercase SHA-256")
    table_name = f"fundamentals_quarterly_resolved_{scope[:16]}"
    target_dir = output_root / table_name
    output_files = {year: target_dir / f"{table_name}_{year}.parquet" for year in frame["datadate"].dt.year().unique().sort().to_list()}
    manifest_path = target_dir / "manifest.json"
    if target_dir.exists():
        if not manifest_path.is_file():
            raise ResolutionError("existing output scope has no manifest; refusing to overwrite")
        existing = json.loads(manifest_path.read_text())
        if existing.get("scope_id") != scope or existing.get("input_hashes") != manifest.get("input_hashes"):
            raise ResolutionError("existing output scope does not match this input; refusing to erase broader data")
        for year, path in output_files.items():
            expected = existing.get("output_sha256", {}).get(str(year))
            if not path.is_file() or not expected or sha256_file(path) != expected:
                raise ResolutionError(f"existing output for {year} fails immutable manifest verification")
        return target_dir
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{table_name}.", dir=target_dir.parent))
    output_hashes = {}
    try:
        for year, final_path in output_files.items():
            rows = frame.filter(pl.col("datadate").dt.year() == year)
            path = staging / final_path.name
            temp = path.with_name(path.name + f".{os.getpid()}.tmp")
            try:
                rows.write_parquet(temp)
                os.replace(temp, path)
            finally:
                temp.unlink(missing_ok=True)
            output_hashes[str(year)] = sha256_file(path)
        final_manifest = {**manifest, "scope_id": scope, "table_name": table_name, "output_sha256": output_hashes}
        staged_manifest = staging / manifest_path.name
        temp_manifest = staged_manifest.with_name(staged_manifest.name + f".{os.getpid()}.tmp")
        temp_manifest.write_text(json.dumps(final_manifest, indent=2, sort_keys=True) + "\n")
        os.replace(temp_manifest, staged_manifest)
        # A concurrent writer or changed scope must never replace existing output.
        os.rename(staging, target_dir)
    except Exception:
        # Only the private staging directory is removed; published scopes are immutable.
        import shutil
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return target_dir
