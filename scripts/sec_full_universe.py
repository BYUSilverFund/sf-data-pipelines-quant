"""Persist and reuse the validated full-universe null-gap discovery input."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import polars as pl

from scripts.build_sec_ocf_ytd_cohort import _write_content_addressed, discover_additional_null_gaps


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _original_exclusion_review(
    mapping: pl.DataFrame, quarterly: pl.DataFrame,
) -> list[dict[str, Any]]:
    """Explain the legacy GVKEY/date exclusions before fiscal-key discovery."""
    mapping_by_gvkey: dict[str, dict[str, Any]] = {}
    for group_key, links in mapping.group_by("gvkey", maintain_order=True):
        gvkey = str(group_key[0] if isinstance(group_key, tuple) else group_key)
        ciks = sorted({str(value).zfill(10) for value in links["cik"].drop_nulls().to_list()})
        mapping_by_gvkey[gvkey] = {
            "ciks": ciks,
            "mapping_statuses": sorted(str(value) for value in links["mapping_status"].drop_nulls().unique().to_list()),
            "barrids": sorted(str(value) for value in links["barrid"].drop_nulls().unique().to_list()),
            "relationships": links.select([
                column for column in (
                    "cusip9", "iid", "cik", "barrid", "issuerid", "rootid",
                    "mapping_source", "cik_source", "mapping_status",
                    "compustat_efffrom", "compustat_effthru",
                    "barra_start_date", "barra_end_date", "barra_mapping_status",
                ) if column in links.columns
            ]).unique().to_dicts(),
        }
    null_keys = quarterly.filter(pl.col("oancfy").is_null()).select("gvkey", "datadate").unique()
    reviewed: list[dict[str, Any]] = []
    fields = [column for column in ("gvkey", "datadate", "fyearq", "fqtr", "fyr", "curcdq", "currency", "rdq", "oancfy") if column in quarterly.columns]
    for key in null_keys.sort(["datadate", "gvkey"]).iter_rows(named=True):
        gvkey, datadate = str(key["gvkey"]), key["datadate"]
        rows = quarterly.filter((pl.col("gvkey") == gvkey) & (pl.col("datadate") == datadate))
        mapping_info = mapping_by_gvkey.get(
            gvkey, {"ciks": [], "mapping_statuses": [], "barrids": [], "relationships": []},
        )
        duplicate_period_rows = rows.height > 1
        unique_mapped_cik = (
            len(mapping_info["ciks"]) == 1
            and "mapped_current_cik_metadata" in mapping_info["mapping_statuses"]
        )
        if not duplicate_period_rows and unique_mapped_cik:
            continue
        reasons = []
        if duplicate_period_rows:
            reasons.append("multiple_compustat_rows_share_gvkey_datadate")
        if not unique_mapped_cik:
            reasons.append("no_unique_mapped_current_cik_metadata")
        reviewed.append({
            "gvkey": gvkey,
            "datadate": datadate.isoformat(),
            "original_exclusion_reasons": reasons,
            "compustat_rows": rows.select(fields).to_dicts(),
            "mapping": {
                **mapping_info,
                "relationships": [
                    {
                        field: value.isoformat() if hasattr(value, "isoformat") else value
                        for field, value in relationship.items()
                    }
                    for relationship in mapping_info["relationships"]
                ],
            },
        })
    return reviewed


def load_or_create_discovery(
    *, run_id: str, compustat_root: Path, archive_root: Path, mapping_path: Path,
) -> tuple[tuple[dict[str, Any], ...], Path, dict[str, Any]]:
    run_dir = compustat_root / "source_extracts" / run_id
    quarterly_path = run_dir / "quarterly.parquet"
    quarterly = pl.read_parquet(quarterly_path)
    mapping_path = mapping_path.resolve()
    mapping = pl.read_parquet(mapping_path)
    candidates = discover_additional_null_gaps(mapping, quarterly, limit=None)
    unique_null_periods = quarterly.filter(pl.col("oancfy").is_null()).select("gvkey", "datadate").unique().height
    fiscal_key_columns = ["gvkey", "datadate", "fyearq"]
    fiscal_key_columns.extend(column for column in ("fqtr", "fyr", "curcdq", "currency") if column in quarterly.columns)
    unique_null_fiscal_periods = quarterly.filter(pl.col("oancfy").is_null()).select(fiscal_key_columns).unique().height
    original_exclusions = _original_exclusion_review(mapping, quarterly)
    payload = {
        "schema_version": 2,
        "period_key_version": "gvkey_datadate_fyearq_fqtr_fyr_currency_v1",
        "source_run_id": run_id,
        "quarterly_path": str(quarterly_path),
        "quarterly_sha256": sha256_file(quarterly_path),
        "mapping_path": str(mapping_path),
        "mapping_sha256": sha256_file(mapping_path),
        "null_company_periods": unique_null_periods,
        "null_fiscal_periods": unique_null_fiscal_periods,
        "candidate_periods": len(candidates),
        "candidate_ciks": len({case["cik"] for case in candidates}),
        "unmapped_or_ambiguous_periods": unique_null_fiscal_periods - len(candidates),
        "original_excluded_company_periods": len(original_exclusions),
        "original_exclusion_review": original_exclusions,
        "cases": list(candidates),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    digest = hashlib.sha256(canonical).hexdigest()
    payload["discovery_id"] = digest
    directory = archive_root / "fallback_pilots" / "ocf_ytd_cohort_2026-11"
    path = _write_content_addressed(
        directory,
        f"full_universe_null_candidates_{digest}",
        ".json",
        (json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str) + "\n").encode(),
    )
    return candidates, path, payload
