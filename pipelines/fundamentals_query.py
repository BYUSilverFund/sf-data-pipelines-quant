"""Materialize read-only quant query tables from completed Compustat archives."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any

import polars as pl

from pipelines.utils.enums import DatabaseName
from pipelines.utils.fundamentals_schema import (
    ANNUAL_FUNDAMENTAL_FIELDS,
    QUARTERLY_FUNDAMENTAL_FIELDS,
    fundamentals_table_keys,
    fundamentals_table_schema,
)
from pipelines.utils.tables import Database


FORMAT_FILTERS = {"indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD"}
VALID_MAPPING_STATUSES = {"mapped_current_cik_metadata", "missing_cik"}
STATEMENT_FILES = {
    "balance_sheet": "balance_sheet.parquet",
    "income_statement": "income_statement.parquet",
    "cash_flow": "cash_flow.parquet",
    "capital_metrics": "capital_metrics.parquet",
}
METRIC_FIELDS = {
    "annual": ANNUAL_FUNDAMENTAL_FIELDS,
    "quarterly": QUARTERLY_FUNDAMENTAL_FIELDS,
}
SHARED_REPORT_FIELDS = (
    "gvkey", "datadate", "currency", "rdq",
    "indfmt", "consol", "popsrc", "datafmt",
)
SOURCE_IDENTITY_FIELDS = ("gvkey", "datadate", "indfmt", "consol", "popsrc", "datafmt")
FISCAL_IDENTITY_FIELDS = {
    "annual": ("fyear", "fyr"),
    "quarterly": ("fyearq", "fqtr", "fyr"),
}


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _date_covers(start: Any, end: Any, point: date) -> bool:
    first, last = _parse_date(start), _parse_date(end)
    return first is not None and first <= point and (last is None or point <= last)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read valid JSON metadata at {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return value


def _load_completed_run(compustat_root: Path, run_id: str) -> tuple[Path, dict, dict, dict[str, pl.DataFrame]]:
    if not run_id.startswith("run_") or "/" in run_id or "\\" in run_id:
        raise ValueError(f"Invalid Compustat run ID: {run_id!r}")
    run_dir = compustat_root / "source_extracts" / run_id
    metadata = _read_json(run_dir / "run.json")
    completion = _read_json(run_dir / "completion_report.json")
    coverage = _read_json(run_dir / "coverage_report.json")
    if metadata.get("run_id") != run_id or completion.get("run_id") != run_id:
        raise ValueError(f"Run ID metadata mismatch in {run_dir}")
    if not (
        completion.get("complete") is True
        and completion.get("collector_exit_code") == 0
        and completion.get("collector_process_exited") is True
    ):
        raise ValueError(f"Compustat run {run_id} has no successful completion status")
    if completion.get("missing_reports_count") or completion.get("missing_artifacts_count"):
        raise ValueError(f"Compustat run {run_id} has missing archived reports or artifacts")
    if completion.get("identity_mismatches_count") or completion.get("sample_value_mismatches_count"):
        raise ValueError(f"Compustat run {run_id} has report identity or sampled value mismatches")
    if not isinstance(coverage.get("frequencies"), dict):
        raise ValueError(f"Compustat run {run_id} has no valid coverage report")
    if metadata.get("filters") != FORMAT_FILTERS:
        raise ValueError(
            f"Compustat run {run_id} is not the validated INDL/C/D/STD series: "
            f"{metadata.get('filters')}"
        )

    frames: dict[str, pl.DataFrame] = {}
    duplicate_audit: dict[str, dict[str, Any]] = {}
    quarantined: dict[str, list[dict[str, Any]]] = {}
    for frequency in ("annual", "quarterly"):
        source_path = run_dir / f"{frequency}.parquet"
        if not source_path.is_file():
            raise ValueError(f"Compustat run {run_id} is missing {source_path.name}")
        frame = pl.read_parquet(source_path)
        required = {
            "gvkey", "datadate", "indfmt", "consol", "popsrc", "datafmt",
            *FISCAL_IDENTITY_FIELDS[frequency],
        }
        if not required.issubset(frame.columns):
            raise ValueError(f"{source_path} is missing columns: {sorted(required - set(frame.columns))}")
        original_count = frame.height
        identity = list(SOURCE_IDENTITY_FIELDS)
        source_identity_count = frame.select(identity).unique().height
        exact_duplicate_rows = original_count - frame.unique(maintain_order=True).height
        if frequency in completion.get("source_row_counts", {}) and completion["source_row_counts"][frequency] != original_count:
            raise ValueError(f"Compustat run {run_id} {frequency} row count differs from its completion report")
        freq_coverage = coverage["frequencies"].get(frequency, {})
        if freq_coverage.get("source_row_count") not in (None, original_count):
            raise ValueError(f"Compustat run {run_id} {frequency} row count differs from its coverage report")
        unique_count = completion.get("unique_report_identity_counts", {}).get(frequency)
        if unique_count is not None and unique_count != source_identity_count:
            raise ValueError(f"Compustat run {run_id} {frequency} unique identity count differs from its completion report")

        # The source extract is immutable. Collapse only rows identical across every
        # source column in this in-memory derived build. Distinct fiscal labels can
        # identify separate Compustat periods sharing gvkey/datadate.
        frame = frame.unique(maintain_order=True)
        period_key = [*identity, *FISCAL_IDENTITY_FIELDS[frequency]]
        missing_period = frame.filter(pl.any_horizontal([pl.col(name).is_null() for name in FISCAL_IDENTITY_FIELDS[frequency]]))
        conflict_groups = (
            frame.group_by(period_key).len().filter(pl.col("len") > 1)
            if frame.height else pl.DataFrame(schema={**{name: frame.schema[name] for name in period_key}, "len": pl.UInt32})
        )
        conflict_keys = conflict_groups.select(period_key) if conflict_groups.height else pl.DataFrame(schema={name: frame.schema[name] for name in period_key})
        quarantine_keys = set()
        for row in missing_period.select(identity).unique().to_dicts():
            quarantine_keys.add(tuple(row[name] for name in identity))
        for row in conflict_keys.to_dicts():
            quarantine_keys.add(tuple(row[name] for name in identity))
        quarantine_rows = [
            {"frequency": frequency, **{name: str(value) for name, value in zip(identity, key, strict=True)},
             "reason": "missing_fiscal_identity" if any(key_part is None for key_part in key) else "conflicting_values_for_same_fiscal_identity"}
            for key in sorted(quarantine_keys, key=str)
        ]
        if quarantine_keys:
            blocked = pl.DataFrame([{name: value for name, value in zip(identity, key, strict=True)} for key in quarantine_keys], schema={name: frame.schema[name] for name in identity})
            frame = frame.join(blocked, on=identity, how="anti")
        duplicate_audit[frequency] = {
            "source_row_count": original_count,
            "source_report_identity_count": source_identity_count,
            "duplicate_source_identity_rows": original_count - source_identity_count,
            "exact_duplicate_rows_removed_in_derived_build": exact_duplicate_rows,
            "distinct_fiscal_periods_sharing_report_identity": max(source_identity_count and (original_count - exact_duplicate_rows - source_identity_count) or 0, 0),
            "quarantined_report_identity_count": len(quarantine_keys),
        }
        quarantined[frequency] = quarantine_rows
        frames[frequency] = frame
    selected_gvkeys = {str(value) for value in metadata.get("selected_gvkeys", [])}
    if len(selected_gvkeys) != metadata.get("selected_gvkey_count"):
        raise ValueError(f"Compustat run {run_id} selected GVKEY list/count does not match")
    for frequency, frame in frames.items():
        source_gvkeys = set(frame.get_column("gvkey").cast(pl.String).unique().to_list()) if frame.height else set()
        if not source_gvkeys.issubset(selected_gvkeys):
            raise ValueError(f"Compustat run {run_id} {frequency} extract contains an unselected GVKEY")
    return run_dir, metadata, {"completion": completion, "coverage": coverage, "duplicate_audit": duplicate_audit, "quarantined_identities": quarantined}, frames


def _mapping_path_for_run(
    compustat_root: Path,
    run_metadata: dict[str, Any],
    mapping_snapshot_path: str | Path,
    mapping_date: date,
) -> tuple[Path, pl.DataFrame, str]:
    explicit_path = Path(mapping_snapshot_path).expanduser().resolve()
    run_mapping = Path(str(run_metadata.get("mapping_snapshot", ""))).expanduser()
    if not run_mapping.is_absolute():
        run_mapping = compustat_root / run_mapping
    run_mapping = run_mapping.resolve()
    if explicit_path != run_mapping:
        raise ValueError(
            "The requested mapping snapshot must be the exact snapshot recorded by the selected collection run"
        )
    try:
        explicit_path.relative_to(compustat_root.resolve() / "security_mappings")
    except ValueError as exc:
        raise ValueError("Mapping snapshots must be saved under the Compustat archive security_mappings directory") from exc
    if not explicit_path.is_file():
        raise FileNotFoundError(f"Mapping snapshot does not exist: {explicit_path}")
    mapping = pl.read_parquet(explicit_path)
    if "membership_date" not in mapping.columns:
        raise ValueError(f"Mapping snapshot has no membership_date column: {explicit_path}")
    dates = mapping.get_column("membership_date").drop_nulls().unique().to_list()
    if dates != [mapping_date]:
        raise ValueError(f"Mapping snapshot date must exactly match requested mapping_date {mapping_date}")
    if _parse_date(run_metadata.get("membership_snapshot_date")) != mapping_date:
        raise ValueError("mapping_date must match the collection run's membership snapshot date")
    canonical_rows = mapping.sort([
        column for column in ("cusip9", "gvkey", "iid", "barrid") if column in mapping.columns
    ]).to_dicts()
    digest = hashlib.sha256(json.dumps(
        canonical_rows, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")).hexdigest()
    expected_name = f"membership_{mapping_date.isoformat()}_{digest}.parquet"
    if explicit_path.name != expected_name:
        raise ValueError("Mapping snapshot filename digest does not match its saved contents")
    stem = explicit_path.stem
    snapshot_id = stem.removeprefix("membership_")
    return explicit_path, mapping, snapshot_id


def _identity_key(frequency: str, row: dict[str, Any]) -> str:
    date_value = _parse_date(row.get("datadate"))
    if date_value is None:
        raise ValueError(f"Invalid datadate in {frequency} source identity: {row}")
    return "|".join(str(value) for value in (
        frequency, row.get("gvkey"), date_value.isoformat(), row.get("indfmt"),
        row.get("consol"), row.get("popsrc"), row.get("datafmt"),
    ))


def _identity_directory(compustat_root: Path, frequency: str, row: dict[str, Any]) -> Path:
    datadate = _parse_date(row.get("datadate"))
    if datadate is None:
        raise ValueError(f"Invalid datadate in source row: {row}")
    gvkey = str(row["gvkey"])
    base = compustat_root / str(datadate.year) / f"gvkey_{gvkey}"
    expected = base / f"{frequency}_{datadate.isoformat()}_{row['indfmt']}_{row['datafmt']}"
    candidates = [expected] if expected.is_dir() else []
    if not candidates:
        candidates = sorted(path for path in base.glob(f"{frequency}_{datadate.isoformat()}*") if path.is_dir())
    matches = []
    for path in candidates:
        report_path = path / "report.json"
        if not report_path.is_file():
            continue
        report = _read_json(report_path)
        filters = report.get("format_filters") or {}
        if (
            report.get("period_frequency") == frequency
            and str(report.get("gvkey")) == gvkey
            and str(report.get("datadate"))[:10] == datadate.isoformat()
            and str(report.get("indfmt") or filters.get("indfmt")) == str(row["indfmt"])
            and str(filters.get("consol")) == str(row["consol"])
            and str(filters.get("popsrc")) == str(row["popsrc"])
            and str(report.get("datafmt") or filters.get("datafmt")) == str(row["datafmt"])
        ):
            matches.append(path)
    if len(matches) != 1:
        raise ValueError(f"Expected one archived report directory for {_identity_key(frequency, row)}; found {len(matches)}")
    return matches[0]


def _select_snapshot(
    identity_dir: Path,
    frequency: str,
    row: dict[str, Any],
    version_policy: str | dict[str, str],
) -> tuple[str, Path, dict[str, Any]]:
    key = _identity_key(frequency, row)
    if version_policy == "latest":
        pointer = _read_json(identity_dir / "latest_snapshot.json")
        snapshot_id = pointer.get("snapshot_id")
        expected_rel = pointer.get("snapshot_path")
        version_dir = identity_dir / str(expected_rel or f"snapshots/{snapshot_id}")
    elif isinstance(version_policy, dict):
        if key not in version_policy:
            raise ValueError(f"Specific extraction version policy has no version for report {key}")
        snapshot_id = version_policy[key]
        version_dir = identity_dir / "snapshots" / snapshot_id
    else:
        raise ValueError("version_policy must be 'latest' or a report-identity-to-snapshot-ID mapping")
    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise ValueError(f"No extraction snapshot ID is available for report {key}")
    if not version_dir.is_dir():
        raise FileNotFoundError(f"Selected extraction version is missing: {version_dir}")
    report = _read_json(version_dir / "report.json")
    expected = {
        "period_frequency": frequency,
        "gvkey": str(row["gvkey"]),
        "datadate": _parse_date(row["datadate"]).isoformat(),
        "indfmt": str(row["indfmt"]),
        "datafmt": str(row["datafmt"]),
        "source_snapshot_id": snapshot_id,
    }
    filters = report.get("format_filters") or {}
    if any(str(report.get(field)) != value for field, value in expected.items()):
        raise ValueError(f"Selected report metadata does not match source identity/version {key}")
    if any(str(filters.get(field)) != value for field, value in FORMAT_FILTERS.items()):
        raise ValueError(f"Selected report does not use validated INDL/C/D/STD filters: {version_dir}")
    return snapshot_id, version_dir, report


def _read_report_values(
    version_dir: Path,
    frequency: str,
    source_identity: dict[str, Any],
) -> list[tuple[dict[str, Any], dict[str, str]]]:
    datadate = _parse_date(source_identity["datadate"])
    gvkey = str(source_identity["gvkey"])
    period_fields = FISCAL_IDENTITY_FIELDS[frequency]
    rows_by_statement: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {}
    source_paths: dict[str, str] = {}
    for statement, filename in STATEMENT_FILES.items():
        path = version_dir / filename
        if not path.is_file():
            raise FileNotFoundError(f"Selected report version is missing {filename}: {version_dir}")
        frame = pl.read_parquet(path)
        required = {"gvkey", "datadate", *[field for field in ("indfmt", "consol", "popsrc", "datafmt") if field in frame.columns]}
        if not required.issubset(frame.columns):
            raise ValueError(f"Statement artifact {path} is missing identity columns")
        matched = frame.filter(
            (pl.col("gvkey").cast(pl.String) == gvkey)
            & (pl.col("datadate") == pl.lit(datadate))
        ).unique(maintain_order=True)
        if matched.height == 0:
            raise ValueError(f"No rows for {gvkey}/{datadate} in {path}")
        if not set(period_fields).issubset(matched.columns):
            raise ValueError(f"Statement artifact {path} lacks fiscal identity columns {period_fields}")
        period_rows = {}
        for row in matched.to_dicts():
            for field in ("indfmt", "consol", "popsrc", "datafmt"):
                if field in row and str(row[field]) != str(source_identity[field]):
                    raise ValueError(f"Statement artifact format identity mismatch in {path}: {field}")
            period_key = tuple(row[field] for field in period_fields)
            if any(value is None for value in period_key):
                raise ValueError(f"Missing fiscal identity in archived statement artifact {path}: {period_key}")
            if period_key in period_rows:
                raise ValueError(f"Conflicting duplicate fiscal identity in archived statement artifact {path}: {period_key}")
            period_rows[period_key] = row
        rows_by_statement[statement] = period_rows
        source_paths[f"source_{statement}_path"] = str(path)
    expected_periods = set(rows_by_statement["balance_sheet"])
    if any(set(rows) != expected_periods for rows in rows_by_statement.values()):
        raise ValueError(f"Statement artifacts have mismatched fiscal period rows for {gvkey}/{datadate}")
    result = []
    for period_key in sorted(expected_periods, key=str):
        statement_rows = {name: values[period_key] for name, values in rows_by_statement.items()}
        first_row = statement_rows["balance_sheet"]
        common_values = {field: first_row.get(field) for field in (*SHARED_REPORT_FIELDS, *period_fields) if field in first_row}
        for statement, row in statement_rows.items():
            for field in SHARED_REPORT_FIELDS:
                if field in row and common_values.get(field) != row[field]:
                    raise ValueError(f"Statement artifacts disagree on {field} for {gvkey}/{datadate}/{period_key}")
        if str(common_values.get("gvkey")) != gvkey or _parse_date(common_values.get("datadate")) != datadate:
            raise ValueError(f"Statement common columns do not match {gvkey}/{datadate}/{period_key}")
        values: dict[str, Any] = {}
        for statement, row in statement_rows.items():
            for field in METRIC_FIELDS[frequency]:
                if field in row and field in fundamentals_table_schema(frequency):
                    values[field] = row[field]
        for field in METRIC_FIELDS[frequency]:
            values.setdefault(field, None)
        values.update(common_values)
        result.append((values, source_paths))
    return result


def _collect_relationships(
    mapping: pl.DataFrame,
    mapping_date: date,
    selected_gvkeys: set[str],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    required = {
        "membership_date", "gvkey", "iid", "cusip9", "barrid", "mapping_status",
        "barra_mapping_status", "compustat_efffrom", "compustat_effthru",
        "barra_start_date", "barra_end_date", "mapping_source", "cik_source",
    }
    if not required.issubset(mapping.columns):
        raise ValueError(f"Mapping snapshot is missing columns: {sorted(required - set(mapping.columns))}")
    rows = [row for row in mapping.to_dicts() if row.get("gvkey") is not None and str(row["gvkey"]) in selected_gvkeys]
    candidates_by_barrid: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        barrid, gvkey = row.get("barrid"), row.get("gvkey")
        if (
            barrid and gvkey
            and row.get("barra_mapping_status") == "mapped"
            and _date_covers(row.get("barra_start_date"), row.get("barra_end_date"), mapping_date)
        ):
            candidates_by_barrid[str(barrid)].add(str(gvkey))
    ambiguous_barrids = {barrid: sorted(gvkeys) for barrid, gvkeys in candidates_by_barrid.items() if len(gvkeys) > 1}

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    exclusions = {
        "ambiguous_company_mappings": [],
        "missing_barra_mappings": [],
        "invalid_effective_intervals": [],
        "ambiguous_barrid_mappings": [
            {"barrid": barrid, "candidate_gvkeys": gvkeys} for barrid, gvkeys in sorted(ambiguous_barrids.items())
        ],
    }
    seen_exclusions: set[tuple[str, str, str]] = set()
    for row in rows:
        gvkey, cusip9 = str(row["gvkey"]), str(row.get("cusip9"))
        status = row.get("mapping_status")
        if status not in VALID_MAPPING_STATUSES:
            key = (cusip9, gvkey, str(status))
            if key not in seen_exclusions:
                exclusions["ambiguous_company_mappings"].append({"cusip9": cusip9, "gvkey": gvkey, "status": status})
                seen_exclusions.add(key)
            continue
        barrid = row.get("barrid")
        if not barrid or row.get("barra_mapping_status") != "mapped":
            key = (cusip9, gvkey, str(row.get("barra_mapping_status")))
            if key not in seen_exclusions:
                exclusions["missing_barra_mappings"].append({"cusip9": cusip9, "gvkey": gvkey, "status": row.get("barra_mapping_status")})
                seen_exclusions.add(key)
            continue
        if str(barrid) in ambiguous_barrids:
            continue
        if not _date_covers(row.get("compustat_efffrom"), row.get("compustat_effthru"), mapping_date) or not _date_covers(
            row.get("barra_start_date"), row.get("barra_end_date"), mapping_date
        ):
            key = (cusip9, gvkey, str(barrid))
            if key not in seen_exclusions:
                exclusions["invalid_effective_intervals"].append({"cusip9": cusip9, "gvkey": gvkey, "barrid": str(barrid)})
                seen_exclusions.add(key)
            continue
        grouped[(str(barrid), gvkey)].append(row)

    relationships: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (barrid, gvkey), rel_rows in sorted(grouped.items()):
        unique_relations = {
            (
                str(row.get("cusip9")), str(row.get("iid")),
                str(row.get("compustat_efffrom")), str(row.get("compustat_effthru")),
                str(row.get("barra_start_date")), str(row.get("barra_end_date")),
            ): row for row in rel_rows
        }
        rel_list = [unique_relations[key] for key in sorted(unique_relations)]
        comp_intervals = {(row.get("compustat_efffrom"), row.get("compustat_effthru")) for row in rel_list}
        barra_intervals = {(row.get("barra_start_date"), row.get("barra_end_date")) for row in rel_list}
        relationships[gvkey].append({
            "barrid": barrid,
            "mapping_status": ";".join(sorted({str(row.get("mapping_status")) for row in rel_list})),
            "barra_mapping_status": "mapped",
            "compustat_efffrom": next(iter(comp_intervals))[0] if len(comp_intervals) == 1 else None,
            "compustat_effthru": next(iter(comp_intervals))[1] if len(comp_intervals) == 1 else None,
            "barra_start_date": next(iter(barra_intervals))[0] if len(barra_intervals) == 1 else None,
            "barra_end_date": next(iter(barra_intervals))[1] if len(barra_intervals) == 1 else None,
            "mapping_provenance": "; ".join(sorted({
                f"Compustat={row.get('mapping_source')}; CIK={row.get('cik_source')}; "
                "Barra=effective saved CUSIP and issuer mapping" for row in rel_list
            })),
            "mapping_relationships_json": json.dumps([
                {key: row.get(key) for key in (
                    "cusip9", "iid", "compustat_efffrom", "compustat_effthru",
                    "barra_start_date", "barra_end_date", "mapping_status", "barra_mapping_status",
                )} for row in rel_list
            ], sort_keys=True, default=str),
        })
    for gvkey in relationships:
        relationships[gvkey].sort(key=lambda row: row["barrid"])
    exclusions["ambiguous_company_mapping_count"] = len(exclusions["ambiguous_company_mappings"])
    exclusions["missing_barra_mapping_count"] = len(exclusions["missing_barra_mappings"])
    exclusions["invalid_effective_interval_count"] = len(exclusions["invalid_effective_intervals"])
    exclusions["ambiguous_barrid_mapping_count"] = len(exclusions["ambiguous_barrid_mappings"])
    exclusions["companies_without_valid_barrid"] = sorted(selected_gvkeys - set(relationships))
    return relationships, exclusions


def _materialize_frames(
    compustat_root: Path,
    run_ids: list[str],
    mapping_snapshot_path: str | Path,
    mapping_date: date,
    version_policy: str | dict[str, str],
) -> tuple[dict[str, pl.DataFrame], dict[str, Any]]:
    if not run_ids or len(run_ids) != len(set(run_ids)):
        raise ValueError("run_ids must contain one or more unique completed collection IDs")
    if not isinstance(mapping_date, date):
        raise ValueError("mapping_date must be a date")
    if version_policy != "latest" and not isinstance(version_policy, dict):
        raise ValueError("version_policy must explicitly be 'latest' or a per-report snapshot ID mapping")

    run_inputs = []
    loaded_runs = []
    identities: dict[str, dict[str, dict[str, Any]]] = {"annual": {}, "quarterly": {}}
    selected_gvkeys: set[str] = set()
    for run_id in run_ids:
        run_dir, metadata, reports, frames = _load_completed_run(compustat_root, run_id)
        loaded_runs.append((run_id, run_dir, metadata, frames, reports))
        run_map_path, _, _ = _mapping_path_for_run(
            compustat_root, metadata, mapping_snapshot_path, mapping_date
        )
        selected_gvkeys.update(str(value) for value in metadata.get("selected_gvkeys", []))
        run_inputs.append({
            "run_id": run_id,
            "run_dir": str(run_dir),
            "start_date": metadata.get("start_date"),
            "end_date": metadata.get("end_date"),
            "selected_gvkey_count": metadata.get("selected_gvkey_count"),
            "coverage_report": str(run_dir / "coverage_report.json"),
            "completion_report": str(run_dir / "completion_report.json"),
            "excluded_format_counts": metadata.get("excluded_format_counts", {}),
            "mapping_snapshot": str(run_map_path),
            "completion_status": {
                "complete": reports["completion"].get("complete"),
                "collector_exit_code": reports["completion"].get("collector_exit_code"),
            },
            "duplicate_audit": reports["duplicate_audit"],
        })
        for frequency, frame in frames.items():
            accepted = frame.filter(
                pl.all_horizontal([pl.col(name).cast(pl.String) == value for name, value in FORMAT_FILTERS.items()])
            )
            for row in accepted.select("gvkey", "datadate", "indfmt", "consol", "popsrc", "datafmt").to_dicts():
                identities[frequency][_identity_key(frequency, row)] = row

    if isinstance(version_policy, dict):
        all_identities = set(identities["annual"]) | set(identities["quarterly"])
        missing_versions = sorted(all_identities - set(version_policy))
        extra_versions = sorted(set(version_policy) - all_identities)
        if missing_versions or extra_versions:
            raise ValueError(
                "Specific version policy must name exactly the selected report identities; "
                f"missing={missing_versions[:10]}, extra={extra_versions[:10]}"
            )

    mapping_path, mapping, mapping_snapshot_id = _mapping_path_for_run(
        compustat_root, _read_json(compustat_root / "source_extracts" / run_ids[0] / "run.json"),
        mapping_snapshot_path, mapping_date,
    )
    # A prior complete materialization can serve as a value cache only when it
    # used the same content-addressed mapping and report snapshot IDs. Snapshot
    # pointers and report metadata are still checked below for every identity.
    cached_rows: dict[str, dict[tuple[str, str, str], list[dict[str, Any]]]] = {
        "annual": {}, "quarterly": {}
    }
    cached_versions: dict[str, dict[str, str]] = {"annual": {}, "quarterly": {}}
    query_manifest_path = compustat_root.parent.parent / "compustat_query_manifest.json"
    try:
        prior = _read_json(query_manifest_path)
        cache_ok = (
            prior.get("build_status") == "complete"
            and prior.get("target_database") == DatabaseName.DEVELOPMENT.value
            and prior.get("mapping_snapshot_id") == mapping_snapshot_id
            and prior.get("mapping_date") == mapping_date.isoformat()
            and prior.get("filters") == FORMAT_FILTERS
            and prior.get("extraction_version_policy") == "latest"
            and prior.get("duplicate_key_check") == {"annual": "passed", "quarterly": "passed"}
        )
        if cache_ok:
            prior_db = Database(DatabaseName.DEVELOPMENT)
            for frequency, table in (
                ("annual", prior_db.fundamentals_annual_table),
                ("quarterly", prior_db.fundamentals_quarterly_table),
            ):
                prior_frame = table.read().collect()
                if prior_frame.height != prior.get("row_counts", {}).get(frequency):
                    cache_ok = False
                    break
                keys = fundamentals_table_keys(frequency)
                if prior_frame.select(keys).unique().height != prior_frame.height:
                    cache_ok = False
                    break
                version_map = prior.get("selected_report_versions", {}).get(frequency, {})
                cached_versions[frequency] = version_map
                for row in prior_frame.to_dicts():
                    cache_key = (
                        str(row["gvkey"]), str(row["datadate"]), str(row["source_snapshot_id"])
                    )
                    cached_rows[frequency].setdefault(cache_key, []).append(row)
        if not cache_ok:
            cached_rows = {"annual": {}, "quarterly": {}}
            cached_versions = {"annual": {}, "quarterly": {}}
    except (FileNotFoundError, ValueError, KeyError, pl.exceptions.PolarsError):
        cached_rows = {"annual": {}, "quarterly": {}}
        cached_versions = {"annual": {}, "quarterly": {}}
    relationships, mapping_exclusions = _collect_relationships(mapping, mapping_date, selected_gvkeys)
    all_rows: dict[str, list[dict[str, Any]]] = {"annual": [], "quarterly": []}
    selected_snapshot_ids: dict[str, set[str]] = {"annual": set(), "quarterly": set()}
    selected_report_versions: dict[str, dict[str, str]] = {"annual": {}, "quarterly": {}}
    row_exclusions: list[dict[str, Any]] = []
    format_exclusions = {
        frequency: {
            "source_rows_excluded": 0,
            "reported_by_collection": [
                {"run_id": item[0], "counts": item[2].get("excluded_format_counts", {}).get(frequency, [])}
                for item in loaded_runs
            ],
        }
        for frequency in ("annual", "quarterly")
    }
    input_source_counts = {}
    processed_identities: dict[str, set[str]] = {"annual": set(), "quarterly": set()}
    report_value_cache_hits = {"annual": 0, "quarterly": 0}
    report_value_reads = {"annual": 0, "quarterly": 0}
    duplicate_audits = {item[0]: item[4]["duplicate_audit"] for item in loaded_runs}
    quarantined_identities = [
        {"run_id": item[0], **quarantine}
        for item in loaded_runs
        for frequency_items in item[4]["quarantined_identities"].values()
        for quarantine in frequency_items
    ]

    for run_id, run_dir, _metadata, run_frames, reports in loaded_runs:
        for frequency in ("annual", "quarterly"):
            source = run_frames[frequency]
            input_source_counts.setdefault(frequency, 0)
            input_source_counts[frequency] += reports["duplicate_audit"][frequency]["source_row_count"]
            exact = source.filter(
                pl.all_horizontal([pl.col(name).cast(pl.String) == value for name, value in FORMAT_FILTERS.items()])
            )
            format_exclusions[frequency]["source_rows_excluded"] += source.height - exact.height
            for row in exact.select("gvkey", "datadate", "indfmt", "consol", "popsrc", "datafmt").to_dicts():
                key = _identity_key(frequency, row)
                # Multiple completed runs may contain the same report identity; emit it once.
                if key in processed_identities[frequency]:
                    continue
                processed_identities[frequency].add(key)
                identity_dir = _identity_directory(compustat_root, frequency, row)
                snapshot_id, version_dir, report = _select_snapshot(identity_dir, frequency, row, version_policy)
                selected_snapshot_ids[frequency].add(snapshot_id)
                selected_report_versions[frequency][key] = snapshot_id
                if cached_versions[frequency].get(key) == snapshot_id:
                    date_key = str(row["datadate"])
                    reusable = cached_rows[frequency].get((str(row["gvkey"]), date_key, snapshot_id), [])
                    if reusable:
                        report_years = report.get("fiscal_year")
                        report_years = report_years if isinstance(report_years, list) else [report_years]
                        cached_years = sorted({int(item["fyear"] if frequency == "annual" else item["fyearq"]) for item in reusable})
                        if sorted({int(value) for value in report_years if value is not None}) != cached_years:
                            raise ValueError(f"Cached query values disagree with report fiscal years for {key}")
                        if frequency == "quarterly":
                            report_quarters = report.get("fiscal_quarter")
                            report_quarters = report_quarters if isinstance(report_quarters, list) else [report_quarters]
                            cached_quarters = sorted({int(item["fqtr"]) for item in reusable})
                            if sorted({int(value) for value in report_quarters if value is not None}) != cached_quarters:
                                raise ValueError(f"Cached query values disagree with report fiscal quarters for {key}")
                        all_rows[frequency].extend({**item, "_identity_key": key} for item in reusable)
                        report_value_cache_hits[frequency] += 1
                        continue
                report_value_reads[frequency] += 1
                report_period_rows = _read_report_values(version_dir, frequency, row)
                year_field = "fyear" if frequency == "annual" else "fyearq"
                observed_years = sorted({int(values[year_field]) for values, _ in report_period_rows if values.get(year_field) is not None})
                metadata_years = report.get("fiscal_year")
                metadata_years = metadata_years if isinstance(metadata_years, list) else [metadata_years]
                try:
                    metadata_years = sorted({int(value) for value in metadata_years if value is not None})
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"Invalid fiscal_year metadata for selected report version {key}") from exc
                if metadata_years != observed_years:
                    raise ValueError(f"Report metadata and selected statement values disagree on fiscal years for {key}")
                if frequency == "quarterly":
                    observed_quarters = sorted({int(values["fqtr"]) for values, _ in report_period_rows if values.get("fqtr") is not None})
                    metadata_quarters = report.get("fiscal_quarter")
                    metadata_quarters = metadata_quarters if isinstance(metadata_quarters, list) else [metadata_quarters]
                    try:
                        metadata_quarters = sorted({int(value) for value in metadata_quarters if value is not None})
                    except (TypeError, ValueError) as exc:
                        raise ValueError(f"Invalid fiscal_quarter metadata for selected report version {key}") from exc
                    if metadata_quarters != observed_quarters:
                        raise ValueError(f"Report metadata and selected statement values disagree on fiscal quarters for {key}")
                gvkey = str(row["gvkey"])
                gvkey_relationships = relationships.get(gvkey, [])
                if not gvkey_relationships:
                    row_exclusions.append({"frequency": frequency, "gvkey": gvkey, "datadate": str(row["datadate"]), "reason": "no_valid_barrid_mapping"})
                    continue
                for values, source_paths in report_period_rows:
                    for relationship in gvkey_relationships:
                        query_row = {
                            **values,
                            "barrid": relationship["barrid"],
                            "gvkey": gvkey,
                            "mapping_status": relationship["mapping_status"],
                            "barra_mapping_status": relationship["barra_mapping_status"],
                            "mapping_date": mapping_date,
                            "mapping_snapshot_id": mapping_snapshot_id,
                            "mapping_snapshot_path": str(mapping_path),
                            "mapping_provenance": relationship["mapping_provenance"],
                            "mapping_relationships_json": relationship["mapping_relationships_json"],
                            "compustat_efffrom": relationship["compustat_efffrom"],
                            "compustat_effthru": relationship["compustat_effthru"],
                            "barra_start_date": relationship["barra_start_date"],
                            "barra_end_date": relationship["barra_end_date"],
                            "source_snapshot_id": snapshot_id,
                            "source_report_path": str(version_dir / "report.json"),
                            **source_paths,
                            "source_field_definitions_json": json.dumps(report.get("source_field_definitions", {}), sort_keys=True),
                            "_identity_key": key,
                        }
                        all_rows[frequency].append(query_row)

    frames = {
        frequency: pl.DataFrame(
            [{key: value for key, value in row.items() if key != "_identity_key"} for row in rows],
            schema=fundamentals_table_schema(frequency),
            orient="row",
            strict=False,
        ).sort(["barrid", "datadate", "gvkey", "source_snapshot_id"])
        for frequency, rows in all_rows.items()
    }
    for frequency, frame in frames.items():
        keys = fundamentals_table_keys(frequency)
        if frame.height != frame.select(keys).unique().height:
            raise ValueError(f"Materialized {frequency} query data contains duplicate logical keys")

    policy_payload = version_policy if isinstance(version_policy, str) else dict(sorted(version_policy.items()))
    identity_payload = {
        "run_ids": sorted(run_ids),
        "mapping_date": mapping_date.isoformat(),
        "mapping_snapshot_id": mapping_snapshot_id,
        "mapping_snapshot_path": str(mapping_path),
        "version_policy": policy_payload,
        "selected_report_versions": {
            frequency: dict(sorted(report_versions.items()))
            for frequency, report_versions in selected_report_versions.items()
        },
        "selected_source_snapshot_ids": {
            frequency: sorted(snapshot_ids) for frequency, snapshot_ids in selected_snapshot_ids.items()
        },
        "filters": FORMAT_FILTERS,
        "report_identity_fields": ["period_frequency", "gvkey", "datadate", "indfmt", "consol", "popsrc", "datafmt"],
        "query_key_fields": {frequency: fundamentals_table_keys(frequency) for frequency in frames},
    }
    materialization_id = hashlib.sha256(
        json.dumps(identity_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    for frequency, frame in frames.items():
        frames[frequency] = frame.with_columns(pl.lit(materialization_id).alias("materialization_id"))
    manifest = {
        "schema_version": 1,
        "materialization_id": materialization_id,
        "target_database": None,
        "input_runs": run_inputs,
        "input_run_ids": sorted(run_ids),
        "mapping_date": mapping_date.isoformat(),
        "mapping_snapshot_id": mapping_snapshot_id,
        "mapping_snapshot_path": str(mapping_path),
        "extraction_version_policy": policy_payload,
        "filters": FORMAT_FILTERS,
        "partitioning": "calendar year of datadate; fiscal year fields are retained separately",
        "query_key_fields": {frequency: fundamentals_table_keys(frequency) for frequency in frames},
        "source_row_counts": input_source_counts,
        "duplicate_resolution": duplicate_audits,
        "quarantined_report_identities": quarantined_identities,
        "quarantined_report_identity_count": len(quarantined_identities),
        "build_status": "partial" if quarantined_identities else "complete",
        "row_counts": {frequency: frame.height for frequency, frame in frames.items()},
        "year_partitions": {
            frequency: sorted({str(year) for year in frame.get_column("datadate").dt.year().unique().to_list()})
            for frequency, frame in frames.items()
        },
        "format_exclusions": format_exclusions,
        "report_value_cache": {
            "reused_from_verified_query_scope": report_value_cache_hits,
            "read_from_archived_statement_parquets": report_value_reads,
        },
        "selected_source_snapshot_ids": {
            frequency: sorted(snapshot_ids) for frequency, snapshot_ids in selected_snapshot_ids.items()
        },
        "selected_report_versions": {
            frequency: dict(sorted(report_versions.items()))
            for frequency, report_versions in selected_report_versions.items()
        },
        "mapping_exclusions": mapping_exclusions,
        "report_rows_without_valid_barrid": row_exclusions,
        "duplicate_key_check": {frequency: "passed" for frequency in frames},
        "point_in_time_vintage": False,
        "created_at_policy": "written_at_publish_time",
    }
    return frames, {**manifest, "_identity_payload": identity_payload}


def _yearly_paths(table, frame: pl.DataFrame) -> dict[int, Path]:
    if frame.is_empty():
        return {}
    years = frame.get_column("datadate").dt.year().unique().sort().to_list()
    return {int(year): Path(table._file_path(int(year))) for year in years}


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")
    os.replace(temp, path)


def _publish(
    database: Database,
    frames: dict[str, pl.DataFrame],
    manifest: dict[str, Any],
    rebuild_scope: bool,
) -> dict[str, Any]:
    annual_table = database.fundamentals_annual_table
    quarterly_table = database.fundamentals_quarterly_table
    tables = {"annual": annual_table, "quarterly": quarterly_table}
    manifest_path = Path(annual_table._base_path).parent / "compustat_query_manifest.json"
    previous = _read_json(manifest_path) if manifest_path.exists() else None
    if previous and previous.get("materialization_id") != manifest["materialization_id"] and not rebuild_scope:
        raise ValueError(
            "Existing query tables belong to a different run/mapping/version scope. "
            "Pass rebuild_scope=True to replace the complete derived-table scope."
        )
    if previous is None:
        existing = [
            path for table in tables.values()
            for path in Path(table._base_path).glob(f"{table._name}_*.parquet")
        ]
        if existing and not rebuild_scope:
            raise ValueError("Query parquet files exist without a manifest; refuse an unscoped overwrite")

    file_plans: dict[Path, pl.DataFrame] = {}
    for frequency, table in tables.items():
        frame = frames[frequency]
        for year, path in _yearly_paths(table, frame).items():
            file_plans[path] = frame.filter(pl.col("datadate").dt.year() == year)

    staged: dict[Path, Path] = {}
    backups: dict[Path, Path] = {}
    created_paths: set[Path] = set()
    stale_paths: list[Path] = []
    temp_root = Path(tempfile.mkdtemp(prefix=".compustat-query-stage-", dir=Path(annual_table._base_path).parent))
    try:
        for index, (destination, frame) in enumerate(sorted(file_plans.items(), key=lambda item: str(item[0]))):
            temp_path = temp_root / f"new_{index}.parquet"
            frame.write_parquet(temp_path)
            staged[destination] = temp_path
        # Stage copies of old files first so ordinary write failures can be rolled back.
        old_files = [
            path for table in tables.values()
            for path in Path(table._base_path).glob(f"{table._name}_*.parquet")
        ]
        expected = set(file_plans)
        stale_paths = [path for path in old_files if path not in expected]
        for index, path in enumerate(old_files):
            backup = temp_root / f"old_{index}.parquet"
            shutil.copyfile(path, backup)
            backups[path] = backup
        manifest["target_database"] = database.database_name.value
        manifest["materialized_at"] = datetime.now(timezone.utc).isoformat()
        manifest["table_paths"] = {frequency: str(table._base_path) for frequency, table in tables.items()}
        manifest["yearly_files"] = {str(path): pl.read_parquet_schema(staged_path).names() for path, staged_path in staged.items()}
        for destination, staged_path in staged.items():
            if not destination.exists():
                created_paths.add(destination)
            os.replace(staged_path, destination)
        for stale in stale_paths:
            stale.unlink()
        _atomic_json(manifest_path, manifest)
    except Exception:
        for path, backup in backups.items():
            if backup.exists():
                restore = path.with_name(f".{path.name}.{os.getpid()}.restore")
                shutil.copyfile(backup, restore)
                os.replace(restore, path)
        for path in created_paths:
            path.unlink(missing_ok=True)
        raise
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return {
        "manifest_path": str(manifest_path),
        "annual_paths": [str(path) for path in sorted(_yearly_paths(annual_table, frames["annual"]).values())],
        "quarterly_paths": [str(path) for path in sorted(_yearly_paths(quarterly_table, frames["quarterly"]).values())],
        "row_counts": manifest["row_counts"],
        "materialization_id": manifest["materialization_id"],
    }


def build_compustat_query_tables(
    compustat_root: str | Path,
    run_ids: list[str],
    mapping_snapshot_path: str | Path,
    mapping_date: date,
    version_policy: str | dict[str, str],
    target_database: DatabaseName = DatabaseName.DEVELOPMENT,
    rebuild_scope: bool = False,
) -> dict[str, Any]:
    """Build annual and quarterly derived tables from completed local archives.

    No collection or network access occurs. The selected run must have a
    successful completion report. ``version_policy`` is either ``"latest"``
    or a mapping from the report identity key to one exact extraction ID.
    """
    if not isinstance(target_database, DatabaseName) or target_database not in {
        DatabaseName.DEVELOPMENT, DatabaseName.RESEARCH, DatabaseName.PRODUCTION,
    }:
        raise ValueError("target_database must be development, research, or production")
    frames, manifest = _materialize_frames(
        Path(compustat_root).expanduser().resolve(),
        run_ids,
        mapping_snapshot_path,
        mapping_date,
        version_policy,
    )
    manifest.pop("_identity_payload", None)
    database = Database(target_database)
    return _publish(database, frames, manifest, rebuild_scope)

