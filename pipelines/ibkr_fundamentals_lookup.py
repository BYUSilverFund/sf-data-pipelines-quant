"""Look up archived Compustat metrics by Barrid.

The historical module name is retained for compatibility with callers.
This module does not connect to IBKR.
"""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from typing import Any

import polars as pl


def _report_format(report: dict[str, Any]) -> tuple[str | None, str | None]:
    filters = report.get("format_filters") or {}
    if not isinstance(filters, dict):
        filters = {}
    indfmt = report.get("indfmt") or filters.get("indfmt")
    datafmt = report.get("datafmt") or filters.get("datafmt")
    return (
        str(indfmt).upper() if indfmt is not None else None,
        str(datafmt).upper() if datafmt is not None else None,
    )


def _report_period_directories(
    root: Path,
    gvkey: str,
    frequencies: tuple[str, ...],
) -> list[tuple[Path, dict[str, Any]]]:
    """Find report periods, preferring format-keyed paths over legacy paths."""
    grouped: dict[tuple[str | None, str | None, str | None, str | None], list[tuple[Path, dict[str, Any]]]] = {}
    for period_dir in sorted(root.glob(f"20??/gvkey_{gvkey}/*")):
        if not period_dir.is_dir():
            continue
        report_path = period_dir / "report.json"
        if not report_path.is_file():
            continue
        try:
            report = json.loads(report_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        frequency = report.get("period_frequency")
        if frequency is None:
            frequency = next((name for name in ("annual", "quarterly") if period_dir.name.startswith(f"{name}_")), None)
        if frequency not in frequencies:
            continue
        indfmt, datafmt = _report_format(report)
        key = (frequency, str(report.get("datadate")), indfmt, datafmt)
        grouped.setdefault(key, []).append((period_dir, report))

    selected: list[tuple[Path, dict[str, Any]]] = []
    for (_, _, indfmt, datafmt), paths in grouped.items():
        suffix = f"_{indfmt}_{datafmt}" if indfmt and datafmt else None
        format_keyed = [item for item in paths if suffix and item[0].name.endswith(suffix)]
        selected.extend(format_keyed or paths)
    return sorted(selected, key=lambda item: str(item[0]))


STATEMENT_FILES = {
    "balance_sheet": "balance_sheet.parquet",
    "income_statement": "income_statement.parquet",
    "cash_flow": "cash_flow.parquet",
    "capital_metrics": "capital_metrics.parquet",
}


def _mapping_snapshot_for_date(
    compustat_root: Path,
    mapping_date: date,
    mapping_snapshot: str | Path | pl.DataFrame | None,
) -> tuple[pl.DataFrame | None, str | None, list[str]]:
    if mapping_snapshot is None:
        candidates = sorted((compustat_root / "security_mappings").glob(
            f"membership_{mapping_date.isoformat()}_*.parquet"
        ))
        if len(candidates) != 1:
            return None, None, [str(path) for path in candidates]
        path = candidates[0]
        frame = pl.read_parquet(path)
        snapshot_path = str(path)
    elif isinstance(mapping_snapshot, pl.DataFrame):
        frame = mapping_snapshot
        snapshot_path = "caller-supplied DataFrame"
    else:
        path = Path(mapping_snapshot)
        frame = pl.read_parquet(path)
        snapshot_path = str(path)

    if "membership_date" not in frame.columns:
        return None, snapshot_path, []
    dates = frame.get_column("membership_date").drop_nulls().unique().to_list()
    if len(dates) != 1 or dates[0] != mapping_date:
        return None, snapshot_path, []
    return frame, snapshot_path, []


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _date_interval_contains(row: dict[str, Any], start_field: str, end_field: str, point: date) -> bool | None:
    start = _as_date(row.get(start_field))
    end = _as_date(row.get(end_field))
    if start is None:
        return None
    return start <= point and (end is None or point <= end)


def _period_candidates(
    root: Path,
    gvkey: str,
    fiscal_year: int,
    frequency: str,
    fiscal_quarter: int | None,
    indfmt: str | None = None,
    datafmt: str | None = None,
) -> list[tuple[Path, dict[str, Any]]]:
    candidates: list[tuple[Path, dict[str, Any]]] = []
    for period_dir, report in _report_period_directories(root, gvkey, (frequency,)):
        report_indfmt, report_datafmt = _report_format(report)
        if indfmt is not None and report_indfmt != indfmt:
            continue
        if datafmt is not None and report_datafmt != datafmt:
            continue
        years = report.get("fiscal_year")
        years = years if isinstance(years, list) else [years]
        if fiscal_year not in years:
            continue
        if frequency == "quarterly":
            quarters = report.get("fiscal_quarter")
            quarters = quarters if isinstance(quarters, list) else [quarters]
            if fiscal_quarter not in quarters:
                continue
        candidates.append((period_dir, report))
    return candidates


def _version_candidates(period_dir: Path, report: dict[str, Any]) -> tuple[list[str], str | None]:
    snapshots_dir = period_dir / "snapshots"
    snapshots = sorted(path.name for path in snapshots_dir.iterdir() if path.is_dir()) if snapshots_dir.is_dir() else []
    if snapshots:
        latest_id = None
        latest_path = period_dir / "latest_snapshot.json"
        if latest_path.is_file():
            try:
                latest_id = json.loads(latest_path.read_text()).get("snapshot_id")
            except (OSError, json.JSONDecodeError):
                latest_id = None
        return snapshots, latest_id
    report_id = report.get("source_snapshot_id")
    return ([str(report_id)] if report_id else []), str(report_id) if report_id else None


def lookup_compustat_metric_by_barrid(
    compustat_root: str | Path,
    barrid: str,
    mapping_date: date,
    fiscal_year: int,
    frequency: str,
    metric: str,
    statement: str,
    fiscal_quarter: int | None = None,
    information_available_as_of: date | None = None,
    mapping_snapshot: str | Path | pl.DataFrame | None = None,
    source_snapshot_id: str | None = None,
    indfmt: str | None = None,
    datafmt: str | None = None,
) -> dict[str, Any]:
    """Look up one source Compustat value by Barrid and fiscal period.

    ``mapping_date`` is mandatory and selects a saved membership mapping
    snapshot. It is separate from the report's fiscal period and the optional
    backtest information date. ``source_snapshot_id`` may be an exact saved
    version ID or the explicit string ``"latest"``; omission never picks among
    multiple extraction versions.

    This is a local, read-only lookup. It never starts a collection or makes an
    external service call.
    """
    if not barrid or not barrid.strip():
        raise ValueError("barrid must be a non-empty string")
    if not isinstance(mapping_date, date):
        raise ValueError("mapping_date is required and must be a date")
    if not isinstance(fiscal_year, int):
        raise ValueError("fiscal_year must be an integer")
    if frequency not in {"annual", "quarterly"}:
        raise ValueError("frequency must be annual or quarterly")
    if frequency == "quarterly" and fiscal_quarter not in {1, 2, 3, 4}:
        raise ValueError("quarterly lookups require fiscal_quarter in 1..4")
    if frequency == "annual" and fiscal_quarter is not None:
        raise ValueError("annual lookups do not accept fiscal_quarter")
    if information_available_as_of is not None and not isinstance(information_available_as_of, date):
        raise ValueError("information_available_as_of must be a date or None")
    if statement not in STATEMENT_FILES:
        raise ValueError(f"statement must be one of {sorted(STATEMENT_FILES)}")
    if indfmt is not None and not indfmt.strip():
        raise ValueError("indfmt must be a non-empty format code or None")
    if datafmt is not None and not datafmt.strip():
        raise ValueError("datafmt must be a non-empty format code or None")
    indfmt = indfmt.strip().upper() if indfmt is not None else None
    datafmt = datafmt.strip().upper() if datafmt is not None else None
    requested_metric = metric.strip().lower() if metric else ""
    if not requested_metric:
        raise ValueError("metric must be a Compustat source field name")

    root = Path(compustat_root)
    base: dict[str, Any] = {
        "status": None,
        "value": None,
        "units": None,
        "currency": None,
        "barrid": barrid.strip().upper(),
        "resolved_gvkey": None,
        "mapping_date": mapping_date,
        "mapping_snapshot_path": None,
        "mapping_status": None,
        "mapping_snapshot_semantics": "The selected mapping is the snapshot for mapping_date; it is not automatically treated as a verified historical Barrid-to-GVKEY mapping.",
        "mapping_relationships": [],
        "mapping_temporal_status": None,
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "frequency": frequency,
        "indfmt": indfmt,
        "datafmt": datafmt,
        "reporting_date": None,
        "information_available_as_of": information_available_as_of,
        "information_availability_status": "not_verified_current_compustat_non_pit_extract",
        "source_table": "comp.funda" if frequency == "annual" else "comp.fundq",
        "statement": statement,
        "original_field_name": requested_metric,
        "period_semantics": None,
        "source_snapshot_id": None,
        "source_file_path": None,
        "extracted_at": None,
        "point_in_time_vintage": False,
        "vintage_limitation": "Saved Compustat extracts are current vendor data and are not historical point-in-time vintages.",
    }

    mapping, mapping_path, mapping_candidates = _mapping_snapshot_for_date(root, mapping_date, mapping_snapshot)
    base["mapping_snapshot_path"] = mapping_path
    if mapping is None:
        if mapping_candidates:
            base.update(status="ambiguous_mapping_snapshot", candidate_mapping_snapshots=mapping_candidates)
            return base
        base.update(status="no_valid_mapping", reason=(
            "No unique saved mapping snapshot exists for the requested mapping date."
            if mapping_path is None else "The supplied mapping snapshot does not have the requested membership date."
        ))
        return base

    required = {"barrid", "gvkey", "mapping_status"}
    if not required.issubset(mapping.columns):
        base.update(status="no_valid_mapping", reason=f"Mapping snapshot lacks required columns: {sorted(required - set(mapping.columns))}")
        return base
    matching = mapping.filter(pl.col("barrid").cast(pl.String).str.to_uppercase() == base["barrid"])
    if matching.is_empty():
        base.update(status="no_valid_mapping", reason="Barrid is absent from the selected mapping snapshot.")
        return base
    map_rows = matching.to_dicts()
    candidate_gvkeys = sorted({str(row["gvkey"]) for row in map_rows if row.get("gvkey") is not None})
    statuses = {str(row.get("mapping_status")) for row in map_rows}
    if len(candidate_gvkeys) > 1 or "ambiguous_gvkey" in statuses:
        base.update(status="ambiguous_mapping", candidate_gvkeys=candidate_gvkeys, reason="Barrid maps to multiple or explicitly ambiguous GVKEYs in the selected snapshot.")
        return base
    eligible_statuses = {"mapped_current_cik_metadata", "missing_cik"}
    if len(candidate_gvkeys) != 1 or not statuses or not statuses.issubset(eligible_statuses):
        base.update(status="no_valid_mapping", candidate_gvkeys=candidate_gvkeys, reason=f"Mapping status is not eligible for report lookup: {sorted(statuses)}")
        return base

    gvkey = candidate_gvkeys[0]
    barra_statuses = {str(row.get("barra_mapping_status")) for row in map_rows if row.get("barra_mapping_status") is not None}
    if barra_statuses and not barra_statuses.issubset({"mapped"}):
        base.update(status="no_valid_mapping", candidate_gvkeys=candidate_gvkeys, reason=f"Barrid relationship is not eligible in the selected Barra mapping snapshot: {sorted(barra_statuses)}")
        return base
    base["resolved_gvkey"] = gvkey
    base["mapping_status"] = sorted(statuses)[0] if len(statuses) == 1 else sorted(statuses)
    base["mapping_relationships"] = [
        {key: row.get(key) for key in (
            "gvkey", "iid", "cusip9", "barrid", "mapping_status", "barra_mapping_status",
            "compustat_efffrom", "compustat_effthru", "barra_start_date", "barra_end_date",
        )}
        for row in map_rows
    ]
    candidates = _period_candidates(root, gvkey, fiscal_year, frequency, fiscal_quarter, indfmt, datafmt)
    if not candidates:
        base.update(status="report_not_collected", reason="No saved report matches the requested GVKEY, frequency, fiscal year, and quarter.", candidate_reports=[])
        base["mapping_temporal_status"] = "not_tested_report_not_found"
        return base

    candidate_summaries = []
    for period_dir, report in candidates:
        version_ids, latest_id = _version_candidates(period_dir, report)
        candidate_summaries.append({
            "reporting_date": report.get("datadate"),
            "indfmt": _report_format(report)[0],
            "datafmt": _report_format(report)[1],
            "source_snapshot_ids": version_ids,
            "latest_snapshot_id": latest_id,
            "period_path": str(period_dir),
        })
    if len(candidates) > 1:
        base.update(status="multiple_matching_reports", candidate_reports=candidate_summaries, reason="More than one saved report matches this fiscal period; choose a reporting date or resolve the candidates explicitly.")
        return base

    period_dir, report = candidates[0]
    base["indfmt"], base["datafmt"] = _report_format(report)
    reporting_date = _as_date(report.get("datadate"))
    base["reporting_date"] = reporting_date
    interval_checks = [
        (
            _date_interval_contains(row, "compustat_efffrom", "compustat_effthru", reporting_date),
            _date_interval_contains(row, "barra_start_date", "barra_end_date", reporting_date),
        )
        for row in map_rows
    ] if reporting_date else []
    any_complete_interval_match = any(a is True and b is True for a, b in interval_checks)
    all_complete_interval_miss = bool(interval_checks) and all(a is False or b is False for a, b in interval_checks) and not any_complete_interval_match
    if reporting_date and all_complete_interval_miss:
        base.update(status="mapping_not_valid_for_reporting_period", reason="The saved Compustat or Barra effective interval does not cover the report date.", mapping_temporal_status="effective_interval_excludes_reporting_date")
        return base
    if reporting_date and any_complete_interval_match:
        base["mapping_temporal_status"] = "saved_effective_intervals_cover_period; snapshot_is_not_a_historical_vintage"
    else:
        base["mapping_temporal_status"] = "effective_interval_not_available_or_incomplete; snapshot_is_not_a_historical_vintage"

    version_ids, latest_id = _version_candidates(period_dir, report)
    if source_snapshot_id == "latest":
        selected_id = latest_id
        if selected_id is None or selected_id not in version_ids:
            base.update(status="extraction_version_unavailable", candidate_source_snapshot_ids=version_ids, reason="The report has no valid latest-snapshot pointer.")
            return base
    elif source_snapshot_id is not None:
        selected_id = source_snapshot_id
        if selected_id not in version_ids:
            base.update(status="extraction_version_not_found", candidate_source_snapshot_ids=version_ids, reason="The requested extraction version is not saved for this report.")
            return base
    elif len(version_ids) > 1:
        base.update(status="multiple_extraction_versions", candidate_source_snapshot_ids=version_ids, latest_snapshot_id=latest_id, reason="Choose an exact version ID or pass source_snapshot_id='latest'.")
        return base
    else:
        selected_id = version_ids[0] if version_ids else None

    version_dir = period_dir / "snapshots" / selected_id if selected_id and (period_dir / "snapshots" / selected_id).is_dir() else period_dir
    version_report_path = version_dir / "report.json"
    if version_dir != period_dir:
        if not version_report_path.is_file():
            base.update(status="extraction_metadata_unavailable", source_snapshot_id=selected_id,
                        source_report_metadata_path=str(version_report_path),
                        reason="The selected extraction snapshot has no report.json metadata.")
            return base
        try:
            version_report = json.loads(version_report_path.read_text())
        except (OSError, json.JSONDecodeError):
            base.update(status="extraction_metadata_unavailable", source_snapshot_id=selected_id,
                        source_report_metadata_path=str(version_report_path),
                        reason="The selected extraction snapshot report.json is unreadable.")
            return base
        version_years = version_report.get("fiscal_year")
        version_years = version_years if isinstance(version_years, list) else [version_years]
        version_quarters = version_report.get("fiscal_quarter")
        version_quarters = version_quarters if isinstance(version_quarters, list) else [version_quarters]
        if (version_report.get("source_snapshot_id") != selected_id
                or str(version_report.get("gvkey")) != gvkey
                or _as_date(version_report.get("datadate")) != reporting_date
                or version_report.get("period_frequency") != frequency
                or (indfmt is not None and _report_format(version_report)[0] != indfmt)
                or (datafmt is not None and _report_format(version_report)[1] != datafmt)
                or fiscal_year not in version_years
                or (frequency == "quarterly" and fiscal_quarter not in version_quarters)):
            base.update(status="extraction_metadata_mismatch", source_snapshot_id=selected_id,
                        source_report_metadata_path=str(version_report_path),
                        reason="Selected snapshot metadata does not match the selected version, GVKEY, or report date.")
            return base
        report = version_report

    filename = STATEMENT_FILES[statement]
    source_path = version_dir / filename
    base["source_snapshot_id"] = selected_id or report.get("source_snapshot_id")
    base["extracted_at"] = report.get("retrieved_at")
    base["source_report_metadata_path"] = str(version_report_path) if version_report_path.is_file() else None
    base["extraction_schema_version"] = report.get("schema_version")
    base["extraction_mapping_references"] = report.get("mapping_references")
    base["source_file_path"] = str(source_path)
    schema_missing = set(report.get("source_fields_missing_from_schema") or [])
    definitions = report.get("source_field_definitions") or {}
    labels = report.get("source_field_labels") or {}
    units_map = report.get("source_field_units") or {}
    labels = labels if isinstance(labels, dict) else {}
    units_map = units_map if isinstance(units_map, dict) else {}
    definition = definitions.get(requested_metric, {}) if isinstance(definitions, dict) else {}
    units = units_map.get(requested_metric) or definition.get("unit")
    semantics = definition.get("period_semantics") or units
    base.update({
        "units": units,
        "original_field_label": labels.get(requested_metric) or definition.get("label"),
        "period_semantics": (
            "Compustat fiscal year-to-date value as reported; no standalone quarter derivation."
            if frequency == "quarterly" and statement == "cash_flow" else semantics
        ),
    })
    if not source_path.is_file():
        base.update(status="field_unavailable", reason=f"This extraction does not contain {filename}; older reports remain readable through their existing files.")
        return base
    frame = pl.read_parquet(source_path)
    if frame.height:
        preview = frame.row(0, named=True)
        base["currency"] = preview.get("currency")
        base["reported_rdq"] = preview.get("rdq")
    if requested_metric in schema_missing:
        base.update(status="field_unavailable", reason="The field was absent from the connected Compustat source schema for this extraction.")
        return base

    if requested_metric not in frame.columns:
        field_groups = report.get("field_groups") or {}
        other_groups = [name for name, fields in field_groups.items() if name != statement and requested_metric in fields]
        if other_groups:
            base.update(status="metric_not_in_statement", reason=f"The source field belongs to {other_groups}, not {statement}.")
            return base
        if requested_metric in set(labels) | set(definitions) | set(units_map):
            reason = "The source field is not present in this statement file for this extraction."
        else:
            reason = "The requested original field name is not documented in this report's source metadata."
        base.update(status="field_unavailable", reason=reason)
        return base

    rows = frame.to_dicts()
    if len(rows) != 1:
        base.update(status="multiple_matching_values", candidate_row_count=len(rows), reason="The selected statement file contains multiple rows; no row was selected implicitly.")
        return base
    row = rows[0]
    base.update({
        "currency": row.get("currency"),
        "fiscal_year": report.get("fiscal_year"),
        "fiscal_quarter": report.get("fiscal_quarter"),
        "source_fyear": row.get("fyear"),
        "source_fyearq": row.get("fyearq"),
        "fiscal_year_end_month": row.get("fyr"),
        "reported_rdq": row.get("rdq"),
        "source_table": report.get("source_table", base["source_table"]),
        "value": row.get(requested_metric),
        "information_availability_note": (
            "The requested information date is retained separately. Current Compustat extracts are not point-in-time vintages, so historical value availability is not established."
        ),
    })
    base["status"] = "field_present_null" if row.get(requested_metric) is None else "ok"
    return base
