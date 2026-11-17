"""Write a read-only completion audit for a retained Compustat collection run."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

import polars as pl


ARTIFACTS = (
    "report.json",
    "balance_sheet.parquet",
    "income_statement.parquet",
    "cash_flow.parquet",
    "capital_metrics.parquet",
)


def process_is_running(pid: int) -> bool:
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        stat = stat_path.read_text()
    except FileNotFoundError:
        return False
    # The process name in /proc/PID/stat can contain spaces and parentheses.
    state = stat[stat.rfind(")") + 2 :].split(" ", 1)[0]
    return state != "Z"


def wait_for_process(pid: int, poll_seconds: int) -> None:
    while process_is_running(pid):
        time.sleep(poll_seconds)


def _identity_path(output_root: Path, frequency: str, row: dict) -> Path:
    date_value = row["datadate"]
    year = date_value.year if hasattr(date_value, "year") else int(str(date_value)[:4])
    date_text = date_value.isoformat() if hasattr(date_value, "isoformat") else str(date_value)[:10]
    return output_root / str(year) / f"gvkey_{row['gvkey']}" / (
        f"{frequency}_{date_text}_{row['indfmt']}_{row['datafmt']}"
    )


def _same_value(left, right) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return abs(float(left) - float(right)) <= 1e-9
    return left == right


def audit(run_dir: Path, output_root: Path, collector_pid: int, collector_exit_code: int) -> dict:
    metadata = json.loads((run_dir / "run.json").read_text())
    coverage_path = run_dir / "coverage_report.json"
    coverage = json.loads(coverage_path.read_text()) if coverage_path.exists() else None
    identities: dict[str, list[dict]] = {}
    for frequency in ("annual", "quarterly"):
        source = pl.read_parquet(run_dir / f"{frequency}.parquet")
        identities[frequency] = source.select(
            "gvkey", "datadate", "indfmt", "datafmt"
        ).unique().sort(["gvkey", "datadate", "indfmt", "datafmt"]).to_dicts()

    missing_reports: list[str] = []
    missing_artifacts: list[str] = []
    identity_mismatches: list[str] = []
    artifact_checks = 0
    for frequency, rows in identities.items():
        for row in rows:
            identity = _identity_path(output_root, frequency, row)
            latest_path = identity / "latest_snapshot.json"
            if not latest_path.is_file():
                missing_reports.append(str(identity))
                continue
            try:
                latest = json.loads(latest_path.read_text())
                version = identity / latest["snapshot_path"]
                report_path = version / "report.json"
                report = json.loads(report_path.read_text())
            except (OSError, KeyError, json.JSONDecodeError):
                missing_reports.append(str(identity))
                continue
            date_text = row["datadate"].isoformat()
            expected = {
                "period_frequency": frequency,
                "gvkey": str(row["gvkey"]),
                "datadate": date_text,
                "indfmt": str(row["indfmt"]),
                "datafmt": str(row["datafmt"]),
            }
            if any(str(report.get(key)) != value for key, value in expected.items()):
                identity_mismatches.append(str(identity))
            for name in ARTIFACTS:
                artifact_checks += 1
                if not (version / name).is_file() or not (identity / name).is_file():
                    missing_artifacts.append(f"{identity}/{name}")

    output_schemas: dict[str, dict[str, list[str]]] = {}
    sampled_value_checks: dict[str, list[dict]] = {}
    metric_files = {
        "annual": {
            "at": "balance_sheet.parquet", "sale": "income_statement.parquet",
            "oancf": "cash_flow.parquet", "csho": "capital_metrics.parquet",
            "dvpd": "capital_metrics.parquet",
        },
        "quarterly": {
            "atq": "balance_sheet.parquet", "saleq": "income_statement.parquet",
            "oancfy": "cash_flow.parquet", "cshoq": "capital_metrics.parquet",
        },
    }
    for frequency, rows in identities.items():
        output_schemas[frequency] = {}
        sampled_value_checks[frequency] = []
        source = pl.read_parquet(run_dir / f"{frequency}.parquet")
        source_schema = pl.read_parquet_schema(run_dir / f"{frequency}.parquet")
        output_schemas[frequency]["source_extract.parquet"] = list(source_schema)
        if not rows:
            continue
        row = rows[0]
        identity = _identity_path(output_root, frequency, row)
        try:
            latest = json.loads((identity / "latest_snapshot.json").read_text())
            version = identity / latest["snapshot_path"]
            source_row = source.filter(
                (pl.col("gvkey") == row["gvkey"])
                & (pl.col("datadate") == row["datadate"])
                & (pl.col("indfmt") == row["indfmt"])
                & (pl.col("datafmt") == row["datafmt"])
            ).row(0, named=True)
            for artifact in ARTIFACTS[1:]:
                artifact_frame = pl.read_parquet(version / artifact)
                output_schemas[frequency][artifact] = artifact_frame.columns
            for field, artifact in metric_files[frequency].items():
                artifact_frame = pl.read_parquet(version / artifact)
                artifact_row = artifact_frame.filter(
                    (pl.col("gvkey") == row["gvkey"])
                    & (pl.col("datadate") == row["datadate"])
                ).row(0, named=True)
                source_value = source_row.get(field)
                saved_value = artifact_row.get(field)
                sampled_value_checks[frequency].append({
                    "gvkey": str(row["gvkey"]),
                    "datadate": row["datadate"].isoformat(),
                    "field": field,
                    "source_value": source_value,
                    "saved_value": saved_value,
                    "matches": _same_value(source_value, saved_value),
                })
        except (OSError, KeyError, IndexError, json.JSONDecodeError):
            output_schemas[frequency]["sample_check_error"] = ["sample report or artifact unreadable"]

    counts = {frequency: len(rows) for frequency, rows in identities.items()}
    sample_mismatches = [
        check for checks in sampled_value_checks.values() for check in checks
        if not check["matches"]
    ]
    sample_errors = any(
        "sample_check_error" in output_schemas.get(frequency, {})
        for frequency in ("annual", "quarterly")
    )
    collector_exited = not process_is_running(collector_pid)
    return {
        "completion_report_schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "collector_pid": collector_pid,
        "collector_process_exited": collector_exited,
        "collector_exit_code": collector_exit_code,
        "run_id": metadata["run_id"],
        "membership_snapshot_date": metadata["membership_snapshot_date"],
        "start_date": metadata["start_date"],
        "end_date": metadata["end_date"],
        "filters": metadata["filters"],
        "report_identity_fields": metadata["report_identity_fields"],
        "selected_gvkey_count": metadata["selected_gvkey_count"],
        "source_row_counts": {
            "annual": pl.scan_parquet(run_dir / "annual.parquet").select(pl.len()).collect().item(),
            "quarterly": pl.scan_parquet(run_dir / "quarterly.parquet").select(pl.len()).collect().item(),
        },
        "unique_report_identity_counts": counts,
        "source_extract_schemas": {
            frequency: output_schemas[frequency].get("source_extract.parquet", [])
            for frequency in ("annual", "quarterly")
        },
        "sample_output_schemas": output_schemas,
        "sampled_value_checks": sampled_value_checks,
        "sample_value_mismatches_count": len(sample_mismatches),
        "artifact_file_checks": artifact_checks,
        "missing_reports_count": len(missing_reports),
        "missing_reports_sample": missing_reports[:20],
        "missing_artifacts_count": len(missing_artifacts),
        "missing_artifacts_sample": missing_artifacts[:20],
        "identity_mismatches_count": len(identity_mismatches),
        "identity_mismatches_sample": identity_mismatches[:20],
        "coverage_report_available": coverage is not None,
        "coverage_report_summary": ({
            frequency: {
                key: coverage["frequencies"][frequency][key]
                for key in (
                    "source_row_count", "reporting_company_count",
                    "companies_without_reports", "duplicate_report_identity_rows",
                    "field_coverage",
                )
            }
            for frequency in ("annual", "quarterly")
        } if coverage else None),
        "complete": collector_exit_code == 0 and collector_exited and not (
            missing_reports or missing_artifacts or identity_mismatches
        ) and not sample_mismatches and not sample_errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True, help="Active collector process to wait for")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--collector-exit-code", type=int, default=0)
    parser.add_argument("--poll-seconds", type=int, default=60)
    args = parser.parse_args()
    wait_for_process(args.pid, max(10, args.poll_seconds))
    report = audit(args.run_dir, args.output_root, args.pid, args.collector_exit_code)
    destination = args.run_dir / "completion_report.json"
    temporary = destination.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, destination)
    print(f"Completion report: {destination}")
    print(f"Complete: {report['complete']}")


if __name__ == "__main__":
    main()
