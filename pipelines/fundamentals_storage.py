"""Filing-oriented storage for selected SEC Company Facts observations."""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any

import polars as pl


BALANCE_SHEET_METRICS = {
    "assets",
    "current_assets",
    "cash_and_equivalents",
    "accounts_receivable",
    "inventory",
    "property_plant_equipment_net",
    "current_liabilities",
    "long_term_debt_current",
    "long_term_debt_noncurrent",
    "liabilities",
    "stockholders_equity",
}

FILING_FORMS = {"10-K", "10-Q", "20-F", "40-F", "6-K"}

STATEMENT_SCHEMA = {
    "cik": pl.String,
    "accession": pl.String,
    "taxonomy": pl.String,
    "tag": pl.String,
    "metric": pl.String,
    "unit": pl.String,
    "currency": pl.String,
    "value_text": pl.String,
    "value": pl.Float64,
    "period_start": pl.Date,
    "period_end": pl.Date,
    "period_type": pl.String,
    "report_period": pl.Date,
    "fiscal_year": pl.Int32,
    "fiscal_period": pl.String,
    "frame": pl.String,
    "form": pl.String,
    "filing_date": pl.Date,
    "source_accession": pl.String,
    "source_api": pl.String,
}


def parse_iso_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def filing_form_supported(form: str | None) -> bool:
    """Accept supported report forms and their amendments."""
    if not form:
        return False
    return form in FILING_FORMS or form in {f"{base}/A" for base in FILING_FORMS}


def _currency_from_unit(unit: str | None) -> str | None:
    if not unit:
        return None
    match = re.fullmatch(r"([A-Z]{3})(?:/.*)?", unit)
    return match.group(1) if match else None


def extract_accession_facts(
    cik: str,
    accession: str,
    filing: dict[str, Any],
    facts_payload: dict[str, Any],
    tag_to_metric: dict[str, str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return selected fact observations for one accession without period collapse."""
    report_period = parse_iso_date(filing.get("reportDate"))
    filing_date = parse_iso_date(filing.get("filingDate"))
    form = filing.get("form")
    balance_rows: list[dict[str, Any]] = []
    income_rows: list[dict[str, Any]] = []

    if filing_form_supported(form):
        for taxonomy, concepts in facts_payload.get("facts", {}).items():
            for tag, concept in concepts.items():
                metric = tag_to_metric.get(tag)
                if metric is None:
                    continue
                for unit, observations in concept.get("units", {}).items():
                    for observation in observations:
                        if observation.get("accn") != accession:
                            continue
                        period_start = parse_iso_date(observation.get("start"))
                        period_end = parse_iso_date(observation.get("end"))
                        raw_value = observation.get("val")
                        try:
                            value = float(raw_value) if raw_value is not None else None
                        except (TypeError, ValueError, OverflowError):
                            value = None
                        row = {
                            "cik": cik,
                            "accession": accession,
                            "taxonomy": taxonomy,
                            "tag": tag,
                            "metric": metric,
                            "unit": unit,
                            "currency": _currency_from_unit(unit),
                            "value_text": str(raw_value) if raw_value is not None else None,
                            "value": value,
                            "period_start": period_start,
                            "period_end": period_end,
                            "period_type": (
                                "duration"
                                if period_start is not None
                                else (
                                    "instant"
                                    if metric in BALANCE_SHEET_METRICS
                                    else "duration_start_missing"
                                )
                            ),
                            "report_period": report_period,
                            "fiscal_year": observation.get("fy"),
                            "fiscal_period": observation.get("fp"),
                            "frame": observation.get("frame"),
                            "form": form,
                            "filing_date": filing_date,
                            "source_accession": observation.get("accn"),
                            "source_api": "sec_companyfacts",
                        }
                        (balance_rows if metric in BALANCE_SHEET_METRICS else income_rows).append(row)

    balance = pl.DataFrame(balance_rows, schema=STATEMENT_SCHEMA) if balance_rows else pl.DataFrame(schema=STATEMENT_SCHEMA)
    income = pl.DataFrame(income_rows, schema=STATEMENT_SCHEMA) if income_rows else pl.DataFrame(schema=STATEMENT_SCHEMA)
    return balance, income


def accession_folder_name(filing: dict[str, Any], accession: str) -> str:
    form = re.sub(r"[^A-Za-z0-9.-]+", "_", str(filing.get("form") or "UNKNOWN"))
    report_period = parse_iso_date(filing.get("reportDate"))
    period = report_period.isoformat() if report_period else "unknown-period"
    safe_accession = re.sub(r"[^A-Za-z0-9-]+", "_", accession)
    return f"{form}_{period}_{safe_accession}"


def filing_source_url(cik: str, accession: str, primary_document: str | None) -> str | None:
    if not re.fullmatch(r"\d{1,10}", cik):
        return None
    accession_compact = accession.replace("-", "")
    if not re.fullmatch(r"\d{18}", accession_compact):
        return None
    safe_document = "/".join(
        part for part in str(primary_document or "").split("/") if part not in {"", ".", ".."}
    )
    suffix = f"/{safe_document}" if safe_document else "/"
    return (
        f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
        f"{accession_compact}{suffix}"
    )


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n").encode()
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_bytes(encoded)
    os.replace(temp, path)


def _copy_atomic(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    shutil.copyfile(source, temp)
    os.replace(temp, target)


def _bundle_hash(report: dict[str, Any]) -> str:
    hash_inputs = {
        "companyfacts_sha256": report.get("companyfacts_sha256"),
        "submission_metadata_sha256": report.get("submission_metadata_sha256"),
        "filing": {
            key: report.get(key)
            for key in (
                "form",
                "accession",
                "cik",
                "report_period",
                "filing_date",
                "acceptance_timestamp",
                "primary_document",
            )
        },
        "gvkey_relationships": report.get("gvkey_relationships"),
    }
    return hashlib.sha256(
        json.dumps(hash_inputs, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def write_filing_bundle(
    output_root: Path,
    filing: dict[str, Any],
    accession: str,
    cik: str,
    report: dict[str, Any],
    balance_sheet: pl.DataFrame,
    income_statement: pl.DataFrame,
) -> tuple[Path, str, bool]:
    """Store a filing accession and immutable extraction versions idempotently."""
    filing_date = parse_iso_date(filing.get("filingDate"))
    if filing_date is None:
        raise ValueError(f"Accession {accession} has no usable filingDate")
    filing_dir = (
        output_root
        / str(filing_date.year)
        / f"CIK{cik.zfill(10)}"
        / accession_folder_name(filing, accession)
    )
    filing_dir.mkdir(parents=True, exist_ok=True)
    snapshot_id = _bundle_hash(report)
    version_dir = filing_dir / "snapshots" / snapshot_id
    created_version = not version_dir.exists()
    report = {**report, "source_snapshot_id": snapshot_id}

    if created_version:
        version_dir.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=".filing-snapshot-", dir=filing_dir))
        try:
            _write_json_atomic(temporary / "report.json", report)
            balance_sheet.write_parquet(temporary / "balance_sheet.parquet")
            income_statement.write_parquet(temporary / "income_statement.parquet")
            try:
                os.replace(temporary, version_dir)
            except FileExistsError:
                shutil.rmtree(temporary)
                created_version = False
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise

    canonical_report = filing_dir / "report.json"
    current_snapshot_id = None
    if canonical_report.exists():
        try:
            current_snapshot_id = json.loads(canonical_report.read_text()).get(
                "source_snapshot_id"
            )
        except (json.JSONDecodeError, OSError):
            current_snapshot_id = None

    if current_snapshot_id != snapshot_id:
        if canonical_report.exists() and current_snapshot_id:
            old_version = filing_dir / "snapshots" / current_snapshot_id
            if not old_version.exists():
                old_version.mkdir(parents=True, exist_ok=True)
                for filename in (
                    "report.json",
                    "balance_sheet.parquet",
                    "income_statement.parquet",
                ):
                    existing = filing_dir / filename
                    if existing.exists():
                        _copy_atomic(existing, old_version / filename)
        for filename in (
            "balance_sheet.parquet",
            "income_statement.parquet",
            "report.json",
        ):
            _copy_atomic(version_dir / filename, filing_dir / filename)
    return filing_dir, snapshot_id, created_version


def update_jsonl_inventory(path: Path, record: dict[str, Any], key_fields: tuple[str, ...]) -> None:
    """Upsert one inventory row atomically, preserving unrelated records."""
    path.parent.mkdir(parents=True, exist_ok=True)
    records: dict[tuple[str, ...], dict[str, Any]] = {}
    if path.exists():
        with path.open("r", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                existing = json.loads(line)
                records[tuple(str(existing.get(key, "")) for key in key_fields)] = existing
    records[tuple(str(record.get(key, "")) for key in key_fields)] = record
    content = "".join(
        json.dumps(value, sort_keys=True, default=str) + "\n"
        for _, value in sorted(records.items())
    )
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(content, encoding="utf-8")
    os.replace(temp, path)


def archive_api_response(
    output_root: Path,
    cik: str,
    endpoint: str,
    response_bytes: bytes,
    retrieved_at: datetime | None = None,
) -> tuple[Path, str, datetime]:
    """Archive distinct response contents without duplicating identical reruns."""
    retrieved_at = retrieved_at or datetime.now(timezone.utc)
    if retrieved_at.tzinfo is None:
        retrieved_at = retrieved_at.replace(tzinfo=timezone.utc)
    retrieved_at = retrieved_at.astimezone(timezone.utc)
    digest = hashlib.sha256(response_bytes).hexdigest()
    directory = output_root / "api_responses" / f"CIK{cik.zfill(10)}" / endpoint
    directory.mkdir(parents=True, exist_ok=True)
    existing = next(directory.glob(f"*_{digest}.json"), None)
    if existing:
        try:
            stored_timestamp = datetime.strptime(
                existing.name.split("_", 1)[0], "%Y%m%dT%H%M%S%fZ"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            stored_timestamp = retrieved_at
        return existing, digest, stored_timestamp
    timestamp = retrieved_at.strftime("%Y%m%dT%H%M%S%fZ")
    path = directory / f"{timestamp}_{digest}.json"
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_bytes(response_bytes)
    os.replace(temp, path)
    return path, digest, retrieved_at


def persist_mapping_snapshot(output_root: Path, mapping: pl.DataFrame, membership_date: date) -> Path:
    """Write a content-addressed, non-overwriting security mapping snapshot."""
    output_dir = output_root / "security_mappings"
    output_dir.mkdir(parents=True, exist_ok=True)
    records = mapping.sort([column for column in ("cusip9", "gvkey", "iid", "barrid") if column in mapping.columns]).to_dicts()
    canonical = json.dumps(records, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    path = output_dir / f"membership_{membership_date.isoformat()}_{digest}.parquet"
    if path.exists():
        return path
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    mapping.write_parquet(temp)
    os.replace(temp, path)
    return path

