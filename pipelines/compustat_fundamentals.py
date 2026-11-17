"""Compustat-first annual and quarterly financial statement archive."""

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
import wrds
from dotenv import load_dotenv
from click import ClickException

from pipelines.fundamentals_flow import (
    build_security_mapping,
    fetch_compustat_security_candidates,
    load_latest_russell_snapshot,
    load_barra_security_candidates,
)
from pipelines.fundamentals_storage import parse_iso_date, persist_mapping_snapshot


FORMAT_FILTERS = {"indfmt": "INDL", "consol": "C", "popsrc": "D", "datafmt": "STD"}
FORMAT_LIMITATION = (
    "INDL records only are selected. Financial Services format (FS) and other formats "
    "are excluded and may require separate field interpretation."
)

# Compustat standardized item labels and period semantics; currency follows curcd/curcdq.
BALANCE_FIELDS = {
    "at": ("Total assets", "Millions of reported currency; period-end instant"),
    "act": ("Current assets", "Millions of reported currency; period-end instant"),
    "che": ("Cash and short-term investments", "Millions of reported currency; period-end instant"),
    "rect": ("Receivables", "Millions of reported currency; period-end instant"),
    "invt": ("Inventories", "Millions of reported currency; period-end instant"),
    "ppent": ("Property, plant and equipment, net", "Millions of reported currency; period-end instant"),
    "lct": ("Current liabilities", "Millions of reported currency; period-end instant"),
    "dlc": ("Debt in current liabilities", "Millions of reported currency; period-end instant"),
    "dltt": ("Long-term debt", "Millions of reported currency; period-end instant"),
    "lt": ("Total liabilities", "Millions of reported currency; period-end instant"),
    "seq": ("Stockholders' equity", "Millions of reported currency; period-end instant"),
}
INCOME_FIELDS = {
    "sale": ("Sales/turnover (net)", "Millions of reported currency; period duration"),
    "cogs": ("Cost of goods sold", "Millions of reported currency; period duration"),
    "gp": ("Gross profit", "Millions of reported currency; period duration"),
    "xrd": ("Research and development expense", "Millions of reported currency; period duration"),
    "xsga": ("Selling, general and administrative expense", "Millions of reported currency; period duration"),
    "oiadp": ("Operating income after depreciation", "Millions of reported currency; period duration"),
    "xint": ("Interest and related expense", "Millions of reported currency; period duration"),
    "txt": ("Income taxes", "Millions of reported currency; period duration"),
    "ni": ("Net income (loss)", "Millions of reported currency; period duration"),
    "epspx": ("Earnings per share (basic, excluding extraordinary items)", "Reported currency per share; period duration"),
    "epsfx": ("Earnings per share (diluted, excluding extraordinary items)", "Reported currency per share; period duration"),
}

# Labels below were checked against the connected WRDS comp table column labels.
# Quarterly cash-flow items ending in ``y`` are explicitly year-to-date, as
# documented by the WRDS QUARTERIZE macro. No quarterization occurs here.
ANNUAL_CASH_FLOW_FIELDS = {
    "oancf": ("Operating Activities - Net Cash Flow", "Millions of reported currency; annual duration"),
    "ivncf": ("Investing Activities - Net Cash Flow", "Millions of reported currency; annual duration"),
    "fincf": ("Financing Activities - Net Cash Flow", "Millions of reported currency; annual duration"),
    "capx": ("Capital Expenditures", "Millions of reported currency; annual duration"),
    "dpc": ("Depreciation and Amortization (Cash Flow)", "Millions of reported currency; annual duration"),
    "chech": ("Cash and Cash Equivalents - Increase/(Decrease)", "Millions of reported currency; annual duration"),
    "cdvc": ("Cash Dividends on Common Stock (Cash Flow)", "Millions of reported currency; annual duration"),
    "pdvc": ("Cash Dividends on Preferred/Preference Stock (Cash Flow)", "Millions of reported currency; annual duration"),
    "dltis": ("Long-Term Debt - Issuance", "Millions of reported currency; annual duration"),
    "dltr": ("Long-Term Debt - Reduction", "Millions of reported currency; annual duration"),
    "dlcch": ("Current Debt - Changes", "Millions of reported currency; annual duration"),
    "prstkc": ("Purchase of Common and Preferred Stock", "Millions of reported currency; annual duration"),
    "prstkcc": ("Purchase of Common Stock (Cash Flow)", "Millions of reported currency; annual duration"),
    "prstkpc": ("Purchase of Preferred/Preference Stock (Cash Flow)", "Millions of reported currency; annual duration"),
}
QUARTERLY_CASH_FLOW_FIELDS = {
    "oancfy": ("Operating Activities - Net Cash Flow", "Millions of reported currency; fiscal year-to-date duration"),
    "ivncfy": ("Investing Activities - Net Cash Flow", "Millions of reported currency; fiscal year-to-date duration"),
    "fincfy": ("Financing Activities - Net Cash Flow", "Millions of reported currency; fiscal year-to-date duration"),
    "capxy": ("Capital Expenditures", "Millions of reported currency; fiscal year-to-date duration"),
    "dpcy": ("Depreciation and Amortization - Statement of Cash Flows", "Millions of reported currency; fiscal year-to-date duration"),
    "chechy": ("Cash and Cash Equivalents - Increase (Decrease)", "Millions of reported currency; fiscal year-to-date duration"),
    "cdvcy": ("Cash Dividends on Common Stock (Cash Flow)", "Millions of reported currency; fiscal year-to-date duration"),
    "pdvcy": ("Cash Dividends on Preferred/Preference Stock (Cash Flow)", "Millions of reported currency; fiscal year-to-date duration"),
    "dltisy": ("Long-Term Debt - Issuance", "Millions of reported currency; fiscal year-to-date duration"),
    "dltry": ("Long-Term Debt - Reduction", "Millions of reported currency; fiscal year-to-date duration"),
    "dlcchy": ("Changes in Current Debt", "Millions of reported currency; fiscal year-to-date duration"),
    "prstkcy": ("Purchase of Common and Preferred Stock", "Millions of reported currency; fiscal year-to-date duration"),
    "prstkccy": ("Purchase of Common Stock (Cash Flow)", "Millions of reported currency; fiscal year-to-date duration"),
    "prstkpcy": ("Purchase of Preferred/Preference Stock (Cash Flow)", "Millions of reported currency; fiscal year-to-date duration"),
}
ANNUAL_CAPITAL_FIELDS = {
    "csho": ("Common Shares Outstanding", "Millions of shares; period-end instant"),
    "cshpri": ("Common Shares Used to Calculate Earnings Per Share - Basic", "Millions of shares; annual weighted average"),
    "cshfd": ("Common Shares Used to Calc Earnings Per Share - Fully Diluted", "Millions of shares; annual weighted average"),
    "dvpsp_f": ("Dividends per Share - Pay Date - Fiscal", "Reported currency per share; fiscal-year basis"),
    "dvpsx_f": ("Dividends per Share - Ex-Date - Fiscal", "Reported currency per share; fiscal-year basis"),
    "dvc": ("Dividends Common/Ordinary", "Millions of reported currency; annual duration (declared/provided)"),
    "dvp": ("Dividends - Preferred/Preference", "Millions of reported currency; annual duration (declared/provided)"),
    "dvt": ("Dividends - Total", "Millions of reported currency; annual duration (declared/provided)"),
    "dvpd": ("Cash Dividends Paid", "Millions of reported currency; annual cash-flow duration"),
}
QUARTERLY_CAPITAL_FIELDS = {
    "cshoq": ("Common Shares Outstanding", "Millions of shares; period-end instant"),
    "cshprq": ("Common Shares Used to Calculate Earnings Per Share - Basic", "Millions of shares; quarterly weighted average"),
    "cshfdq": ("Com Shares for Diluted EPS", "Millions of shares; quarterly weighted average"),
    "dvpspq": ("Dividends per Share - Pay Date - Quarter", "Reported currency per share; quarter"),
    "dvpsxq": ("Div per Share - Exdate - Quarter", "Reported currency per share; quarter"),
    "dvpq": ("Dividends - Preferred/Preference", "Millions of reported currency; quarterly duration (declared/provided)"),
}

ADDITIONAL_INCOME_ANNUAL = {
    "pi": ("Pretax Income", "Millions of reported currency; annual duration"),
    "dp": ("Depreciation and Amortization", "Millions of reported currency; annual duration"),
}
ADDITIONAL_INCOME_QUARTERLY = {
    "piq": ("Pretax Income", "Millions of reported currency; quarterly duration"),
    "dpq": ("Depreciation and Amortization - Total", "Millions of reported currency; quarterly duration"),
}

TABLE_FIELDS = {
    "funda": {**BALANCE_FIELDS, **INCOME_FIELDS, **ANNUAL_CASH_FLOW_FIELDS,
              **ANNUAL_CAPITAL_FIELDS, **ADDITIONAL_INCOME_ANNUAL},
    "fundq": {
        **{f"{field}q": definition for field, definition in BALANCE_FIELDS.items()},
        **{f"{field}q": definition for field, definition in INCOME_FIELDS.items()},
        **QUARTERLY_CASH_FLOW_FIELDS, **QUARTERLY_CAPITAL_FIELDS,
        **{field: definition for field, definition in ADDITIONAL_INCOME_QUARTERLY.items()},
    },
}
STATEMENT_GROUPS = {
    "funda": {
        "balance_sheet": sorted(f"{field}" for field in BALANCE_FIELDS),
        "income_statement": sorted(set(INCOME_FIELDS) | set(ADDITIONAL_INCOME_ANNUAL)),
        "cash_flow": sorted(ANNUAL_CASH_FLOW_FIELDS),
        "capital_metrics": sorted(ANNUAL_CAPITAL_FIELDS),
    },
    "fundq": {
        "balance_sheet": sorted(f"{field}q" for field in BALANCE_FIELDS),
        "income_statement": sorted({f"{field}q" for field in INCOME_FIELDS} | set(ADDITIONAL_INCOME_QUARTERLY)),
        "cash_flow": sorted(QUARTERLY_CASH_FLOW_FIELDS),
        "capital_metrics": sorted(QUARTERLY_CAPITAL_FIELDS),
    },
}
SELECT_FIELDS = sorted(set(TABLE_FIELDS["funda"]) | set(TABLE_FIELDS["fundq"]))

FIELD_DESCRIPTIONS = {
    "csho": "Net common shares outstanding at the annual balance-sheet date; excludes treasury shares.",
    "cshoq": "Net common shares outstanding at the quarterly balance-sheet date; excludes treasury shares.",
    "cshpri": "Common shares used to calculate basic EPS; weighted average where reported as such.",
    "cshprq": "Common shares used to calculate basic quarterly EPS; this is not shares outstanding.",
    "cshfd": "Common shares used to calculate diluted EPS, including dilutive potential shares where applicable.",
    "cshfdq": "Common shares used to calculate diluted quarterly EPS; this is not shares outstanding.",
    "dvc": "Dollar amount of dividends declared on common stock during the year, as reported on the stockholders' equity statement.",
    "dvp": "Preferred dividend requirements and dividends paid on noncumulative preferred stock, as reported on the stockholders' equity statement.",
    "dvt": "Compustat total dividends item; retained under its own source mnemonic and kept distinct from cash dividends paid.",
    "dvpq": "Quarterly preferred dividend item, retained under its own source mnemonic and distinct from cash-flow dividends paid.",
    "dvpd": "Total cash dividends paid for common and preferred stock; an annual cash-flow measure.",
    "cdvc": "Cash dividends on common stock reported in the cash-flow statement.",
    "cdvcy": "Cash dividends on common stock reported in the cash-flow statement, fiscal year to date.",
    "pdvc": "Cash dividends on preferred stock reported in the cash-flow statement.",
    "pdvcy": "Cash dividends on preferred stock reported in the cash-flow statement, fiscal year to date.",
    "dlcch": "Net change in short-term borrowings and/or current maturities of long-term debt; issuance and repayment are not split.",
    "dlcchy": "Net change in short-term borrowings and/or current maturities of long-term debt, fiscal year to date; issuance and repayment are not split.",
    "dltis": "Long-term debt issuance.",
    "dltisy": "Long-term debt issuance, fiscal year to date.",
    "dltr": "Long-term debt reduction.",
    "dltry": "Long-term debt reduction, fiscal year to date.",
    "prstkc": "Purchases of common and preferred stock, retained as the combined source item.",
    "prstkcy": "Purchases of common and preferred stock, fiscal year to date, retained as the combined source item.",
    "dvpsp_f": "Dividends per share by payable date on a fiscal-year basis.",
    "dvpsx_f": "Dividends per share by ex-date on a fiscal-year basis.",
    "dvpspq": "Dividends per share by payable date for the quarter.",
    "dvpsxq": "Dividends per share by ex-date for the quarter.",
    "oancf": "Net cash flow from operating activities.",
    "oancfy": "Net cash flow from operating activities, fiscal year to date.",
    "ivncf": "Net cash flow from investing activities.",
    "ivncfy": "Net cash flow from investing activities, fiscal year to date.",
    "fincf": "Net cash flow from financing activities.",
    "fincfy": "Net cash flow from financing activities, fiscal year to date.",
    "capx": "Capital expenditures.",
    "capxy": "Capital expenditures, fiscal year to date.",
    "chech": "Increase or decrease in cash and cash equivalents.",
    "chechy": "Increase or decrease in cash and cash equivalents, fiscal year to date.",
    "pi": "Operating and nonoperating income before income tax expense and minority interest, subject to Compustat item rules.",
    "piq": "Quarterly operating and nonoperating income before income tax expense and minority interest, subject to Compustat item rules.",
}

UNAVAILABLE_REQUESTED_FIELDS = {
    "funda": [],
    "fundq": [
        {"item": "aggregate cash dividends paid", "status": "unavailable", "reason": "No quarterly DVPD field; source cash-flow components CDVCY and PDVCY are retained as YTD."},
        {"item": "common dividends declared total (DVC equivalent)", "status": "unavailable", "reason": "No DVCQ field in comp.fundq."},
        {"item": "total dividends declared (DVT equivalent)", "status": "unavailable", "reason": "No DVTQ field in comp.fundq."},
    ],
}


def field_definitions(table: str) -> dict[str, dict[str, str]]:
    if table not in TABLE_FIELDS:
        raise ValueError("table must be funda or fundq")
    return {
        field: {
            "label": label,
            "unit": unit,
            "definition": FIELD_DESCRIPTIONS.get(field, label),
            "period_semantics": unit.split("; ", 1)[-1] if "; " in unit else unit,
        }
        for field, (label, unit) in TABLE_FIELDS[table].items()
    }


def source_field(field: str, table: str) -> str:
    return field if table == "funda" else f"{field}q"


def eligible_company_mappings(mapping: pl.DataFrame) -> pl.DataFrame:
    """Unique GVKEYs supported by unambiguous membership relationships."""
    return (
        mapping.filter(
            pl.col("gvkey").is_not_null()
            & pl.col("mapping_status").is_in(
                ["mapped_current_cik_metadata", "missing_cik"]
            )
        )
        .select("gvkey")
        .unique()
        .sort("gvkey")
    )


def select_gvkey_cohort(
    eligible_gvkeys: list[str],
    limit: int | None = None,
    selected_gvkeys: list[str] | None = None,
) -> list[str]:
    """Select an explicit, validated cohort or retain the sorted-limit behavior."""
    eligible = set(eligible_gvkeys)
    if selected_gvkeys is not None:
        if limit is not None:
            raise ValueError("limit and selected_gvkeys cannot both be supplied")
        if len(selected_gvkeys) != len(set(selected_gvkeys)):
            raise ValueError("selected_gvkeys contains duplicates")
        unknown = sorted(set(selected_gvkeys) - eligible)
        if unknown:
            raise ValueError(f"selected_gvkeys contains ineligible GVKEYs: {unknown}")
        if not selected_gvkeys:
            raise ValueError("selected_gvkeys cannot be empty")
        return list(selected_gvkeys)
    ordered = sorted(eligible)
    return ordered[:limit] if limit else ordered


def compustat_sql(table: str, gvkeys: list[str], start_date: date, end_date: date,
                  available_columns: set[str] | None = None) -> str:
    if table not in {"funda", "fundq"}:
        raise ValueError("table must be funda or fundq")
    if any(not re.fullmatch(r"[A-Za-z0-9]+", value) for value in gvkeys):
        raise ValueError("Invalid GVKEY")
    if start_date > end_date:
        raise ValueError("start date must not be after end date")
    gvkey_list = ",".join("'" + key.replace("'", "''") + "'" for key in gvkeys)
    if not gvkey_list:
        return ""
    period_fields = "fyear, fyr" if table == "funda" else "fyearq, fqtr, fyr"
    selected_fields = list(TABLE_FIELDS[table])
    if available_columns is not None:
        selected_fields = [field for field in selected_fields if field in available_columns]
    fields = ", ".join(selected_fields)
    field_clause = f", {fields}" if fields else ""
    currency_field = "curcd" if table == "funda" else "curcdq"
    filters = " AND ".join(f"{key} = '{value}'" for key, value in FORMAT_FILTERS.items())
    rdq_field = ", rdq" if table == "fundq" else ""
    return f"""SELECT gvkey, datadate, {period_fields}, {currency_field}{rdq_field},
       indfmt, consol, popsrc, datafmt{field_clause}
FROM comp.{table}
WHERE gvkey IN ({gvkey_list})
  AND datadate BETWEEN DATE '{start_date.isoformat()}' AND DATE '{end_date.isoformat()}'
  AND {filters}
ORDER BY gvkey, datadate"""


def fetch_statement_extract(
    connection: wrds.Connection, table: str, gvkeys: list[str], start_date: date, end_date: date
) -> tuple[pl.DataFrame, list[str]]:
    if table not in {"funda", "fundq"}:
        raise ValueError("table must be funda or fundq")
    if not gvkeys:
        return pl.DataFrame(), []
    available_columns = set(connection.raw_sql(f"SELECT * FROM comp.{table} LIMIT 0").columns)
    key_columns = {
        "gvkey", "datadate", "indfmt", "consol", "popsrc", "datafmt",
        "curcd" if table == "funda" else "curcdq",
        *( ("fyear", "fyr") if table == "funda" else ("fyearq", "fqtr", "fyr") ),
    }
    if table == "fundq":
        key_columns.add("rdq")
    missing_keys = sorted(key_columns - available_columns)
    if missing_keys:
        raise RuntimeError(f"comp.{table} schema is missing required identifier/format fields: {missing_keys}")
    missing_fields = sorted(
        field for field in TABLE_FIELDS[table] if field not in available_columns
    )
    query = compustat_sql(table, gvkeys, start_date, end_date, available_columns)
    if not query:
        return pl.DataFrame(), missing_fields
    frame = pl.from_pandas(connection.raw_sql(query)).with_columns(
        pl.col("gvkey", "indfmt", "consol", "popsrc", "datafmt").cast(pl.String, strict=False),
        pl.col("datadate").cast(pl.String, strict=False).str.slice(0, 10).str.to_date(strict=False),
    )
    if "rdq" in frame.columns:
        frame = frame.with_columns(pl.col("rdq").cast(pl.String, strict=False).str.slice(0, 10).str.to_date(strict=False))
    else:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Date).alias("rdq"))
    currency_field = "curcd" if table == "funda" else "curcdq"
    frame = frame.with_columns(pl.col(currency_field).cast(pl.String, strict=False).alias("currency"))
    for field, dtype in (("fyear", pl.Int32), ("fyearq", pl.Int32), ("fqtr", pl.Int32), ("fyr", pl.Int32)):
        if field not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(field))
    for field in missing_fields:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(field))
    return frame, missing_fields


def count_excluded_formats(connection: wrds.Connection, table: str, gvkeys: list[str],
                           start_date: date, end_date: date) -> list[dict[str, Any]]:
    if not gvkeys:
        return []
    value_list = ",".join("'" + value.replace("'", "''") + "'" for value in gvkeys)
    filters = " AND ".join(f"{key} = '{value}'" for key, value in FORMAT_FILTERS.items())
    result = connection.raw_sql(f"""SELECT indfmt, consol, popsrc, datafmt, COUNT(*) AS row_count
FROM comp.{table}
WHERE gvkey IN ({value_list})
  AND datadate BETWEEN DATE '{start_date.isoformat()}' AND DATE '{end_date.isoformat()}'
  AND NOT ({filters})
GROUP BY indfmt, consol, popsrc, datafmt
ORDER BY indfmt, consol, popsrc, datafmt""")
    return pl.from_pandas(result).to_dicts()


def _digest_frame(frame: pl.DataFrame) -> str:
    canonical = json.dumps(frame.to_dicts(), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")
    os.replace(temporary, path)


def _statement_views(
    frame: pl.DataFrame, frequency: str
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    common = [
        "gvkey", "datadate", "fyear", "fyr", "fqtr", "fyearq", "currency", "rdq",
        "indfmt", "consol", "popsrc", "datafmt",
    ]
    table = "funda" if frequency == "annual" else "fundq"
    for field in TABLE_FIELDS[table]:
        if field not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(field))
    common_cols = [field for field in common if field in frame.columns]
    views = []
    for group in ("balance_sheet", "income_statement", "cash_flow", "capital_metrics"):
        columns = [field for field in STATEMENT_GROUPS[table][group] if field in frame.columns]
        views.append(frame.select(common_cols + columns))
    return views[0], views[1], views[2], views[3]


def persist_source_extracts(output_root: Path, run_id: str, annual: pl.DataFrame, quarterly: pl.DataFrame,
                            metadata: dict[str, Any]) -> Path:
    directory = output_root / "source_extracts" / run_id
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "run.json").exists():
        return directory
    annual.write_parquet(directory / "annual.parquet")
    quarterly.write_parquet(directory / "quarterly.parquet")
    _write_json(directory / "run.json", metadata)
    return directory


def build_coverage_report(
    gvkeys: list[str], annual: pl.DataFrame, quarterly: pl.DataFrame,
    run_metadata: dict[str, Any],
) -> dict[str, Any]:
    """Build a compact, auditable coverage summary for the selected universe."""
    company_rows: dict[str, dict[str, Any]] = {
        gvkey: {"gvkey": gvkey} for gvkey in gvkeys
    }
    frequency_summaries: dict[str, dict[str, Any]] = {}
    for frequency, frame, year_field in (
        ("annual", annual, "fyear"), ("quarterly", quarterly, "fyearq"),
    ):
        report_counts: dict[str, int] = {}
        if not frame.is_empty():
            for group in frame.group_by("gvkey").agg(
                pl.len().alias("row_count"),
                pl.col("datadate").n_unique().alias("report_date_count"),
                pl.col("datadate").min().alias("first_datadate"),
                pl.col("datadate").max().alias("latest_datadate"),
                pl.col(year_field).drop_nulls().unique().sort().alias("fiscal_years"),
            ).to_dicts():
                gvkey = str(group["gvkey"])
                report_counts[gvkey] = int(group["row_count"])
                company_rows[gvkey][frequency] = {
                    "row_count": int(group["row_count"]),
                    "report_date_count": int(group["report_date_count"]),
                    "first_datadate": group["first_datadate"],
                    "latest_datadate": group["latest_datadate"],
                    "fiscal_years": group["fiscal_years"],
                }
        for gvkey in gvkeys:
            company_rows[gvkey].setdefault(frequency, {
                "row_count": 0, "report_date_count": 0, "first_datadate": None,
                "latest_datadate": None, "fiscal_years": [],
            })
        metric_fields = [field for field in TABLE_FIELDS["funda" if frequency == "annual" else "fundq"]
                         if field in frame.columns]
        field_coverage = {
            field: {
                "non_null_rows": frame.get_column(field).len() - frame.get_column(field).null_count(),
                "null_rows": frame.get_column(field).null_count(),
            }
            for field in metric_fields
        }
        frequency_summaries[frequency] = {
            "source_table": "comp.funda" if frequency == "annual" else "comp.fundq",
            "source_row_count": frame.height,
            "reporting_company_count": len(report_counts),
            "companies_without_reports": len(gvkeys) - len(report_counts),
            "duplicate_report_identity_rows": (
                frame.height - frame.select("gvkey", "datadate", "indfmt", "datafmt").unique().height
                if not frame.is_empty() else 0
            ),
            "field_coverage": field_coverage,
        }
    for company in company_rows.values():
        company["status"] = "reports_collected" if (
            company["annual"]["row_count"] or company["quarterly"]["row_count"]
        ) else "no_reports_in_date_range_reason_unverified"
    return {
        "report_schema_version": 1,
        "run_id": run_metadata["run_id"],
        "membership_snapshot_date": run_metadata["membership_snapshot_date"],
        "start_date": run_metadata["start_date"],
        "end_date": run_metadata["end_date"],
        "filters": run_metadata["filters"],
        "report_identity_fields": run_metadata["report_identity_fields"],
        "eligible_gvkey_count": run_metadata["eligible_gvkey_count"],
        "selected_gvkey_count": len(gvkeys),
        "frequencies": frequency_summaries,
        "excluded_format_counts": run_metadata["excluded_format_counts"],
        "fields_missing_from_schema": run_metadata["fields_missing_from_schema"],
        "company_coverage": [company_rows[key] for key in sorted(company_rows)],
    }


def _store_periods(output_root: Path, frame: pl.DataFrame, frequency: str,
                   mapping_refs: dict[str, dict[str, Any]], retrieved_at: str,
                   company_metadata: dict[str, dict[str, Any]],
                   missing_fields: list[str]) -> tuple[int, list[Path]]:
    rows = 0
    paths: list[Path] = []
    if frame.is_empty():
        return rows, paths
    identity_columns = ["gvkey", "datadate", "indfmt", "datafmt"]
    # Partition once; filtering the full annual/quarterly frame once per report
    # makes the archive stage quadratic in the number of report identities.
    period_partitions = frame.partition_by(identity_columns, as_dict=True)
    for partition_key in sorted(period_partitions, key=lambda key: tuple(str(value) for value in key)):
        period_frame = period_partitions[partition_key]
        row = period_frame.row(0, named=True)
        gvkey = str(row["gvkey"])
        datadate = row["datadate"]
        indfmt = str(row["indfmt"])
        datafmt = str(row["datafmt"])
        period = f"{frequency}_{datadate.isoformat()}_{indfmt}_{datafmt}"
        identity = output_root / str(datadate.year) / f"gvkey_{gvkey}" / period
        balance, income, cash_flow, capital_metrics = _statement_views(period_frame, frequency)
        table = "funda" if frequency == "annual" else "fundq"
        fiscal_year_values = sorted(set(period_frame.get_column("fyear" if frequency == "annual" else "fyearq").drop_nulls().to_list()))
        fiscal_quarter_values = sorted(set(period_frame.get_column("fqtr").drop_nulls().to_list()))
        bundle_content = {
            "rows": _digest_frame(period_frame),
            "frequency": frequency,
            "indfmt": indfmt,
            "datafmt": datafmt,
            "company_metadata": company_metadata.get(gvkey, {}),
            "mapping_references": mapping_refs.get(gvkey, {}),
            "fiscal_year": fiscal_year_values,
            "fiscal_quarter": fiscal_quarter_values,
            "missing_fields": missing_fields,
            "format_filters": FORMAT_FILTERS,
            "field_definitions": field_definitions(table),
        }
        content_id = hashlib.sha256(json.dumps(bundle_content, sort_keys=True, default=str).encode()).hexdigest()
        version = identity / "snapshots" / content_id
        if not version.exists():
            identity.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=".compustat-period-", dir=identity))
            try:
                # Move temporary into the period directory only after the complete bundle exists.
                report = {
                    "schema_version": 2,
                    "gvkey": gvkey,
                    "company_name": company_metadata.get(gvkey, {}).get("company_name"),
                    "ticker": company_metadata.get(gvkey, {}).get("ticker"),
                    "tickers": company_metadata.get(gvkey, {}).get("tickers", []),
                    "period_frequency": "annual" if frequency == "annual" else "quarterly",
                    "indfmt": indfmt,
                    "datafmt": datafmt,
                    "datadate": datadate,
                    "calendar_year": datadate.year,
                    "fiscal_year": fiscal_year_values[0] if len(fiscal_year_values) == 1 else fiscal_year_values,
                    "fiscal_quarter": (fiscal_quarter_values[0] if len(fiscal_quarter_values) == 1 else fiscal_quarter_values) if frequency == "quarterly" else None,
                    "source_record_count": period_frame.height,
                    "period_start": None,
                    "balance_sheet_values_are": "period-end instant values",
                    "income_statement_values_are": "annual duration values" if frequency == "annual" else "quarterly income items from fundq; no standalone-quarter derivations",
                    "cash_flow_values_are": "annual duration values" if frequency == "annual" else "Compustat fiscal year-to-date values as reported; no standalone-quarter derivations",
                    "capital_metrics_period_semantics": "shares outstanding are period-end instants; weighted-average shares and dividend totals are period durations; dividend-per-share fields retain their source pay-date/ex-date and fiscal/quarter bases",
                    "source_table": "comp.funda" if frequency == "annual" else "comp.fundq",
                    "source_fields_missing_from_schema": missing_fields,
                    "requested_items_unavailable": UNAVAILABLE_REQUESTED_FIELDS[table],
                    "source_field_labels": {field: definition["label"] for field, definition in field_definitions(table).items()},
                    "source_field_units": {field: definition["unit"] for field, definition in field_definitions(table).items()},
                    "source_field_definitions": field_definitions(table),
                    "field_groups": STATEMENT_GROUPS[table],
                    "debt_change_limitation": "Long-term debt issuance and reduction are separate source fields; current-debt movement is only provided as net changes in current debt.",
                    "units": f"Monetary values are millions of the reported currency; EPS is currency/share; currency field is {'curcd' if frequency == 'annual' else 'curcdq'}.",
                    "format_filters": FORMAT_FILTERS,
                    "retrieved_at": retrieved_at,
                    "source_snapshot_id": content_id,
                    "mapping_references": mapping_refs.get(gvkey, {}),
                    "company_metadata_basis": "Current comp.company name and comp.security ticker metadata; not historical point-in-time.",
                    "announcement_date_field": "rdq (Compustat report date; not an SEC filing date)",
                    "announcement_date_available_in_source_table": frequency == "quarterly",
                    "point_in_time_vintage": False,
                    "format_limitation": FORMAT_LIMITATION,
                }
                _write_json(temporary / "report.json", report)
                balance.write_parquet(temporary / "balance_sheet.parquet")
                income.write_parquet(temporary / "income_statement.parquet")
                cash_flow.write_parquet(temporary / "cash_flow.parquet")
                capital_metrics.write_parquet(temporary / "capital_metrics.parquet")
                identity.mkdir(parents=True, exist_ok=True)
                version.parent.mkdir(parents=True, exist_ok=True)
                os.replace(temporary, version)
            except Exception:
                shutil.rmtree(temporary, ignore_errors=True)
                raise
        latest_path = identity / "latest_snapshot.json"
        latest_id = None
        if latest_path.exists():
            try:
                latest_id = json.loads(latest_path.read_text()).get("snapshot_id")
            except (OSError, json.JSONDecodeError):
                pass
        identity.mkdir(parents=True, exist_ok=True)
        canonical_names = (
            "report.json", "balance_sheet.parquet", "income_statement.parquet",
            "cash_flow.parquet", "capital_metrics.parquet",
        )
        # An unchanged latest snapshot already has canonical copies. Avoid
        # rewriting five shared-filesystem artifacts for every unchanged report.
        canonical_current = latest_id == content_id and all(
            (identity / name).is_file() for name in canonical_names
        )
        if not canonical_current:
            # Canonical files always name the latest observed source version;
            # old immutable snapshot versions remain untouched.
            for name in canonical_names:
                source = version / name
                target = identity / name
                temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
                shutil.copyfile(source, temporary)
                os.replace(temporary, target)
        if latest_id != content_id:
            _write_json(latest_path, {
                "snapshot_id": content_id,
                "snapshot_path": f"snapshots/{content_id}",
                "retrieved_at": retrieved_at,
            })
        rows += 1
        paths.append(identity)
    return rows, paths


def run_compustat_fundamentals(source_database: str, limit: int | None,
                               start_date: date, end_date: date,
                               output_root: str | Path | None = None,
                               selected_gvkeys: list[str] | None = None,
                               selection_metadata: dict[str, Any] | None = None) -> None:
    load_dotenv(override=True)
    root, user = os.getenv("ROOT"), os.getenv("WRDS_USER")
    if not root or not user:
        raise ClickException("ROOT and WRDS_USER must be configured in the environment")
    snapshot, _ = load_latest_russell_snapshot(root, source_database)
    out = Path(output_root) if output_root else Path(root) / "groups/grp_quant/database/fundamentals/compustat"
    connection = wrds.Connection(
        wrds_username=user,
        wrds_password=os.getenv("WRDS_PASSWORD"),
    )
    try:
        comp_candidates = fetch_compustat_security_candidates(connection, snapshot)
        mapping_base, barra_available = load_barra_security_candidates(root, source_database, snapshot)
        mapping = build_security_mapping(snapshot, comp_candidates, mapping_base, barra_available)
        mapping_path = persist_mapping_snapshot(out, mapping, snapshot.get_column("date")[0])
        eligible = eligible_company_mappings(mapping)
        all_gvkeys = eligible.get_column("gvkey").to_list()
        try:
            gvkeys = select_gvkey_cohort(all_gvkeys, limit, selected_gvkeys)
        except ValueError as exc:
            raise ClickException(str(exc)) from exc
        annual, missing_annual_fields = fetch_statement_extract(connection, "funda", gvkeys, start_date, end_date)
        quarterly, missing_quarterly_fields = fetch_statement_extract(connection, "fundq", gvkeys, start_date, end_date)
        excluded_formats = {
            "annual": count_excluded_formats(connection, "funda", gvkeys, start_date, end_date),
            "quarterly": count_excluded_formats(connection, "fundq", gvkeys, start_date, end_date),
        }
        company_metadata: dict[str, dict[str, Any]] = {}
        tickers_by_issue: dict[tuple[str, str], list[str]] = {}
        if gvkeys:
            value_list = ",".join("'" + value.replace("'", "''") + "'" for value in gvkeys)
            company_frame = pl.from_pandas(connection.raw_sql(
                f"SELECT gvkey, conm FROM comp.company WHERE gvkey IN ({value_list})"
            )).with_columns(pl.col("gvkey", "conm").cast(pl.String, strict=False))
            company_metadata = {
                row["gvkey"]: {"company_name": row.get("conm"), "ticker": None}
                for row in company_frame.to_dicts()
            }
            security_frame = pl.from_pandas(connection.raw_sql(
                f"SELECT gvkey, iid, tic FROM comp.security WHERE gvkey IN ({value_list})"
            )).with_columns(pl.col("gvkey", "iid", "tic").cast(pl.String, strict=False))
            for ticker_row in security_frame.group_by("gvkey", "iid").agg(
                pl.col("tic").drop_nulls().unique().sort().alias("tickers")
            ).to_dicts():
                tickers_by_issue[(str(ticker_row["gvkey"]), str(ticker_row["iid"]))] = ticker_row.get("tickers") or []
            for gvkey in gvkeys:
                issue_ids = mapping.filter(pl.col("gvkey") == gvkey).get_column("iid").drop_nulls().unique().to_list()
                tickers = sorted({ticker for iid in issue_ids for ticker in tickers_by_issue.get((gvkey, str(iid)), [])})
                company_metadata[gvkey] = {
                    **company_metadata.get(gvkey, {}),
                    "ticker": tickers[0] if len(tickers) == 1 else tickers or None,
                    "tickers": tickers,
                }
    finally:
        connection.close()

    retrieved_at = datetime.now(timezone.utc).isoformat()
    content_digest = hashlib.sha256(json.dumps({
        "annual": _digest_frame(annual), "quarterly": _digest_frame(quarterly),
        "mapping": str(mapping_path), "gvkeys": gvkeys,
        "start_date": start_date, "end_date": end_date,
        "field_definitions": {
            "annual": field_definitions("funda"), "quarterly": field_definitions("fundq"),
        },
    }, sort_keys=True, default=str).encode()).hexdigest()
    run_id = f"run_{content_digest[:24]}"
    run_metadata = {
        "run_id": run_id, "source_database": source_database,
        "membership_snapshot_date": snapshot.get_column("date")[0],
        "selected_gvkeys": gvkeys, "selected_gvkey_count": len(gvkeys),
        "eligible_gvkey_count": len(all_gvkeys), "start_date": start_date,
        "end_date": end_date, "retrieved_at": retrieved_at,
        "filters": FORMAT_FILTERS, "format_limitation": FORMAT_LIMITATION,
        "excluded_format_counts": excluded_formats,
        "fields_missing_from_schema": {
            "annual": missing_annual_fields, "quarterly": missing_quarterly_fields,
        },
        "requested_items_unavailable": UNAVAILABLE_REQUESTED_FIELDS,
        "field_definitions": {
            "annual": field_definitions("funda"), "quarterly": field_definitions("fundq"),
        },
        "mapping_snapshot": str(mapping_path), "source_tables": ["comp.funda", "comp.fundq"],
        "report_identity_fields": ["period_frequency", "gvkey", "datadate", "indfmt", "datafmt"],
        "point_in_time_vintage": False,
    }
    if selection_metadata is not None:
        run_metadata["cohort_selection"] = selection_metadata
    run_dir = persist_source_extracts(out, run_id, annual, quarterly, run_metadata)
    _write_json(run_dir / "coverage_report.json", build_coverage_report(
        gvkeys, annual, quarterly, run_metadata,
    ))
    mapping_rows = mapping.filter(pl.col("gvkey").is_in(gvkeys)).to_dicts()
    refs_by_gvkey: dict[str, dict[str, Any]] = {gvkey: {
        "snapshot": str(mapping_path.relative_to(out)),
        "relationships": [
            {key: row.get(key) for key in (
                "cusip9", "iid", "russell_1000", "russell_2000", "barrid", "issuerid", "rootid",
                "compustat_efffrom", "compustat_effthru", "barra_start_date", "barra_end_date",
                "mapping_status", "barra_mapping_status",
            )}
            | {"current_tickers": tickers_by_issue.get((gvkey, str(row.get("iid"))), [])}
            for row in mapping_rows if row["gvkey"] == gvkey
        ],
    } for gvkey in gvkeys}
    annual_rows, _ = _store_periods(out, annual, "annual", refs_by_gvkey, retrieved_at, company_metadata, missing_annual_fields)
    quarterly_rows, _ = _store_periods(out, quarterly, "quarterly", refs_by_gvkey, retrieved_at, company_metadata, missing_quarterly_fields)
    per_cusip = mapping.group_by("cusip9").agg(pl.col("mapping_status").first())
    per_cusip_with_barra = mapping.group_by("cusip9").agg(
        pl.col("mapping_status").first(), pl.col("barra_mapping_status").first()
    )
    mapping_status_counts = per_cusip.group_by("mapping_status").len().sort("mapping_status").to_dicts()
    barra_status_counts = per_cusip_with_barra.group_by("barra_mapping_status").len().sort("barra_mapping_status").to_dicts()
    unresolved = per_cusip.filter(pl.col("mapping_status").is_in(["no_gvkey_match", "ambiguous_gvkey", "ambiguous_cik", "invalid_cik"]))
    nonselected = set(all_gvkeys) - set(gvkeys)
    print(f"Output root: {out}")
    print(f"Membership snapshot date: {snapshot.get_column('date')[0]}; selected GVKEYs: {len(gvkeys)} of {len(all_gvkeys)} eligible")
    print(f"Mapping snapshot: {mapping_path}; unresolved Compustat mapping securities: {unresolved.height}")
    print(f"Compustat mapping status counts: {mapping_status_counts}")
    print(f"Barra mapping status counts: {barra_status_counts}")
    print(f"Formats selected: {FORMAT_FILTERS}; non-INDL records excluded by query: {FORMAT_LIMITATION}")
    print(f"Excluded-format record counts: {excluded_formats}")
    print(f"Fields absent from connected schemas: annual={missing_annual_fields}; quarterly={missing_quarterly_fields}")
    print(f"Annual rows/periods: {annual.height}/{annual_rows}; quarterly rows/periods: {quarterly.height}/{quarterly_rows}")
    for label, frame in (("annual", annual), ("quarterly", quarterly)):
        if frame.is_empty():
            print(f"{label} coverage: no records; gap reason unknown")
        else:
            source = "funda" if label == "annual" else "fundq"
            value_fields = list(TABLE_FIELDS[source])
            null_metrics = {field: frame.get_column(field).null_count() for field in value_fields if frame.get_column(field).null_count()}
            print(f"{label} coverage: latest {frame.get_column('datadate').max()}, null metric counts: {null_metrics}")
    print(f"Excluded GVKEYs due to company limit: {len(nonselected)}; retained source extracts: {run_dir}")
