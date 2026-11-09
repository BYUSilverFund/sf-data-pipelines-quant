"""Date-effective security mapping and accession-level SEC Company Facts archive."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import time
import traceback
from typing import Any

import polars as pl
import requests
import wrds
from click import ClickException
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.fundamentals_storage import (
    archive_api_response,
    extract_accession_facts,
    filing_form_supported,
    filing_source_url,
    parse_iso_date,
    persist_mapping_snapshot,
    update_jsonl_inventory,
    write_filing_bundle,
)
from pipelines.utils.tables import Database


# Metric labels are analysis conveniences; original taxonomy, tag and unit remain stored.
FACT_TAGS: dict[str, tuple[str, ...]] = {
    "assets": ("Assets",),
    "current_assets": ("AssetsCurrent",),
    "cash_and_equivalents": (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ),
    "accounts_receivable": (
        "AccountsReceivableNetCurrent",
        "AccountsNotesAndLoansReceivableNetCurrent",
    ),
    "inventory": ("InventoryNet",),
    "property_plant_equipment_net": ("PropertyPlantAndEquipmentNet",),
    "current_liabilities": ("LiabilitiesCurrent",),
    "long_term_debt_current": ("LongTermDebtCurrent", "ShortTermBorrowings"),
    "long_term_debt_noncurrent": (
        "LongTermDebtNoncurrent",
        "LongTermDebtAndFinanceLeaseObligations",
    ),
    "liabilities": ("Liabilities",),
    "stockholders_equity": (
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ),
    "revenue": (
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
    ),
    "cost_of_revenue": ("CostOfRevenue", "CostOfGoodsAndServicesSold"),
    "gross_profit": ("GrossProfit",),
    "research_and_development": ("ResearchAndDevelopmentExpense",),
    "selling_general_and_administrative": ("SellingGeneralAndAdministrativeExpense",),
    "operating_income": ("OperatingIncomeLoss",),
    "interest_expense": ("InterestExpenseNonOperating", "InterestAndDebtExpense"),
    "income_tax_expense": ("IncomeTaxExpenseBenefit",),
    "net_income": ("NetIncomeLoss", "ProfitLoss"),
    "diluted_eps": ("EarningsPerShareDiluted",),
    "diluted_shares": ("WeightedAverageNumberOfDilutedSharesOutstanding",),
}
TAG_TO_METRIC = {tag: metric for metric, tags in FACT_TAGS.items() for tag in tags}
MAPPING_PROVENANCE = "comp.sec_idhist:item=CUSIP; exact CUSIP9; inclusive effective interval"
CIK_PROVENANCE = "comp.company.cik current metadata; not historically point-in-time"

SEC_FACTS_LIMITATIONS = [
    "Company Facts contains selected standardized entity-level facts, not every line of a complete filing statement.",
    "Only tags listed in this pipeline's FACT_TAGS are extracted; absent facts are not manufactured.",
    "Company Facts observations are not the original filing. Original filing links are recorded separately.",
    "Compustat CIK values are current company metadata and are not represented as historical point-in-time CIK mappings.",
]

COMP_CANDIDATE_SCHEMA = {
    "cusip9": pl.String,
    "gvkey": pl.String,
    "iid": pl.String,
    "cik": pl.String,
    "efffrom": pl.Date,
    "effthru": pl.Date,
}
BARRA_CANDIDATE_SCHEMA = {
    "cusip9": pl.String,
    "barrid": pl.String,
    "issuerid": pl.String,
    "rootid": pl.String,
    "instrument": pl.String,
    "barra_start_date": pl.Date,
    "barra_end_date": pl.Date,
}
SECURITY_MAPPING_SCHEMA = {
    "membership_date": pl.Date,
    "cusip9": pl.String,
    "russell_1000": pl.Boolean,
    "russell_2000": pl.Boolean,
    "gvkey": pl.String,
    "iid": pl.String,
    "cik": pl.String,
    "compustat_efffrom": pl.Date,
    "compustat_effthru": pl.Date,
    "mapping_source": pl.String,
    "cik_source": pl.String,
    "mapping_status": pl.String,
    "gvkey_candidate_count": pl.Int32,
    "cik_candidate_count": pl.Int32,
    "iid_candidate_count": pl.Int32,
    "barrid": pl.String,
    "issuerid": pl.String,
    "rootid": pl.String,
    "instrument": pl.String,
    "barra_start_date": pl.Date,
    "barra_end_date": pl.Date,
    "barra_mapping_status": pl.String,
}


def _sql_values(cusips: list[str]) -> str:
    if not cusips:
        raise ValueError("Cannot build a SQL VALUES clause for an empty CUSIP list")
    if any(re.fullmatch(r"[A-Za-z0-9]{9}", value) is None for value in cusips):
        raise ValueError("Only exact nine-character alphanumeric CUSIPs can be queried")
    return ",".join(f"('{value}')" for value in cusips)


def load_latest_russell_snapshot(root: str | Path, database: str) -> tuple[pl.DataFrame, Path]:
    """Read, but do not create or modify, the latest saved Russell snapshot."""
    directory = Path(root) / "groups/grp_quant/database" / database / "ftse_russell"
    files = sorted(directory.glob("ftse_russell_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No FTSE Russell Parquet files found under {directory}")

    history = pl.scan_parquet([str(path) for path in files])
    latest_date = history.select(pl.col("date").max()).collect().item()
    if latest_date is None:
        raise ValueError(f"FTSE Russell Parquet files under {directory} contain no rows")

    snapshot = (
        history.filter(
            (pl.col("date") == latest_date)
            & (pl.col("russell_1000") | pl.col("russell_2000"))
        )
        .select("date", "cusip", "russell_1000", "russell_2000")
        .group_by("cusip")
        .agg(
            pl.col("date").first(),
            pl.col("russell_1000").any(),
            pl.col("russell_2000").any(),
        )
        .sort("cusip")
        .collect()
    )
    return snapshot, directory


def normalize_cik(value: Any) -> str | None:
    if value is None:
        return None
    candidate = str(value).strip()
    if not re.fullmatch(r"\d{1,10}", candidate):
        return None
    return candidate.zfill(10)


def effective_interval_contains(start: Any, end: Any, as_of: date) -> bool:
    """Return true at either inclusive boundary of a Compustat/Barra interval."""
    start_date = parse_iso_date(start)
    end_date = parse_iso_date(end)
    return start_date is not None and start_date <= as_of and (end_date is None or end_date >= as_of)


def _empty_comp_candidates() -> pl.DataFrame:
    return pl.DataFrame(schema=COMP_CANDIDATE_SCHEMA)


def fetch_compustat_security_candidates(
    connection: wrds.Connection, snapshot: pl.DataFrame
) -> pl.DataFrame:
    """Query exact date-effective CUSIP9 to Compustat GVKEY/IID candidates."""
    as_of = snapshot.get_column("date")[0]
    cusips = [
        str(value)
        for value in snapshot.get_column("cusip").to_list()
        if re.fullmatch(r"[A-Za-z0-9]{9}", str(value))
    ]
    if not cusips:
        return _empty_comp_candidates()
    values = _sql_values(cusips)
    result = connection.raw_sql(
        f"""
        WITH universe(cusip9) AS (VALUES {values})
        SELECT u.cusip9,
               h.gvkey,
               h.iid,
               c.cik,
               h.efffrom,
               h.effthru
        FROM universe u
        LEFT JOIN comp.sec_idhist h
          ON h.item = 'CUSIP'
         AND UPPER(h.itemvalue) = UPPER(u.cusip9)
         AND h.efffrom <= DATE '{as_of.isoformat()}'
         AND (h.effthru IS NULL OR h.effthru >= DATE '{as_of.isoformat()}')
        LEFT JOIN comp.company c ON c.gvkey = h.gvkey
        """
    )
    frame = pl.from_pandas(result).with_columns(
        pl.col("cusip9", "gvkey", "iid", "cik").cast(pl.String, strict=False)
    )
    return frame.with_columns(
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.slice(0, 10)
        .str.to_date(strict=False)
        .alias(column)
        for column in ("efffrom", "effthru")
    )


def _empty_barra_candidates() -> pl.DataFrame:
    return pl.DataFrame(schema=BARRA_CANDIDATE_SCHEMA)


def load_barra_security_candidates(
    root: str | Path, database: str, snapshot: pl.DataFrame
) -> tuple[pl.DataFrame, bool]:
    """Read the existing date-effective Barra CUSIP and issuer ID files."""
    base = Path(root) / "groups/grp_quant/database" / database
    barra_path = base / "barra_ids" / "barra_ids.parquet"
    issuer_path = base / "asset_ids" / "asset_ids.parquet"
    if not barra_path.exists() or not issuer_path.exists():
        return _empty_barra_candidates(), False

    as_of = snapshot.get_column("date")[0]
    membership_cusips = snapshot.select(
        pl.col("cusip").cast(pl.String).alias("cusip9")
    ).unique()
    barra_ids = pl.read_parquet(barra_path).filter(
        (pl.col("asset_id_type") == "CUSIP")
        & pl.col("barrid").str.starts_with("US")
        & (pl.col("start_date") <= pl.lit(as_of))
        & (pl.col("end_date").is_null() | (pl.col("end_date") >= pl.lit(as_of)))
    )
    barra_matches = barra_ids.join(
        membership_cusips,
        left_on="asset_id",
        right_on="cusip9",
        how="inner",
    ).select(
        pl.col("asset_id").alias("cusip9"),
        "barrid",
        pl.col("start_date").alias("barra_start_date"),
        pl.col("end_date").alias("barra_end_date"),
    )
    issuer_ids = pl.read_parquet(issuer_path).filter(
        (pl.col("start_date") <= pl.lit(as_of))
        & (pl.col("end_date").is_null() | (pl.col("end_date") >= pl.lit(as_of)))
    ).select("barrid", "issuerid", "rootid", "instrument")
    return (
        barra_matches.join(issuer_ids, on="barrid", how="left")
        .select(
            "cusip9",
            "barrid",
            "issuerid",
            "rootid",
            "instrument",
            "barra_start_date",
            "barra_end_date",
        )
        .unique()
        .with_columns(pl.col("cusip9", "barrid", "issuerid", "rootid", "instrument").cast(pl.String))
    ), True


def _candidate_status(
    cusip: str,
    comp_candidates: list[dict[str, Any]],
    barra_candidates: list[dict[str, Any]],
    barra_available: bool,
) -> tuple[str, str, int, int, int]:
    valid_gvkeys = {str(row["gvkey"]) for row in comp_candidates if row.get("gvkey")}
    valid_ciks = {
        normalize_cik(row.get("cik"))
        for row in comp_candidates
        if normalize_cik(row.get("cik")) is not None
    }
    raw_nonempty_ciks = {
        str(row.get("cik")).strip()
        for row in comp_candidates
        if row.get("cik") is not None and str(row.get("cik")).strip()
    }
    valid_iids = {str(row["iid"]) for row in comp_candidates if row.get("iid")}

    if re.fullmatch(r"[A-Za-z0-9]{9}", cusip) is None:
        mapping_status = "invalid_cusip_format"
    elif not valid_gvkeys:
        mapping_status = "no_gvkey_match"
    elif len(valid_gvkeys) > 1:
        mapping_status = "ambiguous_gvkey"
    elif len(valid_ciks) > 1:
        mapping_status = "ambiguous_cik"
    elif len(valid_ciks) == 1:
        mapping_status = "mapped_current_cik_metadata"
    elif raw_nonempty_ciks:
        mapping_status = "invalid_cik"
    else:
        mapping_status = "missing_cik"

    if not barra_available:
        barra_status = "unavailable"
    else:
        barrids = {str(row["barrid"]) for row in barra_candidates if row.get("barrid")}
        issuerids = {str(row["issuerid"]) for row in barra_candidates if row.get("issuerid")}
        if not barrids:
            barra_status = "unmatched"
        elif len(barrids) > 1:
            barra_status = "ambiguous_barrid"
        elif len(issuerids) > 1:
            barra_status = "ambiguous_issuerid"
        elif not issuerids:
            barra_status = "missing_issuerid"
        else:
            barra_status = "mapped"

    return (
        mapping_status,
        barra_status,
        len(valid_gvkeys),
        len(valid_ciks),
        len(valid_iids),
    )


def build_security_mapping(
    snapshot: pl.DataFrame,
    comp_candidates: pl.DataFrame,
    barra_candidates: pl.DataFrame | None = None,
    barra_available: bool = False,
) -> pl.DataFrame:
    """Keep every valid Compustat issue candidate and each exact Barra candidate."""
    as_of_by_cusip = {
        str(row["cusip"]): row["date"] for row in snapshot.select("cusip", "date").to_dicts()
    }
    comp_by_cusip: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in comp_candidates.to_dicts():
        cusip = str(row["cusip9"])
        if row.get("gvkey") and cusip in as_of_by_cusip and effective_interval_contains(
            row.get("efffrom"), row.get("effthru"), as_of_by_cusip[cusip]
        ):
            comp_by_cusip[cusip].append(row)

    barra_by_cusip: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if barra_candidates is not None:
        for row in barra_candidates.to_dicts():
            cusip = str(row["cusip9"])
            if row.get("barrid") and cusip in as_of_by_cusip and effective_interval_contains(
                row.get("barra_start_date"), row.get("barra_end_date"), as_of_by_cusip[cusip]
            ):
                barra_by_cusip[cusip].append(row)

    output: list[dict[str, Any]] = []
    for member in snapshot.to_dicts():
        cusip = str(member["cusip"])
        date_value = member["date"]
        comp_rows = comp_by_cusip.get(cusip, [])
        barra_rows = barra_by_cusip.get(cusip, [])
        mapping_status, barra_status, gvkey_count, cik_count, iid_count = _candidate_status(
            cusip, comp_rows, barra_rows, barra_available
        )
        # Keep one synthetic row for unresolved securities so they remain visible.
        if not comp_rows:
            comp_rows = [
                {
                    "gvkey": None,
                    "iid": None,
                    "cik": None,
                    "efffrom": None,
                    "effthru": None,
                }
            ]
        if not barra_rows:
            barra_rows = [
                {
                    "barrid": None,
                    "issuerid": None,
                    "rootid": None,
                    "instrument": None,
                    "barra_start_date": None,
                    "barra_end_date": None,
                }
            ]
        for candidate in comp_rows:
            for barra in barra_rows:
                output.append(
                    {
                        "membership_date": date_value,
                        "cusip9": cusip,
                        "russell_1000": bool(member["russell_1000"]),
                        "russell_2000": bool(member["russell_2000"]),
                        "gvkey": str(candidate["gvkey"]) if candidate.get("gvkey") else None,
                        "iid": str(candidate["iid"]) if candidate.get("iid") else None,
                        "cik": normalize_cik(candidate.get("cik")),
                        "compustat_efffrom": parse_iso_date(candidate.get("efffrom")),
                        "compustat_effthru": parse_iso_date(candidate.get("effthru")),
                        "mapping_source": MAPPING_PROVENANCE,
                        "cik_source": CIK_PROVENANCE,
                        "mapping_status": mapping_status,
                        "gvkey_candidate_count": gvkey_count,
                        "cik_candidate_count": cik_count,
                        "iid_candidate_count": iid_count,
                        "barrid": barra.get("barrid"),
                        "issuerid": barra.get("issuerid"),
                        "rootid": barra.get("rootid"),
                        "instrument": barra.get("instrument"),
                        "barra_start_date": parse_iso_date(barra.get("barra_start_date")),
                        "barra_end_date": parse_iso_date(barra.get("barra_end_date")),
                        "barra_mapping_status": barra_status,
                    }
                )
    return pl.DataFrame(output, schema=SECURITY_MAPPING_SCHEMA).unique().sort(
        ["cusip9", "gvkey", "iid", "barrid"], nulls_last=True
    )


def group_security_relationships_by_cik(mapping: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Group mapped securities without replacing multiple members of one CIK."""
    grouped: dict[str, dict[tuple[str, str, str], dict[str, Any]]] = defaultdict(dict)
    for row in mapping.to_dicts():
        cik = normalize_cik(row.get("cik"))
        if not cik or row.get("mapping_status") != "mapped_current_cik_metadata":
            continue
        identity = (
            str(row.get("cusip9")),
            str(row.get("gvkey")),
            str(row.get("iid")),
            str(row.get("barrid")),
            str(row.get("issuerid")),
        )
        grouped[cik][identity] = {
            "cusip9": row.get("cusip9"),
            "gvkey": row.get("gvkey"),
            "iid": row.get("iid"),
            "membership_date": row.get("membership_date"),
            "russell_1000": row.get("russell_1000"),
            "russell_2000": row.get("russell_2000"),
            "compustat_efffrom": row.get("compustat_efffrom"),
            "compustat_effthru": row.get("compustat_effthru"),
            "mapping_source": row.get("mapping_source"),
            "barrid": row.get("barrid"),
            "issuerid": row.get("issuerid"),
            "barra_mapping_status": row.get("barra_mapping_status"),
        }
    return {cik: list(rows.values()) for cik, rows in grouped.items()}


def _validated_response_ciks(mapping: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
    return group_security_relationships_by_cik(mapping)


def _sec_request_failure(error: requests.RequestException) -> str:
    """Describe the failure category and traceback without exposing request headers."""
    details = str(error).lower()
    if isinstance(error, requests.exceptions.SSLError):
        if "system default config" in details or "sslcontext" in details:
            category = "Python/OpenSSL TLS runtime initialization failed"
        elif "certificate_verify_failed" in details or "certificate verify failed" in details:
            category = "SEC TLS certificate validation failed"
        else:
            category = "SEC TLS handshake failed"
    elif isinstance(error, requests.exceptions.ProxyError):
        category = "SEC proxy connection failed"
    elif isinstance(error, requests.exceptions.Timeout):
        category = "SEC request timed out"
    elif isinstance(error, requests.exceptions.ConnectionError):
        category = "SEC network connection failed"
    else:
        category = "SEC request failed"

    trace = "".join(traceback.format_exception(type(error), error, error.__traceback__)).rstrip()
    return (
        f"{category}. The security mapping snapshot was saved before SEC requests; "
        "earlier SEC filing outputs may also have been saved.\n"
        f"Underlying exception: {type(error).__name__}: {error}\n"
        f"Traceback:\n{trace}"
    )


def _make_sec_session(user_agent: str) -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _fetch_and_archive_json(
    session: requests.Session,
    url: str,
    output_root: Path,
    cik: str,
    endpoint: str,
) -> dict[str, Any]:
    response = session.get(url, timeout=(10, 60))
    archive_path, digest, retrieved_at = archive_api_response(
        output_root, cik, endpoint, response.content
    )
    time.sleep(0.12)
    if response.status_code == 404:
        return {
            "data": None,
            "status_code": 404,
            "sha256": digest,
            "path": archive_path,
            "retrieved_at": retrieved_at,
            "url": url,
        }
    response.raise_for_status()
    try:
        payload = json.loads(response.content, parse_float=Decimal)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(f"SEC returned non-JSON content from {url}") from error
    return {
        "data": payload,
        "status_code": response.status_code,
        "sha256": digest,
        "path": archive_path,
        "retrieved_at": retrieved_at,
        "url": url,
    }


def _submission_records(
    payload: dict[str, Any],
    source_url: str,
    source_hash: str,
    source_retrieved_at: datetime,
    source_archive_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    filings = payload.get("filings", {})
    recent = filings.get("recent", {}) if isinstance(filings, dict) else {}
    if not recent and isinstance(payload.get("recent"), dict):
        recent = payload["recent"]
    if not recent and "accessionNumber" in payload:
        recent = payload
    if not isinstance(recent, dict):
        recent = {}

    names = (
        "accessionNumber",
        "filingDate",
        "reportDate",
        "acceptanceDateTime",
        "form",
        "primaryDocument",
    )
    row_count = max((len(recent.get(name, [])) for name in names), default=0)
    records = []
    for index in range(row_count):
        record = {
            name: recent.get(name, [None] * row_count)[index]
            if index < len(recent.get(name, []))
            else None
            for name in names
        }
        accession = record.get("accessionNumber")
        if accession:
            record["_metadata_url"] = source_url
            record["_metadata_sha256"] = source_hash
            record["_metadata_retrieved_at"] = source_retrieved_at
            record["_metadata_archive_path"] = source_archive_path
            records.append(record)

    files = filings.get("files", []) if isinstance(filings, dict) else []
    if not isinstance(files, list):
        files = []
    return records, files


def _company_filing_relationships(
    securities: list[dict[str, Any]], cik: str
) -> list[dict[str, Any]]:
    gvkeys = sorted({str(row.get("gvkey")) for row in securities if row.get("gvkey")})
    return [
        {
            "gvkey": gvkey,
            "relationship_source": MAPPING_PROVENANCE,
            "cik_source": CIK_PROVENANCE,
            "membership_date": str(
                next(
                    row.get("membership_date")
                    for row in securities
                    if str(row.get("gvkey")) == gvkey
                )
            ),
            "historical_cik_point_in_time": False,
        }
        for gvkey in gvkeys
    ]


def _filing_inventory_record(
    cik: str,
    filing: dict[str, Any],
    filing_dir: Path,
    output_root: Path,
    snapshot_id: str,
    extraction_status: str,
    balance_count: int,
    income_count: int,
) -> dict[str, Any]:
    filing_date = parse_iso_date(filing.get("filingDate"))
    report_period = parse_iso_date(filing.get("reportDate"))
    return {
        "cik": cik,
        "accession": filing.get("accessionNumber"),
        "form": filing.get("form"),
        "filing_date": filing_date.isoformat() if filing_date else None,
        "report_period": report_period.isoformat() if report_period else None,
        "extraction_status": extraction_status,
        "balance_sheet_rows": balance_count,
        "income_statement_rows": income_count,
        "source_snapshot_id": snapshot_id,
        "filing_path": str(filing_dir.relative_to(output_root)),
    }


def _archive_company(
    cik: str,
    securities: list[dict[str, Any]],
    output_root: Path,
    session: requests.Session,
) -> tuple[int, int]:
    padded_cik = cik.zfill(10)
    facts_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded_cik}.json"
    submissions_url = f"https://data.sec.gov/submissions/CIK{padded_cik}.json"
    facts_response = _fetch_and_archive_json(
        session, facts_url, output_root, cik, "companyfacts"
    )
    submissions_response = _fetch_and_archive_json(
        session, submissions_url, output_root, cik, "submissions"
    )
    company_status = "ready"
    if submissions_response["status_code"] == 404:
        company_status = "submissions_not_found"
    elif facts_response["status_code"] == 404:
        company_status = "companyfacts_not_found"

    update_jsonl_inventory(
        output_root / "company_inventory.jsonl",
        {
            "cik": cik,
            "status": company_status,
            "companyfacts_sha256": facts_response["sha256"],
            "submissions_sha256": submissions_response["sha256"],
            "companyfacts_retrieved_at": facts_response["retrieved_at"].isoformat(),
            "submissions_retrieved_at": submissions_response["retrieved_at"].isoformat(),
        },
        ("cik", "accession"),
    )
    if submissions_response["data"] is None:
        return 0, 0

    records, older_files = _submission_records(
        submissions_response["data"],
        submissions_response["url"],
        submissions_response["sha256"],
        submissions_response["retrieved_at"],
        submissions_response["path"],
    )
    # Supplemental files make the accession inventory complete beyond SEC's recent window.
    for older_file in older_files:
        name = older_file.get("name") if isinstance(older_file, dict) else None
        if not name or not re.fullmatch(r"[A-Za-z0-9_.-]+\.json", str(name)):
            continue
        older_url = f"https://data.sec.gov/submissions/{name}"
        older_response = _fetch_and_archive_json(
            session, older_url, output_root, cik, "submissions_history"
        )
        if older_response["data"] is None:
            continue
        older_records, _ = _submission_records(
            older_response["data"],
            older_url,
            older_response["sha256"],
            older_response["retrieved_at"],
            older_response["path"],
        )
        records.extend(older_records)

    # Prefer the top-level recent metadata if an accession appears in both sources.
    filing_by_accession: dict[str, dict[str, Any]] = {}
    for record in records:
        accession = str(record["accessionNumber"])
        if accession not in filing_by_accession or record.get("_metadata_url") == submissions_url:
            filing_by_accession[accession] = record

    relationships = _company_filing_relationships(securities, cik)
    filings_written = 0
    filings_without_supported_facts = 0
    facts_payload = facts_response["data"] or {}
    for accession in sorted(filing_by_accession):
        filing = filing_by_accession[accession]
        filing_date = parse_iso_date(filing.get("filingDate"))
        if filing_date is None:
            # Malformed SEC metadata remains visible in the company inventory.
            update_jsonl_inventory(
                output_root / "company_inventory.jsonl",
                {
                    "cik": cik,
                    "status": "invalid_filing_metadata",
                    "accession": accession,
                    "companyfacts_sha256": facts_response["sha256"],
                    "submissions_sha256": submissions_response["sha256"],
                },
                ("cik", "accession"),
            )
            continue

        balance, income = extract_accession_facts(
            cik, accession, filing, facts_payload, TAG_TO_METRIC
        )
        if not filing_form_supported(filing.get("form")):
            extraction_status = "unsupported_form"
        elif balance.is_empty() and income.is_empty():
            extraction_status = "no_supported_facts"
            filings_without_supported_facts += 1
        else:
            extraction_status = "stored_selected_facts"

        primary_document = filing.get("primaryDocument")
        report = {
            "schema_version": 1,
            "form": filing.get("form"),
            "accession": accession,
            "cik": cik,
            "gvkey_relationships": relationships,
            "gvkey_relationship_basis": "Current Russell membership-date CUSIP history plus current Compustat company CIK metadata; not historical CIK point-in-time.",
            "report_period": parse_iso_date(filing.get("reportDate")),
            "filing_date": filing_date,
            "acceptance_timestamp": filing.get("acceptanceDateTime"),
            "primary_document": primary_document,
            "source_url": filing_source_url(cik, accession, primary_document),
            "submissions_api_url": filing.get("_metadata_url"),
            "companyfacts_api_url": facts_url,
            "submission_metadata_response_path": str(
                filing["_metadata_archive_path"].relative_to(output_root)
            ),
            "companyfacts_response_path": str(
                facts_response["path"].relative_to(output_root)
            ),
            "submission_metadata_sha256": filing.get("_metadata_sha256"),
            "companyfacts_sha256": facts_response["sha256"],
            "submission_metadata_retrieved_at": filing.get("_metadata_retrieved_at"),
            "companyfacts_retrieved_at": facts_response["retrieved_at"],
            "extraction_status": extraction_status,
            "balance_sheet_rows": balance.height,
            "income_statement_rows": income.height,
            "extraction_limitations": SEC_FACTS_LIMITATIONS,
        }
        report_for_json = json.loads(json.dumps(report, default=str))
        filing_dir, snapshot_id, _ = write_filing_bundle(
            output_root,
            filing,
            accession,
            cik,
            report_for_json,
            balance,
            income,
        )
        update_jsonl_inventory(
            output_root / "filing_inventory.jsonl",
            _filing_inventory_record(
                cik,
                filing,
                filing_dir,
                output_root,
                snapshot_id,
                extraction_status,
                balance.height,
                income.height,
            ),
            ("cik", "accession"),
        )
        filings_written += 1
    return filings_written, filings_without_supported_facts


def sec_fundamentals_flow(
    source_database: Database,
    target_database: Database,
    limit: int | None = None,
    output_root: str | Path | None = None,
) -> None:
    """Fetch SEC filings once per validated CIK and store accession-level outputs."""
    load_dotenv(override=True)
    root = os.getenv("ROOT")
    user = os.getenv("WRDS_USER")
    user_agent = os.getenv("SEC_USER_AGENT")
    if not root:
        raise EnvironmentError("Missing required environment variable: ROOT")
    if not user:
        raise EnvironmentError("Missing required environment variable: WRDS_USER")
    if not user_agent or re.search(r"Your Organization|contact@example\.com", user_agent, re.I):
        raise EnvironmentError(
            "Set SEC_USER_AGENT to an identifying organization/app name and contact email."
        )

    source_name = source_database.database_name.value
    target_name = target_database.database_name.value
    snapshot, _ = load_latest_russell_snapshot(root, source_name)
    selected_output_root = (
        Path(output_root)
        if output_root is not None
        else Path(root) / "groups/grp_quant/database" / target_name / "sec_filings"
    )

    connection = wrds.Connection(wrds_username=user)
    try:
        comp_candidates = fetch_compustat_security_candidates(connection, snapshot)
    finally:
        connection.close()

    barra_candidates, barra_available = load_barra_security_candidates(
        root, source_name, snapshot
    )
    mapping = build_security_mapping(
        snapshot, comp_candidates, barra_candidates, barra_available
    )
    membership_date = snapshot.get_column("date")[0]
    mapping_path = persist_mapping_snapshot(selected_output_root, mapping, membership_date)
    print(f"Saved security mapping snapshot: {mapping_path}", flush=True)
    per_security_status = mapping.group_by("cusip9").agg(
        pl.col("mapping_status").first(),
        pl.col("barra_mapping_status").first(),
    )
    print(
        "Compustat mapping status counts: "
        f"{per_security_status.group_by('mapping_status').len().sort('mapping_status').to_dicts()}",
        flush=True,
    )
    print(
        "Barra mapping status counts: "
        f"{per_security_status.group_by('barra_mapping_status').len().sort('barra_mapping_status').to_dicts()}",
        flush=True,
    )

    securities_by_cik = _validated_response_ciks(mapping)
    selected_ciks = sorted(securities_by_cik)
    if limit is not None:
        selected_ciks = selected_ciks[:limit]
    print(
        f"Fetching SEC submissions and Company Facts for {len(selected_ciks)} unique CIKs "
        f"from {len(securities_by_cik)} mapped CIKs; output root: {selected_output_root}",
        flush=True,
    )

    session = _make_sec_session(user_agent)
    total_filings = 0
    total_without_facts = 0
    try:
        for index, cik in enumerate(selected_ciks, start=1):
            filings, no_facts = _archive_company(
                cik,
                securities_by_cik[cik],
                selected_output_root,
                session,
            )
            total_filings += filings
            total_without_facts += no_facts
            print(
                f"Processed CIK {index}/{len(selected_ciks)} ({cik}): "
                f"{filings} accessions, {no_facts} without supported facts.",
                flush=True,
            )
    except requests.RequestException as error:
        raise ClickException(_sec_request_failure(error)) from None
    print(
        f"Archived {total_filings} accession folders; "
        f"{total_without_facts} have no supported Company Facts observations.",
        flush=True,
    )
