"""Build a policy-scoped resolved quarterly Compustat view from saved evidence."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

import polars as pl

from pipelines.sec_ocf_cohort import inline_ocf_facts
from pipelines.utils.fundamentals_schema import fundamentals_table_schema


POLICY_VERSION = "sec-ocf-ytd-reviewed-v5"
WFC = {
    "cik": "0000072971",
    "gvkey": "008007",
    "barrid": "USAK1N1",
    "accession": "0000072971-26-000327",
    "taxonomy": "us-gaap",
    "tag": "NetCashProvidedByUsedInOperatingActivities",
    "unit": "USD",
    "period_start": date(2026, 1, 1),
    "period_end": date(2026, 9, 30),
    "fiscal_year": 2026,
    "fiscal_period": "Q3",
    "fiscal_quarter": 3,
    "fiscal_year_end_month": 12,
    "usd_to_compustat_millions": Decimal("0.000001"),
}

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

MECHANICS = {
    "cik": "0001518715",
    "gvkey": "018948",
    "barrid": "USAHMH1",
    "period_start": date(2024, 1, 1),
    "period_end": date(2024, 9, 30),
    "concept": "NetCashProvidedByUsedInOperatingActivities",
    "original_accession": "0001518715-24-000181",
    "comparative_accession": "0001518715-25-000181",
}

SEC_OCF_CANDIDATE_DECISIONS = {
    "008007|2026-09-30": {"classification": "accepted", "reason": "verified archived 10-Q and consolidated YTD context"},
    "018948|2024-09-30": {"classification": "requiring_identity_review", "reason": "reverse acquisition changes accounting predecessor represented by comparative statements"},
    "018626|2024-09-30": {"classification": "incompatible", "reason": "supported us-gaap coverage is absent"},
}


class ResolutionError(ValueError):
    """Raised when archived evidence cannot support a unique fallback."""


class _ContextAndFactParser(HTMLParser):
    """Read just the relevant inline fact and its XBRL context from saved HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.contexts: dict[str, dict[str, Any]] = {}
        self.current_context: str | None = None
        self.capture_kind: str | None = None
        self.capture_dimension: str | None = None
        self.capture_value: list[str] = []
        self.facts: list[dict[str, Any]] = []
        self.current_fact: dict[str, Any] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = {key.lower(): value for key, value in attrs}
        tag = tag.lower()
        if tag.endswith(":context") or tag == "context":
            self.current_context = attrs_map.get("id")
            if self.current_context:
                self.contexts[self.current_context] = {"identifier": None, "start": None, "end": None, "dimensions": []}
        elif self.current_context:
            if tag.endswith(":identifier"):
                self.capture_kind, self.capture_dimension, self.capture_value = "identifier", None, []
            elif tag.endswith(":startdate"):
                self.capture_kind, self.capture_dimension, self.capture_value = "start", None, []
            elif tag.endswith(":enddate"):
                self.capture_kind, self.capture_dimension, self.capture_value = "end", None, []
            elif tag.endswith(":explicitmember") or tag.endswith(":typedmember"):
                self.capture_kind = "dimension"
                self.capture_dimension = attrs_map.get("dimension")
                self.capture_value = []
        if (
            tag == "ix:nonfraction"
            and attrs_map.get("name", "").lower() == "us-gaap:netcashprovidedbyusedinoperatingactivities"
        ):
            self.current_fact = {"attrs": attrs_map, "text": ""}

    def handle_data(self, data: str) -> None:
        if self.capture_kind and self.current_context:
            self.capture_value.append(data.strip())
        if self.current_fact is not None:
            self.current_fact["text"] += data.strip()

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self.current_context and self.capture_kind:
            expected = {"identifier": "identifier", "start": "startdate", "end": "enddate", "dimension": "member"}[self.capture_kind]
            if tag.endswith(expected):
                value = "".join(self.capture_value)
                context = self.contexts[self.current_context]
                if self.capture_kind == "dimension":
                    context["dimensions"].append({"dimension": self.capture_dimension, "member": value})
                else:
                    context[self.capture_kind] = value
                self.capture_kind, self.capture_dimension, self.capture_value = None, None, []
        if (tag.endswith(":context") or tag == "context") and self.current_context:
            self.current_context = None
        if tag == "ix:nonfraction" and self.current_fact is not None:
            attrs = self.current_fact["attrs"]
            try:
                numeric = Decimal(self.current_fact["text"].replace(",", "").replace("$", ""))
                numeric *= Decimal(10) ** int(attrs.get("scale") or "0")
                if attrs.get("sign") == "-":
                    numeric = -numeric
            except (InvalidOperation, ValueError, OverflowError):
                numeric = None
            self.current_fact["scaled_value"] = numeric
            self.facts.append(self.current_fact)
            self.current_fact = None


def _filing_visible_text(document: bytes) -> str:
    class _Text(HTMLParser):
        def __init__(self) -> None:
            super().__init__(convert_charrefs=True)
            self.parts: list[str] = []

        def handle_data(self, data: str) -> None:
            if data.strip():
                self.parts.append(data.strip())

    parser = _Text()
    parser.feed(document.decode("utf-8", errors="replace"))
    return " ".join(parser.parts).lower()


def verify_mechanics_discrepancy(
    facts_payload: dict[str, Any],
    submissions: dict[str, Any],
    original_html: bytes,
    comparative_html: bytes,
    *,
    cutoff: str,
    companyfacts_sha256: str,
    submissions_sha256: str,
    original_html_sha256: str,
    comparative_html_sha256: str,
) -> dict[str, Any]:
    """Verify both Mechanics facts and fail closed on the reverse-acquisition entity break."""
    cutoff_dt = parse_aware_timestamp(cutoff)
    if str(facts_payload.get("cik", "")).zfill(10) != MECHANICS["cik"]:
        raise ResolutionError("Mechanics Company Facts CIK mismatch")
    observations = facts_payload.get("facts", {}).get("us-gaap", {}).get(
        MECHANICS["concept"], {}
    ).get("units", {}).get("USD", [])
    exact = [
        row for row in observations
        if _date(row.get("start")) == MECHANICS["period_start"]
        and _date(row.get("end")) == MECHANICS["period_end"]
    ]
    by_accession = {str(row.get("accn")): row for row in exact}
    if set((MECHANICS["original_accession"], MECHANICS["comparative_accession"])) - set(by_accession):
        raise ResolutionError("saved Company Facts lacks one of the exact Mechanics duration observations")
    recent = submissions.get("filings", {}).get("recent", {})
    filings = {}
    accessions = recent.get("accessionNumber", [])
    for i, accession in enumerate(accessions):
        filings[accession] = {
            key: recent.get(key, [None] * len(accessions))[i]
            for key in ("form", "filingDate", "reportDate", "acceptanceDateTime", "primaryDocument")
        }
    original_facts_parser, comparative_facts_parser = _ContextAndFactParser(), _ContextAndFactParser()
    original_facts_parser.feed(original_html.decode("utf-8", errors="replace"))
    comparative_facts_parser.feed(comparative_html.decode("utf-8", errors="replace"))
    payload = {}
    for label, accession, parser in (
        ("original", MECHANICS["original_accession"], original_facts_parser),
        ("comparative", MECHANICS["comparative_accession"], comparative_facts_parser),
    ):
        filing = filings.get(accession)
        obs = by_accession[accession]
        expected_report_date = "2024-09-30" if label == "original" else "2025-09-30"
        expected_fy = 2024 if label == "original" else 2025
        if not filing or filing.get("form") != "10-Q":
            raise ResolutionError(f"saved Mechanics {label} accession is not a 10-Q")
        if filing.get("reportDate") != expected_report_date:
            raise ResolutionError(f"saved Mechanics {label} filing report date changed unexpectedly")
        if obs.get("form") != "10-Q" or obs.get("unit") not in {None, "USD"}:
            raise ResolutionError(f"saved Mechanics {label} fact has incompatible form or unit")
        if obs.get("fy") != expected_fy or obs.get("fp") != "Q3":
            raise ResolutionError(f"saved Mechanics {label} fiscal labels changed unexpectedly")
        matches = [fact for fact in parser.facts if fact["attrs"].get("contextref") in parser.contexts]
        matches = [fact for fact in matches if fact["scaled_value"] == Decimal(str(obs["val"]))]
        if len(matches) != 1:
            raise ResolutionError(f"Mechanics {label} filing does not uniquely corroborate Company Facts")
        fact = matches[0]
        context = parser.contexts[fact["attrs"]["contextref"]]
        if (
            fact["attrs"].get("unitref", "").lower() != "usd"
            or fact["attrs"].get("name", "").lower() != f"us-gaap:{MECHANICS['concept'].lower()}"
            or _date(context.get("start")) != MECHANICS["period_start"]
            or _date(context.get("end")) != MECHANICS["period_end"]
            or context.get("identifier") != MECHANICS["cik"]
        ):
            raise ResolutionError(f"Mechanics {label} filing context does not match the saved exact period/entity")
        accepted = parse_aware_timestamp(str(filing.get("acceptanceDateTime")))
        if accepted > cutoff_dt:
            raise ResolutionError(f"Mechanics {label} filing was accepted after comparison cutoff")
        payload[label] = {
            "accession": accession,
            "form": filing["form"],
            "filing_date": filing["filingDate"],
            "report_date": filing["reportDate"],
            "acceptance_timestamp": filing["acceptanceDateTime"],
            "fy": obs.get("fy"),
            "fp": obs.get("fp"),
            "period_start": MECHANICS["period_start"].isoformat(),
            "period_end": MECHANICS["period_end"].isoformat(),
            "taxonomy": "us-gaap",
            "concept": MECHANICS["concept"],
            "unit": "USD",
            "raw_value": str(obs["val"]),
            "inline_display": fact["text"],
            "inline_sign": fact["attrs"].get("sign"),
            "inline_scale": fact["attrs"].get("scale"),
            "inline_decimals": fact["attrs"].get("decimals"),
            "context_id": fact["attrs"].get("contextref"),
            "context_entity_cik": context["identifier"],
            "context_dimensions": context["dimensions"],
        }
    # Decode the filing for identity and accounting-predecessor statements.
    from html.parser import HTMLParser as _HTMLTextParser

    class _Text(_HTMLTextParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.parts: list[str] = []

        def handle_data(self, data: str) -> None:
            if data.strip():
                self.parts.append(data.strip())

    original_text, comparative_text = _Text(), _Text()
    original_text.feed(original_html.decode("utf-8", errors="replace"))
    comparative_text.feed(comparative_html.decode("utf-8", errors="replace"))
    original_text_value = " ".join(original_text.parts).lower().replace("’", "'")
    comparative_text_value = " ".join(comparative_text.parts).lower().replace("’", "'")
    required_disclosure = (
        "reverse acquisition" in comparative_text_value
        and "mechanics bank is the accounting acquirer" in comparative_text_value
        and "homestreet bank is the accounting acquiree" in comparative_text_value
        and "reflect legacy mechanics bank's results only on a standalone basis" in comparative_text_value
    )
    if "homestreet, inc. and subsidiaries" not in original_text_value:
        raise ResolutionError("original Mechanics-period filing does not identify HomeStreet as the reporting entity")
    if "mechanics bancorp and subsidiaries" not in comparative_text_value or not required_disclosure:
        raise ResolutionError("comparative filing lacks the saved reverse-acquisition predecessor disclosure")
    if "consolidated statements of cash flows" not in original_text_value or "consolidated statements of cash flows" not in comparative_text_value:
        raise ResolutionError("both filed values must come from consolidated cash-flow statements")
    if any(payload[name]["context_dimensions"] for name in ("original", "comparative")):
        raise ResolutionError("a filing fact has dimensional context; consolidation comparability needs review")
    if not (
        Decimal(payload["original"]["raw_value"]) == Decimal("-47042000")
        and Decimal(payload["comparative"]["raw_value"]) == Decimal("217358000")
    ):
        raise ResolutionError("Mechanics source values differ from the documented discrepancy")
    payload.update({
        "cik": MECHANICS["cik"],
        "gvkey": MECHANICS["gvkey"],
        "barrid": MECHANICS["barrid"],
        "companyfacts_entity_name": facts_payload.get("entityName"),
        "submission_entity_name": submissions.get("name"),
        "original_statement_entity": "HomeStreet, Inc. and subsidiaries",
        "comparative_statement_entity": "Mechanics Bancorp and subsidiaries",
        "filing_context_scope": "Both facts use the default consolidated statement context with no XBRL dimensions.",
        "cause": "reverse-acquisition accounting predecessor change, not a verified restatement of HomeStreet's same-period operating cash flow",
        "decision": "exclude_and_remain_unresolved",
        "decision_reason": (
            "The later 2025 comparative is applicable to the same duration and concept but represents legacy Mechanics Bank, the accounting acquirer, on a standalone basis. The 2024 filing represents HomeStreet, Inc. The CIK is shared because HomeStreet, Inc. became Mechanics Bancorp, the legal acquirer; this does not establish the later comparative as a revision of the original HomeStreet period."
        ),
        "companyfacts_sha256": companyfacts_sha256,
        "submissions_sha256": submissions_sha256,
        "original_filing_html_sha256": original_html_sha256,
        "comparative_filing_html_sha256": comparative_html_sha256,
        "comparison_cutoff": cutoff_dt.isoformat(),
        "cutoff_basis": "supplied comparison-policy cutoff; not original collection time",
    })
    return payload


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


def _submission_filings(submissions: dict[str, Any]) -> list[dict[str, Any]]:
    recent = submissions.get("filings", {}).get("recent", {})
    accessions = recent.get("accessionNumber", [])
    fields = ("filingDate", "reportDate", "acceptanceDateTime", "form", "primaryDocument")
    result = []
    for i, accession in enumerate(accessions):
        row = {key: recent.get(key, [None] * len(accessions))[i] for key in fields}
        row["accessionNumber"] = accession
        if row["reportDate"] == WFC["period_end"].isoformat() and row["form"] in {"10-Q", "10-Q/A"}:
            result.append(row)
    return result


def verify_wells_fargo_evidence(
    facts: dict[str, Any],
    submissions: dict[str, Any],
    filing_html: bytes | dict[str, bytes],
    *,
    cutoff: str,
    companyfacts_sha256: str,
    submissions_sha256: str,
    filing_html_sha256: str,
    mapping_snapshot_path: str,
    mapping_snapshot_sha256: str,
    decision_case: dict[str, Any],
) -> dict[str, Any]:
    """Validate the single allowlisted SEC observation against saved primary evidence."""
    cutoff_dt = parse_aware_timestamp(cutoff)
    for digest in (companyfacts_sha256, submissions_sha256, filing_html_sha256, mapping_snapshot_sha256):
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ResolutionError("all source hashes must be lowercase SHA-256 values")
    if str(facts.get("cik", "")).zfill(10) != WFC["cik"]:
        raise ResolutionError("Company Facts CIK does not match the Wells Fargo allowlist")
    fact_name = re.sub(r"[^a-z0-9]", "", str(facts.get("entityName", "")).lower())
    submission_name = re.sub(r"[^a-z0-9]", "", str(submissions.get("name", "")).lower())
    if not fact_name or fact_name != submission_name:
        raise ResolutionError("SEC entity names do not match")
    taxonomy = facts.get("facts", {}).get(WFC["taxonomy"], {})
    if not taxonomy or WFC["tag"] not in taxonomy:
        raise ResolutionError("required us-gaap concept is absent")
    observations = taxonomy[WFC["tag"]].get("units", {}).get(WFC["unit"], [])
    exact_period = [
        obs for obs in observations
        if _date(obs.get("start")) == WFC["period_start"]
        and _date(obs.get("end")) == WFC["period_end"]
    ]
    filings = _submission_filings(submissions)
    filing_by_accession = {row["accessionNumber"]: row for row in filings}
    labels_match = lambda obs: obs.get("fy") == WFC["fiscal_year"] and obs.get("fp") == WFC["fiscal_period"]
    original_facts = [
        obs for obs in exact_period
        if obs.get("accn") == WFC["accession"] and labels_match(obs)
    ]
    if len(original_facts) != 1:
        raise ResolutionError(f"expected one exact company-period fact, found {len(original_facts)}")
    original_filing = filing_by_accession.get(WFC["accession"])
    if original_filing is None or original_filing.get("form") != "10-Q":
        raise ResolutionError("the allowlisted original 10-Q is absent from saved submissions")
    if original_filing.get("reportDate") != WFC["period_end"].isoformat():
        raise ResolutionError("the filing report period is outside the allowlist")
    original = original_facts[0]
    documents = filing_html if isinstance(filing_html, dict) else {WFC["accession"]: filing_html}
    candidates: list[tuple[datetime, dict[str, Any], dict[str, Any], dict[str, Any], str]] = []
    # An amendment can replace the original only if it carries the exact relevant fact.
    amendment_facts = [
        obs for obs in exact_period
        if (filing := filing_by_accession.get(str(obs.get("accn")))) is not None
        and filing.get("form") == "10-Q/A"
        and filing.get("reportDate") == WFC["period_end"].isoformat()
        and labels_match(obs)
    ]
    non_amendment_competitors = [
        obs for obs in exact_period
        if obs.get("accn") != WFC["accession"]
        and obs not in amendment_facts
        and labels_match(obs)
        and str(obs.get("accn")) in filing_by_accession
        and filing_by_accession[str(obs["accn"])].get("form") in {"10-Q", "10-Q/A"}
    ]
    if non_amendment_competitors:
        raise ResolutionError("conflicting comparative accession exists for the same YTD duration")
    applicable = [(original_filing, original)] + [
        (filing_by_accession[str(obs["accn"])], obs) for obs in amendment_facts
    ]
    for candidate_filing, candidate_fact in applicable:
        acceptance_raw = candidate_filing.get("acceptanceDateTime")
        if not acceptance_raw:
            continue
        acceptance = parse_aware_timestamp(acceptance_raw)
        if acceptance > cutoff_dt:
            continue
        document = documents.get(str(candidate_filing["accessionNumber"]))
        if document is None:
            continue
        value = Decimal(str(candidate_fact.get("val")))
        inline = inline_ocf_facts(
            document, period_start=WFC["period_start"], period_end=WFC["period_end"]
        )
        matches = [row for row in inline if row.get("scaled_value") == value and row.get("unitref", "").lower() == "usd"]
        context_parser = _ContextAndFactParser()
        context_parser.feed(document.decode("utf-8", errors="replace"))
        context_matches = [
            fact for fact in context_parser.facts
            if fact.get("scaled_value") == value
            and fact.get("attrs", {}).get("name", "").lower()
            == f"{WFC['taxonomy']}:{WFC['tag']}".lower()
            and fact.get("attrs", {}).get("unitref", "").lower() == "usd"
            and fact.get("attrs", {}).get("contextref") in context_parser.contexts
        ]
        if len(context_matches) == 1:
            context = context_parser.contexts[context_matches[0]["attrs"]["contextref"]]
            entity_scope_verified = (
                context.get("identifier") == WFC["cik"]
                and _date(context.get("start")) == WFC["period_start"]
                and _date(context.get("end")) == WFC["period_end"]
                and context.get("dimensions") == []
                and "wells fargo & company and subsidiaries consolidated statement of cash flows" in _filing_visible_text(document)
            )
        else:
            entity_scope_verified = False
        if len(matches) == 1 and entity_scope_verified:
            doc_hash = filing_html_sha256 if isinstance(filing_html, bytes) else hashlib.sha256(document).hexdigest()
            candidates.append((acceptance, candidate_filing, candidate_fact, matches[0], doc_hash))
    if not candidates:
        raise ResolutionError(
            "no timely verified filing has the exact Company Facts observation; matching filings may be after the cutoff or lack inline corroboration"
        )
    candidates.sort(key=lambda row: (row[0], str(row[1]["accessionNumber"])))
    latest_acceptance = candidates[-1][0]
    latest = [row for row in candidates if row[0] == latest_acceptance]
    if len({Decimal(str(row[2]["val"])) for row in latest}) != 1:
        raise ResolutionError("conflicting applicable accessions share the latest acceptance time")
    acceptance, filing, fact, matching_inline, selected_filing_hash = latest[-1]
    if decision_case.get("mapping_verified") is not True or decision_case.get("company_identity_verified") is not True:
        raise ResolutionError("archived company-to-CIK mapping verification is absent")
    if str(decision_case.get("gvkey")) != WFC["gvkey"] or str(decision_case.get("cik")) != WFC["cik"]:
        raise ResolutionError("archived mapping decision does not match the allowlisted issuer")
    if decision_case.get("mapping_snapshot_sha256") != mapping_snapshot_sha256:
        raise ResolutionError("mapping snapshot hash does not match the archived decision")
    raw_decimal = Decimal(str(fact.get("val")))
    # Multiple original 10-Q facts for one exact duration are unresolved. Later
    # comparative 10-Qs are ignored; only a relevant 10-Q/A can revise the fact.
    original_period_values = {
        Decimal(str(row["val"])) for row in exact_period
        if row.get("accn") == WFC["accession"] and labels_match(row)
    }
    if len(original_period_values) != 1:
        raise ResolutionError("conflicting original observations exist for the same YTD duration")
    if decision_case.get("all_matching_period_observation_count") != 1 and not amendment_facts:
        raise ResolutionError("archived decision records a conflicting or additional period observation")
    if fact.get("unit") not in {None, WFC["unit"]}:
        raise ResolutionError("SEC unit is incompatible")
    if not raw_decimal.is_finite():
        raise ResolutionError("SEC value is not finite")
    filing_date = _date(filing.get("filingDate"))
    if filing_date is None:
        raise ResolutionError("filing date is missing")
    if not filing.get("primaryDocument"):
        raise ResolutionError("primary filing document is missing")
    return {
        "value": raw_decimal,
        "filing": filing,
        "filing_date": filing_date,
        "acceptance": acceptance,
        "cutoff": cutoff_dt,
        "inline": matching_inline,
        "taxonomy": WFC["taxonomy"],
        "tag": WFC["tag"],
        "unit": WFC["unit"],
        "mapping_snapshot_path": mapping_snapshot_path,
        "mapping_snapshot_sha256": mapping_snapshot_sha256,
        "companyfacts_sha256": companyfacts_sha256,
        "submissions_sha256": submissions_sha256,
        "filing_html_sha256": selected_filing_hash,
        "filing_archive_path": str(next((row.get("archive_path") for row in decision_case.get("filing_evidence", [])
                                          if row.get("accession") == filing.get("accessionNumber")), "")),
        "identity_verified": True,
        "identity_evidence": "SEC Company Facts/submissions entity names, saved company-to-CIK mapping, and inline XBRL context identifier match the configured CIK",
        "consolidation_scope_verified": True,
        "scope_evidence": "target fact is in the undimensioned CIK context and the filing labels the statement Wells Fargo & Company and Subsidiaries Consolidated Statement of Cash Flows",
        "decision_case": decision_case,
    }


def wells_fargo_accepted_candidate(evidence: dict[str, Any]) -> dict[str, Any]:
    """Adapt verified archived Wells Fargo evidence to the generic resolver config."""
    decision = SEC_OCF_CANDIDATE_DECISIONS[f"{WFC['gvkey']}|{WFC['period_end'].isoformat()}"]
    if decision["classification"] != "accepted":
        raise ResolutionError("Wells Fargo candidate decision is not accepted")
    filing = evidence["filing"]
    return {
        "classification": decision["classification"], "gvkey": WFC["gvkey"],
        "barrid": WFC["barrid"], "period_start": WFC["period_start"],
        "period_end": WFC["period_end"], "fiscal_year": WFC["fiscal_year"],
        "fiscal_quarter": WFC["fiscal_quarter"],
        "fiscal_year_end_month": WFC["fiscal_year_end_month"],
        "fiscal_period": WFC["fiscal_period"], "currency": "USD",
        "identity_verified": evidence["identity_verified"],
        "consolidation_scope_verified": evidence["consolidation_scope_verified"],
        "identity_evidence": evidence["identity_evidence"],
        "scope_evidence": evidence["scope_evidence"],
        "conflict_review": "no_conflict_or_verified_amendment",
        "taxonomy": evidence["taxonomy"], "tag": evidence["tag"],
        "definition": "Net cash provided by (used in) operating activities; consolidated SEC us-gaap YTD fact",
        "unit": evidence["unit"], "sec_value_raw": str(evidence["value"]),
        "inline_value_raw": str(evidence["inline"]["scaled_value"]),
        "sec_to_compustat_multiplier": str(WFC["usd_to_compustat_millions"]),
        "accession": filing["accessionNumber"], "form": filing["form"],
        "filing_time": evidence["filing_date"].isoformat(),
        "acceptance_time": filing["acceptanceDateTime"],
        "source_url": f"https://www.sec.gov/Archives/edgar/data/72971/{filing['accessionNumber'].replace('-', '')}/{filing['primaryDocument']}",
        "filing_archive_path": evidence["filing_archive_path"],
        "companyfacts_sha256": evidence["companyfacts_sha256"],
        "submissions_sha256": evidence["submissions_sha256"],
        "filing_html_sha256": evidence["filing_html_sha256"],
        "mapping_snapshot_path": evidence["mapping_snapshot_path"],
        "mapping_snapshot_sha256": evidence["mapping_snapshot_sha256"],
        "mapping_provenance": "saved exact CUSIP relationship plus current comp.company.cik metadata; verified against filing entity; not historical point-in-time",
        "extraction_version": "sec_ocf_ytd_exact_accession_inline_verified_v1",
        "decision_reason": decision["reason"],
    }


def resolve_quarterly(
    quarterly: pl.DataFrame,
    evidence: dict[str, Any],
    *,
    cutoff: str,
    extraction_id: str,
    mechanics_exclusion: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Return all source rows plus a separate resolved oancfy and audit columns."""
    if not extraction_id:
        raise ResolutionError("extraction_id is required")
    if parse_aware_timestamp(cutoff) != evidence.get("cutoff"):
        raise ResolutionError("resolver cutoff must equal the timezone-aware evidence acceptance cutoff")
    needed = {"barrid", "gvkey", "datadate", "fyearq", "fqtr", "fyr", "currency", "oancfy"}
    if not needed.issubset(quarterly.columns):
        raise ResolutionError(f"quarterly input missing columns: {sorted(needed - set(quarterly.columns))}")
    if "original_oancfy" in quarterly.columns or "resolved_oancfy" in quarterly.columns:
        raise ResolutionError("input already contains resolved-layer columns")
    frame = quarterly.with_columns(pl.col("oancfy").alias("original_oancfy"))
    nullable: dict[str, Any] = {name: None for name in RESOLVED_COLUMNS}
    frame = frame.with_columns(
        pl.col("oancfy").alias("resolved_oancfy"),
        pl.when(pl.col("oancfy").is_not_null()).then(pl.lit("compustat"))
        .otherwise(pl.lit("unresolved")).alias("selected_source"),
        pl.when(pl.col("oancfy").is_not_null()).then(pl.lit("preserved non-null Compustat value"))
        .otherwise(pl.lit("no verified allowlisted SEC observation" )).alias("selection_reason"),
        *[pl.lit(nullable[name], dtype=dtype).alias(name) for name, dtype in RESOLVED_COLUMNS.items()
          if name not in {"original_oancfy", "resolved_oancfy", "selected_source", "selection_reason", "policy_version", "policy_cutoff", "policy_cutoff_basis"}],
        pl.lit(POLICY_VERSION).alias("policy_version"),
        pl.lit(parse_aware_timestamp(cutoff).isoformat()).alias("policy_cutoff"),
        pl.lit("supplied comparison-policy cutoff; original collection timestamp not recorded").alias("policy_cutoff_basis"),
    )
    target = (
        (pl.col("gvkey") == WFC["gvkey"])
        & (pl.col("barrid") == WFC["barrid"])
        & (pl.col("datadate") == WFC["period_end"])
        & (pl.col("fyearq") == WFC["fiscal_year"])
        & (pl.col("fqtr") == WFC["fiscal_quarter"])
        & (pl.col("fyr") == WFC["fiscal_year_end_month"])
        & (pl.col("currency") == "USD")
    )
    rows = frame.filter(target)
    if rows.height != 1 or rows["oancfy"][0] is not None:
        raise ResolutionError(f"allowlisted Compustat key must identify one null row; found {rows.height}")
    if evidence["filing"]["accessionNumber"] != WFC["accession"]:
        raise ResolutionError("evidence accession does not match the allowlist")
    unit_value = evidence["value"]
    converted = unit_value * WFC["usd_to_compustat_millions"]
    if converted != Decimal("24785"):
        raise ResolutionError("archived Wells Fargo SEC value failed the expected USD-million conversion")
    inline = evidence["inline"]
    filing = evidence["filing"]
    canonical_sec = {
        "original_oancfy": None,
        "resolved_oancfy": float(converted),
        "selected_source": "sec",
        "selection_reason": "verified exact company, fiscal labels, YTD period, us-gaap definition, USD unit, inline filing value, and timely acceptance",
        "candidate_evidence_json": json.dumps({
            "classification": "accepted",
            "identity_verified": evidence["identity_verified"],
            "identity_evidence": evidence["identity_evidence"],
            "consolidation_scope_verified": evidence["consolidation_scope_verified"],
            "scope_evidence": evidence["scope_evidence"],
            "conflict_review": "no conflict; no exact-fact amendment or comparative accession was present in saved evidence",
            "decision_case": evidence["decision_case"],
        }, sort_keys=True, separators=(",", ":"), default=str),
        "sec_value_raw": str(unit_value),
        "sec_unit": "USD",
        "sec_currency": "USD",
        "sec_taxonomy": WFC["taxonomy"],
        "sec_tag": WFC["tag"],
        "sec_definition": "Net cash provided by (used in) operating activities; SEC us-gaap concept, exact YTD duration corroborated to filing",
        "sec_period_start": WFC["period_start"],
        "sec_period_end": WFC["period_end"],
        "sec_period_semantics": "fiscal_year_to_date",
        "sec_fiscal_year": WFC["fiscal_year"],
        "sec_fiscal_period": WFC["fiscal_period"],
        "sec_accession": filing["accessionNumber"],
        "sec_form": filing["form"],
        "sec_filing_time": evidence["filing_date"].isoformat(),
        "sec_acceptance_time": filing["acceptanceDateTime"],
        "sec_source_url": f"https://www.sec.gov/Archives/edgar/data/72971/{filing['accessionNumber'].replace('-', '')}/{filing['primaryDocument']}",
        "sec_filing_archive_path": evidence.get("filing_archive_path", ""),
        "sec_mapping_provenance": "saved exact CUSIP relationship plus current comp.company.cik metadata; not historical point-in-time",
        "sec_mapping_snapshot_path": evidence["mapping_snapshot_path"],
        "sec_mapping_snapshot_sha256": evidence["mapping_snapshot_sha256"],
        "sec_companyfacts_sha256": evidence["companyfacts_sha256"],
        "sec_submissions_sha256": evidence["submissions_sha256"],
        "sec_filing_html_sha256": evidence["filing_html_sha256"],
        "sec_extraction_id": extraction_id,
        "sec_extraction_version": "sec_ocf_ytd_exact_accession_inline_verified_v1",
        "policy_version": POLICY_VERSION,
        "policy_cutoff": evidence["cutoff"].isoformat(),
        "policy_cutoff_basis": "supplied comparison-policy cutoff; original collection timestamp not recorded",
    }
    patch = pl.DataFrame(
        [{"barrid": WFC["barrid"], **canonical_sec}],
        schema={"barrid": pl.String} | RESOLVED_COLUMNS,
    )
    # Preserve all Barra rows for the one company-period if present; no row is duplicated or dropped.
    targets = frame.filter(
        (pl.col("gvkey") == WFC["gvkey"])
        & (pl.col("datadate") == WFC["period_end"])
        & (pl.col("fyearq") == WFC["fiscal_year"])
        & (pl.col("fqtr") == WFC["fiscal_quarter"])
        & (pl.col("fyr") == WFC["fiscal_year_end_month"])
    ).select("barrid").unique()
    if targets.height != 1 or targets["barrid"][0] != WFC["barrid"]:
        raise ResolutionError("the allowlisted company-period does not map to the expected sole Barra row")
    result = frame.join(patch, on="barrid", how="left", suffix="_sec").with_columns(
        *[
            pl.when(target).then(pl.col(f"{name}_sec")).otherwise(pl.col(name)).alias(name)
            for name in RESOLVED_COLUMNS if name not in {"original_oancfy", "resolved_oancfy", "selected_source", "selection_reason"}
        ],
        pl.when(target).then(pl.col("resolved_oancfy_sec")).otherwise(pl.col("resolved_oancfy")).alias("resolved_oancfy"),
        pl.when(target).then(pl.lit("sec")).otherwise(pl.col("selected_source")).alias("selected_source"),
        pl.when(target).then(pl.col("selection_reason_sec")).otherwise(pl.col("selection_reason")).alias("selection_reason"),
    ).drop([f"{name}_sec" for name in RESOLVED_COLUMNS])
    if mechanics_exclusion is not None:
        mtarget = (
            (pl.col("gvkey") == MECHANICS["gvkey"])
            & (pl.col("barrid") == MECHANICS["barrid"])
            & (pl.col("datadate") == MECHANICS["period_end"])
            & (pl.col("fyearq") == 2024)
            & (pl.col("fqtr") == 3)
            & (pl.col("fyr") == 12)
            & (pl.col("currency") == "USD")
        )
        matched = result.filter(mtarget)
        if matched.height != 1 or matched["original_oancfy"][0] is not None:
            raise ResolutionError(f"Mechanics evidence key must identify one null Compustat row; found {matched.height}")
        if mechanics_exclusion.get("decision") != "exclude_and_remain_unresolved":
            raise ResolutionError("Mechanics evidence does not authorize an unresolved exclusion")
        evidence_json = json.dumps(mechanics_exclusion, sort_keys=True, separators=(",", ":"))
        result = result.with_columns(
            pl.when(mtarget).then(pl.lit(mechanics_exclusion["decision_reason"]))
            .otherwise(pl.col("selection_reason")).alias("selection_reason"),
            pl.when(mtarget).then(pl.lit(evidence_json))
            .otherwise(pl.col("candidate_evidence_json")).alias("candidate_evidence_json"),
        )
    return result


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
        .when(
            ((pl.col("gvkey") == "120318") | (pl.col("gvkey") == "177009"))
            & (pl.col("datadate") == date(2026, 9, 30))
        ).then(pl.lit("FDIC filer; separate source required."))
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
