"""Helpers for a bounded, provenance-preserving SEC operating-cash-flow audit."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from typing import Any


OCF_TAG = "NetCashProvidedByUsedInOperatingActivities"
OCF_TAXONOMY = "us-gaap"
OCF_UNIT = "USD"

VERIFIED_CANDIDATE = "Verified SEC fallback candidate"
AFTER_CUTOFF = "Filing after the relevant cutoff"
NO_DISCLOSURE = "No matching disclosure"
INCOMPATIBLE = "Incompatible definition, period, or taxonomy"
UNRESOLVED = "Unresolved"


def collect_ocf_ytd_observations(
    facts_payload: dict[str, Any], *, period_start: date, period_end: date
) -> list[dict[str, Any]]:
    """Return every matching Company Facts row without collapsing accessions.

    Repeated comparative facts, conflicting values, and amended accessions are
    all retained as distinct observations for review.
    """
    units = (
        facts_payload.get("facts", {})
        .get(OCF_TAXONOMY, {})
        .get(OCF_TAG, {})
        .get("units", {})
    )
    result: list[dict[str, Any]] = []
    for unit, observations in units.items():
        for observation in observations:
            if (
                unit == OCF_UNIT
                and observation.get("start") == period_start.isoformat()
                and observation.get("end") == period_end.isoformat()
                and observation.get("form") in {"10-Q", "10-Q/A"}
            ):
                result.append({"unit": unit, **observation})
    return sorted(
        result,
        key=lambda row: (
            str(row.get("filed", "")),
            str(row.get("accn", "")),
            str(row.get("val", "")),
        ),
    )


class _InlineXbrlParser(HTMLParser):
    """Read target duration facts and their XBRL contexts from an SEC filing."""

    def __init__(self, concept: str) -> None:
        super().__init__(convert_charrefs=True)
        self.concept = concept.lower()
        self.contexts: dict[str, dict[str, str]] = {}
        self.context_id: str | None = None
        self.date_field: str | None = None
        self.date_text: list[str] = []
        self.fact: dict[str, Any] | None = None
        self.facts: list[dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = {key.lower(): (value or "") for key, value in attrs}
        if tag.endswith(":context"):
            self.context_id = attrs_map.get("id")
            if self.context_id:
                self.contexts[self.context_id] = {}
        elif self.context_id and tag.endswith(":startdate"):
            self.date_field = "start"
            self.date_text = []
        elif self.context_id and tag.endswith(":enddate"):
            self.date_field = "end"
            self.date_text = []

        if tag == "ix:nonfraction" and attrs_map.get("name", "").lower() == self.concept:
            self.fact = attrs_map | {"text_parts": []}

    def handle_data(self, data: str) -> None:
        if self.context_id and self.date_field:
            self.date_text.append(data)
        if self.fact is not None:
            self.fact["text_parts"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if self.context_id and self.date_field and tag.endswith(f":{self.date_field}date"):
            self.contexts[self.context_id][self.date_field] = "".join(self.date_text).strip()
            self.date_field = None
            self.date_text = []
        if tag.endswith(":context"):
            self.context_id = None
            self.date_field = None
        if tag == "ix:nonfraction" and self.fact is not None:
            fact = self.fact
            text_value = "".join(fact.pop("text_parts")).strip()
            context = self.contexts.get(fact.get("contextref", ""), {})
            try:
                number = Decimal(text_value.replace(",", "").replace("$", ""))
                scale = int(fact.get("scale", "0") or "0")
                scaled_value = number * (Decimal(10) ** scale)
                if fact.get("sign") == "-":
                    scaled_value = -scaled_value
                fact["scaled_value"] = scaled_value
            except (InvalidOperation, ValueError, OverflowError):
                fact["scaled_value"] = None
            fact["display_value"] = text_value
            fact["period_start"] = context.get("start")
            fact["period_end"] = context.get("end")
            self.facts.append(fact)
            self.fact = None


def inline_ocf_facts(
    filing_html: bytes | str,
    *,
    period_start: date,
    period_end: date,
) -> list[dict[str, Any]]:
    """Extract inline XBRL OCF facts for the exact YTD context."""
    text = filing_html.decode("utf-8", errors="replace") if isinstance(filing_html, bytes) else filing_html
    parser = _InlineXbrlParser(f"{OCF_TAXONOMY}:{OCF_TAG}")
    parser.feed(text)
    return [
        fact
        for fact in parser.facts
        if fact.get("unitref", "").lower() == "usd"
        and fact.get("period_start") == period_start.isoformat()
        and fact.get("period_end") == period_end.isoformat()
    ]


def classify_ocf_gap(
    *,
    mapping_verified: bool,
    entity_verified: bool,
    taxonomy_names: set[str],
    target_filings: list[dict[str, Any]],
    exact_accession_observations: list[dict[str, Any]],
    inline_facts: list[dict[str, Any]],
    expected_period_start: date,
    expected_period_end: date,
    collection_cutoff: date,
    all_period_observations: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    """Classify one mapped Compustat OCF-YTD gap with explicit evidence gates."""
    if not mapping_verified or not entity_verified:
        return UNRESOLVED, "Company-to-CIK identity did not verify against the saved mapping and SEC entity."
    if OCF_TAXONOMY not in taxonomy_names:
        return INCOMPATIBLE, "Company Facts does not expose the us-gaap taxonomy required by this pilot."
    if not target_filings:
        return NO_DISCLOSURE, "No 10-Q or 10-Q/A with the exact report period appears in the saved submissions response."
    if not exact_accession_observations:
        return NO_DISCLOSURE, "The matching filing has no exact Company Facts OCF YTD observation for the required period and USD unit."

    timely_accessions = {
        str(filing.get("accessionNumber"))
        for filing in target_filings
        if date.fromisoformat(str(filing["filingDate"])[:10]) <= collection_cutoff
    }
    timely_observations = [
        observation
        for observation in exact_accession_observations
        if observation.get("accn") in timely_accessions
        and observation.get("unit") == OCF_UNIT
        and observation.get("start") == expected_period_start.isoformat()
        and observation.get("end") == expected_period_end.isoformat()
    ]
    if not timely_observations:
        return AFTER_CUTOFF, "The exact-period filing and matching OCF YTD observation were first filed after the collection cutoff."

    period_values = {
        Decimal(str(row["val"]))
        for row in (all_period_observations or [])
        if row.get("unit") == OCF_UNIT
        and row.get("start") == expected_period_start.isoformat()
        and row.get("end") == expected_period_end.isoformat()
    }
    if len(period_values) > 1:
        values = ", ".join(sorted(str(value) for value in period_values))
        return UNRESOLVED, f"Conflicting SEC accessions report different values for the same OCF YTD period: {values}."

    expected_values = {Decimal(str(row["val"])) for row in timely_observations}
    inline_values = {
        fact.get("scaled_value")
        for fact in inline_facts
        if fact.get("scaled_value") is not None
    }
    if not expected_values or not expected_values.issubset(inline_values):
        return UNRESOLVED, "The filing-page inline XBRL sign/scale/value does not corroborate every timely Company Facts value."
    if any(row.get("unit") != OCF_UNIT for row in timely_observations):
        return INCOMPATIBLE, "The SEC unit is not USD."
    return VERIFIED_CANDIDATE, "Exact company, 10-Q accession, USD concept, YTD duration, and filing-page sign/scale/value verified before the collection cutoff."
