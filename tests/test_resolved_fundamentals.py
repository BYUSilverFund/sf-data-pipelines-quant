from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import json

import polars as pl
import pytest

from pipelines.resolved_fundamentals import (
    POLICY_VERSION,
    ResolutionError,
    resolve_quarterly,
    resolve_quarterly_candidates,
    scope_id,
    verify_wells_fargo_evidence,
    write_scoped_table,
)
from scripts.build_development_fundamentals import validate_sec_filled_keys


CUTOFF = "2026-10-29T23:59:59+00:00"
ACCEPTED = "2026-10-28T12:59:31.000Z"
HASH = "a" * 64


@pytest.mark.parametrize("actual", [set(), {("008007", "USAK1N1", date(2026, 9, 30))},
                                    {("008007", "USAK1N1", date(2026, 9, 30)),
                                     ("008007", "USAK2N1", date(2026, 9, 30))}])
def test_sec_filled_validation_accepts_zero_one_or_multiple_reviewed_mapped_securities(actual):
    eligible = {("008007", "USAK1N1", date(2026, 9, 30)),
                ("008007", "USAK2N1", date(2026, 9, 30))}
    validate_sec_filled_keys(actual, eligible)


def test_sec_filled_validation_rejects_unreviewed_or_unmapped_security():
    with pytest.raises(ResolutionError, match="eligible reviewed"):
        validate_sec_filled_keys({("008007", "UNMAPPED", date(2026, 9, 30))},
                                 {("008007", "USAK1N1", date(2026, 9, 30))})


def sec_inputs(*, accepted: str = ACCEPTED, value: int = 24_785_000_000,
               start: str = "2026-01-01", unit: str = "USD",
               amendment: bool = False, amendment_value: int | None = None,
               comparative: bool = False):
    accession = "0000072971-26-000327"
    fact = {"accn": accession, "start": start, "end": "2026-09-30", "val": value,
            "form": "10-Q", "fy": 2026, "fp": "Q3", "filed": "2026-10-28"}
    filings = [{"accessionNumber": accession, "filingDate": "2026-10-28",
                "reportDate": "2026-09-30", "acceptanceDateTime": accepted,
                "form": "10-Q", "primaryDocument": "wfc-20260930.htm"}]
    if amendment:
        # This amendment does not contain the relevant fact and must not displace the original.
        filings.append({"accessionNumber": "0000072971-26-000999", "filingDate": "2026-10-29",
                        "reportDate": "2026-09-30", "acceptanceDateTime": "2026-10-29T15:00:00.000Z",
                        "form": "10-Q/A", "primaryDocument": "wfc-amendment.htm"})
    if amendment_value is not None:
        fact_list_amendment = {"accn": "0000072971-26-000999", "start": start,
                               "end": "2026-09-30", "val": amendment_value,
                               "form": "10-Q/A", "fy": 2026, "fp": "Q3", "filed": "2026-10-29"}
    else:
        fact_list_amendment = None
    if comparative:
        fact2 = {**fact, "accn": "0000072971-26-000998", "val": value + 1}
        fact_list = [fact, fact2]
        filings.append({"accessionNumber": fact2["accn"], "filingDate": "2026-10-27",
                        "reportDate": "2026-09-30", "acceptanceDateTime": "2026-10-27T10:00:00.000Z",
                        "form": "10-Q", "primaryDocument": "wfc-comparative.htm"})
    else:
        fact_list = [fact]
    if fact_list_amendment:
        fact_list.append(fact_list_amendment)
    facts = {"cik": 72971, "entityName": "WELLS FARGO & COMPANY/MN",
             "facts": {"us-gaap": {"NetCashProvidedByUsedInOperatingActivities":
                      {"units": {unit: fact_list}}}}}
    def inline_doc(display: str) -> bytes:
        return f'''<html><body><xbrli:context id="ctx"><xbrli:entity>
      <xbrli:identifier scheme="http://www.sec.gov/CIK">0000072971</xbrli:identifier></xbrli:entity><xbrli:period>
      <xbrli:startDate>{start}</xbrli:startDate><xbrli:endDate>2026-09-30</xbrli:endDate>
      </xbrli:period></xbrli:context>
      <div>Wells Fargo &amp; Company and Subsidiaries Consolidated Statement of Cash Flows</div>
      <ix:nonFraction name="us-gaap:NetCashProvidedByUsedInOperatingActivities"
        unitRef="usd" contextRef="ctx" scale="6">{display}</ix:nonFraction></body></html>'''.encode()
    inline = inline_doc("24,785")
    documents = {"0000072971-26-000327": inline}
    if amendment_value is not None:
        documents["0000072971-26-000999"] = inline_doc(f"{Decimal(amendment_value) / Decimal(1_000_000):,.0f}")
    submissions = {"name": "WELLS FARGO & COMPANY/MN", "filings": {"recent": {
        key: [row.get(key) for row in filings] for key in (
            "accessionNumber", "filingDate", "reportDate", "acceptanceDateTime", "form", "primaryDocument")
    }}}
    case = {"mapping_verified": True, "company_identity_verified": True, "gvkey": "008007",
            "cik": "0000072971", "mapping_snapshot_sha256": HASH,
            "all_matching_period_observation_count": 1}
    return facts, submissions, documents if amendment_value is not None else inline, case


def verify(*, cutoff=CUTOFF, **kwargs):
    facts, submissions, filing, case = sec_inputs(**kwargs)
    return verify_wells_fargo_evidence(
        facts, submissions, filing, cutoff=cutoff,
        companyfacts_sha256=HASH, submissions_sha256="b" * 64,
        filing_html_sha256="c" * 64, mapping_snapshot_path="mapping.parquet",
        mapping_snapshot_sha256=HASH, decision_case=case,
    )


def quarter_rows() -> pl.DataFrame:
    return pl.DataFrame([
        {"barrid": "USAK1N1", "gvkey": "008007", "datadate": date(2026, 9, 30),
         "fyearq": 2026, "fqtr": 3, "fyr": 12, "currency": "USD", "oancfy": None},
        {"barrid": "USAXXXX", "gvkey": "000001", "datadate": date(2026, 9, 30),
         "fyearq": 2026, "fqtr": 3, "fyr": 12, "currency": "USD", "oancfy": 4.5},
        {"barrid": "USAHMH1", "gvkey": "018948", "datadate": date(2024, 9, 30),
         "fyearq": 2024, "fqtr": 3, "fyr": 12, "currency": "USD", "oancfy": None},
        {"barrid": "USBCBI1", "gvkey": "018626", "datadate": date(2024, 9, 30),
         "fyearq": 2024, "fqtr": 3, "fyr": 12, "currency": "EUR", "oancfy": None},
    ])


def test_wells_fargo_exact_fact_resolves_and_preserves_all_rows():
    evidence = verify()
    frame = resolve_quarterly(quarter_rows(), evidence, cutoff=CUTOFF, extraction_id="d" * 64)
    assert frame.height == 4
    wfc = frame.filter(pl.col("gvkey") == "008007").row(0, named=True)
    assert wfc["original_oancfy"] is None
    assert wfc["resolved_oancfy"] == 24785.0
    assert wfc["sec_value_raw"] == "24785000000"
    assert wfc["selected_source"] == "sec"
    assert wfc["sec_period_semantics"] == "fiscal_year_to_date"
    original = frame.filter(pl.col("gvkey") == "000001").row(0, named=True)
    assert original["original_oancfy"] == original["resolved_oancfy"] == 4.5
    assert original["selected_source"] == "compustat"
    assert frame.filter(pl.col("gvkey").is_in(["018948", "018626"]))["resolved_oancfy"].null_count() == 2


def test_cutoff_must_be_timezone_aware_and_acceptance_boundary_is_inclusive():
    with pytest.raises(ResolutionError, match="timezone-aware"):
        verify(cutoff="2026-10-29T23:59:59")
    verify(cutoff=ACCEPTED)
    with pytest.raises(ResolutionError, match="after"):
        verify(cutoff="2026-10-28T12:59:30.999Z")


def test_wells_fargo_requires_matching_xbrl_entity_and_consolidated_scope():
    facts, submissions, document, case = sec_inputs()
    altered = document.replace(b"0000072971</xbrli:identifier>", b"0000000001</xbrli:identifier>")
    with pytest.raises(ResolutionError, match="no timely verified filing"):
        verify_wells_fargo_evidence(
            facts, submissions, altered, cutoff=CUTOFF,
            companyfacts_sha256=HASH, submissions_sha256="b" * 64,
            filing_html_sha256="c" * 64, mapping_snapshot_path="mapping.parquet",
            mapping_snapshot_sha256=HASH, decision_case=case,
        )


def test_late_filing_wrong_unit_and_wrong_ytd_period_fail_closed():
    with pytest.raises(ResolutionError, match="after"):
        verify(cutoff="2026-10-28T12:59:30Z")
    with pytest.raises(ResolutionError, match="expected one exact company-period fact"):
        verify(unit="shares")
    with pytest.raises(ResolutionError, match="expected one exact company-period fact"):
        verify(start="2026-07-01")


def test_amendment_without_relevant_fact_does_not_displace_original():
    evidence = verify(amendment=True)
    assert evidence["filing"]["accessionNumber"] == "0000072971-26-000327"


def test_latest_relevant_amendment_is_selected_by_acceptance_time():
    evidence = verify(amendment=True, amendment_value=25_000_000_000)
    assert evidence["filing"]["accessionNumber"] == "0000072971-26-000999"
    assert evidence["value"] == Decimal("25000000000")


def test_conflicting_accession_values_remain_unresolved():
    with pytest.raises(ResolutionError, match="conflicting comparative accession"):
        verify(comparative=True)


def test_archived_mechanics_discrepancy_is_a_predecessor_change_not_a_fallback():
    evidence_path = Path(__file__).parent / "fixtures/mechanics_2024q3_discrepancy_verified.json"
    evidence = json.loads(evidence_path.read_text())
    original, comparative = evidence["original"], evidence["comparative"]
    cutoff = evidence["comparison_cutoff"]
    assert original["raw_value"] == "-47042000"
    assert comparative["raw_value"] == "217358000"
    assert original["accession"] == "0001518715-24-000181"
    assert comparative["accession"] == "0001518715-25-000181"
    assert original["acceptance_timestamp"] < cutoff
    assert comparative["acceptance_timestamp"] < cutoff
    assert (original["period_start"], original["period_end"]) == (
        comparative["period_start"], comparative["period_end"]
    ) == ("2024-01-01", "2024-09-30")
    assert original["fy"] == 2024 and comparative["fy"] == 2025
    assert original["unit"] == comparative["unit"] == "USD"
    assert original["concept"] == comparative["concept"] == "NetCashProvidedByUsedInOperatingActivities"
    assert original["context_entity_cik"] == comparative["context_entity_cik"] == "0001518715"
    assert original["context_dimensions"] == comparative["context_dimensions"] == []
    assert original["inline_sign"] == "-" and original["inline_scale"] == "3"
    assert comparative["inline_sign"] is None and comparative["inline_scale"] == "3"
    assert evidence["cause"].startswith("reverse-acquisition accounting predecessor change")
    assert evidence["decision"] == "exclude_and_remain_unresolved"

    frame = resolve_quarterly(
        quarter_rows(), verify(), cutoff=CUTOFF, extraction_id="d" * 64,
        mechanics_exclusion=evidence,
    )
    mechanics = frame.filter(pl.col("gvkey") == "018948").row(0, named=True)
    assert mechanics["selected_source"] == "unresolved"
    assert mechanics["original_oancfy"] is None and mechanics["resolved_oancfy"] is None
    assert "legacy Mechanics Bank" in mechanics["selection_reason"]
    stored = json.loads(mechanics["candidate_evidence_json"])
    assert stored["original"]["raw_value"] == "-47042000"
    assert stored["comparative"]["raw_value"] == "217358000"
    assert frame.filter(pl.col("selected_source") == "sec").height == 1


def test_scoped_atomic_write_is_idempotent_and_scope_isolated(tmp_path):
    frame = resolve_quarterly(quarter_rows(), verify(), cutoff=CUTOFF, extraction_id="d" * 64)
    inputs = {"quarterly.parquet": HASH}
    scope = scope_id(inputs, CUTOFF)
    manifest = {"input_hashes": inputs, "policy_version": POLICY_VERSION}
    output = write_scoped_table(frame, output_root=tmp_path, scope=scope, manifest=manifest)
    expected = sorted(output.glob("*.parquet"))
    assert len(expected) == 2
    before = [p.read_bytes() for p in expected]
    assert write_scoped_table(frame, output_root=tmp_path, scope=scope, manifest=manifest) == output
    assert [p.read_bytes() for p in expected] == before
    different_scope = scope_id({"quarterly.parquet": "e" * 64}, CUTOFF)
    other = write_scoped_table(frame, output_root=tmp_path, scope=different_scope,
                               manifest={"input_hashes": {"quarterly.parquet": "e" * 64}})
    assert other != output and output.exists()
    with pytest.raises(ResolutionError, match="does not match"):
        write_scoped_table(frame, output_root=tmp_path, scope=scope,
                           manifest={"input_hashes": {"quarterly.parquet": "e" * 64}})


def test_configured_candidate_resolution_preserves_nulls_and_provenance():
    candidate = {
        "classification": "accepted", "gvkey": "008007", "barrid": "USAK1N1",
        "period_start": date(2026, 1, 1), "period_end": date(2026, 9, 30),
        "fiscal_year": 2026, "fiscal_quarter": 3, "fiscal_year_end_month": 12,
        "fiscal_period": "Q3", "currency": "USD", "identity_verified": True,
        "consolidation_scope_verified": True, "identity_evidence": "verified mapping and filing entity",
        "scope_evidence": "consolidated statement and context verified",
        "conflict_review": "no_conflict_or_verified_amendment", "taxonomy": "us-gaap",
        "tag": "NetCashProvidedByUsedInOperatingActivities", "definition": "Operating cash flow",
        "unit": "USD", "sec_value_raw": "24785000000", "inline_value_raw": "24785000000",
        "sec_to_compustat_multiplier": "0.000001", "accession": "0000072971-26-000327",
        "form": "10-Q", "filing_time": "2026-10-28", "acceptance_time": ACCEPTED,
        "source_url": "https://www.sec.gov/example", "filing_archive_path": "/archive/wfc.html",
        "companyfacts_sha256": HASH, "submissions_sha256": "b" * 64,
        "filing_html_sha256": "c" * 64, "mapping_snapshot_path": "/archive/map.parquet",
        "mapping_snapshot_sha256": HASH, "mapping_provenance": "verified effective CUSIP mapping",
        "extraction_version": "ocf_ytd_v1", "decision_reason": "evidence gates passed",
    }
    frame = resolve_quarterly_candidates(
        quarter_rows(), [candidate], cutoff=CUTOFF, extraction_id="d" * 64,
    )
    wfc = frame.filter(pl.col("gvkey") == "008007").row(0, named=True)
    assert wfc["resolved_oancfy"] == 24785.0
    assert wfc["sec_filing_archive_path"] == "/archive/wfc.html"
    assert wfc["sec_extraction_version"] == "ocf_ytd_v1"
    assert wfc["sec_period_semantics"] == "fiscal_year_to_date"
    assert frame.filter(pl.col("gvkey") == "000001")["resolved_oancfy"][0] == 4.5
    assert frame.filter(pl.col("gvkey") == "018948")["resolved_oancfy"].null_count() == 1


def test_configured_candidate_resolution_rejects_identity_scope_and_cutoff_failures():
    base = {
        "classification": "accepted", "gvkey": "008007", "barrid": "USAK1N1",
        "period_start": date(2026, 1, 1), "period_end": date(2026, 9, 30),
        "fiscal_year": 2026, "fiscal_quarter": 3, "fiscal_year_end_month": 12,
        "currency": "USD", "identity_verified": False, "consolidation_scope_verified": True,
        "identity_evidence": "missing", "scope_evidence": "scope checked",
        "conflict_review": "no_conflict_or_verified_amendment", "taxonomy": "us-gaap",
        "tag": "ocf", "definition": "Operating cash flow", "unit": "USD",
        "sec_value_raw": "1", "inline_value_raw": "1", "sec_to_compustat_multiplier": "1",
        "accession": "a", "form": "10-Q", "filing_time": "2026-10-28",
        "acceptance_time": ACCEPTED, "source_url": "url", "filing_archive_path": "path",
        "companyfacts_sha256": HASH, "submissions_sha256": HASH, "filing_html_sha256": HASH,
        "mapping_snapshot_path": "map", "mapping_snapshot_sha256": HASH,
        "mapping_provenance": "mapping", "extraction_version": "v1", "decision_reason": "accepted",
    }
    with pytest.raises(ResolutionError, match="entity or consolidation scope"):
        resolve_quarterly_candidates(quarter_rows(), [base], cutoff=CUTOFF, extraction_id="d" * 64)
    late = {**base, "identity_verified": True, "acceptance_time": "2026-10-30T00:00:00Z"}
    with pytest.raises(ResolutionError, match="after the explicit policy cutoff"):
        resolve_quarterly_candidates(quarter_rows(), [late], cutoff=CUTOFF, extraction_id="d" * 64)
