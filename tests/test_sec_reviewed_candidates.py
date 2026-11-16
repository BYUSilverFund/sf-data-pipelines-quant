from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path

import polars as pl
import pytest

from pipelines.resolved_fundamentals import ResolutionError, _ContextAndFactParser
from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database
from scripts.build_development_fundamentals import (
    DEFAULT_CUTOFF, DEFAULT_REVIEW, FULL_UNIVERSE_JOB_LOG,
    _apply_review_artifact, _defaults, _full_universe_inputs,
    _validate_full_universe_reconciliation,
)


EXPECTED = {
    ("013562", "2026-09-30"): 84.357,
    ("033066", "2026-09-30"): 56.989,
    ("166435", "2026-09-30"): -109.723,
    ("039836", "2024-09-30"): -1.566598,
    ("039836", "2024-06-30"): -1.983883,
    ("039836", "2024-03-31"): -0.289841,
}


def test_review_artifact_has_six_exact_company_period_values_and_provenance():
    artifact = json.loads(DEFAULT_REVIEW.read_text())
    got = {(c["gvkey"], c["datadate"]): c for c in artifact["cases"]}
    assert set(got) == set(EXPECTED)
    for key, value in EXPECTED.items():
        case = got[key]
        assert case["decision"] == "accepted"
        assert case["review"]["normalized_compustat_value"] == value
        assert case["evidence"]["filing_sha256"]
        assert case["evidence"]["companyfacts_sha256"]
        assert case["evidence"]["submissions_sha256"]
    syntec = [c for c in artifact["cases"] if c["gvkey"] == "039836"]
    assert all(c["evidence"]["inline_facts"][0]["sign"] == "-" for c in syntec)
    assert [c["review"]["normalized_compustat_value"] for c in syntec] == [
        -0.289841, -1.983883, -1.566598,
    ]


def test_full_universe_reconciliation_is_count_driven():
    _validate_full_universe_reconciliation(
        7, 6, [{"gvkey": "000001", "datadate": "2026-09-30"}],
        [{"gvkey": str(i)} for i in range(6)],
    )
    with pytest.raises(ResolutionError, match="accounting mismatch"):
        _validate_full_universe_reconciliation(8, 6, [{"gvkey": "000001", "datadate": "2026-09-30"}],
                                               [{"gvkey": str(i)} for i in range(6)])


def test_brandywine_consolidated_and_operating_partnership_contexts_are_distinct():
    artifact = json.loads(DEFAULT_REVIEW.read_text())
    case = next(c for c in artifact["cases"] if c["gvkey"] == "013562")
    facts = case["evidence"]["inline_facts"]
    assert [f["contextref"] for f in facts] == ["c-1", "c-2"]
    assert {f["scaled_value"] for f in facts} == {"84357000"}
    fixture = b"""<xbrli:context id='c-1'><xbrli:entity><xbrli:identifier>0000790816</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:startDate>2026-01-01</xbrli:startDate><xbrli:endDate>2026-09-30</xbrli:endDate></xbrli:period></xbrli:context><xbrli:context id='c-2'><xbrli:entity><xbrli:identifier>0000790816</xbrli:identifier><xbrli:segment><xbrldi:explicitMember dimension='dei:LegalEntityAxis'>bdn:BrandywineOperatingPartnershipL.P.Member</xbrldi:explicitMember></xbrli:segment></xbrli:entity><xbrli:period><xbrli:startDate>2026-01-01</xbrli:startDate><xbrli:endDate>2026-09-30</xbrli:endDate></xbrli:period></xbrli:context><ix:nonFraction name='us-gaap:NetCashProvidedByUsedInOperatingActivities' contextRef='c-1'>84357000</ix:nonFraction><ix:nonFraction name='us-gaap:NetCashProvidedByUsedInOperatingActivities' contextRef='c-2'>84357000</ix:nonFraction>"""
    parser = _ContextAndFactParser()
    parser.feed(fixture.decode())
    facts = [f for f in parser.facts if f["attrs"].get("contextref") in {"c-1", "c-2"}]
    assert len(facts) == 2
    assert parser.contexts["c-1"]["dimensions"] == []
    assert parser.contexts["c-2"]["dimensions"] == [
        {"dimension": "dei:LegalEntityAxis", "member": "bdn:BrandywineOperatingPartnershipL.P.Member"}
    ]
    assert parser.contexts["c-1"]["start"] == parser.contexts["c-2"]["start"] == "2026-01-01"
    assert parser.contexts["c-1"]["end"] == parser.contexts["c-2"]["end"] == "2026-09-30"
    assert [f["scaled_value"] for f in facts] == [f["scaled_value"] for f in facts[:1]] * 2


@pytest.mark.skipif(os.environ.get("RUN_FULTON_INTEGRATION") != "1", reason="requires Fulton SEC archive and full-universe log")
def test_review_inputs_match_archived_full_universe_and_pending_404():
    _, _, mapping, _, _ = _defaults()
    decisions, observations, cases = _full_universe_inputs(
        FULL_UNIVERSE_JOB_LOG, run_id="run_e906597c98025ac25cdf006d",
        mapping_path=mapping, cutoff=DEFAULT_CUTOFF,
    )
    reviewed = _apply_review_artifact(
        cases, decisions, DEFAULT_REVIEW, run_id="run_e906597c98025ac25cdf006d",
        cutoff=DEFAULT_CUTOFF, mapping_path=mapping,
    )
    assert observations.is_file()
    assert len(cases) == 977
    assert sum(c.get("classification") != "Pending incomplete evidence" for c in cases) == 976
    assert sum(c.get("classification") == "Pending incomplete evidence" for c in cases) == 1
    assert any(c.get("cik") == "0001569650" and c.get("decision") == "unresolved" for c in cases)
    assert sum(c.get("review_artifact_path") is not None for c in reviewed) == 6


@pytest.mark.skipif(os.environ.get("RUN_FULTON_INTEGRATION") != "1", reason="requires Fulton SEC archive and mapping snapshot")
def test_review_artifact_source_hash_mismatch_is_rejected(tmp_path):
    _, _, mapping, _, _ = _defaults()
    decisions = Path("/home/gunnjake/groups/grp_quant/database/fundamentals/sec_filings/fallback_pilots/ocf_ytd_cohort_2026-11/cohort_decisions_2b6b36637163f96f44a5b9a359d758634404838d4a8af1ab6ad05a643972fe6a.json")
    bad = json.loads(DEFAULT_REVIEW.read_text())
    bad["source_manifest_sha256"] = "0" * 64
    path = tmp_path / "bad-review.json"
    path.write_text(json.dumps(bad))
    with pytest.raises(ResolutionError, match="source manifest hash mismatch"):
        _apply_review_artifact([], decisions, path, run_id="run_e906597c98025ac25cdf006d",
                               cutoff=DEFAULT_CUTOFF, mapping_path=mapping)


@pytest.mark.skipif(os.environ.get("RUN_FULTON_INTEGRATION") != "1", reason="requires Fulton development Database tables")
def test_database_resolved_output_has_expected_fills_and_preserves_source():
    root, *_ = _defaults()
    manifests = sorted((root / "development").glob("fundamentals_quarterly_resolved_*/manifest.json"),
                       key=lambda p: p.stat().st_mtime, reverse=True)
    manifest = json.loads(manifests[0].read_text())
    db = Database(DatabaseName.DEVELOPMENT)
    scope = manifest["scope_id"]
    source = db.fundamentals_quarterly_table.read().collect()
    resolved = db.fundamentals_quarterly_resolved_table(scope).read().collect()
    fills = resolved.filter(pl.col("selected_source") == "sec")
    actual = {(r["gvkey"], r["datadate"].isoformat()): r["resolved_oancfy"]
              for r in fills.select("gvkey", "datadate", "resolved_oancfy").iter_rows(named=True)}
    assert actual == EXPECTED
    assert resolved.height == source.height
    assert resolved.select(db.fundamentals_quarterly_table._ids).unique().height == resolved.height
    assert resolved.select(source.columns).sort(db.fundamentals_quarterly_table._ids).equals(
        source.sort(db.fundamentals_quarterly_table._ids)
    )
    assert resolved.filter(pl.col("original_oancfy").is_not_null()).height == 30805
    assert resolved.filter(pl.col("original_oancfy").is_not_null()
                           & (pl.col("original_oancfy") != pl.col("resolved_oancfy"))).is_empty()
    assert fills["candidate_evidence_json"].is_not_null().all()
    assert fills["sec_accession"].is_not_null().all()
    assert resolved.filter(pl.col("resolved_oancfy").is_null()).height == 978
