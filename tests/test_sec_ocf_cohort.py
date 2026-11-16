from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

from pipelines.sec_ocf_cohort import (
    AFTER_CUTOFF,
    INCOMPATIBLE,
    UNRESOLVED,
    VERIFIED_CANDIDATE,
    classify_ocf_gap,
    collect_ocf_ytd_observations,
    discover_ocf_ytd_gaps,
    inline_ocf_facts,
    IDENTITY_REVIEW,
    OCF_YTD_METRIC,
)
from scripts import build_sec_ocf_ytd_cohort as cohort_builder


CUTOFF = date(2026, 10, 29)
START = date(2026, 1, 1)
END = date(2026, 9, 30)


def filing_html(
    display: str,
    *,
    scale: int,
    sign: str = "",
    start: date = START,
    end: date = END,
) -> str:
    sign_attr = f' sign="{sign}"' if sign else ""
    return f'''<html><body>
    <xbrli:context id="c1"><xbrli:period>
      <xbrli:startDate>{start.isoformat()}</xbrli:startDate>
      <xbrli:endDate>{end.isoformat()}</xbrli:endDate>
    </xbrli:period></xbrli:context>
    <ix:nonFraction name="us-gaap:NetCashProvidedByUsedInOperatingActivities"
      unitRef="usd" contextRef="c1" scale="{scale}"{sign_attr}>{display}</ix:nonFraction>
    </body></html>'''


class SecOcfCohortTests(unittest.TestCase):
    def test_latest_cached_response_skips_invalid_newer_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "companyfacts"
            root.mkdir()
            valid = root / "20261110T000000000000Z_valid.json"
            invalid = root / "20261111T000000000000Z_invalid.json"
            valid.write_text('{"facts": {}}')
            invalid.write_text('{not-json')
            self.assertEqual(cohort_builder._latest_json(root), valid)

    def test_sec_refresh_reuses_cached_responses_and_fetches_once_per_cik(self) -> None:
        class Response:
            content = b"<html>filing evidence</html>"

            def raise_for_status(self) -> None:
                return None

        class Session:
            closed = False

            def get(self, url: str, timeout: tuple[int, int]) -> Response:
                self.url = url
                self.timeout = timeout
                return Response()

            def close(self) -> None:
                self.closed = True

        session = Session()
        case = {
            "cik": "0000000001", "gvkey": "000001", "barrid": "USA1",
            "ticker": None, "datadate": "2026-09-30", "fyearq": 2026,
        }
        submissions = {
            "filings": {"recent": {
                "accessionNumber": ["0000000001-26-000123"],
                "filingDate": ["2026-10-20"], "reportDate": ["2026-09-30"],
                "acceptanceDateTime": ["2026-10-20T16:00:00Z"],
                "form": ["10-Q"], "primaryDocument": ["report.htm"],
            }}
        }
        calls: list[str] = []

        def fetch(_session, url, output_root, cik, endpoint):
            calls.append(f"{cik}:{endpoint}")
            payload = {"cik": 1, "facts": {}} if endpoint == "companyfacts" else submissions
            raw = json.dumps(payload).encode()
            path = output_root / "api_responses" / f"CIK{cik}" / endpoint / f"20261111T000000000000Z_{hashlib.sha256(raw).hexdigest()}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            return {"data": payload, "path": path, "sha256": hashlib.sha256(raw).hexdigest(),
                    "retrieved_at": datetime.now(timezone.utc), "status_code": 200}

        second_case = {**case, "datadate": "2025-09-30", "fyearq": 2025}
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {"SEC_USER_AGENT": "Test team test@example.org"}), patch.object(
            cohort_builder, "load_dotenv"
        ), patch.object(cohort_builder, "_make_sec_session", return_value=session), patch.object(
            cohort_builder, "_fetch_and_archive_json", side_effect=fetch
        ), patch.object(cohort_builder.time, "sleep"):
            result = cohort_builder.refresh_archived_sec_evidence(
                archive_root=Path(directory), cases=(case, second_case)
            )
            self.assertEqual(calls, ["0000000001:companyfacts", "0000000001:submissions"])
            self.assertEqual(result["fetched_ciks"], 1)
            self.assertEqual(result["failed_ciks"], 0)
            self.assertTrue(result["coverage_complete"])
            saved = list((Path(directory) / "fallback_pilots/ocf_ytd_cohort_2026-11/source_documents").glob("CIK0000000001_000000000126000123_*.html"))
            self.assertEqual(len(saved), 1)
            self.assertEqual(saved[0].read_bytes(), Response.content)
            self.assertTrue(session.closed)

            session2 = Session()
            with patch.dict("os.environ", {"SEC_USER_AGENT": "Test team test@example.org"}), patch.object(
                cohort_builder, "load_dotenv"
            ), patch.object(cohort_builder, "_make_sec_session", return_value=session2), patch.object(
                cohort_builder, "_fetch_and_archive_json", side_effect=AssertionError("valid cache should be reused")
            ), patch.object(cohort_builder.time, "sleep"):
                resumed = cohort_builder.refresh_archived_sec_evidence(
                    archive_root=Path(directory), cases=(case, second_case)
                )
                self.assertEqual(resumed["cached_ciks"], 1)
                self.assertEqual(resumed["fetched_ciks"], 0)
                self.assertTrue(resumed["coverage_complete"])

    def test_sec_refresh_records_failed_cik_and_partial_coverage(self) -> None:
        class Session:
            closed = False
            def close(self): self.closed = True

        session = Session()
        case = {"cik": "0000000002", "gvkey": "000002", "barrid": "USA2",
                "datadate": "2026-09-30", "fyearq": 2026}
        calls = []
        def fetch(_session, url, _root, _cik, endpoint):
            calls.append(endpoint)
            if endpoint == "companyfacts":
                raise ConnectionError("offline")
            return {"data": {"name": "issuer", "filings": {}}, "path": Path("submissions.json"),
                    "sha256": "a" * 64, "retrieved_at": datetime.now(timezone.utc), "status_code": 200}

        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {"SEC_USER_AGENT": "Test team test@example.org"}), patch.object(
            cohort_builder, "load_dotenv"
        ), patch.object(cohort_builder, "_make_sec_session", return_value=session), patch.object(
            cohort_builder, "_fetch_and_archive_json", side_effect=fetch
        ):
            result = cohort_builder.refresh_archived_sec_evidence(
                archive_root=Path(directory), cases=(case,)
            )
            self.assertEqual(calls, ["companyfacts", "submissions"])
            self.assertEqual(result["failed_ciks"], 1)
            self.assertFalse(result["coverage_complete"])
            self.assertTrue(result["failed_requests"])
            self.assertTrue(session.closed)

    def test_additional_gap_discovery_is_bounded_and_skips_ambiguous_ciks(self) -> None:
        mapping = pl.DataFrame({
            "gvkey": ["000001", "000002", "000003", "000003"],
            "barrid": ["US1", "US2", "US3", "US3"],
            "cik": ["1111111111", "2222222222", "3333333333", "4444444444"],
            "mapping_status": ["mapped_current_cik_metadata"] * 4,
        })
        quarterly = pl.DataFrame({
            "gvkey": ["000001", "000002", "000003"],
            "datadate": [date(2026, 9, 30)] * 3,
            "fyearq": [2026] * 3,
            "oancfy": [None, None, None],
        })
        cases = cohort_builder.discover_additional_null_gaps(mapping, quarterly, limit=10)
        self.assertEqual(len(cases), 2)
        self.assertEqual({case["gvkey"] for case in cases}, {"000001", "000002"})
        self.assertEqual(
            {case["gvkey"]: case["cik"] for case in cases},
            {"000001": "1111111111", "000002": "2222222222"},
        )

    def test_full_gap_discovery_has_no_ten_case_cap_and_keeps_all_mapped_barrids(self) -> None:
        mapping = pl.DataFrame({
            "gvkey": [f"{i:06d}" for i in range(1, 14)] + ["000001"],
            "barrid": [f"US{i}" for i in range(1, 14)] + ["US1B"],
            "cik": [f"{i:010d}" for i in range(1, 14)] + ["0000000001"],
            "mapping_status": ["mapped_current_cik_metadata"] * 14,
        })
        quarterly = pl.DataFrame({
            "gvkey": [f"{i:06d}" for i in range(1, 14)],
            "datadate": [date(2026, 9, 30)] * 13,
            "fyearq": [2026] * 13,
            "oancfy": [None] * 13,
        })
        cases = cohort_builder.discover_additional_null_gaps(
            mapping, quarterly, limit=None, include_known=True,
        )
        self.assertEqual(len(cases), 13)
        self.assertEqual(cases[0]["additional_barrids"], ["US1", "US1B"])

    def test_discovery_keeps_distinct_fiscal_rows_at_one_report_date(self) -> None:
        mapping = pl.DataFrame({
            "gvkey": ["000001"], "barrid": ["US1"], "cik": ["0000000001"],
            "mapping_status": ["mapped_current_cik_metadata"],
        })
        quarterly = pl.DataFrame({
            "gvkey": ["000001", "000001", "000001"],
            "datadate": [date(2025, 9, 30)] * 3,
            "fyearq": [2025, 2026, 2026],
            "fqtr": [3, 1, 1],
            "fyr": [12, 6, 6],
            "curcdq": ["USD"] * 3,
            "rdq": [date(2025, 11, 4)] * 3,
            "oancfy": [-78.3, None, None],
        })
        cases = cohort_builder.discover_additional_null_gaps(
            mapping, quarterly, limit=None, include_known=True,
        )
        self.assertEqual(len(cases), 0)

        quarterly = quarterly.slice(0, 2)
        cases = cohort_builder.discover_additional_null_gaps(
            mapping, quarterly, limit=None, include_known=True,
        )
        self.assertEqual(len(cases), 1)
        self.assertEqual(
            {key: cases[0][key] for key in ("gvkey", "datadate", "fyearq", "fqtr", "fyr", "currency")},
            {"gvkey": "000001", "datadate": "2025-09-30", "fyearq": 2026,
             "fqtr": 1, "fyr": 6, "currency": "USD"},
        )

    def test_fiscal_ytd_start_uses_and_validates_fiscal_year_end_labels(self) -> None:
        self.assertEqual(
            cohort_builder._fiscal_ytd_start(
                {"fyearq": 2026, "fyr": 6}, date(2025, 9, 30),
            ),
            date(2025, 7, 1),
        )
        self.assertEqual(
            cohort_builder._fiscal_ytd_start(
                {"fyearq": 2025, "fyr": 12}, date(2025, 9, 30),
            ),
            date(2025, 1, 1),
        )
        self.assertIsNone(
            cohort_builder._fiscal_ytd_start(
                {"fyearq": 2025, "fyr": 6}, date(2025, 9, 30),
            )
        )

    def test_security_relationship_must_cover_report_date(self) -> None:
        relationship = {
            "compustat_efffrom": date(2025, 4, 14),
            "compustat_effthru": date(2900, 1, 1),
            "barra_start_date": date(2025, 5, 7),
            "barra_end_date": date(2075, 12, 31),
        }
        self.assertFalse(
            cohort_builder._mapping_relationship_is_valid_at_period(
                relationship, date(2025, 3, 31),
            )
        )
        self.assertTrue(
            cohort_builder._mapping_relationship_is_valid_at_period(
                relationship, date(2025, 6, 30),
            )
        )

    def test_gap_discovery_requires_identity_and_scope_before_acceptance(self) -> None:
        rows = pl.DataFrame([
            {"gvkey": "000001", "barrid": "USA1", "datadate": date(2026, 9, 30),
             "fyearq": 2026, "fqtr": 3, "fyr": 12, "currency": "USD", "oancfy": None},
            {"gvkey": "000002", "barrid": "USA2", "datadate": date(2026, 9, 30),
             "fyearq": 2026, "fqtr": 3, "fyr": 12, "currency": "USD", "oancfy": 5.0},
        ])
        candidates = discover_ocf_ytd_gaps(rows, {
            "000001": {"cik": "0000000001", "mapping_verified": True,
                       "entity_verified": True, "consolidation_scope_verified": False,
                       "identity_evidence": "CIK mapping only"}
        })
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["classification"], IDENTITY_REVIEW)
        self.assertEqual(candidates[0]["decision"], "review_required")
        self.assertEqual(candidates[0]["metric_definition"], OCF_YTD_METRIC)

    def common(self, *, filing_date: str, value: int, inline_html: str):
        filing = {
            "accessionNumber": "0000000001-26-000001",
            "filingDate": filing_date,
            "reportDate": END.isoformat(),
            "form": "10-Q",
        }
        fact = {
            "accn": filing["accessionNumber"],
            "start": START.isoformat(),
            "end": END.isoformat(),
            "val": value,
            "form": "10-Q",
            "fy": 2026,
            "fp": "Q3",
            "filed": filing_date,
            "unit": "USD",
        }
        inline = inline_ocf_facts(
            inline_html.encode(), period_start=START, period_end=END
        )
        return filing, fact, inline

    def test_mechanics_retains_conflicting_comparative_and_is_unresolved(self) -> None:
        payload = {
            "facts": {
                "us-gaap": {
                    "NetCashProvidedByUsedInOperatingActivities": {
                        "units": {
                            "USD": [
                                {
                                    "accn": "0001518715-24-000181",
                                    "start": "2024-01-01",
                                    "end": "2024-09-30",
                                    "val": -47042000,
                                    "form": "10-Q",
                                    "filed": "2024-11-08",
                                },
                                {
                                    "accn": "0001518715-25-000181",
                                    "start": "2024-01-01",
                                    "end": "2024-09-30",
                                    "val": 217358000,
                                    "form": "10-Q",
                                    "filed": "2025-11-17",
                                },
                            ]
                        }
                    }
                }
            }
        }
        observations = collect_ocf_ytd_observations(
            payload, period_start=date(2024, 1, 1), period_end=date(2024, 9, 30)
        )
        self.assertEqual([row["accn"] for row in observations], [
            "0001518715-24-000181", "0001518715-25-000181"
        ])
        self.assertEqual([row["val"] for row in observations], [-47042000, 217358000])
        original_inline = inline_ocf_facts(
            filing_html(
                "47,042",
                scale=3,
                sign="-",
                start=date(2024, 1, 1),
                end=date(2024, 9, 30),
            ),
            period_start=date(2024, 1, 1),
            period_end=date(2024, 9, 30),
        )
        self.assertEqual(original_inline[0]["scaled_value"], -47042000)
        decision, reason = classify_ocf_gap(
            mapping_verified=True,
            entity_verified=True,
            taxonomy_names={"us-gaap"},
            target_filings=[{
                "accessionNumber": "0001518715-24-000181",
                "filingDate": "2024-11-08",
            }],
            exact_accession_observations=[observations[0]],
            all_period_observations=observations,
            inline_facts=original_inline,
            expected_period_start=date(2024, 1, 1),
            expected_period_end=date(2024, 9, 30),
            collection_cutoff=CUTOFF,
        )
        self.assertEqual(decision, UNRESOLVED)
        self.assertIn("Conflicting SEC accessions", reason)

    def test_aat_exact_ytd_fact_is_late_for_collection_cutoff(self) -> None:
        filing, fact, inline = self.common(
            filing_date="2026-10-30",
            value=129807000,
            inline_html=filing_html("129,807", scale=3),
        )
        decision, _ = classify_ocf_gap(
            mapping_verified=True,
            entity_verified=True,
            taxonomy_names={"us-gaap"},
            target_filings=[filing],
            exact_accession_observations=[fact],
            all_period_observations=[fact],
            inline_facts=inline,
            expected_period_start=START,
            expected_period_end=END,
            collection_cutoff=CUTOFF,
        )
        self.assertEqual(inline[0]["scaled_value"], 129807000)
        self.assertEqual(decision, AFTER_CUTOFF)

    def test_globant_is_incompatible_without_us_gaap(self) -> None:
        decision, reason = classify_ocf_gap(
            mapping_verified=True,
            entity_verified=True,
            taxonomy_names={"ifrs-full", "dei", "invest"},
            target_filings=[],
            exact_accession_observations=[],
            inline_facts=[],
            expected_period_start=date(2024, 1, 1),
            expected_period_end=date(2024, 9, 30),
            collection_cutoff=CUTOFF,
        )
        self.assertEqual(decision, INCOMPATIBLE)
        self.assertIn("us-gaap", reason)

    def test_wells_fargo_exact_filing_is_verified_before_cutoff(self) -> None:
        filing, fact, inline = self.common(
            filing_date="2026-10-28",
            value=24785000000,
            inline_html=filing_html("24,785", scale=6),
        )
        decision, _ = classify_ocf_gap(
            mapping_verified=True,
            entity_verified=True,
            taxonomy_names={"us-gaap"},
            target_filings=[filing],
            exact_accession_observations=[fact],
            all_period_observations=[fact],
            inline_facts=inline,
            expected_period_start=START,
            expected_period_end=END,
            collection_cutoff=CUTOFF,
        )
        self.assertEqual(inline[0]["scaled_value"], 24785000000)
        self.assertEqual(decision, VERIFIED_CANDIDATE)


if __name__ == "__main__":
    unittest.main()
