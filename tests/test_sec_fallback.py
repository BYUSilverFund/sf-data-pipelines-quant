from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import unittest

from pipelines.sec_fallback import (
    PILOT,
    SecFallbackValidationError,
    extract_aat_q3_operating_cash_flow_ytd,
)


FACTS_HASH = "5773d485144bf7fd8ba71c44c48b6f6cec13152febad658b4a49a170750851eb"
SUBMISSIONS_HASH = "190e85ddaec5a1134b953989607810d6e18e8ba4014e14a3a7eb0766fbc6b5b2"
FILING = {
    "form": "10-Q",
    "accessionNumber": "0001500217-26-000052",
    "filingDate": "2026-10-30",
    "reportDate": "2026-09-30",
    "acceptanceDateTime": "2026-10-30T19:25:22.000Z",
    "primaryDocument": "aat-20260930.htm",
}


def facts_payload() -> dict:
    fixture = Path(__file__).parent / "fixtures/sec_fallback_aat_2026q3_companyfacts_actual_slice.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    payload.pop("_fixture_note")
    payload.pop("_source_response_sha256")
    return payload


class SecFallbackPilotTests(unittest.TestCase):
    def extract(self, payload=None, filing=None):
        return extract_aat_q3_operating_cash_flow_ytd(
            payload or facts_payload(),
            filing or FILING,
            source_response_sha256=FACTS_HASH,
            submission_response_sha256=SUBMISSIONS_HASH,
            extracted_at="2026-11-04T12:00:00+00:00",
        )

    def test_extracts_only_exact_accession_and_fiscal_ytd_period(self) -> None:
        row = self.extract().row(0, named=True)

        self.assertEqual(self.extract().height, 1)
        self.assertEqual(row["ticker"], "AAT")
        self.assertEqual(row["cik"], "0001500217")
        self.assertEqual(row["gvkey"], "185549")
        self.assertEqual(row["barrid"], "USAC121")
        self.assertEqual(row["membership_date"], date(2026, 8, 31))
        self.assertEqual(row["mapping_snapshot_id"], PILOT["mapping_snapshot_id"])
        self.assertIn("membership_2026-08-31_", row["mapping_snapshot_path"])
        self.assertIn("comp.sec_idhist", row["mapping_source"])
        self.assertIn("current metadata", row["cik_source"])
        self.assertEqual(row["metric"], "operating_cash_flow_ytd")
        self.assertEqual(row["tag"], "NetCashProvidedByUsedInOperatingActivities")
        self.assertEqual(row["unit"], "USD")
        self.assertEqual(row["value_text"], "129807000")
        self.assertEqual(row["value"], 129807000.0)
        self.assertEqual(row["period_start"], date(2026, 1, 1))
        self.assertEqual(row["period_end"], date(2026, 9, 30))
        self.assertEqual(row["period_semantics"], "fiscal_year_to_date")
        self.assertEqual(row["fiscal_year"], 2026)
        self.assertEqual(row["fiscal_period"], "Q3")
        self.assertEqual(row["form"], "10-Q")
        self.assertEqual(row["accession"], PILOT["accession"])
        self.assertEqual(row["filing_date"], date(2026, 10, 30))
        self.assertEqual(row["acceptance_timestamp"], "2026-10-30T19:25:22.000Z")
        self.assertEqual(
            row["source_url"],
            "https://www.sec.gov/Archives/edgar/data/1500217/"
            "000150021726000052/aat-20260930.htm",
        )
        self.assertEqual(row["source_response_sha256"], FACTS_HASH)
        self.assertEqual(row["submission_response_sha256"], SUBMISSIONS_HASH)
        self.assertEqual(row["extraction_version"], "aat_q3_2026_operating_cash_flow_ytd_v1")

    def test_fails_closed_outside_company_or_filing_scope(self) -> None:
        with self.assertRaisesRegex(SecFallbackValidationError, "outside the documented"):
            self.extract(filing={**FILING, "accessionNumber": "0001500217-26-000060"})

        payload = facts_payload()
        payload["cik"] = 1800
        with self.assertRaisesRegex(SecFallbackValidationError, "outside the documented"):
            self.extract(payload=payload)

    def test_rejects_ambiguous_duplicate_target_fact(self) -> None:
        payload = facts_payload()
        observations = payload["facts"]["us-gaap"]["NetCashProvidedByUsedInOperatingActivities"]["units"]["USD"]
        target = next(obs for obs in observations if obs["start"] == "2026-01-01")
        observations.append(dict(target, val=129808000))
        with self.assertRaisesRegex(SecFallbackValidationError, "found 2"):
            self.extract(payload=payload)

    def test_actual_companyfacts_slice_contains_ytd_period_not_standalone_q3(self) -> None:
        payload = facts_payload()
        observations = payload["facts"]["us-gaap"]["NetCashProvidedByUsedInOperatingActivities"]["units"]["USD"]
        self.assertEqual(len(observations), 2)
        self.assertEqual(
            {(obs["start"], obs["end"], obs["val"]) for obs in observations},
            {
                ("2026-01-01", "2026-09-30", 129807000),
                ("2025-01-01", "2025-09-30", 126545000),
            },
        )

    def test_standalone_quarter_and_amended_accession_are_not_selected(self) -> None:
        payload = facts_payload()
        observations = payload["facts"]["us-gaap"]["NetCashProvidedByUsedInOperatingActivities"]["units"]["USD"]
        target = next(obs for obs in observations if obs["start"] == "2026-01-01")
        observations.extend(
            [
                {
                    **target,
                    "val": 1,
                    "start": "2026-07-01",
                },
                {
                    **target,
                    "val": 130000000,
                    "accn": "0001500217-26-000060",
                    "form": "10-Q/A",
                },
            ]
        )
        self.assertEqual(self.extract(payload=payload).get_column("value").to_list(), [129807000.0])

    def test_rejects_bad_hash_and_unsafe_source_document(self) -> None:
        with self.assertRaisesRegex(SecFallbackValidationError, "SHA-256"):
            extract_aat_q3_operating_cash_flow_ytd(
                facts_payload(), FILING, source_response_sha256="bad",
                submission_response_sha256=SUBMISSIONS_HASH,
                extracted_at="2026-11-04T12:00:00Z"
            )
        with self.assertRaisesRegex(SecFallbackValidationError, "safe primaryDocument"):
            self.extract(filing={**FILING, "primaryDocument": "../private.htm"})
        with self.assertRaisesRegex(SecFallbackValidationError, "timezone"):
            extract_aat_q3_operating_cash_flow_ytd(
                facts_payload(), FILING, source_response_sha256=FACTS_HASH,
                submission_response_sha256=SUBMISSIONS_HASH,
                extracted_at="2026-11-04T12:00:00"
            )


if __name__ == "__main__":
    unittest.main()
