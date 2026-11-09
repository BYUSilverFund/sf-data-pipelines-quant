from __future__ import annotations

from datetime import date
import unittest

from pipelines.sec_ocf_cohort import (
    AFTER_CUTOFF,
    INCOMPATIBLE,
    UNRESOLVED,
    VERIFIED_CANDIDATE,
    classify_ocf_gap,
    collect_ocf_ytd_observations,
    inline_ocf_facts,
)


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
