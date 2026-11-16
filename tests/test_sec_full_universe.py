from __future__ import annotations

from datetime import date
import unittest

import polars as pl

from scripts.sec_full_universe import _original_exclusion_review
from scripts.refresh_all_sec_ocf_candidates import _select_company_periods


class SecFullUniverseDiscoveryTests(unittest.TestCase):
    def test_legacy_exclusion_review_separates_duplicate_periods_and_missing_ciks(self) -> None:
        mapping = pl.DataFrame({
            "gvkey": ["000001", "000002"],
            "barrid": ["US1", "US2"],
            "cik": ["0000000001", None],
            "mapping_status": ["mapped_current_cik_metadata", "missing_cik"],
        })
        quarterly = pl.DataFrame({
            "gvkey": ["000001", "000001", "000002"],
            "datadate": [date(2025, 9, 30), date(2025, 9, 30), date(2026, 9, 30)],
            "fyearq": [2025, 2026, 2026],
            "fqtr": [3, 1, 3],
            "fyr": [12, 6, 12],
            "curcdq": ["USD", "USD", "USD"],
            "oancfy": [-78.3, None, None],
        })

        review = _original_exclusion_review(mapping, quarterly)

        self.assertEqual(len(review), 2)
        duplicate = next(item for item in review if item["gvkey"] == "000001")
        missing = next(item for item in review if item["gvkey"] == "000002")
        self.assertEqual(
            duplicate["original_exclusion_reasons"],
            ["multiple_compustat_rows_share_gvkey_datadate"],
        )
        self.assertEqual(len(duplicate["compustat_rows"]), 2)
        self.assertEqual(
            missing["original_exclusion_reasons"],
            ["no_unique_mapped_current_cik_metadata"],
        )
        self.assertEqual(missing["mapping"]["mapping_statuses"], ["missing_cik"])

    def test_sec_fetch_filter_accepts_only_existing_company_periods(self) -> None:
        cases = (
            {"gvkey": "026162", "datadate": "2025-09-30", "fyearq": 2025},
            {"gvkey": "026162", "datadate": "2025-09-30", "fyearq": 2026},
            {"gvkey": "051514", "datadate": "2025-03-31", "fyearq": 2025},
        )
        selected = _select_company_periods(cases, ["026162:2025-09-30"])
        self.assertEqual(len(selected), 2)
        with self.assertRaisesRegex(ValueError, "not SEC candidates"):
            _select_company_periods(cases, ["999999:2025-09-30"])


if __name__ == "__main__":
    unittest.main()
