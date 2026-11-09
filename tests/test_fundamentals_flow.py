from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd
import polars as pl
import requests

from pipelines.fundamentals_flow import (
    BARRA_CANDIDATE_SCHEMA,
    COMP_CANDIDATE_SCHEMA,
    SECURITY_MAPPING_SCHEMA,
    TAG_TO_METRIC,
    _sec_request_failure,
    build_security_mapping,
    effective_interval_contains,
    fetch_compustat_security_candidates,
    group_security_relationships_by_cik,
)
from pipelines.fundamentals_storage import (
    STATEMENT_SCHEMA,
    archive_api_response,
    extract_accession_facts,
    update_jsonl_inventory,
    write_filing_bundle,
)


MEMBERSHIP_DATE = date(2026, 8, 31)


def snapshot_frame(*cusips: str) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "date": MEMBERSHIP_DATE,
                "cusip": cusip,
                "russell_1000": index == 0,
                "russell_2000": index != 0,
            }
            for index, cusip in enumerate(cusips)
        ],
        schema={
            "date": pl.Date,
            "cusip": pl.String,
            "russell_1000": pl.Boolean,
            "russell_2000": pl.Boolean,
        },
    )


def statement_frame(tag: str, value: float) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "cik": "0000000100",
                "accession": "000000000123000001",
                "taxonomy": "us-gaap",
                "tag": tag,
                "metric": "assets",
                "unit": "USD",
                "currency": "USD",
                "value_text": str(value),
                "value": value,
                "period_start": None,
                "period_end": date(2025, 12, 31),
                "period_type": "instant",
                "report_period": date(2025, 12, 31),
                "fiscal_year": 2025,
                "fiscal_period": "FY",
                "frame": "CY2025Q4I",
                "form": "10-K",
                "filing_date": date(2026, 2, 20),
                "source_accession": "000000000123000001",
                "source_api": "sec_companyfacts",
            }
        ],
        schema=STATEMENT_SCHEMA,
    )


class FundamentalsMappingTests(unittest.TestCase):
    def test_sec_request_failure_reports_category_traceback_and_partial_output(self) -> None:
        cases = (
            (
                requests.exceptions.SSLError("[SSL] error in system default config"),
                "TLS runtime initialization failed",
            ),
            (
                requests.exceptions.SSLError("CERTIFICATE_VERIFY_FAILED"),
                "TLS certificate validation failed",
            ),
            (requests.exceptions.ProxyError("proxy unreachable"), "proxy connection failed"),
            (requests.exceptions.ConnectionError("network unreachable"), "network connection failed"),
        )
        for error, expected_category in cases:
            with self.subTest(error=type(error).__name__, category=expected_category):
                try:
                    raise error
                except requests.RequestException as caught:
                    message = _sec_request_failure(caught)

                self.assertIn(expected_category, message)
                self.assertIn("Underlying exception:", message)
                self.assertIn("Traceback:", message)
                self.assertIn("security mapping snapshot was saved", message)

    def test_compustat_query_uses_inclusive_effective_date_predicates(self) -> None:
        class FakeConnection:
            query = ""

            def raw_sql(self, query: str) -> pd.DataFrame:
                self.query = query
                # These rows represent the matches the captured predicates must allow:
                # NULL end, both inclusive boundaries, and the existing future sentinel.
                return pd.DataFrame(
                    [
                        {
                            "cusip9": "000000001",
                            "gvkey": "001000",
                            "iid": "01",
                            "cik": "100",
                            "efffrom": "2026-08-31",
                            "effthru": None,
                        },
                        {
                            "cusip9": "000000001",
                            "gvkey": "002000",
                            "iid": "01",
                            "cik": "200",
                            "efffrom": "2026-08-31",
                            "effthru": "2026-08-31",
                        },
                        {
                            "cusip9": "000000001",
                            "gvkey": "003000",
                            "iid": "01",
                            "cik": "300",
                            "efffrom": "2020-01-01",
                            "effthru": "2026-08-31",
                        },
                        {
                            "cusip9": "000000001",
                            "gvkey": "004000",
                            "iid": "01",
                            "cik": "400",
                            "efffrom": "2020-01-01",
                            "effthru": "2900-01-01",
                        },
                    ]
                )

        connection = FakeConnection()
        candidates = fetch_compustat_security_candidates(connection, snapshot_frame("000000001"))
        query = " ".join(connection.query.split())

        self.assertIn("h.efffrom <= DATE '2026-08-31'", query)
        self.assertIn(
            "(h.effthru IS NULL OR h.effthru >= DATE '2026-08-31')", query
        )
        self.assertNotIn("h.efffrom IS NULL", query)
        self.assertEqual(
            set(candidates.get_column("gvkey").to_list()),
            {"001000", "002000", "003000", "004000"},
        )
        self.assertIsNone(
            candidates.filter(pl.col("gvkey") == "001000").get_column("effthru")[0]
        )
        self.assertEqual(
            candidates.filter(pl.col("gvkey") == "004000").get_column("effthru")[0],
            date(2900, 1, 1),
        )

    def test_shared_cik_and_multiple_iids_preserve_each_security(self) -> None:
        snapshot = snapshot_frame("000000001", "000000002")
        comp = pl.DataFrame(
            [
                {
                    "cusip9": "000000001",
                    "gvkey": "001000",
                    "iid": "01",
                    "cik": "100",
                    "efffrom": date(2020, 1, 1),
                    "effthru": date(2900, 1, 1),
                },
                {
                    "cusip9": "000000001",
                    "gvkey": "001000",
                    "iid": "01C",
                    "cik": "100",
                    "efffrom": date(2021, 1, 1),
                    "effthru": date(2900, 1, 1),
                },
                {
                    "cusip9": "000000002",
                    "gvkey": "001000",
                    "iid": "02",
                    "cik": "0000000100",
                    "efffrom": date(2022, 1, 1),
                    "effthru": date(2900, 1, 1),
                },
            ],
            schema=COMP_CANDIDATE_SCHEMA,
        )
        barra = pl.DataFrame(
            [
                {
                    "cusip9": "000000001",
                    "barrid": "USA0001",
                    "issuerid": "ISSUER1",
                    "rootid": "ROOT1",
                    "instrument": "STOCK",
                    "barra_start_date": date(2020, 1, 1),
                    "barra_end_date": date(2075, 12, 31),
                },
                {
                    "cusip9": "000000002",
                    "barrid": "USA0002",
                    "issuerid": "ISSUER1",
                    "rootid": "ROOT2",
                    "instrument": "STOCK",
                    "barra_start_date": date(2020, 1, 1),
                    "barra_end_date": date(2075, 12, 31),
                },
            ],
            schema=BARRA_CANDIDATE_SCHEMA,
        )

        mapping = build_security_mapping(snapshot, comp, barra, barra_available=True)
        grouped = group_security_relationships_by_cik(mapping)

        self.assertEqual(list(grouped), ["0000000100"])
        self.assertEqual(len(grouped["0000000100"]), 3)
        self.assertEqual(
            {(row["cusip9"], row["iid"], row["barrid"]) for row in grouped["0000000100"]},
            {
                ("000000001", "01", "USA0001"),
                ("000000001", "01C", "USA0001"),
                ("000000002", "02", "USA0002"),
            },
        )
        self.assertEqual(mapping.get_column("cik").unique().to_list(), ["0000000100"])

    def test_effective_intervals_include_both_boundaries(self) -> None:
        self.assertTrue(effective_interval_contains(date(2026, 8, 31), date(2026, 9, 1), MEMBERSHIP_DATE))
        self.assertTrue(effective_interval_contains(date(2026, 8, 1), date(2026, 8, 31), MEMBERSHIP_DATE))
        self.assertFalse(effective_interval_contains(date(2026, 9, 1), date(2026, 9, 2), MEMBERSHIP_DATE))
        self.assertFalse(effective_interval_contains(date(2026, 8, 1), date(2026, 8, 30), MEMBERSHIP_DATE))

        snapshot = snapshot_frame("000000001")
        comp = pl.DataFrame(
            [
                {"cusip9": "000000001", "gvkey": "001000", "iid": "01", "cik": "100", "efffrom": MEMBERSHIP_DATE, "effthru": MEMBERSHIP_DATE},
                {"cusip9": "000000001", "gvkey": "002000", "iid": "01", "cik": "200", "efffrom": date(2026, 9, 1), "effthru": date(2900, 1, 1)},
                {"cusip9": "000000001", "gvkey": "003000", "iid": "01", "cik": "300", "efffrom": date(2020, 1, 1), "effthru": date(2026, 8, 30)},
            ],
            schema=COMP_CANDIDATE_SCHEMA,
        )
        mapping = build_security_mapping(snapshot, comp)
        self.assertEqual(mapping.get_column("gvkey").unique().to_list(), ["001000"])

    def test_conflicts_and_unresolved_members_remain_visible(self) -> None:
        snapshot = snapshot_frame("000000001", "000000002", "000000003")
        comp = pl.DataFrame(
            [
                {"cusip9": "000000001", "gvkey": "001000", "iid": "01", "cik": "100", "efffrom": date(2020, 1, 1), "effthru": date(2900, 1, 1)},
                {"cusip9": "000000001", "gvkey": "002000", "iid": "01", "cik": "200", "efffrom": date(2020, 1, 1), "effthru": date(2900, 1, 1)},
                {"cusip9": "000000002", "gvkey": "003000", "iid": "01", "cik": None, "efffrom": date(2020, 1, 1), "effthru": date(2900, 1, 1)},
            ],
            schema=COMP_CANDIDATE_SCHEMA,
        )
        mapping = build_security_mapping(
            snapshot, comp, pl.DataFrame(schema=BARRA_CANDIDATE_SCHEMA), barra_available=True
        )
        statuses = mapping.group_by("cusip9").agg(pl.col("mapping_status").first()).to_dicts()
        by_cusip = {row["cusip9"]: row["mapping_status"] for row in statuses}

        self.assertEqual(by_cusip["000000001"], "ambiguous_gvkey")
        self.assertEqual(by_cusip["000000002"], "missing_cik")
        self.assertEqual(by_cusip["000000003"], "no_gvkey_match")
        self.assertEqual(mapping.filter(pl.col("cusip9") == "000000001").height, 2)
        self.assertEqual(mapping.filter(pl.col("cusip9") == "000000003").height, 1)
        self.assertEqual(
            mapping.filter(pl.col("cusip9") == "000000003").get_column("barra_mapping_status")[0],
            "unmatched",
        )


class FilingStorageTests(unittest.TestCase):
    def test_comparative_periods_and_original_tags_are_retained(self) -> None:
        accession = "000000000123000001"
        payload = {
            "facts": {
                "us-gaap": {
                    "Assets": {
                        "units": {
                            "USD": [
                                {"accn": accession, "val": 50, "end": "2025-12-31", "fy": 2025, "fp": "FY", "form": "10-K", "filed": "2026-02-20"}
                            ]
                        }
                    },
                    "Revenues": {
                        "units": {
                            "USD": [
                                {"accn": accession, "val": 30, "start": "2025-01-01", "end": "2025-12-31", "fy": 2025, "fp": "FY", "form": "10-K", "filed": "2026-02-20"},
                                {"accn": accession, "val": 20, "start": "2024-01-01", "end": "2024-12-31", "fy": 2024, "fp": "FY", "form": "10-K", "filed": "2026-02-20"},
                            ]
                        }
                    },
                }
            }
        }
        filing = {"form": "10-K", "reportDate": "2025-12-31", "filingDate": "2026-02-20"}

        balance, income = extract_accession_facts(
            "0000000100", accession, filing, payload, TAG_TO_METRIC
        )

        self.assertEqual(balance.height, 1)
        self.assertEqual(balance["tag"][0], "Assets")
        self.assertEqual(balance["period_type"][0], "instant")
        self.assertEqual(income.height, 2)
        self.assertEqual(income.get_column("tag").unique().to_list(), ["Revenues"])
        self.assertEqual(set(income.get_column("period_start").to_list()), {date(2025, 1, 1), date(2024, 1, 1)})
        self.assertEqual(income.get_column("period_type").unique().to_list(), ["duration"])
        self.assertEqual(income.get_column("source_accession").unique().to_list(), [accession])
        self.assertEqual(income.get_column("currency").unique().to_list(), ["USD"])
        self.assertEqual(income.get_column("value_text").to_list(), ["30", "20"])
        self.assertEqual(income.get_column("report_period").unique().to_list(), [date(2025, 12, 31)])

    def test_accessions_are_separate_and_changed_snapshots_are_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            filing = {
                "form": "10-K",
                "reportDate": "2025-12-31",
                "filingDate": "2026-02-20",
                "acceptanceDateTime": "2026-02-20T16:30:00.000Z",
            }
            accession_one = "000000000123000001"
            accession_two = "000000000123000002"
            report_one = {
                "cik": "0000000100",
                "form": "10-K",
                "accession": accession_one,
                "report_period": "2025-12-31",
                "filing_date": "2026-02-20",
                "companyfacts_sha256": "facts-a",
                "submission_metadata_sha256": "submissions-a",
                "gvkey_relationships": [{"gvkey": "001000"}],
            }
            empty = pl.DataFrame(schema=STATEMENT_SCHEMA)
            balance = statement_frame("Assets", 50)

            path_one, snapshot_one, created = write_filing_bundle(
                root, filing, accession_one, "0000000100", report_one, balance, empty
            )
            self.assertTrue(created)
            _, repeated_snapshot, repeated_created = write_filing_bundle(
                root, filing, accession_one, "0000000100", report_one, balance, empty
            )
            self.assertEqual(repeated_snapshot, snapshot_one)
            self.assertFalse(repeated_created)

            changed_report = {**report_one, "companyfacts_sha256": "facts-b"}
            _, snapshot_two, changed_created = write_filing_bundle(
                root, filing, accession_one, "0000000100", changed_report, statement_frame("Assets", 60), empty
            )
            self.assertTrue(changed_created)
            self.assertNotEqual(snapshot_one, snapshot_two)
            self.assertEqual(len(list((path_one / "snapshots").iterdir())), 2)
            old_balance = pl.read_parquet(path_one / "snapshots" / snapshot_one / "balance_sheet.parquet")
            self.assertEqual(old_balance["value"][0], 50)

            filing_two = {**filing, "form": "10-K/A"}
            report_two = {
                **report_one,
                "form": "10-K/A",
                "accession": accession_two,
                "companyfacts_sha256": "facts-c",
            }
            path_two, _, _ = write_filing_bundle(
                root, filing_two, accession_two, "0000000100", report_two, balance, empty
            )
            self.assertNotEqual(path_one, path_two)
            self.assertTrue((path_one / "report.json").exists())
            self.assertTrue((path_two / "report.json").exists())
            self.assertIn("CIK0000000100", str(path_one))

    def test_response_archiving_and_inventory_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first_time = datetime(2026, 10, 1, tzinfo=timezone.utc)
            path_one, hash_one, stored_time = archive_api_response(
                root, "100", "companyfacts", b'{"facts":{}}', first_time
            )
            path_two, hash_two, repeated_time = archive_api_response(
                root,
                "100",
                "companyfacts",
                b'{"facts":{}}',
                datetime(2026, 10, 2, tzinfo=timezone.utc),
            )
            self.assertEqual(path_one, path_two)
            self.assertEqual(hash_one, hash_two)
            self.assertEqual(stored_time, repeated_time)
            archive_api_response(root, "100", "companyfacts", b'{"facts":{"x":1}}', first_time)
            self.assertEqual(len(list(path_one.parent.glob("*.json"))), 2)

            inventory = root / "filing_inventory.jsonl"
            record = {"cik": "0000000100", "accession": "A1", "status": "stored"}
            update_jsonl_inventory(inventory, record, ("cik", "accession"))
            before = inventory.read_bytes()
            update_jsonl_inventory(inventory, record, ("cik", "accession"))
            self.assertEqual(inventory.read_bytes(), before)
            update_jsonl_inventory(
                inventory,
                {**record, "accession": "A2", "status": "no_supported_facts"},
                ("cik", "accession"),
            )
            rows = [json.loads(line) for line in inventory.read_text().splitlines()]
            self.assertEqual(len(rows), 2)

    def test_filing_without_supported_facts_has_empty_tables_and_inventory_status(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            accession = "000000000123000003"
            filing = {
                "form": "10-K",
                "reportDate": "2025-12-31",
                "filingDate": "2026-02-20",
                "accessionNumber": accession,
            }
            balance, income = extract_accession_facts(
                "0000000100", accession, filing, {"facts": {}}, TAG_TO_METRIC
            )
            self.assertTrue(balance.is_empty())
            self.assertTrue(income.is_empty())
            report = {
                "cik": "0000000100",
                "form": "10-K",
                "accession": accession,
                "report_period": "2025-12-31",
                "filing_date": "2026-02-20",
                "companyfacts_sha256": "facts-empty",
                "submission_metadata_sha256": "submissions-empty",
                "extraction_status": "no_supported_facts",
            }
            filing_dir, snapshot_id, _ = write_filing_bundle(
                root, filing, accession, "0000000100", report, balance, income
            )
            update_jsonl_inventory(
                root / "filing_inventory.jsonl",
                {
                    "cik": "0000000100",
                    "accession": accession,
                    "extraction_status": "no_supported_facts",
                    "source_snapshot_id": snapshot_id,
                    "filing_path": str(filing_dir.relative_to(root)),
                },
                ("cik", "accession"),
            )
            stored = json.loads((filing_dir / "report.json").read_text())
            inventory = json.loads((root / "filing_inventory.jsonl").read_text().strip())
            self.assertEqual(stored["extraction_status"], "no_supported_facts")
            self.assertEqual(inventory["extraction_status"], "no_supported_facts")
            self.assertTrue((filing_dir / "balance_sheet.parquet").exists())
            self.assertTrue((filing_dir / "income_statement.parquet").exists())


if __name__ == "__main__":
    unittest.main()
