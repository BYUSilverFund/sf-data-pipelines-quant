"""Extract the single documented SEC fallback pilot from saved JSON evidence."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from pipelines.sec_fallback import extract_aat_q3_operating_cash_flow_ytd, sha256_file


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Extract AAT FY2026 Q3 YTD operating cash flow from saved SEC "
            "Company Facts and submissions responses; writes separate Parquet."
        )
    )
    parser.add_argument("--companyfacts", required=True, type=Path)
    parser.add_argument("--submissions", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--extracted-at",
        help="ISO-8601 UTC extraction time; defaults to now.",
    )
    args = parser.parse_args()

    facts = json.loads(args.companyfacts.read_text(encoding="utf-8"))
    submissions = json.loads(args.submissions.read_text(encoding="utf-8"))
    recent = submissions.get("filings", {}).get("recent", {})
    accession_numbers = recent.get("accessionNumber", [])
    if accession_numbers.count("0001500217-26-000052") != 1:
        parser.error("submissions response must contain the pilot accession exactly once")
    index = accession_numbers.index("0001500217-26-000052")
    filing = {
        key: recent.get(key, [None] * len(accession_numbers))[index]
        for key in (
            "accessionNumber",
            "filingDate",
            "reportDate",
            "acceptanceDateTime",
            "form",
            "primaryDocument",
        )
    }
    extracted_at = args.extracted_at or datetime.now(timezone.utc).isoformat()
    frame = extract_aat_q3_operating_cash_flow_ytd(
        facts,
        filing,
        source_response_sha256=sha256_file(str(args.companyfacts)),
        submission_response_sha256=sha256_file(str(args.submissions)),
        extracted_at=extracted_at,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(args.output)
    print(f"Wrote {frame.height} SEC observation(s) to {args.output}")


if __name__ == "__main__":
    main()
