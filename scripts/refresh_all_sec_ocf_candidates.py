"""Resume SEC evidence downloads for uniquely mapped null OCF-YTD periods."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from datetime import date
from typing import Any

from dotenv import load_dotenv

from scripts.build_sec_ocf_ytd_cohort import (
    refresh_archived_sec_evidence,
)
from scripts.sec_full_universe import load_or_create_discovery


def _select_company_periods(cases: tuple[dict[str, Any], ...], requested: list[str] | None) -> tuple[dict[str, Any], ...]:
    if not requested:
        return cases
    wanted: set[tuple[str, str]] = set()
    for value in requested:
        try:
            gvkey, datadate = value.split(":", 1)
            date.fromisoformat(datadate)
        except (ValueError, TypeError) as error:
            raise ValueError(f"invalid --company-period {value!r}; expected GVKEY:YYYY-MM-DD") from error
        wanted.add((gvkey.zfill(6), datadate))
    available = {(case["gvkey"], case["datadate"]) for case in cases}
    missing = sorted(wanted - available)
    if missing:
        raise ValueError(f"requested company-periods are not SEC candidates: {missing}")
    return tuple(case for case in cases if (case["gvkey"], case["datadate"]) in wanted)


def run(run_id: str, company_periods: list[str] | None = None) -> dict:
    load_dotenv(dotenv_path=Path.cwd() / ".env", override=True)
    root = Path(os.environ["ROOT"]).expanduser().resolve()
    compustat_root = root / "groups/grp_quant/database/fundamentals/compustat"
    run_dir = compustat_root / "source_extracts" / run_id
    metadata = json.loads((run_dir / "run.json").read_text())
    completion = json.loads((run_dir / "completion_report.json").read_text())
    if metadata.get("run_id") != run_id or completion.get("run_id") != run_id:
        raise ValueError("source run metadata identity mismatch")
    if not (completion.get("complete") is True
            and completion.get("collector_exit_code") == 0
            and completion.get("collector_process_exited") is True):
        raise ValueError("source run is not complete and successful")
    mapping_path = Path(metadata["mapping_snapshot"]).resolve()
    if not mapping_path.is_file():
        raise FileNotFoundError(mapping_path)

    archive_root = root / "groups/grp_quant/database/fundamentals/sec_filings"
    gaps, discovery_path, discovery = load_or_create_discovery(
        run_id=run_id, compustat_root=compustat_root, archive_root=archive_root,
        mapping_path=mapping_path,
    )
    gaps = _select_company_periods(gaps, company_periods)
    if not gaps:
        raise ValueError("no SEC candidates selected")
    print(
        f"source_run={run_id} null_company_periods={discovery['null_company_periods']} "
        f"selected_periods={len(gaps)} distinct_ciks={len({case['cik'] for case in gaps})} "
        f"discovery_input={discovery_path}",
        flush=True,
    )

    refresh = refresh_archived_sec_evidence(archive_root=archive_root, cases=gaps)
    print(f"SEC evidence refresh: {refresh}", flush=True)
    result = {
        "run_id": run_id,
        "null_company_periods": discovery["null_company_periods"],
        "mapped_periods": discovery["candidate_periods"],
        "selected_periods": len(gaps),
        "distinct_ciks": len({case["cik"] for case in gaps}),
        "discovery_input": str(discovery_path),
        "sec_refresh": refresh,
        "evidence_stage_only": True,
        "classification_stage": "pending offline Slurm processing",
    }
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--company-period", action="append", default=None,
        help="Limit this resumable SEC fetch to GVKEY:YYYY-MM-DD; repeat to select multiple periods.",
    )
    args = parser.parse_args()
    run(args.run_id, company_periods=args.company_period)


if __name__ == "__main__":
    main()
