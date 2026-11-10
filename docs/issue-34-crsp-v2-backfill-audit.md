# Issue 34: CRSP v2 daily backfill audit

Audit date: 2026-11-09

Slurm job: `15203610` (`crsp-v2-research`)

Target: `research`, 1925-01-01 through 2025-12-31

## Backfill result

Slurm reports `COMPLETED` with exit code `0:0` after 00:58:51. The job rebuilt
the daily files for every year from 1925 through 2025, then completed the
monthly stage. The daily log is
`logs/crsp-v2-research-15203610.out`.

The separate all-years source audit, Slurm job `15204978`, also completed
with exit code `0:0`. It used
`scripts/audit_crsp_v2_daily_source.py`; its results are in
`logs/crsp-v2-source-audit-15204978.out`.

The 101 annual daily parquet files cover 1925-12-31 through 2025-12-31. The
first available date is 1925-12-31; the last available date in each annual
file is within that file's calendar year. The 2025 file covers
2025-01-02 through 2025-12-31. The rebuilt daily files contain 110,240,418
rows in total.

## Issue 34 checks

- The query selects `dlyhigh AS high`, `dlylow AS low`, and
  `dlyclose AS close` as separate WRDS fields.
- The regression suite passed all four checks for field mapping, exact-row
  deduplication, rejection of conflicting date/PERMNO records, and replacing
  an annual file.
- Apple (`PERMNO 14593`) on 2024-01-02 has high `188.44`, low `183.885`, and
  close/prc `185.64`; return and volume are `-0.035787` and `81752737`.
- A fresh comparison of the rebuilt 2024 file against WRDS found 2,404,395
  source rows, including 497 duplicated date/PERMNO groups and 506 extra
  exact-copy rows. After removing exact copies, source and rebuilt output
  each contain 2,403,889 rows. There were no conflicting source duplicate
  keys, missing or extra output keys, or value mismatches in open, high, low,
  close, prc, ret, retx, or vol.
- The all-years WRDS audit passed for 1925–2025. Across 110,280,373 source
  rows, it found 39,955 exact duplicate rows and no conflicting duplicate
  keys. After exact-row deduplication, all 110,240,418 output rows matched
  source keys and values for open, high, low, close, prc, ret, retx, and vol;
  there were zero missing or extra keys and zero field mismatches in every
  year.
- Across all 101 rebuilt annual files, the audit found zero duplicate
  date/PERMNO keys. The backfill removed 39,955 exact duplicate rows across
  100 years; conflicting duplicate records would stop the affected annual
  replacement.

The issue remains open on GitHub. This file records the completed checks and
their results; closing or commenting on the issue is a separate GitHub action.
