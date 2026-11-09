# Compustat Russell run handoff

## Completed state (2026-10-29)

- The full collection and completion audit both finished successfully. No collector or finalizer remains active.
- Run ID: `run_bd766d2869a3a83fce895048`.
- Run window: 2024-01-01 through 2026-10-29; membership snapshot 2026-08-31 (2,970 securities; 2,949 eligible GVKEYs).
- Log: `/home/gunnjake/groups/grp_quant/database/fundamentals/compustat/source_extracts/full_russell_2024-01-01_through_2026-10-29.log`.
- Completion report: `/home/gunnjake/groups/grp_quant/database/fundamentals/compustat/source_extracts/run_bd766d2869a3a83fce895048/completion_report.json`.
- Completion report says `complete: true`, collector exit code 0, 6,261 annual rows and 30,336 quarterly rows representing 30,324 quarterly report identities. It checked all 36,585 identities, with zero missing reports, missing artifacts, identity mismatches, or sampled value mismatches.
- No commit or push was made.

## Authentication history

- Slurm job `14864815` on `m8` was canceled. Batch-mode WRDS authentication prompted for a username and aborted.
- Slurm preflight job `14878641` also failed noninteractively with `EOFError`.
- Login-host preflight succeeded: `wrds.Connection` loaded the WRDS schema list using the existing `.env`/`.pgpass` setup. No credential values were printed.
- Current coverage extract has 6,261 annual rows and 30,336 quarterly rows (12 repeated quarterly composite identities); `dvpd` remains null in all 6,261 annual rows. Excluded annual formats are 1,184 `FS/STD` and 5,753 `INDL/SUMM_STD` rows; quarterly `gpq` is absent from schema.

## Previous interrupted run artifacts

- Run directory: `/home/gunnjake/groups/grp_quant/database/fundamentals/compustat/source_extracts/run_04ddf6ed9238fe10b885a97a`
- Run ID: `run_04ddf6ed9238fe10b885a97a`
- Date range: 2024-01-01 through 2026-10-28 (prior interrupted attempt)
- Membership snapshot: 2026-08-31; 2,949 eligible GVKEYs from 2,970 member securities
- Retained `annual.parquet`, `quarterly.parquet`, `run.json`, and `coverage_report.json` are present.
- Source extracts contain 6,259 annual rows / 6,259 unique identities and 30,187 quarterly rows / 30,175 unique identities, keyed by `(gvkey, datadate, indfmt, datafmt)`.
- `dvpd` is null on all 6,259 annual rows. Excluded annual formats were 1,183 `FS/STD` rows and 5,752 `INDL/SUMM_STD` rows. Quarterly `gpq` is absent from schema.
- The prior interrupted writer created partial format-suffixed reports. They remain preserved; the completed run reused or updated current-series reports without deleting earlier snapshots.

## Code and checks already done

- Report storage identity includes `(period_frequency, gvkey, datadate, indfmt, datafmt)`; new paths include format suffixes and report metadata stores the direct format values.
- Added a coverage report builder and offline regression. Focused tests passed: 24 tests across the finalizer, collector, and lookup modules; `git diff --check` passed.
- Added `scripts/finalize_compustat_run.py` to audit saved report artifacts without changing statement data.
- The login-host run wrapper uses the actual run directory reported by the collector, then runs the completion audit.

## Follow-on notes

1. For detailed field-by-field analysis, use the completed run's `coverage_report.json` and `completion_report.json`.
2. Keep `FS/STD` and `SUMM_STD` separate from this series and preserve source nulls such as `dvpd`; handle specialized formats as a separate follow-on.
3. Preserve earlier snapshots. No commit or push has been made.

Do not call SEC or IBKR, alter saved financial data, collect specialized formats, delete snapshots, commit, or push unless the user gives new instructions.
