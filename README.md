# Silver Fund Data Pipeline Repository

## Setup
1. Sync the Python environment.

```bash
uv sync
```

2. Add a `.env` file at the repo root with:
- `ROOT`: your home directory path
- `WRDS_USER`: your WRDS username
- `SEC_IDENTITY`: the email identity used for SEC / EDGAR requests

## Running Pipelines
Activate the environment and use the package CLI:

```bash
python -m pipelines --help
```

General form:

```bash
python -m pipelines <command> <pipeline_type> [OPTIONS]
```

Common arguments:
- `pipeline_type`: `backfill` or `update` depending on the command
- `--database`: `research`, `production`, or `development`
- `--start`: backfill start date in `YYYY-MM-DD`
- `--end`: backfill end date in `YYYY-MM-DD`

Examples:

```bash
python -m pipelines barra backfill --database production --start 2020-01-01 --end 2020-12-31
python -m pipelines barra update --database production
python -m pipelines ftse backfill --database development --start 2025-01-01 --end 2025-12-31
python -m pipelines signals backfill --database development --start 2024-01-01 --end 2026-03-31
```

## 10-K Pipeline
The `ten-k` command builds the yearly 10-K filings tables used by the text-similarity signal.

The flow:
- reads FTSE Russell membership from the database
- keeps current Russell 1000 / Russell 2000 names
- uses the linked `cik` values to query EDGAR
- stores one yearly parquet file per calendar year in `ten_k_filings`

Historical backfill example:

```bash
python -m pipelines ten-k backfill --database development --start 2024-01-01 --end 2024-12-31
```

Current-year refresh example through today:

```bash
python -m pipelines ten-k backfill --database development --today
```

Daily workflow note:
- `--today` uses the latest FTSE Russell snapshot on or before today
- it only rechecks companies whose last saved 10-K is at least 245 trading days old
- use `--start` / `--end` when you want a true historical backfill instead of the lightweight daily refresh

Typical order for the 10-K research workflow:
1. Backfill FTSE Russell so the table has `cusip` and `cik` coverage.
2. Run `ten-k backfill` for the desired date window.
3. Run `signals backfill` to rebuild `ten_k_similarity` alongside the other signals.
