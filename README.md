# sf-data-pipelines-quant

Data pipelines built by the Silver Fund Quant team for BYU Research Computing.

## Setup

Install the locked project environment with `uv sync`, configure local
credentials and paths in an untracked `.env` based on `example.env`, and check
the environment with:

```bash
.venv/bin/python scripts/check_env.py
```

Do not commit credentials or `.env`.

## Compustat fundamentals

Run commands from the repository root. The Slurm wrapper
`scripts/refresh_development_fundamentals.sh` collects Compustat and creates
development tables. For an existing completed run, archived SEC collection is
resumable and uses the configured SEC identification:

```bash
.venv/bin/python -m scripts.refresh_all_sec_ocf_candidates --run-id "$COMPUSTAT_RUN_ID"
```

Classify only saved SEC evidence and emit a content-addressed processing
manifest, using an explicit collection cutoff:

```bash
.venv/bin/python -m scripts.process_archived_sec_ocf_candidates \
  --run-id "$COMPUSTAT_RUN_ID" --collection-cutoff "$SEC_COLLECTION_CUTOFF"
```

Use its `processing_manifest` output as the explicit input to the development
resolver:

```bash
.venv/bin/python -m scripts.build_development_fundamentals \
  --run-id "$COMPUSTAT_RUN_ID" \
  --full-universe-result "$FULL_UNIVERSE_RESULT" \
  --review-artifact config/sec_ocf_full_universe_candidate_review_v1.json \
  --cutoff "$SEC_COMPARISON_CUTOFF"
```

See [docs/fundamentals_table.md](docs/fundamentals_table.md) for table and
field definitions, units, keys, joins, provenance, and missing-value behavior.
