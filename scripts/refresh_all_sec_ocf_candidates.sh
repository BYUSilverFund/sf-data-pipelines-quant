#!/usr/bin/env bash

set -Eeuo pipefail

PROJECT=/home/gunnjake/projects/sf-data-pipelines-quant
PYTHON="${FUNDAMENTALS_PYTHON_BIN:-$PROJECT/.venv-fips/bin/python}"
RUN_ID="${COMPUSTAT_RUN_ID:-run_e906597c98025ac25cdf006d}"

cd "$PROJECT"
if [[ -n "${SLURM_JOB_ID:-}" ]]; then
    echo "SEC evidence fetching must run from an approved network-enabled login environment, not a Slurm compute job." >&2
    exit 2
fi
printf 'Starting full mapped SEC OCF-YTD evidence refresh for %s\n' "$RUN_ID"
"$PYTHON" -u -m scripts.refresh_all_sec_ocf_candidates --run-id "$RUN_ID"
printf 'Full mapped SEC OCF-YTD evidence refresh and classification completed.\n'
