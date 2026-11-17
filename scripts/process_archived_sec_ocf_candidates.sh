#!/usr/bin/env bash

#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --job-name=sec-ocf-archive-process
#SBATCH --output=logs/slurm-sec-ocf-archive-process-%j.out

set -Eeuo pipefail

PROJECT=/home/gunnjake/projects/sf-data-pipelines-quant
PYTHON="${FUNDAMENTALS_PYTHON_BIN:-$PROJECT/.venv-fips/bin/python}"
RUN_ID="${COMPUSTAT_RUN_ID:-run_e906597c98025ac25cdf006d}"
if [[ -n "${FUNDAMENTALS_COLLECTION_CUTOFF:-}" ]]; then
    COLLECTION_CUTOFF="$FUNDAMENTALS_COLLECTION_CUTOFF"
elif [[ -n "${FUNDAMENTALS_ACCEPTANCE_CUTOFF:-}" ]]; then
    COLLECTION_CUTOFF="${FUNDAMENTALS_ACCEPTANCE_CUTOFF%%T*}"
else
    echo "Set FUNDAMENTALS_COLLECTION_CUTOFF or FUNDAMENTALS_ACCEPTANCE_CUTOFF" >&2
    exit 2
fi

cd "$PROJECT"
printf 'Processing archived SEC OCF-YTD evidence only for %s; no SEC requests will be made.\n' "$RUN_ID"
"$PYTHON" -u -m scripts.process_archived_sec_ocf_candidates \
    --run-id "$RUN_ID" --collection-cutoff "$COLLECTION_CUTOFF"
printf 'Archived-evidence processing completed.\n'
