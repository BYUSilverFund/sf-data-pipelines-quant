#!/usr/bin/env bash
# WRDS PostgreSQL is reachable from the login partition; compute nodes do not
# have direct Internet access.
#SBATCH --partition=login
#SBATCH --qos=login
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=48G
#SBATCH --job-name=crsp-v2-research
#SBATCH --output=logs/crsp-v2-research-%j.out

set -euo pipefail

cd "${SLURM_SUBMIT_DIR:?Submit this job from the repository root}"

exec .venv/bin/python -m pipelines crsp-v2 backfill \
    --database research \
    --start 1925-01-01 \
    --end 2025-12-31
