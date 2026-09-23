#!/usr/bin/env bash

#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=32G
#SBATCH -J "quant-daily"
#SBATCH --output=logs/slurm-%j.out

# Slurm records the directory where sbatch was submitted.
SCRIPT_DIR="$SLURM_SUBMIT_DIR"

cd "$SCRIPT_DIR" || exit 1

mkdir -p "$SCRIPT_DIR/logs"

"$SCRIPT_DIR/scripts/run_pipeline_with_alerts.sh" \
    "$SCRIPT_DIR/logs/barra_update.log"