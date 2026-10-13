#!/usr/bin/env bash

#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=48G
#SBATCH -J "quant-daily"
#SBATCH --output=logs/slurm-%j.out

# Slurm records the directory where sbatch was submitted.
SCRIPT_DIR="$SLURM_SUBMIT_DIR"

cd "$SCRIPT_DIR" || exit 1

mkdir -p "$SCRIPT_DIR/logs"
OVERALL_EXIT=0

for DATABASE in production development research; do
    "$SCRIPT_DIR/scripts/run_pipeline_with_alerts.sh" \
        "$SCRIPT_DIR/logs/barra_update_${DATABASE}.log" \
        "$DATABASE"

    RESULT=$?
    if [ "$RESULT" -ne 0 ]; then
        OVERALL_EXIT=1
    fi
done

exit "$OVERALL_EXIT"