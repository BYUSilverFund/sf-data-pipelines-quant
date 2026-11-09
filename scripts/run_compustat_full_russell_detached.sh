#!/usr/bin/env bash
set -uo pipefail

PROJECT=/home/gunnjake/projects/sf-data-pipelines-quant
OUTPUT_ROOT=/home/gunnjake/groups/grp_quant/database/fundamentals/compustat
START_DATE=2024-01-01
END_DATE=2026-10-29
LOG_PATH="$OUTPUT_ROOT/source_extracts/full_russell_${START_DATE}_through_${END_DATE}.log"
PYTHON="$PROJECT/.venv/bin/python"

cd "$PROJECT"
mkdir -p "$OUTPUT_ROOT/source_extracts"
"$PYTHON" -m pipelines compustat \
  --source-database development \
  --start "$START_DATE" \
  --end "$END_DATE" >"$LOG_PATH" 2>&1 &
collector_pid=$!
wait "$collector_pid"
collector_status=$?

run_dir=$(sed -n 's/^.*retained source extracts: //p' "$LOG_PATH" | tail -1)
if [[ -n "$run_dir" && -f "$run_dir/run.json" ]]; then
  "$PYTHON" "$PROJECT/scripts/finalize_compustat_run.py" \
    --pid "$collector_pid" \
    --collector-exit-code "$collector_status" \
    --run-dir "$run_dir" \
    --output-root "$OUTPUT_ROOT" >>"$LOG_PATH" 2>&1
  audit_status=$?
else
  audit_status=1
  printf '\nCompletion audit unavailable: collection did not report a retained run directory. Exit code: %s\n' \
    "$collector_status" >>"$LOG_PATH"
fi

if [[ "$collector_status" -ne 0 ]]; then
  exit "$collector_status"
fi
exit "$audit_status"
