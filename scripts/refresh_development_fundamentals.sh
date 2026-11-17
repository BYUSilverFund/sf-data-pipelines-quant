#!/usr/bin/env bash

#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --job-name=dev-fundamentals
#SBATCH --output=logs/slurm-development-fundamentals-%j.out

set -Eeuo pipefail

RESUME_RUN_ID=""
if [[ "${1:-}" == "--resume-from-run" ]]; then
    [[ $# -eq 2 ]] || { echo "Usage: $0 [--resume-from-run RUN_ID]" >&2; exit 2; }
    RESUME_RUN_ID="$2"
elif [[ $# -gt 0 ]]; then
    echo "Usage: $0 [--resume-from-run RUN_ID]" >&2
    exit 2
fi

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${FUNDAMENTALS_PYTHON_BIN:-$PROJECT/.venv-fips/bin/python}"
START_DATE="${COMPUSTAT_START_DATE:-2024-01-01}"
END_DATE="${COMPUSTAT_END_DATE:-$(date -u +%F)}"
JOB_LABEL="${SLURM_JOB_ID:-manual-$(date -u +%Y%m%dT%H%M%SZ)}"
REVIEW_ARTIFACT="${SEC_OCF_REVIEW_ARTIFACT:-$PROJECT/config/sec_ocf_full_universe_candidate_review_v1.json}"
if [[ "${SEC_OCF_NO_REVIEWS:-0}" == "1" ]]; then
    RESOLVER_REVIEW_ARGS=(--no-sec-reviews)
else
    RESOLVER_REVIEW_ARGS=(--review-artifact "$REVIEW_ARTIFACT")
fi

process_archived_evidence() {
    local process_output
    local collection_cutoff="${FUNDAMENTALS_COLLECTION_CUTOFF:-${CUTOFF%%T*}}"
    process_output=$("$PYTHON" -m scripts.process_archived_sec_ocf_candidates \
        --run-id "$RUN_ID" --collection-cutoff "$collection_cutoff")
    FULL_UNIVERSE_RESULT=$(printf '%s\n' "$process_output" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["processing_manifest"])')
    [[ -f "$FULL_UNIVERSE_RESULT" ]] || { echo "Full-universe processing manifest is missing" >&2; return 1; }
}

cd "$PROJECT"
mkdir -p "$PROJECT/logs"

ROOT="$($PYTHON -c 'from dotenv import load_dotenv; import os; load_dotenv(override=True); print(os.environ["ROOT"])')"
COMPUSTAT_ROOT="$ROOT/groups/grp_quant/database/fundamentals/compustat"
RUN_LOG="$COMPUSTAT_ROOT/source_extracts/development_refresh_${JOB_LABEL}_${START_DATE}_through_${END_DATE}.log"
mkdir -p "$COMPUSTAT_ROOT/source_extracts"

on_error() {
    local status=$?
    trap - ERR
    printf 'Development fundamentals refresh failed (exit %s). Log: %s\n' "$status" "$RUN_LOG" >&2
    if [[ -n "${RUN_LOG:-}" ]]; then
        printf 'Development fundamentals refresh failed (exit %s).\n' "$status" >> "$RUN_LOG" 2>/dev/null || true
    fi
    exit "$status"
}
trap on_error ERR

if [[ -n "$RESUME_RUN_ID" ]]; then
    [[ "$RESUME_RUN_ID" =~ ^run_[a-f0-9]{24}$ ]] || {
        echo "Invalid completed Compustat run ID: $RESUME_RUN_ID" >&2; exit 2;
    }
    RUN_DIR="$COMPUSTAT_ROOT/source_extracts/$RESUME_RUN_ID"
    [[ -f "$RUN_DIR/run.json" && -f "$RUN_DIR/completion_report.json" ]] || {
        echo "Run metadata or completion report is missing: $RUN_DIR" >&2; exit 1;
    }
    mapfile -t run_values < <("$PYTHON" - "$RUN_DIR/run.json" "$RUN_DIR/completion_report.json" <<'PY'
import json
import sys
from pathlib import Path

metadata = json.loads(Path(sys.argv[1]).read_text())
completion = json.loads(Path(sys.argv[2]).read_text())
if metadata.get("run_id") != completion.get("run_id"):
    raise SystemExit("Run metadata and completion report IDs differ")
if not (completion.get("complete") is True and completion.get("collector_exit_code") == 0
        and completion.get("collector_process_exited") is True):
    raise SystemExit("Saved Compustat run is not complete and successful")
mapping = Path(metadata["mapping_snapshot"]).resolve()
if not mapping.is_file():
    raise SystemExit("Exact mapping snapshot recorded by the run is missing")
print(metadata["run_id"])
print(mapping)
print(metadata["membership_snapshot_date"])
PY
    )
    RUN_ID="${run_values[0]}"
    MAPPING_SNAPSHOT="${run_values[1]}"
    MAPPING_DATE="${run_values[2]}"
    [[ "$RUN_ID" == "$RESUME_RUN_ID" ]] || { echo "Run ID validation failed" >&2; exit 1; }
    CUTOFF="${FUNDAMENTALS_ACCEPTANCE_CUTOFF:?Set timezone-aware FUNDAMENTALS_ACCEPTANCE_CUTOFF}"
    printf 'Resuming saved Compustat run %s with its recorded mapping %s\n' "$RUN_ID" "$MAPPING_SNAPSHOT"
    process_archived_evidence
    "$PYTHON" -u -m scripts.build_development_fundamentals \
        --run-id "$RUN_ID" --mapping-snapshot "$MAPPING_SNAPSHOT" \
        --mapping-date "$MAPPING_DATE" --cutoff "$CUTOFF" \
        --full-universe-result "$FULL_UNIVERSE_RESULT" \
        "${RESOLVER_REVIEW_ARGS[@]}"
    printf 'Development fundamentals saved-run workflow completed successfully.\n'
    exit 0
fi

# The WRDS client can prompt when credentials are unavailable. Batch jobs must
# fail before starting a collector rather than wait on an interactive prompt.
"$PYTHON" - <<'PY'
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path.cwd() / ".env", override=True)
if not os.getenv("WRDS_USER"):
    raise SystemExit("WRDS authentication preflight failed: WRDS_USER is not configured")
password = os.getenv("WRDS_PASSWORD")
pgpass = Path.home() / ".pgpass"
pgpass_ok = pgpass.is_file() and os.access(pgpass, os.R_OK)
if not password and not pgpass_ok:
    raise SystemExit("WRDS authentication preflight failed: configure WRDS_PASSWORD or a readable ~/.pgpass")
print("WRDS authentication configuration preflight passed (values withheld)")
PY

printf 'Starting development Compustat refresh %s through %s\n' "$START_DATE" "$END_DATE"
set +e
"$PYTHON" -u -m pipelines compustat \
    --source-database development --start "$START_DATE" --end "$END_DATE" \
    2>&1 | tee -a "$RUN_LOG" &
collector_pid=$!
wait "$collector_pid"
collector_status=$?
set -e

RUN_DIR=$(sed -n 's/^.*retained source extracts: //p' "$RUN_LOG" | tail -1)
if [[ -z "$RUN_DIR" || ! -f "$RUN_DIR/run.json" ]]; then
    printf 'Collector did not retain a run directory; exit code %s.\n' "$collector_status" >&2
    if [[ "$collector_status" -eq 0 ]]; then
        exit 1
    fi
    exit "$collector_status"
fi

"$PYTHON" "$PROJECT/scripts/finalize_compustat_run.py" \
    --pid "$collector_pid" --collector-exit-code "$collector_status" \
    --run-dir "$RUN_DIR" --output-root "$COMPUSTAT_ROOT" 2>&1 | tee -a "$RUN_LOG"

if [[ "$collector_status" -ne 0 ]]; then
    exit "$collector_status"
fi

mapfile -t run_values < <("$PYTHON" - "$RUN_DIR/run.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    run = json.load(stream)
print(run["run_id"])
print(run["mapping_snapshot"])
print(run["membership_snapshot_date"])
PY
)
RUN_ID="${run_values[0]}"
MAPPING_SNAPSHOT="${run_values[1]}"
MAPPING_DATE="${run_values[2]}"
[[ "$RUN_ID" =~ ^run_[a-f0-9]{24}$ ]]
[[ -f "$RUN_DIR/completion_report.json" ]]
[[ -f "$MAPPING_SNAPSHOT" ]]
"$PYTHON" - "$RUN_DIR/completion_report.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    completion = json.load(stream)
if completion.get("complete") is not True:
    raise SystemExit("Compustat completion audit did not pass")
PY

CUTOFF="${FUNDAMENTALS_ACCEPTANCE_CUTOFF:?Set timezone-aware FUNDAMENTALS_ACCEPTANCE_CUTOFF}"
printf 'Building development tables from %s with mapping %s and cutoff %s\n' \
    "$RUN_ID" "$MAPPING_SNAPSHOT" "$CUTOFF" | tee -a "$RUN_LOG"
process_archived_evidence
"$PYTHON" -u -m scripts.build_development_fundamentals \
    --run-id "$RUN_ID" --mapping-snapshot "$MAPPING_SNAPSHOT" \
    --mapping-date "$MAPPING_DATE" --cutoff "$CUTOFF" \
    --full-universe-result "$FULL_UNIVERSE_RESULT" \
    "${RESOLVER_REVIEW_ARGS[@]}" \
    --rebuild-query-scope 2>&1 | tee -a "$RUN_LOG"

printf 'Development fundamentals refresh completed successfully.\n' | tee -a "$RUN_LOG"
