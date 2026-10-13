#!/usr/bin/env bash

set -o pipefail

# Resolve the repository root.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$SCRIPT_DIR" || exit 1

# Load configuration.
ENV_FILE="$SCRIPT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE" >&2
    exit 1
fi

set -a
source "$ENV_FILE"
ENV_EXIT=$?
set +a

if [ "$ENV_EXIT" -ne 0 ]; then
    echo "Error: could not load $ENV_FILE" >&2
    exit "$ENV_EXIT"
fi

if [ "${1:-}" = "--test-failure" ]; then
    # Test alerts without modifying data.
    PIPELINE_NAME="test-failure"
    LOG_FILE="${2:-}"

    if [ -z "$LOG_FILE" ]; then
        echo "Usage: $0 --test-failure LOG_FILE" >&2
        exit 1
    fi

    mkdir -p "$(dirname "$LOG_FILE")" || exit 1

    echo "Intentional Quant pipeline failure test" > "$LOG_FILE"
    EXIT_CODE=1
else
    # An explicit database overrides the .env default.
    LOG_FILE="${1:-}"
    DATABASE_ENV="${2:-${DATABASE_ENV:-}}"

    if [ -z "$LOG_FILE" ]; then
        echo "Usage: $0 LOG_FILE [DATABASE]" >&2
        exit 1
    fi

    case "$DATABASE_ENV" in
        production|development|research)
            ;;
        *)
            echo "Error: database must be production, development, or research." >&2
            exit 1
            ;;
    esac

    export DATABASE_ENV

    PIPELINE_NAME="barra update --database $DATABASE_ENV"

    mkdir -p "$(dirname "$LOG_FILE")" || exit 1

    "$SCRIPT_DIR/.venv/bin/python" -m pipelines \
        barra update --database "$DATABASE_ENV" \
        > "$LOG_FILE" 2>&1

    EXIT_CODE=$?
fi

# Send a Slack alert if the pipeline failed.
if [ "$EXIT_CODE" -ne 0 ]; then
    ERROR_DETAILS="$(tail -n 20 "$LOG_FILE")"

    if [ -z "${SLACK_BOT_TOKEN:-}" ] || [ -z "${SLACK_CHANNEL_ID:-}" ]; then
        echo "Slack credentials are missing. Pipeline failed; see $LOG_FILE." >&2
        exit "$EXIT_CODE"
    fi

    printf -v SLACK_MESSAGE \
        'Quant pipeline failed\n*Pipeline:* `%s`\n*Server:* `%s`\n*Exit code:* `%s`\n*Recent log output:*\n```%s```' \
        "$PIPELINE_NAME" \
        "$(hostname)" \
        "$EXIT_CODE" \
        "$ERROR_DETAILS"

    SLACK_RESPONSE="$(curl -sS \
        --connect-timeout 10 \
        --max-time 30 \
        -X POST "https://slack.com/api/chat.postMessage" \
        -H "Authorization: Bearer $SLACK_BOT_TOKEN" \
        --data-urlencode "channel=$SLACK_CHANNEL_ID" \
        --data-urlencode "text=$SLACK_MESSAGE")"

    CURL_EXIT=$?

    if [ "$CURL_EXIT" -ne 0 ]; then
        echo "Slack request failed with curl exit code $CURL_EXIT." >&2
    elif ! printf '%s' "$SLACK_RESPONSE" |
        "$SCRIPT_DIR/.venv/bin/python" -c \
        'import json, sys; sys.exit(0 if json.load(sys.stdin).get("ok") is True else 1)'
    then
        echo "Slack notification failed: $SLACK_RESPONSE" >&2
    fi
fi

# Preserve the pipeline's exit status for Slurm.
exit "$EXIT_CODE"