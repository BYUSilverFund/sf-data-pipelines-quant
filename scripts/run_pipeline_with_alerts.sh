#!/usr/bin/env bash

# Make pipeline failures inside pipes return a non-zero exit code.
set -o pipefail

# Resolve the repository root.
# This script lives in /scripts, so ".." moves up to the repo root.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load environment variables from the repo's .env file.
ENV_FILE="${SCRIPT_DIR}/.env"

if [ -f "$ENV_FILE" ]; then
    # Export all variables loaded from .env so child processes can use them.
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "Error: .env file not found at $ENV_FILE" >&2
    exit 1
fi


# ----------------------------------------------------
# Test Mode
# ----------------------------------------------------
# Run:
# ./scripts/run_pipeline_with_alerts.sh --test-failure logs/slack_test.log
#
# This intentionally creates a failed run so the Slack
# notification system can be tested without touching data.
if [ "$1" = "--test-failure" ]; then

    PIPELINE_NAME="test-failure"
    LOG_FILE="$2"

    # A log path is required for the test.
    if [ -z "$LOG_FILE" ]; then
        echo "Usage: ./run_pipeline_with_alerts.sh --test-failure LOG_FILE" >&2
        exit 1
    fi

    # Create the log directory if it does not already exist.
    mkdir -p "$(dirname "$LOG_FILE")"

    # Write a fake failure message to the log.
    echo "Intentional Quant pipeline failure test" > "$LOG_FILE"

    # Simulate a failed process.
    EXIT_CODE=1


# ----------------------------------------------------
# Pipeline Mode
# ----------------------------------------------------
else

    # Make sure DATABASE_ENV is set to a valid database.
    case "$DATABASE_ENV" in
        production|development|research)
            ;;
        *)
            echo "Error: DATABASE_ENV must be production, development, or research." >&2
            exit 1
            ;;
    esac

    PIPELINE_NAME="barra update --database $DATABASE_ENV"
    LOG_FILE="$1"

    # A log path is required when running the real pipeline.
    if [ -z "$LOG_FILE" ]; then
        echo "Usage: ./run_pipeline_with_alerts.sh LOG_FILE" >&2
        exit 1
    fi

    # Create the log directory if it does not already exist.
    mkdir -p "$(dirname "$LOG_FILE")"

    # Run the Quant Barra update using the selected database environment.
    # All stdout and stderr are written to the supplied log file.
    "$SCRIPT_DIR/.venv/bin/python" -m pipelines \
        barra update --database "$DATABASE_ENV" \
        > "$LOG_FILE" 2>&1

    # Save the pipeline's exit code so we know whether it succeeded.
    EXIT_CODE=$?
fi


# ----------------------------------------------------
# Failure Notification
# ----------------------------------------------------
# If either the real pipeline or test mode fails, send
# the last 20 lines of the log to Slack.
if [ "$EXIT_CODE" -ne 0 ]; then

    ERROR_DETAILS="$(tail -n 20 "$LOG_FILE")"

    # Verify Slack credentials were loaded from .env.
    if [ -z "$SLACK_BOT_TOKEN" ] || [ -z "$SLACK_CHANNEL_ID" ]; then
        echo "Error: Slack environment variables are not set. Cannot send alert." >&2
        exit "$EXIT_CODE"
    fi

    # Build the Slack alert message.
    printf -v SLACK_MESSAGE \
    'Quant pipeline failed\n*Pipeline:* `%s`\n*Server:* `%s`\n*Exit code:* `%s`\n*Recent log output:*\n```%s```' \
    "$PIPELINE_NAME" \
    "$(hostname)" \
    "$EXIT_CODE" \

    # Send the failure notification to Slack.
    SLACK_RESPONSE="$(curl -sS \
        -X POST "https://slack.com/api/chat.postMessage" \
        -H "Authorization: Bearer $SLACK_BOT_TOKEN" \
        --data-urlencode "channel=$SLACK_CHANNEL_ID" \
        --data-urlencode "text=$SLACK_MESSAGE")"

    # Slack can return HTTP success while still rejecting the API request,
    # so explicitly check the JSON response for "ok": true.
    if [[ "$SLACK_RESPONSE" != *'"ok":true'* ]]; then
        echo "Slack notification failed: $SLACK_RESPONSE" >&2
    fi
fi


# Return the same exit code as the pipeline.
# This lets cron correctly detect whether the job succeeded or failed.
exit "$EXIT_CODE"