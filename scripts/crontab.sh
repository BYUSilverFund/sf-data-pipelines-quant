#!/usr/bin/env bash

# Find absolute path to current directory if running from script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [ -f "$ENV_FILE" ]; then
    # Sourcing .env natively in Bash allows nested variable references like ${USERNAME} to resolve
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "Error: .env file not found at $ENV_FILE" >&2
    exit 1
fi

mkdir -p "$SCRIPT_DIR/logs"

EXISTING_CRON=$(crontab -l 2>/dev/null | grep -v "sf-data-pipelines-quant" || true)

TEMP_CRON=$(mktemp)

cat > "$TEMP_CRON" << EOF
$EXISTING_CRON
10 2 * * * cd $SCRIPT_DIR && $SCRIPT_DIR/run_pipeline_with_alerts.sh $SCRIPT_DIR/logs/production_database.log
EOF

sed -i '/^$/d' "$TEMP_CRON"

crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo "Quant crontab updated successfully."
echo "View it with: crontab -l"
echo "Monitor logs with: tail -f $SCRIPT_DIR/logs/production_database.log"