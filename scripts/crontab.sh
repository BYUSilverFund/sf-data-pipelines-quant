#!/bin/bash

mkdir -p logs

TEMP_CRON=$(mktemp)

cat > "$TEMP_CRON" << 'EOF'
0 2 * * * cd /home/amh1124/Projects/sf-data-pipelines-fulton && .venv/bin/python -m pipelines covariance-matrix > logs/covariance_matrix.log 2>&1
0 2 * * * cd /home/amh1124/Projects/sf-data-pipelines-quant && .venv/bin/python -m pipelines barra update --database production > logs/production_database.log 2>&1
EOF

crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo "Crontab updated. Monitor with: tail -f logs/covariance_matrix.log"