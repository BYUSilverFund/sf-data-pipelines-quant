# Developer Guide

This document covers deployment, scheduling, monitoring, and maintenance for the Silver Fund Quant data pipelines.

For basic setup and manual pipeline usage, see `README.md`.

## Pipeline Infrastructure

The Quant pipelines run on BYU Research Computing and use:

* shared storage through `grp_quant`
* Barra source data through `grp_msci_barra`
* Slurm for compute-intensive scheduled jobs
* cron for automatic job submission
* optional Slack alerts for pipeline failures

The scheduled flow is:

```text
cron
  ↓
sbatch scripts/daily_barra.sh
  ↓
Slurm compute node
  ↓
scripts/run_pipeline_with_alerts.sh
  ↓
Quant pipeline
```

Heavy pipeline operations should not be run directly on a Research Computing login node.

## Environment

The account responsible for running scheduled pipelines should have a `.env` file in the repository root.

Example:

```bash
USERNAME=your_username

ROOT=/home/${USERNAME}
PROJECT_ROOT=/home/${USERNAME}/projects/sf-data-pipelines-quant

WRDS_USER=your_wrds_username
BYU_EMAIL=your_email@byu.edu

DATABASE_ENV=production
```

`USERNAME` is automatically substituted into variables containing `${USERNAME}`.

For example:

```bash
USERNAME=gunnjake
ROOT=/home/${USERNAME}
```

resolves to:

```text
/home/gunnjake
```

Valid values for `DATABASE_ENV` are:

```text
development
research
production
```

Use `development` when testing infrastructure changes.

The scheduled production account should normally use:

```bash
DATABASE_ENV=production
```

## Required Research Computing Groups

The pipeline account needs access to:

```text
grp_quant
grp_msci_barra
```

`grp_quant` provides access to the shared Quant database.

`grp_msci_barra` provides access to the Barra source data.

Verify access before enabling scheduled jobs.

## Environment Check

Run:

```bash
.venv/bin/python scripts/check_env.py
```

This should verify the required environment variables and shared filesystem access.

## Daily Slurm Job

The scheduled Quant pipeline should be submitted through Slurm:

```bash
sbatch scripts/daily_barra.sh
```

Do not schedule the Python pipeline directly from cron.

Slurm provides the memory and compute resources needed by the assets portion of the pipeline.

Check active jobs with:

```bash
squeue -u $USER
```

Check the status of a specific completed job with:

```bash
sacct -j <JOB_ID> \
    --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS
```

A successful job should report:

```text
COMPLETED
ExitCode 0:0
```

## Slurm Memory

The assets pipeline can use a significant amount of memory.

Memory is configured in:

```text
scripts/daily_barra.sh
```

For example:

```bash
#SBATCH --mem=96G
```

If a job ends with:

```text
OUT_OF_MEMORY
```

check the peak memory usage:

```bash
sacct -j <JOB_ID> \
    --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS
```

If the job repeatedly approaches its allocated memory, either increase the Slurm allocation or investigate memory usage in the assets pipeline.

Do not attempt to solve memory limits by running the job directly on a login node.

## Pipeline Logs

The main scheduled pipeline log is:

```text
logs/barra_update.log
```

View the most recent output:

```bash
tail -n 100 logs/barra_update.log
```

Follow the log while a pipeline is running:

```bash
tail -f logs/barra_update.log
```

Slurm also creates job-specific output files:

```text
logs/slurm-<JOB_ID>.out
```

## Assets Pipeline

The assets dataset is derived from several Quant datasets and is the most memory-intensive part of the daily update.

Historical assets are stored as yearly Parquet files:

```text
assets_1995.parquet
assets_1996.parquet
...
assets_2026.parquet
```

Historical years that already exist do not need to be rebuilt every day.

The daily pipeline should preserve completed historical years and update the current year as new Barra data becomes available.

Full historical backfills should be treated separately from normal daily updates.

## Cron

Cron is responsible only for submitting the Slurm job.

Install or update the Quant cron with:

```bash
./scripts/crontab.sh
```

View installed cron entries:

```bash
crontab -l
```

The expected scheduled flow is:

```text
cron
  ↓
sbatch scripts/daily_barra.sh
```

The actual pipeline then runs on a Slurm compute node.

Only one developer account should have the active production cron installed at a time. This prevents duplicate production updates.

## Optional Slack Alerts

Slack alerts are optional and only need to be configured on an account responsible for pipeline monitoring.

Add these values to that account's `.env`:

```bash
SLACK_BOT_TOKEN=
SLACK_CHANNEL_ID=
```

Do not commit real Slack credentials.

The wrapper sends an alert when a pipeline exits unsuccessfully. The alert includes:

* pipeline name
* server
* exit code
* recent log output

Test the alert system without modifying data:

```bash
./scripts/run_pipeline_with_alerts.sh \
    --test-failure \
    logs/slack_test.log
```

Delete the test log afterward if desired:

```bash
rm -f logs/slack_test.log
```

Developers who are not responsible for Slack monitoring do not need these variables.

## Testing Infrastructure Changes

Infrastructure changes should be tested against the development database first.

Set:

```bash
DATABASE_ENV=development
```

Then submit:

```bash
sbatch scripts/daily_barra.sh
```

Monitor:

```bash
squeue -u $USER
```

and:

```bash
tail -f logs/barra_update.log
```

After the job finishes:

```bash
sacct -j <JOB_ID> \
    --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS
```

Confirm:

```text
COMPLETED
0:0
```

before testing against production.

## Production Deployment

Before enabling the scheduled production job:

1. Confirm `scripts/check_env.py` passes.
2. Confirm `grp_quant` and `grp_msci_barra` access.
3. Test the Slurm job against `development`.
4. Confirm the development job completes successfully.
5. Set:

```bash
DATABASE_ENV=production
```

6. Submit one manual production Slurm job:

```bash
sbatch scripts/daily_barra.sh
```

7. Verify the production job completes successfully.
8. Install the production cron:

```bash
./scripts/crontab.sh
```

9. Confirm:

```bash
crontab -l
```

## Useful Commands

Active Slurm jobs:

```bash
squeue -u $USER
```

Today's Slurm jobs:

```bash
sacct -u $USER --starttime today \
    --format=JobID,JobName,State,ExitCode,Elapsed,MaxRSS
```

Pipeline log:

```bash
tail -f logs/barra_update.log
```

Installed cron:

```bash
crontab -l
```

Check database environment:

```bash
grep DATABASE_ENV .env
```
