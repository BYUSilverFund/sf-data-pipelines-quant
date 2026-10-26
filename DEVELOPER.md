# Developer Guide

Scheduling, monitoring, and maintenance for the Quant pipelines.
For basic setup and pipeline commands, see [README.md](README.md).

## Daily Pipeline

Cron submits `scripts/daily_barra.sh` to Slurm.

The job runs production, development, and research sequentially.
For each database, it updates:

1. Daily Barra datasets and asset mappings.
2. Assets.
3. Signals, scores, and alphas.
4. Composite alphas.
5. Optimal weights.

Existing historical assets files are skipped; missing years and the
current year are rebuilt. Signals, scores, alphas, and composite
alphas currently recalculate the full available assets history.

Optimal weights run one optimization per date, so the daily job only
fills missing dates from the last 30 days. Historical weights come
from a separate `optimal-weights backfill` job.

If one database fails, the job attempts the remaining databases.
The overall job returns a nonzero exit code if any run fails.

FTSE Russell and CRSP refreshes are not included.

## Environment

The scheduling account needs:

- A configured `.env` in the repository root.
- Membership in `grp_quant` and `grp_msci_barra`.
- Installed project dependencies.

Check configuration:

```bash
.venv/bin/python scripts/check_env.py
```

The wrapper defaults to `DATABASE_ENV` from `.env`, but an explicit
database argument overrides it.

The daily Slurm script explicitly selects all three databases,
regardless of the `.env` default.

Heavy pipeline work should run on Slurm compute nodes.

## Slurm Resources

Current settings in `scripts/daily_barra.sh`:

```bash
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=48G
```

The October 13, 2026 scheduled run completed all three databases
in 46 minutes 40 seconds, with approximately 37.9 GiB peak memory.

That run predates composite alphas and optimal weights. The first
run after they were added fills up to 30 days of weights per database,
so it will take longer than later runs, which usually fill one day.

Runtime and memory could increase as data grows. Investigate
`TIMEOUT` or `OUT_OF_MEMORY` results before changing allocations.

## Optimal Weights Backfill

Run historical optimal weights as their own Slurm job, outside the
daily window:

```bash
mkdir -p logs

sbatch \
    --job-name=quant-optimal-weights \
    --time=24:00:00 \
    --nodes=1 \
    --ntasks=1 \
    --mem=48G \
    --output=logs/optimal-weights-%j.out \
    --wrap="cd '$PWD' && .venv/bin/python -m pipelines optimal-weights backfill --database development"
```

Each year is written when it finishes. Dates that already have weights
are skipped, so a timed-out job can be resubmitted with the same
command. The full-history runtime has not been measured yet; adjust
`--time` after the first run.

Avoid running a backfill while the daily job writes to the same
database.

## Test Development Only

Run from the repository root:

```bash
mkdir -p logs

sbatch \
    --job-name=quant-signals-dev \
    --time=02:00:00 \
    --nodes=1 \
    --ntasks=1 \
    --mem=48G \
    --output=logs/signals-dev-%j.out \
    --wrap="cd '$PWD' && '$PWD/scripts/run_pipeline_with_alerts.sh' '$PWD/logs/signals_dev.log' development"
```

This tests development without running production or research.

Changing `DATABASE_ENV` does not limit `daily_barra.sh` to one database.

## Run All Three Databases

From the repository root:

```bash
mkdir -p logs
/apps/slurm/latest/bin/sbatch scripts/daily_barra.sh
```

Save the returned numeric job ID.

Avoid overlapping jobs that write to the same database.

## Monitor and Verify

Check active jobs:

```bash
squeue -u "$USER"
```

Follow a pipeline log:

```bash
tail -f logs/barra_update_production.log
```

Press Ctrl+C to stop watching. The job continues running.

For a completed job, replace `JOB_ID` with its numeric ID:

```bash
sacct -j JOB_ID \
    --format=JobID,State,ExitCode,Elapsed,MaxRSS
```

Successful execution reports `COMPLETED` and `0:0`.

Check each database's output:

```bash
tail -n 5 logs/barra_update_production.log
tail -n 5 logs/barra_update_development.log
tail -n 5 logs/barra_update_research.log
```

Each should end with:

```text
Optimal weights completed.
```

Lines starting with `Skipping <date>:` mean the optimizer could not
solve that date. Those dates are retried on the next run.

Also inspect the job-specific Slurm log:

```bash
cat logs/slurm-JOB_ID.out
```

Pipeline logs are overwritten each run. Check their timestamps
before using them to verify a particular job.

Successful execution does not prove data quality. When validating
changes, check output dates, duplicate keys, and unexpected null
or infinite values in signals, scores, alphas, and composite alphas.
For optimal weights, also check that each date's weights sum to 1
and that the latest date matches the latest composite alphas date.

## Cron Setup

Only the designated scheduling account should install this cron.

Create the logs directory first:

```bash
mkdir -p logs
```

Edit cron:

```bash
EDITOR=nano crontab -e
```

Add the following line, replacing `YOUR_USERNAME` with the scheduling
account's username. Preserve unrelated cron entries.

```cron
10 2 * * * cd /home/YOUR_USERNAME/projects/sf-data-pipelines-quant && /apps/slurm/latest/bin/sbatch scripts/daily_barra.sh >> logs/cron_submit.log 2>&1
```

Adjust the repository path if needed.

This submits daily at 2:10 a.m. according to the cron server's timezone.
Slurm starts the job when resources become available.

Verify the entry:

```bash
crontab -l
```

Check submission results:

```bash
tail -n 10 logs/cron_submit.log
```

Then verify the submitted job using `sacct`.

Cron submission failures occur before the pipeline wrapper starts,
so they do not trigger its Slack alert.

If using `scripts/crontab.sh`, check that it installs the same
full-path command and submission logging before running it.

## Slack Failure Alerts

Configure these values in the scheduling account's `.env`:

```bash
SLACK_BOT_TOKEN=
SLACK_CHANNEL_ID=
```

Do not commit credentials.

Pipeline failure alerts include the database, server, exit code,
and last 20 log lines.

Test without modifying database data:

```bash
./scripts/run_pipeline_with_alerts.sh \
    --test-failure logs/slack_test.log
```

The test intentionally exits with code 1. Confirm the Slack message
contains the intentional test error.

Slurm termination, such as a timeout or memory kill, might prevent
the wrapper from sending an alert. Check Slurm status as well.

## FTSE Russell Refresh

FTSE membership data is an input to assets and is not refreshed
by the daily Barra job.

Run a recent FTSE refresh manually within a compute allocation:

```bash
python -m pipelines ftse backfill \
    --database development \
    --start 2026-10-01 \
    --end 2026-10-13
```

Adjust the dates and target database.

The tested connection required a WRDS password and Duo push.
Unattended FTSE scheduling remains unverified.

After refreshing FTSE, run the daily update for the same database
to rebuild current-year assets and recalculate signals, scores,
alphas, and composite alphas.

Existing optimal weights are not recomputed. To rebuild weights
after a membership change, delete the affected
`optimal_weights_<year>.parquet` files and run the optimal weights
backfill.

Existing historical assets are skipped by daily updates. Changes
to historical membership data might require a separate assets rebuild.

## Before Pushing Changes

Check syntax and whitespace:

```bash
bash -n scripts/daily_barra.sh
bash -n scripts/run_pipeline_with_alerts.sh
python -m py_compile pipelines/all_pipelines.py pipelines/__main__.py
git diff --check
```

Then:

1. Test development through Slurm.
2. Verify output data and resource usage.
3. Test all three databases.
4. Test Slack failure alerts if the wrapper changed.
5. Confirm the next cron submission and completion.
6. Update README and this guide to match the implementation.