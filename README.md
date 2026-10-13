# sf-data-pipelines-quant

Data pipelines built by the Silver Fund Quant team for BYU Research Computing.

## Setup

### 1. Clone the repository

```bash
mkdir -p ~/projects
cd ~/projects
git clone https://github.com/BYUSilverFund/sf-data-pipelines-quant.git
cd sf-data-pipelines-quant
```

Run the remaining commands from this directory.

### 2. Install dependencies

If `uv` is not installed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
```

Install dependencies and activate the environment:

```bash
uv sync
source .venv/bin/activate
```

Activate the environment again whenever you open a new terminal.

### 3. Configure `.env`

For a new installation:

```bash
cp example.env .env
nano .env
```

If `.env` already exists, edit it without replacing your settings.

Fill in these values:

```bash
USERNAME=your_research_computing_username
ROOT=/home/${USERNAME}
PROJECT_ROOT=/home/${USERNAME}/projects/sf-data-pipelines-quant

WRDS_USER=your_wrds_username
BYU_EMAIL=your_email@byu.edu

DATABASE_ENV=development
```

Adjust `PROJECT_ROOT` if you cloned the repository elsewhere.

Valid databases are `development`, `research`, and `production`.
`DATABASE_ENV` sets the wrapper's default; `--database` selects the
database for an individual Python command.

Do not commit `.env` or credentials.

### 4. Check access and configuration

Your account needs access to `grp_quant` and `grp_msci_barra`.

```bash
groups
.venv/bin/python scripts/check_env.py
```

Request any missing group access and resolve reported errors before running pipelines.

## WRDS and Duo

FTSE Russell and CRSP require a working WRDS account.

1. Sign in at https://wrds-www.wharton.upenn.edu/.
2. Complete account activation and Duo Mobile setup if prompted.
3. Set `WRDS_USER` in `.env`.
4. When running a WRDS pipeline, enter your WRDS credentials and approve the Duo push if requested.

Your WRDS credentials might differ from your BYU credentials.
Password characters might not appear while typing.

See the [WRDS Duo instructions](https://wrds-www.wharton.upenn.edu/pages/about/log-in-to-wrds-using-two-factor-authentication/).

## Running Pipelines

Run heavy updates and backfills within a Slurm compute allocation.
See [DEVELOPER.md](DEVELOPER.md) for submission instructions.

### Daily Update

Choose the database you want to update:

```bash
python -m pipelines barra update --database development
```

Replace `development` with `research` or `production` as needed.

The update runs:

1. Daily Barra datasets and asset mappings.
2. Assets.
3. Signals, scores, and alphas.

Existing historical assets files are skipped, while the current year
is rebuilt. Signals, scores, and alphas are recalculated from the
full available assets history.

This command does not refresh FTSE Russell or CRSP.

### FTSE Russell Backfill

If the database is missing FTSE Russell files, run:

```bash
python -m pipelines ftse backfill --database development
```

After it succeeds, retry the daily update for the same database.

Without date options, FTSE backfill requests July 31, 1995 through today.
WRDS authentication and Duo approval might be required.

### Barra Backfill

To rebuild a historical date range:

```bash
python -m pipelines barra backfill \
    --database development \
    --start 2026-01-01 \
    --end 2026-10-12
```

Adjust the database and dates as needed. This is separate from the
daily update and does not automatically run signals.

### Signals, Scores, and Alphas Only

To recalculate these outputs using existing assets:

```bash
python -m pipelines signals backfill --database development
```

This processes the full available assets history. The current
implementation does not use `--start` or `--end` to limit processing.

### Help

```bash
python -m pipelines --help
python -m pipelines barra --help
python -m pipelines ftse --help
python -m pipelines signals --help
```

## Common Problems

- **Missing `ftse_russell_*.parquet` files:** run FTSE backfill for the same database, then retry Barra.
- **WRDS authentication failed:** check your WRDS credentials, account status, and Duo setup.
- **WRDS connection closed:** check for a Duo request. A connection issue or authentication timeout could also cause this.

Do not include passwords or tokens when sharing error logs.

## Data Storage

Data is stored as yearly Parquet files under:

```text
/home/<username>/groups/grp_quant/database/
```

Each database has its own directory: `production`, `development`, or `research`.

## Developer Documentation

For Slurm, cron, scheduled updates across all three databases,
Slack alerts, logs, and verification, see [DEVELOPER.md](DEVELOPER.md).