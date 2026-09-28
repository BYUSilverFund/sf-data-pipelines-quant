# sf-data-pipelines-quant

Data pipelines built by the Silver Fund Quant team that run on BYU Research Computing infrastructure.

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/BYUSilverFund/sf-data-pipelines-quant.git
cd sf-data-pipelines-quant
```

### 2. Install `uv`

If `uv` is not already installed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
```

Then install project dependencies:

```bash
uv sync
```

### 3. Configure environment variables

Copy the example environment file:

```bash
cp example.env .env
```

Update the required values:

```bash
USERNAME=your_username
ROOT=/home/${USERNAME}
PROJECT_ROOT=/home/${USERNAME}/projects/sf-data-pipelines-quant

WRDS_USER=your_wrds_username
BYU_EMAIL=your_email@byu.edu

DATABASE_ENV=development
```

`USERNAME` is automatically inserted into paths that use `${USERNAME}`, so those paths do not need to be manually rewritten.

Valid database environments are:

* `development`
* `research`
* `production`

You must also have access to the following BYU Research Computing groups:

* `grp_quant`
* `grp_msci_barra`

### 4. Verify the environment

```bash
.venv/bin/python scripts/check_env.py
```

## Running Pipelines

Activate the virtual environment:

```bash
source .venv/bin/activate
```
```
### Barra Update

```bash
python -m pipelines barra update --database development
```

### Barra Backfill

```bash
python -m pipelines barra backfill \
    --database development \
    --start YYYY-MM-DD \
    --end YYYY-MM-DD
```

## Database

Quant data is stored in the shared Research Computing filesystem under:

```text
/home/<username>/groups/grp_quant/database/
```

The three database environments are:

```text
production/
development/
research/
```

Most datasets are stored as yearly Parquet files.

## Scheduled Pipelines

Production pipeline infrastructure uses Slurm and cron.

Developers working on deployment, scheduling, monitoring, or pipeline ownership should see:

```text
DEVELOPER.md
```
