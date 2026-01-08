# Silver Fund Data Pipeline Repository

## Setup
1. Initialize Python virtual environment

```bash
uv sync
```

2. Add a .env file in the root of your working directory with the following environment variables
- ROOT: The path to your home directory
- WRDS_USER: The username to your WRDS account

## Running pipelines
1. Activate Python virtual environment

```bash
source .venv/bin/activate
```

2. Run Desired Pipeline

### Usage
The CLI entrypoint exposes multiple subcommands:

```bash
python -m <package_name> <command> <pipeline_type> [OPTIONS]
```

To see all available commands:
```bash
python -m <package_name> --help
```

To see help for a specific command:
```bash
python -m <package_name> barra --help
```

### Common Arguments
1. pipeline_type
Specifies how the pipeline should run.
- backfill: Run a historical backfill between two dates
- update: Run the latest incremental update (only supported for Barra data)

2. --database
Valid values:
- research
- production
- development

3. --start
Start date for backfills
- format: YYYY-MM-DD

4. --end
End date for backfilss
- format: YYYY-MM-DD

### example usage for Barra data
1. Run the following to backfill or update barra data

```bash
python pipelines barra backfill --database production
python piplines barra update --database production
```