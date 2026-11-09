import click
import datetime as dt
from pipelines.all_pipelines import (
    barra_backfill_pipeline,
    ftse_backfill_pipeline,
    crsp_backfill_pipeline,
    crsp_v2_backfill_pipeline,
    barra_daily_pipeline,
    fama_french_5_factors_flow,
)
from pipelines.signals_flow import signals_flow
from pipelines.fundamentals_flow import sec_fundamentals_flow
from pipelines.compustat_fundamentals import run_compustat_fundamentals
from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database

from dotenv import load_dotenv
import os

# Valid options
VALID_DATABASES = ["research", "production", "development"]
PIPELINE_TYPES = ["backfill", "update"]


@click.group()
def cli():
    """Main CLI entrypoint."""
    pass


@cli.command()
@click.argument(
    "pipeline_type", type=click.Choice(PIPELINE_TYPES, case_sensitive=False)
)
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1995, 7, 31)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def barra(pipeline_type, database, start, end):
    match pipeline_type:
        case "backfill":
            start = start.date() if hasattr(start, "date") else start
            end = end.date() if hasattr(end, "date") else end

            click.echo(f"Running barra backfill on '{database}' from {start} to {end}.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            barra_backfill_pipeline(start, end, database_instance)

        case "update":
            click.echo(f"Running update for {database} database.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            barra_daily_pipeline(database_instance)


@cli.command()
@click.argument(
    "pipeline_type",
    type=click.Choice(
        ["backfill"], case_sensitive=False
    ),  # Update is currently not supported
)
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1925, 1, 1)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def crsp(pipeline_type, database, start, end):
    load_dotenv(override=True)
    
    user = os.getenv("WRDS_USER")
    if user is None:
        raise EnvironmentError(
            "Missing required environment variable: WRDS_USER. "
            "Check your .env file."
        )

    match pipeline_type:
        case "backfill":
            start = start.date() if hasattr(start, "date") else start
            end = end.date() if hasattr(end, "date") else end

            click.echo(f"Running crsp backfill on '{database}' from {start} to {end}.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            crsp_backfill_pipeline(start, end, database_instance, user)


@cli.command()
@click.argument(
    "pipeline_type",
    type=click.Choice(
        ["backfill"], case_sensitive=False
    ),  # Update is currently not supported
)
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1925, 1, 1)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def crsp_v2(pipeline_type, database, start, end):
    load_dotenv(override=True)
    
    user = os.getenv("WRDS_USER")
    if user is None:
        raise EnvironmentError(
            "Missing required environment variable: WRDS_USER. "
            "Check your .env file."
        )

    match pipeline_type:
        case "backfill":
            start = start.date() if hasattr(start, "date") else start
            end = end.date() if hasattr(end, "date") else end

            click.echo(f"Running crsp backfill on '{database}' from {start} to {end}.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            crsp_v2_backfill_pipeline(start, end, database_instance, user)

@cli.command()
@click.argument(
    "pipeline_type",
    type=click.Choice(
        ["backfill"], case_sensitive=False
    ),  # Update is currently not supported
)
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1995, 7, 31)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def ftse(pipeline_type, database, start, end):
    load_dotenv(override=True)

    user = os.getenv("WRDS_USER")
    if user is None:
        raise EnvironmentError(
            "Missing required environment variable: WRDS_USER. "
            "Check your .env file."
        )

    match pipeline_type:
        case "backfill":
            start = start.date() if hasattr(start, "date") else start
            end = end.date() if hasattr(end, "date") else end

            click.echo(f"Running ftse backfill on '{database}' from {start} to {end}.")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            ftse_backfill_pipeline(start, end, database_instance, user)


@cli.command()
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research, production, or development).",
)
def fama_french(database):
    click.echo("Running Fama-French 5-factors pipeline...")

    database_name = DatabaseName(database)
    database_instance = Database(database_name)

    fama_french_5_factors_flow(database_instance)
    click.echo("Fama-French pipeline completed successfully.")


@cli.command()
@click.argument(
    "pipeline_type",
    type=click.Choice(
        ["backfill"], case_sensitive=False
    ),  # Update is currently not supported
)
@click.option(
    "--database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    required=True,
    help="Target database (research or database).",
)
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date(1996, 7, 31)),
    show_default=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(dt.date.today()),
    show_default=True,
    help="End date (YYYY-MM-DD).",
)
def signals(pipeline_type, database, start, end):
    match pipeline_type:
        case "backfill":
            start = start.date() if hasattr(start, "date") else start
            end = end.date() if hasattr(end, "date") else end

            click.echo(f"Running signals backfill on '{database}' from {start} to {end}...")

            database_name = DatabaseName(database)
            database_instance = Database(database_name)

            signals_flow(database_instance)


@cli.command()
@click.option(
    "--source-database",
    type=click.Choice(VALID_DATABASES, case_sensitive=False),
    default="development",
    show_default=True,
    help="Database whose latest Russell snapshot defines the company universe.",
)
@click.option(
    "--target-database",
    type=click.Choice([DatabaseName.FUNDAMENTALS.value], case_sensitive=False),
    default=DatabaseName.FUNDAMENTALS.value,
    show_default=True,
    help="Separate grp_quant database where SEC facts are stored.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    default=None,
    help="Fetch only the first N uniquely mapped CIKs (useful for a small initial run).",
)
@click.option(
    "--output-root",
    type=click.Path(file_okay=False, dir_okay=True),
    default=None,
    help="Override the SEC filing archive root; defaults under the target database.",
)
def fundamentals(source_database, target_database, limit, output_root):
    """Archive SEC filings and selected Company Facts for Russell members."""
    source = Database(DatabaseName(source_database))
    target = Database(DatabaseName(target_database))
    sec_fundamentals_flow(source, target, limit=limit, output_root=output_root)


@cli.command("compustat")
@click.option(
    "--source-database", type=click.Choice(VALID_DATABASES, case_sensitive=False),
    default="development", show_default=True,
    help="Database whose latest Russell snapshot defines the company universe.",
)
@click.option("--limit", type=click.IntRange(min=1), default=None,
              help="Collect only the first N eligible GVKEYs in sorted order.")
@click.option("--start", type=click.DateTime(formats=["%Y-%m-%d"]), required=True,
              help="First reporting date to include (YYYY-MM-DD).")
@click.option("--end", type=click.DateTime(formats=["%Y-%m-%d"]), default=lambda: dt.date.today().isoformat(),
              show_default=True, help="Last reporting date to include (YYYY-MM-DD).")
@click.option("--output-root", type=click.Path(file_okay=False, dir_okay=True), default=None,
              help="Override the Compustat fundamentals output root.")
def compustat(source_database, limit, start, end, output_root):
    """Collect annual and quarterly Compustat statements for Russell companies."""
    start_date = start.date() if hasattr(start, "date") else start
    end_date = end.date() if hasattr(end, "date") else end
    if start_date > end_date:
        raise click.BadParameter("--start must not be after --end", param_hint="--start")
    run_compustat_fundamentals(source_database.lower(), limit, start_date, end_date, output_root)


if __name__ == "__main__":
    cli()
