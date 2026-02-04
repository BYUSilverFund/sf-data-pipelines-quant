import click
import datetime as dt
from pipelines.ingestion.all_pipelines import (
    barra_backfill_pipeline,
    ftse_backfill_pipeline,
    crsp_backfill_pipeline,
    crsp_v2_backfill_pipeline,
    barra_daily_pipeline,
)
from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database

from pipelines.utils.security import guard_production_backfill

from dotenv import load_dotenv
import os

from pipelines.signals.base import BaseSignal
import pipelines.signals
from pipelines.signals.pipeline import run_single_backfill

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
    guard_production_backfill(database, pipeline_type)
    
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
    guard_production_backfill(database, pipeline_type)
    
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
    guard_production_backfill(database, pipeline_type)
    
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
    guard_production_backfill(database, pipeline_type)
    
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
@click.argument(
    "pipeline_type",
    type=click.Choice(
        ["backfill"], case_sensitive=False
    ),  # Update is currently not supported
)
@click.option(
    "--signal",
    "signal_to_run",
    default="all",
    help="Signal name (e.g., 'momentum') or all"
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
def signals(pipeline_type, signal_to_run, database, start, end):
    """"""
    match pipeline_type:
            case "backfill":
                start = start.date() if hasattr(start, "date") else start
                end = end.date() if hasattr(end, "date") else end

                click.echo(f"Running signal creation backfill on '{database}' from {start} to {end}.")

                database_name = DatabaseName(database)
                database_instance = Database(database_name)

                available_signals = {cls().name: cls() for cls in BaseSignal.__subclasses__()}

                if signal_to_run == "all":
                    targets = list(available_signals.values())
                else:
                    if signal_to_run not in available_signals:
                        raise ValueError(f"Signal '{signal_to_run}' not found. Available signals: {list(available_signals.keys())}")
                    targets = [available_signals[signal_to_run]]

                for signal_inst in targets:
                    click.echo(f"\nStarting backfill for signal: {signal_inst.name}...\n")

                    try:
                        run_single_backfill(
                            signal_inst, 
                            start, 
                            end, 
                            database_instance
                        )
                        click.echo(f"Completed backfill for signal: {signal_inst.name}.\n")
                    except Exception as e:
                        click.echo(f"Error processing signal '{signal_inst.name}': {e}")


# python pipelines signals backfill --database production

if __name__ == "__main__":
    cli()
