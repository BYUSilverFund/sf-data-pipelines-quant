from datetime import date

import click

from pipelines.barra_signals_flow import barra_signals_flow
from pipelines.ten_k_signals_flow import ten_k_signals_flow
from pipelines.utils.tables import Database


def all_signals_flow(start_date: date, end_date: date, database: Database) -> None:
    click.echo("Running barra-signals flow...")
    barra_signals_flow(start_date, end_date, database)
    click.echo("Running ten-k-signals flow...")
    ten_k_signals_flow(start_date, end_date, database)
    click.echo("Finished all signals backfill.")
