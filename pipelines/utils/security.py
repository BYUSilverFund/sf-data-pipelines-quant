import os
import click


class ProductionBackfillBlocked(click.ClickException):
    pass


def guard_production_backfill(database: str, pipeline_type: str) -> None:
    """
    Prevent destructive operations against the production PIT database
    unless an explicit break-glass procedure is followed.
    """

    if pipeline_type.lower() != "backfill":
        return

    if database.lower() != "production":
        return

    phrase = click.prompt(
        "WARNING: You are trying to backfill 'production.' \n"
        "This database is a point-in-time system and cannot be mutated.\n\n"
        "If you are authorized, type the confirmation phrase to proceede:\n",
        default="",
        show_default=False
    )

    if phrase != "BACKFILL_PRODUCTION_I_UNDERSTAND":
        raise ProductionBackfillBlocked("Confirmation phrase mismatch. Aborting.")
