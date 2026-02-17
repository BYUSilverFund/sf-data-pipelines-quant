import polars as pl
from pipelines.alpha.scores import backfill_scores_flow
from pipelines.alpha.alpha import backfill_alpha_flow



def run_alpha_backfill_flow(
    start_date: pl.Date, 
    end_date: pl.Date, 
    database: pl.Database
) -> None:
    """Executes the alpha backfill flow across a date range."""
    
    # backfill scores first
    backfill_scores_flow(
        start_date=start_date,
        end_date=end_date,
        database=database
    )

    # backfill alpha next
    backfill_alpha_flow(
        start_date=start_date,
        end_date=end_date,
        database=database
    )

    # backfill combined alpha last
    backfill_combined_alpha_flow(
        start_date=start_date,
        end_date=end_date,
        database=database
    )
    
    # Further steps to compute and backfill alpha would go here
    # (e.g., loading scores, computing alpha values, and upserting them into the database)