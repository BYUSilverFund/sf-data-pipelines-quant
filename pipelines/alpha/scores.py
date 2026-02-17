import polars as pl
from pipelines.utils.tables import Database
import datetime as dt
from sf_quant.data._tables import signals_table
from pipelines.utils.enums import DatabaseName



def load_signals_data(
    start: dt.date, end: dt.date, columns: list[str] | None = None
) -> pl.DataFrame:
    """
    Loads signal data for assets in our universe on a specific date.
    """

    if columns is None:
        columns = [
            "date", 
            "barrid", 
            "ticker", 
            "signal_name", 
            "signal_value", 
            "specific_risk",
            "in_universe"
        ]

    return (
        signals_table.scan()
        .filter(
            pl.col("date").is_between(start, end),
            pl.col('in_universe')
        )
        .sort(["barrid", "date"])
        .select(columns)
        .collect()
    )

# def compute_score(df: pl.DataFrame) -> pl.DataFrame:
#     return (
#         df
#         .with_columns(
#            (
#             (pl.col("signal_value") - pl.col("signal_value").mean(),over("date")) /
#             pl.col("signal_value").std().over("date")
#            ).alias("signal_score")
#         )
#     )

def compute_scores(df: pl.DataFrame) -> pl.DataFrame:
    """"""
    return (
        df.with_columns(
            pl.col("signal_value")
            .pipe(lambda x: (x - x.mean().over(["date", "signal_name"])) / 
                            x.std().over(["date", "signal_name"]))
            .alias("signal_score")
        )
    )


# def compute_scores(df: pl.DataFrame, signals: list) -> pl.DataFrame:
#     """"""
#     return df.with_columns([
#         ((pl.col(s.name) - pl.col(s.name).mean().over("date")) /
#         pl.col(s.name).std().over("date")).alias(f"{s.name}_score")
#         for s in signals
#     ])



def backfill_scores_flow(
    start_date: dt.date, 
    end_date: dt.date, 
    database: Database
) -> None:
    """"""

    for year in range(start_date.year, end_date.year + 1):
        # load signals data for the year
        yearly_signals = load_signals_data(
            start=dt.date(year, 1, 1),
            end=dt.date(year, 12, 31)
        )
        print(f"\nSignals data for {year}:\n{yearly_signals}")

        # score the signals over date and signal_name
        scored_signals = compute_score(yearly_signals)
        print(f"\nScored signals for {year}:\n{scored_signals}")

        # upsert into database
        database.scores_table.create_if_not_exists(year)
        database.scores_table.upsert(year, rows=scored_signals)


# if __name__ == "__main__":
#     database_name = DatabaseName("production")
#     database_instance = Database(database_name)

#     backfill_scores_flow(
#         start_date=dt.date(2023, 1, 1),
#         end_date=dt.date(2025, 12, 31),
#         database=database_instance
#     )