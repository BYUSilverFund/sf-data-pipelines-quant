import datetime as dt
from pipelines.utils.tables import Database
from pipelines.utils.enums import DatabaseName
import polars as pl
from sf_quant.data._tables import scores_table
# from config import Config


def load_scores_data(
    start: dt.date, end: dt.date, columns: list[str] | None = None
) -> pl.DataFrame:
    """"""

    if columns is None:
        columns = [
            "date",
            "barrid",
            "ticker",
            "signal_name",
            "signal_score",
            "specific_risk"
        ]

    return (
        scores_table.scan()
        .filter(pl.col("date").is_between(start, end))
        .sort(["barrid", "date"])
        .select(columns)
        .collect()
    )


def compute_alpha(df: pl.DataFrame, ic: pl.Float64) -> pl.DataFrame:
    """"""
    return (
        df
        .with_columns(
            pl.col("signal_score")
            .mul(ic)
            .mul(pl.col("specific_risk"))
            .fill_null(0)
            .alias("alpha_value")
        ).select([
            "date", "barrid", "ticker", "signal_name", "alpha_value"
        ])
    )




def backfill_alpha_flow(
    start_date: dt.date,
    end_date: dt.date,
    database: Database
) -> None:
    """"""
    # ic = _config.ic
    ic = 0.5  # placeholder until config is integrated

    for year in range(start_date.year, end_date.year + 1):
        # get the scored signal data
        yearly_score_data = load_scores_data(
            start=dt.date(year, 1, 1),
            end=dt.date(year, 12, 31)
        )

        # compute the alphas
        yearly_alphas = compute_alpha(yearly_score_data, ic)

        # upsert into database
        database.alphas_table.create_if_not_exists(year)
        database.alphas_table.upsert(year, rows=yearly_alphas)


# if __name__ == "__main__":
#     database_name = DatabaseName("production")
#     database_instance = Database(database_name)

#     backfill_alpha_flow(
#         start_date=dt.date(2023, 1, 1),
#         end_date=dt.date(2025, 12, 31),
#         database=database_instance
#     )


    