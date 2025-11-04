import datetime as dt
import pipelines.utils.s3
import polars as pl
import os
import zipfile
import io
from dotenv import load_dotenv
import numpy as np
from pipelines.utils import get_last_market_date
from pipelines.utils.factors import factors

load_dotenv(override=True)

ROOT = os.getenv("ROOT", "/home/amh1124/groups/grp_msci_barra/nobackup/archive")

exposures_column_mapping = {
    "!Barrid": "barrid",
    "Factor": "factor",
    "Exposure": "exposure",
    "DataDate": "date",
}

covariances_column_mapping = {
    "!Factor1": "factor_1",
    "Factor2": "factor_2",
    "VarCovar": "covariance",
    "DataDate": "date",
}

specific_risk_column_mapping = {
    "!Barrid": "barrid",
    "Yield%": "yield",
    "TotalRisk%": "total_risk",
    "SpecRisk%": "specific_risk",
    "HistBeta": "historical_beta",
    "PredBeta": "predicted_beta",
    "DataDate": "date",
}

cusips_column_mapping = {
    "!Barrid": "barrid",
    "AssetIDType": "asset_id_type",
    "AssetID": "asset_id",
    "StartDate": "start_date",
    "EndDate": "end_date",
}

root_ids_column_mapping = {
    "!Barrid": "barrid",
    "Name": "name",
    "Instrument": "instrument",
    "IssuerID": "issuer_id",
    "ISOCountryCode": "iso_country_code",
    "ISOCurrencyCode": "iso_currency_code",
    "RootID": "root_id",
    "StartDate": "start_date",
    "EndDate": "end_date",
}


def clean_exposures(df: pl.DataFrame, barrids: list[str]) -> pl.DataFrame:
    return (
        df.rename(exposures_column_mapping)
        .filter(pl.col("barrid").ne("[End of File]"), pl.col("barrid").is_in(barrids))
        .with_columns(pl.col("date").cast(pl.String).str.strptime(pl.Date, "%Y%m%d"))
        .pivot(index=["date", "barrid"], on="factor", values="exposure")
        .sort("barrid")
        .select("date", "barrid", *factors)
    )


def clean_covariances(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(covariances_column_mapping)
        .filter(pl.col("factor_1").ne("[End of File]"))
        .with_columns(
            pl.col("date").cast(pl.String).str.strptime(pl.Date, "%Y%m%d"),
            pl.col("covariance").str.strip_chars().cast(pl.Float64),
        )
        .pivot(index=["date", "factor_1"], on="factor_2", values="covariance")
        .sort("factor_1")
        .select("date", "factor_1", *factors)
    )


def clean_specific_risk(df: pl.DataFrame, barrids: list[str]) -> pl.DataFrame:
    return (
        df.rename(specific_risk_column_mapping)
        .filter(pl.col("barrid").ne("[End of File]"), pl.col("barrid").is_in(barrids))
        .with_columns(
            pl.col("date").cast(pl.String).str.strptime(pl.Date, "%Y%m%d"),
        )
        .select("barrid", "specific_risk")
        .sort("barrid")
    )


def clean_tickers(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(cusips_column_mapping)
        .filter(pl.col("asset_id_type").eq("LOCALID"))
        .rename({"asset_id": "ticker"})
        .with_columns(pl.col("ticker").str.replace("US", ""))
        .sort("barrid")
        .select("barrid", "ticker")
    )


def clean_root_ids(df: pl.DataFrame, date_: dt.date) -> pl.DataFrame:
    return (
        df.rename(root_ids_column_mapping)
        .with_columns(
            pl.col("start_date", "end_date")
            .cast(pl.String)
            .str.strptime(pl.Date, "%Y%m%d")
        )
        .filter(
            pl.col("barrid").ne("[End of File]"),
            pl.col("instrument").is_in(["STOCK", "ETF", "ADR"]),
            pl.col("start_date").le(date_),
            pl.col("end_date").ge(date_),
            pl.col("iso_country_code").eq("USA"),
        )
        .select("barrid")
    )


def get_stock_exposures(date_: dt.date, barrids: list[str]) -> pl.DataFrame:
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOWL_100_{date_str_1}.zip"
    file_name = f"USSLOWL_100_Asset_Exposure.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )

        return clean_exposures(df, barrids)


def get_etf_exposures(date_: dt.date, barrids: list[str]) -> pl.DataFrame:
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOWL_100_ETF_{date_str_1}.zip"
    file_name = f"USSLOWL_ETF_100_Asset_Exposure.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )

        return clean_exposures(df, barrids)


def get_factor_covariances(date_: dt.date):
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOWL_100_{date_str_1}.zip"
    file_name = f"USSLOWL_100_Covariance.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )

        return clean_covariances(df)


def get_stock_specific_risk(date_: dt.date, barrids: list[str]) -> pl.DataFrame:
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOWL_100_{date_str_1}.zip"
    file_name = f"USSLOWL_100_Asset_Data.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )

        return clean_specific_risk(df, barrids)


def get_etf_specific_risk(date_: dt.date, barrids: list[str]) -> pl.DataFrame:
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOWL_100_ETF_{date_str_1}.zip"
    file_name = f"USSLOWL_ETF_100_Asset_Data.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )

        return clean_specific_risk(df, barrids)


def construct_covariance_matrix(
    exposures: pl.DataFrame, covariances: pl.DataFrame, specific_risk: pl.DataFrame
) -> pl.DataFrame:
    barrids = exposures["barrid"].to_list()

    exposures = (
        exposures.drop("date", "barrid")
        .with_columns(
            pl.all().fill_null(0)  # fill null factor exposures
        )
        .to_numpy()
    )

    covariances = (
        covariances.drop("date", "factor_1")
        .with_columns(pl.all().truediv(100**2))
        .to_numpy()
    )

    mask = np.isnan(covariances)
    covariances[mask] = covariances.T[mask]  # fill symetric values


    specific_risk = (
        specific_risk.drop("barrid")
        .with_columns(pl.all().truediv(100))
        .to_numpy()
        .flatten()
    )

    # Square the specific risk to get specific variance
    specific_variance = np.diag(specific_risk ** 2)

    covariance_matrix = exposures @ covariances @ exposures.T + specific_variance

    covariance_matrix_df = (
        pl.from_numpy(covariance_matrix, barrids)
        .with_columns(pl.Series(barrids).alias("barrid"))
        .select("barrid", *barrids)
    )

    return covariance_matrix_df


def get_tickers(date_: dt.date):
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOW_XSEDOL_ID_{date_str_1}.zip"
    file_name = f"USA_XSEDOL_Asset_ID.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=1,
            separator="|",
        )

        return clean_tickers(df)


def get_barrids(date_: dt.date):
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOW_XSEDOL_ID_{date_str_1}.zip"
    file_name = f"USA_Asset_Identity.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=1,
            separator="|",
        )

        return clean_root_ids(df, date_)


def covariance_matrix_daily_flow() -> None:
    date_ = get_last_market_date()[0]

    # 1. Get barrids and tickers
    tickers_df = get_tickers(date_)

    barrids_df = (
        get_barrids(date_)
        .join(tickers_df, on="barrid", how="left")
        .filter(pl.col("ticker").is_not_null())
        .select("barrid", "ticker")
        .sort("barrid")
    )

    barrids = barrids_df["barrid"].to_list()
    tickers = barrids_df["ticker"].to_list()
    ticker_mapping = {barrid: ticker for barrid, ticker in zip(barrids, tickers)}

    # 2. Get exposures
    stock_exposures = get_stock_exposures(date_, barrids)
    etf_exposures = get_etf_exposures(date_, barrids)

    exposures: pl.DataFrame = pl.concat([stock_exposures, etf_exposures])

    # 3. Get covariances
    covariances = get_factor_covariances(date_)

    # 4. Get specific risk
    stock_specific_risk = get_stock_specific_risk(date_, barrids)
    etf_specific_risk = get_etf_specific_risk(date_, barrids)
    specific_risk: pl.DataFrame = pl.concat([stock_specific_risk, etf_specific_risk])

    # 5. Construct covariance matrix
    covariance_matrix = construct_covariance_matrix(
        exposures, covariances, specific_risk
    )

    # 6. Re-key covariance matrix
    covariance_matrix_re_keyed = (
        covariance_matrix.rename(ticker_mapping, strict=False)
        .with_columns(pl.col("barrid").replace(ticker_mapping))
        .rename({"barrid": "ticker"})
    )

    tickers = covariance_matrix_re_keyed["ticker"].unique().sort().to_list()

    covariance_matrix_clean = covariance_matrix_re_keyed.select(
        pl.lit(date_).alias("date"), "ticker", *sorted(tickers)
    ).sort("ticker")

    # 7. Upload to s3
    pipelines.utils.s3.write_parquet(
        bucket_name="barra-covariance-matrices",
        file_name="latest.parquet",
        file_data=covariance_matrix_clean,
    )
