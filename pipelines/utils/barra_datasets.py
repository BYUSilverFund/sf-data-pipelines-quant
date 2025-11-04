from dotenv import load_dotenv
from datetime import date
import os
from pathlib import Path


class BarraDataset:
    def __init__(
        self,
        history_folder: str | None,
        daily_folder: str,
        history_zip_file: str | None,
        daily_zip_file: str,
        file_name: str,
    ) -> None:
        load_dotenv(override=True)

        home, user = os.getenv("ROOT").split("/")[1:3]
        self._base_path = Path(f"/{home}/{user}/groups/grp_msci_barra/nobackup/archive")

        self._history_folder = history_folder
        self._daily_folder = daily_folder
        self._history_zip_file = history_zip_file
        self._daily_zip_file = daily_zip_file
        self._file_name = file_name

    def history_zip_folder(self) -> Path:
        return self._base_path / self._history_folder

    def history_zip_file(self, year: int) -> str:
        return f"{self._history_zip_file}_{year}"

    def history_zip_folder_path(self, year: int) -> Path:
        return (
            self._base_path
            / self._history_folder
            / f"{self._history_zip_file}_{year}.zip"
        )

    def file_name(self, date_: date | None = None) -> str:
        if date_:
            return f"{self._file_name}.{date_.strftime('%Y%m%d')}"
        else:
            return self._file_name

    def daily_zip_folder_path(self, date_: date) -> Path:
        return (
            self._base_path
            / self._daily_folder
            / f"{self._daily_zip_file}_{date_.strftime('%y%m%d')}.zip"
        )


barra_returns = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOW_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOW_Daily_Asset_Price",
)

barra_specific_returns = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOW_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOW_100_Asset_DlySpecRet",
)

barra_risk = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOWL_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOWL_100_Asset_Data",
)

barra_volume = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOW_100_D",
    daily_folder="bime",
    daily_zip_file="SMD_USSLOW_Market_Data",
    file_name="USSLOW_Market_Data",
)

barra_assets = BarraDataset(
    history_folder=None,
    history_zip_file=None,
    daily_folder="bime",
    daily_zip_file="SMD_USSLOW_XSEDOL_ID",
    file_name="USA_Asset_Identity",
)

barra_ids = BarraDataset(
    history_folder=None,
    history_zip_file=None,
    daily_folder="bime",
    daily_zip_file="SMD_USSLOW_XSEDOL_ID",
    file_name="USA_XSEDOL_Asset_ID",
)

barra_covariances = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOWL_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOWL_100_Covariance",
)

barra_exposures = BarraDataset(
    history_folder="history/usslow/sm/daily",
    history_zip_file="SMD_USSLOWL_100_D",
    daily_folder="us/usslow",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOWL_100_Asset_Exposure",
)


barra_factors = BarraDataset(
    history_folder=None,
    history_zip_file=None,
    daily_folder="bime",
    daily_zip_file="SMD_USSLOWL_100",
    file_name="USSLOWL_100_DlyFacRet",
)
