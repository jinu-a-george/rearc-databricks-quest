from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BLS_DIRECTORY_URL = "https://download.bls.gov/pub/time.series/pr/"
POPULATION_API_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)


@dataclass(frozen=True)
class IngestionConfig:
    raw_volume_path: Path
    bls_contact_email: str
    connect_timeout_seconds: int = 10
    read_timeout_seconds: int = 120
    polling_interval_seconds: int = 604800

    @classmethod
    def from_environment(
        cls,
        bls_contact_email: str | None = None,
        polling_interval_seconds: int | None = None,
        raw_volume_path: str | None = None,
    ) -> IngestionConfig:
        contact_email = (
            bls_contact_email or os.environ.get("BLS_CONTACT_EMAIL", "")
        ).strip()
        if not contact_email or "@" not in contact_email:
            raise ValueError(
                "Set BLS_CONTACT_EMAIL to a valid contact address for BLS requests."
            )

        volume_path = (
            raw_volume_path
            or os.environ.get(
                "RAW_VOLUME_PATH",
                "/Volumes/rearc_catalog/data_quest_bronze/raw_data",
            )
        ).strip()
        if not volume_path:
            raise ValueError("RAW_VOLUME_PATH must not be empty.")

        interval = polling_interval_seconds
        if interval is None:
            interval = int(os.environ.get("POLLING_INTERVAL_SECONDS", "604800"))
        if interval <= 0:
            raise ValueError("POLLING_INTERVAL_SECONDS must be a positive integer.")

        return cls(
            raw_volume_path=Path(volume_path),
            bls_contact_email=contact_email,
            polling_interval_seconds=interval,
        )

    @property
    def user_agent(self) -> str:
        return f"rearc-databricks-quest/0.1 (contact: {self.bls_contact_email})"

    @property
    def timeout(self) -> tuple[int, int]:
        return self.connect_timeout_seconds, self.read_timeout_seconds
