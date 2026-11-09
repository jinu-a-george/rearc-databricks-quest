from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import unquote, urlparse

import requests

from src.ingestion.bls_client import discover_file_urls
from src.ingestion.config import BLS_DIRECTORY_URL, POPULATION_API_URL, IngestionConfig


@dataclass(frozen=True)
class SourceFile:
    source_name: str
    source_file: str
    source_url: str


def discover_sources(
    session: requests.Session, config: IngestionConfig
) -> list[SourceFile]:
    listing = session.get(BLS_DIRECTORY_URL, timeout=config.timeout)
    try:
        listing.raise_for_status()
        urls = discover_file_urls(listing.text)
    finally:
        listing.close()

    if not urls:
        raise RuntimeError("No BLS source files were discovered.")

    sources = [
        SourceFile(
            source_name="BLS",
            source_file=unquote(PurePosixPath(urlparse(url).path).name),
            source_url=url,
        )
        for url in urls
    ]
    sources.append(
        SourceFile(
            source_name="DataUSA",
            source_file="population.json",
            source_url=POPULATION_API_URL,
        )
    )
    return sources
