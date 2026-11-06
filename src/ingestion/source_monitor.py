from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urlparse

import requests

from src.ingestion.bls_client import discover_file_urls
from src.ingestion.config import BLS_DIRECTORY_URL, POPULATION_API_URL, IngestionConfig


@dataclass(frozen=True)
class SourceFile:
    source_name: str
    source_file: str
    source_url: str


@dataclass(frozen=True)
class SourceMetadata:
    source: SourceFile
    http_status: int | None
    etag: str | None
    last_modified: str | None
    content_length: str | None


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


def _head_metadata(
    session: requests.Session,
    source: SourceFile,
    config: IngestionConfig,
) -> SourceMetadata:
    try:
        response = session.head(
            source.source_url,
            allow_redirects=True,
            timeout=config.timeout,
        )
    except requests.RequestException:
        return SourceMetadata(source, None, None, None, None)

    try:
        if response.status_code < 200 or response.status_code >= 300:
            return SourceMetadata(
                source, response.status_code, None, None, None
            )
        return SourceMetadata(
            source,
            response.status_code,
            response.headers.get("ETag"),
            response.headers.get("Last-Modified"),
            response.headers.get("Content-Length"),
        )
    finally:
        response.close()


def inspect_sources(
    session: requests.Session,
    sources: list[SourceFile],
    config: IngestionConfig,
) -> list[SourceMetadata]:
    return [_head_metadata(session, source, config) for source in sources]


def unchanged_from_previous_snapshot(
    current: list[SourceMetadata],
    previous_manifest: dict[str, Any] | None,
) -> bool:
    if not previous_manifest:
        return False

    previous_files = previous_manifest.get("files")
    if not isinstance(previous_files, list):
        return False
    previous_by_key = {
        (item.get("source_name"), item.get("source_file")): item
        for item in previous_files
        if isinstance(item, dict)
    }
    if set(previous_by_key) != {
        (item.source.source_name, item.source.source_file) for item in current
    }:
        return False

    for metadata in current:
        previous = previous_by_key[
            (metadata.source.source_name, metadata.source.source_file)
        ]
        if metadata.http_status is None or metadata.http_status >= 300:
            return False

        current_values = {
            "etag": metadata.etag,
            "last_modified": metadata.last_modified,
            "content_length": metadata.content_length,
        }
        previous_values = {
            "etag": previous.get("etag"),
            "last_modified": previous.get("last_modified"),
            "content_length": previous.get("content_length"),
        }
        if any(value is None for value in current_values.values()):
            return False
        if current_values != previous_values:
            return False

    return True
