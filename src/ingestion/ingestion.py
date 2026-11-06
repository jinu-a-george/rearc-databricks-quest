from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.ingestion.config import IngestionConfig, BLS_DIRECTORY_URL, POPULATION_API_URL
from src.ingestion.manifest import (
    commit_successful_snapshot,
    load_latest_manifest,
    new_snapshot_id,
    read_json,
    utc_timestamp,
    write_failed_manifest,
    write_json_atomic,
)
from src.ingestion.population_client import validate_population_payload
from src.ingestion.source_monitor import (
    SourceFile,
    SourceMetadata,
    discover_sources,
    inspect_sources,
    unchanged_from_previous_snapshot,
)


LOGGER = logging.getLogger(__name__)
APPLICATION_VERSION = "0.2.0"
HTTP_RETRY = Retry(
    total=4,
    connect=4,
    read=4,
    status=4,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET", "HEAD"}),
    respect_retry_after_header=True,
)
EXPECTED_BLS_HEADERS = {
    "pr.class": ["class_code", "class_text", "display_level", "selectable", "sort_sequence"],
    "pr.data.0.Current": ["series_id", "year", "period", "value", "footnote_codes"],
    "pr.data.1.AllData": ["series_id", "year", "period", "value", "footnote_codes"],
    "pr.duration": ["duration_code", "duration_text", "display_level", "selectable", "sort_sequence"],
    "pr.footnote": ["footnote_code", "footnote_text"],
    "pr.measure": ["measure_code", "measure_text", "display_level", "selectable", "sort_sequence"],
    "pr.period": ["period", "period_abbr", "period_name"],
    "pr.seasonal": ["Seasonal_code", "Seasonal_text"],
    "pr.sector": ["sector_code", "sector_name", "display_level", "selectable", "sort_sequence"],
    "pr.series": [
        "series_id", "sector_code", "class_code", "measure_code",
        "duration_code", "seasonal", "base_year", "footnote_codes",
        "series_title", "begin_year", "begin_period", "end_year", "end_period",
    ],
}


class SnapshotIngestionFailure(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        snapshot_id: str,
        audit_rows: list[dict[str, Any]],
    ) -> None:
        super().__init__(message)
        self.snapshot_id = snapshot_id
        self.audit_rows = audit_rows


def _read_previous_processed_snapshot(raw_volume_path: Path) -> str | None:
    path = raw_volume_path / "processing" / "latest.json"
    if not path.exists():
        return None
    pointer = read_json(path)
    if (
        pointer.get("status") != "SUCCESS"
        or pointer.get("audit_finalized") is not True
    ):
        return None
    snapshot_id = pointer.get("snapshot_id")
    return snapshot_id if isinstance(snapshot_id, str) else None


def _source_path(
    raw_volume_path: Path, source: SourceFile, snapshot_id: str
) -> Path:
    if source.source_name == "BLS":
        return raw_volume_path / "bls" / "snapshots" / snapshot_id / source.source_file
    return (
        raw_volume_path
        / "population"
        / "snapshots"
        / snapshot_id
        / "population.json"
    )


def _stage_path(staging_path: Path, source: SourceFile) -> Path:
    if source.source_name == "BLS":
        return staging_path / "bls" / source.source_file
    return staging_path / "population" / "population.json"


def _is_html(content_prefix: bytes) -> bool:
    normalized = content_prefix.lstrip()[:256].lower()
    return normalized.startswith((b"<!doctype html", b"<html", b"<head"))


def _validate_bls_file(path: Path, filename: str) -> tuple[int, list[str] | None, int]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source, delimiter="\t", strict=True)
        header = next(reader, None)
        if header is None or not any(field.strip() for field in header):
            raise ValueError(f"BLS file has no non-empty header: {filename}")
        normalized_header = [field.strip() for field in header]
        if any(not field for field in normalized_header):
            raise ValueError(f"BLS file has an empty column name: {filename}")
        if len(set(normalized_header)) != len(normalized_header):
            raise ValueError(f"BLS file has duplicate column names: {filename}")

        expected = EXPECTED_BLS_HEADERS.get(filename)
        if expected is not None and normalized_header != expected:
            raise ValueError(
                f"Unexpected header in {filename}: {normalized_header!r}"
            )

        record_count = 0
        malformed_count = 0
        for row in reader:
            if not row:
                continue
            record_count += 1
            if len(row) != len(normalized_header):
                malformed_count += 1
        return record_count, normalized_header, malformed_count


def _validate_download(path: Path, source: SourceFile) -> tuple[int, list[str] | None, int]:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"Downloaded source is empty: {source.source_file}")

    if source.source_name == "DataUSA":
        with path.open("r", encoding="utf-8") as raw_file:
            payload = json.load(raw_file)
        validated = validate_population_payload(payload)
        return len(validated["data"]), None, 0

    with path.open("rb") as raw_file:
        prefix = raw_file.read(512)
    if _is_html(prefix):
        raise ValueError(
            f"BLS returned HTML instead of a source file: {source.source_file}"
        )

    with path.open("r", encoding="utf-8-sig", errors="strict") as raw_file:
        first_line = raw_file.readline()
    if "\t" not in first_line:
        with path.open("r", encoding="utf-8-sig", errors="strict") as raw_file:
            line_count = sum(1 for line in raw_file if line.strip())
        return line_count, None, 0
    return _validate_bls_file(path, source.source_file)


def _download_source(
    session: requests.Session,
    source: SourceFile,
    destination: Path,
    metadata: SourceMetadata,
    config: IngestionConfig,
    snapshot_id: str,
) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination.with_name(f"{destination.name}.part")
    started_at = utc_timestamp()
    digest = hashlib.sha256()
    byte_count = 0
    prefix = bytearray()
    response = session.get(source.source_url, stream=True, timeout=config.timeout)
    try:
        response.raise_for_status()
        with temporary_path.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                if len(prefix) < 512:
                    prefix.extend(chunk[: 512 - len(prefix)])
                output.write(chunk)
                digest.update(chunk)
                byte_count += len(chunk)
        if byte_count == 0:
            raise ValueError(f"Downloaded source is empty: {source.source_file}")
        if source.source_name == "BLS" and _is_html(bytes(prefix)):
            raise ValueError(
                f"BLS returned HTML instead of a source file: {source.source_file}"
            )
        os.replace(temporary_path, destination)
    finally:
        response.close()
        temporary_path.unlink(missing_ok=True)

    record_count, headers, malformed_count = _validate_download(destination, source)
    return {
        "snapshot_id": snapshot_id,
        "source_name": source.source_name,
        "source_file": source.source_file,
        "source_url": source.source_url,
        "snapshot_path": destination.as_posix(),
        "download_started_at": started_at,
        "downloaded_at": utc_timestamp(),
        "file_size_bytes": byte_count,
        "checksum_sha256": digest.hexdigest(),
        "http_status": response.status_code,
        "etag": response.headers.get("ETag") or metadata.etag,
        "last_modified": response.headers.get("Last-Modified")
        or metadata.last_modified,
        "content_length": response.headers.get("Content-Length")
        or metadata.content_length,
        "record_count": record_count,
        "malformed_record_count": malformed_count,
        "headers": headers,
        "ingestion_status": "SUCCESS",
        "error_message": None,
    }


def _manifest_file_record(
    snapshot_id: str,
    source: SourceFile,
    metadata: SourceMetadata | None,
    status: str,
    error_message: str | None = None,
    downloaded_path: Path | None = None,
) -> dict[str, Any]:
    file_size_bytes = None
    checksum_sha256 = None
    if downloaded_path is not None and downloaded_path.is_file():
        file_size_bytes = downloaded_path.stat().st_size
        digest = hashlib.sha256()
        with downloaded_path.open("rb") as downloaded_file:
            for chunk in iter(lambda: downloaded_file.read(1024 * 1024), b""):
                digest.update(chunk)
        checksum_sha256 = digest.hexdigest()
    return {
        "snapshot_id": snapshot_id,
        "source_name": source.source_name,
        "source_file": source.source_file,
        "source_url": source.source_url,
        "download_started_at": None,
        "downloaded_at": None,
        "file_size_bytes": file_size_bytes,
        "checksum_sha256": checksum_sha256,
        "http_status": metadata.http_status if metadata else None,
        "etag": metadata.etag if metadata else None,
        "last_modified": metadata.last_modified if metadata else None,
        "content_length": metadata.content_length if metadata else None,
        "record_count": None,
        "malformed_record_count": None,
        "headers": None,
        "snapshot_path": None,
        "ingestion_status": status,
        "error_message": error_message,
    }


def _as_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _audit_rows(
    snapshot_id: str,
    files: list[dict[str, Any]],
    job_id: int | None,
    job_run_id: int | None,
    snapshot_status: str,
    raw_volume_path: Path,
) -> list[dict[str, Any]]:
    return [
        {
            "snapshot_id": snapshot_id,
            "source_snapshot_id": _source_snapshot_id(item),
            "databricks_job_id": job_id,
            "databricks_job_run_id": job_run_id,
            "source_name": item["source_name"],
            "source_file": item["source_file"],
            "source_url": item.get("source_url"),
            "source_path": _source_volume_path(item, raw_volume_path),
            "checksum_sha256": item.get("checksum_sha256"),
            "file_size_bytes": item.get("file_size_bytes"),
            "http_status": item.get("http_status"),
            "etag": item.get("etag"),
            "last_modified": item.get("last_modified"),
            "content_length": item.get("content_length"),
            "download_started_at": _as_datetime(item.get("download_started_at")),
            "download_completed_at": _as_datetime(item.get("downloaded_at")),
            "records_downloaded": item.get("record_count"),
            "records_ingested": None,
            "snapshot_status": snapshot_status,
            "status": item["ingestion_status"],
            "error_message": item.get("error_message"),
        }
        for item in files
    ]


def _source_volume_path(item: dict[str, Any], raw_volume_path: Path) -> str | None:
    snapshot_path = item.get("snapshot_path")
    if not isinstance(snapshot_path, str) or not snapshot_path:
        return None
    if Path(snapshot_path).is_absolute():
        return snapshot_path
    return (raw_volume_path / snapshot_path).as_posix()


def _source_snapshot_id(item: dict[str, Any]) -> str | None:
    snapshot_path = item.get("snapshot_path")
    if not isinstance(snapshot_path, str):
        return None
    for path_part in snapshot_path.replace("\\", "/").split("/"):
        if len(path_part) == 20 and path_part.isdigit():
            return path_part
    return item.get("snapshot_id")


def _latest_snapshot_id(manifest: dict[str, Any] | None) -> str | None:
    if manifest is None:
        return None
    snapshot_id = manifest.get("snapshot_id")
    return snapshot_id if isinstance(snapshot_id, str) else None


def _processed_snapshot_id(raw_volume_path: Path) -> str | None:
    try:
        return _read_previous_processed_snapshot(raw_volume_path)
    except (OSError, ValueError, RuntimeError):
        LOGGER.warning("Could not read the processed-snapshot pointer.")
        return None


def _same_checksums(
    current: list[dict[str, Any]],
    previous_manifest: dict[str, Any] | None,
) -> bool:
    if not previous_manifest:
        return False
    old_files = {
        (item.get("source_name"), item.get("source_file")): item
        for item in previous_manifest.get("files", [])
        if isinstance(item, dict)
    }
    if len(old_files) != len(current):
        return False
    for item in current:
        old = old_files.get((item["source_name"], item["source_file"]))
        if not old or old.get("checksum_sha256") != item.get("checksum_sha256"):
            return False
    return True


def ingest_snapshot(
    config: IngestionConfig,
    *,
    databricks_job_id: int | None = None,
    databricks_job_run_id: int | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    snapshot_id = new_snapshot_id()
    while (config.raw_volume_path / "manifests" / f"{snapshot_id}.json").exists():
        snapshot_id = new_snapshot_id()

    started_at = utc_timestamp()
    staging_root = config.raw_volume_path / ".staging" / snapshot_id
    sources: list[SourceFile] = []
    metadata_by_key: dict[tuple[str, str], SourceMetadata] = {}
    file_records: list[dict[str, Any]] = []
    promoted_paths: list[Path] = []
    own_session = session is None
    http = session or requests.Session()
    if own_session:
        adapter = HTTPAdapter(max_retries=HTTP_RETRY)
        http.mount("http://", adapter)
        http.mount("https://", adapter)
    http.headers.update(
        {
            "User-Agent": config.user_agent,
            "Accept": "text/html, text/plain, application/json, */*",
        }
    )
    previous_manifest = load_latest_manifest(config.raw_volume_path)
    latest_snapshot_id = _latest_snapshot_id(previous_manifest)
    previous_files = {
        (item.get("source_name"), item.get("source_file")): item
        for item in (previous_manifest or {}).get("files", [])
        if isinstance(item, dict)
    }
    manifest: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "started_at": started_at,
        "completed_at": None,
        "ingestion_status": "FAILED",
        "source_names": ["BLS", "DataUSA"],
        "files": file_records,
        "error_details": None,
        "application_version": APPLICATION_VERSION,
        "databricks_job_id": databricks_job_id,
        "databricks_job_run_id": databricks_job_run_id,
    }

    try:
        sources = discover_sources(http, config)
        metadata = inspect_sources(http, sources, config)
        metadata_by_key = {
            (item.source.source_name, item.source.source_file): item
            for item in metadata
        }
        if unchanged_from_previous_snapshot(metadata, previous_manifest):
            for source in sources:
                old = previous_files[(source.source_name, source.source_file)]
                source_metadata = metadata_by_key[
                    (source.source_name, source.source_file)
                ]
                record = {
                    **old,
                    "snapshot_id": snapshot_id,
                    "source_url": source.source_url,
                    "http_status": source_metadata.http_status,
                    "etag": source_metadata.etag,
                    "last_modified": source_metadata.last_modified,
                    "content_length": source_metadata.content_length,
                    "download_started_at": None,
                    "downloaded_at": None,
                    "ingestion_status": "UNCHANGED",
                    "error_message": None,
                }
                file_records.append(record)
            manifest.update(
                {
                    "completed_at": utc_timestamp(),
                    "ingestion_status": "UNCHANGED",
                    "error_details": None,
                }
            )
            write_failed_manifest(config.raw_volume_path, manifest)
            LOGGER.info(
                "All %d source files are unchanged; no new snapshot was created.",
                len(sources),
            )
        else:
            stage_bls = staging_root / "bls"
            stage_population = staging_root / "population"
            stage_bls.mkdir(parents=True, exist_ok=False)
            stage_population.mkdir(parents=True, exist_ok=True)

            for source in sources:
                metadata_item = metadata_by_key[
                    (source.source_name, source.source_file)
                ]
                destination = _stage_path(staging_root, source)
                try:
                    file_records.append(
                        _download_source(
                            http,
                            source,
                            destination,
                            metadata_item,
                            config,
                            snapshot_id,
                        )
                    )
                except (OSError, ValueError, requests.RequestException) as error:
                    failure_record = _manifest_file_record(
                        snapshot_id,
                        source,
                        metadata_item,
                        "FAILED",
                        str(error),
                        _stage_path(staging_root, source),
                    )
                    file_records.append(failure_record)
                    raise

            if _same_checksums(file_records, previous_manifest):
                manifest.update(
                    {
                        "completed_at": utc_timestamp(),
                        "ingestion_status": "UNCHANGED",
                    }
                )
                for item in file_records:
                    item["ingestion_status"] = "UNCHANGED"
                    item["snapshot_path"] = previous_files[
                        (item["source_name"], item["source_file"])
                    ].get("snapshot_path")
                write_failed_manifest(config.raw_volume_path, manifest)
                shutil.rmtree(staging_root)
            else:
                bls_stage = staging_root / "bls"
                population_stage = staging_root / "population"
                bls_snapshot = config.raw_volume_path / "bls" / "snapshots" / snapshot_id
                population_snapshot = (
                    config.raw_volume_path / "population" / "snapshots" / snapshot_id
                )
                bls_snapshot.parent.mkdir(parents=True, exist_ok=True)
                population_snapshot.parent.mkdir(parents=True, exist_ok=True)
                bls_stage.rename(bls_snapshot)
                promoted_paths.append(bls_snapshot)
                population_stage.rename(population_snapshot)
                promoted_paths.append(population_snapshot)

                for item in file_records:
                    source = next(
                        source
                        for source in sources
                        if source.source_name == item["source_name"]
                        and source.source_file == item["source_file"]
                    )
                    item["snapshot_path"] = _source_path(
                        config.raw_volume_path, source, snapshot_id
                    ).relative_to(config.raw_volume_path).as_posix()
                manifest.update(
                    {
                        "completed_at": utc_timestamp(),
                        "ingestion_status": "SUCCESS",
                    }
                )
                commit_successful_snapshot(config.raw_volume_path, manifest)

            LOGGER.info(
                "Ingestion snapshot %s completed with status %s.",
                snapshot_id,
                manifest["ingestion_status"],
            )

        authoritative_snapshot_id = (
            snapshot_id
            if manifest["ingestion_status"] == "SUCCESS"
            else latest_snapshot_id
        )
        processed_snapshot_id = _processed_snapshot_id(config.raw_volume_path)
        process_required = bool(
            authoritative_snapshot_id
            and authoritative_snapshot_id != processed_snapshot_id
        )
        audit_rows = _audit_rows(
            snapshot_id,
            file_records,
            databricks_job_id,
            databricks_job_run_id,
            manifest["ingestion_status"],
            config.raw_volume_path,
        )
        return {
            "snapshot_id": snapshot_id,
            "authoritative_snapshot_id": authoritative_snapshot_id,
            "snapshot_changed": manifest["ingestion_status"] == "SUCCESS",
            "process_required": process_required,
            "manifest": manifest,
            "audit_rows": audit_rows,
        }
    except Exception as error:
        if not sources:
            sources = [
                SourceFile("BLS", "<directory-listing>", BLS_DIRECTORY_URL),
                SourceFile("DataUSA", "population.json", POPULATION_API_URL),
            ]
        existing_keys = {
            (item.get("source_name"), item.get("source_file"))
            for item in file_records
        }
        for source in sources:
            key = (source.source_name, source.source_file)
            if key not in existing_keys:
                file_records.append(
                    _manifest_file_record(
                        snapshot_id,
                        source,
                        metadata_by_key.get(key),
                        "FAILED",
                        "Snapshot aborted before this source file was downloaded.",
                    )
                )
        manifest.update(
            {
                "completed_at": utc_timestamp(),
                "ingestion_status": "FAILED",
                "error_details": f"{type(error).__name__}: {error}",
            }
        )
        for promoted_path in promoted_paths:
            if promoted_path.exists():
                shutil.rmtree(promoted_path)
        if staging_root.exists():
            shutil.rmtree(staging_root)
        write_failed_manifest(config.raw_volume_path, manifest)
        raise SnapshotIngestionFailure(
            f"Snapshot {snapshot_id} failed: {error}",
            snapshot_id=snapshot_id,
            audit_rows=_audit_rows(
                snapshot_id,
                file_records,
                databricks_job_id,
                databricks_job_run_id,
                "FAILED",
                config.raw_volume_path,
            ),
        ) from error
    finally:
        if own_session:
            http.close()
