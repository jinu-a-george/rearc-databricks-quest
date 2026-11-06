from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SNAPSHOT_ID_PATTERN = re.compile(r"^\d{20}$")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_snapshot_id(now: datetime | None = None) -> str:
    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        raise ValueError("Snapshot IDs must be generated from a timezone-aware time.")
    snapshot_id = current_time.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    if not SNAPSHOT_ID_PATTERN.fullmatch(snapshot_id):
        raise RuntimeError("Generated an invalid ingestion snapshot ID.")
    return snapshot_id


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Could not read JSON metadata: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}.")
    return value


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary_path.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def latest_snapshot_path(raw_volume_path: Path) -> Path:
    return raw_volume_path / "latest.json"


def load_latest_manifest(raw_volume_path: Path) -> dict[str, Any] | None:
    pointer_path = latest_snapshot_path(raw_volume_path)
    if not pointer_path.exists():
        return None

    pointer = read_json(pointer_path)
    snapshot_id = pointer.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not SNAPSHOT_ID_PATTERN.fullmatch(
        snapshot_id
    ):
        raise ValueError(f"Invalid snapshot ID in {pointer_path}.")
    if pointer.get("status") != "SUCCESS":
        raise ValueError(f"Latest snapshot pointer is not successful: {pointer_path}")

    manifest_path = raw_volume_path / "manifests" / f"{snapshot_id}.json"
    manifest = read_json(manifest_path)
    if (
        manifest.get("snapshot_id") != snapshot_id
        or manifest.get("ingestion_status") != "SUCCESS"
    ):
        raise ValueError(f"Latest pointer references an invalid manifest: {manifest_path}")
    return manifest


def write_failed_manifest(
    raw_volume_path: Path, manifest: dict[str, Any]
) -> None:
    manifest_path = raw_volume_path / "manifests" / f"{manifest['snapshot_id']}.json"
    write_json_atomic(manifest_path, manifest)


def commit_successful_snapshot(
    raw_volume_path: Path, manifest: dict[str, Any]
) -> None:
    snapshot_id = manifest["snapshot_id"]
    completed_at = manifest["completed_at"]
    manifest_path = raw_volume_path / "manifests" / f"{snapshot_id}.json"
    write_json_atomic(manifest_path, manifest)

    pointer_common = {
        "snapshot_id": snapshot_id,
        "manifest_path": manifest_path.relative_to(raw_volume_path).as_posix(),
        "completed_at": completed_at,
        "status": "SUCCESS",
    }
    for source_name, source_path in (
        ("BLS", raw_volume_path / "bls" / "snapshots" / snapshot_id),
        ("DataUSA", raw_volume_path / "population" / "snapshots" / snapshot_id),
    ):
        write_json_atomic(
            source_path.parent.parent / "latest.json",
            {
                **pointer_common,
                "source_name": source_name,
                "snapshot_path": source_path.relative_to(raw_volume_path).as_posix(),
            },
        )

    # This pointer is the commit marker readers use if per-source pointers race.
    write_json_atomic(
        latest_snapshot_path(raw_volume_path),
        {
            **pointer_common,
            "source_name": "BLS+DataUSA",
            "snapshot_path": ".",
        },
    )
