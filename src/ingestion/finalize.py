from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_bootstrap_parser = argparse.ArgumentParser(add_help=False)
_bootstrap_parser.add_argument("--project-root")
_bootstrap_arguments, _ = _bootstrap_parser.parse_known_args()
if _bootstrap_arguments.project_root:
    sys.path.insert(0, _bootstrap_arguments.project_root)
elif __package__ in (None, "") and "__file__" in globals():
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, functions as F

from src.ingestion.audit import BRONZE_TABLES, update_audit_ingested_counts
from src.ingestion.manifest import read_json, write_json_atomic


def finalize_snapshot(
    snapshot_id: str,
    raw_volume_path: Path,
    ingestion_attempt_id: str | None = None,
) -> None:
    manifest_path = raw_volume_path / "manifests" / f"{snapshot_id}.json"
    manifest = read_json(manifest_path)
    if (
        manifest.get("snapshot_id") != snapshot_id
        or manifest.get("ingestion_status") != "SUCCESS"
    ):
        raise ValueError(f"Cannot finalize unsuccessful snapshot {snapshot_id}.")

    spark = SparkSession.builder.getOrCreate()
    counts: dict[tuple[str, str], int] = {}
    for source in manifest.get("files", []):
        source_name = source["source_name"]
        source_file = source["source_file"]
        if source_name == "DataUSA":
            table_name = "population"
        elif source_name == "BLS":
            table_name = BRONZE_TABLES.get(source_file)
        else:
            continue
        if table_name is None:
            continue

        rows = (
            spark.read.table(f"rearc_catalog.data_quest_bronze.{table_name}")
            .filter(F.col("_snapshot_id") == snapshot_id)
            .count()
        )
        counts[(source_name, source_file)] = rows

    update_audit_ingested_counts(ingestion_attempt_id or snapshot_id, counts)
    write_json_atomic(
        raw_volume_path / "processing" / "latest.json",
        {
            "snapshot_id": snapshot_id,
            "status": "SUCCESS",
            "audit_finalized": True,
            "manifest_path": manifest_path.relative_to(raw_volume_path).as_posix(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Finalize snapshot processing counts and checkpoint the run."
    )
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--ingestion-attempt-id")
    parser.add_argument("--raw-volume-path")
    parser.add_argument("--project-root", help=argparse.SUPPRESS)
    arguments = parser.parse_args(argv)
    raw_volume_path = Path(
        arguments.raw_volume_path
        or os.environ.get(
            "RAW_VOLUME_PATH",
            "/Volumes/rearc_catalog/data_quest_bronze/raw_data",
        )
    )
    finalize_snapshot(
        arguments.snapshot_id,
        raw_volume_path,
        ingestion_attempt_id=arguments.ingestion_attempt_id,
    )


if __name__ == "__main__":
    main()
