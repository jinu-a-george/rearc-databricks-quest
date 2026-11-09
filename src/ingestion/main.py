from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_bootstrap_parser = argparse.ArgumentParser(add_help=False)
_bootstrap_parser.add_argument("--project-root")
_bootstrap_arguments, _ = _bootstrap_parser.parse_known_args()
if _bootstrap_arguments.project_root:
    sys.path.insert(0, _bootstrap_arguments.project_root)
elif __package__ in (None, "") and "__file__" in globals():
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.ingestion.audit import append_audit_rows
from src.ingestion.config import IngestionConfig
from src.ingestion.ingestion import SnapshotIngestionFailure, ingest_snapshot


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def _optional_integer(value: str | None, name: str) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer.") from error


def _job_context(arguments: argparse.Namespace) -> tuple[int | None, int | None]:
    job_id = _optional_integer(arguments.job_id, "Databricks job ID")
    run_id = _optional_integer(arguments.job_run_id, "Databricks job run ID")
    if job_id is not None or run_id is not None:
        return job_id, run_id

    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        job_id = _optional_integer(
            spark.conf.get("spark.databricks.job.id", None),
            "Databricks job ID",
        )
        run_id = _optional_integer(
            spark.conf.get("spark.databricks.job.runId", None),
            "Databricks job run ID",
        )
    except (ImportError, RuntimeError):
        return None, None
    return job_id, run_id


def _task_values_client():
    injected = globals().get("dbutils")
    if injected is not None:
        return injected
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        return DBUtils(SparkSession.builder.getOrCreate())
    except (ImportError, RuntimeError):
        return None


def _set_task_values(result: dict[str, object], required: bool) -> None:
    client = _task_values_client()
    if client is None:
        if required:
            raise RuntimeError(
                "Databricks task values are unavailable; the dependent pipeline "
                "cannot safely determine whether a source snapshot needs processing."
            )
        return

    task_values = client.jobs.taskValues
    task_values.set("snapshot_changed", result["snapshot_changed"])
    task_values.set("process_required", result["process_required"])
    snapshot_id = result.get("authoritative_snapshot_id") or ""
    task_values.set("snapshot_id", snapshot_id)
    task_values.set("ingestion_attempt_id", result["snapshot_id"])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Monitor source files and create immutable coordinated snapshots."
    )
    parser.add_argument("--bls-contact-email")
    parser.add_argument("--project-root", help=argparse.SUPPRESS)
    parser.add_argument("--raw-volume-path")
    parser.add_argument("--job-id")
    parser.add_argument("--job-run-id")
    parser.add_argument(
        "--force-publish",
        choices=("true", "false"),
        default="false",
        help="Publish a new snapshot and run downstream pipelines even if checksums match.",
    )
    arguments = parser.parse_args(argv)

    config = IngestionConfig.from_environment(
        bls_contact_email=arguments.bls_contact_email,
        raw_volume_path=arguments.raw_volume_path,
    )
    job_id, job_run_id = _job_context(arguments)
    try:
        result = ingest_snapshot(
            config,
            databricks_job_id=job_id,
            databricks_job_run_id=job_run_id,
            force_publish=arguments.force_publish == "true",
        )
    except SnapshotIngestionFailure as error:
        try:
            append_audit_rows(error.audit_rows)
        except Exception:
            LOGGER.exception("Could not persist failed source ingestion audit rows.")
        raise

    append_audit_rows(result["audit_rows"])
    _set_task_values(result, required=job_id is not None or job_run_id is not None)
    LOGGER.info(
        "Snapshot attempt %s completed: status=%s, authoritative_snapshot=%s, "
        "process_required=%s",
        result["snapshot_id"],
        result["manifest"]["ingestion_status"],
        result["authoritative_snapshot_id"],
        result["process_required"],
    )


if __name__ == "__main__":
    main()
