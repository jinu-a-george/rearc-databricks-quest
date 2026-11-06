from __future__ import annotations

from typing import Any


AUDIT_TABLE = "rearc_catalog.data_quest_bronze.pipeline_job_details"
BRONZE_TABLES = {
    "pr.class": "pr_class",
    "pr.data.0.Current": "pr_data_0_current",
    "pr.data.1.AllData": "pr_data_1_alldata",
    "pr.contacts": "pr_contacts",
    "pr.duration": "pr_duration",
    "pr.footnote": "pr_footnote",
    "pr.measure": "pr_measure",
    "pr.period": "pr_period",
    "pr.seasonal": "pr_seasonal",
    "pr.sector": "pr_sector",
    "pr.series": "pr_series",
    "pr.txt": "pr_txt",
}
AUDIT_STATUSES = {"SUCCESS", "UNCHANGED", "FAILED"}


def append_audit_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        LongType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("snapshot_id", StringType(), False),
            StructField("source_snapshot_id", StringType(), True),
            StructField("databricks_job_id", LongType(), True),
            StructField("databricks_job_run_id", LongType(), True),
            StructField("source_name", StringType(), False),
            StructField("source_file", StringType(), False),
            StructField("source_url", StringType(), True),
            StructField("source_path", StringType(), True),
            StructField("checksum_sha256", StringType(), True),
            StructField("file_size_bytes", LongType(), True),
            StructField("http_status", IntegerType(), True),
            StructField("etag", StringType(), True),
            StructField("last_modified", StringType(), True),
            StructField("content_length", StringType(), True),
            StructField("download_started_at", TimestampType(), True),
            StructField("download_completed_at", TimestampType(), True),
            StructField("records_downloaded", LongType(), True),
            StructField("records_ingested", LongType(), True),
            StructField("snapshot_status", StringType(), False),
            StructField("status", StringType(), False),
            StructField("error_message", StringType(), True),
        ]
    )
    invalid_statuses = {
        row["status"] for row in rows if row["status"] not in AUDIT_STATUSES
    }
    if invalid_statuses:
        raise ValueError(f"Unsupported source status values: {sorted(invalid_statuses)}")
    spark = SparkSession.builder.getOrCreate()
    spark.createDataFrame(rows, schema=schema).write.mode("append").saveAsTable(
        AUDIT_TABLE
    )


def update_audit_ingested_counts(
    snapshot_id: str, counts: dict[tuple[str, str], int]
) -> None:
    if not counts:
        return

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    updates = [
        {
            "snapshot_id": snapshot_id,
            "source_name": source_name,
            "source_file": source_file,
            "records_ingested": count,
        }
        for (source_name, source_file), count in counts.items()
    ]
    spark.createDataFrame(updates).createOrReplaceTempView("audit_count_updates")
    spark.sql(
        f"""
        MERGE INTO {AUDIT_TABLE} AS target
        USING audit_count_updates AS source
        ON target.snapshot_id = source.snapshot_id
           AND target.source_name = source.source_name
           AND target.source_file = source.source_file
           AND target.status IN ('SUCCESS', 'UNCHANGED')
        WHEN MATCHED THEN UPDATE SET
          target.records_ingested = source.records_ingested
        """
    )
