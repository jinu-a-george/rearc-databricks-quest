CREATE CATALOG IF NOT EXISTS rearc_catalog;

CREATE SCHEMA IF NOT EXISTS rearc_catalog.data_quest_bronze;
CREATE SCHEMA IF NOT EXISTS rearc_catalog.data_quest_silver;
CREATE SCHEMA IF NOT EXISTS rearc_catalog.data_quest_gold;

CREATE VOLUME IF NOT EXISTS rearc_catalog.data_quest_bronze.raw_data;

CREATE TABLE IF NOT EXISTS rearc_catalog.data_quest_bronze.pipeline_job_details (
    snapshot_id STRING NOT NULL,
    source_snapshot_id STRING,
    databricks_job_id BIGINT,
    databricks_job_run_id BIGINT,
    source_name STRING NOT NULL,
    source_file STRING NOT NULL,
    source_url STRING,
    source_path STRING,
    checksum_sha256 STRING,
    file_size_bytes BIGINT,
    http_status INT,
    etag STRING,
    last_modified STRING,
    content_length STRING,
    download_started_at TIMESTAMP,
    download_completed_at TIMESTAMP,
    records_downloaded BIGINT,
    records_ingested BIGINT,
    snapshot_status STRING NOT NULL,
    status STRING NOT NULL,
    error_message STRING
)
USING DELTA
COMMENT 'One row per source file and ingestion attempt. status is SUCCESS, UNCHANGED, or FAILED.';
