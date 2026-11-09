CREATE CATALOG IF NOT EXISTS rearc_catalog;

CREATE SCHEMA IF NOT EXISTS rearc_catalog.data_quest_bronze;
CREATE SCHEMA IF NOT EXISTS rearc_catalog.data_quest_silver;
CREATE SCHEMA IF NOT EXISTS rearc_catalog.data_quest_gold;

GRANT USE CATALOG ON CATALOG rearc_catalog
TO `grp_rearc_data_quest_analyst_readonly`;
GRANT USE SCHEMA ON SCHEMA rearc_catalog.data_quest_gold
TO `grp_rearc_data_quest_analyst_readonly`;
GRANT SELECT ON SCHEMA rearc_catalog.data_quest_gold
TO `grp_rearc_data_quest_analyst_readonly`;

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

COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.snapshot_id
IS 'Identifier of the ingestion attempt that recorded this file result.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.source_snapshot_id
IS 'Coordinated source snapshot ID associated with this file, when published.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.databricks_job_id
IS 'Databricks job ID that performed the ingestion attempt.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.databricks_job_run_id
IS 'Databricks job run ID that performed the ingestion attempt.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.source_name
IS 'Source system identifier, such as BLS or DataUSA.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.source_file
IS 'Original source filename recorded for the attempt.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.source_url
IS 'URL from which the source file was retrieved.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.source_path
IS 'Unity Catalog Volume path containing the preserved source file.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.checksum_sha256
IS 'SHA-256 checksum of the downloaded source file.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.file_size_bytes
IS 'Size of the downloaded source file in bytes.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.http_status
IS 'HTTP status code returned for the source request, when available.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.download_started_at
IS 'Timestamp when the source download attempt started.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.download_completed_at
IS 'Timestamp when the source download attempt completed.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.records_downloaded
IS 'Number of records validated from the downloaded source file.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.records_ingested
IS 'Number of source records loaded into the corresponding Bronze table.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.snapshot_status
IS 'Status of the coordinated multi-source snapshot attempt.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.status
IS 'Status of this source file attempt: SUCCESS, UNCHANGED, or FAILED.';
COMMENT ON COLUMN rearc_catalog.data_quest_bronze.pipeline_job_details.error_message
IS 'Diagnostic message when the source file attempt failed.';
