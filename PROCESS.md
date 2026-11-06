# Process and design notes

## Architecture and data flow

The DAB defines an environment setup job and a scheduled ingestion workflow. The workflow discovers BLS files dynamically, retrieves the complete DataUSA response, validates both sources, writes immutable snapshot directories under `/Volumes/rearc_catalog/data_quest_bronze/raw_data/`, and only then advances the root snapshot pointer. A condition task skips pipeline refreshes when the latest snapshot has already been processed.

Three serverless Lakeflow Spark Declarative Pipelines implement the layers:

- **Bronze** parses source formats while retaining source values and appending `_snapshot_id` and `_ingested_at` as the final columns. File path, checksum, HTTP, and job metadata are held once per source file in `pipeline_job_details`. It loads only committed snapshots and retains successful snapshot history.
- **Silver** casts and validates all committed productivity and population snapshots into `bls_productivity`, `bls_series`, and `population`. `bls_series` enriches series metadata with sector, class, measure, duration, and seasonal descriptions, then derives system-observed SCD Type 2 versions from attribute hashes and snapshot IDs. The fact tables retain snapshot keys so revised observations are not overwritten. Invalid rows are retained and surfaced through expectations; malformed records and missing identifiers are also represented in quarantine.
- **Gold** materializes `population_statistics`, `best_productivity_year_by_series`, and `target_series_with_population`, using the existing canonical PySpark transformations.

The `pipeline_job_details` table has one row per source file and ingestion attempt. Its source `status` is `SUCCESS` (file downloaded and validated), `UNCHANGED` (source validators or checksums match the prior copy), or `FAILED` (download or validation failed). Its `snapshot_status` is the overall coordinated attempt status: `SUCCESS`, `UNCHANGED`, or `FAILED`. The final workflow task counts Bronze rows per file and updates the processing pointer only after all three pipelines succeed. This makes an unchanged source retryable when a prior downstream run failed.

## Source and schema decisions

BLS files are tab-separated; their discovered inventory is recorded in the manifest. Known tabular files use headers confirmed by `data/source_inspection/profile_report.json`. Every discovered original remains preserved in the Volume, but only the inspected source schemas become typed Bronze tables; there is no duplicate catch-all `bls_raw_records` table. `pr.data.0.Current` and `pr.data.1.AllData` are loaded independently. DataUSA's original JSON envelope is preserved in the Volume; its record fields are normalized for Delta table compatibility.

Bronze is source-shaped and keeps source fields as strings rather than applying business cleanup. For DataUSA population, Bronze quotes the numeric token in the raw JSON text before parsing records, preserving the original string form (such as `325719178.0`) instead of Spark's exponent-format rendering. Only `_snapshot_id` and `_ingested_at` are added to each Bronze table, at the end of the schema. `pipeline_job_details` contains source file/path/checksum and job/run details, avoiding per-row duplication. Silver owns type conversion and data quality. Population values are cast through `DECIMAL(20,1)` before converting to integer counts. The root `latest.json` is the authoritative commit marker; source-specific pointers are informational. A candidate manifest can exist without being consumable if promotion did not reach the root pointer. Snapshot IDs are fixed-width UTC timestamps, so lexical ordering lets Bronze exclude manifests newer than the committed snapshot.

The raw Volume is in the existing `data_quest_bronze` schema; there is no separate `data_quest` schema. The one-time migration removes the obsolete `data_quest` schema/Volume and clears prior contents from the target raw Volume before the new initial snapshot. Setup does not perform those destructive operations on subsequent runs.

The series dimension is system-observed SCD Type 2: `valid_from_snapshot_id` is the first snapshot where an attribute hash was observed, and `valid_to_snapshot_id` is the next snapshot where a different hash appeared. These are observation boundaries, not source-provided effective dates. The two fact tables retain a row per business key and snapshot so revisions in productivity or population are not overwritten.

The monitor uses BLS directory discovery and HEAD metadata where available. It downloads all sources if metadata is incomplete or differs, then compares SHA-256 checksums. This favors a coordinated, reproducible snapshot over per-file incremental updates. When every discovered source has complete matching validators, it avoids downloading the files.

## Analytics

The population analysis computes population standard deviation across 2013–2018 and reports the number of distinct years so missing annual coverage is visible. The productivity ranking sums Q01–Q04 values by series and year, excluding Q05 (the annual-average period), then chooses the earliest year in the event of a tie. The target-series analysis uses a left join so a BLS year is not discarded when population is missing. Gold and SQL analyses select the latest coordinated snapshot while Silver retains historical snapshots. SQL and PySpark implementations are maintained in parallel.

## Data quality and operational trade-offs

Download and schema validation failures stop the ingestion task and do not advance the snapshot pointer. Bronze retains raw representations; its quarantine view identifies malformed TSV records and missing identifiers. Silver expectations retain invalid rows while recording expectation metrics, rather than silently dropping data. The separate audit table records source status, checksums, timestamps, downloaded record counts, and post-pipeline ingested row counts.

The source metadata optimization depends on upstream ETag, Last-Modified, and Content-Length values being complete and reliable. If not, the workflow falls back to full downloads and checksum comparison. The pipeline uses batch materialized views because the workflow creates complete coordinated snapshots. This is intentionally simpler than file-level streaming or a change-data-capture design for this assignment.

The weekly default schedule is configurable. Workspace permissions, Free Edition features, and serverless Python task behavior must be verified in the target workspace; successful local validation is not evidence that a Databricks run completed.

## Validation and deployment record

Local validation passed with 16 unit tests, Python compilation, and `databricks bundle validate -t dev`. The environment setup job completed successfully. Workflow run `188330031364843` completed ingestion, Bronze, Silver, Gold, and finalization successfully. The committed snapshot is `20261106182546392457`; its processing marker records `SUCCESS` with `audit_finalized: true`. Audit verification covered 12 BLS files and 1 DataUSA response; the finalizer reconciled 139,292 BLS Bronze rows and 11 population rows. The workspace has 13 source-shaped Bronze tables (12 BLS tables and 1 DataUSA table), exactly three Silver tables, and three Gold analytics tables. Each source-shaped Bronze table has `_snapshot_id` and `_ingested_at`, without per-row `_source_*` columns. The obsolete `bls_raw_records` and `silver_bls_observations` materialized views were explicitly removed. Silver schema verification confirmed snapshot-preserving productivity and population facts and the SCD Type 2 series dimension. SQL-versus-Gold comparisons for all three analytics returned zero rows in both difference directions.

The first live pipeline update exposed Unity Catalog's prohibition on `input_file_name()`, which was replaced with `_metadata.file_path`. A later Gold update exposed that a workspace pipeline module could not import the local `src` package by default; the DAB now supplies the workspace project root. Final audit verification also confirmed that row-count updates must use the ingestion-attempt ID, while the processing checkpoint uses the authoritative source snapshot ID. Those runtime findings were fixed and the full workflow was rerun successfully.

Add sanitized screenshots under `screenshots/` to demonstrate the successful ingestion, Volume layout, pipeline run, tables, and analytical outputs before submission.

## AI usage

AI assistance was used to review the existing repository, implement snapshot-aware ingestion and pipeline wiring, update tests and documentation, and reason about failure recovery. The implementation is checked against the source profile, mocked ingestion tests, bundle validation, and—after deployment—actual Databricks run and table results. Review the final changes and workspace outputs before submission.

## Retrospective and future improvements

After the Databricks run, record the Free Edition limitations or compatibility adjustments encountered and what was learned about coordinating source snapshots, pipeline dependencies, and retryable processing. Potential follow-ups include stronger schema-drift alerting, explicit expectations/quarantine policies for each dataset, and automated SQL-versus-PySpark output comparison.
