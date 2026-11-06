# Rearc Data Quest: Databricks Edition

This project downloads the BLS productivity time-series directory and the complete DataUSA population response, preserves each coordinated source snapshot in a Unity Catalog Volume, and processes it through Bronze, Silver, and Gold Lakeflow Spark Declarative Pipelines.

## Architecture

```text
BLS directory + DataUSA API
          |
          v
Weekly DAB job: discover, monitor, download, validate and commit snapshot
          |
          v
/Volumes/rearc_catalog/data_quest_bronze/raw_data/
          |
          v
Bronze source-shaped tables -> Silver validated/typed tables -> Gold analytics
```

The root `latest.json` file is the coordinated snapshot commit marker. Snapshots are written under `bls/snapshots/<snapshot_id>/` and `population/snapshots/<snapshot_id>/`; manifests are stored under `manifests/`. A failed attempt does not advance the commit marker. Bronze and the two fact tables retain successful committed snapshots. The series dimension records metadata versions observed across snapshots. Gold selects the latest snapshot for its assignment analyses. If source metadata cannot prove that all files are unchanged, ingestion downloads the complete source set and compares checksums before deciding whether a new snapshot is needed.

The workflow writes one row per source file and attempt to `rearc_catalog.data_quest_bronze.pipeline_job_details`, including source path, URL, checksum, size, HTTP metadata, job context, and downloaded/ingested counts. `status` describes the source-file attempt: `SUCCESS` means downloaded and validated, `UNCHANGED` means validators or checksums matched the prior source and no new copy was needed, and `FAILED` means the file could not be downloaded or validated. `snapshot_status` describes the coordinated attempt (`SUCCESS`, `UNCHANGED`, or `FAILED`). After all pipelines complete, the final task records per-file Bronze counts and advances `processing/latest.json`. If downstream processing fails, that pointer remains unchanged so the snapshot can be retried.

## Prerequisites and deployment

- Databricks CLI 1.24.0 or later, authenticated to the target workspace.
- Unity Catalog permissions to create the catalog/schema/volume and pipeline tables.
- A Databricks SQL warehouse for the setup SQL task.
- A valid contact email for the BLS User-Agent.

In PowerShell, from the repository root:

```powershell
$env:BUNDLE_VAR_bls_contact_email = "your-contact-email@example.com"
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run setup_environment -t dev
databricks bundle run bronze_ingestion_and_load -t dev
```

The setup job creates the Bronze, Silver, and Gold schemas, `/Volumes/rearc_catalog/data_quest_bronze/raw_data/`, and the `pipeline_job_details` table. It must complete before the data workflow runs. The job is configured to monitor sources every Monday at 06:00 UTC; override `monitor_cron_expression` when deploying to another cadence. The job allows only one concurrent run to avoid overlapping snapshot promotion. The obsolete `rearc_catalog.data_quest` schema is not used.

Set `BUNDLE_VAR_bls_contact_email` in the shell before deployment or provide `--var="bls_contact_email=..."`. The value is not stored in the bundle. Override `raw_volume_path` and `sql_warehouse_id` if the target workspace uses different resources.

## Ingestion and data layers

BLS file names are discovered from the live directory listing; the implementation does not treat the known files as the complete inventory. Downloads use bounded retries for transient HTTP/network failures, stream to temporary files, and are checked for valid content, size, and checksum before snapshot promotion. The original TSV files and complete DataUSA JSON response remain unmodified in the Volume.

Bronze is source-oriented and uses table names without a redundant `bronze_` prefix, for example `rearc_catalog.data_quest_bronze.pr_data_1_alldata` and `rearc_catalog.data_quest_bronze.population`. BLS TSV fields and DataUSA record fields are strings. For population, the reader quotes the numeric token in the raw JSON text before parsing, preserving its original representation (for example `325719178.0`) instead of Spark's normalized exponent form. `_snapshot_id` and `_ingested_at` are the final two columns in each Bronze table. File paths, checksums, and other ingestion metadata live in `pipeline_job_details`, not duplicated on every Bronze row. Plain-text BLS documents are represented as line records. All dynamically discovered originals remain in the Volume; the declarative Bronze tables cover the inspected, known BLS file schemas. `pr.data.0.Current` and `pr.data.1.AllData` remain separate and are never blindly unioned.

Silver contains `bls_productivity`, `bls_series`, and `population`; Gold contains `population_statistics`, `best_productivity_year_by_series`, and `target_series_with_population`. The layer schemas identify the data layer, so table names do not repeat `silver_` or `gold_`. The two Silver fact tables retain each source snapshot so revisions remain reproducible. `bls_series` enriches BLS codes with lookup descriptions and derives system-observed SCD Type 2 versions using snapshot IDs; it does not claim business-effective dates. Silver converts population values to integer counts. Invalid rows are retained and surfaced through expectation metrics; malformed source rows and missing identifiers are also exposed in the Bronze `quarantine` table. Gold analytics use canonical PySpark logic in `src/pipeline/analytics.py`, with equivalent Spark SQL in `sql/analysis_*.sql`.

The analytics use population standard deviation for 2013–2018, sum BLS Q01–Q04 values before ranking years by series (earliest year wins ties), and left-join population to `PRS30006032` Q01 observations by year.

## Local development and tests

The local Python environment is for unit tests and source inspection; it is not a substitute for a mounted Unity Catalog Volume.

```powershell
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

The project also includes a local source profiler. It downloads originals to `data/source_inspection/` and produces `profile_report.json`; do not commit source data or credentials. See `PROCESS.md` for design choices, limitations, and the execution retrospective.
