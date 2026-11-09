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

The root `latest.json` file is the only source commit marker; source-specific `bls/latest.json` and `population/latest.json` pointers are not used. Snapshots are written under `bls/snapshots/<snapshot_id>/` and `population/snapshots/<snapshot_id>/`; manifests are stored under `manifests/`. A failed attempt does not advance the commit marker. Bronze and the two fact tables retain successful committed snapshots. The series dimension records metadata versions observed across snapshots. Gold produces results for each retained Silver source snapshot.

Every scheduled run downloads all discovered BLS files and the complete DataUSA response, then compares their SHA-256 checksums with the latest successful manifest. If the inventory and all checksums match, it marks the attempt `UNCHANGED`, removes staging, and skips downstream processing unless the previous snapshot still needs processing. Any changed file or source inventory creates a new coordinated snapshot. Successful promotion removes that snapshot's `.staging/<snapshot_id>` directory; interrupted runs can leave a staging directory for later manual cleanup.

The optional `force_publish` job parameter defaults to `"false"` and accepts only `"true"` or `"false"`. When `"true"`, the job still downloads and validates all sources but skips the comparison decision, publishes a new snapshot even when all checksums match, and requests Bronze, Silver, and Gold processing. Override it with **Run now with different settings** for a one-time forced run; the default remains `"false"` for scheduled runs. The ingestion script also accepts `--force-publish true|false` when run directly.

The workflow writes one row per source file and attempt to `rearc_catalog.data_quest_bronze.pipeline_job_details`, including source path, URL, checksum, downloaded size, HTTP status, job context, and downloaded/ingested counts. `status` describes the source-file attempt: `SUCCESS` means downloaded and validated, `UNCHANGED` means the checksum matched the prior source and no new copy was kept, and `FAILED` means the file could not be downloaded or validated. `snapshot_status` describes the coordinated attempt (`SUCCESS`, `UNCHANGED`, or `FAILED`). After all pipelines complete, the final task records per-file Bronze counts and advances `processing/latest.json`. If downstream processing fails, that pointer remains unchanged so the snapshot can be retried.

## Prerequisites and deployment

- Databricks CLI 1.24.0 or later, authenticated to the target workspace.
- Unity Catalog permissions to create the catalog/schema/volume and pipeline tables, and to grant privileges.
- The Unity Catalog group `grp_rearc_data_quest_analyst_readonly` must already exist.
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

`setup_environment` is a separate, manually run bootstrap job. Run it once after deployment to create the Bronze, Silver, and Gold schemas, `/Volumes/rearc_catalog/data_quest_bronze/raw_data/`, and the `pipeline_job_details` table. It grants the existing `grp_rearc_data_quest_analyst_readonly` group `USE CATALOG` on `rearc_catalog`, plus `USE SCHEMA` and `SELECT` on `rearc_catalog.data_quest_gold`. Schema-level `SELECT` covers current and future Gold tables/views; the group receives no Bronze/Silver or raw Volume access from these grants. Create and manage the account-level group outside this bundle, and ensure the setup identity is allowed to grant the privileges.

After setup completes, run `bronze_ingestion_and_load` to ingest sources and refresh the three pipelines. This workflow does not rerun environment setup. It is scheduled every Monday at 06:00 UTC; override `monitor_cron_expression` when deploying to another cadence. The job allows only one concurrent run to avoid overlapping snapshot promotion. The obsolete `rearc_catalog.data_quest` schema is not used.

Set `BUNDLE_VAR_bls_contact_email` in the shell before deployment or provide `--var="bls_contact_email=..."`. The value is not stored in the bundle. Override `raw_volume_path` and `sql_warehouse_id` if the target workspace uses different resources.

## Ingestion and data layers

BLS file names are discovered from the live directory listing; the implementation does not treat the known files as the complete inventory. Downloads use bounded retries for transient HTTP/network failures, stream to temporary files, and are checked for valid content, size, and checksum before snapshot promotion. The original TSV files and complete DataUSA JSON response remain unmodified in the Volume.

Bronze is source-oriented and uses table names without a redundant `bronze_` prefix, for example `rearc_catalog.data_quest_bronze.pr_data_1_alldata` and `rearc_catalog.data_quest_bronze.population`. BLS TSV fields and DataUSA record fields are strings. For population, the reader quotes the numeric token in the raw JSON text before parsing, preserving its original representation (for example `325719178.0`) instead of Spark's normalized exponent form. `_snapshot_id` and `_ingested_at` are the final two columns in each Bronze table. File paths, checksums, and other ingestion metadata live in `pipeline_job_details`, not duplicated on every Bronze row. All dynamically discovered originals, including `pr.txt` and `pr.contacts`, remain preserved in the Volume; those two text-only documents do not have Bronze tables. The declarative Bronze tables cover the inspected, known BLS tabular schemas. `pr.data.0.Current` and `pr.data.1.AllData` remain separate and are never blindly unioned. Updating pipeline source code does not remove existing workspace views; inspect and remove obsolete views manually in the Databricks SQL editor if needed. The setup SQL creates `pipeline_job_details` when absent but does not migrate an existing table; compare an existing table with the current definition in `sql/setup_environment.sql` and reconcile its schema before running the updated ingestion workflow.

Bronze, Silver, and Gold materialized views declare Unity Catalog table descriptions with the SDP `comment` parameter and column comments in the SDP `schema` definitions. BLS field descriptions follow `data/source_inspection/bls/pr.txt` and the inspected source headers; DataUSA fields and derived Silver/Gold fields are described according to their source meaning and transformation. The separate Bronze ingestion audit table is an operational table created by the workflow setup SQL, not an SDP dataset.

Silver contains `bls_productivity`, `bls_series`, `population`, and `quarantine`; Gold contains `population_statistics`, `productivity_annual_ranking`, and `productivity_population_comparison`. The layer schemas identify the data layer, so table names do not repeat `silver_` or `gold_`. The two Silver fact tables retain each source snapshot so revisions remain reproducible. `bls_series` enriches BLS codes with lookup descriptions and derives system-observed SCD Type 2 versions using snapshot IDs; it does not claim business-effective dates. Silver converts population values to integer counts. Records that fail any Silver expectation are excluded from the three curated Silver tables and written to Silver `quarantine` with the source record and failed expectation names. Malformed BLS TSV rows and missing identifiers in BLS lookup files are also exposed in the Bronze `quarantine` table; BLS `series_id` and population `nation_id` validation is performed in Silver. Gold analytics use canonical PySpark logic in `src/pipeline/analytics.py`, with equivalent Spark SQL in `sql/analysis_*.sql`.

Gold retains results for every Silver source snapshot and includes `_snapshot_id` and `_calculated_at` in each output. `population_statistics` reports one row per nation/snapshot; statistics are populated only when there are six unique, non-null annual records for 2013–2018, and duplicate nation/year records are excluded from the calculation. `productivity_annual_ranking` ranks every year on its observed Q01–Q04 sum (Q05 excluded), retains all years tied for the highest annual sum, and exposes each winner's coverage through `quarterly_observation_count` and `is_full_calendar_year`, following the assessment example that treats partial years as valid candidates. `productivity_population_comparison` left-joins the same-snapshot population for nation `01000US` by year, retaining BLS observations with an availability flag when population is missing. A forced publish of unchanged source data creates a distinct snapshot, and Gold retains a separate result for it. The audit table also intentionally records a separate row per source file and ingestion attempt, so audit/history row counts are not expected to remain unchanged.

Because Gold tables hold one result set per snapshot, read current values by filtering to the newest `_snapshot_id`; snapshot IDs are fixed-width UTC timestamps, so a text comparison selects the latest version without casting:

```sql
SELECT *
FROM rearc_catalog.data_quest_gold.productivity_annual_ranking
WHERE _snapshot_id = (
    SELECT MAX(_snapshot_id) FROM rearc_catalog.data_quest_gold.productivity_annual_ranking
);
```

For convenience, owners can layer `*_current` views with that same filter on top of the Gold tables. The deployed workspace contains the three: `population_statistics_current`, `productivity_annual_ranking_current`, and `productivity_population_comparison_current`; each keeps `_snapshot_id` and `_calculated_at`, and the base tables remain the complete snapshot history. They are created once after the first successful Gold pipeline run (the bundle's setup cannot reference Gold tables that do not exist yet), using the `SELECT MAX(_snapshot_id)` filter pattern above; recreate them after a fresh deployment if needed.

The population analysis uses `STDDEV_POP` for 2013–2018 and does not fill missing years with zero. Productivity annual sums exclude Q05 (the annual-average period), rank partial years alongside complete ones with a completeness flag, and preserve ties. The target-series analysis returns `PRS30006032` Q01 observations with series description, productivity value, population, nation, and a population-availability indicator.

## Genie agent (bonus)

A Databricks Genie space is configured over the three Gold tables so analysts can ask natural-language questions about population statistics, annual productivity rankings, and the target-series comparison. Its instructions cover the per-snapshot results model, the pre-aggregated nature of Gold measures, tied peak years in the annual ranking, and the scope of the loaded datasets. Sample questions and answers are captured in [`SCREENSHOTS.md`](./SCREENSHOTS.md). The space reads only the analyst-granted Gold tables and inherits the existing access-control model.

## Local development and tests

The local Python environment is for unit tests and source inspection; it is not a substitute for a mounted Unity Catalog Volume.

```powershell
python -m pip install -r requirements.txt
python -m pytest tests -q
```

The tests are `unittest` classes, so `python -m unittest discover -s tests -v` works as well; both report all test cases passing when `BLS_CONTACT_EMAIL` is set to any valid email in the environment.

`tools/local_source_profiler.py` is a one-time local exploration utility. It downloads originals to `data/source_inspection/` and produces `profile_report.json`; the folder is gitignored, so these inspection artifacts are referenced by the documentation as local analysis inputs and are not part of the repository. Do not commit source data or credentials. See `PROCESS.md` for design choices, limitations, and the execution retrospective.
