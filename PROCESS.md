# Process and Design Notes

## Architecture and data flow

The solution is a snapshot-based batch workflow deployed with Databricks Asset Bundles (DABs). It uses the BLS productivity directory and the DataUSA annual population API as sources, Unity Catalog Volumes for immutable source files, and three Lakeflow Spark Declarative Pipelines for the Bronze, Silver, and Gold layers.

```mermaid
flowchart TD
    BLS[BLS directory listing and TSV files]
    DATAUSA[DataUSA population API]
    JOB[Weekly DAB workflow]
    DOWNLOAD[Discover, download, validate, checksum]
    VOLUME[Unity Catalog Volume: raw_data]
    MANIFEST[Manifests and latest.json]
    BRONZE[Bronze materialized views]
    SILVER[Silver materialized views and quarantine]
    GOLD[Gold analytical materialized views]
    AUDIT[pipeline_job_details audit table]
    SQL[Spark SQL alternatives]
    ANALYST[Read-only Gold analysts]

    BLS --> JOB
    DATAUSA --> JOB
    JOB --> DOWNLOAD
    DOWNLOAD --> VOLUME
    DOWNLOAD --> MANIFEST
    DOWNLOAD --> AUDIT
    VOLUME --> BRONZE
    MANIFEST --> BRONZE
    BRONZE --> SILVER
    SILVER --> GOLD
    SILVER --> SQL
    GOLD --> ANALYST
```

### Source discovery, snapshots, and repeat runs

Before designing the transformations, I reviewed the assessment requirements and inspected both source systems. I downloaded source copies locally for exploration and ran [`tools/local_source_profiler.py`](./tools/local_source_profiler.py). It profiles delimited files and DataUSA JSON for headers, delimiters, row and malformed-row counts, nulls and blanks, candidate data types, type mismatches, and suspicious characters. It also checks BLS observation keys, overlap between `pr.data.0.Current` and `pr.data.1.AllData`, lookup references, the target series, and year/period patterns. This helped me confirm the source layouts and choose the Bronze, Silver, and Gold schemas. Local copies are for inspection only; the deployed workflow downloads directly into the Unity Catalog Volume.

The `download_source_files` task dynamically discovers files from the BLS directory listing and retrieves the complete DataUSA JSON response. It uses a `requests.Session`, a contact-bearing BLS User-Agent, bounded retries for transient errors, explicit timeouts, streamed downloads, temporary files, and validation before promotion. It rejects invalid JSON, empty files, HTML returned in place of BLS data, and changed headers for known tabular files. Download failures are surfaced and do not advance the committed snapshot.

Each coordinated attempt gets a fixed-width UTC `snapshot_id`. Source files are first staged and then promoted to:

```text
/Volumes/rearc_catalog/data_quest_bronze/raw_data/
├── bls/snapshots/<snapshot_id>/
├── population/snapshots/<snapshot_id>/population.json
├── manifests/<snapshot_id>.json
├── latest.json
└── processing/latest.json
```

The source originals are not modified. A manifest records the discovered source inventory, paths, URLs, download timestamps, sizes, checksums, and status. The root `latest.json` is the authoritative commit marker; an incomplete or failed attempt cannot replace it. The separate `processing/latest.json` marker records which committed snapshot has completed all downstream pipelines and audit finalization.

Every scheduled run downloads the current source inventory and compares its checksums and file list against the latest successful manifest. If the inventory and checksums match, the attempt is marked unchanged and its staging files are removed. The workflow skips the pipelines only when that committed snapshot has already been processed. If processing previously failed, the unchanged committed snapshot remains eligible for a retry. If a file is added, removed, or changed, a new coordinated snapshot is committed and the Bronze, Silver, and Gold pipelines run. The optional `force_publish` parameter publishes a new snapshot and requests processing even when the checksums match.

The job runs weekly (Monday at 06:00 UTC by default) and permits one concurrent run to avoid overlapping snapshot promotion. The cadence is configurable through the bundle. This is a simple whole-source-batch design: a change to any source causes all three downstream pipelines to refresh.

### Bronze

Bronze consists of source-oriented materialized views in `rearc_catalog.data_quest_bronze`. The current pipeline defines ten BLS tabular source views and one DataUSA population view. The complete discovered directory—including text-only reference files such as `pr.txt` and `pr.contacts`—is retained in the Volume, but the two text-only documents do not have structured Bronze tables. They are plain-text documents with no keys, headers, or consistent columns, so a tabular Bronze representation would add no queryable value beyond the preserved originals. `pr.txt` is not unused: its field-definition sections guided the profiling of each TSV and became the wording for the table and column comments declared throughout Bronze, Silver, and Gold. These inspection artifacts, including `profile_report.json`, live under `data/source_inspection/`, which is gitignored: the repository references them as local analysis inputs rather than committing source data. A separate Bronze `quarantine` view exposes malformed BLS TSV records and missing required keys in BLS lookup files.

BLS TSV fields and parsed DataUSA record fields are kept as strings in Bronze. This preserves source representations and leaves business type conversion to Silver. The original DataUSA JSON envelope remains in the Volume; the Bronze reader extracts its records and preserves the population number's source representation as text. `pr.data.0.Current` and `pr.data.1.AllData` are loaded independently and never blindly unioned. Source profiling confirmed the overlap precisely: every one of `pr.data.0.Current`'s 46,159 observations for 1995–2026 is also present in `pr.data.1.AllData` (92,328 rows), so Current is a complete subset of the historical file. Silver types only the AllData file—a union of the two would double-count every shared observation, and the complete history is what the annual ranking needs. Current remains preserved as its own source-shaped Bronze table for completeness of the discovered directory; it is not part of the Silver curation path.

Each Bronze data row has `_snapshot_id` and `_ingested_at` as its final columns. The snapshot ID links the row to its committed source version; the ingestion timestamp records when that source file was downloaded. File-level metadata is kept in the operational `rearc_catalog.data_quest_bronze.pipeline_job_details` table rather than repeated on every row. Bronze reads only files in already-committed snapshots: snapshot IDs are fixed-width UTC timestamps, so lexical comparison against the root `latest.json` pointer excludes files belonging to snapshots that are newer than the commit marker. A just-promoted snapshot therefore becomes consumable only once the pointer advances.

The audit table has one row per source file and ingestion attempt. It records the attempt and source snapshot IDs, Databricks job and run IDs when available, source name, filename, URL and Volume path, SHA-256 checksum, file size, HTTP status, download timestamps, downloaded and ingested record counts, source status, overall snapshot status, and any error message. `SUCCESS`, `UNCHANGED`, and `FAILED` describe a file attempt; `snapshot_status` describes the coordinated attempt. After the pipelines complete, the final workflow task updates the ingested counts and advances the processing marker. This ties a row's `_snapshot_id` to the source files and the Databricks run that produced it.

### Silver

Silver is implemented as three curated materialized views—`bls_productivity`, `bls_series`, and `population`—plus a supporting `quarantine` view. The curated views retain the source snapshot columns so that revisions from later source snapshots do not overwrite history.

- **`bls_productivity`** reads `pr_data_1_alldata`, trims text, casts `year` to `INT` and `value` to `DECIMAL(20, 6)`, and retains the series ID, period, footnote codes, snapshot ID, and ingestion timestamp.
- **`bls_series`** joins each `pr.series` snapshot to its sector, class, measure, duration, and seasonal lookup rows from that same snapshot. It creates a human-readable description and derives system-observed SCD Type 2 versions using an attribute hash and snapshot IDs. These snapshot boundaries indicate when a metadata version was observed by this pipeline; they are not business-effective dates supplied by BLS.
- **`population`** standardizes the DataUSA fields, casts year to `INT`, and converts the numeric population through `DECIMAL(20, 1)` to `BIGINT`. The nation and snapshot identifiers remain available for validation and lineage.

The expectations validate required identifiers and snapshot keys, positive years, supported period codes, numeric values, valid series metadata, and non-negative population. The curated tables enforce these rules declaratively with `@dp.expect_all_or_drop`, which drops failing rows and reports their counts in the pipeline flow metrics. Rows that fail are also written to the Silver `quarantine` view with the source table and file, serialized raw record, snapshot metadata, rejection timestamp, and all failed expectation names. Bronze raw files are retained, so rejected rows can be inspected and reprocessed after a code or source correction. Quarantine is a diagnostic dataset, not an automated remediation process.

One documented limitation of this split: the original text of a malformed TSV line survives only in the Bronze quarantine `raw_record`. Spark's permissive CSV reader hands Silver a row whose parsed columns are all null, so Silver quarantine's `raw_record` for such a row is the serialized null record, not the bad line. Bronze quarantine therefore preserves malformed-row evidence, and Silver quarantine records the failed rules.

Bronze quarantine handles malformed TSV rows and missing identifiers in BLS lookup files. Required BLS observation `series_id` and DataUSA `nation_id` validation is applied in Silver. The curated Silver facts and the quarantined records are separate outputs; a rejected record is not silently allowed into a curated table.

### Gold and analytics

Gold contains three business-facing materialized views in `rearc_catalog.data_quest_gold`. All three retain results for every Silver source snapshot and include `_snapshot_id` and `_calculated_at`, so historical analytical outputs remain reproducible.

1. **`population_statistics`** reports one row per nation and snapshot. It filters each snapshot's population to 2013–2018 inclusive and calculates the average and **population** standard deviation from unique, non-null annual records. It also reports `observation_count`; the mean and standard deviation are populated only when all six years are present, and missing years are never replaced with zero.
2. **`productivity_annual_ranking`** sums each series's observed Q01–Q04 values by year and snapshot and ranks the totals with `DENSE_RANK`, retaining all years tied for the highest sum. Q05 is excluded because it is the annual average, not a quarter. Following the assessment's own example, partial years remain ranking candidates: each winning row carries `quarterly_observation_count` and an `is_full_calendar_year` flag, so coverage is explicit rather than used as an exclusion filter. Every row is enriched with the human-readable BLS series description that was valid for that snapshot, using the `bls_series` SCD boundaries.
3. **`productivity_population_comparison`** selects `PRS30006032` Q01 observations and left-joins population by year and coordinated snapshot ID, pinned to nation `01000US` so a future multi-nation source response cannot fan the join out. This retains BLS observations even when population is missing, and an explicit `population_available` flag records availability. The analyst-facing result contains the series ID, snapshot-current series description, year, period, productivity value, nation, and population.

Because Gold retains results for every Silver source snapshot, a Gold table contains one result set per committed snapshot rather than only the newest one. Downstream consumers therefore have two supported ways to read current values. The first is to filter to the newest snapshot in the query: snapshot IDs are fixed-width UTC timestamps, so `MAX(_snapshot_id)` returns the latest result set and needs no casting because lexical order equals numeric order.

```sql
SELECT *
FROM rearc_catalog.data_quest_gold.productivity_population_comparison
WHERE _snapshot_id = (
    SELECT MAX(_snapshot_id) FROM rearc_catalog.data_quest_gold.productivity_population_comparison
);
```

The second is to expose that filter once as a `*_current` view for convenience (this pattern was verified on the deployed workspace; the bundle itself cannot create such views during setup because the Gold tables do not exist until the Gold pipeline first runs, so they are created once after the first successful pipeline run). The deployed workspace contains all three:

```sql
CREATE OR REPLACE VIEW rearc_catalog.data_quest_gold.productivity_population_comparison_current AS
SELECT *
FROM rearc_catalog.data_quest_gold.productivity_population_comparison
WHERE _snapshot_id = (
    SELECT MAX(_snapshot_id) FROM rearc_catalog.data_quest_gold.productivity_population_comparison
);
```

The views keep `_snapshot_id` and `_calculated_at`, and the base tables remain the complete, reproducible snapshot history. Keeping snapshot retention in the base tables and making current-value reads a filter or view decision preserves full reproducibility without duplicating storage.

PySpark transformations in [`src/pipeline/analytics.py`](./src/pipeline/analytics.py) are the primary implementation feeding Gold, chosen so that all three answers share one typed module with unit-testable DataFrame logic and window functions. Equivalent Spark SQL is maintained in [`sql/analysis_1.sql`](./sql/analysis_1.sql), [`sql/analysis_2.sql`](./sql/analysis_2.sql), and [`sql/analysis_3.sql`](./sql/analysis_3.sql) as a secondary implementation and for direct validation. Keeping both implementations makes the calculations accessible to DataFrame and SQL users while making it possible to compare their outputs.

### Measurement semantics and edge cases

- **Q05 is excluded from the ranking.** The BLS fifth period code is the annual average, not a calendar quarter. The ranking sums by year within a series, so leaving Q05 rows in would add the annual average alongside the four quarterly values and inflate the total; Q01–Q04 are the quarters the question asks about.
- **Partial years are ranked, not discarded.** The assessment's example treats a year with two observations as a valid candidate, so the ranking sums whatever Q01–Q04 values a year actually contains and records each winning year's coverage. On the current source this matters for exactly one series: `PRS32006212`'s 1987 has three quarters summing to 18.7 and outranks its best complete year (2021, sum 16.3), and the table marks it with `quarterly_observation_count = 3` and `is_full_calendar_year = false`. Data coverage is exposed as an attribute instead of being used to silently drop real winners.
- **Annual values are literal sums of non-additive series.** BLS `value` is not one unit across series: some series store index points and others store percent changes (for example, `PRS30006011` measures year-over-year percent change, so its four-quarter sum is an arithmetic construction rather than a physical productivity amount). The question explicitly defines the ranking on "the largest sum of `value` across its quarters," and the ranking only ever compares years within a single series, so the literal sum is the well-defined operation requested. No cross-series normalization is attempted, and consumers should not compare `annual_value` between series of different measures.
- **Padded headers and fields.** Several BLS files pad header names and data fields with leading spaces (visible in `pr.data.1.AllData` as `series_id        <TAB>`). The ingestion validator strips header whitespace before comparing against the expected header list, so padded source headers validate correctly. Bronze keeps the fields as received, and Silver trims every text column before casting, so the padding never reaches the typed tables or the analytics.

### Deployment and access control

The DAB configuration in [`databricks.yml`](./databricks.yml) defines a one-time `setup_environment` job, a scheduled ingestion-and-load job, and separate serverless Bronze, Silver, and Gold pipelines. The setup SQL creates the three layer schemas, the raw Volume, and the audit table. It grants the existing `grp_rearc_data_quest_analyst_readonly` Unity Catalog group `USE CATALOG` on `rearc_catalog`, and `USE SCHEMA` plus `SELECT` on the Gold schema. It does not grant this analyst group access to Bronze, Silver, or the raw Volume. The group must exist before setup, and the identity running setup must have permission to grant access. Analysts also need access to an approved SQL warehouse or other compute; these grants provide data access only.

Deployment and execution from PowerShell, at the repository root:

```powershell
$env:BUNDLE_VAR_bls_contact_email = "your-contact-email@example.com"
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run setup_environment -t dev
databricks bundle run bronze_ingestion_and_load -t dev
```

The contact email is provided at deployment/runtime and is not stored as a credential in the bundle. Run setup once after deployment; the scheduled workflow does not repeat environment bootstrap.

### Genie agent space for Gold data

As a bonus capability, a native Databricks Genie space is configured over the three Gold tables so that business users can ask natural-language questions without querying the pipeline layers directly. The space description states the dataset scope and suggests example questions. Its instructions cover the semantics a questioner is most likely to miss: Gold results exist per source snapshot, so the agent must interpret which snapshot a question refers to; the Gold measures are pre-aggregated analytics, not raw facts to re-derive; the annual ranking sums Q01–Q04 per series-year and retains tied peak years, with `is_full_calendar_year` marking complete years; and the agent should stay within the scope of the three loaded Gold tables. Sample questions and answers are captured under the Genie section of [`SCREENSHOTS.md`](./SCREENSHOTS.md). Because the space reads only the analyst-granted Gold tables, it inherits the access-control model instead of widening it; it does not need Bronze, Silver, or Volume access.

## Trade-offs and limitations

### Source change detection and processing scope

Event-driven source notifications would be preferable in a production integration, but these public sources do not provide a notification mechanism for this project. A metadata-only polling approach using HTTP `HEAD` fields such as `Last-Modified`, `ETag`, and `Content-Length` was investigated. The observed BLS metadata differed and led to downloads even where file checksums matched; the DataUSA response did not provide the metadata needed for a reliable fast path. The current implementation therefore downloads and validates the sources, then compares SHA-256 checksums. This costs more network bandwidth but bases change detection on file contents and detects source inventory changes.

The default weekly schedule is a reasonable starting point for the apparent update cadence of these datasets, not a claim about their official refresh guarantees. For a client, I would first confirm the source publication cadence and business freshness requirement, then schedule accordingly.

For assessment simplicity, any changed file triggers the full coordinated batch: download, Bronze, Silver, and Gold. A production design should consider tracking changes at the individual file or source level and refreshing only dependent tables, once the lineage and recovery model make that safe. That would reduce unnecessary compute and processing as sources and tables grow.

### Storage, schema, and performance

The data volume does not currently justify partitioning or liquid clustering. If the BLS productivity fact grows, `bls_productivity` is the first table I would evaluate for liquid clustering on `series_id` and `year`, which align with the series-specific and annual aggregation query patterns. I would not initially cluster `bls_series` or `population`, which are comparatively small reference/fact datasets in the current profile.

The project does not implement a general schema-evolution policy. Known BLS tabular sources have expected headers and a changed header causes ingestion validation to fail; dynamically discovered files without a defined Bronze schema remain preserved as originals but are not automatically promoted into a typed table. New, removed, renamed, or structurally changed fields require deliberate detection, schema review, and code changes. A production extension should alert on drift, preserve unexpected fields or records, and define whether each change is compatible, quarantined, or blocking.

The full source snapshot is intentionally retained to make replay and audit possible. Retention and cleanup are not automatic: interrupted ingestion may leave a staging directory, and a production deployment should add a reviewed retention policy that never removes the latest committed or unprocessed snapshot.

## Validation and deployment record

Local validation passed with 13 unit tests and Python compilation, and `databricks bundle validate -t dev` succeeded. The environment setup job completed successfully, and deployed workflow run `188330031364843` completed ingestion, Bronze, Silver, Gold, and finalization successfully. The committed snapshot is `20261106182546392457`; its processing marker records `SUCCESS` with `audit_finalized: true`. Audit verification covered 12 BLS files and 1 DataUSA response; the finalizer reconciled 139,292 BLS Bronze rows and 11 population rows. That run's workspace had 13 source-shaped Bronze tables (12 BLS and 1 DataUSA, including line-record views over the two text-only documents that were later retired), three curated Silver tables plus a Silver `quarantine` view, a Bronze `quarantine` view, and three Gold analytics tables. Each source-shaped Bronze table has `_snapshot_id` and `_ingested_at`, without per-row `_source_*` columns. Silver schema verification confirmed snapshot-preserving productivity and population facts and the SCD Type 2 series dimension. The Gold tables were later renamed to `population_statistics`, `productivity_annual_ranking`, and `productivity_population_comparison` and the workspace redeployed; the refreshed tables and pipeline runs are captured under [`SCREENSHOTS.md`](./SCREENSHOTS.md). SQL-versus-Gold comparisons for all three analytics returned zero rows in both difference directions.

Runtime findings from the first live deployments were fixed and the affected workflows rerun: Unity Catalog's prohibition on `input_file_name()` (replaced with `_metadata.file_path`), a workspace pipeline module that could not import the local `src` package (the DAB now supplies the project root), and audit row-count updates that must use the ingestion-attempt ID while the processing checkpoint uses the authoritative source snapshot ID.

For committed snapshot `20261109103858915519` (forced run `583718480677949`), the per-file audit counts reconcile exactly with the Bronze rows carrying that `_snapshot_id`:

| Source file | Audit `records_downloaded` | Bronze rows for the snapshot |
| --- | ---: | ---: |
| `pr.class` | 2 | 2 |
| `pr.data.0.Current` | 46,159 | 46,159 |
| `pr.data.1.AllData` | 92,328 | 92,328 |
| `pr.duration` | 4 | 4 |
| `pr.footnote` | 1 | 1 |
| `pr.measure` | 22 | 22 |
| `pr.period` | 5 | 5 |
| `pr.seasonal` | 2 | 2 |
| `pr.sector` | 6 | 6 |
| `pr.series` | 337 | 337 |
| `population.json` | 11 | 11 |
| **Total** | **138,877** | **138,877** |

The directory's two text-only documents, `pr.contacts` (11 lines) and `pr.txt` (279 lines), add 290 downloaded records with no Bronze table by design (plain-text reference documents; see the Bronze section), so the full inventory is 139,167 validated source records.

After the Silver expectations were moved from hand-written prefilters to declarative `@dp.expect_all_or_drop`, forced run `583718480677949` re-ingested and refreshed all three pipelines successfully. Both quarantine tables reported zero rows, curated Silver datasets retained one dataset per committed snapshot, and `bls_series` collapsed the three identical snapshots into one observed metadata version per series, confirming the SCD derivation and the retention design live.

Current workspace composition, verified against `information_schema.tables` for snapshot `20261109103858915519`: `data_quest_bronze` holds 11 source-shaped tables (10 BLS and 1 DataUSA) plus the `quarantine` view and the `pipeline_job_details` audit table; `data_quest_silver` holds the three curated tables plus `quarantine`; `data_quest_gold` holds the three analytics tables plus three `*_current` views. Each schema also contains Lakeflow SDP service objects (`event_log_*` and internal `__materialization_mat_*` tables), including materializations left behind by retired dataset names; these are pipeline bookkeeping artifacts inherited from earlier iterations and are not part of the published data model.

After the ranking was revised to follow the assessment example literally (all observed Q01–Q04 sums are ranked, with `is_full_calendar_year` marking complete years), a full Gold refresh (`update 1801f35a-426d-4842-a97b-ea2f440a5e9a`) recomputed every committed snapshot from Silver. The revised PySpark output matches the revised [`sql/analysis_2.sql`](./sql/analysis_2.sql) row-for-row over all 1,148 ranking rows: zero missing, zero extra, and zero value differences, including on the 16 series–snapshot groups with tied peak years. The one series the revision affects, `PRS32006212`, now reports its 1987 winner (18.7 from three quarters, `is_full_calendar_year = false`) instead of dropping it, and the population and comparison tables recomputed identically (mean 322,069,808.00, population standard deviation 3,796,119.94 from six observations, and 39 Q01 rows per snapshot with 11 population-matched years). The three `*_current` views in Gold were created once after this refresh using the statement shown in the Gold section.

## AI usage

I used the GitHub Copilot free plan and ChatGPT while working through the assessment. I first used AI assistance to understand the sources and to shape the local source profiler, then reviewed the profile output and source documentation to confirm the actual fields, layouts, values, and data-quality concerns. I had an initial architecture in mind and used ChatGPT to challenge and refine the design and trade-offs.

I then used GitHub Copilot to help draft the DAB and pipeline implementation from that design. I configured the Databricks CLI locally to connect to the workspace and used the agent to deploy, diagnose pipeline issues, and iterate on the implementation. AI output was not treated as proof of correctness: I checked it against the source files and profile, unit tests, bundle validation, and Databricks pipeline and query results. I reviewed and corrected runtime issues as they surfaced, including workspace pipeline import behavior and Unity Catalog compatibility.

## Retrospective

The hardest part was making source change detection, pipeline processing, and lineage behave as one recoverable unit. A successful download is not enough: a partial attempt must not become the latest source snapshot, and a committed snapshot must remain retryable if one of the downstream pipelines fails. Separating the source commit marker from the processing marker, recording per-file audit rows, and carrying `_snapshot_id` through Bronze and Silver made those states explicit.

Profiling the real source files before modeling was also important. It confirmed the BLS TSV schemas, showed why the current and historical productivity files must remain separate, and helped distinguish quarterly observations from the annual-average period. That groundwork made the Silver types and the three Gold calculations easier to explain and validate.

The project was validated locally with the ingestion unit tests, Python compilation, and DAB bundle validation. The recorded Databricks validation includes a successful end-to-end workflow through ingestion, Bronze, Silver, Gold, and finalization; source-file audit/count reconciliation; and SQL comparisons against the Gold analytics with no row differences. These results validate the current implementation in the tested workspace, but do not guarantee that another workspace has the same permissions or platform behavior.

Two structural choices are deliberate simplifications worth recording as honest trade-offs. The Bronze, Silver, and Gold layers run as three separate Spark Declarative Pipelines chained as job tasks instead of one pipeline with `LIVE` references to each other: that keeps each layer's lifecycle, change frequency, and ownership independent, which matches the multi-team medallion pattern these layers model, while giving up a single end-to-end DAG, unified cross-layer flow metrics in one pipeline view, and some container start-up overhead. A single-team build would more naturally use one combined pipeline. Similarly, Bronze reads every stored snapshot copy of each file through a wildcard path, so a refresh reprocesses all history for each table. That is simple and correct as a batch materialized view and satisfies the rerun requirement at the ingestion level, but the production evolution is a streaming table fed by Auto Loader so that only newly committed files are scanned and unchanged data is never reprocessed.

Transformation unit tests were deliberately deferred to stay within the assessment window: the pure-Python ingestion logic is the only part that runs outside Databricks, and it is fully unit-tested (the pipeline modules import the workspace `dp` module, so they fit workspace validation better than local pytest). Transformation correctness was instead validated with SQL-versus-PySpark comparisons of all three analytics and live pipeline runs, which is recorded in the validation record.

With more time, I would add a deliberate schema-drift and alerting policy, increase isolated tests for transformation edge cases, automate SQL/PySpark result comparison, and assess source-level incremental refreshes and retention. Sanitized screenshots of ingestion, the Volume, the pipeline runs and tables, Genie-agent answers, and each analytical output are captured under `screenshots/` and indexed in [`SCREENSHOTS.md`](./SCREENSHOTS.md).
