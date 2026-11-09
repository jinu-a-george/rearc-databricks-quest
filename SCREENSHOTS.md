# Rearc Data Quest: Databricks Screenshots

This page brings together the Databricks evidence for the Data Quest implementation. The screenshots are grouped by the data flow: Unity Catalog and source landing, Bronze, Silver, Gold analytics, and workflow execution. Captions describe what each screenshot shows.

## Unity Catalog and Bronze

The project stores source snapshots in a Unity Catalog Volume and loads source-shaped Bronze tables into the Bronze schema.

### Catalog and Bronze tables

![Unity Catalog catalog root](screenshots/01-unity_catalog-rearc_catalog_root.png)

*Unity Catalog catalog root used by the project.*

![Bronze schema tables](screenshots/02-unity_catalog-data_quest_bronze_tables.png)

*Tables registered in the Bronze schema.*

### Raw source snapshots and manifest

![Raw data Volume root](screenshots/09.1-unity_catalog-bronze.raw_data_volume_root.png)

*Root of the Volume containing the landed source data.*

![BLS snapshot directory](screenshots/09.2-unity_catalog-bronze.raw_data_volume_bls_snapshot.png)

*BLS snapshot directory in the Volume.*

![BLS snapshot files](screenshots/09.3-unity_catalog-bronze.raw_data_volume_bls_snapshot_files.png)

*Discovered BLS source files preserved in the snapshot.*

![Population snapshot directory](screenshots/09.4-unity_catalog-bronze.raw_data_volume_population_snapshot.png)

*DataUSA population snapshot directory.*

![Population snapshot files](screenshots/09.5-unity_catalog-bronze.raw_data_volume_population_snapshot_files.png)

*Population source file preserved in the snapshot.*

![Manifest files](screenshots/09.6-unity_catalog-bronze.raw_data_volume_manifest_files.png)

*Ingestion manifest files associated with the source snapshots.*

![Manifest content](screenshots/09.7-unity_catalog-bronze.raw_data_volume_manifest_file_content.png)

*Example manifest content with source snapshot details.*

![Latest snapshot pointer](screenshots/09.8-unity_catalog-bronze.raw_data_volume_latest_json_file_content.png)

*Latest committed snapshot pointer.*

### Bronze tables and ingestion audit

![BLS productivity Bronze columns](screenshots/03.1-unity_catalog-bronze.pr_data_1_alldata_table_columns.png)

*Schema of the historical BLS productivity Bronze table.*

![BLS productivity Bronze records](screenshots/03.1-unity_catalog-bronze.pr_data_1_alldata_table_data.png)

*Sample records from the historical BLS productivity table.*

![BLS series Bronze columns](screenshots/04.1-unity_catalog-bronze.pr_series_table_columns.png)

*Schema of the BLS series metadata table.*

![BLS series records, first view](screenshots/04.2-unity_catalog-bronze.pr_series_table_data_01.png)

*BLS series metadata records.*

![BLS series records, second view](screenshots/04.2-unity_catalog-bronze.pr_series_table_data_02.png)

*Additional BLS series metadata records.*

![BLS sector lookup](screenshots/05-unity_catalog-bronze.pr_sector_table.png)

*BLS sector lookup table.*

![Population Bronze table](screenshots/06-unity_catalog-bronze.population_table.png)

*DataUSA population records in Bronze.*

![Ingestion audit table schema](screenshots/07.1-unity_catalog-bronze.pipeline_job_details_table_columns.png)

*Columns recorded for ingestion and pipeline job details.*

![Ingestion audit records, first view](screenshots/07.2-unity_catalog-bronze.pipeline_job_details_table_data_01.png)

*Ingestion audit records showing source processing details.*

![Ingestion audit records, second view](screenshots/07.2-unity_catalog-bronze.pipeline_job_details_table_data_02.png)

*Additional source processing and status records.*

![Ingestion audit records, third view](screenshots/07.2-unity_catalog-bronze.pipeline_job_details_table_data_03.png)

*Further ingestion audit records.*

![Bronze quarantine table](screenshots/08-unity_catalog-bronze.quarantine_table_columns.png)

*Schema of the Bronze `quarantine` table that exposes malformed BLS records and missing lookup identifiers.*

## Silver tables

Silver applies typed, validated data structures and enriches productivity observations with descriptive series metadata.

![Bronze schema navigation before Silver tables](screenshots/10.1-unity_catalog-data_quest_bronze_tables.png)

*Unity Catalog table navigation around the Bronze and Silver datasets.*

![Silver productivity columns](screenshots/10.2-unity_catalog-data_quest_silver.bls_productivity_table_columns.png)

*Schema of the curated BLS productivity table.*

![Silver productivity records](screenshots/10.3-unity_catalog-data_quest_silver.bls_productivity_table_data.png)

*Typed BLS productivity observations in Silver.*

![Silver series metadata, first view](screenshots/10.4-unity_catalog-data_quest_silver.bls_series_table_data_01.png)

*Enriched BLS series metadata.*

![Silver series metadata, second view](screenshots/10.4-unity_catalog-data_quest_silver.bls_series_table_data_02.png)

*Additional enriched series records.*

![Silver series metadata, third view](screenshots/10.4-unity_catalog-data_quest_silver.bls_series_table_data_03.png)

*Further series metadata and descriptive fields.*

![Silver series metadata, fourth view](screenshots/10.4-unity_catalog-data_quest_silver.bls_series_table_data_04.png)

*Additional rows from the Silver series table.*

![Silver population columns](screenshots/10.5-unity_catalog-data_quest_silver.population_table_columns.png)

*Schema of the typed annual population table.*

![Silver population records](screenshots/10.5-unity_catalog-data_quest_silver.population_table_data.png)

*Annual population records in Silver.*

## Gold analytics and access

Gold contains business-ready outputs for the population summary, annual productivity ranking, and productivity/population comparison.

![Gold schema and Unity Catalog access](screenshots/11.1-unity_catalog-data_quest_gold_schema_unity_access_control.png)

*Gold schema and its Unity Catalog access configuration.*

![Gold tables](screenshots/11.1-unity_catalog-data_quest_gold_tables.png)

*The three analytical Gold tables.*

### Population statistics

![Population statistics columns](screenshots/11.2-unity_catalog-data_quest_gold.population_statistics_table_columns.png)

*Output schema for the annual population statistics.*

![Population statistics result](screenshots/11.2-unity_catalog-data_quest_gold.population_statistics_table_data.png)

*Population mean and standard deviation output for the requested period.*

![Population statistics current view](screenshots/11.2-unity_catalog-data_quest_gold.population_statistics_current_view.png)

*Current Gold view definition, filtered to the latest snapshot of the population statistics table.*

### Highest annual productivity per series

![Annual productivity ranking columns](screenshots/11.3-unity_catalog-data_quest_gold.productivity_annual_ranking_table_columns.png)

*Output schema for the per-series annual productivity ranking.*

![Annual productivity ranking current view](screenshots/11.3-unity_catalog-data_quest_gold.productivity_annual_ranking_current_view.png)

*Current Gold view definition, filtered to the latest snapshot of the annual ranking table.*

![Annual productivity ranking result, first view](screenshots/11.3-unity_catalog-data_quest_gold.productivity_annual_ranking_table_data_01.png)

*Selected highest annual productivity totals with series descriptions.*

![Annual productivity ranking result, second view](screenshots/11.3-unity_catalog-data_quest_gold.productivity_annual_ranking_table_data_02.png)

*Additional ranking output columns, including quarterly coverage and snapshot details.*

### Productivity with population

![Productivity-population comparison columns](screenshots/11.4-unity_catalog-data_quest_gold.productivity_population_comparison_table_columns.png)

*Output schema for the target productivity-series and population comparison.*

![Productivity-population comparison current view](screenshots/11.4-unity_catalog-data_quest_gold.productivity_population_comparison_current_view.png)

*Current Gold view definition, filtered to the latest snapshot of the comparison table.*

![Productivity-population comparison, first view](screenshots/11.4-unity_catalog-data_quest_gold.productivity_population_comparison_table_data_01.png)

*Comparison output showing productivity observations and available population.*

![Productivity-population comparison, second view](screenshots/11.4-unity_catalog-data_quest_gold.productivity_population_comparison_table_data_02.png)

*Additional comparison rows.*

![Productivity-population comparison, third view](screenshots/11.4-unity_catalog-data_quest_gold.productivity_population_comparison_table_data_03.png)

*Additional comparison rows across years.*

![Productivity-population comparison, fourth view](screenshots/11.4-unity_catalog-data_quest_gold.productivity_population_comparison_table_data_04.png)

*Further comparison rows, including years with population data where available.*

## Databricks jobs and pipeline runs

These screenshots show the deployed jobs, the environment setup task, and the multi-step data pipeline workflow.

![Data Quest jobs list](screenshots/12-job-all_dataquest_jobs.png)

*Databricks jobs configured for the Data Quest project.*

![Environment setup job](screenshots/13.1-job-environment_setup_job.png)

*Environment setup job configuration.*

![Environment setup job timeline](screenshots/13.2-job-environment_setup_job_timeline.png)

*Environment setup job run history and task timeline.*

![Data pipeline job runs](screenshots/14.1-job-data_pipeline_job.png)

*Data pipeline job runs and execution status.*

![Data pipeline job tasks](screenshots/14.2-job-data_pipeline_job_task_list.png)

*Tasks that make up the ingestion and Bronze-to-Gold workflow.*

### Lakeflow pipeline runs

![Configured ETL pipelines](screenshots/15.1-pipeline-bronze_silver_gold_etl_pipelines.png)

*Bronze, Silver, and Gold ETL pipelines configured in Databricks.*

![Bronze pipeline run](screenshots/15.2-pipeline-bronze_layer_pipeline.png)

*Completed Bronze pipeline run and its source-table outputs.*

![Silver pipeline run](screenshots/15.3-pipeline-silver_layer_pipeline.png)

*Completed Silver pipeline run, showing curated tables and data quality expectations.*

## Genie Agent (bonus)

The Genie Agent is configured to answer questions about the Gold productivity and population datasets. Its configuration and rationale are documented in `PROCESS.md` and `README.md`.

![Genie Agent overview](screenshots/16.1-genie_agent-overview.png)

*Agent description and suggested questions for the productivity and population data.*

![Genie Agent data sources](screenshots/16.2-genie_agent-data_sources.png)

*Gold analytical tables connected as the agent's data sources.*

![Genie Agent instructions](screenshots/16.3-genie_agent-instructions.png)

*Guidance for interpreting snapshots, pre-aggregated measures, tied peak years, and dataset scope.*

![Genie population statistics answer](screenshots/16.4-genie_agent-population_statistics_chat.png)

*Agent response reporting the 2013–2018 population mean and population standard deviation.*

![Genie annual ranking answer](screenshots/16.5-genie_agent-annual_ranking_chat_01.png)

*Agent response summarizing the highest annual productivity values across BLS series.*

![Genie annual ranking sample](screenshots/16.5-genie_agent-annual_ranking_chat_02.png)

*Sample series rankings with human-readable descriptions, peak years, and annual values.*

![Genie tied peak years answer](screenshots/16.6-genie_agent-tied_peak_years_chat_01.png)

*Agent identifies BLS series with multiple years tied for their highest annual value.*

![Genie tied peak years details](screenshots/16.6-genie_agent-tied_peak_years_chat_02.png)

*Tied peak years and values, with brief descriptions of the affected series.*
