# Pipeline implementation status

`bronze.py` defines Lakeflow Spark Declarative Pipeline materialized views for all currently inspected BLS files and the DataUSA population response. Tables use source-oriented names (`pr_data_1_alldata`, `pr_series`, `population`) within `data_quest_bronze`; a redundant `bronze_` prefix is not used. The ingestion workflow downloads the original files first, then refreshes the pipeline. BLS delimited columns stay strings to preserve source values; DataUSA records use the source's string, integer, and double types, with field names normalized to lowercase snake case for Delta compatibility. The source text documents are exposed as line-based text tables. Bronze does source-format parsing only; quality expectations and business validation will be applied in Silver.

`analytics.py` contains the three PySpark analyses over the canonical Silver-layer schemas described in the root README. Matching Spark SQL queries are in `src/sql/`.

The tables are batch materialized views because the ingestion step refreshes complete source snapshots. Silver and Gold transformations remain pending.
