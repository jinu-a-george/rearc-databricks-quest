# Lakeflow pipeline implementation

The three pipeline modules define batch materialized views for the coordinated source snapshots downloaded into the Unity Catalog Volume. The ingestion workflow commits each snapshot before refreshing the pipelines.

- [`bronze.py`](./bronze.py) parses the discovered BLS tabular sources and DataUSA population response, preserving source values and snapshot metadata. It also exposes malformed or key-incomplete source records in Bronze quarantine.
- [`silver.py`](./silver.py) casts and validates productivity, series metadata, and population records; enriches series with BLS lookup descriptions; tracks observed series metadata versions; and routes rejected records to Silver quarantine.
- [`gold.py`](./gold.py) defines the three analytical materialized views using the reusable PySpark transformations in [`analytics.py`](./analytics.py). Equivalent Spark SQL is in [`../../sql/`](../../sql/).

The Gold outputs are population statistics, annual productivity rankings, and the target-series productivity/population comparison. Their snapshot-aware logic and validation behavior are described in the root [README](../../README.md).
