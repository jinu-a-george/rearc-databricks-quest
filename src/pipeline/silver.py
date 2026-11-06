from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


BRONZE_SCHEMA = "rearc_catalog.data_quest_bronze"


def _read_bronze(table_name: str) -> DataFrame:
    return spark.read.table(f"{BRONZE_SCHEMA}.{table_name}")


def _lookup(
    table_name: str,
    code_column: str,
    description_column: str,
    output_code: str,
    output_description: str,
) -> DataFrame:
    return _read_bronze(table_name).select(
        F.trim(F.col(code_column)).alias(output_code),
        F.trim(F.col(description_column)).alias(output_description),
        "_snapshot_id",
    )


@dp.materialized_view(
    name="bls_productivity",
    comment="Typed BLS productivity observations preserved by source snapshot.",
)
@dp.expect(
    "observation_key_required",
    "series_id IS NOT NULL AND series_id <> '' AND year IS NOT NULL "
    "AND period IS NOT NULL AND _snapshot_id IS NOT NULL",
)
@dp.expect("year_is_valid", "year IS NOT NULL AND year > 0")
@dp.expect(
    "period_is_valid",
    "period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')",
)
@dp.expect("value_is_numeric", "value IS NOT NULL")
def bls_productivity():
    source = _read_bronze("pr_data_1_alldata").select(
        F.trim("series_id").alias("series_id"),
        F.expr("try_cast(trim(year) AS INT)").alias("year"),
        F.trim("period").alias("period"),
        F.expr("try_cast(trim(value) AS DECIMAL(20, 6))").alias("value"),
        F.trim("footnote_codes").alias("footnote_codes"),
        "_snapshot_id",
        "_ingested_at",
    )
    return source.select(
        "series_id",
        "year",
        "period",
        "value",
        "footnote_codes",
        "_snapshot_id",
        "_ingested_at",
    )


@dp.materialized_view(
    name="bls_series",
    comment="Snapshot-observed SCD Type 2 versions of enriched BLS series metadata.",
)
@dp.expect("series_identifier_required", "series_id IS NOT NULL AND series_id <> ''")
@dp.expect("attribute_hash_required", "attribute_hash IS NOT NULL")
@dp.expect(
    "series_begin_year_valid",
    "begin_year IS NULL OR begin_year > 0",
)
def bls_series():
    series = _read_bronze("pr_series").select(
        F.trim("series_id").alias("series_id"),
        F.trim("sector_code").alias("sector_code"),
        F.trim("class_code").alias("class_code"),
        F.trim("measure_code").alias("measure_code"),
        F.trim("duration_code").alias("duration_code"),
        F.trim("seasonal").alias("seasonal"),
        F.trim("base_year").alias("base_year"),
        F.trim("footnote_codes").alias("footnote_codes"),
        F.trim("series_title").alias("source_series_title"),
        F.expr("try_cast(trim(begin_year) AS INT)").alias("begin_year"),
        F.trim("begin_period").alias("begin_period"),
        F.expr("try_cast(trim(end_year) AS INT)").alias("end_year"),
        F.trim("end_period").alias("end_period"),
        "_snapshot_id",
        "_ingested_at",
    )
    sector = _lookup(
        "pr_sector", "sector_code", "sector_name", "sector_code", "sector_name"
    )
    class_lookup = _lookup(
        "pr_class", "class_code", "class_text", "class_code", "class_description"
    )
    measure = _lookup(
        "pr_measure",
        "measure_code",
        "measure_text",
        "measure_code",
        "measure_description",
    )
    duration = _lookup(
        "pr_duration",
        "duration_code",
        "duration_text",
        "duration_code",
        "duration_description",
    )
    seasonal = _lookup(
        "pr_seasonal",
        "Seasonal_code",
        "Seasonal_text",
        "seasonal",
        "seasonal_description",
    )

    enriched = (
        series.join(sector, on=["_snapshot_id", "sector_code"], how="left")
        .join(class_lookup, on=["_snapshot_id", "class_code"], how="left")
        .join(measure, on=["_snapshot_id", "measure_code"], how="left")
        .join(duration, on=["_snapshot_id", "duration_code"], how="left")
        .join(seasonal, on=["_snapshot_id", "seasonal"], how="left")
        .withColumn(
            "series_description",
            F.concat_ws(
                " - ",
                "sector_name",
                "class_description",
                "measure_description",
                "duration_description",
                "seasonal_description",
            ),
        )
    )
    tracked_attributes = [
        "sector_code",
        "sector_name",
        "class_code",
        "class_description",
        "measure_code",
        "measure_description",
        "duration_code",
        "duration_description",
        "seasonal",
        "seasonal_description",
        "series_description",
        "source_series_title",
        "base_year",
        "footnote_codes",
        "begin_year",
        "begin_period",
        "end_year",
        "end_period",
    ]
    versioned = enriched.withColumn(
        "attribute_hash",
        F.sha2(F.to_json(F.struct(*[F.col(name) for name in tracked_attributes])), 256),
    )
    series_history = Window.partitionBy("series_id").orderBy("_snapshot_id")
    changes = (
        versioned.withColumn(
            "_previous_attribute_hash",
            F.lag("attribute_hash").over(series_history),
        )
        .filter(
            F.col("_previous_attribute_hash").isNull()
            | (F.col("attribute_hash") != F.col("_previous_attribute_hash"))
        )
        .drop("_previous_attribute_hash")
    )
    changed_versions = Window.partitionBy("series_id").orderBy("_snapshot_id")
    return (
        changes.withColumn(
            "valid_from_snapshot_id", F.col("_snapshot_id")
        )
        .withColumn(
            "valid_to_snapshot_id", F.lead("_snapshot_id").over(changed_versions)
        )
        .withColumn("is_current", F.col("valid_to_snapshot_id").isNull())
        .select(
            "series_id",
            "sector_code",
            "sector_name",
            "class_code",
            "class_description",
            "measure_code",
            "measure_description",
            "duration_code",
            "duration_description",
            "seasonal",
            "seasonal_description",
            "series_description",
            "source_series_title",
            "base_year",
            "footnote_codes",
            "begin_year",
            "begin_period",
            "end_year",
            "end_period",
            "valid_from_snapshot_id",
            "valid_to_snapshot_id",
            "is_current",
            "attribute_hash",
            "_snapshot_id",
            "_ingested_at",
        )
    )


@dp.materialized_view(
    name="population",
    comment="Typed annual population facts retained by source snapshot.",
)
@dp.expect("nation_id_required", "nation_id IS NOT NULL AND nation_id <> ''")
@dp.expect("nation_required", "nation IS NOT NULL AND nation <> ''")
@dp.expect(
    "snapshot_key_required",
    "year IS NOT NULL AND year > 0 AND _snapshot_id IS NOT NULL",
)
@dp.expect("population_is_numeric", "population IS NOT NULL")
@dp.expect("population_non_negative", "population >= 0")
def population():
    source = _read_bronze("population").select(
        F.trim("nation_id").alias("nation_id"),
        F.trim("nation").alias("nation"),
        F.expr("try_cast(trim(year) AS INT)").alias("year"),
        F.expr(
            "try_cast(try_cast(trim(population) AS DECIMAL(20, 1)) AS BIGINT)"
        ).alias("population"),
        "_snapshot_id",
        "_ingested_at",
    )
    return source.select(
        "nation_id",
        "nation",
        "year",
        "population",
        "_snapshot_id",
        "_ingested_at",
    )
