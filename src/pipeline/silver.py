from __future__ import annotations

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


BRONZE_SCHEMA = "rearc_catalog.data_quest_bronze"

PRODUCTIVITY_EXPECTATIONS = {
    "observation_key_required": (
        "series_id IS NOT NULL AND series_id <> '' AND year IS NOT NULL "
        "AND period IS NOT NULL AND _snapshot_id IS NOT NULL"
    ),
    "year_is_valid": "year IS NOT NULL AND year > 0",
    "period_is_valid": "period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')",
    "value_is_numeric": "value IS NOT NULL",
}

SERIES_EXPECTATIONS = {
    "series_identifier_required": "series_id IS NOT NULL AND series_id <> ''",
    "attribute_hash_required": "attribute_hash IS NOT NULL",
    "series_begin_year_valid": "begin_year IS NULL OR begin_year > 0",
}

POPULATION_EXPECTATIONS = {
    "nation_id_required": "nation_id IS NOT NULL AND nation_id <> ''",
    "nation_required": "nation IS NOT NULL AND nation <> ''",
    "snapshot_key_required": (
        "year IS NOT NULL AND year > 0 AND _snapshot_id IS NOT NULL"
    ),
    "population_is_numeric": "population IS NOT NULL",
    "population_non_negative": "population >= 0",
}

SNAPSHOT_COLUMN_COMMENTS = {
    "_snapshot_id": "Identifier of the committed coordinated source snapshot.",
    "_ingested_at": "Timestamp the source file was downloaded for this snapshot.",
}

QUARANTINE_COLUMN_COMMENTS = {
    "source_name": "Source system from which the rejected record originated.",
    "source_table": "Silver dataset whose expectation the record failed.",
    "source_file": "Bronze source file associated with the rejected record.",
    "raw_record": "Serialized source record before Silver type conversion.",
    "rejection_reason": "Names of all failed Silver expectations.",
    "rejected_at": "Timestamp when the record was routed to Silver quarantine.",
    **SNAPSHOT_COLUMN_COMMENTS,
}

PRODUCTIVITY_COLUMN_COMMENTS = {
    "series_id": "BLS identifier for the specific productivity time series.",
    "year": "Calendar year of the data observation, parsed as an integer.",
    "period": "Quarter or annual-average period code for the observation.",
    "value": "Source-reported productivity value parsed as a decimal.",
    "footnote_codes": "Comma-separated BLS footnote codes for the observation.",
    **SNAPSHOT_COLUMN_COMMENTS,
}

SERIES_COLUMN_COMMENTS = {
    "series_id": "BLS identifier composed from survey and series dimension codes.",
    "sector_code": "BLS code identifying the economic sector.",
    "sector_name": "Human-readable economic-sector name from the BLS lookup.",
    "class_code": "BLS code identifying the employee group.",
    "class_description": "Human-readable employee-group description.",
    "measure_code": "BLS code identifying the factor measured.",
    "measure_description": "Human-readable description of the measured factor.",
    "duration_code": "BLS code identifying the duration or comparison basis.",
    "duration_description": "Human-readable description of the duration basis.",
    "seasonal": "BLS code indicating seasonal-adjustment status.",
    "seasonal_description": "Human-readable seasonal-adjustment status.",
    "series_description": "Combined human-readable sector, class, measure, duration, and seasonal description.",
    "source_series_title": "Title supplied in the BLS series metadata file.",
    "base_year": "Index base year supplied by BLS, when applicable.",
    "footnote_codes": "Comma-separated BLS footnote codes applying to the series.",
    "begin_year": "First year for which observations are available.",
    "begin_period": "First observation period within begin_year.",
    "end_year": "Last year for which observations are available.",
    "end_period": "Last observation period within end_year.",
    "valid_from_snapshot_id": "First observed snapshot for this metadata version.",
    "valid_to_snapshot_id": "Snapshot where the next metadata version was first observed; null for the current version.",
    "is_current": "True when this is the latest observed metadata version.",
    "attribute_hash": "SHA-256 fingerprint of tracked series attributes used to detect changes.",
    **SNAPSHOT_COLUMN_COMMENTS,
}

POPULATION_COLUMN_COMMENTS = {
    "nation_id": "DataUSA identifier for the nation.",
    "nation": "DataUSA human-readable nation name.",
    "year": "Calendar year associated with the population estimate, parsed as an integer.",
    "population": "Annual population count parsed as a whole number.",
    **SNAPSHOT_COLUMN_COMMENTS,
}


def _schema_with_comments(
    fields: list[tuple[str, str]], comments: dict[str, str]
) -> str:
    field_names = {name for name, _ in fields}
    missing_comments = field_names - comments.keys()
    if missing_comments:
        raise ValueError(
            f"Missing column comments for: {', '.join(sorted(missing_comments))}"
        )
    schema_fields = []
    for name, data_type in fields:
        comment = comments[name].replace("'", "''")
        schema_fields.append(f"`{name}` {data_type} COMMENT '{comment}'")
    return ",\n".join(schema_fields)


PRODUCTIVITY_SCHEMA = _schema_with_comments(
    [
        ("series_id", "STRING"),
        ("year", "INT"),
        ("period", "STRING"),
        ("value", "DECIMAL(20, 6)"),
        ("footnote_codes", "STRING"),
        ("_snapshot_id", "STRING"),
        ("_ingested_at", "TIMESTAMP"),
    ],
    PRODUCTIVITY_COLUMN_COMMENTS,
)

SERIES_SCHEMA = _schema_with_comments(
    [
        ("series_id", "STRING"),
        ("sector_code", "STRING"),
        ("sector_name", "STRING"),
        ("class_code", "STRING"),
        ("class_description", "STRING"),
        ("measure_code", "STRING"),
        ("measure_description", "STRING"),
        ("duration_code", "STRING"),
        ("duration_description", "STRING"),
        ("seasonal", "STRING"),
        ("seasonal_description", "STRING"),
        ("series_description", "STRING"),
        ("source_series_title", "STRING"),
        ("base_year", "STRING"),
        ("footnote_codes", "STRING"),
        ("begin_year", "INT"),
        ("begin_period", "STRING"),
        ("end_year", "INT"),
        ("end_period", "STRING"),
        ("valid_from_snapshot_id", "STRING"),
        ("valid_to_snapshot_id", "STRING"),
        ("is_current", "BOOLEAN"),
        ("attribute_hash", "STRING"),
        ("_snapshot_id", "STRING"),
        ("_ingested_at", "TIMESTAMP"),
    ],
    SERIES_COLUMN_COMMENTS,
)

POPULATION_SCHEMA = _schema_with_comments(
    [
        ("nation_id", "STRING"),
        ("nation", "STRING"),
        ("year", "INT"),
        ("population", "BIGINT"),
        ("_snapshot_id", "STRING"),
        ("_ingested_at", "TIMESTAMP"),
    ],
    POPULATION_COLUMN_COMMENTS,
)

QUARANTINE_SCHEMA = _schema_with_comments(
    [
        ("source_name", "STRING"),
        ("source_table", "STRING"),
        ("source_file", "STRING"),
        ("raw_record", "STRING"),
        ("rejection_reason", "STRING"),
        ("rejected_at", "TIMESTAMP"),
        ("_snapshot_id", "STRING"),
        ("_ingested_at", "TIMESTAMP"),
    ],
    QUARANTINE_COLUMN_COMMENTS,
)


def _read_bronze(table_name: str) -> DataFrame:
    return spark.read.table(f"{BRONZE_SCHEMA}.{table_name}")


def _record_json(dataframe: DataFrame, columns: list[str]) -> Column:
    return F.to_json(F.struct(*[F.col(column) for column in columns]))


def _passes_expectations(
    dataframe: DataFrame, expectations: dict[str, str]
) -> DataFrame:
    valid = reduce(
        lambda left, right: left & right,
        [
            F.coalesce(F.expr(condition), F.lit(False))
            for condition in expectations.values()
        ],
    )
    return dataframe.filter(valid)


def _quarantine_rows(
    dataframe: DataFrame,
    source_name: str,
    source_table: str,
    source_file: str,
    expectations: dict[str, str],
) -> DataFrame:
    failed_rules = [
        F.when(
            ~F.coalesce(F.expr(condition), F.lit(False)),
            F.lit(rule_name),
        )
        for rule_name, condition in expectations.items()
    ]
    return (
        dataframe.withColumn(
            "_rejection_reason", F.concat_ws(", ", *failed_rules)
        )
        .filter(F.col("_rejection_reason") != "")
        .select(
            F.lit(source_name).alias("source_name"),
            F.lit(source_table).alias("source_table"),
            F.lit(source_file).alias("source_file"),
            F.col("_raw_record").alias("raw_record"),
            F.col("_rejection_reason").alias("rejection_reason"),
            F.current_timestamp().alias("rejected_at"),
            "_snapshot_id",
            "_ingested_at",
        )
    )


def _productivity_source() -> DataFrame:
    source_columns = [
        "series_id",
        "year",
        "period",
        "value",
        "footnote_codes",
    ]
    raw_productivity = _read_bronze("pr_data_1_alldata")
    return raw_productivity.select(
        *[
            F.trim(F.col(column)).alias(column)
            for column in source_columns
            if column not in {"year", "value"}
        ],
        F.expr("try_cast(trim(year) AS INT)").alias("year"),
        F.expr("try_cast(trim(value) AS DECIMAL(20, 6))").alias("value"),
        _record_json(raw_productivity, source_columns).alias("_raw_record"),
        "_snapshot_id",
        "_ingested_at",
    )


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


def _series_source() -> DataFrame:
    source_columns = [
        "series_id",
        "sector_code",
        "class_code",
        "measure_code",
        "duration_code",
        "seasonal",
        "base_year",
        "footnote_codes",
        "series_title",
        "begin_year",
        "begin_period",
        "end_year",
        "end_period",
    ]
    raw_series = _read_bronze("pr_series")
    series = raw_series.select(
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
        _record_json(raw_series, source_columns).alias("_raw_record"),
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
    return enriched.withColumn(
        "attribute_hash",
        F.sha2(F.to_json(F.struct(*[F.col(name) for name in tracked_attributes])), 256),
    )


def _population_source() -> DataFrame:
    raw_population = _read_bronze("population")
    source_columns = ["nation_id", "nation", "year", "population"]
    return raw_population.select(
        F.trim("nation_id").alias("nation_id"),
        F.trim("nation").alias("nation"),
        F.expr("try_cast(trim(year) AS INT)").alias("year"),
        F.expr(
            "try_cast(try_cast(trim(population) AS DECIMAL(20, 1)) AS BIGINT)"
        ).alias("population"),
        _record_json(raw_population, source_columns).alias("_raw_record"),
        "_snapshot_id",
        "_ingested_at",
    )


@dp.materialized_view(
    name="bls_productivity",
    comment="Typed BLS productivity observations preserved by source snapshot.",
    schema=PRODUCTIVITY_SCHEMA,
)
@dp.expect(
    "observation_key_required", PRODUCTIVITY_EXPECTATIONS["observation_key_required"]
)
@dp.expect("year_is_valid", PRODUCTIVITY_EXPECTATIONS["year_is_valid"])
@dp.expect(
    "period_is_valid", PRODUCTIVITY_EXPECTATIONS["period_is_valid"]
)
@dp.expect("value_is_numeric", PRODUCTIVITY_EXPECTATIONS["value_is_numeric"])
def bls_productivity():
    source = _passes_expectations(_productivity_source(), PRODUCTIVITY_EXPECTATIONS)
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
    schema=SERIES_SCHEMA,
)
@dp.expect(
    "series_identifier_required",
    SERIES_EXPECTATIONS["series_identifier_required"],
)
@dp.expect("attribute_hash_required", SERIES_EXPECTATIONS["attribute_hash_required"])
@dp.expect(
    "series_begin_year_valid", SERIES_EXPECTATIONS["series_begin_year_valid"]
)
def bls_series():
    versioned = _passes_expectations(_series_source(), SERIES_EXPECTATIONS)
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
    series_history = (
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
    return series_history


@dp.materialized_view(
    name="population",
    comment="Typed annual population facts retained by source snapshot.",
    schema=POPULATION_SCHEMA,
)
@dp.expect("nation_id_required", POPULATION_EXPECTATIONS["nation_id_required"])
@dp.expect("nation_required", POPULATION_EXPECTATIONS["nation_required"])
@dp.expect(
    "snapshot_key_required", POPULATION_EXPECTATIONS["snapshot_key_required"]
)
@dp.expect("population_is_numeric", POPULATION_EXPECTATIONS["population_is_numeric"])
@dp.expect(
    "population_non_negative", POPULATION_EXPECTATIONS["population_non_negative"]
)
def population():
    source = _passes_expectations(_population_source(), POPULATION_EXPECTATIONS)
    return source.select(
        "nation_id",
        "nation",
        "year",
        "population",
        "_snapshot_id",
        "_ingested_at",
    )


@dp.materialized_view(
    name="quarantine",
    comment="Silver source records rejected by one or more data quality expectations.",
    schema=QUARANTINE_SCHEMA,
)
def quarantine():
    rejected_frames = [
        _quarantine_rows(
            _productivity_source(),
            "BLS",
            "bls_productivity",
            "pr.data.1.AllData",
            PRODUCTIVITY_EXPECTATIONS,
        ),
        _quarantine_rows(
            _series_source(),
            "BLS",
            "bls_series",
            "pr.series",
            SERIES_EXPECTATIONS,
        ),
        _quarantine_rows(
            _population_source(),
            "DataUSA",
            "population",
            "population.json",
            POPULATION_EXPECTATIONS,
        ),
    ]
    return reduce(
        lambda left, right: left.unionByName(right),
        rejected_frames,
    )
