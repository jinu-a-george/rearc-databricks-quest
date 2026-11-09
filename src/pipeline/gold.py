from __future__ import annotations

import sys

from pyspark import pipelines as dp

PROJECT_ROOT = spark.conf.get("project_root")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.pipeline.analytics import (
    productivity_annual_ranking as calculate_productivity_annual_ranking,
    productivity_population_comparison as calculate_productivity_population_comparison,
    population_statistics as calculate_population_statistics,
)


SILVER_SCHEMA = "rearc_catalog.data_quest_silver"

POPULATION_STATISTICS_COLUMN_COMMENTS = {
    "nation_id": "DataUSA identifier for the nation.",
    "nation": "DataUSA human-readable nation name.",
    "start_year": "First year in the population statistics window, inclusive.",
    "end_year": "Last year in the population statistics window, inclusive.",
    "observation_count": "Number of unique, non-null annual observations used.",
    "mean_population": "Mean population when all six unique annual values are available.",
    "stddev_population": "Population standard deviation when all six annual values are available.",
    "calculation_method": "Standard deviation method used; STDDEV_POP treats the six years as the full population.",
    "_snapshot_id": "Coordinated source snapshot used for this result.",
    "_calculated_at": "Timestamp when this Gold result was calculated.",
}

PRODUCTIVITY_RANKING_COLUMN_COMMENTS = {
    "series_id": "BLS identifier for the productivity time series.",
    "series_description": "Human-readable description valid for the source snapshot.",
    "winning_year": "Year with the highest complete annual sum; ties are retained.",
    "annual_value": "Sum of the four quarterly productivity values.",
    "quarterly_observation_count": "Number of distinct quarterly observations included; always four.",
    "_snapshot_id": "Coordinated source snapshot used for this result.",
    "_calculated_at": "Timestamp when this Gold result was calculated.",
}

PRODUCTIVITY_POPULATION_COLUMN_COMMENTS = {
    "series_id": "BLS identifier for the selected productivity time series.",
    "series_description": "Human-readable description valid for the source snapshot.",
    "year": "Calendar year of the Q01 productivity observation.",
    "period": "BLS observation period; this output contains Q01.",
    "productivity_value": "Source-reported productivity value for the series and year.",
    "nation": "DataUSA nation associated with the population estimate, when available.",
    "population": "Annual population for the same year and source snapshot, when available.",
    "population_available": "True when a population row exists for the observation year and snapshot.",
    "_snapshot_id": "Coordinated source snapshot used for this result.",
    "_calculated_at": "Timestamp when this Gold result was calculated.",
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


POPULATION_STATISTICS_SCHEMA = _schema_with_comments(
    [
        ("nation_id", "STRING"),
        ("nation", "STRING"),
        ("start_year", "INT"),
        ("end_year", "INT"),
        ("observation_count", "INT"),
        ("mean_population", "DOUBLE"),
        ("stddev_population", "DOUBLE"),
        ("calculation_method", "STRING"),
        ("_snapshot_id", "STRING"),
        ("_calculated_at", "TIMESTAMP"),
    ],
    POPULATION_STATISTICS_COLUMN_COMMENTS,
)

PRODUCTIVITY_RANKING_SCHEMA = _schema_with_comments(
    [
        ("series_id", "STRING"),
        ("series_description", "STRING"),
        ("winning_year", "INT"),
        ("annual_value", "DOUBLE"),
        ("quarterly_observation_count", "INT"),
        ("_snapshot_id", "STRING"),
        ("_calculated_at", "TIMESTAMP"),
    ],
    PRODUCTIVITY_RANKING_COLUMN_COMMENTS,
)

PRODUCTIVITY_POPULATION_SCHEMA = _schema_with_comments(
    [
        ("series_id", "STRING"),
        ("series_description", "STRING"),
        ("year", "INT"),
        ("period", "STRING"),
        ("productivity_value", "DOUBLE"),
        ("nation", "STRING"),
        ("population", "BIGINT"),
        ("population_available", "BOOLEAN"),
        ("_snapshot_id", "STRING"),
        ("_calculated_at", "TIMESTAMP"),
    ],
    PRODUCTIVITY_POPULATION_COLUMN_COMMENTS,
)


@dp.materialized_view(
    name="population_statistics",
    comment="US annual population mean and population standard deviation for 2013-2018 by source snapshot.",
    schema=POPULATION_STATISTICS_SCHEMA,
)
@dp.expect(
    "exactly_six_valid_annual_observations",
    "observation_count = 6 AND mean_population IS NOT NULL "
    "AND stddev_population IS NOT NULL",
)
def population_statistics():
    population = spark.read.table(f"{SILVER_SCHEMA}.population")
    return calculate_population_statistics(population)


@dp.materialized_view(
    name="productivity_annual_ranking",
    comment="All tied peak annual BLS Q01-Q04 sums per series and source snapshot; incomplete years are excluded.",
    schema=PRODUCTIVITY_RANKING_SCHEMA,
)
def productivity_annual_ranking():
    observations = spark.read.table(f"{SILVER_SCHEMA}.bls_productivity")
    metadata = spark.read.table(f"{SILVER_SCHEMA}.bls_series")
    return calculate_productivity_annual_ranking(observations, metadata)


@dp.materialized_view(
    name="productivity_population_comparison",
    comment="PRS30006032 Q01 productivity joined to same-snapshot annual population by year.",
    schema=PRODUCTIVITY_POPULATION_SCHEMA,
)
def productivity_population_comparison():
    observations = spark.read.table(f"{SILVER_SCHEMA}.bls_productivity")
    population = spark.read.table(f"{SILVER_SCHEMA}.population")
    metadata = spark.read.table(f"{SILVER_SCHEMA}.bls_series")
    return calculate_productivity_population_comparison(
        observations, population, metadata
    )
