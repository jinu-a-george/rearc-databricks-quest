from __future__ import annotations

import sys

from pyspark import pipelines as dp

PROJECT_ROOT = spark.conf.get("project_root")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.pipeline.analytics import (
    best_year_by_series,
    population_statistics as calculate_population_statistics,
    series_values_with_population,
)


SILVER_SCHEMA = "rearc_catalog.data_quest_silver"


@dp.materialized_view(
    name="population_statistics",
    comment="US annual population mean and population standard deviation, 2013-2018.",
)
@dp.expect("exactly_six_population_years", "population_year_count = 6")
def population_statistics():
    population = spark.read.table(f"{SILVER_SCHEMA}.population")
    return calculate_population_statistics(population)


@dp.materialized_view(
    name="best_productivity_year_by_series",
    comment="Peak annual sum of quarterly BLS values per series, excluding Q05.",
)
def best_productivity_year_by_series():
    observations = spark.read.table(f"{SILVER_SCHEMA}.bls_productivity")
    metadata = spark.read.table(f"{SILVER_SCHEMA}.bls_series")
    return best_year_by_series(observations, metadata)


@dp.materialized_view(
    name="target_series_with_population",
    comment="Q01 values for PRS30006032 with annual population where available.",
)
def target_series_with_population():
    observations = spark.read.table(f"{SILVER_SCHEMA}.bls_productivity")
    population = spark.read.table(f"{SILVER_SCHEMA}.population")
    return series_values_with_population(observations, population)
