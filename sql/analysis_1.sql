WITH selected_population AS (
    SELECT
        nation_id,
        nation,
        year,
        population,
        _snapshot_id,
        COUNT(*) OVER (
            PARTITION BY nation_id, year, _snapshot_id
        ) AS annual_record_count
    FROM rearc_catalog.data_quest_silver.population
    WHERE year BETWEEN 2013 AND 2018
),
snapshot_nations AS (
    SELECT DISTINCT
        nation_id,
        nation,
        _snapshot_id
    FROM selected_population
),
unique_years AS (
    SELECT
        nation_id,
        nation,
        year,
        population,
        _snapshot_id
    FROM selected_population
    WHERE annual_record_count = 1
      AND population IS NOT NULL
),
population_aggregates AS (
    SELECT
        nation_id,
        nation,
        _snapshot_id,
        COUNT(*) AS observation_count,
        AVG(population) AS mean_population,
        STDDEV_POP(population) AS stddev_population
    FROM unique_years
    GROUP BY nation_id, nation, _snapshot_id
),
statistics_by_snapshot AS (
    SELECT
        nations.nation_id,
        nations.nation,
        2013 AS start_year,
        2018 AS end_year,
        COALESCE(aggregates.observation_count, 0) AS observation_count,
        CASE
            WHEN aggregates.observation_count = 6
            THEN aggregates.mean_population
        END AS mean_population,
        CASE
            WHEN aggregates.observation_count = 6
            THEN aggregates.stddev_population
        END AS stddev_population,
        'STDDEV_POP' AS calculation_method,
        nations._snapshot_id,
        CURRENT_TIMESTAMP() AS _calculated_at
    FROM snapshot_nations AS nations
    LEFT JOIN population_aggregates AS aggregates
      ON nations.nation_id = aggregates.nation_id
     AND nations.nation = aggregates.nation
     AND nations._snapshot_id = aggregates._snapshot_id
)
SELECT
    nation_id,
    nation,
    start_year,
    end_year,
    CAST(observation_count AS INT) AS observation_count,
    CAST(mean_population AS DOUBLE) AS mean_population,
    CAST(stddev_population AS DOUBLE) AS stddev_population,
    calculation_method,
    _snapshot_id,
    _calculated_at
FROM statistics_by_snapshot;
