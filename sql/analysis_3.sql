SELECT
    observations.series_id,
    series.series_description,
    CAST(observations.year AS INT) AS year,
    observations.period,
    CAST(observations.value AS DOUBLE) AS productivity_value,
    population.nation,
    CAST(population.population AS BIGINT) AS population,
    population.population IS NOT NULL AS population_available,
    observations._snapshot_id,
    CURRENT_TIMESTAMP() AS _calculated_at
FROM rearc_catalog.data_quest_silver.bls_productivity AS observations
LEFT JOIN rearc_catalog.data_quest_silver.population AS population
  ON observations.year = population.year
 AND observations._snapshot_id = population._snapshot_id
LEFT JOIN rearc_catalog.data_quest_silver.bls_series AS series
  ON observations.series_id = series.series_id
 AND series.valid_from_snapshot_id <= observations._snapshot_id
 AND (
        series.valid_to_snapshot_id IS NULL
        OR series.valid_to_snapshot_id > observations._snapshot_id
     )
WHERE observations.series_id = 'PRS30006032'
  AND observations.period = 'Q01'
ORDER BY observations._snapshot_id, observations.year;
