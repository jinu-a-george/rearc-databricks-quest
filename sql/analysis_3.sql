SELECT
    observations.series_id,
    observations.year,
    observations.period,
    observations.value,
    population.population
FROM rearc_catalog.data_quest_silver.bls_productivity AS observations
LEFT JOIN rearc_catalog.data_quest_silver.population AS population
    ON observations.year = population.year
   AND observations._snapshot_id = population._snapshot_id
WHERE observations.series_id = 'PRS30006032'
  AND observations.period = 'Q01'
  AND observations._snapshot_id = (
      SELECT MAX(_snapshot_id)
      FROM rearc_catalog.data_quest_silver.bls_productivity
  )
ORDER BY observations.year;
