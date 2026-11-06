SELECT
    COUNT(DISTINCT year) AS population_year_count,
    AVG(population) AS mean_population,
    STDDEV_POP(population) AS population_stddev
FROM rearc_catalog.data_quest_silver.population
WHERE year BETWEEN 2013 AND 2018
  AND _snapshot_id = (
      SELECT MAX(_snapshot_id)
      FROM rearc_catalog.data_quest_silver.population
  );
