WITH annual_totals AS (
    SELECT
        series_id,
        year,
        SUM(value) AS annual_value
    FROM rearc_catalog.data_quest_silver.bls_productivity
    WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
      AND _snapshot_id = (
          SELECT MAX(_snapshot_id)
          FROM rearc_catalog.data_quest_silver.bls_productivity
      )
    GROUP BY series_id, year
),
ranked_years AS (
    SELECT
        series_id,
        year,
        annual_value,
        ROW_NUMBER() OVER (
            PARTITION BY series_id
            ORDER BY annual_value DESC, year ASC
        ) AS year_rank
    FROM annual_totals
)
SELECT
    ranked.series_id,
    ranked.year AS best_year,
    ranked.annual_value,
    series.series_description
FROM ranked_years AS ranked
LEFT JOIN rearc_catalog.data_quest_silver.bls_series AS series
    ON ranked.series_id = series.series_id
   AND series.is_current
WHERE ranked.year_rank = 1;
