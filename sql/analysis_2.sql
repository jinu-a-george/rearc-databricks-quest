WITH annual_totals AS (
    SELECT
        series_id,
        year,
        _snapshot_id,
        SUM(value) AS annual_value,
        COUNT(*) AS observation_count,
        COUNT(DISTINCT period) AS distinct_quarter_count
    FROM rearc_catalog.data_quest_silver.bls_productivity
    WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
    GROUP BY series_id, year, _snapshot_id
),
ranked_years AS (
    SELECT
        series_id,
        year,
        _snapshot_id,
        annual_value,
        observation_count,
        COUNT(DISTINCT period) AS distinct_quarter_count,
        DENSE_RANK() OVER (
            PARTITION BY series_id, _snapshot_id
            ORDER BY annual_value DESC
        ) AS year_rank
    FROM annual_totals
),
winning_years AS (
    SELECT
        series_id,
        year,
        _snapshot_id,
        annual_value,
        observation_count,
        (
            observation_count = 4
            AND distinct_quarter_count = 4
        ) AS is_full_calendar_year
    FROM ranked_years
    WHERE year_rank = 1
)
SELECT
    winners.series_id,
    series.series_description,
    CAST(winners.year AS INT) AS winning_year,
    CAST(winners.annual_value AS DOUBLE) AS annual_value,
    CAST(winners.observation_count AS INT) AS quarterly_observation_count,
    winners.is_full_calendar_year,
    winners._snapshot_id,
    CURRENT_TIMESTAMP() AS _calculated_at
FROM winning_years AS winners
LEFT JOIN rearc_catalog.data_quest_silver.bls_series AS series
  ON winners.series_id = series.series_id
 AND series.valid_from_snapshot_id <= winners._snapshot_id
 AND (
        series.valid_to_snapshot_id IS NULL
        OR series.valid_to_snapshot_id > winners._snapshot_id
     );
