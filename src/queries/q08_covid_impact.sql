WITH period_2019 AS (
    SELECT
        state_name,
        AVG(monthly_avg) AS avg_2019
    FROM monthly_state_measurements
    WHERE pollutant = 'PM25'
      AND year = 2019
      AND month IN (3, 4, 5)
    GROUP BY state_name
),
period_2020 AS (
    SELECT
        state_name,
        AVG(monthly_avg) AS avg_2020
    FROM monthly_state_measurements
    WHERE pollutant = 'PM25'
      AND year = 2020
      AND month IN (3, 4, 5)
    GROUP BY state_name
),
comparison AS (
    SELECT
        p19.state_name,
        ROUND(p19.avg_2019, 2) AS avg_2019,
        ROUND(p20.avg_2020, 2) AS avg_2020,
        ROUND(((p19.avg_2019 - p20.avg_2020) / p19.avg_2019) * 100, 2) AS pct_reduction
    FROM period_2019 p19
    JOIN period_2020 p20 ON p19.state_name = p20.state_name
)
SELECT
    state_name,
    avg_2019,
    avg_2020,
    pct_reduction,
    RANK() OVER (ORDER BY pct_reduction DESC) AS reduction_rank
FROM comparison
ORDER BY pct_reduction DESC;
