WITH ranked AS (
    SELECT
        state_name,
        year,
        annual_avg,
        yoy_pct_change,
        RANK() OVER (PARTITION BY year ORDER BY annual_avg DESC) AS state_rank
    FROM annual_state_measurements
    WHERE pollutant = 'PM25'
)
SELECT
    state_name,
    year,
    ROUND(annual_avg, 2) AS annual_avg,
    yoy_pct_change,
    state_rank,
    LAG(state_rank) OVER (PARTITION BY state_name ORDER BY year) AS prev_rank,
    NVL(LAG(state_rank) OVER (PARTITION BY state_name ORDER BY year) - state_rank, 0) AS rank_change
FROM ranked
ORDER BY year, state_rank;
