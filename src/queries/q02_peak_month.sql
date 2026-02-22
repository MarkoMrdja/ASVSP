WITH ranked_months AS (
    SELECT
        state_name,
        year,
        month,
        monthly_avg,
        RANK() OVER (PARTITION BY state_name, year ORDER BY monthly_avg DESC) AS month_rank
    FROM monthly_state_measurements
    WHERE pollutant = 'PM25'
)
SELECT
    state_name,
    year,
    month AS peak_month,
    ROUND(monthly_avg, 2) AS peak_month_avg
FROM ranked_months
WHERE month_rank = 1
ORDER BY state_name, year;
