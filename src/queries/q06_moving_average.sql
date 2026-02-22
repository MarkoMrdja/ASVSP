SELECT
    state_name,
    date_local,
    ROUND(daily_avg, 2) AS daily_avg,
    ROUND(
        AVG(daily_avg) OVER (
            PARTITION BY state_name
            ORDER BY date_local
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ),
    2) AS moving_avg_30d
FROM daily_state_measurements
WHERE pollutant = 'PM25'
ORDER BY state_name, date_local;
