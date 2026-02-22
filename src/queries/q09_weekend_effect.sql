WITH weekend_weekday AS (
    SELECT
        state_name,
        AVG(CASE WHEN is_weekend THEN daily_avg END) AS weekend_avg,
        AVG(CASE WHEN NOT is_weekend THEN daily_avg END) AS weekday_avg
    FROM daily_state_measurements
    WHERE pollutant = 'PM25'
    GROUP BY state_name
)
SELECT
    state_name,
    ROUND(weekend_avg, 2) AS weekend_avg,
    ROUND(weekday_avg, 2) AS weekday_avg,
    ROUND(weekend_avg / weekday_avg, 3) AS weekend_weekday_ratio,
    RANK() OVER (ORDER BY weekend_avg / weekday_avg ASC) AS weekend_effect_rank
FROM weekend_weekday
ORDER BY weekend_weekday_ratio ASC;
