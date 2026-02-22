WITH quartiles AS (
    SELECT
        state_name,
        year,
        date_local,
        daily_avg,
        NTILE(4) OVER (PARTITION BY state_name ORDER BY daily_avg DESC) AS quartile
    FROM daily_state_measurements
    WHERE pollutant = 'PM25'
)
SELECT
    state_name,
    year,
    quartile,
    COUNT(*) AS day_count,
    CASE quartile
        WHEN 1 THEN 'Top 25%'
        WHEN 2 THEN '25-50%'
        WHEN 3 THEN '50-75%'
        WHEN 4 THEN 'Bottom 25%'
    END AS quartile_label
FROM quartiles
GROUP BY state_name, year, quartile
ORDER BY state_name, year, quartile;
