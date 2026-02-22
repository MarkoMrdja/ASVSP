SELECT
    state_name,
    year,
    month,
    ROUND(monthly_avg, 2) AS monthly_avg,
    ROUND(same_month_prev_year_avg, 2) AS same_month_last_year,
    yoy_month_change AS yoy_change
FROM monthly_state_measurements
WHERE pollutant = 'PM25'
ORDER BY state_name, month, year;
