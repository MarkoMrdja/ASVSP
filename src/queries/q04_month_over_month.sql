SELECT
    state_name,
    year,
    month,
    ROUND(monthly_avg, 2) AS monthly_avg,
    ROUND(prev_month_avg, 2) AS prev_month_avg,
    mom_pct_change AS pct_change
FROM monthly_state_measurements
WHERE pollutant = 'PM25'
ORDER BY state_name, year, month;
