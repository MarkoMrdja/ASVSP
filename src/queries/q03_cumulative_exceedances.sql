SELECT
    state_name,
    year,
    total_exceedance_days AS yearly_exceedances,
    cumulative_exceedance_days
FROM annual_state_measurements
WHERE pollutant = 'PM25'
ORDER BY state_name, year;
