-- Q03: Top 10 država s najviše kumulativnih prekoračenja NAAQS standarda PM2.5
WITH totals AS (
    SELECT
        state_name,
        MAX(cumulative_exceedance_days) AS ukupno_kumulativno
    FROM annual_state_measurements
    WHERE pollutant = 'PM25'
    GROUP BY state_name
),
top_states AS (
    SELECT state_name
    FROM totals
    ORDER BY ukupno_kumulativno DESC
    LIMIT 10
)
SELECT
    a.state_name                    AS drzava,
    a.year                          AS godina,
    a.total_exceedance_days         AS prekoracenja_u_godini,
    a.cumulative_exceedance_days    AS kumulativna_prekoracenja
FROM annual_state_measurements a
JOIN top_states t ON a.state_name = t.state_name
WHERE a.pollutant = 'PM25'
ORDER BY a.cumulative_exceedance_days DESC, a.state_name, a.year;
