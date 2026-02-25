-- Q01: Top 10 najzagađenijih država po godišnjem prosjeku PM2.5, sa promjenom ranga
WITH ranked AS (
    SELECT
        state_name,
        year,
        annual_avg,
        yoy_pct_change,
        RANK() OVER (PARTITION BY year ORDER BY annual_avg DESC) AS state_rank
    FROM annual_state_measurements
    WHERE pollutant = 'PM25'
),
with_prev_rank AS (
    SELECT
        state_name                                                                      AS drzava,
        year                                                                            AS godina,
        ROUND(annual_avg, 2)                                                            AS godisnji_prosjek,
        yoy_pct_change                                                                  AS promjena_yoy_pct,
        state_rank                                                                      AS rang,
        LAG(state_rank) OVER (PARTITION BY state_name ORDER BY year)                   AS prethodni_rang,
        NVL(LAG(state_rank) OVER (PARTITION BY state_name ORDER BY year) - state_rank, 0) AS promjena_ranga
    FROM ranked
)
SELECT
    drzava,
    godina,
    godisnji_prosjek,
    promjena_yoy_pct,
    rang,
    prethodni_rang,
    promjena_ranga
FROM with_prev_rank
WHERE rang <= 10
ORDER BY godina, rang;
