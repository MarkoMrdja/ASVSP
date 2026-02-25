-- Q08: Top 15 država prema procentualnom smanjenju PM2.5 tokom marta-maja 2020. vs 2019.
WITH period_2019 AS (
    SELECT
        state_name,
        AVG(monthly_avg) AS prosjek_2019
    FROM monthly_state_measurements
    WHERE pollutant = 'PM25'
      AND year = 2019
      AND month IN (3, 4, 5)
    GROUP BY state_name
),
period_2020 AS (
    SELECT
        state_name,
        AVG(monthly_avg) AS prosjek_2020
    FROM monthly_state_measurements
    WHERE pollutant = 'PM25'
      AND year = 2020
      AND month IN (3, 4, 5)
    GROUP BY state_name
),
comparison AS (
    SELECT
        p19.state_name,
        ROUND(p19.prosjek_2019, 2)                                          AS prosjek_mart_maj_2019,
        ROUND(p20.prosjek_2020, 2)                                          AS prosjek_mart_maj_2020,
        ROUND(((p19.prosjek_2019 - p20.prosjek_2020) / p19.prosjek_2019) * 100, 2) AS smanjenje_pct
    FROM period_2019 p19
    JOIN period_2020 p20 ON p19.state_name = p20.state_name
)
SELECT
    state_name              AS drzava,
    prosjek_mart_maj_2019,
    prosjek_mart_maj_2020,
    smanjenje_pct,
    RANK() OVER (ORDER BY smanjenje_pct DESC) AS rang_smanjenja
FROM comparison
ORDER BY smanjenje_pct DESC
LIMIT 15;
