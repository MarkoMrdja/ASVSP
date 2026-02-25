-- Q06: Datum s najvišim 30-dnevnim pokretnim prosjekom PM2.5 po državi i godini
WITH with_moving_avg AS (
    SELECT
        state_name,
        date_local,
        YEAR(date_local)        AS godina,
        ROUND(daily_avg, 2)     AS dnevni_prosjek,
        ROUND(
            AVG(daily_avg) OVER (
                PARTITION BY state_name
                ORDER BY date_local
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ),
        2)                      AS pokretni_prosjek_30d
    FROM daily_state_measurements
    WHERE pollutant = 'PM25'
),
ranked AS (
    SELECT
        state_name,
        godina,
        date_local,
        dnevni_prosjek,
        pokretni_prosjek_30d,
        RANK() OVER (PARTITION BY state_name, godina ORDER BY pokretni_prosjek_30d DESC) AS rn
    FROM with_moving_avg
)
SELECT
    state_name              AS drzava,
    godina,
    date_local              AS vrsni_datum,
    dnevni_prosjek,
    pokretni_prosjek_30d    AS vrsni_pokretni_prosjek_30d
FROM ranked
WHERE rn = 1
ORDER BY vrsni_pokretni_prosjek_30d DESC
LIMIT 50;
