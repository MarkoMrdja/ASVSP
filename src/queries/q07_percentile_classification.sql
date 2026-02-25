-- Q07: Klasifikacija dnevnih mjerenja PM2.5 u kvartile po državi i godini
WITH quartiles AS (
    SELECT
        state_name,
        year,
        date_local,
        daily_avg,
        NTILE(4) OVER (PARTITION BY state_name ORDER BY daily_avg DESC) AS kvartil
    FROM daily_state_measurements
    WHERE pollutant = 'PM25'
)
SELECT
    state_name          AS drzava,
    year                AS godina,
    kvartil,
    COUNT(*)            AS broj_dana,
    CASE kvartil
        WHEN 1 THEN 'Gornji kvartal (najzagađenije 25%)'
        WHEN 2 THEN 'Drugi kvartal (25–50%)'
        WHEN 3 THEN 'Treći kvartal (50–75%)'
        WHEN 4 THEN 'Donji kvartal (najmanje zagađeno 25%)'
    END                 AS oznaka_kvartila
FROM quartiles
GROUP BY drzava, godina, kvartil
ORDER BY drzava, godina, kvartil;
