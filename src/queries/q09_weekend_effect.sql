-- Q09: Top 10 država s najvećim i top 10 s najmanjim vikend efektom PM2.5
WITH weekend_weekday AS (
    SELECT
        state_name,
        AVG(CASE WHEN is_weekend THEN daily_avg END)     AS prosjek_vikend,
        AVG(CASE WHEN NOT is_weekend THEN daily_avg END) AS prosjek_radni_dan
    FROM daily_state_measurements
    WHERE pollutant = 'PM25'
    GROUP BY state_name
),
ranked AS (
    SELECT
        state_name,
        ROUND(prosjek_vikend, 2)                                        AS prosjek_vikend,
        ROUND(prosjek_radni_dan, 2)                                     AS prosjek_radni_dan,
        ROUND(prosjek_vikend / prosjek_radni_dan, 3)                    AS omjer_vikend_radni,
        RANK() OVER (ORDER BY prosjek_vikend / prosjek_radni_dan DESC)  AS rang_najveci_efekat,
        RANK() OVER (ORDER BY prosjek_vikend / prosjek_radni_dan ASC)   AS rang_najmanji_efekat
    FROM weekend_weekday
)
SELECT
    state_name          AS drzava,
    prosjek_vikend,
    prosjek_radni_dan,
    omjer_vikend_radni,
    CASE
        WHEN rang_najveci_efekat <= 10 THEN 'Top 10 vikend efekat'
        WHEN rang_najmanji_efekat <= 10 THEN 'Top 10 bez vikend efekta'
    END                 AS kategorija
FROM ranked
WHERE rang_najveci_efekat <= 10 OR rang_najmanji_efekat <= 10
ORDER BY omjer_vikend_radni DESC;
