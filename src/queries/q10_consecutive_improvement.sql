-- Q10: Top 15 država s najdužim nizom uzastopnih mjeseci opadanja PM2.5
WITH with_prev AS (
    SELECT
        state_name,
        year,
        month,
        monthly_avg,
        LAG(monthly_avg) OVER (
            PARTITION BY state_name
            ORDER BY year, month
        ) AS prosjek_prethodnog_mjeseca
    FROM monthly_state_measurements
    WHERE pollutant = 'PM25'
),
with_improvement AS (
    SELECT
        state_name,
        year,
        month,
        monthly_avg,
        prosjek_prethodnog_mjeseca,
        CASE WHEN monthly_avg < prosjek_prethodnog_mjeseca THEN 1 ELSE 0 END AS je_poboljsanje
    FROM with_prev
),
with_groups AS (
    SELECT
        state_name,
        year,
        month,
        je_poboljsanje,
        SUM(CASE WHEN je_poboljsanje = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY state_name
            ORDER BY year, month
        ) AS grupa_niza
    FROM with_improvement
),
streak_lengths AS (
    SELECT
        state_name,
        grupa_niza,
        COUNT(*)                    AS duzina_niza,
        MIN(year * 100 + month)     AS pocetak_niza
    FROM with_groups
    WHERE je_poboljsanje = 1
    GROUP BY state_name, grupa_niza
)
SELECT
    state_name              AS drzava,
    MAX(duzina_niza)        AS najduzi_niz_mjeseci
FROM streak_lengths
GROUP BY state_name
ORDER BY najduzi_niz_mjeseci DESC
LIMIT 15;
