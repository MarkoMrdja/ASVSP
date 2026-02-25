-- Q05: Top 10 država s najvećim poboljšanjem i top 10 s najvećim pogoršanjem PM2.5 (isti mjesec, prethodna godina)
WITH yoy AS (
    SELECT
        state_name,
        year,
        month,
        ROUND(monthly_avg, 2)               AS mjesecni_prosjek,
        ROUND(same_month_prev_year_avg, 2)  AS isti_mjesec_prosle_godine,
        yoy_month_change
    FROM monthly_state_measurements
    WHERE pollutant = 'PM25'
      AND same_month_prev_year_avg IS NOT NULL
),
avg_change_per_state AS (
    SELECT
        state_name,
        ROUND(AVG(yoy_month_change), 4) AS prosjecna_yoy_promjena
    FROM yoy
    GROUP BY state_name
),
ranked AS (
    SELECT
        state_name,
        prosjecna_yoy_promjena,
        RANK() OVER (ORDER BY prosjecna_yoy_promjena ASC)  AS rang_poboljsanja,
        RANK() OVER (ORDER BY prosjecna_yoy_promjena DESC) AS rang_pogorsanja
    FROM avg_change_per_state
)
SELECT
    state_name              AS drzava,
    prosjecna_yoy_promjena,
    CASE
        WHEN rang_poboljsanja  <= 10 THEN 'Top 10 poboljšanje'
        WHEN rang_pogorsanja   <= 10 THEN 'Top 10 pogoršanje'
    END                     AS kategorija,
    rang_poboljsanja,
    rang_pogorsanja
FROM ranked
WHERE rang_poboljsanja <= 10 OR rang_pogorsanja <= 10
ORDER BY prosjecna_yoy_promjena ASC;
