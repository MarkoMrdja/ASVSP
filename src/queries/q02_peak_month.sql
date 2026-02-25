-- Q02: Najzagađeniji mjesec po državi i godini + koji mjesec najčešće predvodi u 9-godišnjem periodu
WITH ranked_months AS (
    SELECT
        state_name,
        year,
        month,
        monthly_avg,
        RANK() OVER (PARTITION BY state_name, year ORDER BY monthly_avg DESC) AS month_rank
    FROM monthly_state_measurements
    WHERE pollutant = 'PM25'
),
peak_per_year AS (
    SELECT
        state_name,
        year,
        month                       AS najzagadjeniji_mjesec,
        ROUND(monthly_avg, 2)       AS prosjek_najzagadjenijeg_mjeseca
    FROM ranked_months
    WHERE month_rank = 1
),
most_frequent_peak AS (
    SELECT
        state_name,
        najzagadjeniji_mjesec       AS najcesci_vrsni_mjesec,
        COUNT(*)                    AS broj_godina_na_vrhu,
        RANK() OVER (PARTITION BY state_name ORDER BY COUNT(*) DESC) AS freq_rank
    FROM peak_per_year
    GROUP BY state_name, najzagadjeniji_mjesec
)
SELECT
    p.state_name                            AS drzava,
    p.year                                  AS godina,
    p.najzagadjeniji_mjesec,
    p.prosjek_najzagadjenijeg_mjeseca,
    m.najcesci_vrsni_mjesec,
    m.broj_godina_na_vrhu
FROM peak_per_year p
JOIN most_frequent_peak m ON p.state_name = m.state_name AND m.freq_rank = 1
ORDER BY p.prosjek_najzagadjenijeg_mjeseca DESC
LIMIT 50;
