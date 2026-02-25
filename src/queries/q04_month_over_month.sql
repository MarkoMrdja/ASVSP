-- Q04: Procentualna promjena prosječne koncentracije PM2.5 u odnosu na prethodni mjesec
SELECT
    state_name               AS drzava,
    year                     AS godina,
    month                    AS mjesec,
    ROUND(monthly_avg, 2)    AS mjesecni_prosjek,
    ROUND(prev_month_avg, 2) AS prosjek_prethodnog_mjeseca,
    mom_pct_change           AS promjena_mom_pct
FROM monthly_state_measurements
WHERE pollutant = 'PM25'
ORDER BY drzava, godina, mjesec;
