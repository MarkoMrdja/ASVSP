WITH with_prev AS (
    SELECT
        state_name,
        year,
        month,
        monthly_avg,
        LAG(monthly_avg) OVER (
            PARTITION BY state_name
            ORDER BY year, month
        ) AS prev_month_avg
    FROM monthly_state_measurements
    WHERE pollutant = 'PM25'
),
with_improvement AS (
    SELECT
        state_name,
        year,
        month,
        monthly_avg,
        prev_month_avg,
        CASE WHEN monthly_avg < prev_month_avg THEN 1 ELSE 0 END AS is_improvement
    FROM with_prev
),
with_groups AS (
    SELECT
        state_name,
        year,
        month,
        is_improvement,
        SUM(CASE WHEN is_improvement = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY state_name
            ORDER BY year, month
        ) AS streak_group
    FROM with_improvement
),
streak_lengths AS (
    SELECT
        state_name,
        streak_group,
        COUNT(*) AS streak_length,
        MIN(year * 100 + month) AS streak_start
    FROM with_groups
    WHERE is_improvement = 1
    GROUP BY state_name, streak_group
)
SELECT
    state_name,
    MAX(streak_length) AS longest_streak
FROM streak_lengths
GROUP BY state_name
ORDER BY longest_streak DESC;
