WITH PlayerStrikeRates AS (
    SELECT
        batter,
        COUNT(*) AS total_runs,
        SUM(runs_batter) AS total_runs_scored,
        COUNT(DISTINCT d.match_id) AS total_matches,
        ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 2) AS strike_rate
    FROM
        deliveries d
    JOIN
        innings i ON d.inning_id = i.inning_id
    JOIN
        matches m ON i.match_id = m.match_id
    WHERE
        d.runs_batter > 0
        AND m.season = '2019'
    GROUP BY
        batter
)

SELECT
    batter,
    strike_rate
FROM
    PlayerStrikeRates
ORDER BY
    strike_rate DESC
LIMIT 10; -- Adjust the limit based on the number of top players you want to retrieve
