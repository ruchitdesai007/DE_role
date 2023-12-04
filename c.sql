WITH PlayerStrikeRates AS (
    SELECT
        p.player_name,
        ROUND((SUM(d.runs_batter) * 100.0) / NULLIF(SUM(d.balls_faced), 0), 2) AS strike_rate
    FROM
        deliveries d
    JOIN
        innings i ON d.inning_id = i.inning_id
    JOIN
        matches m ON i.match_id = m.match_id
    JOIN
        players p ON d.batter = p.player_name
    WHERE
        m.year = '2019'
        AND d.bye_runs = 0
        AND d.legbye_runs = 0
        AND d.wide_runs = 0
        AND d.noball_runs = 0
    GROUP BY
        p.player_name
)

SELECT
    player_name,
    strike_rate
FROM
    PlayerStrikeRates
ORDER BY
    strike_rate DESC
LIMIT 10; 
