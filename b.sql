WITH TeamWinPercentages AS (
    SELECT
        team_name,
        gender,
        COUNT(*) AS total_matches,
        SUM(CASE WHEN outcome_result = 'won' THEN 1 ELSE 0 END) AS total_wins,
        ROUND(SUM(CASE WHEN outcome_result = 'won' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS win_percentage
    FROM
        matches m
    JOIN
        innings i ON m.match_id = i.match_id
    JOIN
        deliveries d ON i.inning_id = d.inning_id
    WHERE
        gender IN ('male', 'female')
        AND season = '2019'
    GROUP BY
        team_name, gender
)

SELECT
    team_name,
    gender,
    win_percentage
FROM
    TeamWinPercentages
WHERE
    (gender, win_percentage) IN (
        SELECT
            gender,
            MAX(win_percentage) AS max_win_percentage
        FROM
            TeamWinPercentages
        GROUP BY
            gender
    );
