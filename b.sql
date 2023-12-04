WITH TeamWinPercentages AS (
    SELECT
        team_name,
        gender,
        ROUND(SUM(CASE WHEN outcome_result = 'won' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS win_percentage
    FROM
        matches m
    JOIN
        innings i ON m.match_id = i.match_id
    JOIN
        deliveries d ON i.inning_id = d.inning_id
    WHERE
        outcome_result = 'won'
        AND year = '2019'
        AND gender IN ('male', 'female')
    GROUP BY
        team_name, gender
)

SELECT
    gender,
    team_name,
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
        WHERE
            gender IN ('male', 'female')
        GROUP BY
            gender
    );
