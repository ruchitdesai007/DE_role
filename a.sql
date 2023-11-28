SELECT
    team_name,
    gender,
    year,
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
    outcome_result IN ('won', 'no result') -- Exclude ties and other outcomes
GROUP BY
    team_name, gender, year;
