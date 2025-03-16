WITH sta AS (
    SELECT 
        ps.id, 
        gameweek, 
        yellow_cards, 
        red_cards, 
        match_date_time
    FROM player_stats ps
    JOIN team_fixtures f
    ON ps.gameweek = f.event
    WHERE yellow_cards > 0 OR red_cards > 0
),
cumulative_reset AS (
    SELECT 
        id, 
        gameweek, 
        red_cards,
        match_date_time, 
        SUM(yellow_cards) OVER (
            PARTITION BY id 
            ORDER BY match_date_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS yellow_running_total
    FROM sta
),
grouped_reset AS (
    SELECT 
        *, 
        (yellow_running_total % 5) AS yellow_reset_group
    FROM cumulative_reset
),
warning_and_miss AS (
    SELECT 
        id, 
        gameweek, 
        red_cards,
        match_date_time, 
        CASE 
            WHEN yellow_reset_group = 4 THEN 'warning'
            WHEN yellow_reset_group = 0 THEN 'miss_next_match'
            WHEN red_cards > 0 THEN 'miss_next_match' -- Red card suspension
            ELSE NULL
        END AS alert_type,
        CASE 
	        WHEN yellow_reset_group = 4 THEN '4 yellows'
            WHEN yellow_reset_group = 0 THEN 'Accumulated Yellow Card(5)'
            WHEN red_cards > 0 THEN 'Straight Red' 
            ELSE NULL
        END AS reason
    FROM grouped_reset
),
latest_alerts AS (
    SELECT 
        id, 
        MAX(gameweek) AS latest_gameweek, 
        MAX(match_date_time) AS latest_date
    FROM warning_and_miss
     WHERE alert_type IS NOT NULL
    GROUP BY id
),
filtered_alerts AS (
    SELECT 
        wam.id, 
        wam.gameweek, 
        wam.match_date_time, 
        wam.alert_type,
        wam.reason,
        wam.red_cards
    FROM warning_and_miss wam
    JOIN latest_alerts la
    ON wam.id = la.id 
       AND wam.gameweek = la.latest_gameweek
       AND wam.match_date_time = la.latest_date
    WHERE wam.alert_type IN ('warning', 'miss_next_match')
),
max_gameweek AS (
    SELECT MAX(event) AS max_gameweek
    FROM team_fixtures
    where finished = 1
),
valid_alerts AS (
    SELECT 
        fa.*
    FROM filtered_alerts fa
    CROSS JOIN max_gameweek mg
    WHERE NOT (fa.alert_type = 'miss_next_match' AND fa.gameweek < mg.max_gameweek)
)
insert into alert (id, first_name, second_name, gameweek, match_date_time, alert_type, reason)
SELECT 
    va.id, 
    p.first_name, 
    p.second_name,
    va.gameweek, 
    va.match_date_time, 
    va.alert_type,
    va.reason
FROM valid_alerts va
JOIN players p
USING(id);