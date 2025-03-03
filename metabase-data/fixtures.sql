with away as (
    select event, match_date_time, name as team_away, team_a_score
    from team_fixtures tf
    join teams t
    on tf.team_a = t.id
    where finished =true
),
home as (
    select event, match_date_time, name as team_home, team_h_score
    from team_fixtures tf
    join teams t
    on tf.team_h = t.id
    where finished =true
)
select a.event as game_week, a.match_date_time, team_home, team_away, team_h_score, team_a_score
from home h
join away a
on a.event = h.event

