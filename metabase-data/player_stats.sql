select concat(first_name, ' ', second_name) as player_name,position, count(minutes) as total_matches_played, sum(minutes) as total_minutes_played,
    sum(yellow_cards) as no_yellows, sum(red_cards)as no_reds
from player_stats
join players p
using(id)
join position po
on p.position_id = po.id
where minutes > 0
group by player_name, position