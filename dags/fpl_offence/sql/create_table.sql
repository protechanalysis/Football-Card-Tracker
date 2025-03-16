CREATE TABLE IF NOT EXISTS player_stats (
    id int,
    minutes int,
    yellow_cards int,
    red_cards int,
    gameweek int,
    primary key (id, gameweek)
);

CREATE TABLE IF NOT EXISTS team_fixtures (
    code int primary key,
    event int,
    finished int,
    id int,
    match_date_time timestamp,
    team_a int,
    team_a_score int,
    team_h int,
    team_h_score int,
    season varchar
);

CREATE TABLE IF NOT EXISTS position (
    id int primary key,
    position varchar
);

CREATE TABLE IF NOT EXISTS teams (
    id int primary key,
    name varchar,
    short_name varchar
);

CREATE TABLE IF NOT EXISTS players (
    id int primary key,
    first_name varchar,
    second_name varchar,
    team_id int,
    position_id int
);

CREATE TABLE IF NOT EXISTS alert (
    id int primary key,
    first_name varchar,
    second_name varchar,
    gameweek int,
    match_date_time timestamp,
    alert_type varchar,
    reason varchar
);
