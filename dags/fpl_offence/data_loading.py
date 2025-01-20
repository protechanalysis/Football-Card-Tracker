from airflow.providers.postgres.hooks.postgres import PostgresHook
from fpl_offence.extraction import (fetch_fixtures, fetch_players,
                                    fetch_position, fetch_teams,
                                    manu_player)


def load_fixtures():
    """
    Fetch and load team fixtures into the "team_fixtures" table in PostgreSQL.

    Replaces the table if it already exists.
    """
    fixtures_dd = fetch_fixtures()
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()
    table_name = "team_fixtures"
    fixtures_dd.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)


def load_manu_stats():
    """
    Fetch and load Manchester United player stats into the "player_stats"
    table in PostgreSQL.

    Replaces the table if it already exists.
    """
    player_game_stats = manu_player()
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()
    table_name = "player_stats"
    player_game_stats.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)


def load_position():
    """
    Fetch and load player positions into the "position" table in PostgreSQL.

    Replaces the table if it already exists.
    """
    player_position = fetch_position()
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()
    table_name = "position"
    player_position.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)


def load_teams():
    """
    Fetch and load team data into the "teams" table in PostgreSQL.

    Replaces the table if it already exists.
    """
    teams = fetch_teams()
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()
    table_name = "teams"
    teams.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)


def load_players():
    """
    Fetch and load player data into the "players" table in PostgreSQL.

    Replaces the table if it already exists.
    """
    players_data = fetch_players()
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()
    table_name = "players"
    players_data.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)
