from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {'owner': 'adewunmi',
                'depends_on_past': False,
                'retries': 2,
                'postgres_conn_id': "postgres_id",
                'retry_delay': timedelta(minutes=5)
                }

with DAG(
    dag_id="standings",
    start_date=datetime(2024, 12, 18),
    default_args=default_args,
    schedule_interval=None
) as dag:

    player_stats = PostgresOperator(
        task_id="player_stats",
        # postgres_conn_id="postgres_id",
        sql="""
        CREATE TABLE player_stats (
            id int,
            minutes int,
            yellow_cards int,
            red_cards int,
            gameweek int
        );
         """
    )

    team_fixtures = PostgresOperator(
        task_id="team_fixtures",
        # postgres_conn_id="postgres_id",
        sql="""
        CREATE TABLE team_fixtures (
            code int,
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
         """
    )

    field_position = PostgresOperator(
        task_id="field_position",
        # postgres_conn_id="postgres_id",
        sql="""
        CREATE TABLE position (
            id int,
            position varchar
        );
        """
    )

    epl_teams = PostgresOperator(
        task_id="epl_teams",
        # postgres_conn_id="postgres_id",
        sql="""
        CREATE TABLE teams (
            id int,
            name varchar,
            short_name varchar
        );
        """
    )

    players_team = PostgresOperator(
        task_id="player_manu",
        # postgres_conn_id="postgres_id",
        sql="""
        CREATE TABLE players (
            id int,
            first_name varchar,
            second_name varchar,
            team_id int,
            position_id int
        );
        """
    )

    offence_tracker = PostgresOperator(
        task_id="cards_tracker",
        # postgres_conn_id="postgres_id",
        sql="""
        CREATE TABLE alert (
            id int,
            first_name varchar,
            second_name varchar,
            gameweek int,
            match_date_time timestamp,
            alert_type varchar,
            reason varchar
        );
        """
    )

player_stats >> team_fixtures >> players_team >> field_position
field_position >> epl_teams >> offence_tracker
