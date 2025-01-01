from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fpl_offence.alert_notification import process_and_notify
from fpl_offence.data_loading import (load_fixtures, load_manu_stats,
                                      load_offence_into_table,
                                      load_players, load_position,
                                      load_teams)
from fpl_offence.extraction import (fetch_data, fetch_fixtures,
                                    fetch_game_week, fetch_player_stats,
                                    fetch_players, fetch_position,
                                    fetch_teams, manu_player)

default_args = {'owner': 'adewunmi',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }


with DAG(
    dag_id="epl_fixtures",
    start_date=datetime(2024, 12, 17),
    default_args=default_args,
    schedule_interval=None
) as dag:

    get_teams_data = PythonOperator(
        task_id="get_team_data",
        python_callable=fetch_data
    )

    get_teams = PythonOperator(
        task_id="team",
        python_callable=fetch_teams
    )

    get_players = PythonOperator(
        task_id="teams_player",
        python_callable=fetch_players
    )

    get_position = PythonOperator(
        task_id="player_position",
        python_callable=fetch_position
    )

    get_fixtures = PythonOperator(
        task_id="get_fixtures",
        python_callable=fetch_fixtures
    )

    get_event_week = PythonOperator(
        task_id="list_game_week",
        python_callable=fetch_game_week
    )

    get_epl_players_stats = PythonOperator(
        task_id="players_stats",
        python_callable=fetch_player_stats
    )

    manu_players_stats = PythonOperator(
        task_id="manu_players_stats",
        python_callable=manu_player
    )

    load_team_epl = PythonOperator(
        task_id="load_epl_team",
        python_callable=load_teams
    )

    load_position_player = PythonOperator(
        task_id="load_to_database",
        python_callable=load_position
    )

    load_team_fixtures = PythonOperator(
        task_id="fixtures_load",
        python_callable=load_fixtures
    )

    load_player_stat = PythonOperator(
        task_id="load_stats_player",
        python_callable=load_manu_stats
    )

    load_team_players = PythonOperator(
        task_id="players",
        python_callable=load_players
    )

    offence = PythonOperator(
        task_id="alerts",
        python_callable=load_offence_into_table
    )

    notify = PythonOperator(
        task_id="alerts_notify",
        python_callable=process_and_notify
    )


get_teams_data >> [get_teams, get_players, get_position] >> get_fixtures
get_fixtures >> get_event_week >> get_epl_players_stats >> manu_players_stats
get_players >> load_team_players
get_fixtures >> load_team_fixtures
manu_players_stats >> load_player_stat >> offence >> notify
get_position >> load_position_player
get_teams >> load_team_epl
