from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from fpl_offence.alert_notification import process_and_notify
from fpl_offence.data_loading import (load_fixtures, load_manu_stats,
                                      load_players, load_position, load_teams)
from fpl_offence.extraction import (download_fixtures, fetch_data,
                                    fetch_fixtures, fetch_game_week,
                                    fetch_player_stats, fetch_players,
                                    fetch_position, fetch_teams, manu_player)

default_args = {'owner': 'adewunmi',
                'depends_on_past': False,
                'conn_id': "postgres_id",
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }


with DAG(
    dag_id="epl_fixtures",
    start_date=datetime(2025, 1, 19),
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

    fixtures_download = PythonOperator(
        task_id="fixtures_api",
        python_callable=download_fixtures
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
        task_id="loading_position",
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

    truncate_table = SQLExecuteQueryOperator(
        task_id="offence_table_truncate",
        sql="sql/truncate.sql"
    )

    notify = PythonOperator(
        task_id="alerts_notify",
        python_callable=process_and_notify
    )

    offence = SQLExecuteQueryOperator(
        task_id="offences_load",
        sql="sql/offence.sql"
    )

    create_table = SQLExecuteQueryOperator(
        task_id="creat_table",
        sql="sql/create_table.sql"

    )

    dummy = dummy_task = DummyOperator(
        task_id="dummy_task"
    )

get_teams_data >> [get_teams, get_position,
                   get_epl_players_stats, get_players]
fixtures_download >> get_fixtures >> get_event_week
get_epl_players_stats >> manu_players_stats
[get_event_week, get_teams, get_position, get_epl_players_stats,
 get_players] >> dummy
dummy >> create_table >> [load_team_epl, load_position_player,
                          load_team_fixtures, load_player_stat,
                          load_team_players, truncate_table]
truncate_table >> offence >> notify
