import logging

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_fixtures():
    """
    Fetch and load team fixtures into the "team_fixtures" table in PostgreSQL.

    Replaces the table if it already exists.
    """
    try:
        logging.info("Establish connection & loading fixtures into database..")
        postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        logging.info('Connected to the database successfully')
        table_name = "team_fixtures"
        file_path = "/opt/airflow/dags/fpl_offence/data/fixtures.csv"
        fixtures = pd.read_csv(file_path)
        print('Data reading successfully')
        # Convert DataFrame to list of tuples for batch insertion
        records = [tuple(x) for x in fixtures.itertuples(index=False,
                                                         name=None)]

        insert_query = f"""
            INSERT INTO {table_name} (code, event, finished, id,
            match_date_time, team_a,team_a_score,team_h,team_h_score,season)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (code) DO NOTHING
        """
        # Batch insert
        cursor.executemany(insert_query, records)
        connection.commit()
        logging.info('Data inserted successfully')

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def load_manu_stats():
    """
    Fetch and load Manchester United player stats into the "player_stats"
    table in PostgreSQL.

    Replaces the table if it already exists.
    """
    try:
        logging.info("Establish connection & \
        loading manu players stats into database....")
        postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        logging.info('Connected to the database successfully')
        table_name = "player_stats"
        file_path = (
            "/opt/airflow/dags/fpl_offence/data/manunited_players_stats.csv"
            )
        stat = pd.read_csv(file_path)
        print('Data reading successfully')
        # Convert DataFrame to list of tuples for batch insertion
        records = [tuple(x) for x in stat.itertuples(index=False, name=None)]

        insert_query = f"""
            INSERT INTO {table_name} (id, minutes, yellow_cards,
            red_cards, gameweek)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id, gameweek) DO NOTHING
        """
        # Batch insert
        cursor.executemany(insert_query, records)
        connection.commit()
        logging.info('Data inserted successfully')

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def load_position():
    """
    Fetch and load player positions into the "position" table in PostgreSQL.

    Replaces the table if it already exists.
    """
    try:
        logging.info("Establish connection & loading position into database..")
        postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        logging.info('Connected to the database successfully')
        table_name = "position"
        file_path = "/opt/airflow/dags/fpl_offence/data/position.csv"
        position_data = pd.read_csv(file_path)
        print('Data reading successfully')
        # Convert DataFrame to list of tuples for batch insertion
        for _, row in position_data.iterrows():
            insert_query = f"""
                INSERT INTO {table_name} (id, position)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """
            cursor.execute(insert_query, (row['id'], row['position']))

        connection.commit()
        logging.info('Data inserted successfully')

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def load_teams():
    """
    Fetch and load team data into the "teams" table in PostgreSQL.

    Replaces the table if it already exists.
    """
    try:
        logging.info("Establish connection & loading fixtures into database..")
        postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        logging.info('Connected to the database successfully')
        table_name = "teams"
        file_path = "/opt/airflow/dags/fpl_offence/data/epl_team.csv"
        team_epl = pd.read_csv(file_path)
        print('Data reading successfully')
        # Convert DataFrame to list of tuples for batch insertion
        records = [tuple(x) for x in team_epl.itertuples(index=False,
                                                         name=None)]

        insert_query = f"""
            INSERT INTO {table_name} (id, name, short_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """
        # Batch insert
        cursor.executemany(insert_query, records)
        connection.commit()
        logging.info('Data inserted successfully')

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def load_players():
    """
    Fetch and load player data into the "players" table in PostgreSQL.

    Replaces the table if it already exists.
    """
    try:
        logging.info("Establish connection & loading players into database..")
        postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        logging.info('Connected to the database successfully')
        table_name = "players"
        file_path = "/opt/airflow/dags/fpl_offence/data/team_players.csv"
        team = pd.read_csv(file_path)
        print('Data reading successfully')
        # Convert DataFrame to list of tuples for batch insertion
        records = [tuple(x) for x in team.itertuples(index=False, name=None)]

        insert_query = f"""
            INSERT INTO {table_name} (id, first_name, second_name, team_id,
            position_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """
        # Batch insert
        cursor.executemany(insert_query, records)
        connection.commit()
        logging.info('Data inserted successfully')

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
