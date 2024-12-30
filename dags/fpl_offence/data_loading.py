from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

from dags.fpl_offence.extraction import (fetch_fixtures, fetch_players,
                                         fetch_position, fetch_teams,
                                         manu_player)


def load_manu_stats():

    player_game_stats = manu_player()
    # Connect to PostgreSQL using Airflow's PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Define table name
    table_name = "player_stats"

    player_game_stats.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)


def load_fixtures():

    fixtures_dd = fetch_fixtures()
    # Connect to PostgreSQL using Airflow's PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Define table name
    table_name = "team_fixtures"

    fixtures_dd.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)


def load_position():

    player_position = fetch_position()
    # Connect to PostgreSQL using Airflow's PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Define table name
    table_name = "position"

    player_position.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)


def load_teams():

    teams = fetch_teams()
    # Connect to PostgreSQL using Airflow's PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Define table name
    table_name = "teams"

    teams.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)


def load_players():

    players_data = fetch_players()
    # Connect to PostgreSQL using Airflow's PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Define table name
    table_name = "players"

    players_data.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False)


def load_offence_into_table():
    # Path to the SQL script
    sql_file_path = "dags/fpl_offence/offence.sql"

    # Read the SQL script content
    with open(sql_file_path, "r") as file:
        sql_query = file.read()

    # Connect to PostgreSQL using Airflow's PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Define your target table
    target_table = "alert"  # Replace with your actual target table name

    # Execute the SQL query and load the results into the target table
    with engine.connect() as connection:
        with connection.begin():
            # Step 1: Truncate the target table to clear existing data
            truncate_query = f"TRUNCATE TABLE {target_table};"
            connection.execute(text(truncate_query))
            print(f"Truncated table {target_table}")

            # Step 2: Execute the SQL query and fetch results
            query_result = connection.execute(text(sql_query))

            if query_result.returns_rows:
                results = query_result.fetchall()
                print(f"Fetched {len(results)} rows from query.")

                # Step 3: Insert results into the target table
                if results:
                    column_count = len(results[0])  # Get number of columns
                    placeholders = ", ".join(["%s"] * column_count)
                    insert_query = f"""
                        INSERT INTO {target_table}
                        VALUES ({placeholders});
                    """
                    rows = [tuple(row) for row in results]
                    connection.execute(insert_query, rows)
                    print(f"Inserted {len(rows)} rows into table \
                          {target_table}")
                else:
                    print("No data to insert.")
            else:
                print("Query did not return any rows.")
