# """
# Documentation of pageview format: 
# https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
# """

# Import neccessary classes and functions
from airflow.utils.dates import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from wiki.naj import get_data, fetch_pageviews

DAG_DIRECTORY = "/opt/airflow/dags/wiki_results"


with DAG(
    dag_id="wikipedia_pageviews",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    # template_searchpath="/tmp",
    # #setting a base path for templates at the DAG-level
    max_active_runs=1,
    catchup=False,
) as dag:

    get_datla = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        # Paasing templated keyword arguments to the function _get_data
        op_kwargs={
            "year": "{{ data_interval_start.year }}",
            "month": "{{ data_interval_start.month }}",
            "day": "{{ data_interval_start.day }}",
            "hour": "{{ data_interval_start.hour }}",
            "output_path": f"{DAG_DIRECTORY}/data/wikipageviews.gz",
        },
    )

    extract_gz = BashOperator(
        task_id="extract_gz",
        bash_command=f"gunzip --force {DAG_DIRECTORY}/data/wikipageviews.gz",
    )

    fetch_pagoeviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=fetch_pageviews,
        # Paasing templated keyword argument to the function _fetch_pageviews
        op_kwargs={"pagenames": {"Google", "Amazon", "Apple",
                                 "Microsoft", "Facebook"},
                   "dag_directory": DAG_DIRECTORY
                   },
    )

    write_to_postgres = PostgresOperator(
        task_id="write_to_postgres",
        # id of the connection defined through the web UI
        postgres_conn_id="my_postgres",
        # The relative path to the SQL file containing the SQL query to
        # execute on the Postgres db.
        sql="sql/load_pageviews.sql",
    )

get_datla >> extract_gz >> fetch_pagoeviews >> write_to_postgres
