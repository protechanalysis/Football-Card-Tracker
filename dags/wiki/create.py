from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from airflow import DAG

default_args = {'owner': 'adewunmi',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(
    dag_id="create_epl_tables",
    start_date=datetime(2025, 1, 2),
    default_args=default_args,
    schedule_interval=None
) as dag:

    create_postgres_table = PostgresOperator(
        task_id="wiki",
        postgres_conn_id="postgres_id",
        sql="""
        CREATE TABLE pageview_counts (
            pagename VARCHAR(50) NOT NULL,
            pageviewcount INT NOT NULL,
            datetime TIMESTAMP NOT NULL
        );
         """
    )


create_postgres_table
