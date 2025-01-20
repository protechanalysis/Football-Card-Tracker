import csv
from datetime import datetime
from datetime import timedelta
from urllib import request
from airflow import DAG
# from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Define the DAG
dag = DAG(
    dag_id="adewunmi",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
)


# Python function to download data
def _get_data(output_path, **context):
    # Extract execution date from Airflow context
    execution_date = context["execution_date"]
    adjusted_date = execution_date - timedelta(hours=1)
    year, month, day, hour = adjusted_date.year, adjusted_date.month, adjusted_date.day, adjusted_date.hour
    # year, month, day, hour = context["execution_date"].timetuple()
    # year, month, day, hour = execution_date.year, execution_date.month, \
    # execution_date.day, execution_date.hour

    # Construct the URL
    url = f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:02}/pageviews-{year}{month:02}{day:02}-{hour:02}0000.gz"
    # Download the file
    request.urlretrieve(url, output_path)


# Define the PythonOperator
get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={"output_path": "/tmp/wikipageviews.gz"},
    dag=dag,
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
    dag=dag,
)


def _fetch_pageviews(pagenames, dag_directory="/opt/airflow/dags", **context):
    execution_date = context["execution_date"]
    adjusted_date = execution_date - timedelta(hours=1)
    year, month, day, hour = adjusted_date.year, adjusted_date.month, adjusted_date.day, adjusted_date.hour
    time_frame = f"{year}{month:02}{day:02}-{hour:02}"
    result = dict.fromkeys(pagenames, 0)
    with open("/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
    print(result)
    output_csv = f"{dag_directory}/pageviews_{time_frame}.csv"
    
    # Save the result to the CSV file
    with open(output_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Page Title", "View Counts"])  # Header
        for page_title, view_counts in result.items():
            writer.writerow([page_title, view_counts])


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook"}
            },
    dag=dag,
)

get_data >> extract_gz >> fetch_pageviews
