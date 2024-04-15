from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Param
from airflow.operators.python_operator import PythonOperator
from data_fetch_helpers import fetch_data_from_rest_api, process_api_data, fetch_data_from_csv, process_csv_data
from db_helpers import create_table, update_table, merge_duplicates, fetch_users_by_interest_and_signup_date

use_csv_param = "{{ params.use_csv }}"
db_name_param = "{{ params.db_name }}"
interest_param = "{{ params.interest }}"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'params': {
        "use_csv": Param(default=True, type="boolean", ),
        "db_name": Param(default="users.db", type="string", ),
        "interest": Param(default="coding", type="string", ),
    }
}

dag = DAG(
    'aaa_data_ingestion_and_processing',
    default_args=default_args,
    description='A DAG to ingest data from REST API and CSV file, process it, and store it in a database',
    schedule_interval=timedelta(hours=1),  # run hourly
)


def process_and_store_data(use_csv, db_name) -> None:
    # in a real scenario, this is where a config would be loaded to populate the fetch methods

    if use_csv:
        csv_data = fetch_data_from_csv()
        processed_data = process_csv_data(csv_data)
    else:
        api_data = fetch_data_from_rest_api()
        processed_data = process_api_data(api_data)

    create_table(db_name)
    update_table(db_name, 'users', processed_data)


def merge_duplicates_in_task(db_name) -> None:
    merge_duplicates(db_name)


def fetch_users_interest_signup(db_name, interest) -> None:
    users = fetch_users_by_interest_and_signup_date(db_name, interest)
    print(f"users interested in {interest} are:")
    print(users)


process_store_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_and_store_data,
    op_kwargs={'use_csv': use_csv_param, 'db_name': db_name_param},
    provide_context=True,
    dag=dag,
)

merge_duplicates_task = PythonOperator(
    task_id='merge_duplicates',
    python_callable=merge_duplicates_in_task,
    op_kwargs={'db_name': db_name_param},
    provide_context=True,
    dag=dag,
)

fetch_by_interest_task = PythonOperator(
    task_id='fetch_by_interest',
    python_callable=fetch_users_interest_signup,
    op_kwargs={'db_name': db_name_param, 'interest': interest_param},
    provide_context=True,
    dag=dag,
)

process_store_data_task >> merge_duplicates_task >> fetch_by_interest_task
