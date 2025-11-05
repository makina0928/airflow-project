# ======================================= #
#                                         #
#   DLT + DBT Orchestration with Airflow 3 #
#                                         #
# ======================================= #
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from data_extraction.load_data import load_loans, load_customers, load_locations

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_pipeline_dlt_dbt",
    description="Daily DLT → DBT pipeline (modular tasks)",
    start_date=datetime(2025, 10, 20),
    schedule="10 1 * * *",  # 01:10 UTC
    catchup=False,
    default_args=default_args,
    tags=["dlt", "dbt", "etl"],
) as dag:

    t1_loans = PythonOperator(task_id="load_loans_task", python_callable=load_loans)
    t2_customers = PythonOperator(task_id="load_customers_task", python_callable=load_customers)
    t3_locations = PythonOperator(task_id="load_locations_task", python_callable=load_locations)

    t4_dbt = BashOperator(
        task_id="dbt_transform_task",
        bash_command="cd /opt/airflow/data_transformation && dbt build --profiles-dir ~/.dbt"
    )

    [t1_loans, t2_customers, t3_locations] >> t4_dbt
