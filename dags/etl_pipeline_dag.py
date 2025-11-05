# ======================================= #
#                                         #
#   DLT + DBT Orchestration with Airflow 3 #
#                                         #
# ======================================= #

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import sys
from pathlib import Path
import dlt
from dotenv import load_dotenv


# ==================== #
#        Setup         #
# ==================== #

load_dotenv()

# Add local data_extraction folder to sys.path for imports
sys.path.insert(0, str(Path(__file__).parents[1] / "data_extraction"))

# Import your DLT pipeline source
from data_extraction.load_data import source

# PostgreSQL connection string from .env
connection_string = os.getenv("POSTGRES_CONNECTION_STRING")
pg_destination = dlt.destinations.postgres(connection_string)


# ==================== #
#   DLT load function  #
# ==================== #
def run_dlt_load():
    """Load local CSV data into PostgreSQL using DLT."""
    print(" Starting DLT pipeline for loans/customers/location...")
    pipeline = dlt.pipeline(
        pipeline_name="loans_pipeline",
        dataset_name="staging",
        destination=pg_destination,
    )
    pipeline.run(source())
    print(" DLT load completed successfully.")


# ==================== #
#        DAG           #
# ==================== #
with DAG(
    dag_id="etl_pipeline_dlt_dbt",
    description="Daily DLT → DBT ETL pipeline (dbt Core)",
    start_date=datetime(2025, 10, 20),
    schedule="10 1 * * *",  # 01:10 UTC = 04:10 Nairobi
    catchup=False,
    default_args={
        "owner": "airflow",
        "email_on_failure": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dlt", "dbt", "etl", "airflow3"],
) as dag:

    # Step 1 – Extract & Load CSVs via DLT
    t1_dlt_load = PythonOperator(
        task_id="dlt_load_task",
        python_callable=run_dlt_load,
    )

    # Step 2 – Transform using dbt Core CLI
    dbt_project_dir = Path(__file__).parents[1] / "data_transformation"
    t2_dbt_transform = BashOperator(
        task_id="dbt_transform_task",
        bash_command=f"cd {dbt_project_dir} && dbt build --profiles-dir ~/.dbt"
    )

    # DAG flow → DLT then DBT
    t1_dlt_load >> t2_dbt_transform
