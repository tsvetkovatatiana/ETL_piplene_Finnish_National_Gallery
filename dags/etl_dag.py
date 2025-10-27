import sys
sys.path.append('/opt/airflow/src')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.load import run_etl

default_args = {
    "owner": "fng_data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "fng_etl_pipeline",
    default_args=default_args,
    description="Finnish National Gallery ETL pipeline",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 10, 25),
    catchup=False,
    tags=["fng", "etl", "supabase"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_fng_etl",
        python_callable=run_etl,
    )
    run_etl
