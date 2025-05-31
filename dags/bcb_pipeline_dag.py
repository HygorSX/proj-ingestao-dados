from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys
from datetime import timedelta

DAGS_FOLDER = os.path.dirname(os.path.realpath(__file__))
SRC_PATH = os.path.join(DAGS_FOLDER, "src")
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

from bcb_pipeline.main_bcb import run_all_bcb_pipelines
from common.utils import setup_logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bcb_indicadores_pipeline",
    description="Pipeline diário de ingestão de dados do BCB",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["bcb", "bigquery"],
) as dag:

    def run_bcb_pipeline():
        setup_logging()
        run_all_bcb_pipelines()

    task = PythonOperator(
        task_id="run_bcb_pipeline",
        python_callable=run_bcb_pipeline,
    )
