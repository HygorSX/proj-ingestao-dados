from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys
from datetime import timedelta

# Ajustar o caminho para importar os módulos
DAGS_FOLDER = os.path.dirname(os.path.realpath(__file__))
SRC_PATH = os.path.join(DAGS_FOLDER, "src")
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

from ibge_pipeline.main_ibge import run_all_ibge_pipelines
from common.utils import setup_logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ibge_indicadores_pipeline",
    description="Pipeline diário de ingestão de dados do IBGE",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ibge", "bigquery"],
) as dag:

    def run_ibge_pipeline():
        setup_logging()
        run_all_ibge_pipelines()

    task = PythonOperator(
        task_id="run_ibge_pipeline",
        python_callable=run_ibge_pipeline,
    )
