import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery

from src.common.utils import setup_logging
from src.bcb_pipeline.extractor import fetch_bcb_series_data
from src.bcb_pipeline.transformer import transform_bcb_data
from src.common.bigquery_operations import (
    load_df_to_staging_table,
    merge_data_to_final_table
)

setup_logging()
logger = logging.getLogger(__name__)
load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET_BCB = os.getenv("BIGQUERY_DATASET_BCB", "dados_publicos_bcb")
GCP_LOCATION = os.getenv("GCP_LOCATION", "southamerica-east1")

SERIES_TO_PROCESS = [
    {"name": "selic_diaria", "code": 11},
    {"name": "selic_acumulada_mes", "code": 4390},
    {"name": "dolar_ptax_venda", "code": 1},
    {"name": "euro_ptax_venda", "code": 21619},
    {"name": "inadimplencia_credito_inst_publicas", "code": 13667},
    {"name": "saldo_credito_pf", "code": 20541},
]


def run_full_bcb_pipeline_for_series(
    series_name: str,
    series_code: int,
    start_date: str,
    end_date: str
) -> bool:
    logger.info(f"--- Iniciando pipeline para: {series_name} (código {series_code}) ---")

    base_name = f"bcb_{series_name}"
    staging_id = f"{base_name}_staging"
    final_id = base_name

    df_raw = fetch_bcb_series_data(series_code, start_date, end_date)
    if df_raw.empty:
        logger.warning(f"[{series_name}] Nenhum dado extraído. Pulando.")
        return True

    df_transformed = transform_bcb_data(df_raw, series_code)
    if df_transformed.empty:
        logger.warning(f"[{series_name}] Transformação vazia. Pulando.")
        return True

    if not load_df_to_staging_table(df_transformed, GCP_PROJECT_ID, BIGQUERY_DATASET_BCB, staging_id, gcp_location=GCP_LOCATION):
        logger.error(f"[{series_name}] Falha no carregamento para staging.")
        return False

    if not merge_data_to_final_table(GCP_PROJECT_ID, BIGQUERY_DATASET_BCB, staging_id, final_id, gcp_location=GCP_LOCATION):
        logger.error(f"[{series_name}] Falha na operação MERGE.")
        return False

    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        client.delete_table(f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_BCB}.{staging_id}", not_found_ok=True)
        logger.info(f"[{series_name}] Tabela de staging '{staging_id}' deletada.")
    except Exception as e:
        logger.warning(f"[{series_name}] Erro ao deletar tabela de staging: {e}")

    logger.info(f"--- Pipeline para {series_name} concluído com sucesso ---")
    return True


def run_all_bcb_pipelines(start_date: str = None, end_date: str = None) -> None:
    logger.info("==== Iniciando execução dos pipelines do BCB ====")

    if not GCP_PROJECT_ID:
        logger.error("Variável GCP_PROJECT_ID ausente. Abortando.")
        return

    if not start_date or not end_date:
        today = datetime.today()
        start_date = (today - timedelta(days=90)).strftime("%d/%m/%Y")
        end_date = today.strftime("%d/%m/%Y")
        logger.info(f"Usando período padrão de 90 dias: {start_date} a {end_date}")

    sucesso_geral = True
    for serie in SERIES_TO_PROCESS:
        sucesso = run_full_bcb_pipeline_for_series(
            serie["name"], serie["code"], start_date, end_date
        )
        if not sucesso:
            logger.error(f"[{serie['name']}] Pipeline falhou.")
            sucesso_geral = False
        logger.info("-" * 80)

    if sucesso_geral:
        logger.info("Todos os pipelines BCB executados com sucesso.")
    else:
        logger.error("Um ou mais pipelines do BCB falharam. Veja os logs.")


if __name__ == "__main__":
    logger.info("Executando script main_bcb.py diretamente.")
    run_all_bcb_pipelines()
