import os
import logging
from dotenv import load_dotenv
from google.cloud import bigquery

from src.common.utils import setup_logging
from src.common.bigquery_operations import load_df_to_staging_table, merge_data_to_final_table
from src.ibge_pipeline.extractor import fetch_ibge_aggregate_data
from src.ibge_pipeline.transformer import transform_ibge_data

# Setup
setup_logging()
logger = logging.getLogger(__name__)
load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET_IBGE = os.getenv("BIGQUERY_DATASET_IBGE", "dados_publicos_ibge")
GCP_LOCATION = os.getenv("GCP_LOCATION", "southamerica-east1")

IBGE_INDICATORS_TO_PROCESS = [
    {
        "indicator_name_table": "ipca_variacao_mensal_brasil",
        "aggregate_code": "1737",
        "variable_code": "63",
        "variable_name_meta": "IPCA - Variação Mensal (Índice Geral, Brasil)",
        "periods": "202301-202412",
        "localities": "N1[all]",
        "classification_filter": "315[7169]"
    },
    {
        "indicator_name_table": "taxa_desocupacao_trimestral_brasil",
        "aggregate_code": "4099",
        "variable_code": "4099",
        "variable_name_meta": "Taxa de Desocupação Trimestral (Brasil)",
        "periods": "202301-202402",
        "localities": "N1[all]"
    },
    {
        "indicator_name_table": "pib_anual_valores_correntes_brasil",
        "aggregate_code": "5938",
        "variable_code": "37",
        "variable_name_meta": "PIB Anual a Preços Correntes (Brasil)",
        "periods": "2020-2022",
        "localities": "N1[all]"
    },
    {
        "indicator_name_table": "populacao_estimada_anual_brasil",
        "aggregate_code": "6579",
        "variable_code": "9324",
        "variable_name_meta": "População Estimada Anual (Brasil)",
        "periods": "2020-2022",
        "localities": "N1[all]"
    }
]


def run_full_ibge_pipeline_for_indicator(config: dict) -> bool:
    name = config["indicator_name_table"]
    agg_code = config["aggregate_code"]
    var_code = config["variable_code"]
    var_name = config["variable_name_meta"]
    periods = config["periods"]
    localities = config["localities"]
    filter_code = config.get("classification_filter")

    logger.info(f"--- Iniciando pipeline IBGE: {name} ---")

    staging_id = f"ibge_{name}_staging"
    final_id = f"ibge_{name}"

    df_raw = fetch_ibge_aggregate_data(
        aggregate_code=agg_code,
        variable_codes=var_code,
        periods=periods,
        localities_specifier=localities
    )
    if df_raw.empty:
        logger.warning(f"[{name}] Nenhum dado extraído. Pulando.")
        return True

    df_transformed = transform_ibge_data(
        df_raw=df_raw,
        aggregate_code=agg_code,
        variable_code=var_code,
        variable_name=var_name
    )
    if df_transformed.empty:
        logger.warning(f"[{name}] DataFrame transformado está vazio. Pulando.")
        return True

    if not load_df_to_staging_table(df_transformed, GCP_PROJECT_ID, BIGQUERY_DATASET_IBGE, staging_id, gcp_location=GCP_LOCATION):
        logger.error(f"[{name}] Falha ao carregar staging.")
        return False

    if not merge_data_to_final_table(GCP_PROJECT_ID, BIGQUERY_DATASET_IBGE, staging_id, final_id, gcp_location=GCP_LOCATION):
        logger.error(f"[{name}] Falha na operação MERGE.")
        return False

    try:
        bigquery.Client(project=GCP_PROJECT_ID).delete_table(
            f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_IBGE}.{staging_id}", not_found_ok=True
        )
        logger.info(f"[{name}] Tabela de staging '{staging_id}' deletada com sucesso.")
    except Exception as e:
        logger.warning(f"[{name}] Erro ao deletar tabela de staging: {e}")

    logger.info(f"--- Pipeline IBGE: {name} finalizado com sucesso ---")
    return True


def run_all_ibge_pipelines():
    logger.info("==== Execução de todos os pipelines IBGE iniciada ====")

    if not GCP_PROJECT_ID or not BIGQUERY_DATASET_IBGE:
        logger.error("Variáveis de ambiente GCP_PROJECT_ID ou BIGQUERY_DATASET_IBGE não estão definidas. Abortando.")
        return

    sucesso_geral = True
    for indicador in IBGE_INDICATORS_TO_PROCESS:
        sucesso = run_full_ibge_pipeline_for_indicator(indicador)
        if not sucesso:
            logger.error(f"[{indicador['indicator_name_table']}] Pipeline falhou.")
            sucesso_geral = False
        logger.info("-" * 80)

    if sucesso_geral:
        logger.info("Todos os pipelines do IBGE foram executados com sucesso.")
    else:
        logger.error("Um ou mais pipelines do IBGE falharam. Verifique os logs.")


if __name__ == "__main__":
    logger.info("main_ibge.py executado diretamente.")
    run_all_ibge_pipelines()