import logging
import os
from dotenv import load_dotenv
from google.cloud import bigquery

from src.common.utils import setup_logging
from src.common.bigquery_operations import load_df_to_staging_table, merge_data_to_final_table
from src.ibge_pipeline.extractor import fetch_ibge_aggregate_data
from src.ibge_pipeline.transformer import transform_ibge_data

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
        "variable_name_meta": "IPCA - Variação Mensal (Índice Geral, Brasil)", # Nome mais específico
        "periods": "202301-202412", # Mantenha o período de teste menor por enquanto
        "localities": "N1[all]",
        "classification_filter": "315[7169]"  # <-- FILTRO ADICIONADO PARA ÍNDICE GERAL
    },
    {
        "indicator_name_table": "taxa_desocupacao_trimestral_brasil",
        "aggregate_code": "4099",
        "variable_code": "4099",
        "variable_name_meta": "Taxa de Desocupação Trimestral (Brasil)",
        "periods": "202301-202402",
        "localities": "N1[all]",
        "classification_filter": None # Ou omitir a chave, pois não precisa de filtro específico aqui
    },
    {
        "indicator_name_table": "pib_anual_valores_correntes_brasil",
        "aggregate_code": "5938",
        "variable_code": "37",
        "variable_name_meta": "PIB Anual a Preços Correntes (Brasil)",
        "periods": "2020-2022",
        "localities": "N1[all]",
        "classification_filter": None
    },
    {
        "indicator_name_table": "populacao_estimada_anual_brasil",
        "aggregate_code": "6579",
        "variable_code": "9324",
        "variable_name_meta": "População Estimada Anual (Brasil)",
        "periods": "2020-2022",
        "localities": "N1[all]",
        "classification_filter": None
    }
]

def run_full_ibge_pipeline_for_indicator(
    indicator_config: dict
) -> bool:
    
    indicator_name = indicator_config["indicator_name_table"]
    agg_code = indicator_config["aggregate_code"]
    var_code = indicator_config["variable_code"]
    var_name_meta = indicator_config["variable_name_meta"]
    periods = indicator_config["periods"]
    localities = indicator_config["localities"]

    logger.info(f"--- Iniciando pipeline para o indicador IBGE: {indicator_name} (Agregado: {agg_code}, Variável: {var_code}) ---")

    staging_table_id = f"ibge_{indicator_name}_staging"
    final_table_id = f"ibge_{indicator_name}"

    logger.info(f"Fase de Extração para {indicator_name}...")
    df_raw = fetch_ibge_aggregate_data(
        aggregate_code=agg_code,
        variable_codes=var_code,
        periods=periods,
        localities_specifier=localities
    )
    if df_raw is None or df_raw.empty:
        logger.warning(f"Nenhum dado extraído para {indicator_name} com os parâmetros fornecidos. Pulando este indicador.")
        return True

    logger.info(f"Fase de Transformação para {indicator_name}...")
    df_transformed = transform_ibge_data(
        df_raw=df_raw,
        aggregate_code_meta=agg_code,
        variable_code_meta=var_code,
        variable_name_meta=var_name_meta
    )
    if df_transformed.empty:
        logger.warning(f"Transformação resultou em DataFrame vazio para {indicator_name}. Pulando carregamento.")
        return True

    logger.info(f"Fase de Carregamento para {indicator_name}...")
    success_staging = load_df_to_staging_table(
        df_transformed, GCP_PROJECT_ID, BIGQUERY_DATASET_IBGE, staging_table_id, gcp_location=GCP_LOCATION
    )
    if not success_staging:
        logger.error(f"Falha ao carregar dados para a staging table de {indicator_name}. Abortando carregamento para este indicador.")
        return False

    success_merge = merge_data_to_final_table(
        GCP_PROJECT_ID, BIGQUERY_DATASET_IBGE, staging_table_id, final_table_id, gcp_location=GCP_LOCATION
    )
    
    if success_merge:
        logger.info(f"Operação MERGE para {final_table_id} bem-sucedida. Tentando deletar tabela de staging: {staging_table_id}...")
        try:
            client = bigquery.Client(project=GCP_PROJECT_ID)
            staging_table_ref = client.dataset(BIGQUERY_DATASET_IBGE).table(staging_table_id)
            client.delete_table(staging_table_ref, not_found_ok=True)
            logger.info(f"Tabela de staging {staging_table_id} deletada com sucesso.")
        except Exception as e:
            logger.warning(f"Falha ao tentar deletar a tabela de staging {staging_table_id}: {e}")
    elif not success_merge:
        logger.error(f"Falha na operação MERGE para a tabela final de {indicator_name}. A tabela de staging {staging_table_id} será mantida para depuração.")
        return False

    logger.info(f"--- Pipeline para o indicador IBGE: {indicator_name} concluído com sucesso! ---")
    return True


def run_all_ibge_pipelines():
    
    logger.info("===== Iniciando execução de todos os pipelines do IBGE =====")

    if GCP_PROJECT_ID is None:
        logger.error("A variável de ambiente GCP_PROJECT_ID não está configurada. Abortando.")
        return
    if BIGQUERY_DATASET_IBGE is None:
        logger.error("A variável de ambiente BIGQUERY_DATASET_IBGE não está configurada. Abortando.")
        return

    overall_success = True
    for indicator_config in IBGE_INDICATORS_TO_PROCESS:
        success = run_full_ibge_pipeline_for_indicator(indicator_config)
        if not success:
            overall_success = False
            logger.error(f"Pipeline falhou para o indicador IBGE: {indicator_config['indicator_name_table']}")
        logger.info("-" * 80)

    if overall_success:
        logger.info("===== Todos os pipelines do IBGE foram concluídos com sucesso! =====")
    else:
        logger.error("===== Execução dos pipelines do IBGE concluída com uma ou mais falhas. Verifique os logs. =====")

if __name__ == "__main__":
    logger.info("Executando main_ibge.py como script principal.")
    run_all_ibge_pipelines()