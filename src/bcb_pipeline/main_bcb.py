import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from src.common.utils import setup_logging 
from src.bcb_pipeline.extractor import fetch_bcb_series_data
from src.bcb_pipeline.transformer import transform_bcb_data
from src.bcb_pipeline.loader import load_df_to_staging_table, merge_data_to_final_table

setup_logging()      
logger = logging.getLogger(__name__)

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET_BCB = os.getenv("BIGQUERY_DATASET_BCB", "dados_publicos_bcb") 
GCP_LOCATION = os.getenv("GCP_LOCATION", "southamerica-east1")

SERIES_TO_PROCESS = [
    {"name": "selic_diaria", "code": 11, "description": "Taxa de juros Selic diária"},
    {"name": "selic_acumulada_mes", "code": 4390, "description": "Taxa de juros Selic acumulada no mês"},
    {"name": "dolar_ptax_venda", "code": 1, "description": "Taxa de câmbio Dólar Americano PTAX Venda"},
    {"name": "euro_ptax_venda", "code": 21619, "description": "Taxa de câmbio Euro PTAX Venda"},
    {"name": "inadimplencia_credito_inst_publicas", "code": 13667, "description": "Inadimplência da carteira de crédito das instituições financeiras sob controle público - Total"},
    {"name": "saldo_credito_pf", "code": 20541, "description": "Saldo da carteira de crédito - Pessoas físicas - Total"}
]

def run_full_bcb_pipeline_for_series(
    series_name: str,
    series_code: int,
    start_date_str: str,
    end_date_str: str
) -> bool:
    logger.info(f"--- Iniciando pipeline para a série BCB: {series_name} (Código: {series_code}) ---")

    base_table_name = f"bcb_{series_name}"
    staging_table_id = f"{base_table_name}_staging"
    final_table_id = base_table_name

    logger.info(f"Fase de Extração para {series_name}...")
    df_raw = fetch_bcb_series_data(series_code, start_date_str, end_date_str)
    if df_raw is None or df_raw.empty:
        logger.warning(f"Nenhum dado extraído para {series_name} no período {start_date_str} - {end_date_str}. Pulando esta série.")
        return True

    logger.info(f"Fase de Transformação para {series_name}...")
    df_transformed = transform_bcb_data(df_raw, series_code)
    if df_transformed.empty:
        logger.warning(f"Transformação resultou em DataFrame vazio para {series_name}. Pulando carregamento.")
        return True

    logger.info(f"Fase de Carregamento para {series_name}...")
    success_staging = load_df_to_staging_table(
        df_transformed, GCP_PROJECT_ID, BIGQUERY_DATASET_BCB, staging_table_id, gcp_location=GCP_LOCATION
    )
    if not success_staging:
        logger.error(f"Falha ao carregar dados para a staging table de {series_name}. Abortando carregamento para esta série.")
        return False

    success_merge = merge_data_to_final_table(
        GCP_PROJECT_ID, BIGQUERY_DATASET_BCB, staging_table_id, final_table_id, gcp_location=GCP_LOCATION
    )
    
    # --- ADICIONAR LÓGICA DE DELEÇÃO DA STAGING TABLE ABAIXO ---
    if success_merge:
        logger.info(f"Operação MERGE para {final_table_id} bem-sucedida. Tentando deletar tabela de staging: {staging_table_id}...")
        try:
            client = bigquery.Client(project=GCP_PROJECT_ID)
            staging_table_ref = client.dataset(BIGQUERY_DATASET_BCB).table(staging_table_id)
            client.delete_table(staging_table_ref, not_found_ok=True) # not_found_ok=True não falha se a tabela já não existir
            logger.info(f"Tabela de staging {staging_table_id} deletada com sucesso.")
        except Exception as e:
            logger.warning(f"Falha ao tentar deletar a tabela de staging {staging_table_id}: {e}")
        # Continua mesmo se a deleção da staging falhar, pois o MERGE principal foi bem-sucedido.
    # --- FIM DA LÓGICA DE DELEÇÃO ---
    
    elif not success_merge:
        logger.error(f"Falha na operação MERGE para a tabela final de {series_name}. A tabela de staging {staging_table_id} será mantida para depuração.")
        return False

    logger.info(f"--- Pipeline para a série BCB: {series_name} concluído com sucesso! ---")
    return True

def run_all_bcb_pipelines(
    start_date_str: str = None, 
    end_date_str: str = None
):
    
    logger.info("===== Iniciando execução de todos os pipelines do BCB =====")

    if GCP_PROJECT_ID is None:
        logger.error("A variável de ambiente GCP_PROJECT_ID não está configurada. Abortando.")
        return

    if start_date_str is None or end_date_str is None:
        today = datetime.today()
        default_end_date = today
        default_start_date = today - timedelta(days=90)
        
        end_date_str = default_end_date.strftime("%d/%m/%Y")
        start_date_str = default_start_date.strftime("%d/%m/%Y")
        logger.info(f"Datas de extração não fornecidas. Usando período padrão: de {start_date_str} a {end_date_str} (últimos 90 dias).")

    overall_success = True
    for series_info in SERIES_TO_PROCESS:
        success = run_full_bcb_pipeline_for_series(
            series_name=series_info["name"],
            series_code=series_info["code"],
            start_date_str=start_date_str,
            end_date_str=end_date_str
        )
        if not success:
            overall_success = False
            logger.error(f"Pipeline falhou para a série: {series_info['name']} (Código: {series_info['code']})")
        logger.info("-" * 80) # Separador entre séries

    if overall_success:
        logger.info("===== Todos os pipelines do BCB foram concluídos com sucesso! =====")
    else:
        logger.error("===== Execução dos pipelines do BCB concluída com uma ou mais falhas. Verifique os logs. =====")


if __name__ == "__main__":
    logger.info("Executando main_bcb.py como script principal.")
    
    # Exemplo de como executar para um período específico:
    # run_all_bcb_pipelines(start_date_str="01/01/2024", end_date_str="31/01/2024")
    
run_all_bcb_pipelines()
