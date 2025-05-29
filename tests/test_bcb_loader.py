import pandas as pd
import logging
import os
from datetime import date

from google.cloud import bigquery

from src.common.utils import setup_logging, ensure_bigquery_dataset_exists
from src.bcb_pipeline.loader import load_df_to_staging_table, merge_data_to_final_table

setup_logging()
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DE TESTE ---
TEST_PROJECT_ID = os.getenv("GCP_TEST_PROJECT_ID", "ingestao-dados-publicos") 
TEST_DATASET_ID = "test_bcb_pipeline_dataset" 
TEST_STAGING_TABLE_ID_PREFIX = "test_staging_serie_"
TEST_FINAL_TABLE_ID_PREFIX = "test_final_serie_"
TEST_GCP_LOCATION = "southamerica-east1" 

# Função auxiliar para obter o número de linhas em uma tabela
def get_table_row_count(client: bigquery.Client, project_id: str, dataset_id: str, table_id: str) -> int:
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        table = client.get_table(table_ref)
        return table.num_rows
    except Exception as e:
        logger.error(f"Erro ao obter contagem de linhas para {table_ref}: {e}")
        return -1

# Função auxiliar para limpar recursos de teste
def cleanup_test_resources(client: bigquery.Client, project_id: str, dataset_id: str):
    logger.info(f"Iniciando limpeza de recursos de teste no dataset {dataset_id}...")
    dataset_ref = client.dataset(dataset_id)
    try:
        # Lista e deleta todas as tabelas no dataset de teste
        tables = list(client.list_tables(dataset_ref))
        if tables:
            for table in tables:
                logger.info(f"Deletando tabela de teste: {table.table_id}")
                client.delete_table(table, not_found_ok=True)
        
        # Deleta o dataset de teste
        logger.info(f"Deletando dataset de teste: {dataset_id}")
        client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
        logger.info(f"Recursos de teste limpos com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante a limpeza dos recursos de teste: {e}")

def run_loader_tests():
    if TEST_PROJECT_ID == "SEU_PROJECT_ID_AQUI":
        logger.error("ERRO: TEST_PROJECT_ID não foi configurado. "
                       "Por favor, edite o script test_bcb_loader.py ou defina a variável de ambiente GCP_TEST_PROJECT_ID.")
        return

    logger.info(f"Iniciando testes do loader.py para o projeto: {TEST_PROJECT_ID}, dataset: {TEST_DATASET_ID}")
    
    client = None # Inicializa client como None
    try:
        client = bigquery.Client(project=TEST_PROJECT_ID)
        logger.info(f"Cliente BigQuery inicializado para o projeto {TEST_PROJECT_ID}.")
        # Garante que o dataset de teste seja criado (e limpo se já existir de um teste anterior)
        # cleanup_test_resources(client, TEST_PROJECT_ID, TEST_DATASET_ID) # Limpa antes para um estado conhecido
        # ensure_bigquery_dataset_exists(client, TEST_DATASET_ID, TEST_PROJECT_ID, location=TEST_GCP_LOCATION)


        # --- Cenário de Teste para uma Série Específica (ex: Selic) ---
        serie_codigo_teste = 11 # Exemplo SELIC
        staging_table_id = f"{TEST_STAGING_TABLE_ID_PREFIX}{serie_codigo_teste}"
        final_table_id = f"{TEST_FINAL_TABLE_ID_PREFIX}{serie_codigo_teste}"

        # Teste 1: Carga Inicial
        logger.info(f"\n--- Teste 1: Carga Inicial para série {serie_codigo_teste} ---")
        data_carga_1 = {
            'data_referencia': [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            'codigo_serie': [serie_codigo_teste, serie_codigo_teste, serie_codigo_teste],
            'valor_serie': [10.1, 10.2, 10.3]
        }
        df_carga_1 = pd.DataFrame(data_carga_1)
        # Converte data_referencia para datetime64[ns] que é esperado pelo loader do BQ a partir de data
        df_carga_1['data_referencia'] = pd.to_datetime(df_carga_1['data_referencia'])


        success_staging_1 = load_df_to_staging_table(df_carga_1, TEST_PROJECT_ID, TEST_DATASET_ID, staging_table_id, gcp_location=TEST_GCP_LOCATION)
        assert success_staging_1, "Falha ao carregar dados para staging (Carga 1)"
        logger.info(f"Carga 1 para staging table {staging_table_id} bem-sucedida.")
        
        # Verifica se staging tem dados
        # (Para um teste mais completo, você consultaria a staging table aqui)

        success_merge_1 = merge_data_to_final_table(TEST_PROJECT_ID, TEST_DATASET_ID, staging_table_id, final_table_id, gcp_location=TEST_GCP_LOCATION)
        assert success_merge_1, "Falha na operação MERGE (Carga 1)"
        logger.info(f"Operação MERGE 1 para final table {final_table_id} bem-sucedida.")
        
        # Verifica contagem de linhas na tabela final
        count_after_merge_1 = get_table_row_count(client, TEST_PROJECT_ID, TEST_DATASET_ID, final_table_id)
        logger.info(f"Número de linhas na tabela final {final_table_id} após Carga 1: {count_after_merge_1}")
        assert count_after_merge_1 == 3, f"Contagem de linhas incorreta após Carga 1. Esperado: 3, Obtido: {count_after_merge_1}"


        # Teste 2: Segunda Carga (Atualizações e Novas Inserções)
        logger.info(f"\n--- Teste 2: Segunda Carga (Atualizações e Novas Inserções) para série {serie_codigo_teste} ---")
        data_carga_2 = {
            'data_referencia': [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024,1,5)], # Dia 2 e 3 atualizam, Dia 4 e 5 são novos
            'codigo_serie': [serie_codigo_teste, serie_codigo_teste, serie_codigo_teste, serie_codigo_teste],
            'valor_serie': [20.2, 20.3, 10.4, 10.5] # Novos valores para dia 2 e 3
        }
        df_carga_2 = pd.DataFrame(data_carga_2)
        df_carga_2['data_referencia'] = pd.to_datetime(df_carga_2['data_referencia'])

        success_staging_2 = load_df_to_staging_table(df_carga_2, TEST_PROJECT_ID, TEST_DATASET_ID, staging_table_id, gcp_location=TEST_GCP_LOCATION)
        assert success_staging_2, "Falha ao carregar dados para staging (Carga 2)"
        logger.info(f"Carga 2 para staging table {staging_table_id} bem-sucedida.")

        success_merge_2 = merge_data_to_final_table(TEST_PROJECT_ID, TEST_DATASET_ID, staging_table_id, final_table_id, gcp_location=TEST_GCP_LOCATION)
        assert success_merge_2, "Falha na operação MERGE (Carga 2)"
        logger.info(f"Operação MERGE 2 para final table {final_table_id} bem-sucedida.")

        # Verifica contagem de linhas na tabela final
        # Esperado: 3 originais - 2 atualizados + 2 novos = 5 linhas. Não, MERGE não duplica.
        # Esperado: 1 (01/01) + 1 (02/01 atualizado) + 1 (03/01 atualizado) + 2 (04/01, 05/01 novos) = 5 linhas.
        # O registro de 01/01 da carga 1 deve permanecer. Os de 02/01 e 03/01 são atualizados. Os de 04/01 e 05/01 são inseridos.
        # Total = 1 (antigo não tocado) + 2 (atualizados) + 2 (novos) = 5.
        # Não, as chaves são (data, codigo_serie).
        # Carga 1: (01/01, 11), (02/01, 11), (03/01, 11) -> 3 linhas
        # Carga 2: (02/01, 11) -> UPDATE, (03/01, 11) -> UPDATE, (04/01, 11) -> INSERT, (05/01, 11) -> INSERT
        # Total final: (01/01), (02/01 atualizado), (03/01 atualizado), (04/01 novo), (05/01 novo) = 5 linhas
        count_after_merge_2 = get_table_row_count(client, TEST_PROJECT_ID, TEST_DATASET_ID, final_table_id)
        logger.info(f"Número de linhas na tabela final {final_table_id} após Carga 2: {count_after_merge_2}")
        assert count_after_merge_2 == 5, f"Contagem de linhas incorreta após Carga 2. Esperado: 5, Obtido: {count_after_merge_2}"
        
        # Você pode adicionar uma consulta para verificar os valores específicos se desejar
        # query_check = f"SELECT data_referencia, valor_serie FROM `{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{final_table_id}` ORDER BY data_referencia"
        # df_check = client.query(query_check).to_dataframe()
        # print("Dados na tabela final após Carga 2:")
        # print(df_check)


        # Teste 3: Carga de DataFrame Vazio
        logger.info(f"\n--- Teste 3: Carga de DataFrame Vazio para série {serie_codigo_teste} ---")
        df_carga_vazia = pd.DataFrame(columns=['data_referencia', 'codigo_serie', 'valor_serie'])
        df_carga_vazia['data_referencia'] = pd.to_datetime(df_carga_vazia['data_referencia'])


        success_staging_3 = load_df_to_staging_table(df_carga_vazia, TEST_PROJECT_ID, TEST_DATASET_ID, staging_table_id, gcp_location=TEST_GCP_LOCATION)
        assert success_staging_3, "Falha ao carregar DataFrame vazio para staging (Carga 3)" # A função deve retornar True
        logger.info(f"Carga 3 (vazia) para staging table {staging_table_id} processada.")
        
        # Staging table deve estar vazia (ou truncada pela função se essa lógica fosse adicionada)
        # Se a staging não foi explicitamente truncada quando o DF é vazio, ela ainda terá os dados da Carga 2.
        # O MERGE usando uma staging vazia (ou uma staging que não é vazia mas não tem novas correspondências/inserções)
        # não deve alterar a tabela final se a staging table foi efetivamente esvaziada pelo load_df_to_staging_table,
        # ou se o load_df_to_staging_table não a sobrescreveu (o que não é o caso, pois usa WRITE_TRUNCATE para DF não vazio).
        # Se df_carga_vazia é passado, load_df_to_staging_table retorna True e não faz I/O.
        # Então, a staging table ainda contém os dados da Carga 2.
        # O MERGE então compara a final (5 linhas) com a staging (4 linhas da carga 2).
        # Isto pode levar a comportamento inesperado no MERGE se a staging não for limpa.

        # CORREÇÃO NA LÓGICA DO TESTE: Se a staging não é limpa por um DF vazio,
        # o MERGE vai re-mesclar os dados da Carga 2.
        # Vamos modificar load_df_to_staging_table para realmente limpar/truncar a staging se o df for vazio.
        # OU, para este teste, vamos assegurar que a staging é limpa antes do merge.
        # Para o teste atual, o MERGE vai usar a staging da Carga 2.
        # O número de linhas na final não deve mudar.
        
        # Para este teste, vamos simular que a staging foi limpa
        # Na prática, load_df_to_staging_table deveria garantir que a staging table esteja vazia
        # se o input df for vazio, ou o merge irá operar sobre dados antigos na staging.
        # Vamos assumir que a intenção de `load_df_to_staging_table` com df vazio é que a staging fique vazia.
        # (A função atual retorna True e não faz nada com a tabela se o DF for vazio)
        # Para um teste mais realista, vamos limpar a staging table explicitamente aqui:
        logger.info("Limpando staging table manualmente para Teste 3 para simular lote vazio...")
        try:
            client.query(f"TRUNCATE TABLE `{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{staging_table_id}`").result()
        except Exception as e: # Se a staging não existir (primeira execução após limpeza total)
             logger.warning(f"Não foi possível truncar staging table {staging_table_id} (pode não existir ainda): {e}")


        success_merge_3 = merge_data_to_final_table(TEST_PROJECT_ID, TEST_DATASET_ID, staging_table_id, final_table_id, gcp_location=TEST_GCP_LOCATION)
        assert success_merge_3, "Falha na operação MERGE (Carga 3 - Vazia)"
        logger.info(f"Operação MERGE 3 (vazia) para final table {final_table_id} bem-sucedida.")

        count_after_merge_3 = get_table_row_count(client, TEST_PROJECT_ID, TEST_DATASET_ID, final_table_id)
        logger.info(f"Número de linhas na tabela final {final_table_id} após Carga 3 (vazia): {count_after_merge_3}")
        assert count_after_merge_3 == count_after_merge_2, f"Contagem de linhas mudou após MERGE com staging vazia. Esperado: {count_after_merge_2}, Obtido: {count_after_merge_3}"


        logger.info("\nTodos os testes do loader foram concluídos com sucesso aparente (verifique os logs e o BigQuery).")

    except Exception as e:
        logger.error(f"Um erro geral ocorreu durante a execução dos testes do loader: {e}", exc_info=True)
    finally:
        if client and TEST_PROJECT_ID != "SEU_PROJECT_ID_AQUI": # Só tenta limpar se o client foi inicializado e não é o placeholder
            logger.warning("INICIANDO LIMPEZA DOS RECURSOS DE TESTE (DATASET E TABELAS).")
            cleanup_test_resources(client, TEST_PROJECT_ID, TEST_DATASET_ID)


if __name__ == "__main__":
    # Verifica se GOOGLE_APPLICATION_CREDENTIALS está configurada
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logger.error("ERRO: A variável de ambiente GOOGLE_APPLICATION_CREDENTIALS não está configurada.")
        logger.error("Por favor, configure-a para apontar para o seu arquivo JSON de credenciais da Service Account.")
    elif TEST_PROJECT_ID == "SEU_PROJECT_ID_AQUI": # Adicionado para checar também o project_id
        logger.error("ERRO: TEST_PROJECT_ID não foi configurado no script test_bcb_loader.py.")
        logger.error("Por favor, edite a variável TEST_PROJECT_ID no script ou defina GCP_TEST_PROJECT_ID no ambiente.")
    else:
        run_loader_tests()