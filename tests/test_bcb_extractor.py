# src/tests/test_bcb_extractor.py
import logging
from src.common.utils import setup_logging # Importa a configuração
from src.bcb_pipeline.extractor import fetch_bcb_series_data # Importa a função a ser testada

# Configura o logging para este script de teste
setup_logging()
logger = logging.getLogger(__name__)

def run_tests():
    logger.info("Iniciando testes para bcb_pipeline.extractor...")

    # Teste 1: Série SELIC diária (código 11)
    codigo_selic_diaria = 11
    # Use datas que você sabe que têm dados ou que são boas para teste
    data_inicio_teste = "01/05/2025" 
    data_fim_teste = "05/05/2025"

    logger.info(f"Testando fetch_bcb_series_data com código {codigo_selic_diaria}, de {data_inicio_teste} a {data_fim_teste}")
    df_selic = fetch_bcb_series_data(
        series_code=codigo_selic_diaria,
        start_date=data_inicio_teste,
        end_date=data_fim_teste
    )

    if not df_selic.empty:
        print("\n--- Dados SELIC (raw) ---")
        print(df_selic.head())
        print(f"Total de registros retornados: {len(df_selic)}")
        assert len(df_selic) > 0 # Exemplo de uma asserção simples
    else:
        # Esta condição pode indicar um problema se você espera dados
        logger.warning(f"Nenhum dado retornado para SELIC (código {codigo_selic_diaria}) no período testado.")


    # Teste 2: Tentativa com um código de série que causa timeout/erro
    codigo_inexistente = 9999999 # Este código causou timeout no teste anterior
    logger.info(f"\nTestando fetch_bcb_series_data com código {codigo_inexistente}...")
    df_inexistente = fetch_bcb_series_data(
        series_code=codigo_inexistente,
        start_date="01/01/2024", # Datas curtas para o teste
        end_date="02/01/2024"
    )
    if df_inexistente.empty:
        logger.info(f"Teste com código {codigo_inexistente} retornou DataFrame vazio, como esperado.")
        assert df_inexistente.empty # Exemplo de asserção
    else:
        logger.error(f"Teste com código {codigo_inexistente} retornou dados, o que não era esperado.")
        print(df_inexistente)
        assert False # Falha o teste se dados inesperados forem retornados

    logger.info("\nFim dos testes para bcb_pipeline.extractor.")

if __name__ == "__main__":
    run_tests()