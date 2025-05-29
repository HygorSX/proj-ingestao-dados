import pandas as pd
import logging

from src.common.utils import setup_logging
from src.ibge_pipeline.extractor import fetch_ibge_aggregate_data

setup_logging()
logger = logging.getLogger(__name__)

def run_single_extraction_test(
    indicator_name: str,
    aggregate_code: str,
    variable_codes: str,
    periods: str,
    localities_specifier: str = "N1[all]" 
):
    logger.info(f"\n--- Testando Extração para: {indicator_name} ---")
    logger.info(f"Agregado: {aggregate_code}, Variável(eis): {variable_codes}, Períodos: {periods}, Localidades: {localities_specifier}")

    df = fetch_ibge_aggregate_data(
        aggregate_code=aggregate_code,
        variable_codes=variable_codes,
        periods=periods,
        localities_specifier=localities_specifier
    )

    assert df is not None, f"{indicator_name}: A função retornou None, esperava-se um DataFrame."
    if df.empty:
        logger.warning(f"{indicator_name}: Nenhum dado retornado para os parâmetros fornecidos. Verifique os parâmetros ou a disponibilidade dos dados na API.")
    else:
        logger.info(f"{indicator_name}: Dados extraídos com sucesso ({len(df)} linhas).")
        print(f"\n{indicator_name} - Primeiras linhas do DataFrame:")
        print(df.head())
        print(f"\n{indicator_name} - Informações do DataFrame:")
        df.info(verbose=True, show_counts=True)
        
        expected_data_cols = ['V', 'D3C', 'D2C', 'D1C'] 
        
        for col in expected_data_cols:
            assert col in df.columns, f"{indicator_name}: Coluna de dados esperada '{col}' não encontrada. Colunas: {df.columns.tolist()}"
        logger.info(f"{indicator_name}: Verificação de colunas de dados ('V', 'D3C', 'D2C', 'D1C') - Aprovada.")
    logger.info(f"--- Teste para {indicator_name} concluído ---")
    return df

def run_all_ibge_extractor_tests():
    logger.info("===== Iniciando testes para ibge_pipeline.extractor =====")

    run_single_extraction_test(
        indicator_name="IPCA (Variação Mensal)",
        aggregate_code="1737",
        variable_codes="63",
        periods="202410-202412" 
    )

    run_single_extraction_test(
        indicator_name="Taxa de Desocupação (PNAD Contínua Trimestral)",
        aggregate_code="4099",
        variable_codes="4099",
        periods="202401,202402" 
    )

    run_single_extraction_test(
        indicator_name="PIB Anual (Valores Correntes)",
        aggregate_code="5938",
        variable_codes="37",
        periods="2020-2021" 
    )

    run_single_extraction_test(
        indicator_name="População Estimada Anual",
        aggregate_code="6579",
        variable_codes="9324",
        periods="2020-2021" 
    )

    logger.info("\n===== Testes para ibge_pipeline.extractor concluídos =====")

if __name__ == "__main__":
    run_all_ibge_extractor_tests()