import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
import logging
import numpy as np

from src.common.utils import setup_logging
from src.bcb_pipeline.transformer import transform_bcb_data

setup_logging()
logger = logging.getLogger(__name__)

def run_all_transformer_tests():
    logger.info("Iniciando testes para bcb_pipeline.transformer.transform_bcb_data...")
    
    series_code_for_test = 11 

    # --- Teste 1: Caminho Feliz (Dados Válidos) ---
    logger.info("\n--- Teste 1: Caminho Feliz (Dados Válidos) ---")
    raw_data_valid = {
        'data': ['01/01/2024', '02/01/2024', '03/01/2024'],
        'valor': ['10.50', '11,75', '12']
    }
    df_raw_valid = pd.DataFrame(raw_data_valid)
    df_transformed_valid = transform_bcb_data(df_raw_valid, series_code_for_test)

    print("DataFrame Transformado (Válido):")
    print(df_transformed_valid)

    assert not df_transformed_valid.empty, "Resultado do Teste 1 não deveria ser vazio"
    assert len(df_transformed_valid) == 3, "Teste 1: Número de linhas incorreto"
    assert 'data_referencia' in df_transformed_valid.columns, "Teste 1: Coluna 'data_referencia' ausente"
    assert is_datetime64_any_dtype(df_transformed_valid['data_referencia']), "Teste 1: 'data_referencia' não é datetime"
    assert 'codigo_serie' in df_transformed_valid.columns, "Teste 1: Coluna 'codigo_serie' ausente"
    assert df_transformed_valid['codigo_serie'].iloc[0] == series_code_for_test, "Teste 1: Valor de 'codigo_serie' incorreto"
    assert 'valor_serie' in df_transformed_valid.columns, "Teste 1: Coluna 'valor_serie' ausente"
    assert is_numeric_dtype(df_transformed_valid['valor_serie']), "Teste 1: 'valor_serie' não é numérica"
    # Verifica se os valores foram convertidos corretamente (11,75 -> 11.75, 12 -> 12.0)
    assert df_transformed_valid['valor_serie'].iloc[1] == 11.75, "Teste 1: Conversão de valor com vírgula falhou"
    assert df_transformed_valid['valor_serie'].iloc[2] == 12.0, "Teste 1: Conversão de valor inteiro para float falhou"
    logger.info("Teste 1: Caminho Feliz - Aprovado")

    # --- Teste 2: DataFrame de Entrada Vazio ---
    logger.info("\n--- Teste 2: DataFrame de Entrada Vazio ---")
    df_raw_empty = pd.DataFrame({'data': pd.Series(dtype='str'), 'valor': pd.Series(dtype='str')})
    df_transformed_empty = transform_bcb_data(df_raw_empty, series_code_for_test)

    print("DataFrame Transformado (Entrada Vazia):")
    print(df_transformed_empty)
    assert df_transformed_empty.empty, "Teste 2: Resultado deveria ser um DataFrame vazio"
    logger.info("Teste 2: DataFrame de Entrada Vazio - Aprovado")

    # --- Teste 3: Dados com Datas e Valores Inválidos ---
    logger.info("\n--- Teste 3: Datas e Valores Inválidos ---")
    raw_data_invalid_formats = {
        'data': ['30/02/2024', 'texto-invalido', '03/03/2024'],
        'valor': ['100.0', 'abc', '200,50']
    }
    df_raw_invalid_formats = pd.DataFrame(raw_data_invalid_formats)
    df_transformed_invalid_formats = transform_bcb_data(df_raw_invalid_formats, series_code_for_test)

    print("DataFrame Transformado (Formatos Inválidos):")
    print(df_transformed_invalid_formats)

    assert not df_transformed_invalid_formats.empty, "Resultado do Teste 3 não deveria ser vazio"
    assert len(df_transformed_invalid_formats) == 3, "Teste 3: Número de linhas incorreto"
    # Verifica NaT (Not a Time) para datas inválidas
    assert pd.isna(df_transformed_invalid_formats['data_referencia'].iloc[0]), "Teste 3: Primeira data inválida não é NaT"
    assert pd.isna(df_transformed_invalid_formats['data_referencia'].iloc[1]), "Teste 3: Segunda data inválida não é NaT"
    assert not pd.isna(df_transformed_invalid_formats['data_referencia'].iloc[2]), "Teste 3: Data válida tornou-se NaT"
    # Verifica NaN (Not a Number) para valores inválidos
    assert pd.isna(df_transformed_invalid_formats['valor_serie'].iloc[1]), "Teste 3: Valor inválido não é NaN"
    assert not pd.isna(df_transformed_invalid_formats['valor_serie'].iloc[0]), "Teste 3: Valor válido tornou-se NaN"
    assert df_transformed_invalid_formats['valor_serie'].iloc[2] == 200.50, "Teste 3: Conversão de valor com vírgula falhou após dados inválidos"
    logger.info("Teste 3: Datas e Valores Inválidos (verificação de NaT/NaN) - Aprovado")

    # --- Teste 4: Coluna 'data' Faltando na Entrada ---
    logger.info("\n--- Teste 4: Coluna 'data' Faltando ---")
    raw_data_no_date_col = {'valor': ['123.45']}
    df_raw_no_date_col = pd.DataFrame(raw_data_no_date_col)
    df_transformed_no_date_col = transform_bcb_data(df_raw_no_date_col, series_code_for_test)
    
    print("DataFrame Transformado (Coluna 'data' Faltando):")
    print(df_transformed_no_date_col)
    assert df_transformed_no_date_col.empty, "Teste 4: Resultado deveria ser DataFrame vazio (coluna 'data' faltando)"
    logger.info("Teste 4: Coluna 'data' Faltando - Aprovado")

    # --- Teste 5: Coluna 'valor' Faltando na Entrada ---
    logger.info("\n--- Teste 5: Coluna 'valor' Faltando ---")
    raw_data_no_value_col = {'data': ['01/04/2024']}
    df_raw_no_value_col = pd.DataFrame(raw_data_no_value_col)
    df_transformed_no_value_col = transform_bcb_data(df_raw_no_value_col, series_code_for_test)

    print("DataFrame Transformado (Coluna 'valor' Faltando):")
    print(df_transformed_no_value_col)
    assert df_transformed_no_value_col.empty, "Teste 5: Resultado deveria ser DataFrame vazio (coluna 'valor' faltando)"
    logger.info("Teste 5: Coluna 'valor' Faltando - Aprovado")
    
    logger.info("\nFim dos testes para bcb_pipeline.transformer.transform_bcb_data.")

if __name__ == "__main__":
    run_all_transformer_tests()