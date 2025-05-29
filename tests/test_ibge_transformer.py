import pandas as pd
import numpy as np
from datetime import datetime as dt_datetime
import logging

from src.common.utils import setup_logging
from src.ibge_pipeline.transformer import transform_ibge_data, _parse_ibge_period_to_date

setup_logging()
logger = logging.getLogger(__name__)

def create_mock_ibge_raw_df(data_rows_as_dicts: list) -> pd.DataFrame:
    
    header_row = {
        "NC": "Nível Territorial (Código)", "NN": "Nível Territorial",
        "MC": "Unidade de Medida (Código)", "MN": "Unidade de Medida",
        "V": "Valor",
        "D1C": "Localidade (Código)", "D1N": "Localidade",
        "D2C": "Período (Código)", "D2N": "Período",
        "D3C": "Variável (Código)", "D3N": "Variável"
    }
    
    all_cols = list(header_row.keys())
    processed_data_rows = []
    for row in data_rows_as_dicts:
        processed_row = {col: row.get(col) for col in all_cols}
        processed_data_rows.append(processed_row)

    return pd.DataFrame([header_row] + processed_data_rows)

def test_period_parser():
    logger.info("\n--- Testando _parse_ibge_period_to_date ---")
    assert _parse_ibge_period_to_date("2023") == dt_datetime(2023, 1, 1), "Falha para ano"
    assert _parse_ibge_period_to_date("202301") == dt_datetime(2023, 1, 1), "Falha para mês Jan ou Trimestre 1" #Assume Q1
    assert _parse_ibge_period_to_date("202307") == dt_datetime(2023, 7, 1), "Falha para mês Jul"
    assert _parse_ibge_period_to_date("202312") == dt_datetime(2023, 12, 1), "Falha para mês Dez"
    assert _parse_ibge_period_to_date("202301") == dt_datetime(2023, 1, 1), "Falha para Trimestre 1 (Janeiro)"
    assert _parse_ibge_period_to_date("202302") == dt_datetime(2023, 4, 1), "Falha para Trimestre 2 (Abril)"
    assert _parse_ibge_period_to_date("202303") == dt_datetime(2023, 7, 1), "Falha para Trimestre 3 (Julho)"
    assert _parse_ibge_period_to_date("202304") == dt_datetime(2023, 10, 1), "Falha para Trimestre 4 (Outubro)"
    assert _parse_ibge_period_to_date("invalido") is None, "Falha para entrada inválida"
    assert _parse_ibge_period_to_date("202300") is None, "Falha para mês/trimestre 00" # Teste de borda
    assert _parse_ibge_period_to_date("202313") is None, "Falha para mês 13"     # Teste de borda
    assert _parse_ibge_period_to_date("202305") == dt_datetime(2023, 5, 1), "Falha para mês 05"
    logger.info("Testes de _parse_ibge_period_to_date - Aprovados")


def run_single_transformation_test(
    indicator_name: str,
    mock_data_rows: list,
    aggregate_code: str,
    variable_code: str,
    variable_name: str,
    expected_rows_after_transform: int
):
    logger.info(f"\n--- Testando Transformação para: {indicator_name} ---")
    df_raw_mock = create_mock_ibge_raw_df(mock_data_rows)
    
    logger.info("DataFrame Bruto Mock:")
    print(df_raw_mock)

    df_transformed = transform_ibge_data(
        df_raw=df_raw_mock,
        aggregate_code_meta=aggregate_code,
        variable_code_meta=variable_code,
        variable_name_meta=variable_name
    )

    logger.info("DataFrame Transformado:")
    print(df_transformed)
    if not df_transformed.empty:
        df_transformed.info()

    assert df_transformed is not None, f"{indicator_name}: A função retornou None."
    assert len(df_transformed) == expected_rows_after_transform, \
        f"{indicator_name}: Número de linhas esperado {expected_rows_after_transform}, obtido {len(df_transformed)}."

    if expected_rows_after_transform > 0:
        expected_cols = [
            'data_referencia', 'codigo_agregado', 'codigo_variavel', 
            'nome_variavel_principal', 'valor_serie', 'unidade_medida', 
            'localidade_codigo', 'localidade_nome'
        ]
        for col in expected_cols:
            assert col in df_transformed.columns, f"{indicator_name}: Coluna final esperada '{col}' não encontrada."
        
        assert pd.api.types.is_datetime64_any_dtype(df_transformed['data_referencia']), \
            f"{indicator_name}: 'data_referencia' não é do tipo datetime."
        assert pd.api.types.is_numeric_dtype(df_transformed['valor_serie']), \
            f"{indicator_name}: 'valor_serie' não é do tipo numérico."
        
        assert df_transformed['data_referencia'].notna().all(), f"{indicator_name}: 'data_referencia' contém NaT após transformação e filtro."
        assert df_transformed['valor_serie'].notna().all(), f"{indicator_name}: 'valor_serie' contém NaN após transformação e filtro."


    logger.info(f"--- Teste para {indicator_name} concluído com status esperado. ---")


def run_all_ibge_transformer_tests():
    logger.info("===== Iniciando testes para ibge_pipeline.transformer =====")

    test_period_parser()

    ipca_data = [
        {"NC": "1", "D1N": "Brasil", "D2C": "202410", "D2N": "outubro 2024", "D3C": "63", "D3N": "IPCA Mensal", "V": "0.56", "MN": "%"},
        {"NC": "1", "D1N": "Brasil", "D2C": "202411", "D2N": "novembro 2024", "D3C": "63", "D3N": "IPCA Mensal", "V": "...", "MN": "%"}, # Valor "..."
        {"NC": "1", "D1N": "Brasil", "D2C": "INVALIDO", "D2N": "invalido", "D3C": "63", "D3N": "IPCA Mensal", "V": "0.30", "MN": "%"} # Período inválido
    ]
    run_single_transformation_test(
        "IPCA (Mensal)", ipca_data, "1737", "63", "IPCA - Variação Mensal", expected_rows_after_transform=1
    )

    desocupacao_data = [
        {"NC": "1", "D1N": "Brasil", "D2C": "202401", "D2N": "1º trimestre 2024", "D3C": "4099", "D3N": "Taxa Desoc.", "V": "7.9", "MN": "%"},
        {"NC": "1", "D1N": "Brasil", "D2C": "202402", "D2N": "2º trimestre 2024", "D3C": "4099", "D3N": "Taxa Desoc.", "V": "6.5", "MN": "%"}
    ]
    run_single_transformation_test(
        "Taxa de Desocupação (Trimestral)", desocupacao_data, "4099", "4099", "Taxa de Desocupação", expected_rows_after_transform=2
    )

    pib_anual_data = [
        {"NC": "1", "D1N": "Brasil", "D2C": "2020", "D2N": "2020", "D3C": "37", "D3N": "PIB Anual", "V": "7609597000", "MN": "Mil Reais"},
        {"NC": "1", "D1N": "Brasil", "D2C": "2021", "D2N": "2021", "D3C": "37", "D3N": "PIB Anual", "V": "9012142000", "MN": "Mil Reais"}
    ]
    run_single_transformation_test(
        "PIB Anual", pib_anual_data, "5938", "37", "PIB Anual a Preços Correntes", expected_rows_after_transform=2
    )

    populacao_data = [
        {"NC": "1", "D1N": "Brasil", "D2C": "2020", "D2N": "2020", "D3C": "9324", "D3N": "Pop Estimada", "V": "211755692", "MN": "Pessoas"},
        {"NC": "1", "D1N": "Brasil", "D2C": "2021", "D2N": "2021", "D3C": "9324", "D3N": "Pop Estimada", "V": "213317639", "MN": "Pessoas"}
    ]
    run_single_transformation_test(
        "População Estimada Anual", populacao_data, "6579", "9324", "População Residente Estimada", expected_rows_after_transform=2
    )
    
    logger.info("\n--- Teste 5: DataFrame Bruto Vazio ---")
    df_empty_raw = pd.DataFrame()
    df_transformed_empty = transform_ibge_data(df_empty_raw, "0000", "00", "Vazio")
    assert df_transformed_empty.empty, "Transformar DataFrame vazio deveria resultar em DataFrame vazio."
    logger.info("Teste 5: DataFrame Bruto Vazio - Aprovado")

    logger.info("\n--- Teste 6: DataFrame Bruto só com Cabeçalho ---")
    df_header_only_raw = create_mock_ibge_raw_df([])
    df_transformed_header_only = transform_ibge_data(df_header_only_raw, "0001", "01", "So Cabecalho")
    assert df_transformed_header_only.empty, "Transformar DataFrame só com cabeçalho deveria resultar em DataFrame vazio."
    logger.info("Teste 6: DataFrame Bruto só com Cabeçalho - Aprovado")

    logger.info("\n--- Teste 7: DataFrame Bruto faltando coluna 'V' ---")
    missing_v_data = [
        {"NC": "1", "D1N": "Brasil", "D2C": "202401", "D2N": "jan 2024", "MN": "%"} # Sem 'V'
    ]
    df_missing_v_raw = create_mock_ibge_raw_df(missing_v_data)
    if 'V' in df_missing_v_raw.columns:
        df_missing_v_raw_test = df_missing_v_raw.drop(columns=['V'])
    else:
        df_missing_v_raw_test = df_missing_v_raw

    df_transformed_missing_v = transform_ibge_data(df_missing_v_raw_test, "0002", "02", "Falta V")
    assert df_transformed_missing_v.empty, "Transformar DataFrame sem coluna 'V' deveria resultar em DataFrame vazio."
    logger.info("Teste 7: DataFrame Bruto faltando coluna 'V' - Aprovado")


    logger.info("\n===== Testes para ibge_pipeline.transformer concluídos =====")

if __name__ == "__main__":
    run_all_ibge_transformer_tests()