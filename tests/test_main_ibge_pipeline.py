import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.ibge_pipeline.main_ibge import run_full_ibge_pipeline_for_indicator


@pytest.fixture
def sample_transformed_df():
    return pd.DataFrame({
        "data_referencia": ["2023-01-01"],
        "codigo_agregado": ["1737"],
        "codigo_serie": [63],
        "nome_variavel_principal": ["IPCA"],
        "valor_serie": [5.1],
        "unidade_medida": ["%"],
        "localidade_codigo": ["1"],
        "localidade_nome": ["Brasil"]
    })


@pytest.fixture
def indicator_config():
    return {
        "indicator_name_table": "ipca_variacao_mensal_brasil",
        "aggregate_code": "1737",
        "variable_code": "63",
        "variable_name_meta": "IPCA - Variação Mensal (Índice Geral, Brasil)",
        "periods": "202301-202302",
        "localities": "N1[all]",
    }


@patch("src.ibge_pipeline.main_ibge.fetch_ibge_aggregate_data")
@patch("src.ibge_pipeline.main_ibge.transform_ibge_data")
@patch("src.ibge_pipeline.main_ibge.load_df_to_staging_table")
@patch("src.ibge_pipeline.main_ibge.merge_data_to_final_table")
@patch("src.ibge_pipeline.main_ibge.bigquery.Client")
def test_pipeline_success(
    mock_bq_client,
    mock_merge,
    mock_staging,
    mock_transform,
    mock_fetch,
    sample_transformed_df,
    indicator_config
):
    mock_fetch.return_value = sample_transformed_df
    mock_transform.return_value = sample_transformed_df
    mock_staging.return_value = True
    mock_merge.return_value = True
    mock_bq_client.return_value = MagicMock()

    result = run_full_ibge_pipeline_for_indicator(indicator_config)
    assert result is True


@patch("src.ibge_pipeline.main_ibge.fetch_ibge_aggregate_data")
def test_pipeline_empty_extraction(mock_fetch, indicator_config):
    mock_fetch.return_value = pd.DataFrame()
    result = run_full_ibge_pipeline_for_indicator(indicator_config)
    assert result is True


@patch("src.ibge_pipeline.main_ibge.fetch_ibge_aggregate_data")
@patch("src.ibge_pipeline.main_ibge.transform_ibge_data")
def test_pipeline_empty_transformation(mock_transform, mock_fetch, indicator_config):
    mock_fetch.return_value = pd.DataFrame({
        "D2C": ["202401"],
        "V": ["4.5"]
    })
    mock_transform.return_value = pd.DataFrame()
    result = run_full_ibge_pipeline_for_indicator(indicator_config)
    assert result is True


@patch("src.ibge_pipeline.main_ibge.fetch_ibge_aggregate_data")
@patch("src.ibge_pipeline.main_ibge.transform_ibge_data")
@patch("src.ibge_pipeline.main_ibge.load_df_to_staging_table")
def test_pipeline_staging_fail(mock_staging, mock_transform, mock_fetch, sample_transformed_df, indicator_config):
    mock_fetch.return_value = sample_transformed_df
    mock_transform.return_value = sample_transformed_df
    mock_staging.return_value = False
    result = run_full_ibge_pipeline_for_indicator(indicator_config)
    assert result is False


@patch("src.ibge_pipeline.main_ibge.fetch_ibge_aggregate_data")
@patch("src.ibge_pipeline.main_ibge.transform_ibge_data")
@patch("src.ibge_pipeline.main_ibge.load_df_to_staging_table")
@patch("src.ibge_pipeline.main_ibge.merge_data_to_final_table")
def test_pipeline_merge_fail(mock_merge, mock_staging, mock_transform, mock_fetch, sample_transformed_df, indicator_config):
    mock_fetch.return_value = sample_transformed_df
    mock_transform.return_value = sample_transformed_df
    mock_staging.return_value = True
    mock_merge.return_value = False
    result = run_full_ibge_pipeline_for_indicator(indicator_config)
    assert result is False