import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from src.bcb_pipeline.main_bcb import run_full_bcb_pipeline_for_series


@pytest.fixture
def sample_transformed_df():
    return pd.DataFrame({
        "data_referencia": ["2024-01-01"],
        "codigo_serie": [11],
        "valor_serie": [13.65]
    })


@patch("src.bcb_pipeline.main_bcb.fetch_bcb_series_data")
@patch("src.bcb_pipeline.main_bcb.transform_bcb_data")
@patch("src.bcb_pipeline.main_bcb.load_df_to_staging_table")
@patch("src.bcb_pipeline.main_bcb.merge_data_to_final_table")
@patch("src.bcb_pipeline.main_bcb.bigquery.Client")
def test_pipeline_success(
    mock_bq_client,
    mock_merge,
    mock_staging,
    mock_transform,
    mock_fetch,
    sample_transformed_df
):
    # Setup
    mock_fetch.return_value = sample_transformed_df
    mock_transform.return_value = sample_transformed_df
    mock_staging.return_value = True
    mock_merge.return_value = True
    mock_bq_client.return_value = MagicMock()

    result = run_full_bcb_pipeline_for_series(
        series_name="selic_diaria",
        series_code=11,
        start_date="01/01/2024",
        end_date="31/01/2024"
    )

    assert result is True


@patch("src.bcb_pipeline.main_bcb.fetch_bcb_series_data")
def test_pipeline_empty_extraction(mock_fetch):
    mock_fetch.return_value = pd.DataFrame()

    result = run_full_bcb_pipeline_for_series(
        series_name="serie_teste",
        series_code=999,
        start_date="01/01/2024",
        end_date="31/01/2024"
    )

    assert result is True


@patch("src.bcb_pipeline.main_bcb.fetch_bcb_series_data")
@patch("src.bcb_pipeline.main_bcb.transform_bcb_data")
def test_pipeline_empty_transformation(mock_transform, mock_fetch):
    mock_fetch.return_value = pd.DataFrame({
        "data": ["01/01/2024"],
        "valor": ["13,65"]
    })
    mock_transform.return_value = pd.DataFrame()

    result = run_full_bcb_pipeline_for_series(
        series_name="serie_teste",
        series_code=999,
        start_date="01/01/2024",
        end_date="31/01/2024"
    )

    assert result is True


@patch("src.bcb_pipeline.main_bcb.fetch_bcb_series_data")
@patch("src.bcb_pipeline.main_bcb.transform_bcb_data")
@patch("src.bcb_pipeline.main_bcb.load_df_to_staging_table")
def test_pipeline_staging_fail(mock_staging, mock_transform, mock_fetch):
    df = pd.DataFrame({
        "data_referencia": ["2024-01-01"],
        "codigo_serie": [999],
        "valor_serie": [42.0]
    })
    mock_fetch.return_value = df
    mock_transform.return_value = df
    mock_staging.return_value = False

    result = run_full_bcb_pipeline_for_series(
        series_name="serie_teste",
        series_code=999,
        start_date="01/01/2024",
        end_date="31/01/2024"
    )

    assert result is False


@patch("src.bcb_pipeline.main_bcb.fetch_bcb_series_data")
@patch("src.bcb_pipeline.main_bcb.transform_bcb_data")
@patch("src.bcb_pipeline.main_bcb.load_df_to_staging_table")
@patch("src.bcb_pipeline.main_bcb.merge_data_to_final_table")
def test_pipeline_merge_fail(mock_merge, mock_staging, mock_transform, mock_fetch):
    df = pd.DataFrame({
        "data_referencia": ["2024-01-01"],
        "codigo_serie": [999],
        "valor_serie": [42.0]
    })
    mock_fetch.return_value = df
    mock_transform.return_value = df
    mock_staging.return_value = True
    mock_merge.return_value = False

    result = run_full_bcb_pipeline_for_series(
        series_name="serie_teste",
        series_code=999,
        start_date="01/01/2024",
        end_date="31/01/2024"
    )

    assert result is False
