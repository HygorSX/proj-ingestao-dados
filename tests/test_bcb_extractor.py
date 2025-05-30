import pytest
import requests
from unittest.mock import patch
import pandas as pd

from src.bcb_pipeline.extractor import fetch_bcb_series_data


@pytest.fixture
def sample_bcb_response():
    return [
        {"data": "01/01/2023", "valor": "13.65"},
        {"data": "02/01/2023", "valor": "13.65"}
    ]


@patch("src.bcb_pipeline.extractor.requests.get")
def test_fetch_bcb_series_data_success(mock_get, sample_bcb_response):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = sample_bcb_response

    df = fetch_bcb_series_data(series_code=11, start_date="01/01/2023", end_date="02/01/2023")
    
    assert not df.empty
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "data" in df.columns
    assert "valor" in df.columns


@patch("src.bcb_pipeline.extractor.requests.get")
def test_fetch_bcb_series_data_empty(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = []

    df = fetch_bcb_series_data(series_code=9999, start_date="01/01/2023")
    
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("src.bcb_pipeline.extractor.requests.get")
def test_fetch_bcb_series_data_http_error(mock_get):
    mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP Error")

    df = fetch_bcb_series_data(series_code=11, start_date="01/01/2023")
    
    assert isinstance(df, pd.DataFrame)
    assert df.empty
