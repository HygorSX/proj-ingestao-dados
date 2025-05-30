import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from src.ibge_pipeline.extractor import fetch_ibge_aggregate_data


@patch("src.ibge_pipeline.extractor.requests.get")
def test_ibge_extractor_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '[{"id":1,"variavel":"IPCA"}]'
    mock_response.json.return_value = [{"id": 1, "variavel": "IPCA"}]
    mock_get.return_value = mock_response

    df = fetch_ibge_aggregate_data(aggregate_code="1737", variable_codes="63", periods="202301", localities_specifier="N1[all]")
    assert not df.empty
    assert isinstance(df, pd.DataFrame)
    assert "variavel" in df.columns


@patch("src.ibge_pipeline.extractor.requests.get")
def test_ibge_extractor_empty_response_text(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "[]"
    mock_get.return_value = mock_response

    df = fetch_ibge_aggregate_data("1737", "63")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("src.ibge_pipeline.extractor.requests.get")
def test_ibge_extractor_empty_json(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '[  ]'
    mock_response.json.return_value = []
    mock_get.return_value = mock_response

    df = fetch_ibge_aggregate_data("1737", "63")
    assert df.empty


@patch("src.ibge_pipeline.extractor.requests.get")
def test_ibge_extractor_json_decode_error(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "invalid"
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_get.return_value = mock_response

    df = fetch_ibge_aggregate_data("1737", "63")
    assert df.empty


@patch("src.ibge_pipeline.extractor.requests.get")
def test_ibge_extractor_http_error(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("Erro HTTP")
    mock_get.return_value = mock_response

    df = fetch_ibge_aggregate_data("1737", "63")
    assert df.empty
