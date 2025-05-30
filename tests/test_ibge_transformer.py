import pytest
import pandas as pd
from src.ibge_pipeline.transformer import transform_ibge_data


@pytest.fixture
def sample_ibge_raw():
    # Simula a estrutura bruta da API IBGE com cabeçalho na primeira linha
    return pd.DataFrame([
        {"D2C": "Período", "V": "Valor", "D1C": "Local", "D1N": "Local Nome", "MN": "Unidade"},
        {"D2C": "202401", "V": "4.5", "D1C": "1", "D1N": "Brasil", "MN": "%"},
        {"D2C": "202402", "V": "5.0", "D1C": "1", "D1N": "Brasil", "MN": "%"},
    ])


def test_transform_ibge_valid(sample_ibge_raw):
    df = transform_ibge_data(sample_ibge_raw, "1737", "63", "IPCA")
    assert not df.empty
    assert len(df) == 2
    assert "data_referencia" in df.columns
    assert df["valor_serie"].iloc[0] == 4.5
    assert df["codigo_serie"].iloc[0] == 63


def test_transform_ibge_empty_input():
    df = pd.DataFrame()
    result = transform_ibge_data(df, "1737", "63", "IPCA")
    assert result.empty


def test_transform_ibge_insufficient_rows():
    df = pd.DataFrame([{"D2C": "Período"}])  # Apenas 1 linha (cabeçalho)
    result = transform_ibge_data(df, "1737", "63", "IPCA")
    assert result.empty


def test_transform_ibge_missing_columns():
    df = pd.DataFrame([
        {"D2C": "Período", "V": "Valor"},  # Cabeçalho simulado
        {"D2C": "202401", "V": "4.5"}      # Linha de dado sem D1C/D1N
    ])
    result = transform_ibge_data(df, "1737", "63", "IPCA")
    assert "localidade_nome" in result.columns
    assert not result.empty


def test_transform_ibge_invalid_period_format():
    df = pd.DataFrame([
        {"D2C": "20ABCD", "V": "3.3", "D1C": "1", "D1N": "Brasil", "MN": "%"},
        {"D2C": "202401", "V": "4.4", "D1C": "1", "D1N": "Brasil", "MN": "%"}
    ])
    df.insert(0, "header", "X")
    result = transform_ibge_data(df, "1737", "63", "IPCA")
    assert len(result) == 1  # Só uma linha válida


def test_transform_ibge_value_with_ellipsis():
    df = pd.DataFrame([
        {"D2C": "202401", "V": "...", "D1C": "1", "D1N": "Brasil", "MN": "%"},
        {"D2C": "202402", "V": "5.1", "D1C": "1", "D1N": "Brasil", "MN": "%"}
    ])
    df.insert(0, "header", "X")
    result = transform_ibge_data(df, "1737", "63", "IPCA")
    assert len(result) == 1
    assert result["valor_serie"].iloc[0] == 5.1
