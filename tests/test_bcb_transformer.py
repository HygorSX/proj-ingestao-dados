import pandas as pd
import pytest

from src.bcb_pipeline.transformer import transform_bcb_data


def test_transform_empty_dataframe():
    df = pd.DataFrame()
    result = transform_bcb_data(df, series_code=11)
    assert result.empty


def test_transform_missing_columns():
    df = pd.DataFrame({"outra_coluna": [1, 2]})
    result = transform_bcb_data(df, series_code=11)
    assert result.empty


def test_transform_with_invalid_dates():
    df = pd.DataFrame({
        "data": ["31/02/2023", "invalid_date"],
        "valor": ["13,65", "14,00"]
    })
    result = transform_bcb_data(df, series_code=11)
    assert len(result) == 2
    assert "data_referencia" in result.columns
    assert result["data_referencia"].isna().sum() == 2


def test_transform_with_invalid_values():
    df = pd.DataFrame({
        "data": ["01/01/2023", "02/01/2023"],
        "valor": ["abc", "13,45"]
    })
    result = transform_bcb_data(df, series_code=11)
    assert len(result) == 2
    assert "valor_serie" in result.columns
    assert pd.isna(result["valor_serie"].iloc[0])
    assert result["valor_serie"].iloc[1] == 13.45


def test_transform_valid_data():
    df = pd.DataFrame({
        "data": ["01/01/2023", "02/01/2023"],
        "valor": ["13,65", "13,70"]
    })
    result = transform_bcb_data(df, series_code=11)
    assert len(result) == 2
    assert set(result.columns) == {"data_referencia", "codigo_serie", "valor_serie"}
    assert result["codigo_serie"].iloc[0] == 11
    assert result["valor_serie"].iloc[1] == 13.70
    assert pd.to_datetime("01/01/2023", dayfirst=True) == result["data_referencia"].iloc[0]
