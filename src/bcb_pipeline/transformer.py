import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def transform_bcb_data(df_raw: pd.DataFrame, series_code: int) -> pd.DataFrame:

    if df_raw.empty:
        logger.info(f"[BCB] Série {series_code}: DataFrame vazio recebido. Nenhuma transformação será aplicada.")
        return pd.DataFrame()

    required_cols = {'data', 'valor'}
    missing_cols = required_cols - set(df_raw.columns)
    if missing_cols:
        logger.error(f"[BCB] Série {series_code}: Colunas ausentes no DataFrame: {missing_cols}")
        return pd.DataFrame()

    df = df_raw.copy()
    logger.info(f"[BCB] Série {series_code}: Iniciando transformação de {len(df)} registros.")

    df['data_referencia'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce')
    _log_invalid_dates(df, series_code)

    df['valor_serie'] = pd.to_numeric(df['valor'].astype(str).str.replace(',', '.'), errors='coerce')
    _log_invalid_values(df, series_code)

    df['codigo_serie'] = int(series_code)

    try:
        df_transformed = df[['data_referencia', 'codigo_serie', 'valor_serie']].copy()
        logger.info(f"[BCB] Série {series_code}: Transformação concluída com {len(df_transformed)} registros.")
        return df_transformed

    except KeyError as e:
        logger.exception(f"[BCB] Série {series_code}: Erro ao selecionar colunas finais: {e}")
        return pd.DataFrame()


def _log_invalid_dates(df: pd.DataFrame, series_code: int) -> None:
    """Loga datas inválidas encontradas durante a transformação."""
    invalid = df[df['data_referencia'].isna() & df['data'].notna()]
    if not invalid.empty:
        logger.warning(
            f"[BCB] Série {series_code}: {len(invalid)} datas inválidas convertidas para NaT. Ex: {invalid['data'].unique().tolist()[:5]}"
        )


def _log_invalid_values(df: pd.DataFrame, series_code: int) -> None:
    """Loga valores não numéricos encontrados durante a transformação."""
    invalid = df[df['valor_serie'].isna() & df['valor'].notna()]
    if not invalid.empty:
        logger.warning(
            f"[BCB] Série {series_code}: {len(invalid)} valores inválidos convertidos para NaN. Ex: {invalid['valor'].unique().tolist()[:5]}"
        )
