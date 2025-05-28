import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def transform_bcb_data(df_raw: pd.DataFrame, series_code: int) -> pd.DataFrame:
    if df_raw.empty:
        logger.info(f"DataFrame bruto para a série {series_code} está vazio. Nenhuma transformação a ser feita.")
        return pd.DataFrame()
    
    df = df_raw.copy()
    logger.info(f"Iniciando transformações para {len(df)} registros da série {series_code}.")

    required_original_cols = ['data', 'valor']
    for col in required_original_cols:
        if col not in df.columns:
            logger.error(f"Coluna original essencial '{col}' ausente no DataFrame para a série {series_code}. Colunas presentes: {df.columns.tolist()}")
            return pd.DataFrame()

    df['data_referencia'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce')
    invalid_dates = df[df['data_referencia'].isna() & df['data'].notna()]

    if not invalid_dates.empty:
        logger.warning(f"Para a série {series_code}, {len(invalid_dates)} datas não puderam ser convertidas e foram setadas para NaT. Exemplos de datas inválidas: {invalid_dates['data'].unique().tolist()[:5]}")
        # df.dropna(subset=['data_referencia'], inplace=True)

    df['valor_serie'] = df['valor'].astype(str).str.replace(',', '.')
    df['valor_serie'] = pd.to_numeric(df['valor_serie'], errors='coerce')
    invalid_values = df[df['valor_serie'].isna() & df['valor'].notna()]

    if not invalid_values.empty:
        logger.warning(f"Para a série {series_code}, {len(invalid_values)} valores não puderam ser convertidos para numérico e foram setados para NaN. Exemplos de valores inválidos: {invalid_values['valor'].unique().tolist()[:5]}")

    df['codigo_serie'] = series_code
    df['codigo_serie'] = df['codigo_serie'].astype(int)

    final_columns = ['data_referencia', 'codigo_serie', 'valor_serie']

    missing_original_columns = False
    if 'data' not in df_raw.columns:
        logger.error(f"Coluna 'data' original ausente no DataFrame bruto para a série {series_code}.")
        missing_original_columns = True

    if 'valor' not in df_raw.columns:
        logger.error(f"Coluna 'valor' original ausente no DataFrame bruto para a série {series_code}.")
        missing_original_columns = True

    if missing_original_columns:
        logger.error(f"Devido a colunas originais ausentes, não é possível criar todas as colunas transformadas para a série {series_code}. Retornando DataFrame vazio.")
        return pd.DataFrame()
    
    try:
        df_transformed = df[final_columns].copy() # Usar .copy() para evitar SettingWithCopyWarning

    except KeyError as e:
        logger.error(f"Erro ao selecionar colunas finais para a série {series_code}: {e}. Colunas disponíveis: {df.columns.tolist()}")
        return pd.DataFrame()
    
    logger.info(f"Transformações concluídas para a série {series_code}. {len(df_transformed)} registros processados.")
    return df_transformed

