import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def _parse_ibge_period_to_date(period_code: str) -> Optional[datetime]:
    """Converte códigos de período do IBGE para datetime (ex: '202401', '2023')."""
    if not isinstance(period_code, str):
        return None
    try:
        if len(period_code) == 4:
            return datetime(int(period_code), 1, 1)
        elif len(period_code) == 6:
            year = int(period_code[:4])
            code = int(period_code[4:])
            if 1 <= code <= 4:
                return datetime(year, {1: 1, 2: 4, 3: 7, 4: 10}[code], 1)
            if 1 <= code <= 12:
                return datetime(year, code, 1)
        logger.warning(f"[IBGE] Período inválido: '{period_code}'")
        return None
    except Exception:
        logger.warning(f"[IBGE] Falha ao converter período: '{period_code}'")
        return None


def transform_ibge_data(
    df_raw: pd.DataFrame,
    aggregate_code: str,
    variable_code: str,
    variable_name: str
) -> pd.DataFrame:
    
    if df_raw.empty or len(df_raw) < 2:
        logger.warning("[IBGE] DataFrame bruto vazio ou com linhas insuficientes.")
        return pd.DataFrame()

    df = df_raw.iloc[1:].copy().reset_index(drop=True)
    logger.info(f"[IBGE] Transformando {len(df)} registros de {aggregate_code} ({variable_code})")

    try:
        df_out = pd.DataFrame()
        df_out["data_referencia"] = df["D2C"].apply(_parse_ibge_period_to_date)
        df_out["valor_serie"] = pd.to_numeric(df["V"].replace("...", np.nan), errors="coerce")
        df_out["localidade_codigo"] = df.get("NC", df.get("D1C", pd.Series([None] * len(df))))
        df_out["localidade_nome"] = df.get("D1N", pd.Series(["Brasil"] * len(df)))
        df_out["unidade_medida"] = df.get("MN", pd.Series([None] * len(df)))
        df_out["codigo_agregado"] = aggregate_code
        df_out["codigo_serie"] = int(variable_code)
        df_out["nome_variavel_principal"] = variable_name

        original_len = len(df_out)
        df_out.dropna(subset=["data_referencia", "valor_serie"], inplace=True)
        if len(df_out) < original_len:
            logger.warning(f"[IBGE] {original_len - len(df_out)} registros descartados por valores nulos.")

        cols = [
            "data_referencia", "codigo_agregado", "codigo_serie",
            "nome_variavel_principal", "valor_serie", "unidade_medida",
            "localidade_codigo", "localidade_nome"
        ]
        for col in cols:
            if col not in df_out.columns:
                df_out[col] = None

        df_out = df_out[cols].drop_duplicates(subset=["data_referencia", "codigo_serie"])
        logger.info(f"[IBGE] Transformação concluída: {len(df_out)} registros válidos.")
        return df_out

    except KeyError as e:
        logger.error(f"[IBGE] Coluna ausente: {e}. Colunas disponíveis: {list(df.columns)}")
    except Exception as e:
        logger.exception(f"[IBGE] Erro inesperado na transformação de {aggregate_code} - {variable_code}: {e}")
    return pd.DataFrame()
