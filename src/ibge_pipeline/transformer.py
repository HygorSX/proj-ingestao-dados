import pandas as pd
import numpy as np
import logging
from datetime import datetime as dt_datetime
from typing import Optional

logger = logging.getLogger(__name__)

def _parse_ibge_period_to_date(period_code_str: str) -> Optional[dt_datetime]:
    
    if not isinstance(period_code_str, str):
        return None
    try:
        if len(period_code_str) == 4: 
            return dt_datetime(int(period_code_str), 1, 1)
        elif len(period_code_str) == 6: 
            year = int(period_code_str[:4])
            month_or_quarter = int(period_code_str[4:])
            
            if month_or_quarter >= 1 and month_or_quarter <= 4:
                quarter_start_month = {1: 1, 2: 4, 3: 7, 4: 10}
                if month_or_quarter in quarter_start_month:
                    return dt_datetime(year, quarter_start_month[month_or_quarter], 1)
            
            if month_or_quarter >= 1 and month_or_quarter <= 12:
                return dt_datetime(year, month_or_quarter, 1)
            
            logger.warning(f"Formato de período IBGE YYYYXX não reconhecido como mensal ou trimestral: {period_code_str}")
            return None
        else:
            logger.warning(f"Formato de período IBGE não reconhecido (comprimento inválido): {period_code_str}")
            return None
    except ValueError:
        logger.warning(f"Erro ao converter string de período IBGE '{period_code_str}' para data.")
        return None

def transform_ibge_data(
    df_raw: pd.DataFrame,
    aggregate_code_meta: str,
    variable_code_meta: str,
    variable_name_meta: str
) -> pd.DataFrame:
    
    if df_raw.empty:
        logger.info("DataFrame bruto do IBGE está vazio. Nenhuma transformação a ser feita.")
        return pd.DataFrame()

    if len(df_raw) < 2:
        logger.warning("DataFrame bruto do IBGE não contém linhas de dados suficientes (esperava-se cabeçalho + dados).")
        return pd.DataFrame()
    
    df = df_raw.iloc[1:].reset_index(drop=True).copy()
    logger.info(f"Iniciando transformações para {len(df)} registros do agregado {aggregate_code_meta} (variável {variable_code_meta}).")

    transformed_data = []

    try:
        df_out = pd.DataFrame()
        
        if 'D2C' not in df.columns:
            logger.error("Coluna 'D2C' (esperada para código do período) não encontrada. Abortando transformação.")
            return pd.DataFrame()
        df_out['data_referencia'] = df['D2C'].apply(_parse_ibge_period_to_date)

        if 'V' not in df.columns:
            logger.error("Coluna 'V' (esperada para valor) não encontrada. Abortando transformação.")
            return pd.DataFrame()
        
        df_out['valor_serie'] = df['V'].replace('...', np.nan)
        df_out['valor_serie'] = pd.to_numeric(df_out['valor_serie'], errors='coerce')

        df_out['localidade_codigo'] = df.get('NC', df.get('D1C', pd.Series([None] * len(df))))
        df_out['localidade_nome'] = df.get('D1N', pd.Series(["Brasil"] * len(df)))

        df_out['unidade_medida'] = df.get('MN', pd.Series([None] * len(df)))

        df_out['codigo_agregado'] = aggregate_code_meta
        df_out['codigo_variavel'] = variable_code_meta
        df_out['nome_variavel_principal'] = variable_name_meta
        
        original_rows = len(df_out)
        df_out.dropna(subset=['data_referencia', 'valor_serie'], inplace=True)
        if len(df_out) < original_rows:
            logger.warning(f"{original_rows - len(df_out)} linhas foram removidas devido a data_referencia ou valor_serie nulos após conversão.")

        final_columns = [
            'data_referencia', 'codigo_agregado', 'codigo_variavel', 
            'nome_variavel_principal', 'valor_serie', 'unidade_medida', 
            'localidade_codigo', 'localidade_nome'
        ]
        
        for col in final_columns:
            if col not in df_out.columns:
                df_out[col] = None 
        
        df_out = df_out[final_columns]

        logger.info(f"Transformações concluídas para agregado {aggregate_code_meta}, variável {variable_code_meta}. {len(df_out)} registros válidos processados.")
        return df_out

    except KeyError as e:
        logger.error(f"Erro de chave (coluna não encontrada) durante a transformação do agregado {aggregate_code_meta}: {e}. Colunas disponíveis: {list(df.columns)}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro inesperado durante a transformação do agregado {aggregate_code_meta}: {e}", exc_info=True)
        return pd.DataFrame()
