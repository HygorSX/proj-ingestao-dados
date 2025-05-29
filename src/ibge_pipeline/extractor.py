import requests
import pandas as pd
import logging
from typing import Optional, Union, List

logger = logging.getLogger(__name__)

IBGE_AGGREGATE_API_BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados" 

def fetch_ibge_aggregate_data(
    aggregate_code: str,
    variable_codes: Union[str, List[str]],
    periods: str = "all",  
    localities_specifier: str = "N1[all]" 
) -> pd.DataFrame:
    
    if isinstance(variable_codes, list):
        variables_path_segment = "|".join(variable_codes)
    else:
        variables_path_segment = str(variable_codes)

    url_path = f"{aggregate_code}/periodos/{periods}/variaveis/{variables_path_segment}"
    request_url = f"{IBGE_AGGREGATE_API_BASE_URL}/{url_path}"

    params = {
        "localidades": localities_specifier,
        "view": "flat" 
    }

    logger.info(f"Requisitando dados da API de Agregados IBGE: {request_url} com parâmetros: {params}")

    try:
        response = requests.get(request_url, params=params, timeout=90)
        response.raise_for_status()

        if not response.text or response.text == "[]":
            logger.warning(f"Nenhum dado (resposta vazia '[]') retornado pela API IBGE para a consulta: {request_url} com params {params}")
            return pd.DataFrame()

        data_json = response.json()

        if not data_json:
            logger.warning(f"Nenhum dado (JSON vazio) retornado pela API IBGE para a consulta: {request_url} com params {params}")
            return pd.DataFrame()

        df = pd.DataFrame(data_json)
        
        logger.info(f"Dados da API de Agregados IBGE extraídos com sucesso. {len(df)} registros retornados.")
        return df

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"Erro HTTP ao buscar dados da API IBGE ({request_url}, params: {params}): {http_err}")
        logger.error(f"Conteúdo da resposta: {response.text[:500] if response and hasattr(response, 'text') else 'N/A'}")

    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Erro de conexão ao buscar dados da API IBGE ({request_url}, params: {params}): {conn_err}")

    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout ao buscar dados da API IBGE ({request_url}, params: {params}): {timeout_err}")

    except ValueError as json_err:
        logger.error(f"Erro ao decodificar JSON da resposta da API IBGE ({request_url}, params: {params}): {json_err}")
        logger.error(f"Conteúdo da resposta: {response.text[:500] if response and hasattr(response, 'text') else 'N/A'}")

    except Exception as e:
        logger.error(f"Erro inesperado ao buscar dados da API IBGE ({request_url}, params: {params}): {e}")
        
    return pd.DataFrame()