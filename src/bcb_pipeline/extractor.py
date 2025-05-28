import requests
import pandas as pd
from datetime import datetime
import logging

from src.common.utils import setup_logging

logger = setup_logging()

logger = logging.getLogger(__name__)

BCB_API_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"

def fetch_bcb_series_data(
    series_code: int, 
    start_date: str, 
    end_date: str = None
) -> pd.DataFrame:
    if end_date is None:
        end_date = datetime.today().strftime('%d/%m/%Y')

    params = {
        "formato": "json",
        "dataInicial": start_date,
        "dataFinal": end_date
    }
    
    request_url = BCB_API_BASE_URL.format(series_code=series_code)
    
    logger.info(f"Requisitando dados para a série BCB {series_code} de {start_date} a {end_date}.")
    logger.debug(f"URL da requisição: {request_url} com parâmetros: {params}")

    try:
        response = requests.get(request_url, params=params, timeout=30) 
        response.raise_for_status()

        data_json = response.json()

        if not data_json: 
            logger.warning(f"Nenhum dado retornado pela API para a série {series_code} no período de {start_date} a {end_date}.")
            return pd.DataFrame()
        
        df = pd.DataFrame(data_json)
        
        logger.info(f"Dados extraídos com sucesso para a série {series_code}. {len(df)} registros retornados.")
        return df

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"Erro HTTP ao buscar dados para a série {series_code}: {http_err}")
        response_text = response.text if 'response' in locals() and response and hasattr(response, 'text') else 'N/A (response object not available or no text)'
        logger.error(f"Conteúdo da resposta (HTTPError): {response_text[:500]}")

    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Erro de conexão ao buscar dados para a série {series_code}: {conn_err}")

    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout ao buscar dados para a série {series_code}: {timeout_err}")

    except requests.exceptions.RequestException as req_err:
        logger.error(f"Erro geral de requisição ao buscar dados para a série {series_code}: {req_err}")
        
    except ValueError as json_err:
        logger.error(f"Erro ao decodificar JSON da resposta para a série {series_code}: {json_err}")
        response_text = response.text if 'response' in locals() and response and hasattr(response, 'text') else 'N/A (response object not available or no text)'
        logger.error(f"Conteúdo da resposta (JSONError): {response_text[:500]}")
        
    return pd.DataFrame()