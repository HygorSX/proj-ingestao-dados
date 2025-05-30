import requests
import pandas as pd
from datetime import datetime
import logging
from typing import Optional

from src.common.utils import setup_logging

logger = setup_logging()
logger = logging.getLogger(__name__)

BCB_API_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"


def fetch_bcb_series_data(
    series_code: int, 
    start_date: str, 
    end_date: Optional[str] = None
) -> pd.DataFrame:

    end_date = end_date or datetime.today().strftime('%d/%m/%Y')

    params = {
        "formato": "json",
        "dataInicial": start_date,
        "dataFinal": end_date
    }

    request_url = BCB_API_BASE_URL.format(series_code=series_code)
    logger.info(f"[BCB] Série: {series_code} | Período: {start_date} a {end_date}")
    logger.debug(f"[BCB] URL: {request_url} | Parâmetros: {params}")

    try:
        response = requests.get(request_url, params=params, timeout=30)
        response.raise_for_status()
        data_json = response.json()

        if not data_json:
            logger.warning(f"[BCB] Nenhum dado retornado para série {series_code}.")
            return pd.DataFrame()

        df = pd.DataFrame(data_json)
        logger.info(f"[BCB] {len(df)} registros retornados para a série {series_code}.")
        return df

    except requests.exceptions.HTTPError as e:
        logger.exception(f"[BCB] Erro HTTP na série {series_code}: {e}")
        _log_response_content(response)

    except requests.exceptions.ConnectionError as e:
        logger.exception(f"[BCB] Erro de conexão na série {series_code}: {e}")

    except requests.exceptions.Timeout as e:
        logger.exception(f"[BCB] Timeout na série {series_code}: {e}")

    except requests.exceptions.RequestException as e:
        logger.exception(f"[BCB] Erro de requisição genérico na série {series_code}: {e}")

    except ValueError as e:
        logger.exception(f"[BCB] Erro ao processar JSON da série {series_code}: {e}")
        _log_response_content(response)

    return pd.DataFrame()


def _log_response_content(response: Optional[requests.Response]) -> None:
    """Loga os primeiros caracteres do corpo da resposta para debug."""
    if response is not None and hasattr(response, 'text'):
        logger.debug(f"[BCB] Conteúdo da resposta: {response.text[:500]}")
