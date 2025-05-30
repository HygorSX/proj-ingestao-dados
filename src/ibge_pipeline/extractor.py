import requests
import pandas as pd
import logging
from typing import Optional, Union, List

from src.common.utils import setup_logging

logger = setup_logging()
logger = logging.getLogger(__name__)

IBGE_AGGREGATE_API_BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"


def fetch_ibge_aggregate_data(
    aggregate_code: str,
    variable_codes: Union[str, List[str]],
    periods: str = "all",
    localities_specifier: str = "N1[all]"
) -> pd.DataFrame:
    
    variables_segment = "|".join(variable_codes) if isinstance(variable_codes, list) else str(variable_codes)
    url_path = f"{aggregate_code}/periodos/{periods}/variaveis/{variables_segment}"
    request_url = f"{IBGE_AGGREGATE_API_BASE_URL}/{url_path}"

    params = {
        "localidades": localities_specifier,
        "view": "flat"
    }

    logger.info(f"[IBGE] Requisição: {request_url} | Parâmetros: {params}")

    try:
        response = requests.get(request_url, params=params, timeout=90)
        response.raise_for_status()

        if not response.text or response.text == "[]":
            logger.warning(f"[IBGE] Resposta vazia da API: {request_url}")
            return pd.DataFrame()

        data_json = response.json()
        if not data_json:
            logger.warning(f"[IBGE] JSON vazio retornado da API: {request_url}")
            return pd.DataFrame()

        df = pd.DataFrame(data_json)
        logger.info(f"[IBGE] {len(df)} registros retornados para o agregado {aggregate_code}.")
        return df

    except requests.exceptions.HTTPError as e:
        logger.exception(f"[IBGE] HTTPError para {request_url}: {e}")
        _log_response_content(response)

    except requests.exceptions.ConnectionError as e:
        logger.exception(f"[IBGE] ConnectionError para {request_url}: {e}")

    except requests.exceptions.Timeout as e:
        logger.exception(f"[IBGE] Timeout para {request_url}: {e}")

    except ValueError as e:
        logger.exception(f"[IBGE] Erro ao decodificar JSON: {e}")
        _log_response_content(response)

    except Exception as e:
        logger.exception(f"[IBGE] Erro inesperado ao requisitar {request_url}: {e}")

    return pd.DataFrame()


def _log_response_content(response: Optional[requests.Response]) -> None:
    """Loga os primeiros caracteres da resposta da API para debug."""
    if response is not None and hasattr(response, 'text'):
        logger.debug(f"[IBGE] Conteúdo da resposta: {response.text[:500]}")
