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

        if not data_json: # Verifica se a resposta JSON está vazia
            logger.warning(f"Nenhum dado retornado pela API para a série {series_code} no período de {start_date} a {end_date}.")
            return pd.DataFrame()
        
        df = pd.DataFrame(data_json)
        
        logger.info(f"Dados extraídos com sucesso para a série {series_code}. {len(df)} registros retornados.")
        return df

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"Erro HTTP ao buscar dados para a série {series_code}: {http_err}")
        # Tentamos obter o texto da resposta, se disponível
        response_text = response.text if 'response' in locals() and response and hasattr(response, 'text') else 'N/A (response object not available or no text)'
        logger.error(f"Conteúdo da resposta (HTTPError): {response_text[:500]}") # Mostra apenas os primeiros 500 chars
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Erro de conexão ao buscar dados para a série {series_code}: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout ao buscar dados para a série {series_code}: {timeout_err}")
    except requests.exceptions.RequestException as req_err: # Captura outras exceções de 'requests'
        logger.error(f"Erro geral de requisição ao buscar dados para a série {series_code}: {req_err}")
    except ValueError as json_err: # Erro ao decodificar JSON
        logger.error(f"Erro ao decodificar JSON da resposta para a série {series_code}: {json_err}")
        response_text = response.text if 'response' in locals() and response and hasattr(response, 'text') else 'N/A (response object not available or no text)'
        logger.error(f"Conteúdo da resposta (JSONError): {response_text[:500]}")
        
    return pd.DataFrame()

# --- BLOCO DE TESTE ADICIONADO ABAIXO ---
if __name__ == "__main__":
    logger.info("Iniciando teste direto do módulo extractor.py...")

    # Vamos testar buscando a série SELIC (código 11) para um período curto
    # O código 11 refere-se à Taxa de juros - Selic acumulada no mês anualizada base 252, 
    # mas o exemplo de URL no plano usa o código 11 para SELIC meta diária[cite: 30, 35].
    # A API do BCB usa o código 11 para a "Taxa de juros - Selic diária".
    # Vamos usar o código 4390 para "Selic acumulada no mês" para ter menos dados no teste.
    # Ou podemos usar a SELIC diária (código 11) para um período bem curto.

    # Teste 1: Série SELIC diária (código 11) para 5 dias de Maio de 2025
    # (Ajuste as datas se necessário para um período com dados)
    codigo_selic_diaria = 11
    data_inicio_teste = "01/05/2025" # Use uma data recente que você saiba que tem dados
    data_fim_teste = "05/05/2025"   # Use uma data recente que você saiba que tem dados
    
    logger.info(f"Testando fetch_bcb_series_data com código {codigo_selic_diaria}, de {data_inicio_teste} a {data_fim_teste}")
    df_selic = fetch_bcb_series_data(
        series_code=codigo_selic_diaria,
        start_date=data_inicio_teste,
        end_date=data_fim_teste
    )

    if not df_selic.empty:
        print("\n--- Dados SELIC (raw) ---")
        print(df_selic.head())
        print(f"Total de registros retornados: {len(df_selic)}")
    else:
        print(f"\nNenhum dado retornado para SELIC (código {codigo_selic_diaria}) no período testado ou ocorreu um erro.")

    # Teste 2: Tentativa com um código de série inexistente para ver o log de erro/warning
    codigo_inexistente = 9999999
    logger.info(f"\nTestando fetch_bcb_series_data com código inexistente {codigo_inexistente}...")
    df_inexistente = fetch_bcb_series_data(
        series_code=codigo_inexistente,
        start_date="01/01/2024",
        end_date="02/01/2024"
    )
    if df_inexistente.empty:
        logger.info(f"Teste com código inexistente ({codigo_inexistente}) retornou DataFrame vazio, como esperado.")
    else:
        logger.warning(f"Teste com código inexistente ({codigo_inexistente}) retornou dados, o que não era esperado.")
        print(df_inexistente)

    logger.info("\nFim do teste direto do módulo extractor.py.")


