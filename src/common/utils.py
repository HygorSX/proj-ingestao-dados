import logging
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

LOG_LEVEL_DEFAULT = logging.INFO
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

def setup_logging(log_level=LOG_LEVEL_DEFAULT):
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=log_level,
            format=LOG_FORMAT,
            datefmt=LOG_DATE_FORMAT
        )
        logging.info("Configuração de logging inicializada.")
    else:
        logging.info("Configuração de logging já inicializada.")

logger_utils = logging.getLogger(__name__)  

def ensure_bigquery_dataset_exists(
    client: bigquery.Client,
    dataset_id: str,
    project_id: str,
    location: str = "southamerica-east1" # Localização padrão para o Brasil
):
    
    full_dataset_id = f"{project_id}.{dataset_id}"
    dataset_ref = client.dataset(dataset_id) 

    try:
        client.get_dataset(dataset_ref)
        logger_utils.info(f"Dataset {full_dataset_id} já existe.")

    except NotFound:
        logger_utils.info(f"Dataset {full_dataset_id} não encontrado. Criando na localização {location}...")
        dataset_object_to_create = bigquery.Dataset(dataset_ref)
        dataset_object_to_create.location = location
        
        try:
            client.create_dataset(dataset_object_to_create, timeout=30)
            logger_utils.info(f"Dataset {full_dataset_id} criado com sucesso na localização {location}.")

        except Exception as e:
            logger_utils.error(f"Falha ao criar o dataset {full_dataset_id} na localização {location}: {e}")
            raise

    except Exception as e:
        logger_utils.error(f"Erro inesperado ao verificar a existência do dataset {full_dataset_id}: {e}")
        raise