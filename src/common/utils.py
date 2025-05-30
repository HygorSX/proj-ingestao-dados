import logging
import os

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