import datetime
import logging
import dask.dataframe as dd
from config import config
from ..utils import file_extractor, file_delete
from ..download import download_files_parallel

logger = logging.getLogger(__name__)

def process_simples(soup, url: str, path_zip: str, path_unzip: str) -> bool:
    """Processa os dados do Simples Nacional."""
    logger.info('Início da manipulação do Simples Nacional')
    inter_time: datetime = datetime.datetime.now()

    try:
        table_name: str = 'simples'
        if not download_files_parallel(soup, table_name.capitalize(), url, path_zip):
            return False

        file_extractor(path_zip, path_unzip, 'Sim*.*')

        dd_simples: dd = dd.read_csv(
            'dados-abertos/*.SIMPLES',
            sep=config.file.separator,
            names=config.simples_columns,
            encoding=config.file.encoding,
            dtype=config.simples_dtypes
        )

        inter_timer: datetime = datetime.datetime.now()
        logger.info(f'Tempo de manipulação dos dados: {str(datetime.datetime.now() - inter_timer)}')

        create_parquet(dd_simples, table_name)

    except Exception as e:
        logger.error(f'Erro ao manipular dados do Simples Nacional: {str(e)}')
        return False

    file_delete(path_unzip)
    logger.info(f'Tempo total de manipulação do Simples Nacional: {str(datetime.datetime.now() - inter_time)}')
    return True 