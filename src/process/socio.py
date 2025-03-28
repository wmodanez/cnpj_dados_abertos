import datetime
import logging
import dask.dataframe as dd
from config import config
from ..utils import file_extractor, file_delete
from ..download import check_download

logger = logging.getLogger(__name__)

def process_socio(soup, url: str, path_zip: str, path_unzip: str) -> bool:
    """Processa os dados de sócios."""
    logger.info('Início da manipulação dos Sócios')
    inter_time: datetime = datetime.datetime.now()

    try:
        table_name: str = 'socios'
        for link in [x for x in soup.find_all('a') if str(x.get('href')).endswith('.zip')]:
            if not check_download(link, table_name.capitalize(), url, path_zip):
                return False

        file_extractor(path_zip, path_unzip, 'Soc*.*')

        dd_socio: dd = dd.read_csv(
            'dados-abertos/*.SOCIO',
            sep=config.file.separator,
            names=config.socio_columns,
            encoding=config.file.encoding,
            dtype=config.socio_dtypes
        )

        inter_timer: datetime = datetime.datetime.now()
        logger.info(f'Tempo de manipulação dos dados: {str(datetime.datetime.now() - inter_timer)}')

        create_parquet(dd_socio, table_name)

    except Exception as e:
        logger.error(f'Erro ao manipular dados de sócios: {str(e)}')
        return False

    file_delete(path_unzip)
    logger.info(f'Tempo total de manipulação dos Sócios: {str(datetime.datetime.now() - inter_time)}')
    return True 