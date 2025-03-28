import datetime
import logging
import dask.dataframe as dd
from config import config
from ..utils import file_extractor, file_delete
from ..download import download_files_parallel

logger = logging.getLogger(__name__)

def process_estabelecimento(soup, url: str, path_zip: str, path_unzip: str) -> bool:
    """Processa os dados de estabelecimentos."""
    logger.info('Início da manipulação dos Estabelecimentos')
    inter_time: datetime = datetime.datetime.now()

    try:
        table_name: str = 'estabelecimentos'
        if not download_files_parallel(soup, table_name.capitalize(), url, path_zip):
            return False

        file_extractor(path_zip, path_unzip, 'Est*.*')

        dd_estabelecimento: dd = dd.read_csv(
            'dados-abertos/*.ESTABELE',
            sep=config.file.separator,
            names=config.estabelecimento_columns,
            encoding=config.file.encoding,
            dtype=config.estabelecimento_dtypes
        )

        inter_timer: datetime = datetime.datetime.now()
        logger.info(f'Tempo de manipulação dos dados: {str(datetime.datetime.now() - inter_timer)}')

        create_parquet(dd_estabelecimento, table_name)

        try:
            # Tratamento para criar os estabelecimentos de Goiás
            dd_estabelecimento_go: dd = dd_estabelecimento[dd_estabelecimento['uf'] == 'GO']
            create_parquet(dd_estabelecimento_go, 'estabelecimentos_go')
        except Exception as e:
            logger.error(f'Erro ao processar estabelecimentos de Goiás: {str(e)}')
            return False

    except Exception as e:
        logger.error(f'Erro ao manipular estabelecimentos: {str(e)}')
        return False

    file_delete(path_unzip)
    logger.info(f'Tempo total de manipulação dos Estabelecimentos: {str(datetime.datetime.now() - inter_time)}')
    return True 