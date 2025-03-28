import datetime
import logging
import dask.dataframe as dd
from config import config
from ..utils import file_extractor, file_delete
from ..download import download_files_parallel

logger = logging.getLogger(__name__)

def process_empresa(soup, url: str, path_zip: str, path_unzip: str) -> bool:
    """Processa os dados de empresas."""
    logger.info('Início da manipulação das Empresas')
    inter_time: datetime = datetime.datetime.now()

    try:
        table_name: str = 'empresas'
        if not download_files_parallel(soup, table_name.capitalize(), url, path_zip):
            return False

        file_extractor(path_zip, path_unzip, 'Emp*.*')

        dd_empresa: dd = dd.read_csv(
            'dados-abertos/*.EMPRECSV', 
            sep=config.file.separator, 
            names=config.empresa_columns, 
            encoding=config.file.encoding,
            dtype=config.empresa_dtypes
        )

        inter_timer: datetime = datetime.datetime.now()
        logger.info(f'Tempo de manipulação dos dados: {str(datetime.datetime.now() - inter_timer)}')

        create_parquet(dd_empresa, table_name)

        try:
            # Tratamento para criar as empresas privadas
            dd_empresa_privada: dd = dd_empresa[dd_empresa['natureza_juridica'].between(2046, 2348)]
            dd_empresa_privada = dd_empresa_privada.drop('qualificacao_responsavel', axis=1)
            dd_empresa_privada = dd_empresa_privada.drop('ente_federativo_responsavel', axis=1)

            create_parquet(dd_empresa_privada, 'empresa_privada')
        except Exception as e:
            logger.error(f'Erro ao processar empresas privadas: {str(e)}')
            return False

    except Exception as e:
        logger.error(f'Erro ao manipular empresas: {str(e)}')
        return False

    file_delete(path_unzip)
    logger.info(f'Tempo total de manipulação das Empresas: {str(datetime.datetime.now() - inter_time)}')
    return True 