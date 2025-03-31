import datetime
import logging
import dask.dataframe as dd
from config import config
from ..utils import file_extractor, file_delete
from ..download import download_files_parallel
from ..utils.logging import setup_logging, Colors
import os
import zipfile

logger = logging.getLogger(__name__)

def process_simples(soup, url: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados do Simples Nacional."""
    logger = setup_logging()
    logger.info('='*50)
    logger.info('Iniciando processamento do SIMPLES NACIONAL')
    logger.info('='*50)
    
    # Faz o download dos arquivos em paralelo
    logger.info('Iniciando downloads em paralelo...')
    if not download_files_parallel(soup, 'Simples', url, path_zip):
        logger.error('Erro ao baixar arquivos do SIMPLES NACIONAL')
        return False
    logger.info('Downloads concluídos com sucesso')
    
    # Descompacta o arquivo
    logger.info('Iniciando descompactação...')
    try:
        for file_name in os.listdir(path_zip):
            if file_name.startswith('Simples') and file_name.endswith('.zip'):
                file_path = os.path.join(path_zip, file_name)
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(path_unzip)
        logger.info('Descompactação concluída com sucesso')
    except Exception as e:
        logger.error(f'Erro ao descompactar arquivo: {str(e)}')
        return False
    
    # Processa os dados
    logger.info('Iniciando processamento dos dados...')
    try:
        # Lê o arquivo CSV
        csv_file = next(f for f in os.listdir(path_unzip) if f.startswith('Simples') and f.endswith('.csv'))
        if not csv_file:
            logger.error('Nenhum arquivo CSV encontrado após descompactação')
            return False
            
        dd_simples = dd.read_csv(
            os.path.join(path_unzip, csv_file),
            dtype={
                'cnpj_basico': 'object',
                'opcao_simples': 'object',
                'data_opcao_simples': 'object',
                'data_exclusao_simples': 'object',
                'opcao_mei': 'object',
                'data_opcao_mei': 'object',
                'data_exclusao_mei': 'object'
            }
        )
        
        # Renomeia as colunas
        dd_simples = dd_simples.rename(columns={
            'cnpj_basico': 'cnpj',
            'opcao_simples': 'opcao_simples',
            'data_opcao_simples': 'data_opcao_simples',
            'data_exclusao_simples': 'data_exclusao_simples',
            'opcao_mei': 'opcao_mei',
            'data_opcao_mei': 'data_opcao_mei',
            'data_exclusao_mei': 'data_exclusao_mei'
        })
        
        # Converte para parquet
        table_name = 'simples'
        logger.info(f'Criando arquivo parquet {table_name}...')
        create_parquet(dd_simples, table_name, path_parquet)
        logger.info('Processamento concluído com sucesso')
        
        # Limpa os arquivos temporários
        file_delete(path_unzip)
        logger.info('Arquivos temporários removidos')
        
        return True
        
    except Exception as e:
        logger.error(f'Erro ao processar dados: {str(e)}')
        return False 