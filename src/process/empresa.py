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

def process_empresa(soup, url: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de empresas."""
    logger = setup_logging()
    logger.info('='*50)
    logger.info('Iniciando processamento de EMPRESAS')
    logger.info('='*50)
    
    # Faz o download dos arquivos em paralelo
    logger.info('Iniciando downloads em paralelo...')
    if not download_files_parallel(soup, 'Empresa', url, path_zip):
        logger.error('Erro ao baixar arquivos de EMPRESAS')
        return False
    logger.info('Downloads concluídos com sucesso')
    
    # Descompacta os arquivos
    logger.info('Iniciando descompactação...')
    try:
        for file_name in os.listdir(path_zip):
            if file_name.startswith('Empresa') and file_name.endswith('.zip'):
                file_path = os.path.join(path_zip, file_name)
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(path_unzip)
        logger.info('Descompactação concluída com sucesso')
    except Exception as e:
        logger.error(f'Erro ao descompactar arquivos: {str(e)}')
        return False
    
    # Processa os dados
    logger.info('Iniciando processamento dos dados...')
    try:
        # Lê todos os arquivos CSV
        csv_files = [f for f in os.listdir(path_unzip) if f.startswith('Empresa') and f.endswith('.csv')]
        if not csv_files:
            logger.error('Nenhum arquivo CSV encontrado após descompactação')
            return False
            
        # Concatena todos os DataFrames
        dfs = []
        for csv_file in csv_files:
            df = dd.read_csv(
                os.path.join(path_unzip, csv_file),
                dtype={
                    'cnpj_basico': 'object',
                    'razao_social': 'object',
                    'natureza_juridica': 'object',
                    'qualificacao_responsavel': 'object',
                    'capital_social': 'object',
                    'porte_empresa': 'object',
                    'ente_federativo_responsavel': 'object'
                }
            )
            dfs.append(df)
        
        dd_empresa = dd.concat(dfs)
        
        # Renomeia as colunas
        dd_empresa = dd_empresa.rename(columns={
            'cnpj_basico': 'cnpj',
            'razao_social': 'razao_social',
            'natureza_juridica': 'natureza_juridica',
            'qualificacao_responsavel': 'qualificacao_responsavel',
            'capital_social': 'capital_social',
            'porte_empresa': 'porte_empresa',
            'ente_federativo_responsavel': 'ente_federativo_responsavel'
        })
        
        # Converte para parquet
        table_name = 'empresa'
        logger.info(f'Criando arquivo parquet {table_name}...')
        create_parquet(dd_empresa, table_name, path_parquet)
        logger.info('Processamento concluído com sucesso')
        
        # Limpa os arquivos temporários
        file_delete(path_unzip)
        logger.info('Arquivos temporários removidos')
        
        return True
        
    except Exception as e:
        logger.error(f'Erro ao processar dados: {str(e)}')
        return False

    file_delete(path_unzip)
    logger.info(f'Tempo total de manipulação das Empresas: {str(datetime.datetime.now() - inter_time)}')
    return True 