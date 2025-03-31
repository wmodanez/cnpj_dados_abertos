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

def process_socio(soup, url: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios."""
    logger = setup_logging()
    logger.info('='*50)
    logger.info('Iniciando processamento de SÓCIOS')
    logger.info('='*50)
    
    # Faz o download dos arquivos em paralelo
    logger.info('Iniciando downloads em paralelo...')
    if not download_files_parallel(soup, 'Socio', url, path_zip):
        logger.error('Erro ao baixar arquivos de SÓCIOS')
        return False
    logger.info('Downloads concluídos com sucesso')
    
    # Descompacta os arquivos
    logger.info('Iniciando descompactação...')
    try:
        for file_name in os.listdir(path_zip):
            if file_name.startswith('Socio') and file_name.endswith('.zip'):
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
        csv_files = [f for f in os.listdir(path_unzip) if f.startswith('Socio') and f.endswith('.csv')]
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
                    'identificador_socio': 'object',
                    'nome_socio': 'object',
                    'cnpj_cpf_socio': 'object',
                    'codigo_qualificacao_socio': 'object',
                    'percentual_capital_social': 'object',
                    'data_entrada_sociedade': 'object',
                    'codigo_pais': 'object',
                    'nome_pais_socio': 'object',
                    'cpf_representante_legal': 'object',
                    'nome_representante_legal': 'object',
                    'codigo_qualificacao_representante_legal': 'object'
                }
            )
            dfs.append(df)
        
        dd_socio = dd.concat(dfs)
        
        # Renomeia as colunas
        dd_socio = dd_socio.rename(columns={
            'cnpj_basico': 'cnpj',
            'identificador_socio': 'tipo',
            'nome_socio': 'nome',
            'cnpj_cpf_socio': 'documento',
            'codigo_qualificacao_socio': 'qualificacao',
            'percentual_capital_social': 'capital',
            'data_entrada_sociedade': 'data_entrada',
            'codigo_pais': 'pais',
            'nome_pais_socio': 'nome_pais',
            'cpf_representante_legal': 'cpf_representante',
            'nome_representante_legal': 'nome_representante',
            'codigo_qualificacao_representante_legal': 'qualificacao_representante'
        })
        
        # Converte para parquet
        table_name = 'socio'
        logger.info(f'Criando arquivo parquet {table_name}...')
        create_parquet(dd_socio, table_name, path_parquet)
        logger.info('Processamento concluído com sucesso')
        
        # Limpa os arquivos temporários
        file_delete(path_unzip)
        logger.info('Arquivos temporários removidos')
        
        return True
        
    except Exception as e:
        logger.error(f'Erro ao processar dados: {str(e)}')
        return False 