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

def process_estabelecimento(soup, url: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de estabelecimentos."""
    logger = setup_logging()
    logger.info('='*50)
    logger.info('Iniciando processamento de ESTABELECIMENTOS')
    logger.info('='*50)
    
    # Faz o download dos arquivos em paralelo
    logger.info('Iniciando downloads em paralelo...')
    if not download_files_parallel(soup, 'Estabelecimento', url, path_zip):
        logger.error('Erro ao baixar arquivos de ESTABELECIMENTOS')
        return False
    logger.info('Downloads concluídos com sucesso')
    
    # Descompacta os arquivos
    logger.info('Iniciando descompactação...')
    try:
        for file_name in os.listdir(path_zip):
            if file_name.startswith('Estabelecimento') and file_name.endswith('.zip'):
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
        csv_files = [f for f in os.listdir(path_unzip) if f.startswith('Estabelecimento') and f.endswith('.csv')]
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
                    'cnpj_ordem': 'object',
                    'cnpj_dv': 'object',
                    'identificador_matriz_filial': 'object',
                    'nome_fantasia': 'object',
                    'situacao_cadastral': 'object',
                    'data_situacao_cadastral': 'object',
                    'motivo_situacao_cadastral': 'object',
                    'nome_cidade_exterior': 'object',
                    'pais': 'object',
                    'data_inicio_atividade': 'object',
                    'cnae_fiscal_principal': 'object',
                    'cnae_fiscal_secundaria': 'object',
                    'tipo_logradouro': 'object',
                    'logradouro': 'object',
                    'numero': 'object',
                    'complemento': 'object',
                    'bairro': 'object',
                    'cep': 'object',
                    'uf': 'object',
                    'municipio': 'object',
                    'ddd_1': 'object',
                    'telefone_1': 'object',
                    'ddd_2': 'object',
                    'telefone_2': 'object',
                    'ddd_fax': 'object',
                    'fax': 'object',
                    'correio_eletronico': 'object',
                    'situacao_especial': 'object',
                    'data_situacao_especial': 'object'
                }
            )
            dfs.append(df)
        
        dd_estabelecimento = dd.concat(dfs)
        
        # Renomeia as colunas
        dd_estabelecimento = dd_estabelecimento.rename(columns={
            'cnpj_basico': 'cnpj',
            'cnpj_ordem': 'ordem',
            'cnpj_dv': 'dv',
            'identificador_matriz_filial': 'matriz_filial',
            'nome_fantasia': 'nome_fantasia',
            'situacao_cadastral': 'situacao_cadastral',
            'data_situacao_cadastral': 'data_situacao_cadastral',
            'motivo_situacao_cadastral': 'motivo_situacao_cadastral',
            'nome_cidade_exterior': 'cidade_exterior',
            'pais': 'pais',
            'data_inicio_atividade': 'data_inicio_atividade',
            'cnae_fiscal_principal': 'cnae_principal',
            'cnae_fiscal_secundaria': 'cnae_secundaria',
            'tipo_logradouro': 'tipo_logradouro',
            'logradouro': 'logradouro',
            'numero': 'numero',
            'complemento': 'complemento',
            'bairro': 'bairro',
            'cep': 'cep',
            'uf': 'uf',
            'municipio': 'municipio',
            'ddd_1': 'ddd_1',
            'telefone_1': 'telefone_1',
            'ddd_2': 'ddd_2',
            'telefone_2': 'telefone_2',
            'ddd_fax': 'ddd_fax',
            'fax': 'fax',
            'correio_eletronico': 'email',
            'situacao_especial': 'situacao_especial',
            'data_situacao_especial': 'data_situacao_especial'
        })
        
        # Converte para parquet
        table_name = 'estabelecimento'
        logger.info(f'Criando arquivo parquet {table_name}...')
        create_parquet(dd_estabelecimento, table_name, path_parquet)
        logger.info('Processamento concluído com sucesso')
        
        # Limpa os arquivos temporários
        file_delete(path_unzip)
        logger.info('Arquivos temporários removidos')
        
        return True
        
    except Exception as e:
        logger.error(f'Erro ao processar dados: {str(e)}')
        return False 