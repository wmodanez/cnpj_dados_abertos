import datetime
import logging
import dask.dataframe as dd
import pandas as pd
from config import config
from ..utils import file_extractor, file_delete
from ..download import download_files_parallel
from ..utils.logging import setup_logging, Colors
import os
import zipfile
import csv
import io

logger = logging.getLogger(__name__)

def create_parquet(df, table_name, path_parquet):
    """Converte um DataFrame para formato parquet."""
    yyyymm = datetime.date.today().strftime('%Y%m')
    output_dir = os.path.join(path_parquet, yyyymm, table_name)
    os.makedirs(output_dir, exist_ok=True)
    df.to_parquet(output_dir, write_index=False)

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
    
    # Processa um arquivo ZIP por vez
    logger.info('Iniciando processamento de arquivos...')
    success = False
    
    try:
        # Lista todos os arquivos ZIP de estabelecimentos
        try:
            zip_files = [f for f in os.listdir(path_zip) if f.startswith('Estabelecimento') and f.endswith('.zip')]
            if not zip_files:
                logger.error('Nenhum arquivo ZIP encontrado')
                return False
        except FileNotFoundError as e:
            logger.error(f'Diretório de arquivos ZIP não encontrado: {str(e)}')
            return False
        except PermissionError as e:
            logger.error(f'Sem permissão para acessar o diretório de arquivos ZIP: {str(e)}')
            return False
        except Exception as e:
            logger.error(f'Erro inesperado ao listar arquivos ZIP: {str(e)}')
            return False
        
        all_dfs = []
        
        # Processa cada arquivo ZIP individualmente
        for zip_file in zip_files:
            zip_path = os.path.join(path_zip, zip_file)
            logger.info(f'Processando arquivo ZIP: {zip_file}')
            
            # Limpa o diretório de descompactação antes de começar
            try:
                file_delete(path_unzip)
            except PermissionError as e:
                logger.error(f'Sem permissão para limpar diretório de descompactação: {str(e)}')
                continue
            except Exception as e:
                logger.error(f'Erro inesperado ao limpar diretório de descompactação: {str(e)}')
                continue
            
            # Descompacta apenas este arquivo ZIP
            try:
                logger.info(f'Descompactando arquivo: {zip_path}')
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(path_unzip)
                logger.info('Descompactação concluída com sucesso')
            except zipfile.BadZipFile as e:
                logger.error(f'Arquivo ZIP corrompido ou inválido {zip_path}: {str(e)}')
                continue
            except zipfile.LargeZipFile as e:
                logger.error(f'Arquivo ZIP muito grande para processamento {zip_path}: {str(e)}')
                continue
            except PermissionError as e:
                logger.error(f'Sem permissão para extrair arquivo ZIP {zip_path}: {str(e)}')
                continue
            except MemoryError as e:
                logger.error(f'Memória insuficiente para extrair arquivo ZIP {zip_path}: {str(e)}')
                continue
            except Exception as e:
                logger.error(f'Erro inesperado ao descompactar arquivo {zip_path}: {str(e)}')
                continue
            
            # Processa os dados deste arquivo
            try:
                # Lê todos os arquivos CSV descompactados
                try:
                    csv_files = [f for f in os.listdir(path_unzip) if f.startswith('Estabelecimento') and f.endswith('.csv')]
                    if not csv_files:
                        logger.warning(f'Nenhum arquivo CSV encontrado após descompactar {zip_file}')
                        continue
                except FileNotFoundError as e:
                    logger.error(f'Diretório de descompactação não encontrado: {str(e)}')
                    continue
                except PermissionError as e:
                    logger.error(f'Sem permissão para acessar o diretório de descompactação: {str(e)}')
                    continue
                except Exception as e:
                    logger.error(f'Erro inesperado ao listar arquivos CSV: {str(e)}')
                    continue
                
                # Lê e concatena todos os DataFrames deste arquivo ZIP
                for csv_file in csv_files:
                    csv_path = os.path.join(path_unzip, csv_file)
                    logger.info(f'Processando arquivo CSV: {csv_file}')
                    
                    # Verifica integridade do CSV antes de processá-lo
                    try:
                        # Tenta ler as primeiras linhas para verificar integridade
                        with open(csv_path, 'r', encoding='utf-8') as f:
                            reader = csv.reader(f)
                            for _ in range(5):
                                next(reader, None)
                    except UnicodeDecodeError as e:
                        logger.error(f'Erro de codificação no arquivo {csv_file}: {str(e)}')
                        continue
                    except csv.Error as e:
                        logger.error(f'Erro de formato CSV no arquivo {csv_file}: {str(e)}')
                        continue
                    except Exception as e:
                        logger.error(f'Erro ao verificar integridade do arquivo {csv_file}: {str(e)}')
                        continue
                    
                    try:
                        df = dd.read_csv(
                            csv_path,
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
                        all_dfs.append(df)
                    except pd.errors.EmptyDataError as e:
                        logger.error(f'Arquivo CSV vazio {csv_file}: {str(e)}')
                        continue
                    except pd.errors.ParserError as e:
                        logger.error(f'Erro de parse no arquivo CSV {csv_file}: {str(e)}')
                        continue
                    except MemoryError as e:
                        logger.error(f'Memória insuficiente para processar arquivo CSV {csv_file}: {str(e)}')
                        continue
                    except Exception as e:
                        logger.error(f'Erro inesperado ao processar arquivo CSV {csv_file}: {str(e)}')
                        continue
                
                # Limpa os arquivos temporários após processamento
                try:
                    file_delete(path_unzip)
                    logger.info(f'Arquivos temporários de {zip_file} removidos')
                except Exception as e:
                    logger.warning(f'Não foi possível remover arquivos temporários de {zip_file}: {str(e)}')
                
                success = True
                
            except Exception as e:
                logger.error(f'Erro inesperado ao processar dados do arquivo {zip_file}: {str(e)}')
                # Mesmo em caso de erro, limpa os arquivos temporários
                try:
                    file_delete(path_unzip)
                except Exception as clean_error:
                    logger.warning(f'Não foi possível limpar arquivos temporários após erro: {str(clean_error)}')
        
        # Se temos DataFrames para processar, concatena todos e cria o parquet
        if all_dfs:
            logger.info('Concatenando todos os DataFrames...')
            try:
                dd_estabelecimento = dd.concat(all_dfs)
                
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
                try:
                    create_parquet(dd_estabelecimento, table_name, path_parquet)
                    logger.info('Processamento concluído com sucesso')
                    success = True
                except PermissionError as e:
                    logger.error(f'Sem permissão para criar arquivo parquet: {str(e)}')
                    success = False
                except IOError as e:
                    logger.error(f'Erro de I/O ao criar arquivo parquet: {str(e)}')
                    success = False
                except Exception as e:
                    logger.error(f'Erro inesperado ao criar arquivo parquet: {str(e)}')
                    success = False
            except MemoryError as e:
                logger.error(f'Memória insuficiente para concatenar DataFrames: {str(e)}')
                success = False
            except Exception as e:
                logger.error(f'Erro inesperado ao concatenar DataFrames: {str(e)}')
                success = False
        else:
            logger.error('Nenhum dado foi processado com sucesso')
        
        return success
        
    except Exception as e:
        logger.error(f'Erro inesperado no processo principal: {str(e)}')
        # Certifica-se de limpar os arquivos temporários em caso de erro
        try:
            file_delete(path_unzip)
        except Exception as clean_error:
            logger.warning(f'Não foi possível limpar arquivos temporários após erro fatal: {str(clean_error)}')
        return False 