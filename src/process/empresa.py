import datetime
import logging
import dask.dataframe as dd
import pandas as pd
from config import config
from ..utils import file_extractor, file_delete, check_disk_space, estimate_zip_extracted_size, check_internet_connection
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

def process_empresa(soup, url: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de empresas."""
    logger = setup_logging()
    logger.info('='*50)
    logger.info('Iniciando processamento de EMPRESAS')
    logger.info('='*50)
    
    # Verifica conexão com a internet antes de prosseguir
    logger.info('Verificando conexão com a internet...')
    internet_ok, message = check_internet_connection()
    if not internet_ok:
        logger.error(f'Falha na conexão com a internet: {message}')
        logger.error('Não é possível continuar o processamento sem conexão com a internet.')
        return False
    logger.info(f'Conexão com a internet verificada com sucesso: {message}')
    
    # Verifica espaço em disco para o diretório de trabalho
    # Requisito mínimo: 5GB para trabalhar com segurança
    has_space, available_mb = check_disk_space(path_unzip, 5000)
    if not has_space:
        logger.error(f"Espaço em disco insuficiente para processar os dados. Disponível: {available_mb:.2f}MB, necessário: 5000MB")
        return False
    logger.info(f"Verificação de espaço em disco concluída: {available_mb:.2f}MB disponível")
    
    # Faz o download dos arquivos em paralelo
    logger.info('Iniciando downloads em paralelo...')
    if not download_files_parallel(soup, 'Empresa', url, path_zip):
        logger.error('Erro ao baixar arquivos de EMPRESAS')
        return False
    logger.info('Downloads concluídos com sucesso')
    
    # Processa um arquivo ZIP por vez
    logger.info('Iniciando processamento de arquivos...')
    success = False
    
    try:
        # Lista todos os arquivos ZIP de empresas
        try:
            zip_files = [f for f in os.listdir(path_zip) if f.startswith('Empresa') and f.endswith('.zip')]
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
            
            # Estima o tamanho que o arquivo ocupará quando descompactado
            estimated_size_mb = estimate_zip_extracted_size(zip_path)
            logger.info(f"Tamanho estimado após descompactação: {estimated_size_mb:.2f}MB")
            
            # Verifica se há espaço suficiente para descompactar este arquivo
            has_space, available_mb = check_disk_space(path_unzip, estimated_size_mb * 1.2)  # 20% de margem extra
            if not has_space:
                logger.error(f"Espaço insuficiente para descompactar {zip_file}. Disponível: {available_mb:.2f}MB, necessário: {estimated_size_mb * 1.2:.2f}MB")
                continue
            
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
                    csv_files = [f for f in os.listdir(path_unzip) if f.startswith('Empresa') and f.endswith('.csv')]
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
                                'razao_social': 'object',
                                'natureza_juridica': 'object',
                                'qualificacao_responsavel': 'object',
                                'capital_social': 'object',
                                'porte_empresa': 'object',
                                'ente_federativo_responsavel': 'object'
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
                # Verifica espaço para criação do arquivo parquet
                # Estima o tamanho como 50% do tamanho dos DataFrames em memória (compressão)
                parquet_size_estimate = sum([df.memory_usage(deep=True).sum().compute() for df in all_dfs]) * 0.5 / (1024 * 1024)
                has_space, available_mb = check_disk_space(path_parquet, parquet_size_estimate)
                
                if not has_space:
                    logger.error(f"Espaço insuficiente para criar arquivo parquet. Disponível: {available_mb:.2f}MB, estimado: {parquet_size_estimate:.2f}MB")
                    return False
                
                dd_empresa = dd.concat(all_dfs)
                
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
                try:
                    create_parquet(dd_empresa, table_name, path_parquet)
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