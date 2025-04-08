import datetime
import logging
import dask.dataframe as dd
import pandas as pd
from ..config import config
from ..utils import file_extractor, file_delete, check_disk_space, estimate_zip_extracted_size, create_parquet_filename
import os
import zipfile
import csv
import io

logger = logging.getLogger(__name__)

def create_parquet(df, table_name, path_parquet):
    """Converte um DataFrame para formato parquet.
    
    Args:
        df: DataFrame Dask a ser convertido
        table_name: Nome da tabela
        path_parquet: Caminho base para os arquivos parquet
    """
    output_dir = os.path.join(path_parquet, table_name)
    
    # Limpa o diretório antes de criar os novos arquivos
    try:
        file_delete(output_dir)
        logger.info(f'Diretório {output_dir} limpo antes de criar novos arquivos parquet')
    except Exception as e:
        logger.warning(f'Não foi possível limpar diretório {output_dir}: {str(e)}')
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Configura o nome dos arquivos parquet com prefixo da tabela
    df.to_parquet(
        output_dir,
        write_index=False,
        name_function=lambda i: create_parquet_filename(table_name, i)
    )

def process_socio(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios."""
    logger.info('='*50)
    logger.info('Iniciando processamento de SÓCIOS')
    logger.info('='*50)
    
    logger.info("Verificando espaço em disco necessário para descompactação...")
    has_space, available_mb = check_disk_space(path_unzip, 4000)
    if not has_space:
        logger.error(f"Espaço em disco insuficiente para processar os dados. Disponível: {available_mb:.2f}MB, necessário: 4000MB")
        return False
    logger.info(f"Verificação de espaço em disco concluída: {available_mb:.2f}MB disponível")
    
    # Limpa o diretório de descompactação antes de começar
    try:
        file_delete(path_unzip)
    except PermissionError as e:
        logger.error(f'Sem permissão para limpar diretório de descompactação: {str(e)}')
        return False
    except Exception as e:
        logger.error(f'Erro inesperado ao limpar diretório de descompactação: {str(e)}')
        return False

    logger.info(f'Iniciando processamento de arquivos ZIP existentes em {path_zip}...')
    success = False
    
    try:
        zip_files = [f for f in os.listdir(path_zip) if f.startswith('Socio') and f.endswith('.zip')]
        if not zip_files:
            logger.warning(f'Nenhum arquivo ZIP de Sócios encontrado em {path_zip} para processar.')
            return True

        all_dfs = []
        
        for zip_file in zip_files:
            zip_path = os.path.join(path_zip, zip_file)
            logger.info(f'Processando arquivo ZIP: {zip_file}')
            estimated_size_mb = estimate_zip_extracted_size(zip_path)
            logger.info(f"Tamanho estimado após descompactação: {estimated_size_mb:.2f}MB")
            has_space_for_file, available_mb_now = check_disk_space(path_unzip, estimated_size_mb * 1.2)
            if not has_space_for_file:
                logger.error(f"Espaço insuficiente para descompactar {zip_file}. Disponível: {available_mb_now:.2f}MB, necessário: {estimated_size_mb * 1.2:.2f}MB")
                continue
            
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
            
            try:
                csv_files = [f for f in os.listdir(path_unzip) if 'CSV' in f]
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
            
            for csv_file in csv_files:
                csv_path = os.path.join(path_unzip, csv_file)
                logger.info(f'Processando arquivo CSV: {csv_file}')
                
                try:
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
                            'identificador_socio': 'object',
                            'nome_socio': 'object',
                            'cnpj_cpf_socio': 'object',
                            'qualificacao_socio': 'object',
                            'data_entrada_sociedade': 'object',
                            'pais': 'object',
                            'representante_legal': 'object',
                            'nome_representante': 'object',
                            'qualificacao_representante': 'object',
                            'faixa_etaria': 'object',
                            'Unnamed: 8': 'object'
                        },
                        sep=';',
                        encoding='latin1',
                        quoting=csv.QUOTE_MINIMAL,
                        escapechar='\\',
                        on_bad_lines='warn',
                        low_memory=False
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
            
            success = True
            
        if all_dfs:
            logger.info('Concatenando todos os DataFrames...')
            try:
                parquet_size_estimate = sum([df.memory_usage(deep=True).sum().compute() for df in all_dfs]) * 0.5 / (1024 * 1024)
                has_space, available_mb = check_disk_space(path_parquet, parquet_size_estimate)
                
                if not has_space:
                    logger.error(f"Espaço insuficiente para criar arquivo parquet. Disponível: {available_mb:.2f}MB, estimado: {parquet_size_estimate:.2f}MB")
                    return False
                
                dd_socio = dd.concat(all_dfs)
                
                dd_socio = dd_socio.rename(columns={
                    'cnpj_basico': 'cnpj',
                    'identificador_socio': 'identificador_socio',
                    'nome_socio': 'nome_socio',
                    'cnpj_cpf_socio': 'cnpj_cpf_socio',
                    'qualificacao_socio': 'qualificacao_socio',
                    'data_entrada_sociedade': 'data_entrada_sociedade',
                    'pais': 'pais',
                    'representante_legal': 'representante_legal',
                    'nome_representante': 'nome_representante',
                    'qualificacao_representante': 'qualificacao_representante',
                    'faixa_etaria': 'faixa_etaria'
                })
                
                table_name = 'socio'
                logger.info(f'Criando arquivo parquet {table_name}...')
                try:
                    create_parquet(dd_socio, table_name, path_parquet)
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

                # Limpa o diretório após criar os arquivos parquet
                try:
                    file_delete(path_unzip)
                    logger.info('Diretório de descompactação limpo após processamento')
                except Exception as e:
                    logger.warning(f'Não foi possível limpar diretório após processamento: {str(e)}')

            except MemoryError as e:
                logger.error(f'Memória insuficiente para concatenar DataFrames: {str(e)}')
                success = False
            except Exception as e:
                logger.error(f'Erro inesperado ao concatenar DataFrames: {str(e)}')
                success = False
        else:
            logger.error('Nenhum dado foi processado com sucesso')
        
        return success
        
    except FileNotFoundError:
        logger.error(f"Diretório de origem dos ZIPs não encontrado: {path_zip}")
        return False
    except Exception as e:
        logger.exception(f'Erro inesperado no processo principal de sócios: {e}')
        return False 