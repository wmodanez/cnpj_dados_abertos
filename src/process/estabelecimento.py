import datetime
import logging
import dask.dataframe as dd
import pandas as pd
from ..config import config
from ..utils import file_extractor, file_delete, check_disk_space, estimate_zip_extracted_size
from ..utils import process_csv_files_parallel, process_csv_to_df, verify_csv_integrity, create_parquet_filename
import os
import zipfile
import csv
import io
from concurrent.futures import ThreadPoolExecutor

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
    
    # Log das colunas antes de salvar
    logger.info(f"Colunas do DataFrame '{table_name}' antes de salvar em Parquet: {list(df.columns)}")
    
    # Configura o nome dos arquivos parquet com prefixo da tabela
    df.to_parquet(
        output_dir,
        engine='pyarrow',  # Especifica o engine
        write_index=False,
        name_function=lambda i: create_parquet_filename(table_name, i)
    )

def process_csv_file(csv_path):
    """
    Processa um único arquivo CSV de estabelecimento e retorna um DataFrame Dask.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Dask ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None
    
    # Define os tipos de dados e os nomes originais das colunas
    dtype_dict = {
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
    original_column_names = list(dtype_dict.keys())
    
    try:
        # Passa os nomes das colunas explicitamente
        df = process_csv_to_df(csv_path, dtype=dtype_dict, column_names=original_column_names)
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)}: {str(e)}')
        return None

def process_estabelecimento(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de estabelecimentos."""
    logger.info('='*50)
    logger.info('Iniciando processamento de ESTABELECIMENTOS')
    logger.info('='*50)
    
    # Verifica espaço em disco (Manter)
    logger.info("Verificando espaço em disco necessário para descompactação...")
    
    # Limpa o diretório de descompactação antes de começar
    try:
        file_delete(path_unzip)
    except PermissionError as e:
        logger.error(f'Sem permissão para limpar diretório de descompactação: {str(e)}')
        return False
    except Exception as e:
        logger.error(f'Erro inesperado ao limpar diretório de descompactação: {str(e)}')
        return False

    # Processa um arquivo ZIP por vez
    logger.info(f'Iniciando processamento de arquivos ZIP existentes em {path_zip}...')
    success = False
    try:
        # Lista todos os arquivos ZIP de estabelecimentos
        zip_files = [f for f in os.listdir(path_zip) if f.startswith('Estabelecimento') and f.endswith('.zip')]
        if not zip_files:
            logger.warning(f'Nenhum arquivo ZIP de Estabelecimentos encontrado em {path_zip} para processar.')
            return True # Não é erro
        
        all_dfs = []
        for zip_file in zip_files:
            # Mover verificação de espaço para cá
            zip_path = os.path.join(path_zip, zip_file)
            logger.info(f'Processando arquivo ZIP: {zip_file}')
            estimated_size_mb = estimate_zip_extracted_size(zip_path)
            logger.info(f"Tamanho estimado após descompactação: {estimated_size_mb:.2f}MB")
            has_space_for_file, available_mb_now = check_disk_space(path_unzip, estimated_size_mb * 1.2)
            if not has_space_for_file:
                logger.error(f"Espaço insuficiente para descompactar {zip_file}. Disponível: {available_mb_now:.2f}MB, necessário: {estimated_size_mb * 1.2:.2f}MB")
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
                
                # Processa todos os arquivos CSV em paralelo
                logger.info(f'Processando {len(csv_files)} arquivos CSV em paralelo...')
                
                dfs = process_csv_files_parallel(
                    csv_files=csv_files,
                    base_path=path_unzip,
                    process_function=process_csv_file,
                    max_workers=config.dask.n_workers
                )
                
                # Filtra DataFrames vazios ou inválidos
                dfs = [df for df in dfs if df is not None]
                
                if dfs:
                    all_dfs.extend(dfs)
                    logger.info(f'Processamento de {len(dfs)} arquivos CSV concluído com sucesso')
                else:
                    logger.warning(f'Nenhum DataFrame válido foi gerado a partir dos arquivos CSV')
                
                success = True
                
            except Exception as e:
                logger.error(f'Erro inesperado ao processar dados do arquivo {zip_file}: {str(e)}')
                continue
        
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
                
                dd_estabelecimento = dd.concat(all_dfs)
                
                # Renomeia as colunas usando os nomes originais corretos como chave
                dd_estabelecimento = dd_estabelecimento.rename(columns={
                    'cnpj_basico': 'cnpj', # Chave original
                    'cnpj_ordem': 'ordem', # Chave original
                    'cnpj_dv': 'dv', # Chave original
                    'identificador_matriz_filial': 'matriz_filial', # Chave original
                    'nome_fantasia': 'nome_fantasia', # Chave original
                    'situacao_cadastral': 'situacao_cadastral', # Chave original
                    'data_situacao_cadastral': 'data_situacao_cadastral', # Chave original
                    'motivo_situacao_cadastral': 'motivo_situacao_cadastral', # Chave original
                    'nome_cidade_exterior': 'cidade_exterior', # Chave original
                    'pais': 'pais', # Chave original
                    'data_inicio_atividade': 'data_inicio_atividade', # Chave original
                    'cnae_fiscal_principal': 'cnae_principal', # Chave original
                    'cnae_fiscal_secundaria': 'cnae_secundaria', # Chave original
                    'tipo_logradouro': 'tipo_logradouro', # Chave original
                    'logradouro': 'logradouro', # Chave original
                    'numero': 'numero', # Chave original
                    'complemento': 'complemento', # Chave original
                    'bairro': 'bairro', # Chave original
                    'cep': 'cep', # Chave original
                    'uf': 'uf', # Chave original
                    'municipio': 'municipio', # Chave original
                    'ddd_1': 'ddd_1', # Chave original
                    'telefone_1': 'telefone_1', # Chave original
                    'ddd_2': 'ddd_2', # Chave original
                    'telefone_2': 'telefone_2', # Chave original
                    'ddd_fax': 'ddd_fax', # Chave original
                    'fax': 'fax', # Chave original
                    'correio_eletronico': 'email', # Chave original
                    'situacao_especial': 'situacao_especial', # Chave original
                    'data_situacao_especial': 'data_situacao_especial' # Chave original
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
        logger.exception(f'Erro inesperado no processo principal de estabelecimentos: {e}')
        # ... (limpeza de path_unzip) ...
        return False 