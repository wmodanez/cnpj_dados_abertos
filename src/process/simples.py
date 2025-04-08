import logging
import logging
import os
import zipfile

import dask.dataframe as dd
import numpy as np
import pandas as pd

from ..config import config
from ..utils import file_delete, check_disk_space, estimate_zip_extracted_size
from ..utils import process_csv_files_parallel, process_csv_to_df, verify_csv_integrity, create_parquet_filename

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
    Processa um único arquivo CSV de simples e retorna um DataFrame Dask.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Dask ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa colunas e dtypes da config
    original_column_names = config.simples_columns
    dtype_dict = config.simples_dtypes

    try:
        # Passa nomes, separador, encoding da config, na_filter=False
        df = process_csv_to_df(
            csv_path, 
            dtype=dtype_dict, 
            column_names=original_column_names,
            separator=config.file.separator, # Usa separador da config
            encoding=config.file.encoding,   # Usa encoding da config
            na_filter=False # Como em manipular_dados.py
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)}: {str(e)}')
        return None


def apply_simples_transformations(ddf):
    """Aplica as transformações específicas para Simples Nacional."""
    logger.info("Aplicando transformações em Simples...")
    
    # 1. Renomear colunas (Ajustar conforme nomes na config)
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico', # Mantém
        'opcao_pelo_simples': 'opcao_simples',
        'data_opcao_pelo_simples': 'data_opcao_simples',
        'data_exclusao_do_simples': 'data_exclusao_simples',
        'opcao_pelo_mei': 'opcao_mei',
        'data_opcao_pelo_mei': 'data_opcao_mei',
        'data_exclusao_do_mei': 'data_exclusao_mei'
    }
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in ddf.columns}
    ddf = ddf.rename(columns=actual_rename_mapping)
    logger.info(f"Colunas após renomear: {list(ddf.columns)}")

    # 2. Converter colunas de data
    date_cols_to_convert = [
        'data_opcao_simples', 'data_exclusao_simples', 
        'data_opcao_mei', 'data_exclusao_mei'
    ]
    for col in date_cols_to_convert:
        if col in ddf.columns:
            logger.info(f"Convertendo coluna de data: {col}")
            # Substituir '00000000' por NaN antes de converter
            def convert_date_partition(series):
                try:
                    series = series.str.replace('00000000', '', regex=False)
                except AttributeError:
                    pass 
                return pd.to_datetime(series, errors='coerce', format='%Y%m%d')

            meta = (col, 'datetime64[ns]') # Dask pode inferir a meta aqui
            ddf[col] = ddf[col].map_partitions(convert_date_partition, meta=meta)
            logger.info(f"Coluna '{col}' convertida para datetime.")
        else:
             logger.warning(f"Coluna de data '{col}' não encontrada para conversão.")

    # 3. Converter colunas de opção (Simples e MEI)
    option_cols_to_convert = {'opcao_simples': 'Int64', 'opcao_mei': 'Int64'}
    for col, target_type in option_cols_to_convert.items():
        if col in ddf.columns:
            logger.info(f"Convertendo coluna de opção: {col}")
            # Substituir S/N por 1/0 e converter para Int64
            def convert_option_partition(series):
                return series.replace({'S': 1, 'N': 0})
                
            # Usar meta correspondente ao tipo final
            meta = (col, 'int64') # Meta para .replace
            ddf[col] = ddf[col].map_partitions(convert_option_partition, meta=meta)
            ddf[col] = ddf[col].map_partitions(pd.to_numeric, errors='coerce', meta=(col, 'float64')).astype(target_type)
            logger.info(f"Coluna '{col}' convertida para {target_type}.")
        else:
             logger.warning(f"Coluna de opção '{col}' não encontrada para conversão.")

    return ddf


def process_simples(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados do Simples Nacional."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento do SIMPLES NACIONAL')
    logger.info('=' * 50)

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
        # Lista todos os arquivos ZIP do Simples Nacional
        zip_files = [f for f in os.listdir(path_zip) if f.startswith('Simples') and f.endswith('.zip')]
        if not zip_files:
            logger.warning(f'Nenhum arquivo ZIP do Simples encontrado em {path_zip} para processar.')
            return True  # Não é erro

        all_dfs = []
        for zip_file in zip_files:
            # Mover verificação de espaço para cá
            zip_path = os.path.join(path_zip, zip_file)
            logger.info(f'Processando arquivo ZIP: {zip_file}')
            estimated_size_mb = estimate_zip_extracted_size(zip_path)
            logger.info(f"Tamanho estimado após descompactação: {estimated_size_mb:.2f}MB")
            has_space_for_file, available_mb_now = check_disk_space(path_unzip, estimated_size_mb * 1.2)
            if not has_space_for_file:
                logger.error(
                    f"Espaço insuficiente para descompactar {zip_file}. Disponível: {available_mb_now:.2f}MB, necessário: {estimated_size_mb * 1.2:.2f}MB")
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
                parquet_size_estimate = sum([df.memory_usage(deep=True).sum().compute() for df in all_dfs]) * 0.5 / (
                            1024 * 1024)
                has_space, available_mb = check_disk_space(path_parquet, parquet_size_estimate)

                if not has_space:
                    logger.error(
                        f"Espaço insuficiente para criar arquivo parquet. Disponível: {available_mb:.2f}MB, estimado: {parquet_size_estimate:.2f}MB")
                    return False

                dd_simples_raw = dd.concat(all_dfs)

                # Aplicar transformações
                dd_simples = apply_simples_transformations(dd_simples_raw)

                # Converte para parquet
                table_name = 'simples'
                logger.info(f'Criando arquivo parquet {table_name}...')
                try:
                    # Obtém o nome da pasta do mês dos arquivos CSV descompactados
                    csv_files = [f for f in os.listdir(path_unzip) if 'CSV' in f]
                    if not csv_files:
                        raise ValueError("Nenhum arquivo CSV encontrado na pasta de descompactação")

                    # Extrai o nome da pasta do mês do primeiro arquivo CSV
                    create_parquet(dd_simples, table_name, path_parquet)
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
        logger.exception(f'Erro inesperado no processo principal do Simples: {e}')
        return False
