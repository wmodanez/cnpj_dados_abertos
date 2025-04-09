import logging
import logging
import os
import zipfile

import dask.dataframe as dd
import pandas as pd

from ..config import config
from ..utils import file_delete, check_disk_space, estimate_zip_extracted_size, create_parquet_filename
from ..utils import process_csv_files_parallel, process_csv_to_df, verify_csv_integrity

logger = logging.getLogger(__name__)


def create_parquet(df, table_name, path_parquet):
    """Converte um DataFrame para formato parquet.
    
    Args:
        df: DataFrame Dask a ser convertido
        table_name: Nome da tabela
        path_parquet: Caminho base para os
         arquivos parquet
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
    Processa um único arquivo CSV de sócio e retorna um DataFrame Dask.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Dask ou None em caso de erro
    """
    # Verifica integridade
    if not verify_csv_integrity(csv_path):
        logger.warning(f"Arquivo CSV {os.path.basename(csv_path)} inválido ou corrompido, pulando.")
        return None

    # Usa colunas e dtypes da config
    original_column_names = config.socio_columns
    dtype_dict = config.socio_dtypes

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
    except pd.errors.EmptyDataError:
        logger.warning(f'Arquivo CSV vazio {os.path.basename(csv_path)}')
        return None
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo CSV {os.path.basename(csv_path)}: {str(e)}')
        return None


def apply_socio_transformations(ddf):
    """Aplica as transformações específicas para Sócios."""
    logger.info("Aplicando transformações em Sócios...")

    # 1. Renomear colunas (Ajustar conforme nomes na config)
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico', # Mantém
        'identificador_de_socio': 'identificador_socio',
        'nome_do_socio_razao_social': 'nome_socio',
        'cnpj_ou_cpf_do_socio': 'cnpj_cpf_socio',
        'qualificacao_do_socio': 'qualificacao_socio',
        'data_de_entrada_sociedade': 'data_entrada_sociedade',
        'pais': 'pais',
        'representante_legal': 'representante_legal',
        'nome_do_representante': 'nome_representante',
        'qualificacao_do_representante_legal': 'qualificacao_representante_legal', # Corrigido nome
        'faixa_etaria': 'faixa_etaria'
    }
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in ddf.columns}
    ddf = ddf.rename(columns=actual_rename_mapping)
    logger.info(f"Colunas após renomear: {list(ddf.columns)}")

    # 2. Converter tipos para Int64 (permitir nulos)
    cols_to_int = [
        'cnpj_basico', 'identificador_socio', 'qualificacao_socio',
        'qualificacao_representante_legal', 'faixa_etaria'
    ]
    for col in cols_to_int:
        if col in ddf.columns:
            logger.info(f"Convertendo coluna '{col}' para Int64")
            ddf[col] = ddf[col].map_partitions(pd.to_numeric, errors='coerce', meta=(col, 'float64')).astype('Int64')
        else:
            logger.warning(f"Coluna '{col}' não encontrada para conversão para Int64.")
            
    # Converter data_entrada_sociedade para datetime
    if 'data_entrada_sociedade' in ddf.columns:
        logger.info("Convertendo coluna 'data_entrada_sociedade' para datetime")
        def convert_date_partition(series):
            try:
                # Converte para string primeiro caso seja outro tipo
                series = series.astype(str)
                # Substitui valores inválidos por vazio
                series = series.replace(['0', '00000000', 'nan', 'None', 'NaN'], '')
                # Converte para datetime, com erros como NaT
                return pd.to_datetime(series, errors='coerce', format='%Y%m%d')
            except Exception as e:
                logger.warning(f"Erro ao converter data_entrada_sociedade: {str(e)}")
                return pd.Series(pd.NaT, index=series.index)
            
        meta = ('data_entrada_sociedade', 'datetime64[ns]')
        ddf['data_entrada_sociedade'] = ddf['data_entrada_sociedade'].map_partitions(convert_date_partition, meta=meta)
        logger.info("Coluna 'data_entrada_sociedade' convertida para datetime.")
    else:
        logger.warning("Coluna 'data_entrada_sociedade' não encontrada para conversão.")

    return ddf


def process_socio(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento de SÓCIOS')
    logger.info('=' * 50)

    logger.info("Verificando espaço em disco necessário para descompactação...")
    has_space, available_mb = check_disk_space(path_unzip, 4000)
    if not has_space:
        logger.error(
            f"Espaço em disco insuficiente para processar os dados. Disponível: {available_mb:.2f}MB, necessário: 4000MB")
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
                logger.error(
                    f"Espaço insuficiente para descompactar {zip_file}. Disponível: {available_mb_now:.2f}MB, necessário: {estimated_size_mb * 1.2:.2f}MB")
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

            # Processa todos os arquivos CSV em paralelo
            logger.info(f'Processando {len(csv_files)} arquivos CSV em paralelo...')
            dfs = process_csv_files_parallel(
                csv_files=csv_files,
                base_path=path_unzip,
                process_function=process_csv_file, # Usa a função refatorada
                max_workers=config.dask.n_workers
            )
            
            # Filtra DataFrames vazios ou inválidos
            dfs = [df for df in dfs if df is not None]
            
            if dfs:
                all_dfs.extend(dfs)
                logger.info(f'Processamento de {len(dfs)} arquivos CSV concluído com sucesso')
            else:
                logger.warning(f'Nenhum DataFrame válido foi gerado a partir dos arquivos CSV')
            
            success = True # Marca sucesso se chegou aqui sem erro fatal

        if all_dfs:
            logger.info('Concatenando todos os DataFrames...')
            try:
                parquet_size_estimate = sum([df.memory_usage(deep=True).sum().compute() for df in all_dfs]) * 0.5 / (
                            1024 * 1024)
                has_space, available_mb = check_disk_space(path_parquet, parquet_size_estimate)

                if not has_space:
                    logger.error(
                        f"Espaço insuficiente para criar arquivo parquet. Disponível: {available_mb:.2f}MB, estimado: {parquet_size_estimate:.2f}MB")
                    return False

                dd_socio_raw = dd.concat(all_dfs)

                # Aplicar transformações
                dd_socio = apply_socio_transformations(dd_socio_raw)

                # Renomear parquet
                table_name = 'socios' # Nome ajustado
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
