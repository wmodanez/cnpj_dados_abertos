import logging
import os
import zipfile
import dask.dataframe as dd
from dask import delayed

from ..config import config
from ..utils import (
    file_delete, check_disk_space, estimate_zip_extracted_size,
    process_csv_files_parallel, process_csv_to_df, verify_csv_integrity,
    create_parquet_filename
)
from src.utils.dask_manager import DaskManager

logger = logging.getLogger(__name__)


def create_parquet(df, table_name, path_parquet, zip_filename_prefix: str):
    """Converte um DataFrame Dask para formato parquet, usando prefixo.
    
    Args:
        df: DataFrame Dask a ser convertido
        table_name: Nome da tabela (subpasta)
        path_parquet: Caminho base para os arquivos parquet (diretório pai)
        zip_filename_prefix: Prefixo derivado do nome do arquivo ZIP original
    """
    output_dir = os.path.join(path_parquet, table_name)

    # Limpeza de diretório MOVIDA para a função principal (antes do loop)
    # try:
    #     file_delete(output_dir)
    #     logger.debug(f'Diretório {output_dir} limpo antes de criar novos arquivos parquet')
    # except Exception as e:
    #     logger.warning(f'Não foi possível limpar diretório {output_dir}: {str(e)}')

    # Garante que o diretório exista
    os.makedirs(output_dir, exist_ok=True)

    # Log das colunas antes de salvar (DEBUG)
    logger.debug(f"Colunas do DataFrame Dask '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {list(df.columns)}")

    # Configura o nome dos arquivos parquet com prefixo do zip e da tabela
    df.to_parquet(
        output_dir,
        engine='pyarrow',
        write_index=False,
        # Usa prefixo e índice do chunk para nome único
        name_function=lambda i: f"{zip_filename_prefix}_{table_name}_{i:03d}.parquet"
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
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo CSV {os.path.basename(csv_path)}: {str(e)}')
        return None


def apply_socio_transformations(ddf):
    """Aplica transformações específicas para Sócios usando Dask."""
    logger.info("Aplicando transformações em Sócios...")
    
    # Renomeação de colunas
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico',
        'identificador_de_socio': 'identificador_socio',
        'nome_do_socio_razao_social': 'nome_socio',
        'cnpj_ou_cpf_do_socio': 'cnpj_cpf_socio',
        'qualificacao_do_socio': 'qualificacao_socio',
        'data_de_entrada_sociedade': 'data_entrada_sociedade',
        'pais': 'pais',
        'representante_legal': 'representante_legal',
        'nome_do_representante': 'nome_representante',
        'qualificacao_do_representante_legal': 'qualificacao_representante_legal',
        'faixa_etaria': 'faixa_etaria'
    }
    
    # Filtra colunas existentes
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in ddf.columns}
    ddf = ddf.rename(columns=actual_rename_mapping)
    
    # Conversão de tipos usando Dask
    int_cols = ['cnpj_basico', 'identificador_socio', 'qualificacao_socio',
                'qualificacao_representante_legal', 'faixa_etaria']
                
    for col in int_cols:
        if col in ddf.columns:
            ddf[col] = ddf[col].map_partitions(
                lambda s: dd.to_numeric(s, errors='coerce'),
                meta=(col, 'Int64')
            )
    
    # Conversão de data otimizada para Dask
    if 'data_entrada_sociedade' in ddf.columns:
        ddf['data_entrada_sociedade'] = ddf['data_entrada_sociedade'].map_partitions(
            lambda s: dd.to_datetime(s.astype(str), format='%Y%m%d', errors='coerce').dt.normalize(),
            meta=('data_entrada_sociedade', 'datetime64[ns]')
        )
    
    return ddf


@delayed
def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP de sócios de forma otimizada com Dask.
       (Adaptado de estabelecimento.py)
    """
    zip_filename_prefix = os.path.splitext(zip_file)[0] # Prefixo para nomes de arquivo
    path_extracao = ""
    try:
        # --- 1. Extração (em subpasta) ---
        logger.debug(f"[{os.getpid()}] Dask Fase 1: Extração para {zip_file}")
        # Usa o prefixo como nome da subpasta
        path_extracao = os.path.join(path_unzip, zip_filename_prefix)
        if os.path.exists(path_extracao): shutil.rmtree(path_extracao)
        os.makedirs(path_extracao, exist_ok=True)

        path_zip_file = os.path.join(path_zip, zip_file)
        with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
            zip_ref.extractall(path_extracao)
        logger.info(f"[{os.getpid()}] Dask Extração de {zip_file} concluída para {path_extracao}")

        # --- 2. Leitura e Processamento CSV com Dask (dentro da subpasta) ---
        # Encontra arquivos que parecem ser de dados (pode ajustar o critério se necessário)
        all_files_in_extraction = [
            os.path.join(path_extracao, f)
            for f in os.listdir(path_extracao)
            if os.path.isfile(os.path.join(path_extracao, f)) and ('.SOCIO' in f.upper() or '.csv' in f.lower())
        ]

        if not all_files_in_extraction:
            logger.warning(f"[{os.getpid()}] Dask Nenhum arquivo de dados encontrado em {path_extracao} para {zip_file}.")
            return True

        logger.debug(f"[{os.getpid()}] Dask Arquivos encontrados para processar: {all_files_in_extraction}")

        # Ler múltiplos CSVs com Dask
        # Usar a função dask direta é mais eficiente aqui
        ddf = dd.read_csv(
            os.path.join(path_extracao, '*'), # Usa wildcard para ler tudo na pasta
            sep=config.file.separator,
            encoding=config.file.encoding,
            header=None,
            names=config.socio_columns,
            dtype=config.socio_dtypes, # Dtypes Dask
            on_bad_lines='warn', # Avisar sobre linhas ruins
            assume_missing=True # Lida melhor com possíveis NAs implícitos
        )

        # Aplicar transformações Dask
        ddf = apply_socio_transformations(ddf)

        # --- 3. Salvar Parquet ---
        create_parquet(ddf, 'socios', path_parquet, zip_filename_prefix)
        # Não há subset para sócios

        del ddf
        return True

    except Exception as e:
        logger.error(f'[{os.getpid()}] Dask Erro GERAL processando {zip_file}: {str(e)}')
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        # --- 4. Limpeza da subpasta de extração ---
        if path_extracao and os.path.exists(path_extracao):
            try:
                shutil.rmtree(path_extracao)
                logger.info(f"[{os.getpid()}] Dask Diretório de extração {path_extracao} limpo.")
            except Exception as e_clean:
                 logger.warning(f"[{os.getpid()}] Dask Falha ao limpar diretório {path_extracao}: {e_clean}")

def process_socio(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios usando Dask."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento Dask de SÓCIOS') # Mensagem mais clara
    logger.info('=' * 50)

    try:
        # Usa o cliente Dask já configurado
        client = DaskManager.get_instance().client

        zip_files = [f for f in os.listdir(path_zip)
                    if f.startswith('Socio') and f.endswith('.zip')]

        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Sócios encontrado.')
            return True

        # LIMPEZA PRÉVIA do diretório de saída principal
        output_dir_main = os.path.join(path_parquet, 'socios')
        try:
            file_delete(output_dir_main)
            logger.info(f'Diretório {output_dir_main} limpo antes do processamento Dask.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretório de saída {output_dir_main} antes do processamento Dask: {str(e)}')

        # Processamento paralelo dos ZIPs
        futures = [
            client.submit(
                process_single_zip,
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet
            )
            for zip_file in zip_files
        ]
        
        # Coleta resultados
        results = client.gather(futures)
        return all(results)
            
    except Exception as e:
        logger.error(f'Erro no processamento principal de Sócios com Dask: {str(e)}')
        return False


# ----- Implementação para Polars -----

import polars as pl
import numpy as np
import shutil
from rich.progress import track
# (os, zipfile, logging já importados)

def process_csv_file_polars(csv_path: str):
    """Lê um arquivo CSV de sócio usando Polars.

    Args:
        csv_path: Caminho para o arquivo CSV.

    Returns:
        DataFrame Polars ou None em caso de erro.
    """
    if not verify_csv_integrity(csv_path):
        return None

    original_column_names = config.socio_columns

    try:
        df = pl.read_csv(
            csv_path,
            separator=config.file.separator,
            encoding=config.file.encoding,
            has_header=False,
            new_columns=original_column_names,
            infer_schema_length=0,  # Não inferir schema
            dtypes={col: pl.Utf8 for col in original_column_names}, # Ler tudo como string
            ignore_errors=True
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)} com Polars: {str(e)}')
        return None

def apply_socio_transformations_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações específicas para Sócios usando Polars."""
    logger.info("Aplicando transformações em Sócios com Polars...")

    # Renomeação de colunas
    rename_mapping = {
        'identificador_de_socio': 'identificador_socio',
        'nome_do_socio_razao_social': 'nome_socio',
        'cnpj_ou_cpf_do_socio': 'cnpj_cpf_socio',
        'qualificacao_do_socio': 'qualificacao_socio',
        'data_de_entrada_sociedade': 'data_entrada_sociedade',
        # 'pais': 'pais', # Já está correto
        'representante_legal': 'representante_legal',
        'nome_do_representante': 'nome_representante',
        'qualificacao_do_representante_legal': 'qualificacao_representante_legal',
        'faixa_etaria': 'faixa_etaria'
    }
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
    df = df.rename(actual_rename_mapping)

    # Conversão numérica
    int_cols = [
        'cnpj_basico', 'identificador_socio', 'qualificacao_socio',
        'qualificacao_representante_legal', 'faixa_etaria'
    ]
    for col in int_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))

    # Conversão de data
    date_col = 'data_entrada_sociedade'
    if date_col in df.columns:
        df = df.with_columns(
            pl.when(
                pl.col(date_col).is_in(['0', '00000000']) |
                pl.col(date_col).is_null() |
                (pl.col(date_col).str.len_chars() == 0)
            )
            .then(None)
            .otherwise(pl.col(date_col))
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .alias(date_col)
        )

    # Nenhuma coluna a ser removida explicitamente para Sócios, diferente de outros tipos

    logger.info("Transformações Polars aplicadas em Sócios.")
    return df

def create_parquet_polars(df: pl.DataFrame, table_name: str, path_parquet: str, zip_filename_prefix: str):
    """Salva um DataFrame Polars como múltiplos arquivos parquet com prefixo.
       (Adaptado de estabelecimento.py)
    """
    try:
        output_dir = os.path.join(path_parquet, table_name)
        os.makedirs(output_dir, exist_ok=True)

        logger.debug(f"Colunas do DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {df.columns}")

        total_rows = df.height
        if total_rows == 0:
            logger.warning(f"DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) está vazio. Nenhum Parquet será salvo.")
            return True

        chunk_size = 500000  # Definir tamanho do chunk
        num_chunks = max(1, int(np.ceil(total_rows / chunk_size)))
        adjusted_chunk_size = int(np.ceil(total_rows / num_chunks))

        logger.info(f"Salvando '{table_name}' (Origem: {zip_filename_prefix}) com {total_rows} linhas em {num_chunks} chunks Parquet...")

        for i in range(num_chunks):
            start_idx = i * adjusted_chunk_size
            df_chunk = df.slice(start_idx, adjusted_chunk_size)

            file_name = f"{zip_filename_prefix}_{table_name}_{i:03d}.parquet"
            file_path = os.path.join(output_dir, file_name)

            df_chunk.write_parquet(file_path, compression="snappy")
            logger.debug(f"Chunk {i+1}/{num_chunks} salvo como {file_name} ({df_chunk.height} linhas)")

        logger.info(f"DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) salvo com sucesso em {num_chunks} arquivo(s) Parquet.")
        return True

    except Exception as e:
        logger.error(f"Erro ao salvar DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) como parquet: {str(e)}")
        return False

def process_single_zip_polars(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP de sócios usando Polars.
       (Adaptado de estabelecimento.py)
    """
    logger = logging.getLogger()
    pid = os.getpid()
    zip_filename_prefix = os.path.splitext(zip_file)[0]
    logger.info(f"[{pid}] Iniciando processamento Polars para: {zip_file}")
    path_extracao = ""
    success = False

    try:
        # --- 1. Extração (em subpasta) ---
        nome_arquivo_sem_ext = os.path.splitext(zip_file)[0]
        path_extracao = os.path.join(path_unzip, nome_arquivo_sem_ext)
        if os.path.exists(path_extracao):
            shutil.rmtree(path_extracao)
        os.makedirs(path_extracao, exist_ok=True)

        path_zip_file = os.path.join(path_zip, zip_file)
        with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
            zip_ref.extractall(path_extracao)
        logger.info(f"[{pid}] Extração de {zip_file} concluída para {path_extracao}")

        # --- 2. Leitura e Processamento CSV com Polars (dentro da subpasta) ---
        all_files_in_extraction = [
            os.path.join(path_extracao, f)
            for f in os.listdir(path_extracao)
            if os.path.isfile(os.path.join(path_extracao, f))
        ]

        if not all_files_in_extraction:
            logger.warning(f"[{pid}] Nenhum arquivo de dados encontrado em {path_extracao} para {zip_file}.")
            return True

        dataframes = []
        logger.debug(f"[{pid}] Arquivos encontrados em {path_extracao}: {all_files_in_extraction}")
        for data_path in all_files_in_extraction:
            file_name = os.path.basename(data_path)
            logger.debug(f"[{pid}] Tentando processar arquivo: {file_name}")
            df_polars = process_csv_file_polars(data_path)
            if df_polars is not None and not df_polars.is_empty():
                logger.debug(f"[{pid}] Arquivo {file_name} processado com sucesso ({df_polars.height} linhas).")
                dataframes.append(df_polars)
            elif df_polars is not None and df_polars.is_empty():
                 logger.warning(f"[{pid}] Arquivo {file_name} resultou em DataFrame vazio.")

        if not dataframes:
            logger.error(f"[{pid}] Nenhum DataFrame Polars válido gerado a partir do ZIP {zip_file}.")
            return False

        # Concatenar e aplicar transformações
        df_final = pl.concat(dataframes) if len(dataframes) > 1 else dataframes[0]
        df_final = apply_socio_transformations_polars(df_final)

        # --- 3. Salvar Parquet ---
        # Note que para sócios, não há subset UF
        success = create_parquet_polars(df_final, 'socios', path_parquet, zip_filename_prefix)

        del df_final

    except Exception as e:
        logger.exception(f"[{pid}] Erro GERAL no processamento Polars de {zip_file}")
        success = False
    finally:
        # --- 4. Limpeza ---
        if path_extracao and os.path.exists(path_extracao):
            try:
                shutil.rmtree(path_extracao)
                logger.info(f"[{pid}] Diretório de extração {path_extracao} limpo.")
            except Exception as e_clean:
                 logger.warning(f"[{pid}] Falha ao limpar diretório de extração {path_extracao}: {e_clean}")

    logger.info(f"[{pid}] Processamento Polars para {zip_file} concluído com status: {success}")
    return success

def process_socio_with_polars(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios usando Polars.
       (Adaptado de estabelecimento.py)
    """
    logger.info('=' * 50)
    logger.info('Iniciando processamento de SÓCIOS com Polars')
    logger.info('=' * 50)

    try:
        zip_files = [f for f in os.listdir(path_zip)
                    if f.startswith('Socio') and f.endswith('.zip')]

        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Sócios encontrado.')
            return True

        # LIMPEZA PRÉVIA do diretório de saída principal
        output_dir_main = os.path.join(path_parquet, 'socios')
        try:
            file_delete(output_dir_main)
            logger.info(f'Diretório {output_dir_main} limpo antes do processamento Polars.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretório de saída {output_dir_main} antes do processamento Polars: {str(e)}')

        overall_success = False
        for zip_file in track(zip_files, description="[cyan]Processing Sócios ZIPs (Polars)..."):
            result = process_single_zip_polars(
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet
            )
            if result:
                overall_success = True

        if not overall_success:
             logger.warning("Nenhum arquivo ZIP de Sócios foi processado com sucesso usando Polars.")

        return overall_success

    except Exception as e:
        logger.error(f'Erro no processamento principal de Sócios com Polars: {str(e)}')
        return False


# ----- Implementação para Pandas -----

import pandas as pd
# (numpy, os, zipfile, logging, shutil, track já importados)

def process_csv_file_pandas(csv_path: str):
    """Lê um arquivo CSV de sócio usando Pandas.

    Args:
        csv_path: Caminho para o arquivo CSV.

    Returns:
        DataFrame Pandas ou None em caso de erro.
    """
    if not verify_csv_integrity(csv_path):
        return None

    original_column_names = config.socio_columns
    dtype_dict = config.socio_dtypes_pandas # Usar dtypes específicos para Pandas

    try:
        df = pd.read_csv(
            csv_path,
            sep=config.file.separator,
            encoding=config.file.encoding,
            header=None,
            names=original_column_names,
            dtype=dtype_dict,
            na_filter=False, # Evitar que strings vazias virem NaN automaticamente
            low_memory=False # Para evitar avisos de DtypeWarning
        )
        # Substituir strings vazias explicitamente por None onde apropriado (após leitura)
        # for col in df.select_dtypes(include='object').columns:
        #     df[col] = df[col].replace('', None)
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)} com Pandas: {str(e)}')
        return None

def apply_socio_transformations_pandas(df):
    """Aplica transformações específicas para Sócios usando Pandas."""
    logger.info("Aplicando transformações em Sócios com Pandas...")

    # Renomeação de colunas
    rename_mapping = {
        'identificador_de_socio': 'identificador_socio',
        'nome_do_socio_razao_social': 'nome_socio',
        'cnpj_ou_cpf_do_socio': 'cnpj_cpf_socio',
        'qualificacao_do_socio': 'qualificacao_socio',
        'data_de_entrada_sociedade': 'data_entrada_sociedade',
        'representante_legal': 'representante_legal',
        'nome_do_representante': 'nome_representante',
        'qualificacao_do_representante_legal': 'qualificacao_representante_legal',
        'faixa_etaria': 'faixa_etaria'
    }
    df = df.rename(columns=rename_mapping)

    # Conversão numérica
    int_cols = [
        'cnpj_basico', 'identificador_socio', 'qualificacao_socio',
        'qualificacao_representante_legal', 'faixa_etaria'
    ]
    for col in int_cols:
        if col in df.columns:
            # Usar Int64 para permitir NAs
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Conversão de data
    date_col = 'data_entrada_sociedade'
    if date_col in df.columns:
        df[date_col] = df[date_col].replace(['0', '00000000', ''], np.nan)
        df[date_col] = pd.to_datetime(df[date_col], format='%Y%m%d', errors='coerce')

    logger.info("Transformações Pandas aplicadas em Sócios.")
    return df

def create_parquet_chunks_pandas(df, table_name, path_parquet, zip_filename_prefix: str, chunk_size=500000):
    """Salva um DataFrame Pandas como múltiplos arquivos parquet com prefixo e chunking.
       (Adaptado de estabelecimento.py)
    """
    try:
        output_dir = os.path.join(path_parquet, table_name)
        os.makedirs(output_dir, exist_ok=True)

        logger.debug(f"Colunas do DataFrame Pandas '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {list(df.columns)}")

        total_rows = len(df)
        if total_rows == 0:
            logger.warning(f"DataFrame Pandas '{table_name}' (Origem: {zip_filename_prefix}) está vazio. Nenhum Parquet será salvo.")
            return True

        num_chunks = max(1, int(np.ceil(total_rows / chunk_size)))
        logger.info(f"Salvando '{table_name}' (Origem: {zip_filename_prefix}) com {total_rows} linhas em {num_chunks} chunks Parquet...")

        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            df_chunk = df.iloc[start_idx:end_idx]

            file_name = f"{zip_filename_prefix}_{table_name}_{i:03d}.parquet"
            file_path = os.path.join(output_dir, file_name)

            df_chunk.to_parquet(file_path, index=False, compression="snappy")
            logger.debug(f"Chunk {i+1}/{num_chunks} salvo como {file_name} ({len(df_chunk)} linhas)")

        logger.info(f"DataFrame Pandas '{table_name}' (Origem: {zip_filename_prefix}) salvo com sucesso em {num_chunks} arquivo(s) Parquet.")
        return True

    except Exception as e:
        logger.error(f"Erro ao salvar DataFrame Pandas '{table_name}' (Origem: {zip_filename_prefix}) como parquet: {str(e)}")
        return False

def process_single_zip_pandas(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP de sócios usando Pandas.
       (Adaptado de estabelecimento.py)
    """
    logger = logging.getLogger()
    pid = os.getpid()
    zip_filename_prefix = os.path.splitext(zip_file)[0]
    logger.info(f"[{pid}] Iniciando processamento Pandas para: {zip_file}")
    path_extracao = ""
    success = False

    try:
        # --- 1. Extração (em subpasta) ---
        nome_arquivo_sem_ext = os.path.splitext(zip_file)[0]
        path_extracao = os.path.join(path_unzip, nome_arquivo_sem_ext)
        if os.path.exists(path_extracao):
            shutil.rmtree(path_extracao)
        os.makedirs(path_extracao, exist_ok=True)

        path_zip_file = os.path.join(path_zip, zip_file)
        with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
            zip_ref.extractall(path_extracao)
        logger.info(f"[{pid}] Extração de {zip_file} concluída para {path_extracao}")

        # --- 2. Leitura e Processamento CSV com Pandas (dentro da subpasta) ---
        all_files_in_extraction = [
            os.path.join(path_extracao, f)
            for f in os.listdir(path_extracao)
            if os.path.isfile(os.path.join(path_extracao, f))
        ]

        if not all_files_in_extraction:
            logger.warning(f"[{pid}] Nenhum arquivo de dados encontrado em {path_extracao} para {zip_file}.")
            return True

        dataframes = []
        logger.debug(f"[{pid}] Arquivos encontrados em {path_extracao}: {all_files_in_extraction}")
        for data_path in all_files_in_extraction:
            file_name = os.path.basename(data_path)
            logger.debug(f"[{pid}] Tentando processar arquivo: {file_name}")
            df_pandas = process_csv_file_pandas(data_path)
            if df_pandas is not None and not df_pandas.empty:
                logger.debug(f"[{pid}] Arquivo {file_name} processado com sucesso ({len(df_pandas)} linhas).")
                dataframes.append(df_pandas)
            elif df_pandas is not None and df_pandas.empty:
                 logger.warning(f"[{pid}] Arquivo {file_name} resultou em DataFrame vazio.")

        if not dataframes:
            logger.error(f"[{pid}] Nenhum DataFrame Pandas válido gerado a partir do ZIP {zip_file}.")
            return False

        # Concatenar e aplicar transformações
        df_final = pd.concat(dataframes, ignore_index=True) if len(dataframes) > 1 else dataframes[0]
        df_final = apply_socio_transformations_pandas(df_final)

        # --- 3. Salvar Parquet ---
        success = create_parquet_chunks_pandas(df_final, 'socios', path_parquet, zip_filename_prefix)

        del df_final

    except Exception as e:
        logger.exception(f"[{pid}] Erro GERAL no processamento Pandas de {zip_file}")
        success = False
    finally:
        # --- 4. Limpeza ---
        if path_extracao and os.path.exists(path_extracao):
            try:
                shutil.rmtree(path_extracao)
                logger.info(f"[{pid}] Diretório de extração {path_extracao} limpo.")
            except Exception as e_clean:
                 logger.warning(f"[{pid}] Falha ao limpar diretório de extração {path_extracao}: {e_clean}")

    logger.info(f"[{pid}] Processamento Pandas para {zip_file} concluído com status: {success}")
    return success

def process_socio_with_pandas(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios usando Pandas.
       (Adaptado de estabelecimento.py)
    """
    logger.info('=' * 50)
    logger.info('Iniciando processamento de SÓCIOS com Pandas')
    logger.info('=' * 50)

    try:
        zip_files = [f for f in os.listdir(path_zip)
                    if f.startswith('Socio') and f.endswith('.zip')]

        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Sócios encontrado.')
            return True

        # LIMPEZA PRÉVIA
        output_dir_main = os.path.join(path_parquet, 'socios')
        try:
            file_delete(output_dir_main)
            logger.info(f'Diretório {output_dir_main} limpo antes do processamento Pandas.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretório de saída {output_dir_main} antes do processamento Pandas: {str(e)}')

        overall_success = False
        for zip_file in track(zip_files, description="[cyan]Processing Sócios ZIPs (Pandas)..."):
            result = process_single_zip_pandas(
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet
            )
            if result:
                overall_success = True

        if not overall_success:
             logger.warning("Nenhum arquivo ZIP de Sócios foi processado com sucesso usando Pandas.")

        return overall_success

    except Exception as e:
        logger.error(f'Erro no processamento principal de Sócios com Pandas: {str(e)}')
        return False
