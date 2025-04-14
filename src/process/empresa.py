import logging
import os
import zipfile
import pandas as pd
import numpy as np
import dask.dataframe as dd
from dask import delayed
import polars as pl

from ..config import config
from ..utils import (
    file_delete, check_disk_space, estimate_zip_extracted_size,
    process_csv_files_parallel, process_csv_to_df, verify_csv_integrity, 
    create_parquet_filename
)
from src.utils.dask_manager import DaskManager

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
        logger.debug(f'Diretório {output_dir} limpo antes de criar novos arquivos parquet')
    except Exception as e:
        logger.warning(f'Não foi possível limpar diretório {output_dir}: {str(e)}')

    os.makedirs(output_dir, exist_ok=True)

    # Log das colunas antes de salvar
    logger.debug(f"Colunas do DataFrame '{table_name}' antes de salvar em Parquet: {list(df.columns)}")

    # Configura o nome dos arquivos parquet com prefixo da tabela
    df.to_parquet(
        output_dir,
        engine='pyarrow',  # Especifica o engine
        write_index=False,
        name_function=lambda i: create_parquet_filename(table_name, i)
    )


def process_csv_file(csv_path):
    """
    Processa um único arquivo CSV de empresa e retorna um DataFrame Dask.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Dask ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa as colunas e dtypes da configuração global
    original_column_names = config.empresa_columns
    dtype_dict = config.empresa_dtypes

    try:
        # Passa os nomes das colunas, separador e encoding da config
        df = process_csv_to_df(
            csv_path, 
            dtype=dtype_dict, 
            column_names=original_column_names,
            separator=config.file.separator, # Usa separador da config
            encoding=config.file.encoding,   # Usa encoding da config
            na_filter=False # Como em simples.py
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)}: {str(e)}')
        return None


def apply_empresa_transformations(ddf):
    """Aplica as transformações específicas para Empresas usando Dask."""
    logger.info("Aplicando transformações em Empresas...")
    
    # Renomear colunas
    rename_mapping = {
        'cnpj_basico': 'cnpj',
        'razao_social_nome_empresarial': 'razao_social',
        'natureza_juridica': 'natureza_juridica',
        'qualificacao_do_responsavel': 'qualificacao_responsavel',
        'capital_social_da_empresa': 'capital_social',
        'porte_da_empresa': 'porte_empresa',
        'ente_federativo_responsavel': 'ente_federativo_responsavel'
    }
    
    # Filtra mapeamento para colunas existentes
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in ddf.columns}
    ddf = ddf.rename(columns=actual_rename_mapping)
    
    # Conversão de tipos usando Dask
    int_cols = ['natureza_juridica', 'qualificacao_responsavel', 'porte_empresa']
    for col in int_cols:
        if col in ddf.columns:
            ddf[col] = ddf[col].map_partitions(
                lambda s: dd.to_numeric(s, errors='coerce'),
                meta=(col, 'Int64')
            )
    
    # Conversão do capital_social usando Dask
    if 'capital_social' in ddf.columns:
        def convert_monetary(s):
            return (s.astype(str)
                    .str.replace(',', '.', regex=False)
                    .map_partitions(lambda x: dd.to_numeric(x, errors='coerce')))
        
        ddf['capital_social'] = convert_monetary(ddf['capital_social'])
    
    return ddf


def process_empresa(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de empresas usando Dask."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento de EMPRESAS')
    logger.info('=' * 50)
    
    try:
        # Usa o cliente Dask já configurado
        client = DaskManager.get_instance().client
        
        # Lista arquivos ZIP
        zip_files = [f for f in os.listdir(path_zip) 
                    if f.startswith('Empresa') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Empresas encontrado.')
            return True
        
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
        logger.error(f'Erro no processamento principal: {str(e)}')
        return False


@delayed
def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP de forma otimizada."""
    try:
        zip_path = os.path.join(path_zip, zip_file)
        
        # Extração e processamento
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(path_unzip)
        
        # Processamento dos CSVs usando Dask
        csv_files = [f for f in os.listdir(path_unzip) if 'CSV' in f]
        ddf = dd.concat([
            process_csv_to_df(
                os.path.join(path_unzip, csv_file),
                dtype=config.empresa_dtypes,
                column_names=config.empresa_columns
            )
            for csv_file in csv_files
        ])
        
        # Aplicar transformações
        ddf = apply_empresa_transformations(ddf)
        
        # Criar parquets
        create_parquet(ddf, 'empresas', path_parquet)
        
        # Processar empresas privadas
        if 'natureza_juridica' in ddf.columns:
            ddf_privada = ddf[
                (ddf['natureza_juridica'] >= 2046) & 
                (ddf['natureza_juridica'] <= 2348)
            ]
            create_parquet(ddf_privada, 'empresa_privada', path_parquet)
        
        return True
    except Exception as e:
        logger.error(f'Erro processando {zip_file}: {str(e)}')
        return False


# ----- Implementação para Pandas -----

def process_csv_file_pandas(csv_path):
    """
    Processa um único arquivo CSV de empresa usando Pandas.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Pandas ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa colunas e dtypes da config
    original_column_names = config.empresa_columns
    dtype_dict = config.empresa_dtypes

    try:
        # Usa pandas.read_csv com os parâmetros apropriados
        df = pd.read_csv(
            csv_path,
            sep=config.file.separator,
            encoding=config.file.encoding,
            names=original_column_names,
            header=None,
            dtype=str,  # Inicialmente lê tudo como string para evitar inferências incorretas
            quoting=1,  # QUOTE_MINIMAL
            na_filter=False
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)} com Pandas: {str(e)}')
        return None


def apply_empresa_transformations_pandas(df):
    """Aplica transformações específicas para Empresas usando Pandas."""
    logger.info("Aplicando transformações em Empresas com Pandas...")
    
    # Renomeação de colunas
    rename_mapping = {
        'cnpj_basico': 'cnpj',
        'razao_social_nome_empresarial': 'razao_social',
        'natureza_juridica': 'natureza_juridica',
        'qualificacao_do_responsavel': 'qualificacao_responsavel',
        'capital_social_da_empresa': 'capital_social',
        'porte_da_empresa': 'porte_empresa',
        'ente_federativo_responsavel': 'ente_federativo_responsavel'
    }
    
    # Filtrar para incluir apenas colunas que existem no DataFrame
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
    df = df.rename(columns=actual_rename_mapping)
    
    # Conversão de tipos numéricos
    int_cols = ['natureza_juridica', 'qualificacao_responsavel', 'porte_empresa']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Conversão do capital_social
    if 'capital_social' in df.columns:
        df['capital_social'] = (df['capital_social']
                                .astype(str)
                                .str.replace(',', '.', regex=False)
                                .pipe(lambda x: pd.to_numeric(x, errors='coerce')))
    
    return df


def create_parquet_chunks_pandas(df, table_name, path_parquet, chunk_size=100000):
    """Converte um DataFrame Pandas para múltiplos arquivos parquet usando chunks.
    
    Args:
        df: DataFrame Pandas a ser convertido
        table_name: Nome da tabela
        path_parquet: Caminho base para os arquivos parquet
        chunk_size: Número de linhas por arquivo parquet
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
    
    # Calcular número total de chunks baseado no tamanho
    total_rows = len(df)
    # Garantir pelo menos 2 chunks para ter múltiplos arquivos
    num_chunks = max(2, int(np.ceil(total_rows / chunk_size)))
    
    # Ajustar o tamanho dos chunks para distribuir igualmente
    adjusted_chunk_size = int(np.ceil(total_rows / num_chunks))
    
    logger.info(f"Dividindo DataFrame com {total_rows} linhas em {num_chunks} chunks")
    
    # Criar os arquivos parquet em chunks
    for i in range(num_chunks):
        start_idx = i * adjusted_chunk_size
        end_idx = min((i + 1) * adjusted_chunk_size, total_rows)
        
        # Criar um chunk do DataFrame
        df_chunk = df.iloc[start_idx:end_idx]
        
        # Criar nome do arquivo
        file_name = create_parquet_filename(table_name, i)
        file_path = os.path.join(output_dir, file_name)
        
        # Salvar o chunk como parquet
        df_chunk.to_parquet(
            file_path,
            engine='pyarrow',
            index=False,
            compression='snappy'  # Compressão eficiente para leitura/escrita
        )
        
        logger.info(f"Chunk {i+1}/{num_chunks} salvo como {file_name} ({end_idx-start_idx} linhas)")
    
    return True


def process_single_zip_pandas(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP usando Pandas para eficiência."""
    try:
        zip_path = os.path.join(path_zip, zip_file)
        
        # Extração e processamento
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(path_unzip)
        
        # Processamento dos CSVs usando Pandas
        csv_files = [f for f in os.listdir(path_unzip) if 'CSV' in f]
        
        if not csv_files:
            logger.warning(f"Nenhum arquivo CSV encontrado no ZIP {zip_file}")
            return False
        
        # Lista para armazenar os DataFrames
        dataframes = []
        
        # Processar cada arquivo CSV individualmente
        for csv_file in csv_files:
            try:
                csv_path = os.path.join(path_unzip, csv_file)
                df = process_csv_file_pandas(csv_path)
                
                if df is not None and not df.empty:
                    dataframes.append(df)
                    logger.info(f"CSV {csv_file} processado com sucesso com Pandas: {len(df)} linhas")
            except Exception as e:
                logger.error(f"Erro ao processar o CSV {csv_file} com Pandas: {str(e)}")
        
        # Verificar se temos DataFrames para processar
        if not dataframes:
            logger.warning(f"Nenhum DataFrame válido gerado a partir do ZIP {zip_file}")
            return False
        
        # Concatenar os DataFrames se houver mais de um
        if len(dataframes) > 1:
            df = pd.concat(dataframes, ignore_index=True)
        else:
            df = dataframes[0]
        
        # Verificar se o DataFrame resultante tem dados
        if df.empty:
            logger.warning(f"DataFrame vazio após concatenação para o ZIP {zip_file}")
            return False
        
        # Aplicar transformações
        df = apply_empresa_transformations_pandas(df)
        
        # Criar parquets em chunks
        success = create_parquet_chunks_pandas(df, 'empresas', path_parquet)
        
        # Processar empresas privadas
        if 'natureza_juridica' in df.columns:
            df_privada = df[
                (df['natureza_juridica'] >= 2046) & 
                (df['natureza_juridica'] <= 2348)
            ]
            success = success and create_parquet_chunks_pandas(df_privada, 'empresa_privada', path_parquet)
        
        return success
        
    except Exception as e:
        logger.error(f'Erro processando {zip_file} com Pandas: {str(e)}')
        return False


# ----- Implementação para Polars -----

def process_csv_file_polars(csv_path):
    """
    Processa um único arquivo CSV de empresa usando Polars.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Polars ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa colunas da config
    original_column_names = config.empresa_columns

    try:
        # Usa polars.read_csv com os parâmetros apropriados
        df = pl.read_csv(
            csv_path,
            separator=config.file.separator,
            encoding=config.file.encoding,
            has_header=False,
            new_columns=original_column_names,
            infer_schema_length=0,  # Não inferir schema
            dtypes={col: pl.Utf8 for col in original_column_names}  # Inicialmente lê tudo como string
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)} com Polars: {str(e)}')
        return None


def apply_empresa_transformations_polars(df):
    """Aplica transformações específicas para Empresas usando Polars."""
    logger.info("Aplicando transformações em Empresas com Polars...")
    
    # Renomeação de colunas
    rename_mapping = {
        'cnpj_basico': 'cnpj',
        'razao_social_nome_empresarial': 'razao_social',
        'natureza_juridica': 'natureza_juridica',
        'qualificacao_do_responsavel': 'qualificacao_responsavel',
        'capital_social_da_empresa': 'capital_social',
        'porte_da_empresa': 'porte_empresa',
        'ente_federativo_responsavel': 'ente_federativo_responsavel'
    }
    
    # Filtrar para incluir apenas colunas que existem no DataFrame
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
    df = df.rename(actual_rename_mapping)
    
    # Conversão de tipos numéricos
    int_cols = ['natureza_juridica', 'qualificacao_responsavel', 'porte_empresa']
    for col in int_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))
    
    # Conversão do capital_social
    if 'capital_social' in df.columns:
        df = df.with_columns(
            pl.col('capital_social')
              .str.replace(',', '.', literal=True)
              .cast(pl.Float64, strict=False)
              .alias('capital_social')
        )
    
    return df


def create_parquet_polars(df, table_name, path_parquet):
    """Salva um DataFrame Polars como parquet.
    
    Args:
        df: DataFrame Polars
        table_name: Nome da tabela
        path_parquet: Caminho base para os arquivos parquet
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        output_dir = os.path.join(path_parquet, table_name)
        
        # Limpa o diretório antes de criar os novos arquivos
        try:
            file_delete(output_dir)
            logger.info(f'Diretório {output_dir} limpo antes de criar novos arquivos parquet')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretório {output_dir}: {str(e)}')
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Log das colunas antes de salvar
        logger.info(f"Colunas do DataFrame Polars '{table_name}' antes de salvar em Parquet: {df.columns}")
        
        # Calcular número total de chunks baseado no tamanho
        total_rows = df.height
        chunk_size = 100000  # Mesmo tamanho usado nas outras funções
        # Garantir pelo menos 2 chunks para ter múltiplos arquivos
        num_chunks = max(2, int(np.ceil(total_rows / chunk_size)))
        
        # Ajustar o tamanho dos chunks para distribuir igualmente
        adjusted_chunk_size = int(np.ceil(total_rows / num_chunks))
        
        logger.info(f"Dividindo DataFrame com {total_rows} linhas em {num_chunks} chunks")
        
        # Criar os arquivos parquet em chunks
        for i in range(num_chunks):
            start_idx = i * adjusted_chunk_size
            end_idx = min((i + 1) * adjusted_chunk_size, total_rows)
            
            # Criar um chunk do DataFrame
            df_chunk = df.slice(start_idx, end_idx - start_idx)
            
            # Criar nome do arquivo
            file_name = create_parquet_filename(table_name, i)
            file_path = os.path.join(output_dir, file_name)
            
            # Salvar o chunk como parquet
            df_chunk.write_parquet(
                file_path,
                compression="snappy"
            )
            
            logger.info(f"Chunk {i+1}/{num_chunks} salvo como {file_name} ({end_idx-start_idx} linhas)")
        
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar DataFrame Polars como parquet: {str(e)}")
        return False


def process_single_zip_polars(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP usando Polars para eficiência."""
    try:
        zip_path = os.path.join(path_zip, zip_file)
        
        # Extração e processamento
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(path_unzip)
        
        # Processamento dos CSVs usando Polars
        csv_files = [f for f in os.listdir(path_unzip) if 'CSV' in f]
        
        if not csv_files:
            logger.warning(f"Nenhum arquivo CSV encontrado no ZIP {zip_file}")
            return False
        
        # Lista para armazenar os DataFrames
        dataframes = []
        
        # Processar cada arquivo CSV individualmente
        for csv_file in csv_files:
            try:
                csv_path = os.path.join(path_unzip, csv_file)
                df = process_csv_file_polars(csv_path)
                
                if df is not None and not df.is_empty():
                    dataframes.append(df)
                    logger.info(f"CSV {csv_file} processado com sucesso usando Polars: {df.height} linhas")
            except Exception as e:
                logger.error(f"Erro ao processar o CSV {csv_file} com Polars: {str(e)}")
        
        # Verificar se temos DataFrames para processar
        if not dataframes:
            logger.warning(f"Nenhum DataFrame Polars válido gerado a partir do ZIP {zip_file}")
            return False
        
        # Concatenar os DataFrames se houver mais de um
        if len(dataframes) > 1:
            df = pl.concat(dataframes, how="vertical")
        else:
            df = dataframes[0]
        
        # Verificar se o DataFrame resultante tem dados
        if df.is_empty():
            logger.warning(f"DataFrame Polars vazio após concatenação para o ZIP {zip_file}")
            return False
        
        # Aplicar transformações
        df = apply_empresa_transformations_polars(df)
        
        # Criar arquivo parquet
        success = create_parquet_polars(df, 'empresas', path_parquet)
        
        # Processar empresas privadas
        if 'natureza_juridica' in df.columns:
            df_privada = df.filter(
                (pl.col('natureza_juridica') >= 2046) & 
                (pl.col('natureza_juridica') <= 2348)
            )
            success = success and create_parquet_polars(df_privada, 'empresa_privada', path_parquet)
        
        return success
        
    except Exception as e:
        logger.error(f'Erro processando {zip_file} com Polars: {str(e)}')
        return False


def process_empresa_with_pandas(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de empresas usando Pandas."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento de EMPRESAS com Pandas')
    logger.info('=' * 50)
    
    try:
        zip_files = [f for f in os.listdir(path_zip) 
                    if f.startswith('Empresa') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Empresas encontrado.')
            return True
        
        # Processar diretamente com Pandas
        success = False
        for zip_file in zip_files:
            result = process_single_zip_pandas(
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet
            )
            if result:
                success = True
                logger.info(f"Arquivo {zip_file} processado com sucesso usando Pandas")
        
        if not success:
            logger.warning("Nenhum arquivo processado com sucesso usando Pandas.")
        
        return success
            
    except Exception as e:
        logger.error(f'Erro no processamento com Pandas: {str(e)}')
        return False


def process_empresa_with_polars(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de empresas usando Polars."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento de EMPRESAS com Polars')
    logger.info('=' * 50)
    
    try:
        zip_files = [f for f in os.listdir(path_zip) 
                    if f.startswith('Empresa') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Empresas encontrado.')
            return True
        
        # Processar com Polars
        success = False
        for zip_file in zip_files:
            result = process_single_zip_polars(
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet
            )
            if result:
                success = True
                logger.info(f"Arquivo {zip_file} processado com sucesso usando Polars")
        
        if not success:
            logger.warning("Nenhum arquivo processado com sucesso usando Polars.")
        
        return success
            
    except Exception as e:
        logger.error(f'Erro no processamento com Polars: {str(e)}')
        return False
