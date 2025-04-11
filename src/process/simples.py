import logging
import os
import zipfile
import dask.dataframe as dd
import pandas as pd
from dask import delayed
import numpy as np

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
        logger.info(f'Diretório {output_dir} limpo antes de criar novos arquivos parquet')
    except Exception as e:
        logger.warning(f'Não foi possível limpar diretório {output_dir}: {str(e)}')

    os.makedirs(output_dir, exist_ok=True)

    # Log das colunas antes de salvar
    logger.info(f"Colunas do DataFrame '{table_name}' antes de salvar em Parquet: {list(df.columns)}")

    # Configura o nome dos arquivos parquet com prefixo da tabela
    # Forçar reparticionamento para garantir múltiplos arquivos
    if df.npartitions < 2:
        df = df.repartition(npartitions=2)
        
    df.to_parquet(
        output_dir,
        engine='pyarrow',  # Especifica o engine
        write_index=False,
        compression='snappy',  # Compressão eficiente
        name_function=lambda i: create_parquet_filename(table_name, i)
    )


def create_parquet_chunks(df, table_name, path_parquet, chunk_size=100000):
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
    """Aplica transformações específicas para Simples Nacional usando Dask."""
    logger.info("Aplicando transformações em Simples...")
    
    # Renomeação de colunas
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico',
        'opcao_pelo_simples': 'opcao_simples',
        'data_opcao_pelo_simples': 'data_opcao_simples',
        'data_exclusao_do_simples': 'data_exclusao_simples',
        'opcao_pelo_mei': 'opcao_mei',
        'data_opcao_pelo_mei': 'data_opcao_mei',
        'data_exclusao_do_mei': 'data_exclusao_mei'
    }
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in ddf.columns}
    ddf = ddf.rename(columns=actual_rename_mapping)
    
    # Conversão de números usando método mais seguro
    # Evitar usar map_partitions para converter tipos numéricos
    if 'cnpj_basico' in ddf.columns:
        # Usar astype em vez de map_partitions e dd.to_numeric
        ddf['cnpj_basico'] = ddf['cnpj_basico'].astype('Int64')
    
    # Conversão de datas usando método mais seguro
    date_cols = ['data_opcao_simples', 'data_exclusao_simples', 
                 'data_opcao_mei', 'data_exclusao_mei']
    
    for col in date_cols:
        if col in ddf.columns:
            # Primeiro substituir valores inválidos
            ddf[col] = ddf[col].replace(['0', '00000000', 'nan', 'None', 'NaN'], '')
            # Depois converter para datetime
            try:
                ddf[col] = ddf[col].astype('datetime64[ns]')
            except:
                # Se falhar, manter como string
                logger.warning(f"Não foi possível converter coluna {col} para datetime")
    
    # Conversão de opções (S/N) usando método mais seguro
    option_cols = ['opcao_simples', 'opcao_mei']
    for col in option_cols:
        if col in ddf.columns:
            # Substituir valores e converter para numérico
            ddf[col] = ddf[col].replace({'S': '1', 'N': '0', 's': '1', 'n': '0'})
            try:
                ddf[col] = ddf[col].astype('Int64')
            except:
                # Se falhar, manter como string
                logger.warning(f"Não foi possível converter coluna {col} para Int64")
    
    return ddf

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
        
        if not csv_files:
            logger.warning(f"Nenhum arquivo CSV encontrado no ZIP {zip_file}")
            return False
        
        # Criar uma lista para armazenar os DataFrames
        dataframes = []
        
        # Processar cada arquivo CSV individualmente
        for csv_file in csv_files:
            try:
                csv_path = os.path.join(path_unzip, csv_file)
                df = process_csv_to_df(
                    csv_path,
                dtype=config.simples_dtypes,
                column_names=config.simples_columns
            )
                
                # Verificar se o DataFrame não está vazio
                if df is not None:
                    # Forçar a criação de pelo menos 2 partições para garantir múltiplos arquivos
                    if df.npartitions == 1:
                        df = df.repartition(npartitions=2)
                    dataframes.append(df)
                    logger.info(f"CSV {csv_file} processado com sucesso")
            except Exception as e:
                logger.error(f"Erro ao processar o CSV {csv_file}: {str(e)}")
        
        # Verificar se temos DataFrames para processar
        if not dataframes:
            logger.warning(f"Nenhum DataFrame válido gerado a partir do ZIP {zip_file}")
            return False
        
        # Concatenar os DataFrames se houver mais de um
        if len(dataframes) > 1:
            ddf = dd.concat(dataframes)
        else:
            ddf = dataframes[0]
        
        # Verificar se o DataFrame resultante tem dados
        # Usar .shape só funciona se chamarmos compute(), o que pode ser caro
        # Vamos verificar pelo menos se temos partições
        if ddf.npartitions == 0:
            logger.warning(f"DataFrame vazio após concatenação para o ZIP {zip_file}")
            return False
        
        # Aplicar transformações
        ddf = apply_simples_transformations(ddf)
        
        # Criar parquet com partições múltiplas
        create_parquet(ddf, 'simples', path_parquet)
        
        return True
    except Exception as e:
        logger.error(f'Erro processando {zip_file}: {str(e)}')
        return False

def process_simples(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados do Simples Nacional usando Dask."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento do SIMPLES NACIONAL')
    logger.info('=' * 50)
    
    try:
        # Usa o cliente Dask já configurado
        client = DaskManager.get_instance().client
        
        zip_files = [f for f in os.listdir(path_zip) 
                    if f.startswith('Simples') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP do Simples encontrado.')
            return True
        
        # Processamento paralelo dos ZIPs
        futures = []
        for zip_file in zip_files:
            future = client.submit(
                process_single_zip,
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet
            )
            futures.append(future)
        
        # Coleta resultados
        results = client.gather(futures)
        success = any(results)  # Se pelo menos um arquivo foi processado com sucesso
        
        if not success:
            logger.warning("Nenhum arquivo processado com sucesso.")
        
        return success
            
    except Exception as e:
        logger.error(f'Erro no processamento principal: {str(e)}')
        return False


def process_csv_file_pandas(csv_path):
    """
    Processa um único arquivo CSV de simples usando Pandas.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Pandas ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa colunas e dtypes da config
    original_column_names = config.simples_columns
    dtype_dict = config.simples_dtypes

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
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)}: {str(e)}')
        return None


def apply_pandas_transformations(df):
    """Aplica transformações específicas para Simples Nacional usando Pandas."""
    logger.info("Aplicando transformações em Simples com Pandas...")
    
    # Renomeação de colunas
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico',
        'opcao_pelo_simples': 'opcao_simples',
        'data_opcao_pelo_simples': 'data_opcao_simples',
        'data_exclusao_do_simples': 'data_exclusao_simples',
        'opcao_pelo_mei': 'opcao_mei',
        'data_opcao_pelo_mei': 'data_opcao_mei',
        'data_exclusao_do_mei': 'data_exclusao_mei'
    }
    
    # Filtrar para incluir apenas colunas que existem no DataFrame
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
    df = df.rename(columns=actual_rename_mapping)
    
    # Conversão de números
    if 'cnpj_basico' in df.columns:
        df['cnpj_basico'] = pd.to_numeric(df['cnpj_basico'], errors='coerce')
    
    # Conversão de datas
    date_cols = ['data_opcao_simples', 'data_exclusao_simples', 
                 'data_opcao_mei', 'data_exclusao_mei']
    
    for col in date_cols:
        if col in df.columns:
            # Substituir valores inválidos com NaN
            df[col] = df[col].replace(['0', '00000000', 'nan', 'None', 'NaN'], pd.NA)
            # Converter para datetime
            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
    
    # Conversão de opções (S/N)
    option_cols = ['opcao_simples', 'opcao_mei']
    for col in option_cols:
        if col in df.columns:
            df[col] = df[col].replace({'S': '1', 'N': '0', 's': '1', 'n': '0'})
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


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
                    logger.info(f"CSV {csv_file} processado com sucesso: {len(df)} linhas")
            except Exception as e:
                logger.error(f"Erro ao processar o CSV {csv_file}: {str(e)}")
        
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
        df = apply_pandas_transformations(df)
        
        # Criar múltiplos arquivos parquet com chunks
        return create_parquet_chunks(df, 'simples', path_parquet)
        
    except Exception as e:
        logger.error(f'Erro processando {zip_file}: {str(e)}')
        return False


def process_simples(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados do Simples Nacional."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento do SIMPLES NACIONAL')
    logger.info('=' * 50)
    
    try:
        zip_files = [f for f in os.listdir(path_zip) 
                    if f.startswith('Simples') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP do Simples encontrado.')
            return True
        
        # Como estamos processando apenas um arquivo Simples.zip, vamos processar
        # diretamente com Pandas em vez de usar o Dask, para simplicidade e eficiência
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
                logger.info(f"Arquivo {zip_file} processado com sucesso")
        
        if not success:
            logger.warning("Nenhum arquivo processado com sucesso.")
        
        return success
            
    except Exception as e:
        logger.error(f'Erro no processamento principal: {str(e)}')
        return False
