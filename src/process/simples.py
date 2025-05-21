import logging
import os
import zipfile
import pandas as pd
import numpy as np
import shutil
import pyarrow as pa
from pyarrow import parquet as pq
import dask.dataframe as dd
from dask import delayed
import polars as pl  # Importar Polars
from rich.progress import track # Adicionar import
import gc  # Adicionar import para gc

from ..config import config
from ..utils import (
    file_delete, check_disk_space, estimate_zip_extracted_size,
    verify_csv_integrity, create_parquet_filename
)

logger = logging.getLogger(__name__)

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
            
    return True


def create_parquet_chunks_with_dates(df, table_name, path_parquet, chunk_size=100000):
    """Converte um DataFrame Pandas para múltiplos arquivos parquet usando chunks,
    preservando campos de data como date32 do PyArrow.
    
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
    
    logger.info(f"Dividindo DataFrame com {total_rows} linhas em {num_chunks} chunks usando PyArrow com tipo date32")
    
    # Colunas de data que queremos converter para date32
    date_cols = ['data_opcao_simples', 'data_exclusao_simples', 
                'data_opcao_mei', 'data_exclusao_mei']
    
    # Criar os arquivos parquet em chunks
    for i in range(num_chunks):
        start_idx = i * adjusted_chunk_size
        end_idx = min((i + 1) * adjusted_chunk_size, total_rows)
        
        # Criar um chunk do DataFrame
        df_chunk = df.iloc[start_idx:end_idx]
        
        # Converter para PyArrow Table
        table = pa.Table.from_pandas(df_chunk)
        
        # Converter colunas de data para date32
        for col in date_cols:
            if col in table.column_names:
                # Verificar se é realmente uma coluna datetime
                if pa.types.is_timestamp(table[col].type):
                    # Converter para date32
                    date_array = table[col].cast(pa.date32())
                    table = table.set_column(table.column_names.index(col), col, date_array)
                    logger.debug(f"Coluna {col} convertida para date32")
        
        # Criar nome do arquivo
        file_name = create_parquet_filename(table_name, i)
        file_path = os.path.join(output_dir, file_name)
        
        # Salvar o chunk como parquet
        pq.write_table(
            table,
            file_path,
            compression='snappy'
        )
        
        logger.debug(f"Chunk {i+1}/{num_chunks} salvo como {file_name} ({end_idx-start_idx} linhas)")
    
    return True


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


def process_csv_file_dask(csv_path):
    """
    Processa um único arquivo CSV de simples usando Dask.
    
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
        # Usa dask.dataframe.read_csv com os parâmetros apropriados
        ddf = dd.read_csv(
            csv_path,
            sep=config.file.separator,
            encoding=config.file.encoding,
            names=original_column_names,
            header=None,
            dtype=str,  # Inicialmente lê tudo como string para evitar inferências incorretas
            quoting=1,  # QUOTE_MINIMAL
            blocksize="64MB",  # Tamanho do bloco para particionamento
            na_filter=False
        )
        return ddf
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)} com Dask: {str(e)}')
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
            # Converter para datetime para posterior conversão para date32 no PyArrow
            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
    
    # Conversão de opções (S/N)
    option_cols = ['opcao_simples', 'opcao_mei']
    for col in option_cols:
        if col in df.columns:
            df[col] = df[col].replace({'S': '1', 'N': '0', 's': '1', 'n': '0'})
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def apply_dask_transformations(ddf):
    """Aplica transformações específicas para Simples Nacional usando Dask."""
    logger.info("Aplicando transformações em Simples com Dask...")
    
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
    # Note: No Dask, verificamos as colunas de forma diferente
    columns_set = set(ddf.columns)
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in columns_set}
    ddf = ddf.rename(columns=actual_rename_mapping)
    
    # Conversão de números
    if 'cnpj_basico' in ddf.columns:
        ddf['cnpj_basico'] = dd.to_numeric(ddf['cnpj_basico'], errors='coerce')
    
    # Conversão de datas
    date_cols = ['data_opcao_simples', 'data_exclusao_simples', 
                 'data_opcao_mei', 'data_exclusao_mei']
    
    for col in date_cols:
        if col in ddf.columns:
            # Substituir valores inválidos com NaN
            ddf[col] = ddf[col].map(lambda x: pd.NA if x in ['0', '00000000', 'nan', 'None', 'NaN'] else x, meta=(col, 'object'))
            # Converter para datetime para posterior conversão para date32 no PyArrow
            ddf[col] = dd.to_datetime(ddf[col], format='%Y%m%d', errors='coerce')
    
    # Conversão de opções (S/N)
    option_cols = ['opcao_simples', 'opcao_mei']
    for col in option_cols:
        if col in ddf.columns:
            # Substituir S/N com 1/0
            ddf[col] = ddf[col].map(lambda x: '1' if x in ['S', 's'] else ('0' if x in ['N', 'n'] else x), meta=(col, 'object'))
            ddf[col] = dd.to_numeric(ddf[col], errors='coerce')
    
    return ddf


def create_parquet(ddf, table_name, path_parquet):
    """Salva um DataFrame Dask como parquet.
    
    Args:
        ddf: DataFrame Dask
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
        logger.info(f"Colunas do DataFrame Dask '{table_name}' antes de salvar em Parquet: {list(ddf.columns)}")
        
        # Salvar como parquet usando a função do Dask
        ddf.to_parquet(
            output_dir,
            engine='pyarrow',
            compression='snappy',
            write_index=False
        )
        
        logger.info(f"DataFrame Dask salvo como parquet em {output_dir}")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar DataFrame Dask como parquet: {str(e)}")
        return False


def create_parquet_with_dates(ddf, table_name, path_parquet):
    """Salva um DataFrame Dask como parquet, preservando campos de data como date32.
    
    Args:
        ddf: DataFrame Dask
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
        logger.info(f"Colunas do DataFrame Dask '{table_name}' antes de salvar em Parquet: {list(ddf.columns)}")
        
        # Colunas de data que queremos converter para date32
        date_cols = ['data_opcao_simples', 'data_exclusao_simples', 
                    'data_opcao_mei', 'data_exclusao_mei']
        
        # Configurar esquema para colunas de data usando date32
        schema = {}
        for col in date_cols:
            if col in ddf.columns:
                schema[col] = pa.date32()
        
        # Salvar como parquet com o esquema personalizado
        ddf.to_parquet(
            output_dir,
            engine='pyarrow',
            compression='snappy',
            write_index=False,
            schema=schema
        )
        
        logger.info(f"DataFrame Dask salvo como parquet em {output_dir} com colunas de data em formato date32")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar DataFrame Dask como parquet com date32: {str(e)}")
        return False


def process_single_zip_pandas(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP usando Pandas para eficiência."""
    zip_path = os.path.join(path_zip, zip_file)
    extract_dir = os.path.join(path_unzip, os.path.splitext(zip_file)[0]) # Subdir para extração
    
    try:
        # Limpa o diretório de extração específico, se existir
        if os.path.exists(extract_dir):
            logger.debug(f"Removendo diretório de extração existente: {extract_dir}")
            shutil.rmtree(extract_dir)
        os.makedirs(extract_dir) # Cria o diretório

        # Extração e processamento
        logger.info(f"Extraindo {zip_file} para {extract_dir}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir) # Extrai para o subdir
        
        # Processamento dos CSVs a partir do subdir
        csv_files = [f for f in os.listdir(extract_dir) if 'csv' in f.lower()] # Ler do subdir
        
        if not csv_files:
            logger.warning(f"Nenhum arquivo CSV encontrado em {extract_dir} para o ZIP {zip_file}")
            return True # Considera sucesso se não há CSVs
        
        dataframes = []
        processamento_ok = False
        for csv_file in csv_files:
            try:
                csv_path = os.path.join(extract_dir, csv_file) # Caminho completo no subdir
                df = process_csv_file_pandas(csv_path)
                
                if df is not None and not df.empty:
                    dataframes.append(df)
                    processamento_ok = True # Marca que pelo menos um CSV foi lido
                elif df is not None and df.empty:
                    logger.warning(f"DataFrame Pandas vazio para CSV: {csv_file}")
                # Se df is None, erro já logado
            except Exception as e:
                logger.error(f"Erro ao processar o CSV {csv_file} com Pandas: {str(e)}")
        
        if not processamento_ok or not dataframes:
            logger.warning(f"Nenhum DataFrame Pandas válido gerado a partir do ZIP {zip_file}")
            return False # Falha se havia CSVs mas não gerou DataFrames
        
        # Concatenar e transformar
        df_final = None
        try:
            if len(dataframes) > 1:
                df_final = pd.concat(dataframes, ignore_index=True)
            else:
                df_final = dataframes[0]
            
            if df_final.empty:
                logger.warning(f"DataFrame Pandas final vazio após concatenação para o ZIP {zip_file}")
                return True # Sucesso se resultou vazio

            df_transformed = apply_pandas_transformations(df_final)
            
            # Salvar Parquet
            # Usar zip_filename_prefix para nomear chunks
            zip_filename_prefix = os.path.splitext(zip_file)[0]
            return create_parquet_chunks_with_dates(df_transformed, 'simples', path_parquet, zip_filename_prefix)
        
        except Exception as e_concat_transform:
            logger.error(f"Erro durante concatenação ou transformação Pandas para {zip_file}: {e_concat_transform}")
            return False
        finally:
             if 'df_final' in locals() and df_final is not None: del df_final
             if 'df_transformed' in locals() and df_transformed is not None: del df_transformed
             import gc
             gc.collect()

    except Exception as e:
        logger.error(f'Erro processando {zip_file} com Pandas: {str(e)}')
        return False
    finally:
        # Garante a limpeza do diretório de extração
        if os.path.exists(extract_dir):
            logger.debug(f"Limpando diretório de extração final (Pandas): {extract_dir}")
            try:
                shutil.rmtree(extract_dir)
            except Exception as e_clean:
                 logger.warning(f"Não foi possível limpar diretório de extração {extract_dir}: {e_clean}")


@delayed
def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP usando Dask de forma distribuída."""
    zip_path = os.path.join(path_zip, zip_file)
    extract_dir = os.path.join(path_unzip, os.path.splitext(zip_file)[0]) # Subdir para extração
    
    try:
        # Limpa o diretório de extração específico, se existir
        if os.path.exists(extract_dir):
            logger.debug(f"Removendo diretório de extração existente: {extract_dir}")
            shutil.rmtree(extract_dir)
        os.makedirs(extract_dir) # Cria o diretório

        # Extração e processamento
        logger.info(f"Extraindo {zip_file} para {extract_dir}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir) # Extrai para o subdir
        
        # Processamento dos CSVs a partir do subdir
        csv_files = [f for f in os.listdir(extract_dir) if 'csv' in f.lower()] # Ler do subdir
        
        if not csv_files:
            logger.warning(f"Nenhum arquivo CSV encontrado em {extract_dir} para o ZIP {zip_file}")
            return True # Considera sucesso se não há CSVs
        
        dask_dataframes = []
        processamento_ok = False
        for csv_file in csv_files:
            try:
                csv_path = os.path.join(extract_dir, csv_file) # Caminho completo no subdir
                ddf = process_csv_file_dask(csv_path)
                
                if ddf is not None:
                    # Verifica se tem partições (indicativo de dados) antes de adicionar
                    # if ddf.npartitions > 0: # Checagem inicial, pode não ser 100% seguro
                    dask_dataframes.append(ddf)
                    processamento_ok = True
                    logger.info(f"CSV {csv_file} processado com sucesso usando Dask")
                    # else:
                    #     logger.warning(f"DataFrame Dask vazio (0 partições) para CSV: {csv_file}")
                # Se ddf is None, erro já logado
            except Exception as e:
                logger.error(f"Erro ao processar o CSV {csv_file} com Dask: {str(e)}")
        
        if not processamento_ok or not dask_dataframes:
            logger.warning(f"Nenhum DataFrame Dask válido gerado a partir do ZIP {zip_file}")
            return False # Falha se havia CSVs mas não gerou DataFrames
        
        # Concatenar e transformar
        ddf_final = None
        try:
            if len(dask_dataframes) > 1:
                ddf_final = dd.concat(dask_dataframes)
            else:
                ddf_final = dask_dataframes[0]
                
            # Verificar se ddf_final tem dados antes de prosseguir pode ser caro em Dask.
            # Assumimos que se chegou aqui, há potencial para dados.

            ddf_transformed = apply_dask_transformations(ddf_final)
            
            # Salvar Parquet
            # Usar zip_filename_prefix para nomear chunks
            zip_filename_prefix = os.path.splitext(zip_file)[0]
            return create_parquet_with_dates(ddf_transformed, 'simples', path_parquet, zip_filename_prefix)
        
        except Exception as e_concat_transform:
            logger.error(f"Erro durante concatenação ou transformação Dask para {zip_file}: {e_concat_transform}")
            return False
        finally:
             # Limpeza de memória Dask é gerenciada pelo scheduler
             pass 

    except Exception as e:
        logger.error(f'Erro processando {zip_file} com Dask: {str(e)}')
        return False
    finally:
        # Garante a limpeza do diretório de extração
        if os.path.exists(extract_dir):
            logger.debug(f"Limpando diretório de extração final (Dask): {extract_dir}")
            try:
                shutil.rmtree(extract_dir)
            except Exception as e_clean:
                 logger.warning(f"Não foi possível limpar diretório de extração {extract_dir}: {e_clean}")


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
        
        # Processar diretamente com Pandas, para simplicidade e eficiência
        success = False
        for zip_file in track(zip_files, description="[cyan]Processing Simples ZIPs (Pandas)..."):
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

def limpar_diretorios(self):
    """Limpa os diretórios temporários."""
    if not self.executar_limpeza:
        return
        
    for path in [self.path_unzip_pandas, self.path_parquet_pandas]:  # Somente diretórios Pandas
        if os.path.exists(path):
            for item in os.listdir(path):
                item_path = os.path.join(path, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)

def process_csv_file_polars(csv_path):
    """
    Processa um único arquivo CSV de simples usando Polars.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Polars ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa colunas da config
    original_column_names = config.simples_columns

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


def apply_polars_transformations(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações específicas para o Simples Nacional usando Polars."""
    logger.info("Aplicando transformações em Simples Nacional com Polars...")
    
    # Conversão numérica
    numeric_cols = ['cnpj_basico', 'opcao_pelo_simples', 'opcao_pelo_mei']
    numeric_conversions = []
    for col in numeric_cols:
        if col in df.columns:
            numeric_conversions.append(
                pl.col(col).cast(pl.Int64, strict=False)
            )
    
    if numeric_conversions:
        df = df.with_columns(numeric_conversions)
    
    # Conversão de datas
    date_cols = ['data_opcao_simples', 'data_exclusao_simples', 
                'data_opcao_mei', 'data_exclusao_mei']
    date_conversions = []
    
    for col in date_cols:
        if col in df.columns:
            date_conversions.append(
                pl.when(
                    pl.col(col).is_in(['0', '00000000', '']) | 
                    pl.col(col).is_null()
                )
                .then(None)
                .otherwise(pl.col(col))
                .str.strptime(pl.Date, format="%Y%m%d", strict=False)
                .alias(col)
            )
    
    if date_conversions:
        df = df.with_columns(date_conversions)
    
    # Liberar memória explicitamente
    gc.collect()
    
    logger.info("Transformações Polars aplicadas em Simples Nacional.")
    return df


def create_parquet_polars(df: pl.DataFrame, table_name: str, path_parquet: str, zip_filename_prefix: str, partition_size: int = 500000) -> bool:
    """
    Salva DataFrame Polars em arquivos Parquet particionados para reduzir uso de memória.
    
    Args:
        df: DataFrame Polars
        table_name: Nome da tabela (subpasta)
        path_parquet: Caminho base para os arquivos parquet (diretório pai de table_name)
        zip_filename_prefix: Prefixo derivado do nome do arquivo ZIP original
        partition_size: Tamanho de cada partição (número de linhas)
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        output_dir = os.path.join(path_parquet, table_name)
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Colunas do DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {df.columns}")
        
        total_rows = df.height
        if total_rows == 0:
            logger.warning(f"DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) está vazio. Nenhum Parquet será salvo.")
            return True
        
        num_partitions = (total_rows + partition_size - 1) // partition_size
        
        logger.info(f"Salvando DataFrame com {total_rows} linhas em {num_partitions} partições de aproximadamente {partition_size} linhas cada")
        
        for i in range(num_partitions):
            start_idx = i * partition_size
            end_idx = min((i + 1) * partition_size, total_rows)
            
            partition = df.slice(start_idx, end_idx - start_idx)
            output_path = os.path.join(output_dir, f"{zip_filename_prefix}_part{i:03d}.parquet")
            
            logger.info(f"Salvando partição {i+1}/{num_partitions} com {end_idx-start_idx} linhas para {output_path}")
            
            try:
                partition.write_parquet(output_path, compression="snappy")
                logger.info(f"Partição {i+1}/{num_partitions} salva com sucesso")
            except Exception as e:
                logger.error(f"Erro ao salvar partição {i+1}: {str(e)}")
                raise
            
            # Liberar memória
            del partition
            gc.collect()
            
        return True
    except Exception as e:
        logger.error(f"Erro ao criar arquivo Parquet com Polars: {str(e)}")
        return False


def process_single_zip_polars(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP usando Polars para eficiência."""
    logger = logging.getLogger()
    pid = os.getpid()
    logger.info(f"[{pid}] Iniciando processamento Polars para: {zip_file}")
    extract_dir = ""
    success = False
    zip_filename_prefix = os.path.splitext(zip_file)[0]
    
    try:
        zip_path = os.path.join(path_zip, zip_file)
        extract_dir = os.path.join(path_unzip, os.path.splitext(zip_file)[0])
        
        # Limpar diretório de extração se já existir
        if os.path.exists(extract_dir):
            logger.debug(f"[{pid}] Removendo diretório de extração existente: {extract_dir}")
            shutil.rmtree(extract_dir)
        
        # Criar diretório de extração
        os.makedirs(extract_dir, exist_ok=True)
        
        # Extrair arquivo ZIP
        logger.info(f"[{pid}] Extraindo {zip_file} para {extract_dir}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Limpar memória antes de começar processamento
        gc.collect()
        
        # Procurar CSVs na pasta de extração
        csv_files = []
        for root, _, files in os.walk(extract_dir):
            for file in files:
                # Busca arquivos que contenham "CSV" no nome (case-insensitive)
                if "CSV" in file.upper():
                    csv_files.append(os.path.join(root, file))
        
        if not csv_files:
            logger.warning(f"[{pid}] Nenhum arquivo contendo 'CSV' encontrado em {extract_dir}")
            # Retorna False pois não há dados para processar (não é um sucesso)
            return False
        
        logger.info(f"[{pid}] Encontrados {len(csv_files)} arquivos CSV para processar")
        
        # Processar cada CSV
        dataframes = []
        for csv_path in csv_files:
            csv_file = os.path.basename(csv_path)
            logger.debug(f"[{pid}] Processando CSV: {csv_file}")
            
            try:
                df = process_csv_file_polars(csv_path)
                if df is not None and not df.is_empty():
                    logger.info(f"[{pid}] CSV {csv_file} processado com sucesso: {df.height} linhas")
                    dataframes.append(df)
                elif df is not None and df.is_empty():
                    logger.warning(f"[{pid}] DataFrame Polars vazio para CSV: {csv_file}")
            except Exception as e:
                logger.error(f"[{pid}] Erro ao processar o CSV {csv_file} com Polars: {str(e)}")
        
        if not dataframes:
            logger.warning(f"[{pid}] Nenhum DataFrame Polars válido gerado a partir do ZIP {zip_file}")
            return False
        
        # Concatenar DataFrames
        logger.info(f"[{pid}] Concatenando {len(dataframes)} DataFrames para {zip_file}...")
        try:
            if len(dataframes) > 1:
                df_final = pl.concat(dataframes)
            else:
                df_final = dataframes[0]
            
            # Liberar memória dos DataFrames individuais
            del dataframes
            gc.collect()
            
            if df_final.is_empty():
                logger.warning(f"[{pid}] DataFrame Polars final vazio após concatenação para o ZIP {zip_file}")
                del df_final
                gc.collect()
                return True
            
            logger.info(f"[{pid}] Aplicando transformações em {df_final.height} linhas para {zip_file}...")
            df_transformed = apply_polars_transformations(df_final)
            
            # Salvar como parquet
            logger.info(f"[{pid}] Salvando Parquet para {zip_file}...")
            success = create_parquet_polars(df_transformed, 'simples', path_parquet, zip_filename_prefix)
            
            # Liberar memória
            del df_transformed
            del df_final
            gc.collect()
            
            return success
            
        except Exception as e_concat_transform:
            logger.error(f"[{pid}] Erro durante concatenação ou transformação Polars para {zip_file}: {e_concat_transform}")
            return False
            
    except Exception as e:
        logger.error(f"[{pid}] Erro processando {zip_file} com Polars: {str(e)}")
        return False
    finally:
        # Limpar diretório de extração
        if extract_dir and os.path.exists(extract_dir):
            try:
                logger.debug(f"[{pid}] Limpando diretório de extração final (Polars): {extract_dir}")
                shutil.rmtree(extract_dir)
            except Exception as e_clean:
                logger.warning(f"[{pid}] Erro ao limpar diretório de extração: {e_clean}")
        
        # Forçar coleta de lixo novamente
        gc.collect()
    
    return success

# Função adicionada para processamento com Polars
def process_simples_with_polars(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados do Simples Nacional usando Polars."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento do SIMPLES NACIONAL com Polars')
    logger.info('=' * 50)
    
    try:
        zip_files = [f for f in os.listdir(path_zip) 
                    if f.startswith('Simples') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP do Simples encontrado.')
            return True # Considera sucesso se não há arquivos para processar
        
        # LIMPEZA PRÉVIA do diretório de saída principal
        output_dir_main = os.path.join(path_parquet, 'simples')
        try:
            file_delete(output_dir_main)
            logger.info(f'Diretório {output_dir_main} limpo antes do processamento Polars.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretório de saída {output_dir_main} antes do processamento Polars: {str(e)}')
            
        # Forçar coleta de lixo antes de iniciar o processamento
        gc.collect()
        
        # Processar com Polars
        success = False
        arquivos_sem_csv = []
        # Adiciona barra de progresso para o loop de arquivos ZIP
        for zip_file in track(zip_files, description="[cyan]Processing Simples ZIPs (Polars)..."):
            # Chama a função de processamento de ZIP único com Polars
            result = process_single_zip_polars(
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet
                # A função process_single_zip_polars não precisa da flag create_private
            )
            if result:
                success = True
                logger.info(f"Arquivo {zip_file} processado com sucesso usando Polars")
            else:
                # Registra arquivos que não tiveram CSVs válidos para processar
                arquivos_sem_csv.append(zip_file)
                
            # Forçar coleta de lixo após cada arquivo
            gc.collect()
        
        if not success:
            logger.warning('Nenhum arquivo ZIP do Simples foi processado com sucesso com Polars.')
            
        if arquivos_sem_csv:
            logger.warning(f'Os seguintes arquivos não continham CSVs válidos para processar: {", ".join(arquivos_sem_csv)}')
        
        return success
            
    except Exception as e:
        logger.error(f'Erro no processamento principal do Simples Nacional com Polars: {str(e)}')
        traceback.print_exc()
        return False
