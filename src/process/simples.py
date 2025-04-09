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
    
    # Conversão de números usando Dask
    if 'cnpj_basico' in ddf.columns:
        ddf['cnpj_basico'] = ddf['cnpj_basico'].map_partitions(
            lambda s: dd.to_numeric(s, errors='coerce'),
            meta=('cnpj_basico', 'Int64')
        )
    
    # Conversão de datas usando Dask
    date_cols = ['data_opcao_simples', 'data_exclusao_simples', 
                 'data_opcao_mei', 'data_exclusao_mei']
    
    for col in date_cols:
        if col in ddf.columns:
            ddf[col] = ddf[col].map_partitions(
                lambda s: dd.to_datetime(
                    s.astype(str).replace(['0', '00000000', 'nan', 'None', 'NaN'], ''),
                    format='%Y%m%d',
                    errors='coerce'
                ),
                meta=(col, 'datetime64[ns]')
            )
    
    # Conversão de opções (S/N) usando Dask
    option_cols = ['opcao_simples', 'opcao_mei']
    for col in option_cols:
        if col in ddf.columns:
            ddf[col] = ddf[col].map_partitions(
                lambda s: dd.to_numeric(
                    s.astype(str)
                     .replace({'S': '1', 'N': '0', 's': '1', 'n': '0'}, regex=False),
                    errors='coerce'
                ),
                meta=(col, 'Int64')
            )
    
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
        ddf = dd.concat([
            process_csv_to_df(
                os.path.join(path_unzip, csv_file),
                dtype=config.simples_dtypes,
                column_names=config.simples_columns
            )
            for csv_file in csv_files
        ])
        
        # Aplicar transformações
        ddf = apply_simples_transformations(ddf)
        
        # Criar parquet
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
