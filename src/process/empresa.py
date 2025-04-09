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
