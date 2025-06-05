"""
Utilitários para processamento paralelo de arquivos CSV.
"""
import csv
import logging
import os
import concurrent.futures
from typing import List, Callable, Optional, Any, Dict

import polars as pl

logger = logging.getLogger('cnpj')

def verify_csv_integrity(csv_path: str) -> bool:
    """
    Verifica a integridade de um arquivo CSV tentando ler as primeiras linhas.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        bool: True se o arquivo é válido, False caso contrário
    """
    try:
        # Tenta ler as primeiras linhas para verificar integridade
        # Especifica encoding='latin1' e delimiter=';'
        with open(csv_path, 'r', encoding='latin1') as f:
            reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            for _ in range(5):
                next(reader, None)
        return True
    except UnicodeDecodeError as e:
        logger.error(f'Erro de codificação no arquivo {os.path.basename(csv_path)}: {str(e)}')
        return False
    except csv.Error as e:
        logger.error(f'Erro de formato CSV no arquivo {os.path.basename(csv_path)}: {str(e)}')
        return False
    except Exception as e:
        logger.error(f'Erro ao verificar integridade do arquivo {os.path.basename(csv_path)}: {str(e)}')
        return False

def process_csv_to_df(csv_path: str, 
                     dtype: dict, 
                     column_names: List[str],
                     separator: str = ';',
                     encoding: str = 'latin1',
                     na_filter: bool = True) -> pl.DataFrame:
    """
    Processa um arquivo CSV usando Polars.
    
    Args:
        csv_path: Caminho do arquivo
        dtype: Tipos das colunas (ignorado, mantido para compatibilidade)
        column_names: Nomes das colunas
        separator: Separador do CSV
        encoding: Codificação do arquivo
        na_filter: Flag para detectar valores NA (ignorado, mantido para compatibilidade)
        
    Returns:
        DataFrame Polars
    """
    try:
        # Lê o CSV com Polars
        df = pl.read_csv(
            csv_path,
            separator=separator,
            encoding=encoding,
            has_header=False,
            new_columns=column_names,
            infer_schema_length=0,  # Não inferir schema
            dtypes={col: pl.Utf8 for col in column_names}  # Inicialmente lê tudo como string
        )
        return df
    except Exception as e:
        logger.error(f"Erro ao processar {os.path.basename(csv_path)}: {str(e)}")
        raise

def process_dataframe_batch(df: pl.DataFrame, 
                          batch_operation: Callable[[pl.DataFrame], pl.DataFrame],
                          compute: bool = True) -> Any:
    """
    Processa um DataFrame Polars em lotes.
    
    Args:
        df: DataFrame Polars
        batch_operation: Função a ser aplicada
        compute: Flag mantida para compatibilidade (ignorada com Polars)
        
    Returns:
        Resultado processado
    """
    try:
        return batch_operation(df)
    except Exception as e:
        logger.error(f"Erro no processamento em lote: {str(e)}")
        raise

def process_csv_files_parallel(
        csv_files: List[str],
        base_path: str,
        process_function: Callable[[str], Any],
        max_workers: int = 4,
        file_filter: Optional[Callable[[str], bool]] = None,
        memory_limit: str = '4GB'  # Parâmetro mantido para compatibilidade
) -> List[Any]:
    """
    Processa arquivos CSV em paralelo usando ThreadPoolExecutor.
    
    Args:
        csv_files: Lista de nomes de arquivos CSV
        base_path: Diretório base onde os arquivos estão
        process_function: Função para processar cada arquivo
        max_workers: Número máximo de workers
        file_filter: Função opcional para filtrar arquivos
        memory_limit: Parâmetro mantido para compatibilidade (ignorado)
        
    Returns:
        Lista de resultados
    """
    results = []
    full_paths = [os.path.join(base_path, f) for f in csv_files]

    if file_filter:
        full_paths = [p for p in full_paths if file_filter(p)]

    if not full_paths:
        logger.warning("Nenhum arquivo encontrado para processamento")
        return results

    logger.info(f"Iniciando processamento de {len(full_paths)} arquivos em paralelo")
    
    try:
        # Usa ProcessPoolExecutor para processamento paralelo
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submete jobs
            future_to_path = {
                executor.submit(process_function, path): path 
                for path in full_paths
            }
            
            # Coleta resultados à medida que são completados
            for future in concurrent.futures.as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Erro processando {os.path.basename(path)}: {str(e)}")
    except Exception as e:
        logger.error(f"Erro no processamento paralelo: {str(e)}")
    
    finally:
        logger.info(f"Processamento concluído. Resultados: {len(results)}")
    
    return results
