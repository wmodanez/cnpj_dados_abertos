"""
Utilitários para processamento paralelo de arquivos CSV usando Dask.
"""
import csv
import logging
import os
from typing import List, Callable, Optional, Any, Dict

import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

logger = logging.getLogger('cnpj')

def setup_dask_client(n_workers: int = 4, memory_limit: str = '4GB') -> Client:
    """
    Configura e retorna um client Dask para computação distribuída.
    """
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=2,
        memory_limit=memory_limit
    )
    return Client(cluster)

def optimize_dask_read(csv_path: str, 
                      dtype: Dict,
                      column_names: List[str],
                      chunksize: str = "100MB",
                      separator: str = ';',
                      encoding: str = 'latin1') -> dd.DataFrame:
    """
    Lê um arquivo CSV de forma otimizada usando Dask.
    
    Args:
        csv_path: Caminho do arquivo
        dtype: Tipos das colunas
        column_names: Nomes das colunas
        chunksize: Tamanho de cada chunk
        separator: Separador do CSV
        encoding: Codificação do arquivo
        
    Returns:
        DataFrame Dask
    """
    try:
        return dd.read_csv(
            csv_path,
            dtype=dtype,
            sep=separator,
            encoding=encoding,
            quoting=csv.QUOTE_MINIMAL,
            blocksize=chunksize,
            header=None,
            names=column_names,
            assume_missing=True,  # Otimização para valores ausentes
            storage_options={'anon': True}  # Permite leitura paralela
        )
    except Exception as e:
        logger.error(f"Erro ao ler {os.path.basename(csv_path)}: {str(e)}")
        raise

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
                     na_filter: bool = True,
                     partition_size: str = "100MB") -> dd.DataFrame:
    """
    Versão otimizada para processar CSV com Dask.
    """
    try:
        df = optimize_dask_read(
            csv_path,
            dtype=dtype,
            column_names=column_names,
            chunksize=partition_size,
            separator=separator,
            encoding=encoding
        )
        
        # Otimiza o número de partições baseado no tamanho dos dados
        if df.npartitions > 32:
            df = df.repartition(npartitions=32)
        
        return df
    except Exception as e:
        logger.error(f"Erro ao processar {os.path.basename(csv_path)}: {str(e)}")
        raise

def process_dataframe_batch(df: dd.DataFrame, 
                          batch_operation: Callable[[dd.DataFrame], dd.DataFrame],
                          compute: bool = True) -> Any:
    """
    Processa um DataFrame Dask em lotes.
    
    Args:
        df: DataFrame Dask
        batch_operation: Função a ser aplicada
        compute: Se deve computar o resultado imediatamente
        
    Returns:
        Resultado processado
    """
    try:
        result = batch_operation(df)
        if compute:
            return result.compute()
        return result
    except Exception as e:
        logger.error(f"Erro no processamento em lote: {str(e)}")
        raise

def process_csv_files_parallel(
        csv_files: List[str],
        base_path: str,
        process_function: Callable[[str], Any],
        max_workers: int = 4,
        file_filter: Optional[Callable[[str], bool]] = None,
        memory_limit: str = '4GB'
) -> List[Any]:
    """
    Versão otimizada para processamento paralelo usando Dask.
    """
    results = []
    full_paths = [os.path.join(base_path, f) for f in csv_files]

    if file_filter:
        full_paths = [p for p in full_paths if file_filter(p)]

    if not full_paths:
        logger.warning("Nenhum arquivo encontrado para processamento")
        return results

    logger.info(f"Iniciando processamento de {len(full_paths)} arquivos com Dask")
    
    try:
        # Configura client Dask
        with setup_dask_client(max_workers, memory_limit) as client:
            # Cria futures para cada arquivo
            futures = [
                client.submit(process_function, path) 
                for path in full_paths
            ]
            
            # Coleta resultados
            results = client.gather(futures)
            
    except Exception as e:
        logger.error(f"Erro no processamento paralelo: {str(e)}")
        
    finally:
        logger.info(f"Processamento concluído. Resultados: {len(results)}")
        
    return [r for r in results if r is not None]
