"""
Utilitários para processamento paralelo de arquivos CSV usando Dask e ThreadPoolExecutor.
"""
import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable, Optional, Any

import dask.dataframe as dd
import pandas as pd

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


def process_csv_to_df(csv_path: str, dtype: dict, column_names: List[str], separator: str = ';', encoding: str = 'latin1', na_filter: bool = True) -> dd.DataFrame:
    """
    Lê um arquivo CSV e retorna um DataFrame Dask.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        dtype: Dicionário de tipos de dados para as colunas
        column_names: Lista com os nomes das colunas
        separator: Separador usado no arquivo CSV (padrão: ';')
        encoding: Codificação do arquivo CSV (padrão: 'latin1')
        na_filter: Se False, mantém todos os valores como estão sem converter vazios para NaN (padrão: True)
        
    Returns:
        DataFrame Dask ou levanta uma exceção em caso de erro
    """
    try:
        # Lê o CSV especificando os nomes das colunas e sem tentar ler cabeçalho
        df = dd.read_csv(
            csv_path,
            dtype=dtype,
            sep=separator,
            encoding=encoding,
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\',
            on_bad_lines='warn',
            header=None,  # Não lê a primeira linha como cabeçalho
            names=column_names,  # Usa os nomes fornecidos
            low_memory=False,
            na_filter=na_filter  # Adicionado para permitir controlar a conversão de valores vazios
        )
        return df
    except pd.errors.EmptyDataError as e:
        logger.error(f'Arquivo CSV vazio {os.path.basename(csv_path)}: {str(e)}')
        raise
    except pd.errors.ParserError as e:
        logger.error(f'Erro de parse no arquivo CSV {os.path.basename(csv_path)}: {str(e)}')
        raise
    except MemoryError as e:
        logger.error(f'Memória insuficiente para processar arquivo CSV {os.path.basename(csv_path)}: {str(e)}')
        raise
    except Exception as e:
        logger.error(f'Erro inesperado ao processar arquivo CSV {os.path.basename(csv_path)}: {str(e)}')
        raise


def process_csv_files_parallel(
        csv_files: List[str],
        base_path: str,
        process_function: Callable[[str], Any],
        max_workers: int = 4,
        file_filter: Optional[Callable[[str], bool]] = None
) -> List[Any]:
    """
    Processa múltiplos arquivos CSV em paralelo utilizando ThreadPoolExecutor.
    
    Args:
        csv_files: Lista de nomes de arquivos CSV
        base_path: Diretório base onde os arquivos estão localizados
        process_function: Função que processa um único arquivo CSV
        max_workers: Número máximo de workers para processamento paralelo
        file_filter: Função opcional para filtrar arquivos
        
    Returns:
        Lista com os resultados do processamento
    """
    results = []
    full_paths = [os.path.join(base_path, f) for f in csv_files]

    # Aplica filtro se fornecido
    if file_filter:
        full_paths = [p for p in full_paths if file_filter(p)]

    if not full_paths:
        logger.warning("Nenhum arquivo encontrado para processamento após aplicar filtro")
        return results

    logger.info(f"Iniciando processamento paralelo de {len(full_paths)} arquivos com {max_workers} workers")

    # Usa ThreadPoolExecutor para processar arquivos em paralelo
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submete todas as tarefas
        future_to_file = {executor.submit(process_function, path): path for path in full_paths}

        # Coleta resultados à medida que são concluídos
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            file_name = os.path.basename(file_path)
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
                    logger.debug(f"Arquivo {file_name} processado com sucesso")
                else:
                    logger.warning(f"Arquivo {file_name} retornou None")
            except Exception as e:
                logger.error(f"Exceção ao processar arquivo {file_name}: {str(e)}")

    logger.info(f"Processamento paralelo concluído. Resultados válidos: {len(results)}")
    return results
