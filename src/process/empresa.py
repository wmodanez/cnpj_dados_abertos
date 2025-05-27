import logging
import os
import zipfile
import polars as pl
import numpy as np
import gc
import shutil
import traceback
import logging.handlers
import sys
import time
from rich.progress import track
from multiprocessing import Pool, Lock, Value
import re
import datetime
import concurrent.futures
import tempfile
import threading
from queue import Queue, PriorityQueue
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from ..config import config
from ..utils import (
    file_delete, check_disk_space, estimate_zip_extracted_size,
    process_csv_files_parallel, verify_csv_integrity, 
    create_parquet_filename
)
from ..utils.folders import get_output_path, ensure_correct_folder_structure
from ..utils.time_utils import format_elapsed_time
from ..utils.statistics import global_stats
import inspect

logger = logging.getLogger(__name__)

# Flag global para garantir que o logger do worker seja configurado apenas uma vez por processo
_worker_logger_configured = False

# Variáveis globais para controle de recursos
_processing_lock = Lock()
_active_processes = Value('i', 0)
_max_concurrent_processes = Value('i', 2)  # Máximo de 2 processamentos simultâneos
_process_queue = PriorityQueue()
_workers_should_stop = Value('b', False)  # Flag para parar workers

# Configurações globais para otimização de memória
CHUNK_SIZE = 500_000  # Tamanho do chunk para processamento
MAX_MEMORY_GB = 8  # Limite de memória em GB

def configure_worker_logging(log_file, log_level=logging.INFO):
    """Configura o logging para o processo worker."""
    import logging
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Handler de arquivo
    fh = logging.FileHandler(log_file)
    fh.setLevel(log_level)
    
    # Formato
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    
    # Adicionar handler
    logger.addHandler(fh)
    
    return logger


def process_empresa(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Processa os dados de empresas."""
    return process_empresa_files(path_zip, path_unzip, path_parquet, create_private)


# ----- Implementação para Polars -----

def process_csv_file(csv_path):
    """
    Processa um único arquivo CSV de empresa.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame ou None em caso de erro
    """
    try:
        logger.debug(f"Processando arquivo CSV: {os.path.basename(csv_path)}")
        
        # Usa read_csv com os parâmetros apropriados
        df = pl.read_csv(
            csv_path,
            separator=';',
            has_header=False,
            encoding='latin1',
            ignore_errors=True,
            truncate_ragged_lines=True
        )
        
        logger.debug(f"Arquivo {os.path.basename(csv_path)} carregado com {df.height} linhas")
        return df
        
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)}: {str(e)}')
        return None


def process_data_file(data_path: str):
    """
    Processa um único arquivo de dados, seja ele CSV ou outro formato de texto.
    
    Args:
        data_path: Caminho para o arquivo de dados
        
    Returns:
        DataFrame ou None em caso de erro
    """
    logger = logging.getLogger(__name__)
    
    # Verificar se o arquivo é um arquivo de texto
    try:
        # Tentar ler as primeiras linhas para verificar se é um arquivo de texto
        is_text_file = True
        with open(data_path, 'rb') as f:
            sample = f.read(4096)  # Ler os primeiros 4KB
            # Verificar se há caracteres nulos ou muitos bytes não-ASCII
            # o que pode indicar que é um arquivo binário
            if b'\x00' in sample or len([b for b in sample if b > 127]) > len(sample) * 0.3:
                logger.warning(f"Arquivo {os.path.basename(data_path)} parece ser binário, não texto.")
                is_text_file = False
        
        if not is_text_file:
            return None
    except Exception as e:
        logger.error(f"Erro ao verificar se {os.path.basename(data_path)} é um arquivo de texto: {str(e)}")
        return None

    # Usar colunas da config
    original_column_names = config.empresa_columns

    # Primeiro, tentar com o separador padrão
    try:
        df = pl.read_csv(
            data_path,
            separator=config.file.separator,
            encoding=config.file.encoding,
            has_header=False,
            new_columns=original_column_names,
            infer_schema_length=0,  # Não inferir schema
            dtypes={col: pl.Utf8 for col in original_column_names},  # Inicialmente lê tudo como string
            ignore_errors=True  # Ignorar linhas com erros
        )
        if not df.is_empty():
            logger.info(f"Arquivo {os.path.basename(data_path)} processado com sucesso usando separador padrão")
            return df
    except Exception as e:
        logger.warning(f"Erro ao processar {os.path.basename(data_path)} com separador padrão: {str(e)}")
    
    # Se falhar com o separador padrão, tentar detectar o separador
    separators = [';', ',', '|', '\t']
    for sep in separators:
        if sep == config.file.separator:
            continue  # Já tentamos esse
        
        try:
            df = pl.read_csv(
                data_path,
                separator=sep,
                encoding=config.file.encoding,
                has_header=False,
                new_columns=original_column_names,
                infer_schema_length=0,
                dtypes={col: pl.Utf8 for col in original_column_names},
                ignore_errors=True
            )
            if not df.is_empty():
                logger.info(f"Arquivo {os.path.basename(data_path)} processado com sucesso usando separador '{sep}'")
                return df
        except Exception as e:
            logger.debug(f"Erro ao processar {os.path.basename(data_path)} com separador '{sep}': {str(e)}")
    
    # Se ainda falhar, tentar com diferentes codificações
    encodings = ['latin1', 'utf-8', 'utf-16', 'cp1252']
    for enc in encodings:
        if enc == config.file.encoding:
            continue  # Já tentamos esse
        
        try:
            df = pl.read_csv(
                data_path,
                separator=config.file.separator,
                encoding=enc,
                has_header=False,
                new_columns=original_column_names,
                infer_schema_length=0,
                dtypes={col: pl.Utf8 for col in original_column_names},
                ignore_errors=True
            )
            if not df.is_empty():
                logger.info(f"Arquivo {os.path.basename(data_path)} processado com sucesso usando codificação '{enc}'")
                return df
        except Exception as e:
            logger.debug(f"Erro ao processar {os.path.basename(data_path)} com codificação '{enc}': {str(e)}")
    
    # Se chegamos até aqui, não conseguimos processar o arquivo
    logger.error(f"Não foi possível processar o arquivo {os.path.basename(data_path)} com nenhuma combinação de separadores e codificações")
    return None


def apply_empresa_transformations(df: pl.DataFrame, chunk_size: int = 1_000_000) -> pl.DataFrame:
    """Aplica transformações específicas para Empresas."""
    
    logger.info("Aplicando transformações em Empresas...")
    
    # Definir nomes das colunas conforme layout da Receita Federal
    column_names = [
        "cnpj_basico",
        "razao_social", 
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel"
    ]
    
    # Renomear colunas
    df = df.select([
        pl.col(f"column_{i+1}").alias(name) 
        for i, name in enumerate(column_names)
    ])
    
    # --- Extração de CPF da razao_social ---
    logger.info("Extraindo CPF da razao_social...")
    
    # Padrão para CPF: 11 dígitos consecutivos
    cpf_pattern = r'(\d{11})'
    
    df = df.with_columns([
        pl.col("razao_social")
        .str.extract(cpf_pattern, 1)
        .alias("cpf_extraido")
    ])
    
    # Validar CPFs extraídos (verificar se não são sequências inválidas)
    invalid_cpfs = [
        "00000000000", "11111111111", "22222222222", "33333333333",
        "44444444444", "55555555555", "66666666666", "77777777777",
        "88888888888", "99999999999"
    ]
    
    df = df.with_columns([
        pl.when(pl.col("cpf_extraido").is_in(invalid_cpfs))
        .then(None)
        .otherwise(pl.col("cpf_extraido"))
        .alias("cpf_extraido")
    ])
    
    logger.info("Extração de CPF concluída.")
    
    # --- Remoção do CPF da razao_social ---
    logger.info("Removendo CPF da razao_social...")
    
    df = df.with_columns([
        pl.col("razao_social")
        .str.replace_all(cpf_pattern, "")
        .str.strip_chars()
        .alias("razao_social")
    ])
    
    logger.info("Remoção do CPF da razao_social concluída.")
    
    # Converter tipos de dados
    df = df.with_columns([
        pl.col("cnpj_basico").cast(pl.Utf8),
        pl.col("capital_social").str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("natureza_juridica").cast(pl.Int32, strict=False),
        pl.col("qualificacao_responsavel").cast(pl.Int32, strict=False),
        pl.col("porte_empresa").cast(pl.Int32, strict=False)
    ])
    
    return df

def compile_empresa_transformations(sample_df: pl.DataFrame) -> dict:
    """
    Compila as transformações de Empresas uma única vez baseado em um DataFrame de amostra.
    Retorna um dicionário com as transformações pré-compiladas.
    """
    transformations = {
        'column_mapping': [],
        'cpf_pattern': r'(\d{11})',
        'invalid_cpfs': [
            "00000000000", "11111111111", "22222222222", "33333333333",
            "44444444444", "55555555555", "66666666666", "77777777777",
            "88888888888", "99999999999"
        ],
        'type_conversions': [],
        'has_transformations': True  # Empresas sempre tem transformações
    }
    
    # Definir nomes das colunas conforme layout da Receita Federal
    column_names = [
        "cnpj_basico",
        "razao_social", 
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel"
    ]
    
    # Preparar mapeamento de colunas
    transformations['column_mapping'] = [
        (f"column_{i+1}", name) for i, name in enumerate(column_names)
        if f"column_{i+1}" in sample_df.columns
    ]
    
    # Preparar conversões de tipo (sempre as mesmas para empresas)
    transformations['type_conversions'] = [
        ("cnpj_basico", pl.Utf8),
        ("capital_social", "float_with_comma"),  # Tratamento especial
        ("natureza_juridica", pl.Int32),
        ("qualificacao_responsavel", pl.Int32),
        ("porte_empresa", pl.Int32)
    ]
    
    return transformations

def apply_empresa_transformations_optimized(df: pl.DataFrame, compiled_transformations: dict) -> pl.DataFrame:
    """
    Aplica transformações pré-compiladas de Empresas de forma otimizada.
    Não faz logs repetitivos nem recompilação de expressões.
    """
    if not compiled_transformations['has_transformations']:
        return df
    
    # Renomear colunas
    if compiled_transformations['column_mapping']:
        select_expressions = []
        for old_col, new_col in compiled_transformations['column_mapping']:
            if old_col in df.columns:
                select_expressions.append(pl.col(old_col).alias(new_col))
        
        if select_expressions:
            df = df.select(select_expressions)
    
    # Extração de CPF da razao_social
    if "razao_social" in df.columns:
        cpf_pattern = compiled_transformations['cpf_pattern']
        invalid_cpfs = compiled_transformations['invalid_cpfs']
        
        df = df.with_columns([
            pl.col("razao_social")
            .str.extract(cpf_pattern, 1)
            .alias("cpf_extraido")
        ])
        
        # Validar CPFs extraídos
        df = df.with_columns([
            pl.when(pl.col("cpf_extraido").is_in(invalid_cpfs))
            .then(None)
            .otherwise(pl.col("cpf_extraido"))
            .alias("cpf_extraido")
        ])
        
        # Remover CPF da razao_social
        df = df.with_columns([
            pl.col("razao_social")
            .str.replace_all(cpf_pattern, "")
            .str.strip_chars()
            .alias("razao_social")
        ])
    
    # Converter tipos de dados
    type_expressions = []
    for col_name, col_type in compiled_transformations['type_conversions']:
        if col_name in df.columns:
            if col_type == "float_with_comma":
                type_expressions.append(
                    pl.col(col_name).str.replace(",", ".").cast(pl.Float64, strict=False)
                )
            else:
                type_expressions.append(
                    pl.col(col_name).cast(col_type, strict=False)
                )
    
    if type_expressions:
        df = df.with_columns(type_expressions)
    
    return df

def create_parquet(df: pl.DataFrame, table_name: str, path_parquet: str, 
                         zip_filename_prefix: str, partition_size: int = 500_000) -> bool:
    """
    Salva DataFrame em arquivos Parquet particionados para reduzir uso de memória.
    
    Args:
        df: DataFrame
        table_name: Nome da tabela
        path_parquet: Caminho de saída
        zip_filename_prefix: Prefixo do arquivo ZIP (usado para nomear as partições)
        partition_size: Número de linhas por partição
        
    Returns:
        bool: True se salvou com sucesso
    """
    try:
        # Extrair pasta remota do caminho ou do prefixo do arquivo
        remote_folder = None
        
        # Verificar se podemos extrair uma data no formato YYYY-MM do caminho
        parts = path_parquet.split(os.path.sep)
        for part in parts:
            if len(part) == 7 and part[4] == '-':  # Formato AAAA-MM
                remote_folder = part
                break
        
        # Se não conseguimos extrair do caminho, tentar extrair do prefixo do arquivo ou path_parquet
        if not remote_folder:
            # Tentar extrair de path_parquet
            match = re.search(r'(20\d{2}-\d{2})', path_parquet)
            if match:
                remote_folder = match.group(1)
            else:
                # Tentar extrair do prefixo do arquivo
                match = re.search(r'(20\d{2}-\d{2})', zip_filename_prefix)
                if match:
                    remote_folder = match.group(1)
                else:
                    # Tentar extrair do diretório pai do path_parquet
                    parent_dir = os.path.basename(os.path.dirname(path_parquet))
                    if re.match(r'^\d{4}-\d{2}$', parent_dir):
                        remote_folder = parent_dir
                    else:
                        # Último recurso: usar um valor padrão fixo
                        remote_folder = "dados"
                        logger.warning(f"Não foi possível extrair pasta remota do caminho. Usando pasta padrão: {remote_folder}")
        
        logger.info(f"Pasta remota identificada: {remote_folder}")
        
        # Forçar a utilização do remote_folder para garantir que não salve na raiz do parquet
        # Usando a função que garante a estrutura correta de pastas
        output_dir = ensure_correct_folder_structure(path_parquet, remote_folder, table_name)
                
        total_rows = df.height
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
        logger.error(f"Erro ao criar arquivo Parquet: {str(e)}")
        return False


def extract_zip_parallel(zip_path: str, extract_dir: str, max_workers: int = None) -> bool:
    """
    Extrai um arquivo ZIP usando múltiplas threads para melhor performance.
    
    Args:
        zip_path: Caminho para o arquivo ZIP
        extract_dir: Diretório de destino para extração
        max_workers: Número máximo de workers (padrão: número de CPUs)
        
    Returns:
        bool: True se a extração foi bem-sucedida, False caso contrário
    """
    if not os.path.exists(zip_path):
        logger.error(f"Arquivo ZIP não encontrado: {zip_path}")
        return False
    
    # Criar diretório de destino se não existir
    os.makedirs(extract_dir, exist_ok=True)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            
            if not file_list:
                logger.warning(f"Arquivo ZIP vazio: {zip_path}")
                return True
            
            # Determinar número de workers
            if max_workers is None:
                max_workers = min(len(file_list), os.cpu_count() or 4)
            
            logger.info(f"Iniciando extração paralela de {len(file_list)} arquivos do ZIP {os.path.basename(zip_path)}")
            
            start_time = time.time()
            extraction_start = time.time()
            
            def extract_file(file_info):
                """Extrai um único arquivo do ZIP."""
                try:
                    zip_ref.extract(file_info, extract_dir)
                    return file_info, None
                except Exception as e:
                    return file_info, e
            
            # Extrair arquivos em paralelo
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {executor.submit(extract_file, file_info): file_info for file_info in file_list}
                
                extracted_count = 0
                errors = []
                
                for future in as_completed(future_to_file):
                    file_info = future_to_file[future]
                    try:
                        file_name, error = future.result()
                        if error:
                            errors.append(f"Erro ao extrair {file_name}: {error}")
                        else:
                            extracted_count += 1
                    except Exception as e:
                        errors.append(f"Erro inesperado ao extrair {file_info}: {e}")
            
            extraction_end = time.time()
            
            # Mover arquivos se necessário (alguns ZIPs extraem em subdiretórios)
            move_start = time.time()
            moved_files = 0
            
            for root, dirs, files in os.walk(extract_dir):
                if root != extract_dir:  # Se há subdiretórios
                    for file in files:
                        src_path = os.path.join(root, file)
                        dst_path = os.path.join(extract_dir, file)
                        if not os.path.exists(dst_path):
                            shutil.move(src_path, dst_path)
                            moved_files += 1
            
            # Remover diretórios vazios
            for root, dirs, files in os.walk(extract_dir, topdown=False):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    try:
                        if not os.listdir(dir_path):  # Se o diretório está vazio
                            os.rmdir(dir_path)
                    except OSError:
                        pass  # Ignorar erros ao remover diretórios
            
            move_end = time.time()
            total_end = time.time()
            
            # Logs de tempo
            extraction_time = extraction_end - extraction_start
            move_time = move_end - move_start
            total_time = total_end - start_time
            
            logger.info(f"Extração paralela concluída: {extracted_count} arquivos extraídos em {extraction_time:.2f} segundos")
            logger.info(f"Tempo de extração: {extraction_time:.2f} segundos")
            logger.info(f"Tempo de movimentação: {move_time:.2f} segundos")
            logger.info(f"Tempo de movimentação dos arquivos: {move_time:.0f}s")
            logger.info(f"Tempo total de extração: {total_time:.0f}s")
            
            if errors:
                logger.warning(f"Extração concluída com {len(errors)} erros:")
                for error in errors[:5]:  # Mostrar apenas os primeiros 5 erros
                    logger.warning(f"  {error}")
                if len(errors) > 5:
                    logger.warning(f"  ... e mais {len(errors) - 5} erros")
            
            return len(errors) == 0  # Retorna True apenas se não houve erros
            
    except Exception as e:
        logger.error(f"Erro ao extrair ZIP {zip_path}: {e}")
        return False


def extract_file_parallel(zip_path: str, extract_dir: str, max_workers: int = 4) -> bool:
    """
    Wrapper para compatibilidade com código existente.
    """
    return extract_zip_parallel(zip_path, extract_dir, max_workers)


def process_empresa_csv_file(csv_path: str) -> Optional[pl.DataFrame]:
    """
    Processa um único arquivo CSV de empresa.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame ou None em caso de erro
    """
    try:
        logger.debug(f"Processando arquivo CSV: {os.path.basename(csv_path)}")
        
        # Usa polars.read_csv com os parâmetros apropriados
        df = pl.read_csv(
            csv_path,
            separator=';',
            has_header=False,
            encoding='latin1',
            ignore_errors=True,
            truncate_ragged_lines=True
        )
        
        logger.debug(f"Arquivo {os.path.basename(csv_path)} carregado com {df.height} linhas")
        return df
        
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)}: {str(e)}')
        return None


def process_data_file(data_file_path: str) -> Optional[pl.DataFrame]:
    """
    Processa um único arquivo de dados, seja ele CSV ou outro formato de texto.
    
    Args:
        data_file_path: Caminho para o arquivo de dados
        
    Returns:
        DataFrame ou None em caso de erro
    """
    try:
        # Detectar separador automaticamente
        separators = [';', ',', '\t', '|']
        df = None
        
        for sep in separators:
            try:
                df = pl.read_csv(
                    data_file_path,
                    separator=sep,
                    has_header=False,
                    encoding='latin1',
                    ignore_errors=True,
                    truncate_ragged_lines=True
                )
                
                # Verificar se o DataFrame tem dados válidos
                if df.height > 0 and df.width > 1:
                    logger.info(f"Arquivo {os.path.basename(data_file_path)} processado com sucesso usando separador {repr(sep)}")
                    break
                    
            except Exception:
                continue
        
        if df is None or df.height == 0:
            logger.warning(f"Não foi possível processar o arquivo {os.path.basename(data_file_path)} com nenhum separador")
            return None
            
        return df
        
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(data_file_path)}: {str(e)}')
        return None


def save_empresa_parquet(df: pl.DataFrame, output_path: str, zip_prefix: str) -> bool:
    """
    Salva DataFrame em arquivos Parquet particionados para reduzir uso de memória.
    
    Args:
        df: DataFrame
        output_path: Caminho de saída
        zip_prefix: Prefixo do arquivo ZIP (usado para nomear as partições)
        
    Returns:
        bool: True se salvou com sucesso
    """
    try:
        os.makedirs(output_path, exist_ok=True)
        
        # Calcular número de partições baseado no tamanho dos dados
        total_rows = df.height
        rows_per_partition = 500_000  # 500k linhas por partição
        num_partitions = max(1, (total_rows + rows_per_partition - 1) // rows_per_partition)
        
        logger.info(f"Salvando DataFrame de empresas com {total_rows} linhas para {output_path}")
        
        # Salvar em partições
        for i in range(num_partitions):
            start_idx = i * rows_per_partition
            end_idx = min((i + 1) * rows_per_partition, total_rows)
            
            partition_df = df.slice(start_idx, end_idx - start_idx)
            partition_file = os.path.join(output_path, f"{zip_prefix}_part{i:03d}.parquet")
            
            logger.info(f"Salvando partição {i+1}/{num_partitions} para {partition_file}")
            
            partition_df.write_parquet(
                partition_file,
                compression="snappy",
                use_pyarrow=True
            )
            
            logger.info(f"Partição {i+1}/{num_partitions} salva com sucesso")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar parquet para empresas: {e}")
        return False


def create_empresa_privada_subset(df: pl.DataFrame, output_path: str, zip_prefix: str) -> bool:
    """
    Cria um subset apenas com empresas privadas (natureza_juridica específicas).
    
    Args:
        df: DataFrame principal
        output_path: Caminho de saída
        zip_prefix: Prefixo do arquivo ZIP
        
    Returns:
        bool: True se criou com sucesso
    """
    try:
        # Códigos de natureza jurídica para empresas privadas (principais)
        # Lista baseada na tabela da Receita Federal
        private_codes = [
            206,  # Sociedade Empresária Limitada
            213,  # Empresário (Individual)
            230,  # Sociedade Anônima Fechada
            231,  # Sociedade Anônima Aberta
            224,  # Sociedade Simples Limitada
            # Adicionar outros códigos conforme necessário
        ]
        
        # Filtrar apenas empresas privadas
        df_private = df.filter(pl.col("natureza_juridica").is_in(private_codes))
        
        if df_private.height == 0:
            logger.warning(f"Nenhuma empresa privada encontrada para {zip_prefix}")
            return True
        
        # Criar subdiretório para empresas privadas
        private_output_path = os.path.join(output_path, "empresa_privada")
        os.makedirs(private_output_path, exist_ok=True)
        
        logger.info(f"Criando subset de empresas privadas: {df_private.height} empresas")
        
        # Salvar subset
        return save_empresa_parquet(df_private, private_output_path, f"{zip_prefix}_privada")
        
    except Exception as e:
        logger.error(f"Erro ao criar subset de empresas privadas: {e}")
        return False


def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, 
                      remote_folder: str = None, create_private: bool = False) -> bool:
    """
    Processa um único arquivo ZIP de empresas.
    
    Args:
        zip_file: Nome do arquivo ZIP
        path_zip: Diretório onde está o arquivo ZIP
        path_unzip: Diretório para extração
        path_parquet: Diretório para salvar parquets
        remote_folder: Pasta remota (para organização)
        create_private: Se deve criar subset de empresas privadas
        
    Returns:
        bool: True se processou com sucesso
    """
    pid = os.getpid()
    path_extracao = None  # Inicializar para usar no finally
    
    try:
        logger.info(f"[{pid}] Iniciando processamento para: {zip_file} (create_private={create_private})")
        
        # Construir caminhos
        zip_file_path = os.path.join(path_zip, zip_file)
        zip_prefix = os.path.splitext(zip_file)[0]
        
        # Verificar se arquivo ZIP existe
        if not os.path.exists(zip_file_path):
            logger.error(f"[{pid}] Arquivo ZIP {zip_file_path} não existe")
            return False
        
        # Identificar pasta remota se não fornecida
        if remote_folder is None:
            # Tentar extrair da estrutura de diretórios
            parent_dir = os.path.basename(os.path.dirname(zip_file_path))
            if parent_dir and parent_dir != path_zip:
                remote_folder = parent_dir
                logger.info(f"[{pid}] Pasta remota identificada: {remote_folder}")
        
        # --- FASE 1: EXTRAÇÃO ---
        logger.info(f"[{pid}] Fase 1: Iniciando extração de {zip_file}..." )
        
        path_extracao = os.path.join(path_unzip, zip_prefix)
        
        # Limpar diretório de extração se existir
        if os.path.exists(path_extracao):
            logger.debug(f"[{pid}] Limpando diretório de extração: {path_extracao}")
            try:
                shutil.rmtree(path_extracao)
            except Exception as e_clean:
                logger.warning(f"[{pid}] Erro ao limpar diretório de extração prévio {path_extracao}: {e_clean}")
        
        # Extrair usando função paralela
        logger.info(f"[{pid}] Extraindo {zip_file} para {path_extracao}")
        
        start_extraction = time.time()
        extraction_success = extract_zip_parallel(zip_file_path, path_extracao)
        end_extraction = time.time()
        
        if not extraction_success:
            logger.error(f"[{pid}] Falha na extração de {zip_file}")
            return False
        
        extraction_time = end_extraction - start_extraction
        logger.info(f"Tempo de extração: {extraction_time:.0f}s")
        
        # Verificar se há arquivos extraídos
        if not os.path.exists(path_extracao) or not os.listdir(path_extracao):
            logger.error(f"[{pid}] Nenhum arquivo foi extraído de {zip_file}")
            return False
        
        # Listar arquivos de dados extraídos
        data_files = []
        for root, dirs, files in os.walk(path_extracao):
            for file in files:
                # Processar todos os arquivos extraídos (exceto diretórios)
                file_path = os.path.join(root, file)
                if os.path.isfile(file_path):
                    data_files.append(file_path)
        
        if not data_files:
            logger.warning(f"[{pid}] Nenhum arquivo de dados encontrado no diretório {path_extracao} após extração")
            return False
        
        logger.info(f"[{pid}] Arquivos extraídos: {[os.path.basename(f) for f in data_files]}")
        
        # --- FASE 2: LEITURA E PROCESSAMENTO ---
        logger.info(f"[{pid}] Fase 2: Iniciando leitura e processamento de arquivos de dados..." )
        
        logger.info(f"[{pid}] Encontrados {len(data_files)} arquivos para processar")
        
        # Preparar diretório de saída
        output_empresas_path = ensure_correct_folder_structure(
            path_parquet, 
            remote_folder,
            "empresas"
        )
        
        # Processar cada arquivo de dados diretamente em chunks
        chunk_counter = 0
        
        for data_file in data_files:
            try:
                logger.debug(f"[{pid}] Processando arquivo {os.path.basename(data_file)}")
                
                # Processar arquivo em chunks diretamente
                file_chunk_counter = process_data_file_in_chunks(
                    data_file, 
                    output_empresas_path, 
                    zip_prefix, 
                    chunk_counter,
                    create_private
                )
                
                if file_chunk_counter > 0:
                    chunk_counter += file_chunk_counter
                    logger.info(f"[{pid}] Arquivo {os.path.basename(data_file)} processado: {file_chunk_counter} chunks salvos")
                else:
                    logger.warning(f"[{pid}] Nenhum chunk gerado para arquivo: {os.path.basename(data_file)}")
                    
            except Exception as e_data:
                logger.error(f"[{pid}] Erro ao processar arquivo {os.path.basename(data_file)}: {e_data}")
                continue
        
        # Se não temos chunks válidos, encerramos
        if chunk_counter == 0:
            logger.warning(f"[{pid}] Nenhum chunk válido gerado. Encerrando processamento de {zip_file}")
            return False
        
        logger.info(f"[{pid}] Processamento para {zip_file} concluído: {chunk_counter} chunks salvos")
        return True
            
    except Exception as e:
        logger.error(f"[{pid}] Erro processando {zip_file}: {str(e)}")
        return False
    finally:
        # SEMPRE limpar diretório de extração, independentemente de sucesso ou erro
        if path_extracao and os.path.exists(path_extracao):
            try:
                logger.info(f"[{pid}] Limpando diretório de extração: {path_extracao}")
                shutil.rmtree(path_extracao)
                logger.info(f"[{pid}] Diretório de extração {path_extracao} removido com sucesso")
            except Exception as e_clean:
                logger.error(f"[{pid}] Erro ao limpar diretório de extração {path_extracao}: {e_clean}")
                # Tentar forçar a remoção
                try:
                    import stat
                    def handle_remove_readonly(func, path, exc):
                        os.chmod(path, stat.S_IWRITE)
                        func(path)
                    shutil.rmtree(path_extracao, onerror=handle_remove_readonly)
                    logger.info(f"[{pid}] Diretório de extração {path_extracao} removido com sucesso (segunda tentativa)")
                except Exception as e_force:
                    logger.error(f"[{pid}] Falha definitiva ao remover diretório {path_extracao}: {e_force}")


def get_system_resources():
    """Retorna informações sobre os recursos do sistema."""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    return {
        'cpu_percent': cpu_percent,
        'memory_percent': memory.percent,
        'disk_percent': disk.percent
    }

def can_start_processing():
    """Verifica se é possível iniciar um novo processamento."""
    with _processing_lock:
        resources = get_system_resources()
        
        # Verificar recursos do sistema
        if (resources['cpu_percent'] > 80 or 
            resources['memory_percent'] > 80 or 
            resources['disk_percent'] > 90):
            return False
            
        # Verificar número de processos ativos
        if _active_processes.value >= _max_concurrent_processes.value:
            return False
            
        return True

def add_to_process_queue(zip_file: str, priority: int = 1):
    """Adiciona um arquivo à fila de processamento."""
    _process_queue.put((priority, time.time(), zip_file))
    logger.info(f"Arquivo {zip_file} adicionado à fila de processamento")

def process_queue_worker(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False):
    """Worker que processa a fila de arquivos."""
    worker_id = threading.current_thread().name
    logger.info(f"[WORKER-{worker_id}] Worker iniciado para processamento de empresas")
    
    consecutive_empty_checks = 0
    max_empty_checks = 6  # Máximo de 6 verificações vazias (30 segundos) antes de parar
    
    while not _workers_should_stop.value:
        try:
            if _process_queue.empty():
                consecutive_empty_checks += 1
                if consecutive_empty_checks >= max_empty_checks:
                    logger.info(f"[WORKER-{worker_id}] Fila vazia por {max_empty_checks * 5}s. Finalizando worker.")
                    break
                
                logger.debug(f"[WORKER-{worker_id}] Fila vazia, aguardando 5 segundos... ({consecutive_empty_checks}/{max_empty_checks})")
                time.sleep(5)
                continue
            
            # Reset contador se encontrou trabalho
            consecutive_empty_checks = 0
                
            if not can_start_processing():
                resources = get_system_resources()
                logger.debug(f"[WORKER-{worker_id}] Recursos insuficientes - CPU: {resources['cpu_percent']:.1f}%, "
                           f"Memória: {resources['memory_percent']:.1f}%, "
                           f"Processos ativos: {_active_processes.value}/{_max_concurrent_processes.value}")
                time.sleep(10)  # Espera 10 segundos se não puder processar
                continue
                
            # Pega o próximo arquivo da fila
            try:
                priority, timestamp, zip_file = _process_queue.get_nowait()
            except:
                # Fila ficou vazia entre a verificação e o get
                continue
                
            logger.info(f"[WORKER-{worker_id}] Iniciando processamento de {zip_file}")
            
            with _processing_lock:
                _active_processes.value += 1
                logger.debug(f"[WORKER-{worker_id}] Processos ativos: {_active_processes.value}/{_max_concurrent_processes.value}")
                
            try:
                # Obter tamanho do arquivo para estatísticas
                zip_path = os.path.join(path_zip, zip_file)
                file_size = os.path.getsize(zip_path) if os.path.exists(zip_path) else 0
                
                # Processa o arquivo
                start_time = time.time()
                result = process_single_zip(zip_file, path_zip, path_unzip, path_parquet, create_private=create_private)
                elapsed_time = time.time() - start_time
                
                # Registrar estatística de processamento
                global_stats.add_processing_stat(
                    filename=zip_file,
                    file_type="empresas",
                    size_bytes=file_size,
                    start_time=start_time,
                    end_time=time.time(),
                    success=result,
                    error=None if result else "Processamento falhou"
                )
                
                if result:
                    logger.info(f"[WORKER-{worker_id}] ✓ {zip_file} processado com sucesso em {elapsed_time:.2f}s")
                else:
                    logger.error(f"[WORKER-{worker_id}] ✗ Falha ao processar {zip_file} após {elapsed_time:.2f}s")
                    
            finally:
                with _processing_lock:
                    _active_processes.value -= 1
                    logger.debug(f"[WORKER-{worker_id}] Processo finalizado. Processos ativos: {_active_processes.value}/{_max_concurrent_processes.value}")
                    
        except Exception as e:
            logger.error(f"[WORKER-{worker_id}] Erro no worker da fila: {str(e)}")
            # Registrar estatística de erro
            try:
                global_stats.add_processing_stat(
                    filename=zip_file if 'zip_file' in locals() else "unknown",
                    file_type="empresas",
                    size_bytes=0,
                    start_time=start_time if 'start_time' in locals() else time.time(),
                    end_time=time.time(),
                    success=False,
                    error=str(e)
                )
            except:
                pass  # Evitar erro duplo
            time.sleep(5)
    
    logger.info(f"[WORKER-{worker_id}] Worker finalizado")

def start_queue_worker(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False):
    """Inicia o worker da fila em uma thread separada."""
    worker_thread = threading.Thread(target=process_queue_worker, args=(path_zip, path_unzip, path_parquet, create_private), daemon=True)
    worker_thread.start()
    return worker_thread

def process_empresa_files(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Processa os dados de empresas."""
    start_time = time.time()
    
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de EMPRESAS')
    logger.info('=' * 50)
    
    try:
        # Reset flag de parada
        _workers_should_stop.value = False
        
        zip_files = [f for f in os.listdir(path_zip)
                     if f.startswith('Empr') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Empresas encontrado.')
            return True
            
        logger.info(f"Encontrados {len(zip_files)} arquivos ZIP de empresas para processar")
        logger.info(f"Máximo de processos concorrentes: {_max_concurrent_processes.value}")
        
        # Iniciar worker da fila
        logger.info("Iniciando worker de processamento...")
        worker_thread = start_queue_worker(path_zip, path_unzip, path_parquet, create_private)
        
        # Adicionar arquivos à fila
        logger.info("Adicionando arquivos à fila de processamento...")
        for zip_file in zip_files:
            add_to_process_queue(zip_file)
            
        logger.info(f"Todos os {len(zip_files)} arquivos adicionados à fila")
        
        # Aguardar processamento da fila com relatórios de progresso
        last_queue_size = len(zip_files)
        processed_count = 0
        no_progress_count = 0
        
        while not _process_queue.empty() or _active_processes.value > 0:
            current_queue_size = _process_queue.qsize()
            current_active = _active_processes.value
            
            # Calcular progresso
            if current_queue_size != last_queue_size:
                processed_count = len(zip_files) - current_queue_size
                progress_percent = (processed_count / len(zip_files)) * 100
                
                logger.info(f"Progresso: {processed_count}/{len(zip_files)} arquivos processados ({progress_percent:.1f}%) - "
                           f"Fila: {current_queue_size}, Ativos: {current_active}")
                last_queue_size = current_queue_size
                no_progress_count = 0
            else:
                no_progress_count += 1
                
                # Se não há progresso por muito tempo e não há processos ativos, sair
                if no_progress_count > 12 and current_active == 0:  # 60 segundos sem progresso
                    logger.warning("Sem progresso por 60 segundos e nenhum processo ativo. Finalizando...")
                    break
            
            # Mostrar informações detalhadas em modo DEBUG
            if logger.isEnabledFor(logging.DEBUG):
                resources = get_system_resources()
                logger.debug(f"Status detalhado - Fila: {current_queue_size}, Ativos: {current_active}, "
                           f"CPU: {resources['cpu_percent']:.1f}%, Memória: {resources['memory_percent']:.1f}%")
            
            time.sleep(5)
        
        # Sinalizar workers para parar
        _workers_should_stop.value = True
        
        # Aguardar worker finalizar (máximo 30 segundos)
        if worker_thread.is_alive():
            logger.info("Aguardando worker finalizar...")
            worker_thread.join(timeout=30)
            if worker_thread.is_alive():
                logger.warning("Worker não finalizou no tempo esperado")
            
        # Calcular estatísticas finais
        total_time = time.time() - start_time
        
        logger.info("=" * 50)
        logger.info("RESUMO DO PROCESSAMENTO DE EMPRESAS:")
        logger.info("=" * 50)
        logger.info(f"Arquivos processados: {len(zip_files)}")
        logger.info(f"Tempo total de processamento: {format_elapsed_time(total_time)}")
        logger.info(f"Tempo médio por arquivo: {total_time/len(zip_files):.2f}s")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f'Erro no processamento principal de Empresas: {str(e)}')
        traceback.print_exc()
        return False
    finally:
        # Garantir que workers parem
        _workers_should_stop.value = True

def process_data_file_in_chunks(data_file_path: str, output_dir: str, zip_prefix: str, start_chunk_counter: int = 0, create_private: bool = False) -> int:
    """
    Processa um arquivo de dados diretamente em chunks, salvando cada chunk como Parquet.
    
    Args:
        data_file_path: Caminho do arquivo de dados
        output_dir: Diretório de saída
        zip_prefix: Prefixo do arquivo ZIP
        start_chunk_counter: Contador inicial de chunks
        create_private: Se deve criar subset de empresas privadas
        
    Returns:
        int: Número de chunks processados
    """
    chunk_counter = 0
    chunk_size = 500000  # 500k linhas por chunk
    compiled_transformations = None
    
    try:
        logger.info(f"Processando arquivo {os.path.basename(data_file_path)} em chunks de {chunk_size} linhas")
        
        # Detectar separador lendo primeira linha
        with open(data_file_path, 'r', encoding='latin-1') as file:
            first_line = file.readline().strip()
            if not first_line:
                logger.warning(f"Arquivo {os.path.basename(data_file_path)} está vazio")
                return 0
            
            separator = ';' if ';' in first_line else ','
            logger.debug(f"Separador detectado: '{separator}'")
        
        # Usar scan_csv do Polars para leitura eficiente em chunks
        try:
            # Primeiro, verificar quantas linhas tem o arquivo
            total_lines = 0
            with open(data_file_path, 'r', encoding='latin-1') as f:
                for _ in f:
                    total_lines += 1
            
            if total_lines == 0:
                logger.warning(f"Arquivo {os.path.basename(data_file_path)} não tem linhas")
                return 0
            
            logger.info(f"Arquivo tem {total_lines} linhas, processando em chunks de {chunk_size}")
            
            # Processar em chunks usando skip_rows e n_rows
            rows_processed = 0
            
            while rows_processed < total_lines:
                try:
                    # Calcular quantas linhas ler neste chunk
                    rows_to_read = min(chunk_size, total_lines - rows_processed)
                    
                    if rows_to_read <= 0:
                        break
                    
                    logger.debug(f"Lendo chunk {chunk_counter + 1}: linhas {rows_processed} a {rows_processed + rows_to_read - 1}")
                    
                    # Ler chunk específico
                    df_chunk = pl.read_csv(
                        data_file_path,
                        separator=separator,
                        encoding='latin-1',
                        skip_rows=rows_processed,
                        n_rows=rows_to_read,
                        has_header=False,  # Sempre False pois estamos pulando linhas
                        new_columns=config.empresa_columns,
                        infer_schema_length=0,
                        dtypes={col: pl.Utf8 for col in config.empresa_columns},
                        ignore_errors=True,
                        truncate_ragged_lines=True
                    )
                    
                    if df_chunk.is_empty():
                        logger.debug(f"Chunk {chunk_counter + 1} está vazio, avançando...")
                        rows_processed += rows_to_read
                        continue
                    
                    # Compilar transformações apenas no primeiro chunk
                    if compiled_transformations is None:
                        logger.info("Compilando transformações de Empresas (uma única vez)...")
                        compiled_transformations = compile_empresa_transformations(df_chunk)
                        logger.info("✅ Transformações de Empresas compiladas com sucesso")
                    
                    # Aplicar transformações otimizadas
                    df_transformed = apply_empresa_transformations_optimized(df_chunk, compiled_transformations)
                    
                    if not df_transformed.is_empty():
                        # Salvar chunk principal como Parquet
                        chunk_filename = f"{zip_prefix}_chunk{start_chunk_counter + chunk_counter + 1:03d}.parquet"
                        chunk_path = os.path.join(output_dir, chunk_filename)
                        
                        df_transformed.write_parquet(chunk_path, compression="snappy")
                        
                        logger.info(f"Chunk {chunk_counter + 1} salvo: {df_transformed.height} linhas em {chunk_filename}")
                        
                        # Criar subset de empresas privadas se solicitado
                        if create_private:
                            try:
                                private_df = df_transformed.filter(
                                    pl.col("natureza_juridica").is_in([
                                        "206-2", "213-4", "224-0", "230-4", "231-2", "234-7"
                                    ])
                                )
                                if not private_df.is_empty():
                                    private_filename = f"{zip_prefix}_privada_chunk{start_chunk_counter + chunk_counter + 1:03d}.parquet"
                                    private_path = os.path.join(output_dir, "empresa_privada", private_filename)
                                    os.makedirs(os.path.dirname(private_path), exist_ok=True)
                                    private_df.write_parquet(private_path, compression="snappy")
                                    logger.info(f"Chunk privado {chunk_counter + 1} salvo: {private_df.height} linhas")
                                del private_df
                            except Exception as e:
                                logger.warning(f"Erro ao criar subset privado para chunk {chunk_counter + 1}: {e}")
                        
                        chunk_counter += 1
                    
                    # Liberar memória
                    del df_chunk, df_transformed
                    gc.collect()
                    
                    # Avançar para próximo chunk
                    rows_processed += rows_to_read
                    
                except Exception as e:
                    logger.error(f"Erro ao processar chunk {chunk_counter + 1}: {str(e)}")
                    # Avançar mesmo com erro para evitar loop infinito
                    rows_processed += chunk_size
                    break
            
            logger.info(f"Arquivo {os.path.basename(data_file_path)} processado: {chunk_counter} chunks salvos de {total_lines} linhas")
            return chunk_counter
            
        except Exception as e:
            logger.error(f"Erro na leitura do arquivo {data_file_path}: {str(e)}")
            return 0
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {data_file_path} em chunks: {str(e)}")
        return 0
