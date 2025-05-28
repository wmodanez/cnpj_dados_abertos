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
from ..utils.progress_tracker import progress_tracker
import inspect

logger = logging.getLogger(__name__)

# Variáveis globais para controle de recursos
_processing_lock = Lock()
_active_processes = Value('i', 0)
# Usar pelo menos metade dos núcleos do processador
_max_concurrent_processes = Value('i', max(2, (os.cpu_count() or 4) // 2))
_process_queue = PriorityQueue()
_workers_should_stop = Value('b', False)  # Flag para parar workers

def log_system_resources_socio():
    """Log detalhado dos recursos do sistema para processamento de sócios."""
    cpu_count = os.cpu_count() or 4
    memory_info = psutil.virtual_memory()
    memory_total_gb = memory_info.total / (1024**3)
    memory_available_gb = memory_info.available / (1024**3)
    memory_percent = memory_info.percent
    
    max_workers = _max_concurrent_processes.value
    
    logger.info("=" * 50)
    logger.info("👥 MÓDULO SÓCIO - CONFIGURAÇÃO DE RECURSOS")
    logger.info("=" * 50)
    logger.info(f"💻 CPU: {cpu_count} núcleos disponíveis")
    logger.info(f"🧠 RAM: {memory_total_gb:.1f}GB total, {memory_available_gb:.1f}GB disponível ({100-memory_percent:.1f}%)")
    logger.info(f"⚙️  Workers configurados: {max_workers} ({(max_workers/cpu_count)*100:.1f}% dos núcleos)")
    logger.info(f"📊 Estratégia: Usar pelo menos 50% dos núcleos para processamento paralelo")
    logger.info(f"🔄 Capacidade estimada: ~{max_workers * 2} arquivos ZIP simultâneos")
    logger.info(f"💾 Memória por worker: ~{memory_available_gb/max_workers:.1f}GB")
    
    if memory_percent > 80:
        logger.warning(f"⚠️  ATENÇÃO: Uso alto de memória ({memory_percent:.1f}%)")
    if cpu_count < 4:
        logger.warning(f"⚠️  ATENÇÃO: Poucos núcleos CPU ({cpu_count}) - considere upgrade")
    if max_workers == cpu_count:
        logger.info(f"✅ Configuração otimizada: usando todos os núcleos disponíveis")
    elif max_workers >= cpu_count // 2:
        logger.info(f"✅ Configuração balanceada: usando {(max_workers/cpu_count)*100:.0f}% dos núcleos")
    else:
        logger.info(f"⚠️  Configuração conservadora: usando apenas {(max_workers/cpu_count)*100:.0f}% dos núcleos")
    
    logger.info("=" * 50)

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
    logger.info(f"[WORKER-{worker_id}] Worker iniciado para processamento de sócios")
    
    # Registrar início do worker no rastreador de progresso
    progress_tracker.start_worker("socios", worker_id)
    
    consecutive_empty_checks = 0
    max_empty_checks = 6  # Máximo de 6 verificações vazias (30 segundos) antes de parar
    
    try:
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
                
                # Registrar início do processamento do arquivo
                progress_tracker.start_file("socios", zip_file, worker_id)
                
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
                    
                    # Registrar conclusão do arquivo no rastreador de progresso
                    progress_tracker.complete_file("socios", zip_file, result, worker_id, elapsed_time)
                    
                    # Registrar estatística de processamento
                    global_stats.add_processing_stat(
                        filename=zip_file,
                        file_type="socios",
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
                    # Registrar falha no arquivo se estava sendo processado
                    if 'zip_file' in locals():
                        progress_tracker.complete_file("socios", zip_file, False, worker_id)
                    
                    global_stats.add_processing_stat(
                        filename=zip_file if 'zip_file' in locals() else "unknown",
                        file_type="socios",
                        size_bytes=0,
                        start_time=start_time if 'start_time' in locals() else time.time(),
                        end_time=time.time(),
                        success=False,
                        error=str(e)
                    )
                except:
                    pass  # Evitar erro duplo
                time.sleep(5)
    
    finally:
        # Registrar finalização do worker no rastreador de progresso
        progress_tracker.stop_worker("socios", worker_id)
        logger.info(f"[WORKER-{worker_id}] Worker finalizado")

def start_queue_worker(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False):
    """Inicia múltiplos workers da fila em threads separadas."""
    worker_threads = []
    num_workers = _max_concurrent_processes.value
    
    logger.info(f"Iniciando {num_workers} workers para processamento paralelo...")
    
    for i in range(num_workers):
        worker_thread = threading.Thread(
            target=process_queue_worker, 
            args=(path_zip, path_unzip, path_parquet, create_private), 
            daemon=True,
            name=f"Worker-{i+1}"
        )
        worker_thread.start()
        worker_threads.append(worker_thread)
        logger.debug(f"Worker {i+1}/{num_workers} iniciado")
    
    return worker_threads

def process_socio(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios."""
    return process_socio_files(path_zip, path_unzip, path_parquet)

def process_data_file_in_chunks(data_file_path: str, output_dir: str, zip_prefix: str, start_chunk_counter: int = 0) -> int:
    """
    Processa um arquivo de dados diretamente em chunks, salvando cada chunk como Parquet.
    
    Args:
        data_file_path: Caminho do arquivo de dados
        output_dir: Diretório de saída
        zip_prefix: Prefixo do arquivo ZIP
        start_chunk_counter: Contador inicial de chunks
        
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
                        new_columns=config.socio_columns,
                        infer_schema_length=0,
                        dtypes={col: pl.Utf8 for col in config.socio_columns},
                        ignore_errors=True,
                        truncate_ragged_lines=True,
                        # Parâmetros adicionais para lidar com campos mal formatados
                        quote_char=None,  # Desabilitar processamento de aspas
                        null_values=["", "NULL", "null", "00000000", '"00000000', '"00000000"'],  # Valores nulos comuns
                        missing_utf8_is_empty_string=True,  # Tratar UTF-8 inválido como string vazia
                        try_parse_dates=False  # Não tentar parsear datas automaticamente
                    )
                    
                    if df_chunk.is_empty():
                        logger.debug(f"Chunk {chunk_counter + 1} está vazio, avançando...")
                        rows_processed += rows_to_read
                        continue
                    
                    # Compilar transformações apenas no primeiro chunk
                    if compiled_transformations is None:
                        logger.info("Compilando transformações de Sócios (uma única vez)...")
                        compiled_transformations = compile_socio_transformations(df_chunk)
                        if compiled_transformations['has_transformations']:
                            logger.info("✅ Transformações compiladas com sucesso")
                        else:
                            logger.info("ℹ️ Nenhuma transformação necessária para este arquivo")
                    
                    # Aplicar transformações otimizadas
                    df_transformed = apply_socio_transformations_optimized(df_chunk, compiled_transformations)
                    
                    if not df_transformed.is_empty():
                        # Salvar chunk como Parquet
                        chunk_filename = f"{zip_prefix}_chunk{start_chunk_counter + chunk_counter + 1:03d}.parquet"
                        chunk_path = os.path.join(output_dir, chunk_filename)
                        
                        df_transformed.write_parquet(chunk_path, compression="snappy")
                        
                        logger.info(f"Chunk {chunk_counter + 1} salvo: {df_transformed.height} linhas em {chunk_filename}")
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

def process_data_file(data_file_path: str) -> Optional[pl.DataFrame]:
    """
    Processa um arquivo de dados, seja ele CSV ou outro formato de texto.
    
    Args:
        data_file_path: Caminho para o arquivo de dados
        
    Returns:
        DataFrame ou None em caso de erro
    """
    logger = logging.getLogger(__name__)
    
    # Verificar se o arquivo é um arquivo de texto
    try:
        # Tentar ler as primeiras linhas para verificar se é um arquivo de texto
        is_text_file = True
        with open(data_file_path, 'rb') as f:
            sample = f.read(4096)  # Ler os primeiros 4KB
            # Verificar se há caracteres nulos ou muitos bytes não-ASCII
            # o que pode indicar que é um arquivo binário
            if b'\x00' in sample or len([b for b in sample if b > 127]) > len(sample) * 0.3:
                logger.warning(f"Arquivo {os.path.basename(data_file_path)} parece ser binário, não texto.")
                is_text_file = False
        
        if not is_text_file:
            return None
    except Exception as e:
        logger.error(f"Erro ao verificar se {os.path.basename(data_file_path)} é um arquivo de texto: {str(e)}")
        return None

    # Usar colunas da config
    original_column_names = config.socio_columns

    # Primeiro, tentar com o separador padrão
    try:
        df = pl.read_csv(
            data_file_path,
            separator=config.file.separator,
            encoding=config.file.encoding,
            has_header=False,
            new_columns=original_column_names,
            infer_schema_length=0,  # Não inferir schema
            dtypes={col: pl.Utf8 for col in original_column_names},  # Inicialmente lê tudo como string
            ignore_errors=True,  # Ignorar linhas com erros
            # Parâmetros adicionais para lidar com campos mal formatados
            quote_char=None,  # Desabilitar processamento de aspas
            null_values=["", "NULL", "null", "00000000", '"00000000', '"00000000"'],  # Valores nulos comuns
            missing_utf8_is_empty_string=True,  # Tratar UTF-8 inválido como string vazia
            try_parse_dates=False,  # Não tentar parsear datas automaticamente
            truncate_ragged_lines=True  # Truncar linhas com colunas extras
        )
        if not df.is_empty():
            logger.info(f"Arquivo {os.path.basename(data_file_path)} processado com sucesso usando separador padrão")
            return df
    except Exception as e:
        logger.warning(f"Erro ao processar {os.path.basename(data_file_path)} com separador padrão: {str(e)}")
    
    # Se falhar com o separador padrão, tentar detectar o separador
    separators = [';', ',', '|', '\t']
    for sep in separators:
        if sep == config.file.separator:
            continue  # Já tentamos esse
        
        try:
            df = pl.read_csv(
                data_file_path,
                separator=sep,
                encoding=config.file.encoding,
                has_header=False,
                new_columns=original_column_names,
                infer_schema_length=0,
                dtypes={col: pl.Utf8 for col in original_column_names},
                ignore_errors=True,
                # Parâmetros adicionais para lidar com campos mal formatados
                quote_char=None,  # Desabilitar processamento de aspas
                null_values=["", "NULL", "null", "00000000", '"00000000', '"00000000"'],  # Valores nulos comuns
                missing_utf8_is_empty_string=True,  # Tratar UTF-8 inválido como string vazia
                try_parse_dates=False,  # Não tentar parsear datas automaticamente
                truncate_ragged_lines=True  # Truncar linhas com colunas extras
            )
            if not df.is_empty():
                logger.info(f"Arquivo {os.path.basename(data_file_path)} processado com sucesso usando separador '{sep}'")
                return df
        except Exception as e:
            logger.debug(f"Erro ao processar {os.path.basename(data_file_path)} com separador '{sep}': {str(e)}")
    
    # Se ainda falhar, tentar com diferentes codificações
    encodings = ['latin1', 'utf-8', 'utf-16', 'cp1252']
    for enc in encodings:
        if enc == config.file.encoding:
            continue  # Já tentamos esse
        
        try:
            df = pl.read_csv(
                data_file_path,
                separator=config.file.separator,
                encoding=enc,
                has_header=False,
                new_columns=original_column_names,
                infer_schema_length=0,
                dtypes={col: pl.Utf8 for col in original_column_names},
                ignore_errors=True,
                # Parâmetros adicionais para lidar com campos mal formatados
                quote_char=None,  # Desabilitar processamento de aspas
                null_values=["", "NULL", "null", "00000000", '"00000000', '"00000000"'],  # Valores nulos comuns
                missing_utf8_is_empty_string=True,  # Tratar UTF-8 inválido como string vazia
                try_parse_dates=False,  # Não tentar parsear datas automaticamente
                truncate_ragged_lines=True  # Truncar linhas com colunas extras
            )
            if not df.is_empty():
                logger.info(f"Arquivo {os.path.basename(data_file_path)} processado com sucesso usando codificação '{enc}'")
                return df
        except Exception as e:
            logger.debug(f"Erro ao processar {os.path.basename(data_file_path)} com codificação '{enc}': {str(e)}")
    
    # Se chegamos até aqui, não conseguimos processar o arquivo
    logger.error(f"Não foi possível processar o arquivo {os.path.basename(data_file_path)} com nenhuma combinação de separadores e codificações")
    return None

def apply_socio_transformations(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações específicas para Sócios."""
    logger.info("Aplicando transformações em Sócios...")
    
    # Renomeação de colunas se necessário
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico',
        'identificador_de_socio': 'identificador_socio',
        'nome_do_socio_ou_razao_social': 'nome_socio',
        'cnpj_ou_cpf_do_socio': 'cpf_cnpj_socio',
        'qualificacao_do_socio': 'qualificacao_socio',
        'data_de_entrada_sociedade': 'data_entrada_sociedade',
        'pais': 'codigo_pais',
        'representante_legal': 'nome_representante_legal',
        'nome_do_representante': 'nome_representante_legal',
        'qualificacao_do_representante_legal': 'qualificacao_representante',
        'faixa_etaria': 'faixa_etaria'
    }
    
    # Filtrar para manter apenas colunas que existem no DataFrame
    rename_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
    
    # Aplicar renomeação se houver colunas para renomear
    if rename_mapping:
        df = df.rename(rename_mapping)
    
    # Conversão de colunas numéricas
    int_cols = ['identificador_socio', 'qualificacao_socio', 
                'codigo_pais', 'qualificacao_representante', 'faixa_etaria']
    
    int_expressions = []
    for col in int_cols:
        if col in df.columns:
            int_expressions.append(
                pl.col(col).cast(pl.Int64, strict=False)
            )
    
    if int_expressions:
        df = df.with_columns(int_expressions)
    
    # Conversão de datas
    date_cols = ['data_entrada_sociedade']
    date_expressions = []
    
    for col in date_cols:
        if col in df.columns:
            date_expressions.append(
                pl.when(
                    pl.col(col).is_in(['0', '00000000', '']) | 
                    pl.col(col).is_null()
                )
                .then(None)
                .otherwise(pl.col(col))
                .str.strptime(pl.Date, format="%Y%m%d", strict=False)
                .alias(col)
            )
    
    if date_expressions:
        df = df.with_columns(date_expressions)
    
    return df

def compile_socio_transformations(sample_df: pl.DataFrame) -> dict:
    """
    Compila as transformações de Sócios uma única vez baseado em um DataFrame de amostra.
    Retorna um dicionário com as transformações pré-compiladas.
    """
    transformations = {
        'rename_mapping': {},
        'int_expressions': [],
        'date_expressions': [],
        'has_transformations': False
    }
    
    # Renomeação de colunas se necessário
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico',
        'identificador_de_socio': 'identificador_socio',
        'nome_do_socio_ou_razao_social': 'nome_socio',
        'cnpj_ou_cpf_do_socio': 'cpf_cnpj_socio',
        'qualificacao_do_socio': 'qualificacao_socio',
        'data_de_entrada_sociedade': 'data_entrada_sociedade',
        'pais': 'codigo_pais',
        'representante_legal': 'nome_representante_legal',
        'nome_do_representante': 'nome_representante_legal',
        'qualificacao_do_representante_legal': 'qualificacao_representante',
        'faixa_etaria': 'faixa_etaria'
    }
    
    # Filtrar para manter apenas colunas que existem no DataFrame
    transformations['rename_mapping'] = {k: v for k, v in rename_mapping.items() if k in sample_df.columns}
    
    # Conversão de colunas numéricas
    int_cols = ['identificador_socio', 'qualificacao_socio', 
                'codigo_pais', 'qualificacao_representante', 'faixa_etaria']
    
    for col in int_cols:
        if col in sample_df.columns:
            transformations['int_expressions'].append(
                pl.col(col).cast(pl.Int64, strict=False)
            )
    
    # Conversão de datas
    date_cols = ['data_entrada_sociedade']
    
    for col in date_cols:
        if col in sample_df.columns:
            transformations['date_expressions'].append(
                pl.when(
                    pl.col(col).is_in(['0', '00000000', '']) | 
                    pl.col(col).is_null()
                )
                .then(None)
                .otherwise(pl.col(col))
                .str.strptime(pl.Date, format="%Y%m%d", strict=False)
                .alias(col)
            )
    
    # Verificar se há transformações para aplicar
    transformations['has_transformations'] = (
        bool(transformations['rename_mapping']) or 
        bool(transformations['int_expressions']) or 
        bool(transformations['date_expressions'])
    )
    
    return transformations

def apply_socio_transformations_optimized(df: pl.DataFrame, compiled_transformations: dict) -> pl.DataFrame:
    """
    Aplica transformações pré-compiladas de Sócios de forma otimizada.
    Não faz logs repetitivos nem recompilação de expressões.
    """
    if not compiled_transformations['has_transformations']:
        return df
    
    # Aplicar renomeação se houver colunas para renomear
    if compiled_transformations['rename_mapping']:
        df = df.rename(compiled_transformations['rename_mapping'])
    
    # Aplicar transformações de inteiros
    if compiled_transformations['int_expressions']:
        df = df.with_columns(compiled_transformations['int_expressions'])
    
    # Aplicar transformações de data
    if compiled_transformations['date_expressions']:
        df = df.with_columns(compiled_transformations['date_expressions'])
    
    return df

def save_socio_parquet(df: pl.DataFrame, output_path: str, zip_prefix: str, remote_folder: str = None) -> bool:
    """
    FUNÇÃO OBSOLETA - Mantida para compatibilidade.
    O processamento agora é feito diretamente em chunks.
    """
    logger.warning("save_socio_parquet está obsoleta - usando processamento direto em chunks")
    return True

def extract_file_parallel(zip_path: str, extract_dir: str, num_threads: int = 4) -> bool:
    """
    Extrai arquivo ZIP em paralelo usando múltiplas threads.
    """
    try:
        start_time = time.time()
        # Criar diretório temporário para extração
        temp_dir = tempfile.mkdtemp()
        
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Listar todos os arquivos
            file_list = z.namelist()
            total_files = len(file_list)
            
            logger.info(f"Iniciando extração paralela de {total_files} arquivos do ZIP {os.path.basename(zip_path)}")
            
            # Criar fila de trabalho
            work_queue = Queue()
            for file in file_list:
                work_queue.put(file)
            
            # Função para thread worker
            def worker():
                while not work_queue.empty():
                    try:
                        file = work_queue.get_nowait()
                        z.extract(file, temp_dir)
                        work_queue.task_done()
                    except Exception as e:
                        logger.error(f"Erro na thread de extração: {str(e)}")
                        return False
                return True
            
            # Criar e iniciar threads
            threads = []
            for _ in range(num_threads):
                t = threading.Thread(target=worker)
                t.start()
                threads.append(t)
            
            # Aguardar todas as threads terminarem
            for t in threads:
                t.join()
        
        # Mover arquivos do diretório temporário para o diretório final
        move_start = time.time()
        for item in os.listdir(temp_dir):
            s = os.path.join(temp_dir, item)
            d = os.path.join(extract_dir, item)
            if os.path.isdir(s):
                shutil.copytree(s, d, dirs_exist_ok=True)
            else:
                shutil.copy2(s, d)
        
        # Limpar diretório temporário
        shutil.rmtree(temp_dir)
        
        # Registrar tempos
        move_time = time.time() - move_start
        total_time = time.time() - start_time
        logger.info(f"Extração paralela concluída: {total_files} arquivos extraídos em {total_time:.2f} segundos")
        logger.info(f"Tempo de extração: {total_time - move_time:.2f} segundos")
        logger.info(f"Tempo de movimentação: {move_time:.2f} segundos")
        logger.info(f"Tempo de movimentação dos arquivos: {format_elapsed_time(move_time)}")
        logger.info(f"Tempo total de extração: {format_elapsed_time(total_time)}")
        return True
        
    except Exception as e:
        logger.error(f"Erro na extração paralela: {str(e)}")
        return False

def extract_large_zip(zip_path: str, extract_dir: str, chunk_size: int = 1000000) -> bool:
    """
    Extrai arquivo ZIP grande em chunks para economizar memória.
    """
    try:
        start_time = time.time()
        with zipfile.ZipFile(zip_path, 'r') as z:
            # Listar todos os arquivos
            file_list = z.namelist()
            total_files = len(file_list)
            
            if total_files == 0:
                logger.error(f"Arquivo ZIP {zip_path} está vazio")
                return False
                
            logger.info(f"Iniciando extração de {total_files} arquivos do ZIP {os.path.basename(zip_path)}")
            
            # Processar em chunks
            for i in range(0, total_files, chunk_size):
                chunk_start = time.time()
                chunk_files = file_list[i:i + chunk_size]
                logger.info(f"Extraindo chunk {i//chunk_size + 1} ({len(chunk_files)} arquivos)")
                
                # Extrair chunk atual
                for file in chunk_files:
                    try:
                        z.extract(file, extract_dir)
                    except Exception as e:
                        logger.error(f"Erro ao extrair arquivo {file}: {str(e)}")
                        return False
                
                # Forçar coleta de lixo após cada chunk
                gc.collect()
                
                # Registrar tempo do chunk
                chunk_time = time.time() - chunk_start
                logger.info(f"Tempo de processamento do chunk {i+1}: {format_elapsed_time(chunk_time)}")
                
            # Registrar tempo total
            total_time = time.time() - start_time
            logger.info(f"Extração concluída: {total_files} arquivos extraídos em {total_time:.2f} segundos")
            logger.info(f"Tempo total de processamento: {format_elapsed_time(total_time)}")
            return True
            
    except Exception as e:
        logger.error(f"Erro na extração do ZIP: {str(e)}")

def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, remote_folder: str = None, create_private: bool = False) -> bool:
    """Processa um único arquivo ZIP usando chunks diretos."""
    pid = os.getpid()
    logger.info(f"[{pid}] Iniciando processamento para: {zip_file}")
    extract_dir = os.path.join(path_unzip, os.path.splitext(zip_file)[0])
    success = False
    zip_filename_prefix = os.path.splitext(zip_file)[0]
    
    try:
        # Se remote_folder não foi passado, tentamos extrair do caminho
        if not remote_folder:
            # Extrair pasta remota do caminho zip (geralmente algo como 2025-05)
            remote_folder = os.path.basename(os.path.normpath(path_zip))
            # Verificar se o formato é AAAA-MM
            if not re.match(r'^\d{4}-\d{2}$', remote_folder):
                # Se não for uma pasta no formato esperado, tentar extrair do caminho
                match = re.search(r'(20\d{2}-\d{2})', path_zip)
                if match:
                    remote_folder = match.group(1)
                else:
                    # Último recurso: usar um valor padrão
                    remote_folder = "dados"
        
        logger.info(f"[{pid}] Pasta remota identificada: {remote_folder}")
        
        try:
            # Verificar se o arquivo ZIP existe
            zip_path = os.path.join(path_zip, zip_file)
            if not os.path.exists(zip_path):
                logger.error(f"[{pid}] Arquivo ZIP {zip_path} não existe")
                return False
            
            if os.path.getsize(zip_path) == 0:
                logger.error(f"[{pid}] Arquivo ZIP {zip_path} está vazio (tamanho 0 bytes)")
                return False
            
            # Limpar e criar diretório de extração
            if os.path.exists(extract_dir):
                try:
                    shutil.rmtree(extract_dir)
                except Exception as e:
                    logger.warning(f"[{pid}] Não foi possível limpar o diretório {extract_dir}: {str(e)}")
            
            os.makedirs(extract_dir, exist_ok=True)
            
            # Usar extração paralela para arquivos menores
            num_threads = max(1, int((os.cpu_count() or 4) * 0.75))
            if not extract_file_parallel(zip_path, extract_dir, num_threads):
                logger.error(f"[{pid}] Falha na extração paralela de {zip_path}")
                return False
            
            # Buscar arquivos de dados no diretório extraído
            data_files = []
            for root, _, files in os.walk(extract_dir):
                for file in files:
                    # Verificar extensão ou padrão do arquivo
                    file_path = os.path.join(root, file)
                    file_size = os.path.getsize(file_path)
                    
                    # Pular arquivos vazios
                    if file_size == 0:
                        continue
                    
                    # Verificar extensões que claramente não são de dados
                    invalid_extensions = ['.exe', '.dll', '.zip', '.rar', '.gz', '.tar', '.bz2', '.7z', '.png', '.jpg', '.jpeg', '.gif', '.pdf']
                    file_ext = os.path.splitext(file.lower())[1]
                    if file_ext in invalid_extensions:
                        continue
                    
                    # Adicionar à lista de arquivos para processar
                    data_files.append(file_path)
            
            if not data_files:
                logger.warning(f"[{pid}] Nenhum arquivo de dados encontrado em {extract_dir}")
                return False
            
            logger.info(f"[{pid}] Encontrados {len(data_files)} arquivos para processar")
            
            # Preparar diretório de saída
            output_dir = ensure_correct_folder_structure(path_parquet, remote_folder, 'socios')
            
            # Processar cada arquivo de dados diretamente em chunks
            chunk_counter = 0
            total_rows_processed = 0
            
            for data_path in data_files:
                data_file = os.path.basename(data_path)
                logger.debug(f"[{pid}] Processando arquivo: {data_file}")
                
                try:
                    # Processar arquivo em chunks diretamente
                    file_chunk_counter = process_data_file_in_chunks(
                        data_path, 
                        output_dir, 
                        zip_filename_prefix, 
                        chunk_counter
                    )
                    
                    if file_chunk_counter > 0:
                        chunk_counter += file_chunk_counter
                        logger.info(f"[{pid}] Arquivo {data_file} processado: {file_chunk_counter} chunks salvos")
                    else:
                        logger.warning(f"[{pid}] Nenhum chunk gerado para arquivo: {data_file}")
                        
                except Exception as e_data:
                    logger.error(f"[{pid}] Erro ao processar o arquivo {data_file}: {str(e_data)}")
            
            if chunk_counter == 0:
                logger.warning(f"[{pid}] Nenhum chunk válido gerado a partir do ZIP {zip_file}")
                return False
            
            logger.info(f"[{pid}] Processamento concluído para {zip_file}: {chunk_counter} chunks salvos")
            return True
            
        except Exception as e:
            logger.error(f"[{pid}] Erro processando {zip_file}: {str(e)}")
            return False
        finally:
            # Limpar diretório de extração
            if extract_dir and os.path.exists(extract_dir):
                try:
                    logger.debug(f"[{pid}] Limpando diretório de extração final: {extract_dir}")
                    shutil.rmtree(extract_dir)
                except Exception as e:
                    logger.warning(f"[{pid}] Erro ao limpar diretório de extração: {e}")
            
            # Forçar coleta de lixo novamente
            gc.collect()
        
        return success
    except Exception as e:
        logger.error(f"[{pid}] Erro processando {zip_file}: {str(e)}")
        return False

def process_socio_files(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """
    Processa todos os arquivos de sócio encontrados no diretório ZIP.
    """
    # Log detalhado dos recursos do sistema
    log_system_resources_socio()
    
    logger.info(f"Iniciando processamento de arquivos de sócio em {path_zip}")
    
    # Verificar se o diretório existe
    if not os.path.exists(path_zip):
        logger.error(f"Diretório não encontrado: {path_zip}")
        return False
    
    start_time = time.time()
    
    try:
        # Reset flag de parada
        _workers_should_stop.value = False
        
        zip_files = [f for f in os.listdir(path_zip)
                     if f.startswith('Soci') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Sócios encontrado.')
            return True
            
        logger.info(f"Encontrados {len(zip_files)} arquivos ZIP de sócios para processar")
        logger.info(f"Máximo de processos concorrentes: {_max_concurrent_processes.value}")
        
        # Registrar módulo no rastreador de progresso
        progress_tracker.register_module("socios", len(zip_files), _max_concurrent_processes.value)
        
        # Iniciar worker da fila
        logger.info("Iniciando worker de processamento...")
        worker_threads = start_queue_worker(path_zip, path_unzip, path_parquet, create_private)
        
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
                
                # O rastreador de progresso já está logando as atualizações detalhadas
                # Manter apenas log básico aqui para compatibilidade
                logger.debug(f"Progresso básico: {processed_count}/{len(zip_files)} arquivos processados ({progress_percent:.1f}%) - "
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
        for worker_thread in worker_threads:
            if worker_thread.is_alive():
                logger.info(f"Aguardando worker {worker_thread.name} finalizar...")
                worker_thread.join(timeout=30)
                if worker_thread.is_alive():
                    logger.warning(f"Worker {worker_thread.name} não finalizou no tempo esperado")
            
        # Calcular estatísticas finais
        total_time = time.time() - start_time
        
        # Usar o resumo final do rastreador de progresso
        progress_tracker.print_final_summary("socios")
        
        # Log adicional com informações específicas
        logger.info(f"Tempo total de processamento: {format_elapsed_time(total_time)}")
        logger.info(f"Tempo médio por arquivo: {total_time/len(zip_files):.2f}s")
        
        return True
        
    except Exception as e:
        logger.error(f'Erro no processamento principal de Sócios: {str(e)}')
        traceback.print_exc()
        return False
    finally:
        # Garantir que workers parem
        _workers_should_stop.value = True
        # Limpar dados de progresso do módulo
        progress_tracker.cleanup("socios")
