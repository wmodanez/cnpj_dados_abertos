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

from ..config import config
from ..utils import (
    file_delete, check_disk_space, estimate_zip_extracted_size,
    process_csv_files_parallel, verify_csv_integrity, 
    create_parquet_filename
)
from ..utils.folders import get_output_path, ensure_correct_folder_structure
import inspect

logger = logging.getLogger(__name__)

# Variáveis globais para controle de recursos
_processing_lock = Lock()
_active_processes = Value('i', 0)
_max_concurrent_processes = Value('i', 2)  # Máximo de 2 processamentos simultâneos
_process_queue = PriorityQueue()

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

def process_queue_worker(path_zip: str, path_unzip: str, path_parquet: str):
    """Worker que processa a fila de arquivos."""
    while True:
        try:
            if _process_queue.empty():
                time.sleep(5)  # Espera 5 segundos se a fila estiver vazia
                continue
                
            if not can_start_processing():
                time.sleep(10)  # Espera 10 segundos se não puder processar
                continue
                
            # Pega o próximo arquivo da fila
            priority, timestamp, zip_file = _process_queue.get()
            
            with _processing_lock:
                _active_processes.value += 1
                
            try:
                # Processa o arquivo
                process_single_zip(zip_file, path_zip, path_unzip, path_parquet)
            finally:
                with _processing_lock:
                    _active_processes.value -= 1
                    
        except Exception as e:
            logger.error(f"Erro no worker da fila: {str(e)}")
            time.sleep(5)

def start_queue_worker(path_zip: str, path_unzip: str, path_parquet: str):
    """Inicia o worker da fila em uma thread separada."""
    worker_thread = threading.Thread(target=process_queue_worker, args=(path_zip, path_unzip, path_parquet), daemon=True)
    worker_thread.start()
    return worker_thread

def process_socio(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios."""
    return process_socio_files(path_zip, path_unzip, path_parquet)

def process_data_file(data_path: str):
    """
    Processa um arquivo de dados usando Polars, seja ele CSV ou outro formato de texto.
    
    Args:
        data_path: Caminho para o arquivo
        
    Returns:
        DataFrame Polars ou None em caso de erro
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
    original_column_names = config.socio_columns

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

def apply_socio_transformations(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações específicas para Sócios usando Polars."""
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

def create_parquet(df: pl.DataFrame, table_name: str, path_parquet: str, zip_filename_prefix: str, partition_size: int = 500000) -> bool:
    """
    Salva DataFrame Polars em arquivos Parquet particionados.
    
    Args:
        df: DataFrame Polars
        table_name: Nome da tabela (subpasta)
        path_parquet: Caminho base para os arquivos parquet
        zip_filename_prefix: Prefixo derivado do nome do arquivo ZIP original
        partition_size: Tamanho de cada partição (número de linhas)
        
    Returns:
        True se sucesso, False caso contrário
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
                        # Último recurso: usar um valor padrão
                        current_date = datetime.datetime.now()
                        remote_folder = f"{current_date.year}-{current_date.month:02d}"
                        logger.warning(f"Não foi possível extrair pasta remota do caminho. Usando data atual: {remote_folder}")
        
        logger.info(f"Pasta remota identificada: {remote_folder}")
        
        # Forçar a utilização do remote_folder para garantir que não salve na raiz do parquet
        # Usando a função que garante a estrutura correta de pastas
        output_dir = ensure_correct_folder_structure(path_parquet, remote_folder, table_name)
        
        total_rows = df.height
        if total_rows == 0:
            logger.warning(f"DataFrame '{table_name}' (Origem: {zip_filename_prefix}) está vazio. Nenhum Parquet será salvo.")
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
        logger.error(f"Erro ao criar arquivo Parquet: {str(e)}")
        return False

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
                logger.info(f"Chunk {i//chunk_size + 1} extraído em {chunk_time:.2f} segundos")
                
            # Registrar tempo total
            total_time = time.time() - start_time
            logger.info(f"Extração concluída: {total_files} arquivos extraídos em {total_time:.2f} segundos")
            return True
            
    except Exception as e:
        logger.error(f"Erro na extração do ZIP: {str(e)}")

def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, remote_folder: str = None) -> bool:
    """Processa um único arquivo ZIP."""
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
            
            # Processar cada arquivo de dados
            dataframes = []
            for data_path in data_files:
                data_file = os.path.basename(data_path)
                logger.debug(f"[{pid}] Processando arquivo: {data_file}")
                
                try:
                    df = process_data_file(data_path)
                    if df is not None and not df.is_empty():
                        logger.info(f"[{pid}] Arquivo {data_file} processado com sucesso: {df.height} linhas")
                        dataframes.append(df)
                    elif df is not None and df.is_empty():
                        logger.warning(f"[{pid}] DataFrame Polars vazio para arquivo: {data_file}")
                except Exception as e:
                    logger.error(f"[{pid}] Erro ao processar o arquivo {data_file} com Polars: {str(e)}")
            
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
                df_transformed = apply_socio_transformations(df_final)
                
                # Salvar Parquet
                main_saved = create_parquet(
                    df_transformed, 
                    'socios', 
                    path_parquet, 
                    zip_filename_prefix
                )
                logger.info(f"[{pid}] Parquet salvo com sucesso para {zip_file}")
                
                # Liberar memória
                del df_transformed
                del df_final
                gc.collect()
                
                success = main_saved
                return success
                
            except Exception as e:
                logger.error(f"[{pid}] Erro durante concatenação ou transformação Polars para {zip_file}: {e}")
                return False
            
        except Exception as e:
            logger.error(f"[{pid}] Erro processando {zip_file} com Polars: {str(e)}")
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
        logger.error(f"[{pid}] Erro processando {zip_file} com Polars: {str(e)}")
        return False

def process_socio_files(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios."""
    start_time = time.time()
    
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de SÓCIOS')
    logger.info('=' * 50)
    
    try:
        zip_files = [f for f in os.listdir(path_zip)
                     if f.startswith('Socio') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Sócios encontrado.')
            return True
            
        # Iniciar worker da fila
        worker_thread = start_queue_worker(path_zip, path_unzip, path_parquet)
        
        # Adicionar arquivos à fila
        for zip_file in zip_files:
            add_to_process_queue(zip_file)
            
        # Aguardar processamento da fila
        while not _process_queue.empty() or _active_processes.value > 0:
            time.sleep(5)
            
        # Calcular estatísticas finais
        total_time = time.time() - start_time
        
        logger.info("=" * 50)
        logger.info("RESUMO DO PROCESSAMENTO DE SÓCIOS:")
        logger.info("=" * 50)
        logger.info(f"Tempo total de processamento: {total_time:.2f} segundos")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f'Erro no processamento principal de Sócios: {str(e)}')
        traceback.print_exc()
        return False
