import logging
import os
import zipfile
import polars as pl
import numpy as np
import shutil
import traceback
import concurrent.futures
import time
import sys
import re
from typing import Tuple, List, Dict, Any, Optional
import gc
from multiprocessing import Pool
import datetime
import psutil
import threading
from queue import Queue
import tempfile

from ..config import config
from ..utils import file_delete, check_disk_space, verify_csv_integrity
from ..utils.folders import get_output_path, ensure_correct_folder_structure
from ..utils.time_utils import format_elapsed_time
import inspect

logger = logging.getLogger(__name__)


def process_estabelecimento(path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa arquivos de estabelecimentos."""
    return process_estabelecimento_files(path_zip, path_unzip, path_parquet, uf_subset)


def detect_file_encoding(file_path: str) -> str:
    """Detecta o encoding do arquivo tentando diferentes codificações."""
    encodings_to_try = ['latin1', 'utf-8', 'cp1252', 'utf-16']
    
    # Ler uma amostra do arquivo
    sample_size = 50000  # Aumentar tamanho da amostra para melhor detecção
    with open(file_path, 'rb') as f:
        raw_data = f.read(sample_size)
    
    # Tentar decodificar com cada encoding
    for enc in encodings_to_try:
        try:
            raw_data.decode(enc)
            logger.debug(f"Encoding detectado para {os.path.basename(file_path)}: {enc}")
            return enc
        except UnicodeDecodeError:
            continue
    
    # Se nenhum encoding funcionar, retornar latin1 como fallback
    logger.warning(f"Não foi possível detectar encoding para {os.path.basename(file_path)}. Usando latin1 como fallback.")
    return 'latin1'

def process_data_file(data_path: str, chunk_size: int = 1000000, output_dir: str = None, zip_filename_prefix: str = None):
    """Processa um arquivo de dados usando Polars com processamento em chunks para arquivos grandes. Salva cada chunk individualmente como Parquet."""
    logger = logging.getLogger(__name__)

    file_size = os.path.getsize(data_path)
    file_size_mb = file_size / (1024 * 1024)
    
    # Calcular RAM disponível (em MB)
    available_ram_mb = psutil.virtual_memory().available / (1024 * 1024)
    logger.info(f"RAM disponível: {available_ram_mb:.1f}MB")
    
    # Calcular chunk_size baseado no tamanho do arquivo e RAM disponível
    # Usar no máximo 25% da RAM disponível para cada chunk
    max_chunk_mb = available_ram_mb * 0.25
    
    # Estimar tamanho médio de cada linha (em bytes)
    # Assumir que cada linha tem em média 200 bytes (ajuste conforme necessário)
    avg_line_size = 200
    
    # Calcular número máximo de linhas por chunk baseado na RAM
    max_lines_per_chunk = int((max_chunk_mb * 1024 * 1024) / avg_line_size)
    
    # Ajustar chunk_size baseado no tamanho do arquivo
    if file_size_mb > 2000:  # Arquivos >2GB
        chunk_size = min(2000000, max_lines_per_chunk)  # Máximo 2M linhas ou 25% da RAM
    elif file_size_mb > 1000:  # Arquivos >1GB
        chunk_size = min(3000000, max_lines_per_chunk)  # Máximo 3M linhas ou 25% da RAM
    elif file_size_mb > 500:  # Arquivos >500MB
        chunk_size = min(4000000, max_lines_per_chunk)  # Máximo 4M linhas ou 25% da RAM
    else:  # Arquivos menores
        chunk_size = min(5000000, max_lines_per_chunk)  # Máximo 5M linhas ou 25% da RAM
    
    logger.info(f"Arquivo: {os.path.basename(data_path)}")
    logger.info(f"Tamanho do arquivo: {file_size_mb:.1f}MB")
    logger.info(f"RAM disponível: {available_ram_mb:.1f}MB")
    logger.info(f"Chunk size calculado: {chunk_size:,} linhas")
    
    # Se arquivo for menor que 500MB, processar normalmente
    if file_size_mb < 500:
        try:
            df_lazy = pl.scan_csv(
                data_path,
                separator=config.file.separator,
                encoding='utf8-lossy',
                has_header=False,
                new_columns=config.estabelecimento_columns,
                dtypes=config.estabelecimento_dtypes,
                rechunk=True
            )
            df = df_lazy.collect()
            if not df.is_empty():
                logger.info(f"Arquivo {os.path.basename(data_path)} processado com sucesso ({df.height} linhas, {file_size_mb:.1f}MB)")
                return df
            else:
                logger.warning(f"Arquivo {os.path.basename(data_path)} está vazio")
                return None
        except MemoryError as me:
            logger.error("="*60)
            logger.error(f"ERRO DE MEMÓRIA ao processar {os.path.basename(data_path)}: {me}")
            logger.error("Tente reduzir o chunk_size ou aumente a RAM disponível.")
            logger.error("="*60)
            return None
        except Exception as e:
            logger.error("="*60)
            logger.error(f"FALHA ao processar {os.path.basename(data_path)}")
            logger.error(f"Erro detalhado: {str(e)}", exc_info=True)
            logger.error("="*60)
            logger.error("Tentando leitura com ignore_errors=True...")
            try:
                df = pl.read_csv(
                    data_path,
                    separator=config.file.separator,
                    encoding='latin1',
                    has_header=False,
                    new_columns=config.estabelecimento_columns,
                    dtypes=config.estabelecimento_dtypes,
                    ignore_errors=True,
                    rechunk=True
                )
                logger.warning(f"Arquivo {os.path.basename(data_path)} lido com ignore_errors=True ({df.height} linhas)")
                return df
            except Exception as e2:
                logger.error("="*60)
                logger.error(f"Falha também com ignore_errors=True em {os.path.basename(data_path)}: {str(e2)}", exc_info=True)
                logger.error("="*60)
                return None
    else:
        logger.info(f"Arquivo grande detectado ({file_size_mb:.1f}MB). Processando em chunks de {chunk_size:,} linhas...")
        try:
            df_lazy = pl.scan_csv(
                data_path,
                separator=config.file.separator,
                encoding='utf8-lossy',
                has_header=False,
                new_columns=config.estabelecimento_columns,
                dtypes=config.estabelecimento_dtypes
            )
            total_rows = 0
            batch_size = chunk_size
            offset = 0
            chunk_count = 0
            saved_chunks = 0
            failed_chunks = 0
            while True:
                chunk_lazy = df_lazy.slice(offset, batch_size)
                try:
                    chunk_df = chunk_lazy.collect()
                except MemoryError as me:
                    logger.error("="*60)
                    logger.error(f"ERRO DE MEMÓRIA ao coletar chunk {chunk_count+1} de {os.path.basename(data_path)}: {me}")
                    logger.error(f"Chunk atual: offset={offset}, batch_size={batch_size}")
                    logger.error("Tente reduzir o chunk_size ou aumente a RAM disponível.")
                    logger.error("="*60)
                    failed_chunks += 1
                    break
                except Exception as e:
                    logger.error("="*60)
                    logger.error(f"Erro ao coletar chunk {chunk_count+1} de {os.path.basename(data_path)}: {e}", exc_info=True)
                    logger.error(f"Chunk atual: offset={offset}, batch_size={batch_size}")
                    logger.error("="*60)
                    failed_chunks += 1
                    break
                if chunk_df.is_empty():
                    break
                total_rows += chunk_df.height
                chunk_count += 1
                logger.info(f"Chunk {chunk_count} processado: {chunk_df.height} linhas (total: {total_rows:,})")
                if output_dir and zip_filename_prefix:
                    output_path = os.path.join(output_dir, f"{zip_filename_prefix}_chunk{chunk_count:03d}.parquet")
                    try:
                        chunk_df.write_parquet(output_path, compression="lz4")
                        logger.info(f"Chunk {chunk_count} salvo em {output_path}")
                        saved_chunks += 1
                    except Exception as e:
                        logger.error(f"Erro ao salvar chunk {chunk_count} em {output_path}: {e}", exc_info=True)
                        failed_chunks += 1
                del chunk_df
                gc.collect()
                offset += batch_size
                if chunk_count > 100:
                    logger.warning(f"Limite de chunks atingido (100). Parando processamento.")
                    break
            if saved_chunks > 0 and failed_chunks == 0:
                logger.info(f"Processamento em chunks concluído: {saved_chunks} chunks salvos para {os.path.basename(data_path)}")
                return True  # Sucesso total
            elif saved_chunks == 0:
                logger.warning(f"Nenhum chunk válido salvo para {os.path.basename(data_path)}")
                return False
            else:
                logger.error(f"{failed_chunks} chunks falharam ao serem salvos para {os.path.basename(data_path)}")
                return False
        except MemoryError as me:
            logger.error("="*60)
            logger.error(f"ERRO DE MEMÓRIA no processamento em chunks de {os.path.basename(data_path)}: {me}")
            logger.error("Tente reduzir o chunk_size ou aumente a RAM disponível.")
            logger.error("="*60)
            return False
        except Exception as e:
            logger.error("="*60)
            logger.error(f"FALHA no processamento em chunks de {os.path.basename(data_path)}: {e}", exc_info=True)
            logger.error("="*60)
            return False


def apply_estabelecimento_transformations(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações específicas para Estabelecimentos usando Polars de forma otimizada com lazy evaluation."""
    logger.info("Aplicando transformações em Estabelecimentos...")
    
    # STREAMING NO POLARS:
    # O streaming permite processar datasets maiores que a RAM disponível
    # dividindo automaticamente o processamento em chunks menores.
    # 
    # Benefícios do Streaming:
    # 1. Processa arquivos maiores que a memória RAM
    # 2. Reduz picos de uso de memória
    # 3. Permite paralelização automática
    # 4. Otimiza operações de I/O
    #
    # Como funciona:
    # - O Polars divide o dataset em chunks menores
    # - Processa cada chunk independentemente
    # - Combina os resultados automaticamente
    # - Usa lazy evaluation para otimizar o plano de execução
    
    # Configurar streaming chunk size para otimizar memória
    # Chunks menores = menos memória, mas mais overhead
    # Chunks maiores = mais memória, mas melhor performance
    pl.Config.set_streaming_chunk_size(500000)  # 500k linhas por chunk
    
    # Definir colunas que devem ser removidas (não são necessárias)
    columns_to_drop = [
        'cnpj_ordem',           # Usado apenas para formar CNPJ completo
        'cnpj_dv',              # Usado apenas para formar CNPJ completo
        'tipo_logradouro',      # Detalhamento desnecessário
        'logradouro',           # Detalhamento desnecessário
        'numero',               # Detalhamento desnecessário
        'complemento',          # Detalhamento desnecessário
        'bairro',               # Detalhamento desnecessário
        'ddd1',                 # Telefone 1 não é necessário
        'telefone1',            # Telefone 1 não é necessário
        'ddd2',                 # Telefone 2 não é necessário
        'telefone2',            # Telefone 2 não é necessário
        'ddd_fax',              # Fax não é necessário
        'fax',                  # Fax não é necessário
        'pais',                 # Informação desnecessária
        'correio_eletronico',   # Será renomeado para email antes da remoção
        'situacao_especial',    # Raramente usado
        'data_situacao_especial', # Raramente usado
        'nome_cidade_exterior'  # Raramente usado
    ]
    
    try:
        # Usar lazy evaluation para permitir streaming automático
        df_lazy = df.lazy()
        
        # 1. CRIAR CNPJ COMPLETO antes de remover as partes
        if all(col in df.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
            df_lazy = df_lazy.with_columns([
                (
                    pl.col('cnpj_basico').cast(pl.Utf8).str.pad_start(8, '0') + 
                    pl.col('cnpj_ordem').cast(pl.Utf8).str.pad_start(4, '0') + 
                    pl.col('cnpj_dv').cast(pl.Utf8).str.pad_start(2, '0')
                ).alias('cnpj')
            ])
        
        # 2. APLICAR TRANSFORMAÇÕES DE DADOS
        transformations = []
        
        # Conversões de tipos otimizadas (apenas para colunas que existem)
        if "matriz_filial" in df.columns:
            transformations.append(pl.col("matriz_filial").cast(pl.Int8, strict=False))
        if "codigo_situacao_cadastral" in df.columns:
            transformations.append(pl.col("codigo_situacao_cadastral").cast(pl.Int8, strict=False))
        if "codigo_motivo_situacao_cadastral" in df.columns:
            transformations.append(pl.col("codigo_motivo_situacao_cadastral").cast(pl.Int16, strict=False))
        if "codigo_municipio" in df.columns:
            transformations.append(pl.col("codigo_municipio").cast(pl.Int32, strict=False))
        if "codigo_cnae" in df.columns:
            transformations.append(pl.col("codigo_cnae").cast(pl.Int32, strict=False))
        if "cep" in df.columns:
            transformations.append(pl.col("cep").str.replace_all(r"[^\d]", "", literal=False))
        
        # Conversões de data otimizadas
        date_columns = ["data_situacao_cadastral", "data_inicio_atividades"]
        for date_col in date_columns:
            if date_col in df.columns:
                transformations.append(
                    pl.when(
                        pl.col(date_col).is_in(['0', '00000000', '']) | 
                        pl.col(date_col).is_null()
                    )
                    .then(None)
                    .otherwise(pl.col(date_col))
                    .str.strptime(pl.Date, "%Y%m%d", strict=False)
                    .alias(date_col)
                )
        
        # Aplicar todas as transformações se houver alguma
        if transformations:
            df_lazy = df_lazy.with_columns(transformations)
        
        # 3. REMOVER COLUNAS DESNECESSÁRIAS
        # Filtrar apenas colunas que realmente existem no DataFrame
        cols_to_drop = [col for col in columns_to_drop if col in df.columns]
        if cols_to_drop:
            logger.info(f"Removendo {len(cols_to_drop)} colunas desnecessárias: {', '.join(cols_to_drop)}")
            df_lazy = df_lazy.drop(cols_to_drop)
        
        # 4. COLETAR RESULTADO COM STREAMING
        df_transformed = df_lazy.collect(streaming=True)
        
        logger.info(f"Transformações aplicadas com sucesso usando streaming ({df_transformed.height} linhas, {df_transformed.width} colunas)")
        return df_transformed
        
    except Exception as e:
        logger.error(f"Erro ao aplicar transformações com streaming: {str(e)}")
        logger.warning("Tentando aplicar transformações sem streaming...")
        
        # Fallback sem streaming - versão simplificada
        try:
            # Criar CNPJ completo
            if all(col in df.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
                df = df.with_columns([
                    (
                        pl.col('cnpj_basico').cast(pl.Utf8).str.pad_start(8, '0') + 
                        pl.col('cnpj_ordem').cast(pl.Utf8).str.pad_start(4, '0') + 
                        pl.col('cnpj_dv').cast(pl.Utf8).str.pad_start(2, '0')
                    ).alias('cnpj')
                ])
            
            # Remover colunas desnecessárias
            cols_to_drop = [col for col in columns_to_drop if col in df.columns]
            if cols_to_drop:
                df = df.drop(cols_to_drop)
            
            logger.info(f"Transformações aplicadas com fallback ({df.height} linhas, {df.width} colunas)")
            return df
            
        except Exception as e2:
            logger.error(f"Erro também no fallback: {str(e2)}")
            return df


def create_parquet(df: pl.DataFrame, table_name: str, path_parquet: str, zip_filename_prefix: str, partition_size: int = 2000000) -> bool:
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
            
            # Log apenas para partições múltiplas ou em caso de erro
            if num_partitions > 1:
                logger.debug(f"Salvando partição {i+1}/{num_partitions} com {end_idx-start_idx} linhas")
            
            try:
                partition.write_parquet(output_path, compression="lz4")
            except Exception as e:
                logger.error(f"Erro ao salvar partição {i+1}: {str(e)}")
                raise
            
            # Liberar memória
            del partition
            gc.collect()
        
        logger.info(f"Parquet salvo com sucesso: {num_partitions} partições")
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
                logger.info(f"Tempo de processamento do chunk {i//chunk_size + 1}: {format_elapsed_time(chunk_time)}")
                
            # Registrar tempo total
            total_time = time.time() - start_time
            logger.info(f"Tempo total de processamento: {format_elapsed_time(total_time)}")
            return True
            
    except Exception as e:
        logger.error(f"Erro na extração do ZIP: {str(e)}")
        return False


def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa um único arquivo ZIP."""
    pid = os.getpid()
    logger.info(f"[{pid}] Iniciando processamento para: {zip_file}")
    extract_dir = ""
    success = False
    zip_filename_prefix = os.path.splitext(zip_file)[0]
    
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
        zip_path = os.path.join(path_zip, zip_file)
        extract_dir = os.path.join(path_unzip, os.path.splitext(zip_file)[0])
        
        # Limpar diretório de extração se já existir
        if os.path.exists(extract_dir):
            logger.debug(f"[{pid}] Removendo diretório de extração existente: {extract_dir}")
            shutil.rmtree(extract_dir)
        
        # Criar diretório de extração
        os.makedirs(extract_dir, exist_ok=True)
        
        # Verificar se o arquivo ZIP existe e tem tamanho válido
        if not os.path.exists(zip_path):
            logger.error(f"[{pid}] Arquivo ZIP {zip_path} não existe")
            return False
            
        file_size = os.path.getsize(zip_path)
        if file_size == 0:
            logger.error(f"[{pid}] Arquivo ZIP {zip_path} está vazio (tamanho 0 bytes)")
            return False
            
        # Verificar se é um arquivo ZIP válido antes de tentar extrair
        try:
            with zipfile.ZipFile(zip_path, 'r') as test_zip:
                file_list = test_zip.namelist()
                if not file_list:
                    logger.warning(f"[{pid}] Arquivo ZIP {zip_path} existe mas não contém arquivos")
                    return False
                else:
                    logger.info(f"[{pid}] Arquivo ZIP {zip_path} é válido e contém {len(file_list)} arquivos")
        except zipfile.BadZipFile:
            logger.error(f"[{pid}] Arquivo {zip_path} não é um arquivo ZIP válido")
            return False
        except Exception as e_test_zip:
            logger.error(f"[{pid}] Erro ao verificar arquivo ZIP {zip_path}: {str(e_test_zip)}")
            return False
        
        # Extrair arquivo usando método apropriado baseado no tamanho
        logger.info(f"[{pid}] Extraindo {zip_file} para {extract_dir}")
        try:
            file_size_mb = file_size / (1024 * 1024)
            extract_start = time.time()
            
            if file_size_mb > 1000:  # Arquivos >1GB
                # Usar extração em chunks para arquivos grandes
                chunk_size = 1000  # Processar 1000 arquivos por vez
                if not extract_large_zip(zip_path, extract_dir, chunk_size):
                    logger.error(f"[{pid}] Falha na extração em chunks de {zip_path}")
                    return False
            else:
                # Usar extração paralela para arquivos menores
                num_threads = max(1, int((os.cpu_count() or 4) * 0.75))
                if not extract_file_parallel(zip_path, extract_dir, num_threads):
                    logger.error(f"[{pid}] Falha na extração paralela de {zip_path}")
                    return False
                
            # Registrar tempo total de extração
            extract_time = time.time() - extract_start
            logger.info(f"[{pid}] Tempo de extração: {format_elapsed_time(extract_time)}")
                
            # Verificar se os arquivos foram extraídos
            extracted_files = os.listdir(extract_dir)
            if not extracted_files:
                logger.error(f"[{pid}] Nenhum arquivo foi extraído para {extract_dir}")
                return False
            logger.info(f"[{pid}] Arquivos extraídos: {extracted_files}")
                
        except Exception as e_zip:
            logger.error(f"[{pid}] Erro durante a extração do ZIP {zip_path}: {str(e_zip)}")
            logger.error(f"[{pid}] Tipo do erro: {type(e_zip).__name__}")
            logger.error(f"[{pid}] Detalhes do erro: {traceback.format_exc()}")
            return False
        
        # Limpar memória antes de começar processamento
        gc.collect()
        
        # Procurar arquivos de dados na pasta de extração
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
                # Essas extensões são apenas exemplos - ajuste conforme necessário
                invalid_extensions = ['.exe', '.dll', '.zip', '.rar', '.gz', '.tar', '.bz2', '.7z', '.png', '.jpg', '.jpeg', '.gif', '.pdf']
                file_ext = os.path.splitext(file.lower())[1]
                if file_ext in invalid_extensions:
                    continue
                
                # Adicionar à lista de arquivos para processar
                data_files.append(file_path)
        
        if not data_files:
            logger.warning(f"[{pid}] Nenhum arquivo de dados encontrado em {extract_dir}")
            # Retorna False pois não há dados para processar (não é um sucesso)
            return False
        
        logger.info(f"[{pid}] Encontrados {len(data_files)} arquivos para processar")
        
        # Processar cada arquivo de dados
        dataframes = []
        chunks_saved = False
        for data_path in data_files:
            data_file = os.path.basename(data_path)
            logger.debug(f"[{pid}] Processando arquivo: {data_file}")
            
            # Processar arquivo de dados
            logger.info(f"Processando arquivo: {data_file}")
            
            # Usar chunk_size configurável baseado no tamanho do arquivo
            file_size_mb = os.path.getsize(data_path) / (1024 * 1024)
            if file_size_mb > 1000:  # Arquivos >1GB
                chunk_size = 500000   # Chunks menores para arquivos gigantes
            elif file_size_mb > 500:  # Arquivos >500MB
                chunk_size = 1000000  # Chunks médios
            else:
                chunk_size = 2000000  # Chunks maiores para arquivos menores
            
            logger.info(f"Arquivo {data_file}: {file_size_mb:.1f}MB - usando chunks de {chunk_size:,} linhas")
            
            # Passar output_dir e zip_filename_prefix para salvar chunks individualmente
            df = process_data_file(
                data_path,
                chunk_size=chunk_size,
                output_dir=ensure_correct_folder_structure(path_parquet, remote_folder, 'estabelecimentos'),
                zip_filename_prefix=zip_filename_prefix
            )
            # Se df é True, significa que os chunks foram salvos com sucesso
            if df is True:
                logger.info(f"[{pid}] Chunks salvos individualmente para {data_file}, não será necessário salvar Parquet novamente.")
                chunks_saved = True
                continue
            # Se df é False, significa que houve erro no processamento dos chunks
            elif df is False:
                logger.error(f"[{pid}] Erro ao processar chunks para {data_file}")
                return False
            # Se df é um DataFrame, verificar se está vazio
            elif df is not None and not df.is_empty():
                dataframes.append(df)
            else:
                logger.error(f"[{pid}] DataFrame vazio ou None gerado para {data_file}")
                return False

        if not dataframes and not chunks_saved:
            logger.error(f"[{pid}] Nenhum DataFrame válido gerado e nenhum chunk salvo de {zip_file}")
            return False
        
        # Se temos chunks salvos mas não temos DataFrames, consideramos sucesso
        if chunks_saved and not dataframes:
            logger.info(f"[{pid}] Processamento concluído com sucesso via chunks para {zip_file}")
            return True
        
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
                logger.warning(f"[{pid}] DataFrame final vazio após concatenação para o ZIP {zip_file}")
                del df_final
                gc.collect()
                return True
            
            logger.info(f"[{pid}] Aplicando transformações em {df_final.height} linhas para {zip_file}...")
            df_transformed = apply_estabelecimento_transformations(df_final)
            
            # --- 3. Salvar Parquet (Principal) ---
            main_saved = False
            
            # Usar diretamente path_parquet sem adicionar a pasta remota
            # A função ensure_correct_folder_structure já adiciona a pasta remota se necessário
            logger.info(f"[{pid}] Usando path_parquet direto: {path_parquet}")
            
            try:
                # Usamos diretamente path_parquet e a função ensure_correct_folder_structure dentro de create_parquet
                # vai garantir a estrutura correta de pastas
                main_saved = create_parquet(df_transformed, 'estabelecimentos', path_parquet, zip_filename_prefix)
                logger.info(f"[{pid}] Parquet principal salvo com sucesso para {zip_file}")
            except Exception as e_parq_main:
                logger.error(f"[{pid}] Erro ao salvar Parquet principal para {zip_file}: {e_parq_main}")
                # Continua para tentar salvar subset se houver

            # --- 4. Salvar Parquet (Subset UF - Condicional) ---
            subset_saved = True # Assume sucesso se não for criar ou se falhar principal
            if main_saved and uf_subset and 'uf' in df_transformed.columns:
                subset_table_name = f"estabelecimentos_{uf_subset.lower()}"
                logger.info(f"[{pid}] Criando subset {subset_table_name}...")
                df_subset = df_transformed.filter(pl.col('uf') == uf_subset)
                
                # Apenas tentamos salvar se o subset não estiver vazio
                if not df_subset.is_empty():
                    try:
                        # Usamos diretamente path_parquet aqui também
                        subset_saved = create_parquet(df_subset, subset_table_name, path_parquet, zip_filename_prefix)
                        logger.info(f"[{pid}] Subset {subset_table_name} salvo com sucesso para {zip_file}")
                    except Exception as e_parq_subset:
                        logger.error(f"[{pid}] Falha ao salvar subset {subset_table_name} para {zip_file}: {e_parq_subset}")
                        subset_saved = False
                else:
                    logger.warning(f"[{pid}] Subset para UF {uf_subset} está vazio. Pulando.")
                    
                # Liberar memória
                del df_subset
                gc.collect()
            elif main_saved and uf_subset:
                logger.warning(f"[{pid}] Coluna 'uf' não encontrada, não é possível criar subset para UF {uf_subset}.")
                
            # Liberar memória
            del df_transformed
            del df_final
            gc.collect()
            
            success = main_saved and subset_saved
            return success
            
        except Exception as e_concat_transform:
            logger.error(f"[{pid}] Erro durante concatenação ou transformação para {zip_file}: {e_concat_transform}")
            return False
            
    except Exception as e:
        logger.error(f"[{pid}] Erro processando {zip_file}: {str(e)}")
        return False
    finally:
        # Limpar diretório de extração
        if extract_dir and os.path.exists(extract_dir):
            try:
                logger.debug(f"[{pid}] Limpando diretório de extração final: {extract_dir}")
                shutil.rmtree(extract_dir)
            except Exception as e_clean:
                logger.warning(f"[{pid}] Erro ao limpar diretório de extração: {e_clean}")
        
        # Forçar coleta de lixo novamente
        gc.collect()
    
    return success


def process_estabelecimento_files(path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa todos os arquivos de estabelecimentos de um diretório."""
    logger.info("Iniciando processamento de arquivos de estabelecimentos...")
    
    # ===== CONFIGURAÇÕES GLOBAIS DE STREAMING E PERFORMANCE =====
    # Configurar Polars para uso otimizado de memória e streaming
    
    # 1. STREAMING: Permite processar datasets maiores que a RAM
    # Aumentar chunk size para melhor performance com arquivos grandes
    pl.Config.set_streaming_chunk_size(1000000)  # 1M linhas por chunk
    
    # 2. PARALELISMO: Usar 80% dos CPUs para permitir uso em paralelo
    available_cpus = os.cpu_count() or 4
    max_threads = max(1, int(available_cpus * 0.80))
    
    # 3. MEMÓRIA: Configurações para otimizar uso de memória
    pl.Config.set_tbl_rows(20)  # Limitar linhas mostradas em prints
    pl.Config.set_tbl_cols(10)  # Limitar colunas mostradas em prints
    
    # 4. CACHE: Configurar cache para operações repetitivas
    pl.Config.set_auto_structify(True)  # Otimizar estruturas automaticamente
    
    # 5. PERFORMANCE: Configurações adicionais para melhor performance
    pl.Config.set_fmt_str_lengths(100)  # Limitar tamanho de strings em logs
    pl.Config.set_fmt_float("full")     # Formato completo para números
    
    logger.info(f"Configurações de streaming aplicadas:")
    logger.info(f"  - Chunk size: 1.000.000 linhas")
    logger.info(f"  - CPUs disponíveis: {available_cpus} (usando {max_threads} threads)")
    logger.info(f"  - Streaming habilitado para arquivos grandes")
    logger.info(f"  - Processamento em chunks otimizado para RAM disponível")
    
    # ===== INÍCIO DO PROCESSAMENTO =====
    start_time = time.time()
    
    try:
        zip_files = [f for f in os.listdir(path_zip) 
                     if f.startswith('Estabele') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Estabelecimentos encontrado.')
            return True
        
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
        
        logger.info(f"Pasta remota identificada: {remote_folder}")
        
        # Usar ensure_correct_folder_structure para criar o caminho correto
        output_dir_main = ensure_correct_folder_structure(path_parquet, remote_folder, 'estabelecimentos')
            
        # LIMPEZA PRÉVIA do diretório de saída principal
        try:
            file_delete(output_dir_main)
            logger.info(f'Diretório {output_dir_main} limpo antes do processamento.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretório de saída {output_dir_main}: {str(e)}')
        
        # Limpar também o diretório de subset de UF se especificado
        if uf_subset:
            uf_subset = uf_subset.upper()  # Garante caixa alta
            subset_table_name = f"estabelecimentos_{uf_subset.lower()}"
            subset_dir = ensure_correct_folder_structure(path_parquet, remote_folder, subset_table_name)
            try:
                file_delete(subset_dir)
                logger.info(f'Diretório de subset {subset_dir} limpo antes do processamento.')
            except Exception as e:
                logger.warning(f'Não foi possível limpar diretório de subset {subset_dir}: {str(e)}')
        
        # Criar diretórios se não existirem
        os.makedirs(output_dir_main, exist_ok=True)
        if uf_subset:
            os.makedirs(subset_dir, exist_ok=True)
        
        # Forçar coleta de lixo antes de iniciar o processamento
        gc.collect()
        
        # Processar arquivos sequencialmente para evitar problemas de memória
        success = False
        arquivos_com_falha = []
        total_files = len(zip_files)
        completed_files = 0
        
        logger.info(f"Processando {total_files} arquivos de estabelecimentos sequencialmente...")
        
        for zip_file in zip_files:
            completed_files += 1
            elapsed_time = time.time() - start_time
            
            # Calcular métricas de progresso
            progress_pct = (completed_files / total_files) * 100
            avg_time_per_file = elapsed_time / completed_files if completed_files > 0 else 0
            estimated_remaining = avg_time_per_file * (total_files - completed_files)
            
            try:
                result = process_single_zip(zip_file, path_zip, path_unzip, path_parquet, uf_subset)
                if result:
                    success = True
                    logger.info(f"[{completed_files}/{total_files}] ({progress_pct:.1f}%) Arquivo {zip_file} processado com sucesso. "
                                f"Tempo médio: {avg_time_per_file:.1f}s/arquivo. "
                                f"Tempo estimado restante: {estimated_remaining:.1f}s")
                else:
                    arquivos_com_falha.append(zip_file)
                    logger.warning(f"[{completed_files}/{total_files}] ({progress_pct:.1f}%) Falha no processamento do arquivo {zip_file}")
            except Exception as e:
                arquivos_com_falha.append(zip_file)
                logger.error(f"[{completed_files}/{total_files}] ({progress_pct:.1f}%) Exceção no processamento do arquivo {zip_file}: {str(e)}")
                logger.error(traceback.format_exc())
        
        # Calcular estatísticas finais
        total_time = time.time() - start_time
        
        if not success:
            logger.warning('Nenhum arquivo ZIP de Estabelecimentos foi processado com sucesso.')
            
        if arquivos_com_falha:
            logger.warning(f'Os seguintes arquivos falharam no processamento: {", ".join(arquivos_com_falha)}')
        
        # Logar resumo completo
        logger.info("=" * 50)
        logger.info("RESUMO DO PROCESSAMENTO DE ESTABELECIMENTOS:")
        logger.info("=" * 50)
        logger.info(f"Arquivos processados com sucesso: {completed_files - len(arquivos_com_falha)}/{total_files}")
        logger.info(f"Arquivos com falha: {len(arquivos_com_falha)}/{total_files}")
        logger.info(f"Tempo total de processamento: {format_elapsed_time(total_time)}")
        logger.info(f"Tempo médio por arquivo: {total_time/completed_files if completed_files > 0 else 0:.2f} segundos")
        logger.info("=" * 50)
        
        # Verificar se pelo menos um arquivo foi processado com sucesso
        return success
    except Exception as e:
        logger.error(f'Erro no processamento principal de Estabelecimentos: {str(e)}')
        traceback.print_exc()
        return False
