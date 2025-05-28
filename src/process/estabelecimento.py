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

# Variáveis globais para controle de recursos
_processing_lock = threading.Lock()
_active_processes = threading.local()
# Usar pelo menos metade dos núcleos do processador
_max_concurrent_processes = max(2, (os.cpu_count() or 4) // 2)

def log_system_resources_estabelecimento():
    """Log detalhado dos recursos do sistema para processamento de estabelecimentos."""
    cpu_count = os.cpu_count() or 4
    memory_info = psutil.virtual_memory()
    memory_total_gb = memory_info.total / (1024**3)
    memory_available_gb = memory_info.available / (1024**3)
    memory_percent = memory_info.percent
    
    max_workers = _max_concurrent_processes
    
    logger.info("=" * 50)
    logger.info("🏪 MÓDULO ESTABELECIMENTO - CONFIGURAÇÃO DE RECURSOS")
    logger.info("=" * 50)
    
    # Recursos do sistema
    logger.info(f"🖥️  RECURSOS DO SISTEMA:")
    logger.info(f"   • CPU: {cpu_count} núcleos disponíveis")
    logger.info(f"   • Memória: {memory_total_gb:.2f}GB total | {memory_available_gb:.2f}GB disponível ({100-memory_percent:.1f}%)")
    
    # Informações de disco
    try:
        if os.name == 'nt':  # Windows
            disk_path = os.path.splitdrive(os.getcwd())[0] + '\\'
        else:  # Unix/Linux
            disk_path = '/'
        
        disk_info = psutil.disk_usage(disk_path)
        disk_total_gb = disk_info.total / (1024**3)
        disk_free_gb = disk_info.free / (1024**3)
        disk_percent = (disk_info.used / disk_info.total) * 100
        logger.info(f"   • Disco: {disk_total_gb:.2f}GB total | {disk_free_gb:.2f}GB livres ({100-disk_percent:.1f}%)")
    except Exception as e:
        logger.warning(f"   • Disco: Erro ao obter informações ({e})")
    
    # Configurações de processamento
    logger.info(f"⚙️  CONFIGURAÇÕES DE PROCESSAMENTO:")
    logger.info(f"   • Workers máximos: {max_workers}")
    logger.info(f"   • Estratégia: Pelo menos {cpu_count // 2} workers (50% dos núcleos)")
    logger.info(f"   • Tipo de processamento: Estabelecimentos (arquivos grandes)")
    logger.info(f"   • Otimização: Processamento em chunks para arquivos >500MB")
    
    # Estimativas de performance
    estimated_throughput = max_workers * 2  # Estimativa conservadora para estabelecimentos
    estimated_memory_per_worker = memory_available_gb / max_workers if max_workers > 0 else 0
    
    logger.info(f"📊 ESTIMATIVAS DE PERFORMANCE:")
    logger.info(f"   • Throughput estimado: ~{estimated_throughput} arquivos/hora")
    logger.info(f"   • Memória por worker: ~{estimated_memory_per_worker:.1f}GB")
    logger.info(f"   • Eficiência de CPU: {(max_workers/cpu_count)*100:.1f}%")
    
    # Alertas específicos para estabelecimentos
    logger.info(f"⚠️  ALERTAS ESPECÍFICOS:")
    alerts_count = 0
    if memory_percent > 85:
        logger.warning(f"   • CRÍTICO: Uso de memória muito alto ({memory_percent:.1f}%) - estabelecimentos requerem muita RAM")
        alerts_count += 1
    elif memory_percent > 70:
        logger.warning(f"   • ATENÇÃO: Uso de memória alto ({memory_percent:.1f}%) - monitorar durante processamento")
        alerts_count += 1
    
    if memory_total_gb < 8:
        logger.warning(f"   • ATENÇÃO: Pouca RAM ({memory_total_gb:.1f}GB) - estabelecimentos podem ser lentos")
        alerts_count += 1
    
    if disk_free_gb < 50:
        logger.warning(f"   • CRÍTICO: Pouco espaço em disco ({disk_free_gb:.1f}GB) - estabelecimentos geram arquivos grandes")
        alerts_count += 1
    
    if alerts_count == 0:
        logger.info(f"   • ✅ Sistema sem alertas críticos para processamento de estabelecimentos")
    
    # Recomendações específicas
    if memory_total_gb >= 32 and cpu_count >= 8:
        logger.info(f"   • ✅ Sistema EXCELENTE para processamento de estabelecimentos")
    elif memory_total_gb >= 16 and cpu_count >= 6:
        logger.info(f"   • ✅ Sistema BOM para processamento de estabelecimentos")
    elif memory_total_gb >= 8 and cpu_count >= 4:
        logger.info(f"   • ⚠️ Sistema ADEQUADO - processamento pode ser mais lento")
    else:
        logger.info(f"   • ⚠️ Sistema LIMITADO - considere upgrade para melhor performance")
    
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
    """Processa um arquivo de dados com processamento em chunks para arquivos grandes. Salva cada chunk individualmente como Parquet."""
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

def compile_estabelecimento_transformations(sample_df: pl.DataFrame) -> dict:
    """
    Compila as transformações de Estabelecimentos uma única vez baseado em um DataFrame de amostra.
    Retorna um dicionário com as transformações pré-compiladas.
    """
    transformations = {
        'cnpj_columns': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'],
        'cnpj_expression': None,
        'type_conversions': [],
        'date_conversions': [],
        'columns_to_drop': [],
        'has_transformations': False
    }
    
    # Verificar se pode criar CNPJ completo
    if all(col in sample_df.columns for col in transformations['cnpj_columns']):
        transformations['cnpj_expression'] = (
            pl.col('cnpj_basico').cast(pl.Utf8).str.pad_start(8, '0') + 
            pl.col('cnpj_ordem').cast(pl.Utf8).str.pad_start(4, '0') + 
            pl.col('cnpj_dv').cast(pl.Utf8).str.pad_start(2, '0')
        ).alias('cnpj')
        transformations['has_transformations'] = True
    
    # Preparar conversões de tipos
    type_mappings = [
        ("matriz_filial", pl.Int8),
        ("codigo_situacao_cadastral", pl.Int8),
        ("codigo_motivo_situacao_cadastral", pl.Int16),
        ("codigo_municipio", pl.Int32),
        ("codigo_cnae", pl.Int32)
    ]
    
    for col_name, col_type in type_mappings:
        if col_name in sample_df.columns:
            transformations['type_conversions'].append((col_name, col_type))
            transformations['has_transformations'] = True
    
    # Preparar conversão de CEP
    if "cep" in sample_df.columns:
        transformations['type_conversions'].append(("cep", "cep_clean"))
        transformations['has_transformations'] = True
    
    # Preparar conversões de data
    date_columns = ["data_situacao_cadastral", "data_inicio_atividades"]
    for date_col in date_columns:
        if date_col in sample_df.columns:
            transformations['date_conversions'].append(date_col)
            transformations['has_transformations'] = True
    
    # Preparar colunas para remoção
    columns_to_drop = [
        'cnpj_ordem', 'cnpj_dv', 'tipo_logradouro', 'logradouro', 'numero', 
        'complemento', 'bairro', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 
        'ddd_fax', 'fax', 'pais', 'correio_eletronico', 'situacao_especial',
        'data_situacao_especial', 'nome_cidade_exterior'
    ]
    
    transformations['columns_to_drop'] = [col for col in columns_to_drop if col in sample_df.columns]
    if transformations['columns_to_drop']:
        transformations['has_transformations'] = True
    
    return transformations

def apply_estabelecimento_transformations_optimized(df: pl.DataFrame, compiled_transformations: dict) -> pl.DataFrame:
    """
    Aplica transformações pré-compiladas de Estabelecimentos de forma otimizada.
    Não faz logs repetitivos nem recompilação de expressões.
    """
    if not compiled_transformations['has_transformations']:
        return df
    
    try:
        # Usar lazy evaluation para otimização
        df_lazy = df.lazy()
        
        # 1. Criar CNPJ completo
        if compiled_transformations['cnpj_expression'] is not None:
            df_lazy = df_lazy.with_columns([compiled_transformations['cnpj_expression']])
        
        # 2. Aplicar conversões de tipos e CEP
        type_expressions = []
        for col_name, col_type in compiled_transformations['type_conversions']:
            if col_type == "cep_clean":
                type_expressions.append(
                    pl.col(col_name).str.replace_all(r"[^\d]", "", literal=False)
                )
            else:
                type_expressions.append(
                    pl.col(col_name).cast(col_type, strict=False)
                )
        
        if type_expressions:
            df_lazy = df_lazy.with_columns(type_expressions)
        
        # 3. Aplicar conversões de data
        date_expressions = []
        for date_col in compiled_transformations['date_conversions']:
            date_expressions.append(
                pl.when(
                    pl.col(date_col).is_in(['0', '00000000', '']) | 
                    pl.col(date_col).is_null()
                )
                .then(None)
                .otherwise(pl.col(date_col))
                .str.strptime(pl.Date, "%Y%m%d", strict=False)
                .alias(date_col)
            )
        
        if date_expressions:
            df_lazy = df_lazy.with_columns(date_expressions)
        
        # 4. Remover colunas desnecessárias
        if compiled_transformations['columns_to_drop']:
            df_lazy = df_lazy.drop(compiled_transformations['columns_to_drop'])
        
        # 5. Coletar resultado com streaming
        df_transformed = df_lazy.collect(streaming=True)
        return df_transformed
        
    except Exception as e:
        logger.warning(f"Erro com transformações otimizadas: {str(e)}, usando fallback simples")
        
        # Fallback simples
        try:
            # Criar CNPJ completo
            if compiled_transformations['cnpj_expression'] is not None:
                df = df.with_columns([compiled_transformations['cnpj_expression']])
            
            # Remover colunas desnecessárias
            if compiled_transformations['columns_to_drop']:
                df = df.drop(compiled_transformations['columns_to_drop'])
            
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
                        # Último recurso: usar um valor padrão fixo
                        remote_folder = "dados"
                        logger.warning(f"Não foi possível extrair pasta remota do caminho. Usando pasta padrão: {remote_folder}")
        
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


def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, remote_folder: str = None, uf_subset: str | None = None) -> bool:
    """Processa um único arquivo ZIP."""
    pid = os.getpid()
    logger.info(f"[{pid}] Iniciando processamento para: {zip_file}")
    extract_dir = ""
    success = False
    zip_filename_prefix = os.path.splitext(zip_file)[0]
    
    # Extrair pasta remota do caminho zip (geralmente algo como 2025-05)
    if remote_folder is None:
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
        
        # Preparar diretório de saída
        output_dir = ensure_correct_folder_structure(path_parquet, remote_folder, 'estabelecimentos')
        
        # Processar cada arquivo de dados diretamente em chunks
        chunk_counter = 0
        
        for data_file in data_files:
            try:
                logger.debug(f"[{pid}] Processando arquivo {os.path.basename(data_file)}")
                
                # Processar arquivo em chunks diretamente
                file_chunk_counter = process_data_file_in_chunks(
                    data_file, 
                    output_dir, 
                    zip_filename_prefix, 
                    chunk_counter,
                    uf_subset
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
    # Log detalhado dos recursos do sistema
    log_system_resources_estabelecimento()
    
    start_time = time.time()
    
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de ESTABELECIMENTOS')
    if uf_subset:
        logger.info(f'Subset UF: {uf_subset}')
    logger.info('=' * 50)
    
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
    
    try:
        zip_files = [f for f in os.listdir(path_zip) 
                     if f.startswith('Estabele') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Estabelecimentos encontrado.')
            return True
            
        logger.info(f"Encontrados {len(zip_files)} arquivos ZIP de estabelecimentos para processar")
        
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
            
            logger.info(f"[{completed_files}/{total_files}] ({progress_pct:.1f}%) Iniciando processamento de {zip_file}")
            
            # Mostrar informações detalhadas em modo DEBUG
            if logger.isEnabledFor(logging.DEBUG):
                resources = get_system_resources()
                logger.debug(f"Status do sistema - CPU: {resources['cpu_percent']:.1f}%, "
                           f"Memória: {resources['memory_percent']:.1f}%, "
                           f"Disco: {resources['disk_percent']:.1f}%")
            
            try:
                file_start_time = time.time()
                result = process_single_zip(zip_file, path_zip, path_unzip, path_parquet, remote_folder, uf_subset)
                file_elapsed_time = time.time() - file_start_time
                
                if result:
                    success = True
                    logger.info(f"[{completed_files}/{total_files}] ({progress_pct:.1f}%) ✓ {zip_file} processado com sucesso em {file_elapsed_time:.2f}s. "
                                f"Tempo médio: {avg_time_per_file:.1f}s/arquivo. "
                                f"Tempo estimado restante: {estimated_remaining:.1f}s")
                else:
                    arquivos_com_falha.append(zip_file)
                    logger.warning(f"[{completed_files}/{total_files}] ({progress_pct:.1f}%) ✗ Falha no processamento do arquivo {zip_file} após {file_elapsed_time:.2f}s")
            except Exception as e:
                arquivos_com_falha.append(zip_file)
                logger.error(f"[{completed_files}/{total_files}] ({progress_pct:.1f}%) ✗ Exceção no processamento do arquivo {zip_file}: {str(e)}")
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
        logger.info(f"Tempo médio por arquivo: {total_time/completed_files if completed_files > 0 else 0:.2f}s")
        if uf_subset:
            logger.info(f"Subset UF processado: {uf_subset}")
        logger.info("=" * 50)
        
        # Verificar se pelo menos um arquivo foi processado com sucesso
        return success
    except Exception as e:
        logger.error(f'Erro no processamento principal de Estabelecimentos: {str(e)}')
        traceback.print_exc()
        return False

def process_data_file_in_chunks(data_file_path: str, output_dir: str, zip_prefix: str, start_chunk_counter: int = 0, uf_subset: str = None) -> int:
    """
    Processa um arquivo de dados diretamente em chunks, salvando cada chunk como Parquet.
    
    Args:
        data_file_path: Caminho do arquivo de dados
        output_dir: Diretório de saída
        zip_prefix: Prefixo do arquivo ZIP
        start_chunk_counter: Contador inicial de chunks
        uf_subset: UF para filtrar (opcional)
        
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
                        new_columns=config.estabelecimento_columns,
                        infer_schema_length=0,
                        dtypes={col: pl.Utf8 for col in config.estabelecimento_columns},
                        ignore_errors=True,
                        truncate_ragged_lines=True
                    )
                    
                    if df_chunk.is_empty():
                        logger.debug(f"Chunk {chunk_counter + 1} está vazio, avançando...")
                        rows_processed += rows_to_read
                        continue
                    
                    # Compilar transformações apenas no primeiro chunk
                    if compiled_transformations is None:
                        logger.info("Compilando transformações de Estabelecimentos (uma única vez)...")
                        compiled_transformations = compile_estabelecimento_transformations(df_chunk)
                        if compiled_transformations['has_transformations']:
                            logger.info("✅ Transformações de Estabelecimentos compiladas com sucesso")
                        else:
                            logger.info("ℹ️ Nenhuma transformação necessária para este arquivo")
                    
                    # Aplicar transformações otimizadas
                    df_transformed = apply_estabelecimento_transformations_optimized(df_chunk, compiled_transformations)
                    
                    # Aplicar filtro de UF se especificado
                    if uf_subset and 'uf' in df_transformed.columns:
                        df_transformed = df_transformed.filter(pl.col('uf') == uf_subset.upper())
                        if df_transformed.is_empty():
                            logger.debug(f"Chunk {chunk_counter + 1} vazio após filtro UF {uf_subset}")
                            rows_processed += rows_to_read
                            continue
                    
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
