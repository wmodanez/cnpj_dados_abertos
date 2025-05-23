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

from ..config import config
from ..utils import file_delete, check_disk_space, verify_csv_integrity
from ..utils.folders import get_output_path, ensure_correct_folder_structure
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

def process_data_file(data_path: str, chunk_size: int = 1000000):
    """Processa um arquivo de dados usando Polars com processamento em chunks para arquivos grandes."""
    logger = logging.getLogger(__name__)

    # Verificar tamanho do arquivo para decidir estratégia
    file_size = os.path.getsize(data_path)
    file_size_mb = file_size / (1024 * 1024)
    
    # Se arquivo for menor que 500MB, processar normalmente
    if file_size_mb < 500:
        try:
            # Usar lazy evaluation para melhor performance
            df_lazy = pl.scan_csv(
                data_path,
                separator=config.file.separator,
                encoding='utf8-lossy',  # Encoding compatível com scan_csv que funciona com latin1
                has_header=False,
                new_columns=config.estabelecimento_columns,
                dtypes=config.estabelecimento_dtypes,
                rechunk=True
            )
            
            # Coletar apenas quando necessário
            df = df_lazy.collect()
            
            if not df.is_empty():
                logger.info(f"Arquivo {os.path.basename(data_path)} processado com sucesso ({df.height} linhas, {file_size_mb:.1f}MB)")
                return df
            else:
                logger.warning(f"Arquivo {os.path.basename(data_path)} está vazio")
                return None
        except Exception as e:
            logger.error("="*60)
            logger.error(f"FALHA ao processar {os.path.basename(data_path)}")
            logger.error(f"Erro detalhado: {str(e)}")
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
                logger.error(f"Falha também com ignore_errors=True em {os.path.basename(data_path)}: {str(e2)}")
                logger.error("="*60)
                return None
    
    # Para arquivos grandes (>500MB), processar em chunks
    else:
        logger.info(f"Arquivo grande detectado ({file_size_mb:.1f}MB). Processando em chunks de {chunk_size:,} linhas...")
        
        try:
            chunks = []
            chunk_count = 0
            
            # Usar scan_csv com streaming para arquivos grandes
            df_lazy = pl.scan_csv(
                data_path,
                separator=config.file.separator,
                encoding='utf8-lossy',
                has_header=False,
                new_columns=config.estabelecimento_columns,
                dtypes=config.estabelecimento_dtypes
            )
            
            # Processar em batches usando streaming
            total_rows = 0
            batch_size = chunk_size
            offset = 0
            
            while True:
                # Ler um chunk
                chunk_lazy = df_lazy.slice(offset, batch_size)
                chunk_df = chunk_lazy.collect()
                
                if chunk_df.is_empty():
                    break
                
                chunks.append(chunk_df)
                total_rows += chunk_df.height
                chunk_count += 1
                offset += batch_size
                
                logger.debug(f"Chunk {chunk_count} processado: {chunk_df.height} linhas (total: {total_rows:,})")
                
                # Liberar memória do chunk
                del chunk_df
                gc.collect()
                
                # Limite de segurança para evitar loops infinitos
                if chunk_count > 100:
                    logger.warning(f"Limite de chunks atingido (100). Parando processamento.")
                    break
            
            if chunks:
                # Concatenar todos os chunks
                logger.info(f"Concatenando {len(chunks)} chunks com total de {total_rows:,} linhas...")
                df_final = pl.concat(chunks, rechunk=False)
                
                # Liberar memória dos chunks
                del chunks
                gc.collect()
                
                logger.info(f"Arquivo {os.path.basename(data_path)} processado em chunks com sucesso ({df_final.height} linhas)")
                return df_final
            else:
                logger.warning(f"Nenhum chunk válido encontrado em {os.path.basename(data_path)}")
                return None
                
        except Exception as e:
            logger.error("="*60)
            logger.error(f"FALHA no processamento em chunks de {os.path.basename(data_path)}")
            logger.error(f"Erro detalhado: {str(e)}")
            logger.error("="*60)
            
            # Fallback para leitura normal com ignore_errors
            try:
                logger.error("Tentando fallback com leitura normal...")
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
                logger.warning(f"Arquivo {os.path.basename(data_path)} lido com fallback ({df.height} linhas)")
                return df
            except Exception as e3:
                logger.error(f"Fallback também falhou: {str(e3)}")
                return None


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
            
        if os.path.getsize(zip_path) == 0:
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
        
        # Extrair arquivo ZIP
        logger.info(f"[{pid}] Extraindo {zip_file} para {extract_dir}")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
        except zipfile.BadZipFile:
            logger.error(f"[{pid}] Arquivo {zip_path} não é um arquivo ZIP válido")
            return False
        except Exception as e_zip:
            logger.error(f"[{pid}] Erro durante a extração do ZIP {zip_path}: {e_zip}")
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
            
            df = process_data_file(data_path, chunk_size=chunk_size)
            if df is None:
                logger.error(f"[{pid}] Falha crítica ao processar {data_file}")
                return False
            elif df.is_empty():
                logger.error(f"[{pid}] DataFrame vazio gerado para {data_file}")
                return False
            else:
                dataframes.append(df)

        if not dataframes:
            logger.error(f"[{pid}] Nenhum DataFrame válido gerado de {zip_file}")
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
    pl.Config.set_streaming_chunk_size(500000)  # 500k linhas por chunk
    
    # 2. PARALELISMO: Usar 75% dos CPUs para permitir uso em paralelo
    available_cpus = os.cpu_count() or 4
    max_threads = max(1, int(available_cpus * 0.75))
    # Nota: Polars gerencia threads automaticamente, não há set_thread_pool_size
    
    # 3. MEMÓRIA: Configurações para otimizar uso de memória
    pl.Config.set_tbl_rows(20)  # Limitar linhas mostradas em prints
    pl.Config.set_tbl_cols(10)  # Limitar colunas mostradas em prints
    
    # 4. CACHE: Configurar cache para operações repetitivas
    pl.Config.set_auto_structify(True)  # Otimizar estruturas automaticamente
    
    logger.info(f"Configurações de streaming aplicadas:")
    logger.info(f"  - Chunk size: 500.000 linhas")
    logger.info(f"  - CPUs disponíveis: {available_cpus} (Polars gerencia threads automaticamente)")
    logger.info(f"  - Streaming habilitado para arquivos grandes")
    logger.info(f"  - Processamento em chunks para arquivos >500MB")
    
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
        
        # Processar com Polars em paralelo - Otimizado
        # Usar 75% dos CPUs disponíveis para permitir uso em paralelo
        available_cpus = os.cpu_count() or 4
        max_workers = max(1, int(available_cpus * 0.75))
        
        logger.info(f"Iniciando processamento paralelo com {max_workers} workers (75% dos {available_cpus} CPUs disponíveis)")
        
        # Timeout para cada tarefa (em segundos) - para evitar travamentos
        timeout_per_task = 3600  # 1 hora por arquivo
        
        success = False
        arquivos_com_falha = []
        arquivos_com_timeout = []
        total_files = len(zip_files)
        completed_files = 0
        
        # Processar todos os arquivos ZIP em paralelo
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            logger.info(f"Processando {total_files} arquivos de estabelecimentos...")
            
            # Mapear cada arquivo ZIP para ser processado
            futures = {}
            for zip_file in zip_files:
                future = executor.submit(
                    process_single_zip, 
                    zip_file, 
                    path_zip, 
                    path_unzip, 
                    path_parquet,
                    uf_subset
                )
                futures[future] = zip_file
            
            # Coletar resultados à medida que são concluídos
            try:
                for future in concurrent.futures.as_completed(futures, timeout=timeout_per_task):
                    zip_file = futures[future]
                    completed_files += 1
                    elapsed_time = time.time() - start_time
                    
                    # Calcular métricas de progresso
                    progress_pct = (completed_files / total_files) * 100
                    avg_time_per_file = elapsed_time / completed_files if completed_files > 0 else 0
                    estimated_remaining = avg_time_per_file * (total_files - completed_files)
                    
                    try:
                        result = future.result()
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
            
            except concurrent.futures.TimeoutError:
                # Identificar quais tarefas não foram concluídas (timeout)
                for future, zip_file in futures.items():
                    if not future.done():
                        arquivos_com_timeout.append(zip_file)
                        logger.error(f"TIMEOUT: Arquivo {zip_file} excedeu o tempo limite de {timeout_per_task} segundos")
                        # Cancelar a tarefa para liberar recursos
                        future.cancel()
        
        # Calcular estatísticas finais
        total_time = time.time() - start_time
        
        if not success:
            logger.warning('Nenhum arquivo ZIP de Estabelecimentos foi processado com sucesso.')
            
        if arquivos_com_falha:
            logger.warning(f'Os seguintes arquivos falharam no processamento: {", ".join(arquivos_com_falha)}')
            
        if arquivos_com_timeout:
            logger.warning(f'Os seguintes arquivos excederam o tempo limite: {", ".join(arquivos_com_timeout)}')
        
        # Logar resumo completo
        logger.info("=" * 50)
        logger.info("RESUMO DO PROCESSAMENTO DE ESTABELECIMENTOS:")
        logger.info("=" * 50)
        logger.info(f"Arquivos processados com sucesso: {completed_files - len(arquivos_com_falha) - len(arquivos_com_timeout)}/{total_files}")
        logger.info(f"Arquivos com falha: {len(arquivos_com_falha)}/{total_files}")
        logger.info(f"Arquivos com timeout: {len(arquivos_com_timeout)}/{total_files}")
        logger.info(f"Tempo total de processamento: {total_time:.2f} segundos")
        logger.info(f"Tempo médio por arquivo: {total_time/completed_files if completed_files > 0 else 0:.2f} segundos")
        logger.info("=" * 50)
        
        # Verificar se pelo menos um arquivo foi processado com sucesso
        return success
    except Exception as e:
        logger.error(f'Erro no processamento principal de Estabelecimentos: {str(e)}')
        traceback.print_exc()
        return False
