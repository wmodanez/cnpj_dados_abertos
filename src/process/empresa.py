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
from multiprocessing import Pool
import re
import datetime
import concurrent.futures

from ..config import config
from ..utils import (
    file_delete, check_disk_space, estimate_zip_extracted_size,
    process_csv_files_parallel, verify_csv_integrity, 
    create_parquet_filename
)
from ..utils.folders import get_output_path, ensure_correct_folder_structure
import inspect

logger = logging.getLogger(__name__)

# Flag global para garantir que o logger do worker seja configurado apenas uma vez por processo
_worker_logger_configured = False

def configure_worker_logging(log_file):
    """Configura o logging para o processo worker."""
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Handler de arquivo
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    
    # Formato
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    
    # Adicionar handler
    logger.addHandler(fh)
    
    return logger


def process_empresa(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Processa arquivos de empresa usando Polars."""
    return process_empresa_files(path_zip, path_unzip, path_parquet, create_private)


# ----- Implementação para Polars -----

def process_csv_file(csv_path):
    """
    Processa um único arquivo CSV de empresa usando Polars.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Polars ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa colunas da config
    original_column_names = config.empresa_columns

    try:
        # Usa polars.read_csv com os parâmetros apropriados
        df = pl.read_csv(
            csv_path,
            separator=config.file.separator,
            encoding=config.file.encoding,
            has_header=False,
            new_columns=original_column_names,
            infer_schema_length=0,  # Não inferir schema
            dtypes={col: pl.Utf8 for col in original_column_names}  # Inicialmente lê tudo como string
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)} com Polars: {str(e)}')
        return None


def process_data_file(data_path: str):
    """
    Processa um único arquivo de dados usando Polars, seja ele CSV ou outro formato de texto.
    
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
    """Aplica transformações específicas para Empresas usando Polars."""
    logger.info("Aplicando transformações em Empresas...")
    
    # Renomeação de colunas
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico',
        'razao_social_nome_empresarial': 'razao_social',
        'natureza_juridica': 'natureza_juridica',
        'qualificacao_do_responsavel': 'qualificacao_responsavel',
        'capital_social_da_empresa': 'capital_social',
        'porte_da_empresa': 'porte_empresa',
        'ente_federativo_responsavel': 'ente_federativo_responsavel'
    }
    
    # Filtrar para manter apenas colunas que existem no DataFrame
    rename_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
    
    # Aplicar renomeação se houver colunas para renomear
    if rename_mapping:
        df = df.rename(rename_mapping)
    
    # --- Extração de CPF da razao_social (Polars) ---
    if 'razao_social' in df.columns:
        logger.info("Extraindo CPF da razao_social (Polars)...")
        
        # Primeiro criamos as expressões para extrair CPF com máscara e sem máscara
        cpf_com_mascara = pl.col('razao_social').str.extract(r'(\d{3}\.\d{3}\.\d{3}-\d{2})')
        cpf_sem_mascara = pl.col('razao_social').str.extract(r'(\d{11})')
        
        # Combinamos as duas extrações, priorizando o formato com máscara
        cpf_combinado = (pl.when(cpf_com_mascara.is_not_null())
                         .then(cpf_com_mascara)
                         .otherwise(cpf_sem_mascara))
        
        # Removemos pontos e traços dos CPFs com máscara
        cpf_sem_formato = cpf_combinado.str.replace_all(r'[.\-]', '')
        
        # Verificamos se o CPF tem 11 dígitos 
        cpf_validado = (pl.when(cpf_sem_formato.str.len_chars() == 11)
                        .then(cpf_sem_formato)
                        .otherwise(None)
                        .alias('CPF'))
        
        # Aplicamos as transformações e adicionamos a coluna CPF
        df = df.with_columns(cpf_validado)
        
        logger.info("Extração de CPF concluída (Polars).")
        
        # --- Remoção do CPF da razao_social (Polars) ---
        logger.info("Removendo CPF da razao_social (Polars)...")
        
        # Remove primeiro o padrão com máscara, depois o sem máscara
        razao_sem_cpf = (pl.col('razao_social')
                         .str.replace_all(r'\d{3}\.\d{3}\.\d{3}-\d{2}', '')
                         .str.replace_all(r'\d{11}', '')
                         .str.strip_chars()  # Usando strip_chars() em vez de strip()
                         .alias('razao_social'))
        
        df = df.with_columns(razao_sem_cpf)
        logger.info("Remoção do CPF da razao_social concluída (Polars).")
        
    # Garante que a coluna CPF exista se razao_social existia mas CPF não foi extraído
    elif 'razao_social' in df.columns and 'CPF' not in df.columns:
        df = df.with_columns(pl.lit(None).alias('CPF'))
    
    # Liberar memória explicitamente
    gc.collect()
    
    return df


def create_parquet(df: pl.DataFrame, table_name: str, path_parquet: str, 
                         zip_filename_prefix: str, partition_size: int = 500_000) -> bool:
    """
    Salva DataFrame Polars em arquivos Parquet particionados para reduzir uso de memória.
    
    Args:
        df: DataFrame Polars
        table_name: Nome da tabela (subpasta)
        path_parquet: Caminho base para os arquivos parquet (diretório pai de table_name)
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


def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, create_private: bool) -> bool:
    """Processa um único arquivo ZIP."""
    logger = logging.getLogger()
    pid = os.getpid()
    logger.info(f"[{pid}] Iniciando processamento para: {zip_file} (create_private={create_private})" )
    path_extracao = "" # Inicializa fora do try/finally
    success = False
    zip_filename_prefix = os.path.splitext(zip_file)[0] # Usar para nomear arquivos parquet
    
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
        # --- 1. Extração --- 
        logger.info(f"[{pid}] Polars Fase 1: Iniciando extração de {zip_file}..." )
        
        # Verificar se o arquivo ZIP existe
        zip_file_path = os.path.join(path_zip, zip_file)
        if not os.path.exists(zip_file_path):
            logger.error(f"[{pid}] Polars: Arquivo ZIP {zip_file_path} não existe")
            return False
        
        # Criar diretório para extração (específico para este ZIP)
        path_extracao = os.path.join(path_unzip, os.path.splitext(zip_file)[0])
        if os.path.exists(path_extracao):
            logger.debug(f"[{pid}] Polars: Limpando diretório de extração: {path_extracao}")
            try:
                shutil.rmtree(path_extracao)
            except Exception as e_clean:
                logger.warning(f"[{pid}] Polars: Erro ao limpar diretório de extração prévio {path_extracao}: {e_clean}")
        
        os.makedirs(path_extracao, exist_ok=True)
        
        # Extrair arquivo ZIP
        logger.info(f"[{pid}] Polars: Extraindo {zip_file} para {path_extracao}")
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(path_extracao)
        except Exception as e_extract:
            logger.error(f"[{pid}] Polars: Erro ao extrair arquivo ZIP {zip_file}: {e_extract}")
            return False
            
        # --- 2. Leitura e Processamento --- 
        logger.info(f"[{pid}] Polars Fase 2: Iniciando leitura e processamento de arquivos de dados..." )
        
        # Buscar arquivos de dados no diretório extraído
        data_files = []
        for root, _, files in os.walk(path_extracao):
            for file in files:
                # Verificar extensão ou padrão do arquivo
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                
                # Pular arquivos vazios
                if file_size == 0:
                    continue
                
                # Verificar extensões que claramente não são de dados
                invalid_extensions = ['.exe', '.dll', '.zip', '.rar', '.gz', '.tar', '.bz2', '.7z', 
                                     '.png', '.jpg', '.jpeg', '.gif', '.pdf']
                file_ext = os.path.splitext(file.lower())[1]
                if file_ext in invalid_extensions:
                    continue
                
                # Adicionar à lista de arquivos para processar
                data_files.append(file_path)
        
        if not data_files:
            logger.warning(f"[{pid}] Polars: Nenhum arquivo de dados encontrado no diretório {path_extracao} após extração")
            return False
            
        # Processar cada arquivo de dados encontrado
        logger.info(f"[{pid}] Polars: Processando {len(data_files)} arquivos de dados...")
        dataframes_polars = []
        
        for data_file in data_files:
            logger.debug(f"[{pid}] Polars: Processando arquivo {os.path.basename(data_file)}")
            try:
                df_polars = process_data_file(data_file)
                if df_polars is not None and not df_polars.is_empty():
                    dataframes_polars.append(df_polars)
                    logger.info(f"[{pid}] Polars: Arquivo {os.path.basename(data_file)} processado com sucesso: {df_polars.height} linhas")
            except Exception as e_data:
                logger.error(f"[{pid}] Polars: Erro ao processar arquivo {os.path.basename(data_file)}: {e_data}")
                # Continuamos com outros arquivos mesmo se um falhar
        
        # Se não temos DataFrames Polars válidos, encerramos
        if not dataframes_polars:
            logger.warning(f"[{pid}] Polars: Nenhum DataFrame válido gerado. Encerrando processamento de {zip_file}")
            return False
            
        # Concatenar DataFrames do Polars se houver mais de um
        if len(dataframes_polars) > 1:
            logger.info(f"[{pid}] Polars: Concatenando {len(dataframes_polars)} DataFrames...")
            try:
                df_final_polars = pl.concat(dataframes_polars)
                # Liberar memória dos DataFrames individuais
                for df in dataframes_polars:
                    del df
                dataframes_polars = []
                gc.collect()
            except Exception as e_concat:
                logger.error(f"[{pid}] Polars: Erro ao concatenar DataFrames: {e_concat}")
                return False
        else:
            df_final_polars = dataframes_polars[0]
            dataframes_polars = []
            gc.collect()
            
        # Verificar se o DataFrame final tem dados
        if df_final_polars is not None and not df_final_polars.is_empty():
            # Aplicar transformações no DataFrame
            logger.info(f"[{pid}] Polars: Aplicando transformações em DataFrame com {df_final_polars.height} linhas...")
            try:
                df_final_polars = apply_empresa_transformations(df_final_polars)
                
                if df_final_polars.is_empty():
                    logger.warning(f"[{pid}] Polars: DataFrame vazio após transformações para {zip_file}")
                    return False
                
                logger.info(f"[{pid}] Polars: Transformações aplicadas com sucesso para {zip_file}")
                
                # --- 3. Salvar Parquet (Principal) --- 
                logger.info(f"[{pid}] Polars Fase 3: Salvando Parquet principal para {zip_file}..." )
                
                # Criar diretório para cada tipo usando a função ensure_correct_folder_structure
                # que vai cuidar de garantir a estrutura correta
                logger.info(f"[{pid}] Polars: Salvando empresas usando ensure_correct_folder_structure")
                
                # Passa o prefixo do nome do zip para a função de salvar
                parquet_main_saved = False
                try:
                    # Usar ensure_correct_folder_structure para criar o caminho correto
                    output_dir = ensure_correct_folder_structure(path_parquet, remote_folder, 'empresas')
                    
                    # Salvamento direto sem adicionar a pasta remota no caminho
                    total_rows = df_final_polars.height
                    partition_size = 500_000
                    num_partitions = (total_rows + partition_size - 1) // partition_size
                    
                    logger.info(f"[{pid}] Salvando DataFrame de empresas com {total_rows} linhas para {output_dir}")
                    
                    for i in range(num_partitions):
                        start_idx = i * partition_size
                        end_idx = min((i + 1) * partition_size, total_rows)
                        
                        partition = df_final_polars.slice(start_idx, end_idx - start_idx)
                        output_path = os.path.join(output_dir, f"{zip_filename_prefix}_part{i:03d}.parquet")
                        
                        logger.info(f"[{pid}] Salvando partição {i+1}/{num_partitions} para {output_path}")
                        
                        partition.write_parquet(output_path, compression="snappy")
                        logger.info(f"[{pid}] Partição {i+1}/{num_partitions} salva com sucesso")
                        
                        # Liberar memória
                        del partition
                        gc.collect()
                        
                    parquet_main_saved = True
                except Exception as e:
                    logger.error(f"[{pid}] Erro ao salvar Parquet principal: {e}")
                
                if parquet_main_saved:
                     logger.info(f"[{pid}] Polars: Parquet principal salvo para {zip_file}.")
                else:
                     logger.error(f"[{pid}] Polars: Falha ao salvar Parquet principal para {zip_file}." )
                     # Mantém success = False se falhar aqui
            except Exception as e:
                logger.error(f"[{pid}] Erro ao aplicar transformações: {e}")
                return False

            # --- 4. Salvar Parquet (Empresa Privada - Condicional) --- 
            private_subset_success = True # Assume sucesso se não precisar criar
            if parquet_main_saved and create_private:
                logger.info(f"[{pid}] Polars Fase 4: Criando e salvando subset Empresa Privada para {zip_file}..." )
                if 'natureza_juridica' in df_final_polars.columns:
                    logger.debug(f"[{pid}] Polars: Filtrando empresas privadas...")
                    df_privada = df_final_polars.filter(
                        (pl.col('natureza_juridica') >= 2046) & 
                        (pl.col('natureza_juridica') <= 2348)
                    )
                    if not df_privada.is_empty():
                        logger.info(f"[{pid}] Polars: Salvando subset empresa_privada...")
                        
                        # Usa o mesmo padrão de salvamento direto
                        private_saved = False
                        try:
                            # Usar ensure_correct_folder_structure para criar o caminho correto
                            output_dir = ensure_correct_folder_structure(path_parquet, remote_folder, 'empresa_privada')
                            
                            # Salvamento direto 
                            total_rows = df_privada.height
                            partition_size = 500_000
                            num_partitions = (total_rows + partition_size - 1) // partition_size
                            
                            logger.info(f"[{pid}] Salvando DataFrame de empresas privadas com {total_rows} linhas")
                            
                            for i in range(num_partitions):
                                start_idx = i * partition_size
                                end_idx = min((i + 1) * partition_size, total_rows)
                                
                                partition = df_privada.slice(start_idx, end_idx - start_idx)
                                output_path = os.path.join(output_dir, f"{zip_filename_prefix}_part{i:03d}.parquet")
                                
                                logger.info(f"[{pid}] Salvando partição {i+1}/{num_partitions} de empresas privadas")
                                
                                partition.write_parquet(output_path, compression="snappy")
                                logger.info(f"[{pid}] Partição {i+1}/{num_partitions} de empresas privadas salva")
                                
                                # Liberar memória
                                del partition
                                gc.collect()
                                
                            private_saved = True
                        except Exception as e:
                            logger.error(f"[{pid}] Erro ao salvar subset empresa_privada: {e}")
                        
                        if private_saved:
                            logger.info(f"[{pid}] Polars: Subset empresa_privada salvo com sucesso para {zip_file}.")
                        else:
                            logger.error(f"[{pid}] Polars: Falha ao salvar subset empresa_privada para {zip_file}." )
                            private_subset_success = False # Falha no subset
                    else:
                        logger.info(f"[{pid}] Polars: Subset empresa_privada vazio para {zip_file}, não será salvo.")
                    del df_privada
                    gc.collect()
                else:
                    logger.warning(f"[{pid}] Polars: Coluna 'natureza_juridica' não encontrada, não é possível criar subset empresa_privada para {zip_file}." )
                    # Não considera falha se a coluna não existe
            elif not create_private:
                logger.info(f"[{pid}] Polars: Criação do subset empresa_privada pulada (create_private=False) para {zip_file}." )
            
            # Sucesso final depende do principal E do subset (se tentado)
            success = parquet_main_saved and private_subset_success 
        
        elif df_final_polars is not None and df_final_polars.is_empty():
            logger.warning(f"[{pid}] Polars: DataFrame final vazio após concatenação para {zip_file}. Nenhum Parquet será gerado.")
            success = True # Considera sucesso se não havia dados
        
        # Limpar df_final_polars se ele existir
        if df_final_polars is not None:
             del df_final_polars
             gc.collect()

    except Exception as e_general:
        logger.exception(f"[{pid}] Polars: Erro GERAL e inesperado processando {zip_file}")
        success = False
    
    finally:
        # --- 5. Limpeza Final --- 
        # Limpa apenas a subpasta de extração específica deste ZIP
        if path_extracao and os.path.exists(path_extracao):
            logger.info(f"[{pid}] Polars: Limpando diretório de extração específico: {path_extracao}")
            try:
                shutil.rmtree(path_extracao)
            except Exception as e_clean:
                logger.warning(f"[{pid}] Polars: Não foi possível limpar completamente o diretório de extração {path_extracao}: {e_clean}")

    logger.info(f"[{pid}] Processamento Polars para {zip_file} concluído com status final: {success}")
    return success


def process_empresa_files(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Processa os dados de empresas.
    
    Args:
        path_zip: Caminho para o diretório dos arquivos ZIP
        path_unzip: Caminho para o diretório de extração
        path_parquet: Caminho para o diretório dos arquivos parquet
        create_private: Flag para criar subset de empresas privadas
        
    Returns:
        bool: True se o processamento foi bem-sucedido, False caso contrário
    """
    import time
    start_time = time.time()
    
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de EMPRESAS')
    logger.info('=' * 50)
    
    try:
        zip_files = [f for f in os.listdir(path_zip)
                     if f.startswith('Empr') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Empresas encontrado.')
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
        
        # Garantir estrutura correta para o diretório principal de empresas
        output_dir_main = ensure_correct_folder_structure(path_parquet, remote_folder, 'empresas')
        
        # LIMPEZA PRÉVIA do diretório de saída principal
        try:
            file_delete(output_dir_main)
            logger.info(f'Diretório {output_dir_main} limpo antes do processamento.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretório de saída {output_dir_main}: {str(e)}')
        
        # Criar também a estrutura para o diretório de empresas privadas se necessário
        if create_private:
            output_dir_private = ensure_correct_folder_structure(path_parquet, remote_folder, 'empresa_privada')
            try:
                file_delete(output_dir_private)
                logger.info(f'Diretório {output_dir_private} limpo antes do processamento.')
            except Exception as e:
                logger.warning(f'Não foi possível limpar diretório de saída {output_dir_private}: {str(e)}')
            
            # Garantir que o diretório existe
            os.makedirs(output_dir_private, exist_ok=True)
        
        # Garantir que o diretório principal existe
        os.makedirs(output_dir_main, exist_ok=True)
        
        # Forçar coleta de lixo antes de iniciar o processamento
        gc.collect()
        
        # Processar com Polars em paralelo - Otimizado
        # Usar 75% dos CPUs disponíveis para não sobrecarregar
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
            logger.info(f"Processando {total_files} arquivos de empresas...")
            
            # Mapear cada arquivo ZIP para ser processado
            futures = {}
            for zip_file in zip_files:
                future = executor.submit(
                    process_single_zip, 
                    zip_file, 
                    path_zip, 
                    path_unzip, 
                    path_parquet,
                    create_private
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
            logger.warning('Nenhum arquivo ZIP de Empresas foi processado com sucesso.')
            
        if arquivos_com_falha:
            logger.warning(f'Os seguintes arquivos falharam no processamento: {", ".join(arquivos_com_falha)}')
            
        if arquivos_com_timeout:
            logger.warning(f'Os seguintes arquivos excederam o tempo limite: {", ".join(arquivos_com_timeout)}')
        
        # Logar resumo completo
        logger.info("=" * 50)
        logger.info("RESUMO DO PROCESSAMENTO DE EMPRESAS:")
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
        logger.error(f'Erro no processamento principal de Empresas: {str(e)}')
        traceback.print_exc()
        return False
