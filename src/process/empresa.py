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

from ..config import config
from ..utils import (
    file_delete, check_disk_space, estimate_zip_extracted_size,
    process_csv_files_parallel, verify_csv_integrity, 
    create_parquet_filename
)
import inspect

logger = logging.getLogger(__name__)

# Flag global para garantir que o logger do worker seja configurado apenas uma vez por processo
_worker_logger_configured = False

def configure_worker_logging(log_file):
    """Configura o logger para o processo worker."""
    global _worker_logger_configured
    if _worker_logger_configured or log_file is None:
        return
    
    try:
        worker_logger = logging.getLogger() # Pega o logger raiz
        # Remover handlers existentes para evitar duplicação se o worker for reutilizado?
        # Não remover, pois pode afetar outros usos. Adicionar filtro?
        # Ou verificar se handler já existe?
        
        # Verificar se já existe um FileHandler para este arquivo
        handler_exists = False
        for handler in worker_logger.handlers:
            if isinstance(handler, logging.FileHandler) and handler.baseFilename == log_file:
                handler_exists = True
                break
        
        if not handler_exists:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - [Worker:%(process)d] - %(message)s')
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            worker_logger.addHandler(file_handler)
            # Definir o nível do logger raiz do worker (pode ser ajustado)
            worker_logger.setLevel(logging.DEBUG) # Capturar tudo a partir de DEBUG
            worker_logger.info(f"Logger do worker configurado para escrever em {log_file}")
            _worker_logger_configured = True
        else:
             worker_logger.debug(f"FileHandler para {log_file} já existe neste worker.")
             _worker_logger_configured = True # Marcar como configurado mesmo se já existia

    except Exception as e:
        # Usar print aqui pois o logging pode ter falhado
        print(f"[Worker PID: {os.getpid()}] Erro ao configurar logging do worker para {log_file}: {e}", file=sys.stderr)


def process_empresa(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Processa arquivos de empresa usando Polars."""
    return process_empresa_with_polars(path_zip, path_unzip, path_parquet, create_private)


# ----- Implementação para Polars -----

def process_csv_file_polars(csv_path):
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


def apply_empresa_transformations_polars(df: pl.DataFrame, chunk_size: int = 1_000_000) -> pl.DataFrame:
    """Aplica transformações específicas para Empresas usando Polars."""
    logger.info("Aplicando transformações em Empresas com Polars...")
    
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


def create_parquet_polars(df: pl.DataFrame, table_name: str, path_parquet: str, 
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
        output_dir = os.path.join(path_parquet, table_name)
        
        # Garante que o diretório de saída existe
        os.makedirs(output_dir, exist_ok=True)
        
        # Log das colunas antes de salvar
        logger.info(f"Colunas do DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {df.columns}")
        
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
        logger.error(f"Erro ao criar arquivo Parquet com Polars: {str(e)}")
        return False


def process_single_zip_polars(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, create_private: bool) -> bool:
    """Processa um único arquivo ZIP usando Polars para eficiência."""
    logger = logging.getLogger()
    pid = os.getpid()
    logger.info(f"[{pid}] Iniciando processamento Polars para: {zip_file} (create_private={create_private})" )
    path_extracao = "" # Inicializa fora do try/finally
    success = False
    zip_filename_prefix = os.path.splitext(zip_file)[0] # Usar para nomear arquivos parquet

    try:
        # --- 1. Extração --- 
        logger.debug(f"[{pid}] Polars Fase 1: Extração")
        # Cria subpasta única para este ZIP
        nome_arquivo_sem_ext = os.path.splitext(zip_file)[0]
        path_extracao = os.path.join(path_unzip, nome_arquivo_sem_ext)
        
        if os.path.exists(path_extracao):
            logger.warning(f"[{pid}] Polars: Diretório de extração {path_extracao} já existe. Removendo.")
            try:
                shutil.rmtree(path_extracao)
            except Exception as e_rem_dir:
                logger.error(f"[{pid}] Polars: Erro ao remover diretório de extração antigo {path_extracao}: {e_rem_dir}")
                return False

        os.makedirs(path_extracao, exist_ok=True)

        path_zip_file = os.path.join(path_zip, zip_file)
        
        # Verificar se o arquivo ZIP existe e tem tamanho válido
        if not os.path.exists(path_zip_file):
            logger.error(f"[{pid}] Polars: Arquivo ZIP {path_zip_file} não existe")
            return False
            
        if os.path.getsize(path_zip_file) == 0:
            logger.error(f"[{pid}] Polars: Arquivo ZIP {path_zip_file} está vazio (tamanho 0 bytes)")
            return False
            
        # Verificar se é um arquivo ZIP válido antes de tentar extrair
        try:
            with zipfile.ZipFile(path_zip_file, 'r') as test_zip:
                file_list = test_zip.namelist()
                if not file_list:
                    logger.warning(f"[{pid}] Polars: Arquivo ZIP {path_zip_file} existe mas não contém arquivos")
                    return False
                else:
                    logger.info(f"[{pid}] Polars: Arquivo ZIP {path_zip_file} é válido e contém {len(file_list)} arquivos")
        except zipfile.BadZipFile:
            logger.error(f"[{pid}] Polars: Arquivo {path_zip_file} não é um arquivo ZIP válido")
            return False
        except Exception as e_test_zip:
            logger.error(f"[{pid}] Polars: Erro ao verificar arquivo ZIP {path_zip_file}: {str(e_test_zip)}")
            return False
            
        logger.info(f"[{pid}] Polars: Extraindo {zip_file} para {path_extracao}...") # Extrai para subpasta
        try:
            with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
                zip_ref.extractall(path_extracao) # Extrai para subpasta
        except zipfile.BadZipFile:
            logger.error(f"[{pid}] Polars: Arquivo {path_zip_file} não é um arquivo ZIP válido")
            return False
        except Exception as e_zip:
            logger.exception(f"[{pid}] Polars: Erro durante a extração do ZIP {path_zip_file}")
            return False

        # --- 2. Encontrar e Processar CSVs com Polars --- 
        logger.debug(f"[{pid}] Polars Fase 2: Leitura CSV e Transformações")
        data_files_encontrados = []
        try:
            # Busca CSVs apenas dentro da subpasta de extração
            for root, _, files in os.walk(path_extracao):
                for file in files:
                    # Busca arquivos que contenham "CSV" no nome (case-insensitive)
                    if "CSV" in file.upper(): 
                        data_files_encontrados.append(os.path.join(root, file))
        except Exception as e_walk:
             logger.exception(f"[{pid}] Polars: Erro ao percorrer o diretório de extração {path_extracao}")
             return False

        if not data_files_encontrados:
            logger.warning(f"[{pid}] Polars: Nenhum arquivo contendo 'CSV' encontrado em {path_extracao} para {zip_file}." ) # Aviso em vez de erro
            # Retorna False pois não há dados para processar (não é um sucesso)
            return False
        else:
            logger.info(f"[{pid}] Polars: Encontrados {len(data_files_encontrados)} arquivos CSV para processar em {zip_file}." )
            # Lista para armazenar DataFrames Polars lidos
            dataframes = []
            processamento_csv_ok = False
            
            # Processar cada arquivo CSV individualmente (encontrado na subpasta)
            for csv_path in data_files_encontrados:
                csv_file_name = os.path.basename(csv_path)
                try:
                    df_polars = process_csv_file_polars(csv_path)
                    
                    if df_polars is not None and not df_polars.is_empty():
                        dataframes.append(df_polars)
                        logger.info(f"[{pid}] Polars: CSV {csv_file_name} processado com sucesso: {df_polars.height} linhas")
                        processamento_csv_ok = True
                    elif df_polars is not None and df_polars.is_empty():
                         logger.warning(f"[{pid}] Polars: CSV {csv_file_name} resultou em DataFrame vazio.")
                    # Se df_polars for None, o erro já foi logado em process_csv_file_polars
                except Exception as e:
                    logger.error(f"[{pid}] Polars: Erro ao processar o CSV {csv_file_name}: {str(e)}")
            
            # Verificar se temos DataFrames para processar
            if not processamento_csv_ok or not dataframes:
                logger.error(f"[{pid}] Polars: Nenhum DataFrame Polars válido gerado a partir do ZIP {zip_file}. Nenhum Parquet será gerado.")
                # Mantém success = False
                return False
            else:
                # Concatenar os DataFrames se houver mais de um
                df_final_polars = None
                logger.info(f"[{pid}] Polars: Concatenando {len(dataframes)} DataFrames para {zip_file}...")
                try:
                    if len(dataframes) > 1:
                        df_final_polars = pl.concat(dataframes, how="vertical")
                    else:
                        df_final_polars = dataframes[0]
                    logger.info(f"[{pid}] Polars: Concatenação concluída. DataFrame final com {df_final_polars.height} linhas.")
                    del dataframes # Liberar memória
                    gc.collect()
                except Exception as e_concat:
                     logger.exception(f"[{pid}] Polars: Erro ao concatenar DataFrames para {zip_file}")
                     df_final_polars = None
                     # Mantém success = False
                
                if df_final_polars is not None and not df_final_polars.is_empty():
                    # Aplicar transformações
                    logger.info(f"[{pid}] Polars: Aplicando transformações para {zip_file}...")
                    df_final_polars = apply_empresa_transformations_polars(df_final_polars)
                    logger.info(f"[{pid}] Polars: Transformações aplicadas para {zip_file}.")
                    
                    # --- 3. Salvar Parquet (Principal) --- 
                    logger.info(f"[{pid}] Polars Fase 3: Salvando Parquet principal para {zip_file}..." )
                    # Passa o prefixo do nome do zip para a função de salvar
                    parquet_main_saved = create_parquet_polars(df_final_polars, 'empresas', path_parquet, zip_filename_prefix)
                    if parquet_main_saved:
                         logger.info(f"[{pid}] Polars: Parquet principal salvo para {zip_file}.")
                    else:
                         logger.error(f"[{pid}] Polars: Falha ao salvar Parquet principal para {zip_file}." )
                         # Mantém success = False se falhar aqui

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
                                # Passa o prefixo do nome do zip para a função de salvar
                                private_saved = create_parquet_polars(df_privada, 'empresa_privada', path_parquet, zip_filename_prefix)
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


def process_empresa_with_polars(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Processa os dados de empresas usando Polars."""
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de EMPRESAS com Polars (create_private={create_private})')
    logger.info('=' * 50)
    
    try:
        zip_files = [f for f in os.listdir(path_zip) 
                    if f.startswith('Empresa') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Empresas encontrado.')
            return True
        
        # LIMPEZA MOVIDA PARA CÁ (ANTES DO LOOP)
        output_dir_empresas = os.path.join(path_parquet, 'empresas')
        output_dir_privada = os.path.join(path_parquet, 'empresa_privada')
        try:
            file_delete(output_dir_empresas)
            logger.info(f'Diretório {output_dir_empresas} limpo antes do processamento Polars.')
            if create_private:
                 file_delete(output_dir_privada)
                 logger.info(f'Diretório {output_dir_privada} limpo antes do processamento Polars.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretórios de saída antes do processamento Polars: {str(e)}')

        # Forçar coleta de lixo antes de iniciar o processamento
        gc.collect()
            
        success_count = 0
        arquivos_sem_dados = []
        for zip_file in track(zip_files, description="[cyan]Processing Empresas ZIPs (Polars)..."):
            result = process_single_zip_polars(
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet,
                create_private=create_private
            )
            if result:
                success_count += 1
                logger.info(f"Arquivo {zip_file} processado com sucesso usando Polars")
            else:
                # Registra arquivos que não tiveram dados válidos para processar
                arquivos_sem_dados.append(zip_file)
            
            # Forçar coleta de lixo após cada arquivo
            gc.collect()
        
        if success_count == 0:
            logger.warning("Nenhum arquivo ZIP de Empresas foi processado com sucesso usando Polars.")
        else:
            logger.info(f"Processados com sucesso {success_count} de {len(zip_files)} arquivos ZIP de Empresas usando Polars.")
            
        if arquivos_sem_dados:
            logger.warning(f'Os seguintes arquivos não continham dados válidos para processar: {", ".join(arquivos_sem_dados)}')
        
        return success_count > 0

    except Exception as e:
        logger.error(f'Erro no processamento principal de Empresas com Polars: {str(e)}')
        return False
