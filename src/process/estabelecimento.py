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

from ..config import config
from ..utils import file_delete, check_disk_space, verify_csv_integrity
from ..utils.folders import get_output_path, ensure_correct_folder_structure
import inspect

logger = logging.getLogger(__name__)


def process_estabelecimento(path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa os dados de estabelecimentos usando Polars.
    
    Args:
        path_zip: Caminho para o diretório dos arquivos ZIP
        path_unzip: Caminho para o diretório de extração
        path_parquet: Caminho para o diretório dos arquivos parquet
        uf_subset: Sigla da UF para criar um subset (opcional)
        
    Returns:
        bool: True se o processamento foi bem-sucedido, False caso contrário
    """
    return process_estabelecimento_with_polars(path_zip, path_unzip, path_parquet, uf_subset)


def process_data_file_polars(data_path: str):
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
    original_column_names = config.estabelecimento_columns

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


def apply_estabelecimento_transformations_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações específicas para Estabelecimentos usando Polars."""
    logger.info("Aplicando transformações em Estabelecimentos com Polars...")
    
    # Renomeação de colunas
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico',
        'identificador_matriz_filial': 'matriz_filial',
        'nome_fantasia': 'nome_fantasia',
        'situacao_cadastral': 'codigo_situacao_cadastral',
        'data_situacao_cadastral': 'data_situacao_cadastral',
        'motivo_situacao_cadastral': 'codigo_motivo_situacao_cadastral',
        'data_inicio_atividade': 'data_inicio_atividades',
        'cnae_fiscal_principal': 'codigo_cnae'
    }
    
    for old_col, new_col in rename_mapping.items():
        if old_col in df.columns and old_col != new_col:
            df = df.rename({old_col: new_col})
    
    # Conversão de colunas numéricas
    int_cols = ['matriz_filial', 'codigo_situacao_cadastral', 
                'codigo_motivo_situacao_cadastral', 'codigo_cnae', 
                'codigo_municipio']
    
    int_expressions = []
    for col in int_cols:
        if col in df.columns:
            int_expressions.append(
                pl.col(col).cast(pl.Int64, strict=False)
            )
    
    if int_expressions:
        df = df.with_columns(int_expressions)
    
    # Conversão de datas
    date_cols = ['data_situacao_cadastral', 'data_inicio_atividades']
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
    
    # Criação do CNPJ completo
    if all(col in df.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
        df = df.with_columns([
            (
                pl.col('cnpj_basico').cast(pl.Utf8).str.pad_start(8, '0') + 
                pl.col('cnpj_ordem').cast(pl.Utf8).str.pad_start(4, '0') + 
                pl.col('cnpj_dv').cast(pl.Utf8).str.pad_start(2, '0')
            ).alias('cnpj')
        ])
    
    # Remoção de colunas
    cols_to_drop = ['cnpj_ordem', 'cnpj_dv', 'tipo_logradouro', 'logradouro', 
                    'numero', 'complemento', 'bairro', 'ddd1', 'telefone1', 
                    'ddd2', 'telefone2', 'ddd_fax', 'fax', 'pais',
                    'correio_eletronico', 'situacao_especial', 
                    'data_situacao_especial', 'nome_cidade_exterior']
    
    cols_to_drop = [col for col in cols_to_drop if col in df.columns]
    if cols_to_drop:
        df = df.drop(cols_to_drop)
    
    return df


def create_parquet_polars(df: pl.DataFrame, table_name: str, path_parquet: str, zip_filename_prefix: str, partition_size: int = 500000) -> bool:
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
            match = re.search(r'(20\d{2}-\d{2})', path_parquet)
            if match:
                remote_folder = match.group(1)
            else:
                # Tentar extrair do prefixo do arquivo
                match = re.search(r'(20\d{2}-\d{2})', zip_filename_prefix)
                if match:
                    remote_folder = match.group(1)
                else:
                    # Último recurso: usar um valor padrão
                    remote_folder = os.path.basename(os.path.dirname(path_parquet)) or "dados"
        
        logger.info(f"Pasta remota identificada em create_parquet_polars: {remote_folder}")
        
        # Usando a função que garante a estrutura correta de pastas
        output_dir = ensure_correct_folder_structure(path_parquet, remote_folder, table_name)
        
        logger.info(f"Colunas do DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {df.columns}")
        
        total_rows = df.height
        if total_rows == 0:
            logger.warning(f"DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) está vazio. Nenhum Parquet será salvo.")
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
        logger.error(f"Erro ao criar arquivo Parquet com Polars: {str(e)}")
        return False


def process_single_zip_polars(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa um único arquivo ZIP usando Polars para eficiência."""
    pid = os.getpid()
    logger.info(f"[{pid}] Iniciando processamento Polars para: {zip_file}")
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
            logger.error(f"[{pid}] Polars: Arquivo ZIP {zip_path} não existe")
            return False
            
        if os.path.getsize(zip_path) == 0:
            logger.error(f"[{pid}] Polars: Arquivo ZIP {zip_path} está vazio (tamanho 0 bytes)")
            return False
            
        # Verificar se é um arquivo ZIP válido antes de tentar extrair
        try:
            with zipfile.ZipFile(zip_path, 'r') as test_zip:
                file_list = test_zip.namelist()
                if not file_list:
                    logger.warning(f"[{pid}] Polars: Arquivo ZIP {zip_path} existe mas não contém arquivos")
                    return False
                else:
                    logger.info(f"[{pid}] Polars: Arquivo ZIP {zip_path} é válido e contém {len(file_list)} arquivos")
        except zipfile.BadZipFile:
            logger.error(f"[{pid}] Polars: Arquivo {zip_path} não é um arquivo ZIP válido")
            return False
        except Exception as e_test_zip:
            logger.error(f"[{pid}] Polars: Erro ao verificar arquivo ZIP {zip_path}: {str(e_test_zip)}")
            return False
        
        # Extrair arquivo ZIP
        logger.info(f"[{pid}] Extraindo {zip_file} para {extract_dir}")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
        except zipfile.BadZipFile:
            logger.error(f"[{pid}] Polars: Arquivo {zip_path} não é um arquivo ZIP válido")
            return False
        except Exception as e_zip:
            logger.error(f"[{pid}] Polars: Erro durante a extração do ZIP {zip_path}: {e_zip}")
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
            
            try:
                df = process_data_file_polars(data_path)
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
            df_transformed = apply_estabelecimento_transformations_polars(df_final)
            
            # --- 3. Salvar Parquet (Principal) ---
            main_saved = False
            
            # Usar diretamente path_parquet sem adicionar a pasta remota
            # A função ensure_correct_folder_structure já adiciona a pasta remota se necessário
            logger.info(f"[{pid}] Polars: Usando path_parquet direto: {path_parquet}")
            
            try:
                # Usamos diretamente path_parquet e a função ensure_correct_folder_structure dentro de create_parquet_polars
                # vai garantir a estrutura correta de pastas
                main_saved = create_parquet_polars(df_transformed, 'estabelecimentos', path_parquet, zip_filename_prefix)
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
                        subset_saved = create_parquet_polars(df_subset, subset_table_name, path_parquet, zip_filename_prefix)
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
            logger.error(f"[{pid}] Erro durante concatenação ou transformação Polars para {zip_file}: {e_concat_transform}")
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
            except Exception as e_clean:
                logger.warning(f"[{pid}] Erro ao limpar diretório de extração: {e_clean}")
        
        # Forçar coleta de lixo novamente
        gc.collect()
    
    return success


def process_estabelecimento_with_polars(path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa os dados de estabelecimentos usando Polars.
    
    Args:
        path_zip: Caminho para o diretório dos arquivos ZIP
        path_unzip: Caminho para o diretório de extração
        path_parquet: Caminho para o diretório dos arquivos parquet
        uf_subset: Sigla da UF para criar um subset (opcional)
        
    Returns:
        bool: True se o processamento foi bem-sucedido, False caso contrário
    """
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de ESTABELECIMENTOS com Polars')
    logger.info('=' * 50)
    
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
        
        # Processar com Polars em paralelo
        max_workers = os.cpu_count() or 4  # Limitar ao número de CPUs
        success = False
        arquivos_com_falha = []
        
        # Processar todos os arquivos ZIP em paralelo
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Mapear cada arquivo ZIP para ser processado com Polars
            futures = {
                executor.submit(
                    process_single_zip_polars, 
                    zip_file, 
                    path_zip, 
                    path_unzip, 
                    path_parquet,
                    uf_subset
                ): zip_file for zip_file in zip_files
            }
            
            # Coletar resultados à medida que são concluídos
            for future in concurrent.futures.as_completed(futures):
                zip_file = futures[future]
                try:
                    result = future.result()
                    if result:
                        success = True
                        logger.info(f"Arquivo {zip_file} processado com sucesso usando Polars")
                    else:
                        arquivos_com_falha.append(zip_file)
                        logger.warning(f"Falha no processamento do arquivo {zip_file} com Polars")
                except Exception as e:
                    arquivos_com_falha.append(zip_file)
                    logger.error(f"Exceção no processamento do arquivo {zip_file} com Polars: {str(e)}")
        
        if not success:
            logger.warning('Nenhum arquivo ZIP de Estabelecimentos foi processado com sucesso com Polars.')
            
        if arquivos_com_falha:
            logger.warning(f'Os seguintes arquivos falharam no processamento: {", ".join(arquivos_com_falha)}')
        
        return success
    except Exception as e:
        logger.error(f'Erro no processamento principal de Estabelecimentos com Polars: {str(e)}')
        traceback.print_exc()
        return False
