import logging
import os
import zipfile
import polars as pl
import numpy as np
import gc
import shutil
from multiprocessing import Pool
import re
import concurrent.futures
import traceback

from ..config import config
from ..utils import file_delete, verify_csv_integrity
from ..utils.folders import get_output_path, ensure_correct_folder_structure

logger = logging.getLogger(__name__)

def process_simples(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados do Simples Nacional."""
    return process_simples_files(path_zip, path_unzip, path_parquet)
    
def process_data_file_polars(data_path: str):
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
    original_column_names = config.simples_columns

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

def apply_simples_transformations_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações específicas para o Simples Nacional usando Polars."""
    logger.info("Aplicando transformações em Simples Nacional com Polars...")
    
    # Renomeação de colunas se necessário
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico',
        'opcao_pelo_simples': 'opcao_pelo_simples',
        'data_opcao_pelo_simples': 'data_opcao_pelo_simples',
        'data_exclusao_do_simples': 'data_exclusao_do_simples',
        'opcao_pelo_mei': 'opcao_pelo_mei',
        'data_opcao_pelo_mei': 'data_opcao_pelo_mei',
        'data_exclusao_do_mei': 'data_exclusao_do_mei'
    }
    
    # Filtrar para manter apenas colunas que existem no DataFrame
    rename_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
    
    # Aplicar renomeação se houver colunas para renomear
    if rename_mapping:
        df = df.rename(rename_mapping)
    
    # Converter colunas de data
    date_cols = ['data_opcao_pelo_simples', 'data_exclusao_do_simples', 
                 'data_opcao_pelo_mei', 'data_exclusao_do_mei']
    
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
    
    # Converter campos de opção (S/N para booleanos)
    bool_cols = ['opcao_pelo_simples', 'opcao_pelo_mei']
    bool_expressions = []
    
    for col in bool_cols:
        if col in df.columns:
            bool_expressions.append(
                pl.when(pl.col(col) == "S")
                .then(True)
                .otherwise(False)
                .alias(col)
            )
    
    if bool_expressions:
        df = df.with_columns(bool_expressions)
    
    return df

def create_parquet_polars(df: pl.DataFrame, table_name: str, path_parquet: str, 
                          zip_filename_prefix: str, remote_folder: str, 
                          partition_size: int = 500000) -> bool:
    """
    Salva DataFrame Polars em arquivos Parquet particionados.
    
    Args:
        df: DataFrame Polars
        table_name: Nome da tabela (subpasta)
        path_parquet: Caminho base para os arquivos parquet
        zip_filename_prefix: Prefixo derivado do nome do arquivo ZIP original
        remote_folder: Nome da pasta remota
        partition_size: Tamanho de cada partição (número de linhas)
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
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

def process_single_zip_polars(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, remote_folder: str = None) -> bool:
    """Processa um único arquivo ZIP com dados do Simples Nacional.
    
    Esta função é chamada pelo download assíncrono para processar arquivos
    imediatamente após o download.
    
    Args:
        zip_file: Nome do arquivo ZIP a ser processado
        path_zip: Caminho para o diretório contendo o arquivo ZIP
        path_unzip: Caminho para o diretório temporário de extração
        path_parquet: Caminho para o diretório onde os dados processados serão salvos
        remote_folder: Nome da pasta remota (opcional, pode ser determinada automaticamente)
        
    Returns:
        bool: True se o processamento foi bem-sucedido, False caso contrário
    """
    pid = os.getpid()
    logger.info(f"[{pid}] Iniciando processamento de {zip_file}")
    extract_dir = os.path.join(path_unzip, os.path.splitext(zip_file)[0])
    zip_filename_prefix = os.path.splitext(zip_file)[0]
    success = False
    
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
        
        # Extrair o arquivo ZIP
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            logger.info(f"[{pid}] Arquivo {zip_file} extraído com sucesso")
        except Exception as e:
            logger.error(f"[{pid}] Erro ao extrair arquivo {zip_file}: {str(e)}")
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
            df_transformed = apply_simples_transformations_polars(df_final)
            
            # Salvar Parquet
            main_saved = False
            try:
                main_saved = create_parquet_polars(
                    df_transformed, 
                    'simples', 
                    path_parquet, 
                    zip_filename_prefix, 
                    remote_folder
                )
                logger.info(f"[{pid}] Parquet salvo com sucesso para {zip_file}")
            except Exception as e:
                logger.error(f"[{pid}] Erro ao salvar Parquet para {zip_file}: {e}")
            
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

def process_simples_files(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados do Simples Nacional."""
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento do SIMPLES NACIONAL')
    logger.info('=' * 50)
    
    try:
        # Encontrar arquivos ZIP do simples
        zip_files = [f for f in os.listdir(path_zip) 
                     if f.startswith('Simples') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP do Simples Nacional encontrado.')
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
        
        # Criar o diretório de saída com estrutura correta
        # Armazenar o valor de retorno para criar efetivamente o diretório
        output_dir = ensure_correct_folder_structure(path_parquet, remote_folder, 'simples')
        logger.info(f"Diretório de saída para simples: {output_dir}")
        
        # Limpar diretório de saída antes do processamento
        try:
            file_delete(output_dir)
            logger.info(f'Diretório {output_dir} limpo antes do processamento.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretório de saída {output_dir}: {str(e)}')
        
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
                    remote_folder
                ): zip_file for zip_file in zip_files
            }
            
            # Coletar resultados à medida que são concluídos
            for future in concurrent.futures.as_completed(futures):
                zip_file = futures[future]
                try:
                    result = future.result()
                    if result:
                        success = True
                        logger.info(f"Arquivo {zip_file} processado com sucesso")
                    else:
                        arquivos_com_falha.append(zip_file)
                        logger.warning(f"Falha no processamento do arquivo {zip_file}")
                except Exception as e:
                    arquivos_com_falha.append(zip_file)
                    logger.error(f"Exceção no processamento do arquivo {zip_file}: {str(e)}")
        
        if not success:
            logger.warning('Nenhum arquivo ZIP do Simples Nacional foi processado com sucesso.')
            
        if arquivos_com_falha:
            logger.warning(f'Os seguintes arquivos falharam no processamento: {", ".join(arquivos_com_falha)}')
        
        return success
    except Exception as e:
        logger.error(f'Erro no processamento principal do Simples Nacional: {str(e)}')
        traceback.print_exc()
        return False
