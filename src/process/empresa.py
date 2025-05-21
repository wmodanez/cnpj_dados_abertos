import logging
import os
import zipfile
import pandas as pd
import numpy as np
import dask.dataframe as dd
from dask import delayed
import polars as pl
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
    process_csv_files_parallel, process_csv_to_df, verify_csv_integrity, 
    create_parquet_filename
)
from src.utils.dask_manager import DaskManager
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


def create_parquet(df, table_name, path_parquet, zip_filename_prefix: str):
    """Converte um DataFrame Dask para formato parquet, usando prefixo.

    Args:
        df: DataFrame Dask a ser convertido
        table_name: Nome da tabela (subpasta)
        path_parquet: Caminho base para os arquivos parquet (diretório pai)
        zip_filename_prefix: Prefixo derivado do nome do arquivo ZIP original
    """
    output_dir = os.path.join(path_parquet, table_name)

    # Limpa o diretório antes de criar os novos arquivos - MOVIDO PARA ANTES DO LOOP
    # try:
    #     file_delete(output_dir)
    #     logger.debug(f'Diretório {output_dir} limpo antes de criar novos arquivos parquet')
    # except Exception as e:
    #     logger.warning(f'Não foi possível limpar diretório {output_dir}: {str(e)}')

    # Garante que o diretório exista
    os.makedirs(output_dir, exist_ok=True)

    # Log das colunas antes de salvar
    logger.debug(f"Colunas do DataFrame Dask '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {list(df.columns)}")

    # Configura o nome dos arquivos parquet com prefixo da tabela e do zip
    df.to_parquet(
        output_dir,
        engine='pyarrow',  # Especifica o engine
        write_index=False,
        # name_function=lambda i: create_parquet_filename(table_name, i)
        # Usa prefixo e índice do chunk para nome único
        name_function=lambda i: f"{zip_filename_prefix}_{table_name}_{i:03d}.parquet"
    )

    return df


def process_csv_file(csv_path):
    """
    Processa um único arquivo CSV de empresa e retorna um DataFrame Dask.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Dask ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa as colunas e dtypes da configuração global
    original_column_names = config.empresa_columns
    dtype_dict = config.empresa_dtypes

    try:
        # Passa os nomes das colunas, separador e encoding da config
        df = process_csv_to_df(
            csv_path, 
            dtype=dtype_dict, 
            column_names=original_column_names,
            separator=config.file.separator, # Usa separador da config
            encoding=config.file.encoding,   # Usa encoding da config
            na_filter=False # Como em simples.py
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)}: {str(e)}')
        return None


def apply_empresa_transformations_dask(ddf):
    """Aplica transformações ao Dask DataFrame de empresas."""
    # Renomear colunas para minúsculas
    ddf.columns = ddf.columns.str.lower()

    # Colunas para converter para numérico
    colunas_numericas = [
        'capital_social_da_empresa',
        'cnpj', # Mantido como string
        'cnae_fiscal_principal', # Geralmente string
        'opcao_pelo_simples', # Tratar como string/categoria ou booleano?
        'porte_da_empresa', # Geralmente string/categoria
        # Adicione outras colunas numéricas se necessário
    ]

    # Colunas para converter para data
    colunas_data = [
        'data_de_inicio_atividade',
        'data_situacao_cadastral',
        'data_opcao_pelo_simples',
        'data_exclusao_do_simples',
        'data_opcao_pelo_mei',
        'data_exclusao_do_mei'
    ]

    # Converter colunas numéricas diretamente no Dask DataFrame
    for col in colunas_numericas:
        if col in ddf.columns:
            # Usar dd.to_numeric diretamente
            # Especificar meta para ajudar na inferência, se necessário,
            # mas geralmente dd.to_numeric lida bem com isso.
            # Vamos assumir float64 como padrão para numéricos com possíveis NaNs
            try:
                 # Tenta converter diretamente, Dask deve inferir
                 ddf[col] = dd.to_numeric(ddf[col], errors='coerce')
                 # Opcional: Fornecer meta explicitamente se a inferência falhar
                 # ddf[col] = dd.to_numeric(ddf[col], errors='coerce').astype('float64')
                 # Ou usando map_partitions com meta (alternativa mais complexa):
                 # ddf[col] = ddf[col].map_partitions(pd.to_numeric, errors='coerce', meta=pd.Series(dtype='float64'))
            except Exception as e:
                print(f"Aviso: Falha ao converter coluna numérica '{col}': {e}. Mantendo como object.")
                logger.warning(f"Falha ao converter coluna numérica '{col}': {e}. Mantendo como object.")

    # Converter colunas de data
    for col in colunas_data:
        if col in ddf.columns:
             try:
                 # Usar dd.to_datetime diretamente
                 ddf[col] = dd.to_datetime(ddf[col], format='%Y%m%d', errors='coerce')
                 # Opcional: Fornecer meta explicitamente se a inferência falhar
                 # ddf[col] = dd.to_datetime(ddf[col], format='%Y%m%d', errors='coerce', meta=pd.Series(dtype='datetime64[ns]'))
             except Exception as e:
                 print(f"Aviso: Falha ao converter coluna de data '{col}': {e}. Mantendo como object.")
                 logger.warning(f"Falha ao converter coluna de data '{col}': {e}. Mantendo como object.")

    # Outras transformações específicas podem ser adicionadas aqui
    # Exemplo: Converter colunas categóricas
    colunas_categoricas = [
        'natureza_juridica', 'qualificacao_do_responsavel', 'situacao_cadastral',
        'motivo_situacao_cadastral', 'ente_federativo_responsavel', 'opcao_pelo_mei'
    ]
    for col in colunas_categoricas:
        if col in ddf.columns:
             # NÃO converter 'ente_federativo_responsavel' para categoria por enquanto
             if col == 'ente_federativo_responsavel':
                 logger.warning(f"Mantendo coluna '{col}' como string/object para evitar erro de schema Parquet.")
                 continue # Pula a conversão para esta coluna

             # Usar astype('category') que Dask geralmente lida bem para as outras
             ddf[col] = ddf[col].astype('category')
             # Opcional: com meta se necessário
             # ddf[col] = ddf[col].astype('category').cat.as_known()

    # --- Tratamento específico para ente_federativo_responsavel ---
    col_efr = 'ente_federativo_responsavel'
    if col_efr in ddf.columns:
        logger.info(f"Aplicando tratamento especial para coluna: {col_efr}")
        try:
            # 1. Tentar converter para numérico, erros viram NaN
            ddf[col_efr] = dd.to_numeric(ddf[col_efr], errors='coerce')
            # 2. Preencher NaN (nulos/vazios/erros de conversão) com 999999
            ddf[col_efr] = ddf[col_efr].fillna(999999)
            # 3. Garantir que a coluna seja float64 (ou int64 se preferir, mas float é mais seguro pós-fillna)
            ddf[col_efr] = ddf[col_efr].astype('float64') # Ou int64 se tiver certeza que não haverá floats
            logger.info(f"Coluna '{col_efr}' convertida para numérico com nulos substituídos por 999999.")
        except Exception as e:
            logger.error(f"Erro ao aplicar tratamento especial para '{col_efr}': {e}. Coluna pode permanecer como object.")
            # Opcional: Adicionar fallback ou deixar como está

    logger.debug("Transformações Dask aplicadas.")

    # --- Extração de CPF da razao_social (Dask) ---
    if 'razao_social' in ddf.columns:
        logger.info("Extraindo CPF da razao_social (Dask)...")

        # Extrai formato com máscara xxx.xxx.xxx-xx
        # Usar astype('string') para garantir tipo nullable
        cpf_com_mascara = ddf['razao_social'].str.extract(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', expand=False).astype('string')

        # Extrai formato 11 dígitos
        cpf_sem_mascara = ddf['razao_social'].str.extract(r'(\d{11})', expand=False).astype('string')

        # Combina usando fillna (prioriza cpf_com_mascara)
        ddf['CPF_temp'] = cpf_com_mascara.fillna(cpf_sem_mascara)

        # Limpa a máscara (pontos e traço)
        ddf['CPF'] = ddf['CPF_temp'].str.replace(r'[.\-]', '', regex=True)

        # Opcional: Validar comprimento (pode ser complexo/custoso em Dask, omitido por padrão)
        # def validate_cpf_len(series):
        #     return series.where(series.str.len() == 11)
        # ddf['CPF'] = ddf['CPF'].map_partitions(validate_cpf_len, meta=pd.Series(dtype='string'))

        # Remove a coluna temporária
        ddf = ddf.drop(columns=['CPF_temp'])

        logger.info("Extração de CPF concluída (Dask).")

        # --- Remoção do CPF da razao_social (Dask) ---
        logger.info("Removendo CPF da razao_social (Dask)...")
        # Remove primeiro o padrão com máscara, depois o sem máscara
        # Nota: Dask pode ter limitações com regex complexos como lookbehind.
        # Esta abordagem remove ambos os padrões se existirem.
        ddf['razao_social'] = ddf['razao_social'].str.replace(r'\d{3}\.\d{3}\.\d{3}-\d{2}', '', regex=True)
        ddf['razao_social'] = ddf['razao_social'].str.replace(r'\d{11}', '', regex=True)
        ddf['razao_social'] = ddf['razao_social'].str.strip()
        logger.info("Remoção do CPF da razao_social concluída (Dask).")

    # Garante que a coluna CPF exista com o tipo correto se razao_social existia mas CPF não foi criada
    elif 'razao_social' in ddf.columns and 'CPF' not in ddf.columns:
         logger.warning("Coluna 'razao_social' existe, mas 'CPF' não foi criada após extração. Criando coluna CPF com nulos.")
         ddf['CPF'] = None
         ddf['CPF'] = ddf['CPF'].astype('string') # Definir o tipo string nullable

    return ddf


def process_empresa(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Processa os dados de empresas usando Dask."""
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de EMPRESAS com Dask (create_private={create_private})') # Log da flag
    logger.info('=' * 50)
    
    try:
        # Usa o cliente Dask já configurado
        client = DaskManager.get_instance().client
        
        # Lista arquivos ZIP
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
            logger.info(f'Diretório {output_dir_empresas} limpo antes do processamento Dask.')
            if create_private:
                 file_delete(output_dir_privada)
                 logger.info(f'Diretório {output_dir_privada} limpo antes do processamento Dask.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretórios de saída antes do processamento Dask: {str(e)}')

        # Processamento paralelo dos ZIPs
        futures = [
            client.submit(
                process_single_zip,
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet,
                create_private=create_private # Passa a flag para a função delayed
            )
            for zip_file in zip_files
        ]
        
        # Coleta resultados
        results = client.gather(futures)
        return all(results)
            
    except Exception as e:
        logger.error(f'Erro no processamento principal: {str(e)}')
        return False


@delayed
def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, create_private: bool) -> bool:
    """Processa um único arquivo ZIP de forma otimizada com Dask."""
    logger.info(f"Iniciando processamento Dask para {zip_file} (create_private={create_private})")
    zip_filename_prefix = os.path.splitext(zip_file)[0] # Prefixo para nomes de arquivo
    path_extracao = ""
    try:
        # --- 1. Extração --- 
        logger.debug(f"[{os.getpid()}] Fase 1: Extração")
        nome_arquivo = os.path.splitext(zip_file)[0]
        path_extracao = os.path.join(path_unzip, nome_arquivo)
        logger.debug(f"[{os.getpid()}] Caminho de extração definido: {path_extracao}")
        if os.path.exists(path_extracao):
            logger.warning(f"[{os.getpid()}] Diretório de extração {path_extracao} já existe. Removendo.")
            try:
                shutil.rmtree(path_extracao)
                logger.debug(f"[{os.getpid()}] Diretório antigo removido.")
            except Exception as e_rem_dir:
                logger.error(f"[{os.getpid()}] Erro ao remover diretório de extração antigo {path_extracao}: {e_rem_dir}")
                # Considerar se deve parar ou continuar
                # return {'sucesso': False, 'tempo': time.time() - start_time}
        
        try:
            os.makedirs(path_extracao, exist_ok=True)
            logger.debug(f"[{os.getpid()}] Diretório de extração criado/garantido: {path_extracao}")
        except Exception as e_make_dir:
             logger.exception(f"[{os.getpid()}] Erro CRÍTICO ao criar diretório de extração {path_extracao}")
             return False # Retorna False em vez de dict com start_time

        path_zip_file = os.path.join(path_zip, zip_file)
        logger.info(f"[{os.getpid()}] Extraindo {zip_file} para {path_extracao}...")
        try:
            with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
                arquivos_extraidos = zip_ref.namelist()
                zip_ref.extractall(path_extracao)
            logger.info(f"[{os.getpid()}] Extração de {zip_file} concluída. Arquivos na extração: {arquivos_extraidos}")
        except Exception as e_zip:
            logger.exception(f"[{os.getpid()}] Erro durante a extração do ZIP {path_zip_file}")
            return False
        
        # --- 2. Conversão --- 
        # Processa o arquivo CSV principal
        ddf = apply_empresa_transformations_dask(process_csv_to_df(
            path_extracao, 
            dtype=config.empresa_dtypes,
            column_names=config.empresa_columns
        ))
        if ddf is not None:
            create_parquet(ddf, 'empresas', path_parquet, zip_filename_prefix)
        
        # Processa o arquivo CSV secundário (empresa_privada), se create_private for True
        if create_private:
            ddf_privado = ddf[
                (ddf['natureza_juridica'] >= 2046) & 
                (ddf['natureza_juridica'] <= 2348)
            ]
            create_parquet(ddf_privado, 'empresa_privada', path_parquet, zip_filename_prefix)
        
        return True
    except Exception as e:
        logger.error(f'Erro processando {zip_file}: {str(e)}')
        return False


# ----- Implementação para Pandas -----

def process_csv_file_pandas(csv_path):
    """
    Processa um único arquivo CSV de empresa usando Pandas.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Pandas ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa colunas e dtypes da config
    original_column_names = config.empresa_columns
    dtype_dict = config.empresa_dtypes

    try:
        # Usa pandas.read_csv com os parâmetros apropriados
        df = pd.read_csv(
            csv_path,
            sep=config.file.separator,
            encoding=config.file.encoding,
            names=original_column_names,
            header=None,
            dtype=str,  # Inicialmente lê tudo como string para evitar inferências incorretas
            quoting=1,  # QUOTE_MINIMAL
            na_filter=False
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)} com Pandas: {str(e)}')
        return None


def apply_empresa_transformations_pandas(df):
    """Aplica transformações específicas para Empresas usando Pandas."""
    logger.info("Aplicando transformações em Empresas com Pandas...")
    
    # Renomeação de colunas
    rename_mapping = {
        'cnpj_basico': 'cnpj',
        'razao_social_nome_empresarial': 'razao_social',
        'natureza_juridica': 'natureza_juridica',
        'qualificacao_do_responsavel': 'qualificacao_responsavel',
        'capital_social_da_empresa': 'capital_social',
        'porte_da_empresa': 'porte_empresa',
        'ente_federativo_responsavel': 'ente_federativo_responsavel'
    }
    
    # Filtrar para incluir apenas colunas que existem no DataFrame
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
    df = df.rename(columns=actual_rename_mapping)
    
    # Conversão de tipos numéricos
    int_cols = ['natureza_juridica', 'qualificacao_responsavel', 'porte_empresa']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Conversão do capital_social
    if 'capital_social' in df.columns:
        df['capital_social'] = (df['capital_social']
                                .astype(str)
                                .str.replace(',', '.', regex=False)
                                .pipe(lambda x: pd.to_numeric(x, errors='coerce')))
    
    # --- Extração de CPF da razao_social (Pandas) ---
    if 'razao_social' in df.columns:
        logger.info("Extraindo CPF da razao_social (Pandas)...")
        # Tenta extrair formato xxx.xxx.xxx-xx
        cpf_com_mascara = df['razao_social'].str.extract(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', expand=False)
        # Tenta extrair formato ddddddddddd
        cpf_sem_mascara = df['razao_social'].str.extract(r'(\d{11})', expand=False)

        # Combina os resultados, dando prioridade ao formato com máscara
        df['CPF_temp'] = cpf_com_mascara.fillna(cpf_sem_mascara)

        # Limpa a máscara (pontos e traço)
        df['CPF'] = df['CPF_temp'].str.replace(r'[.\-]', '', regex=True)

        # Remove a coluna temporária
        df = df.drop(columns=['CPF_temp'])

        # Valida se o resultado tem 11 dígitos (remove extrações incorretas)
        df['CPF'] = df['CPF'].where(df['CPF'].str.len() == 11)

        logger.info("Extração de CPF concluída (Pandas).")

        # --- Remoção do CPF da razao_social (Pandas) ---
        logger.info("Removendo CPF da razao_social (Pandas)...")
        # Remove primeiro o padrão com máscara, depois o sem máscara
        df['razao_social'] = df['razao_social'].str.replace(r'\d{3}\.\d{3}\.\d{3}-\d{2}', '', regex=True)
        # Usar lookbehind (?<![.\-]) para não remover 11 digitos se já faziam parte de um CPF mascarado removido
        # E \b para garantir que são 11 dígitos como uma "palavra" separada
        df['razao_social'] = df['razao_social'].str.replace(r'(?<![\.\-])\b\d{11}\b', '', regex=True)
        df['razao_social'] = df['razao_social'].str.strip()
        logger.info("Remoção do CPF da razao_social concluída (Pandas).")

    # Garante que a coluna CPF exista se razao_social existia
    elif 'razao_social' in df.columns and 'CPF' not in df.columns:
        df['CPF'] = pd.NA # Cria a coluna com valores nulos (Pandas >= 1.0)

    return df


def create_parquet_chunks_pandas(df, table_name, path_parquet, zip_filename_prefix: str, chunk_size=100000):
    """Converte um DataFrame Pandas para múltiplos arquivos parquet usando chunks.
       Usa prefixo para nomear os arquivos e não limpa o diretório.
    
    Args:
        df: DataFrame Pandas a ser convertido
        table_name: Nome da tabela (subpasta)
        path_parquet: Caminho base para os arquivos parquet (diretório pai de table_name)
        zip_filename_prefix: Prefixo derivado do nome do arquivo ZIP original
        chunk_size: Número de linhas por arquivo parquet
    """
    output_dir = os.path.join(path_parquet, table_name)

    # A LIMPEZA FOI MOVIDA PARA ANTES DO LOOP PRINCIPAL
    # try:
    #     file_delete(output_dir)
    #     logger.info(f'Diretório {output_dir} limpo antes de criar novos arquivos parquet')
    # except Exception as e:
    #     logger.warning(f'Não foi possível limpar diretório {output_dir}: {str(e)}')

    # Garante que o diretório de saída existe
    os.makedirs(output_dir, exist_ok=True)

    # Log das colunas antes de salvar
    logger.info(f"Colunas do DataFrame Pandas '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {list(df.columns)}")
    
    # Calcular número total de chunks baseado no tamanho
    total_rows = len(df)
    # Garantir pelo menos 2 chunks para ter múltiplos arquivos
    num_chunks = max(2, int(np.ceil(total_rows / chunk_size)))
    
    # Ajustar o tamanho dos chunks para distribuir igualmente
    adjusted_chunk_size = int(np.ceil(total_rows / num_chunks))
    
    logger.info(f"Dividindo DataFrame com {total_rows} linhas em {num_chunks} chunks")
    
    # Criar os arquivos parquet em chunks
    for i in range(num_chunks):
        start_idx = i * adjusted_chunk_size
        end_idx = min((i + 1) * adjusted_chunk_size, total_rows)
        
        # Criar um chunk do DataFrame
        df_chunk = df.iloc[start_idx:end_idx]
        
        # Criar nome do arquivo com prefixo
        chunk_index = i
        file_name = f"{zip_filename_prefix}_{table_name}_{chunk_index:03d}.parquet"
        file_path = os.path.join(output_dir, file_name)
        
        # Salvar o chunk como parquet
        df_chunk.to_parquet(
            file_path,
            engine='pyarrow',
            index=False,
            compression='snappy'  # Compressão eficiente para leitura/escrita
        )
        # Não há log por chunk aqui, adicionado debug
        logger.debug(f"Chunk {i+1}/{num_chunks} salvo como {file_name} ({end_idx-start_idx} linhas)")
            
    return True


def process_single_zip_pandas(zip_file, path_zip, path_unzip, path_parquet, create_private):
    """
    Processa um único arquivo ZIP de empresas usando Pandas.
    Lê o(s) arquivo(s) de dados em chunks, concatena e salva um único Parquet.
    
    Args:
        zip_file: Nome do arquivo ZIP
        path_zip: Caminho para o diretório com os arquivos ZIP
        path_unzip: Caminho para extrair os arquivos
        path_parquet: Caminho para salvar os arquivos parquet
        create_private: Flag para criar subset Empresa Privada
        
    Returns:
        dict: Dicionário com resultados {'sucesso': bool, 'tempo': float}
    """
    # Medir tempo do worker - MOVIDO PARA O INÍCIO
    start_time = time.time()
    pid = os.getpid()
    zip_filename_prefix = os.path.splitext(zip_file)[0] # Usar para nomear arquivos parquet
    
    # Configurar logging para este worker (se log_file for fornecido)
    # if log_file: # log_file não é mais passado
    #     configure_worker_logging(log_file)
        
    # Obter o logger configurado (pode ser o raiz)
    logger = logging.getLogger() 
    
    logger.info(f"[{pid}] Iniciando processamento Pandas para: {zip_file}")
    path_extracao = ""
    all_chunks = [] # Lista para guardar todos os chunks lidos do(s) CSV(s)
    processamento_csv_ok = False # Flag para indicar se pelo menos um CSV foi lido
    success = False # Flag de sucesso final

    try:
        # --- 1. Extração --- 
        logger.debug(f"[{pid}] Fase 1: Extração")
        nome_arquivo = os.path.splitext(zip_file)[0]
        path_extracao = os.path.join(path_unzip, nome_arquivo)
        logger.debug(f"[{pid}] Caminho de extração definido: {path_extracao}")
        if os.path.exists(path_extracao):
            logger.warning(f"[{pid}] Diretório de extração {path_extracao} já existe. Removendo.")
            try:
                shutil.rmtree(path_extracao)
                logger.debug(f"[{pid}] Diretório antigo removido.")
            except Exception as e_rem_dir:
                logger.error(f"[{pid}] Erro ao remover diretório de extração antigo {path_extracao}: {e_rem_dir}")
                # Considerar se deve parar ou continuar
                # return {'sucesso': False, 'tempo': time.time() - start_time}
        
        try:
            os.makedirs(path_extracao, exist_ok=True)
            logger.debug(f"[{pid}] Diretório de extração criado/garantido: {path_extracao}")
        except Exception as e_make_dir:
             logger.exception(f"[{pid}] Erro CRÍTICO ao criar diretório de extração {path_extracao}")
             return {'sucesso': False, 'tempo': time.time() - start_time}

        path_zip_file = os.path.join(path_zip, zip_file)
        logger.info(f"[{pid}] Extraindo {zip_file} para {path_extracao}...")
        try:
            with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
                arquivos_extraidos = zip_ref.namelist()
                zip_ref.extractall(path_extracao)
            logger.info(f"[{pid}] Extração de {zip_file} concluída. Arquivos na extração: {arquivos_extraidos}")
        except Exception as e_zip:
            logger.exception(f"[{pid}] Erro durante a extração do ZIP {path_zip_file}")
            return {'sucesso': False, 'tempo': time.time() - start_time}
        
        # --- 2. Encontrar Arquivos de Dados --- 
        logger.debug(f"[{pid}] Fase 2: Encontrar arquivos de dados")
        data_files_encontrados = []
        try:
            for root, _, files in os.walk(path_extracao):
                for file in files:
                    # Procurar por arquivos que contenham 'csv' ou talvez outros padrões comuns
                    if "csv" in file.lower(): 
                        data_files_encontrados.append(os.path.join(root, file))
            logger.debug(f"[{pid}] Busca em {path_extracao} concluída.")
        except Exception as e_walk:
             logger.exception(f"[{pid}] Erro ao percorrer o diretório de extração {path_extracao}")
             return {'sucesso': False, 'tempo': time.time() - start_time}
        
        if not data_files_encontrados:
            logger.error(f"[{pid}] Nenhum arquivo de dados (contendo 'csv') encontrado em {path_extracao}.")
            # Listar conteúdo para depuração
            try:
                conteudo_dir = os.listdir(path_extracao)
                logger.debug(f"[{pid}] Conteúdo do diretório de extração {path_extracao} (para depuração): {conteudo_dir}")
            except Exception as e_list:
                logger.warning(f"[{pid}] Não foi possível listar conteúdo de {path_extracao}: {e_list}")
            # Não retorna False ainda, vai para o finally para limpar
        else:
            logger.info(f"[{pid}] Encontrados {len(data_files_encontrados)} arquivos de dados para processar: {data_files_encontrados}")

            # --- 3. Ler e Processar Chunks de Todos os Arquivos --- 
            logger.debug(f"[{pid}] Fase 3: Leitura e Processamento de Chunks")
            dtype_dict = {
                'CNPJ BÁSICO': 'str', 'RAZÃO SOCIAL': 'str',
                'NATUREZA JURÍDICA': 'str', 'QUALIFICAÇÃO': 'str',
                'CAPITAL SOCIAL': 'str', 'PORTE': 'str', 'ENTE FEDERATIVO': 'str'
            }
            colunas_numericas_potenciais = ['NATUREZA JURÍDICA', 'QUALIFICAÇÃO', 'PORTE']
            coluna_float_potencial = 'CAPITAL SOCIAL'
            chunks_lidos_total = 0

            for data_path in data_files_encontrados:
                data_file_name = os.path.basename(data_path)
                logger.info(f"[{pid}] Lendo arquivo: {data_path}")
                try:
                    # Determinar chunksize...
                    file_size_mb = os.path.getsize(data_path) / (1024 * 1024)
                    if file_size_mb > 1000: chunksize = 50000
                    elif file_size_mb > 500: chunksize = 100000
                    elif file_size_mb > 100: chunksize = 250000
                    else: chunksize = 500000
                    logger.info(f"[{pid}] Lendo {data_file_name} (Tamanho: {file_size_mb:.2f} MB) em chunks de {chunksize} linhas")

                    linhas_processadas_arquivo = 0
                    original_column_names = config.empresa_columns # Garantir que está acessível ou passar como argumento
                    chunk_iterator = pd.read_csv(
                        data_path, sep=';', encoding='latin1',
                        dtype=str, # Ler tudo como string inicialmente é mais seguro com chunks
                        chunksize=chunksize, low_memory=False, # low_memory=False pode consumir mais memória, mas evita erros de tipo
                        na_values=['', '********'], # Definir valores NA explicitamente
                        keep_default_na=False, # Não usar default NAs do pandas
                        header=None, names=original_column_names, # <<< USA AS COLUNAS DA CONFIG
                        quoting=1 # QUOTE_MINIMAL
                    )
                    
                    for i, chunk in enumerate(chunk_iterator):
                        chunks_lidos_total += 1
                        # Aplicar transformações...
                        chunk = apply_empresa_transformations_pandas(chunk) # Chamar a função de transformação aqui

                        all_chunks.append(chunk)
                        linhas_processadas_arquivo += len(chunk)
                        # Logar progresso a cada 10 chunks ou no primeiro/último
                        if i == 0 or (i + 1) % 10 == 0:
                            logger.debug(f"[{pid}] Lendo {data_file_name}: Chunk {i+1} lido ({linhas_processadas_arquivo} linhas acumuladas no arquivo)")
                        # Atualizar print no console
                        print(f"\r[{pid}] Lendo {data_file_name}: Chunk {i+1} lido ({linhas_processadas_arquivo} linhas)", end="", flush=True)
                    
                    print(f"\r[{pid}] Leitura de {data_file_name} concluída. ({linhas_processadas_arquivo} linhas lidas)                          ") # Limpar linha
                    processamento_csv_ok = True
                    del chunk_iterator 
                    gc.collect() 

                except Exception as e_read:
                    logger.exception(f"[{pid}] Erro ao ler ou processar chunks do arquivo {data_path}")
                    # Pula para o próximo arquivo de dados se este falhar
                    continue 
            
            # --- 4. Concatenar e Salvar Parquet --- 
            logger.debug(f"[{pid}] Fase 4: Concatenação e Escrita do Parquet")
            if not processamento_csv_ok or not all_chunks:
                logger.error(f"[{pid}] Nenhum chunk de dados foi lido com sucesso para {zip_file}. Nenhum Parquet será gerado.")
            else:
                logger.info(f"[{pid}] Concatenando {len(all_chunks)} chunks de dados (total lido: {chunks_lidos_total}) para {zip_file}...")
                final_df = None
                try:
                    start_concat_time = time.time()
                    final_df = pd.concat(all_chunks, ignore_index=True)
                    concat_time = time.time() - start_concat_time
                    logger.debug(f"[{pid}] Tempo de concatenação: {concat_time:.2f}s")
                    # Liberar memória da lista original o quanto antes
                    del all_chunks
                    all_chunks = [] # Resetar para garantir
                    gc.collect()
                    memoria_df_mb = final_df.memory_usage(deep=True).sum() / (1024*1024)
                    logger.info(f"[{pid}] Concatenação concluída. DataFrame final com {len(final_df)} linhas e {memoria_df_mb:.2f} MB.")
                    logger.debug(f"[{pid}] Tipos de dados do DataFrame final antes de salvar:\n{final_df.dtypes}")

                except Exception as e_concat:
                    logger.exception(f"[{pid}] Erro ao concatenar chunks para {zip_file}")
                    if final_df is not None: del final_df
                    all_chunks = [] 
                    gc.collect()
                
                # Prosseguir apenas se a concatenação foi bem-sucedida
                if final_df is not None:
                    # Definir caminho e nome do Parquet
                    parquet_dir = os.path.join(path_parquet, 'empresas')
                    try:
                        os.makedirs(parquet_dir, exist_ok=True)
                    except Exception as e_make_parquet_dir:
                        logger.exception(f"[{pid}] Erro ao criar diretório de destino do Parquet {parquet_dir}")
                        final_df = None # Não tentar salvar
                        
                    if final_df is not None:
                        parquet_file_name = f"{os.path.splitext(zip_file)[0]}.parquet"
                        parquet_file = os.path.join(parquet_dir, parquet_file_name)
                        logger.debug(f"[{pid}] Arquivo Parquet de destino definido: {parquet_file}")

                        # Remover Parquet antigo
                        if os.path.exists(parquet_file):
                            logger.warning(f"[{pid}] Removendo Parquet existente: {parquet_file}")
                            try: 
                                os.remove(parquet_file)
                                logger.debug(f"[{pid}] Parquet antigo removido.")
                            except Exception as e_rem: 
                                logger.error(f"[{pid}] Erro ao remover Parquet existente {parquet_file}: {e_rem}")
                                final_df = None # Não tentar salvar
                        
                        if final_df is not None:
                            logger.info(f"[{pid}] Salvando DataFrame final ({len(final_df)} linhas) em {parquet_file}...")
                            try:
                                start_write_time = time.time()
                                final_df.to_parquet(parquet_file, index=False, engine='pyarrow', compression='snappy')
                                write_time = time.time() - start_write_time
                                logger.info(f"[{pid}] Chamada to_parquet concluída em {write_time:.2f}s.")
                                
                                # Verificação final pós-escrita
                                tamanho_arquivo_gerado = -1
                                existe = os.path.exists(parquet_file)
                                if existe:
                                    try:
                                        tamanho_arquivo_gerado = os.path.getsize(parquet_file)
                                    except Exception as e_size:
                                        logger.error(f"[{pid}] Erro ao obter tamanho do arquivo {parquet_file}: {e_size}")
                                
                                if existe and tamanho_arquivo_gerado > 0:
                                    logger.info(f"[{pid}] Arquivo Parquet {parquet_file} salvo com SUCESSO (Tamanho: {tamanho_arquivo_gerado / (1024*1024):.2f} MB).")
                                    success = True # SUCESSO FINAL!
                                elif existe and tamanho_arquivo_gerado == 0:
                                    logger.error(f"[{pid}] FALHA ao salvar Parquet: Arquivo {parquet_file} foi criado mas está VAZIO (0 bytes).")
                                    success = False
                                    try: os.remove(parquet_file); logger.warning(f"[{pid}] Arquivo Parquet vazio removido.") 
                                    except: pass
                                else: # Não existe ou tamanho < 0 (erro)
                                     logger.error(f"[{pid}] FALHA ao salvar Parquet: Arquivo {parquet_file} NÃO FOI ENCONTRADO após a escrita.")
                                     success = False
                                    
                            except Exception as e_write:
                                logger.exception(f"[{pid}] Erro CRÍTICO durante final_df.to_parquet() para {parquet_file}")
                                success = False
                                # Tentar remover arquivo potencialmente corrompido
                                if os.path.exists(parquet_file): 
                                    try: os.remove(parquet_file); logger.warning(f"[{pid}] Tentativa de remover arquivo Parquet potencialmente corrompido.") 
                                    except: pass
                    
                    # Limpar memória do DataFrame final após tentativa de escrita
                    logger.debug(f"[{pid}] Liberando memória do DataFrame final.")
                    del final_df
                    gc.collect()

    except Exception as e_general:
        logger.exception(f"[{pid}] Erro GERAL e inesperado processando {zip_file}")
        success = False
    
    finally:
        # --- 5. Limpeza Final --- 
        logger.debug(f"[{pid}] Fase 5: Limpeza Final")
        # Limpar lista de chunks remanescentes (em caso de erro na concatenação)
        if 'all_chunks' in locals() and all_chunks:
            logger.debug(f"[{pid}] Limpando lista de chunks remanescente.")
            del all_chunks
            gc.collect()
            
        if path_extracao and os.path.exists(path_extracao):
            logger.info(f"[{pid}] Limpando diretório de extração: {path_extracao}")
            try:
                shutil.rmtree(path_extracao)
                logger.debug(f"[{pid}] Diretório de extração removido.")
            except Exception as e_clean:
                logger.warning(f"[{pid}] Não foi possível limpar completamente o diretório de extração {path_extracao}: {e_clean} (Provavelmente devido a handles abertos no Windows - WinError 32). Isso pode ser ignorado se o processamento principal foi concluído.")
        else:
             logger.debug(f"[{pid}] Diretório de extração não existe ou caminho não definido, nada a limpar.")

    end_time = time.time()
    tempo_execucao = end_time - start_time
    logger.info(f"[{pid}] Processamento Pandas para {zip_file} concluído em {tempo_execucao:.2f}s com status final: {success}")
    
    # Retornar dicionário como esperado pelo processo principal
    return {
        'sucesso': success,
        'tempo': tempo_execucao,
    }


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
    
    # Adiciona uma coluna CPF vazia para manter a compatibilidade com o resto do código
    if 'CPF' not in df.columns:
        df = df.with_columns(pl.lit(None).alias('CPF'))
    
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
        logger.info(f"[{pid}] Polars: Extraindo {zip_file} para {path_extracao}...") # Extrai para subpasta
        try:
            with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
                zip_ref.extractall(path_extracao) # Extrai para subpasta
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
                    if "csv" in file.lower(): 
                        data_files_encontrados.append(os.path.join(root, file))
        except Exception as e_walk:
             logger.exception(f"[{pid}] Polars: Erro ao percorrer o diretório de extração {path_extracao}")
             return False

        if not data_files_encontrados:
            logger.warning(f"[{pid}] Polars: Nenhum arquivo de dados (contendo 'csv') encontrado em {path_extracao} para {zip_file}." ) # Aviso em vez de erro
            success = True # Considera sucesso se não há arquivos CSV para processar neste ZIP
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


def process_empresa_with_pandas(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Processa os dados de empresas usando Pandas."""
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de EMPRESAS com Pandas (create_private={create_private})') # Log da flag
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
            logger.info(f'Diretório {output_dir_empresas} limpo antes do processamento Pandas.')
            if create_private:
                 file_delete(output_dir_privada)
                 logger.info(f'Diretório {output_dir_privada} limpo antes do processamento Pandas.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretórios de saída antes do processamento Pandas: {str(e)}')

        # Processar diretamente com Pandas
        success = False
        for zip_file in track(zip_files, description="[cyan]Processing Empresas ZIPs (Pandas)..."):
            result = process_single_zip_pandas(
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet,
                create_private=create_private
            )
            if result['sucesso']:
                success = True
                logger.info(f"Arquivo {zip_file} processado com sucesso usando Pandas")
        
        if not success:
            logger.warning("Nenhum arquivo processado com sucesso usando Pandas.")
        
        return success
            
    except Exception as e:
        logger.error(f'Erro no processamento com Pandas: {str(e)}')
        return False


def process_empresa_with_polars(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Processa os dados de empresas usando Polars."""
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de EMPRESAS com Polars (create_private={create_private})') # Log da flag
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

        # Processar com Polars
        success = False
        for zip_file in track(zip_files, description="[cyan]Processing Empresas ZIPs (Polars)..."):
            result = process_single_zip_polars(
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet,
                create_private=create_private
            )
            if result:
                success = True
                logger.info(f"Arquivo {zip_file} processado com sucesso usando Polars")
        
        if not success:
            logger.warning("Nenhum arquivo processado com sucesso usando Polars.")
        
        return success
            
    except Exception as e:
        logger.error(f'Erro no processamento com Polars: {str(e)}')
        return False
