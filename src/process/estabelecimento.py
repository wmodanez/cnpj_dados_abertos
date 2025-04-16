import logging
import os
import zipfile
import dask.dataframe as dd
from dask import delayed
import polars as pl
import numpy as np
import shutil
from rich.progress import track

from ..config import config
from ..utils import (
    file_delete, check_disk_space, estimate_zip_extracted_size,
    process_csv_files_parallel, process_csv_to_df, verify_csv_integrity, 
    create_parquet_filename
)
from src.utils.dask_manager import DaskManager

logger = logging.getLogger(__name__)


def create_parquet(df, table_name, path_parquet, zip_filename_prefix: str):
    """Converte um DataFrame Dask para formato parquet, usando prefixo.
    
    Args:
        df: DataFrame Dask a ser convertido
        table_name: Nome da tabela (subpasta)
        path_parquet: Caminho base para os arquivos parquet (diretório pai)
        zip_filename_prefix: Prefixo derivado do nome do arquivo ZIP original
    """
    output_dir = os.path.join(path_parquet, table_name)

    # Limpeza de diretório MOVIDA para a função principal (antes do loop)
    # try:
    #     file_delete(output_dir)
    #     logger.debug(f'Diretório {output_dir} limpo antes de criar novos arquivos parquet')
    # except Exception as e:
    #     logger.warning(f'Não foi possível limpar diretório {output_dir}: {str(e)}')

    # Garante que o diretório exista
    os.makedirs(output_dir, exist_ok=True)

    # Log das colunas antes de salvar
    logger.debug(f"Colunas do DataFrame Dask '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {list(df.columns)}")

    # Configura o nome dos arquivos parquet com prefixo do zip e da tabela
    df.to_parquet(
        output_dir,
        engine='pyarrow',  # Especifica o engine
        write_index=False,
        # name_function=lambda i: create_parquet_filename(table_name, i)
        # Usa prefixo e índice do chunk para nome único
        name_function=lambda i: f"{zip_filename_prefix}_{table_name}_{i:03d}.parquet"
    )


def process_csv_file(csv_path):
    """
    Processa um único arquivo CSV de estabelecimento e retorna um DataFrame Dask.
    
    Args:
        csv_path: Caminho para o arquivo CSV
        
    Returns:
        DataFrame Dask ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        return None

    # Usa colunas e dtypes da config
    original_column_names = config.estabelecimento_columns
    dtype_dict = config.estabelecimento_dtypes

    try:
        # Passa nomes, separador, encoding da config
        df = process_csv_to_df(
            csv_path, 
            dtype=dtype_dict, 
            column_names=original_column_names,
            separator=config.file.separator, # Usa separador da config
            encoding=config.file.encoding,   # Usa encoding da config
            na_filter=False  # Como em simples.py
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)}: {str(e)}')
        return None


def apply_estabelecimento_transformations(ddf):
    """Aplica transformações específicas para estabelecimentos usando Dask."""
    logger.info("Aplicando transformações em Estabelecimentos...")
    
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
    ddf = ddf.rename(columns={k: v for k, v in rename_mapping.items() if k in ddf.columns})
    
    # Conversão numérica usando Dask
    int_cols = ['matriz_filial', 'codigo_situacao_cadastral', 
                'codigo_motivo_situacao_cadastral', 'codigo_cnae', 
                'codigo_municipio']
    
    for col in int_cols:
        if col in ddf.columns:
            ddf[col] = ddf[col].map_partitions(
                lambda s: dd.to_numeric(s, errors='coerce'),
                meta=(col, 'Int64')
            )
    
    # Conversão de datas usando Dask
    date_cols = ['data_situacao_cadastral', 'data_inicio_atividades']
    
    for col in date_cols:
        if col in ddf.columns:
            ddf[col] = ddf[col].map_partitions(
                lambda s: dd.to_datetime(
                    s.astype(str).replace(['0', '00000000', 'nan', 'None', 'NaN'], ''),
                    format='%Y%m%d',
                    errors='coerce'
                ).dt.normalize(),  # Normalizar para zerar o componente de hora
                meta=(col, 'datetime64[ns]')  # Mantém o tipo datetime64[ns], compatível com Parquet
            )
    
    # Criação do CNPJ completo
    if all(col in ddf.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
        ddf['cnpj'] = (
            ddf['cnpj_basico'].astype(str).str.zfill(8) + 
            ddf['cnpj_ordem'].astype(str).str.zfill(4) + 
            ddf['cnpj_dv'].astype(str).str.zfill(2)
        )
    
    # Remoção de colunas
    cols_to_drop = ['cnpj_ordem', 'cnpj_dv', 'tipo_logradouro', 'logradouro', 
                    'numero', 'complemento', 'bairro', 'ddd1', 'telefone1', 
                    'ddd2', 'telefone2', 'ddd_fax', 'fax', 'pais',
                    'correio_eletronico', 'situacao_especial', 
                    'data_situacao_especial', 'nome_cidade_exterior']
    
    ddf = ddf.drop([col for col in cols_to_drop if col in ddf.columns], axis=1)
    
    return ddf


@delayed
def process_single_zip(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa um único arquivo ZIP de forma otimizada com Dask.
    
    Args:
        zip_file: Nome do arquivo ZIP
        path_zip: Caminho para o diretório dos arquivos ZIP
        path_unzip: Caminho para o diretório de extração
        path_parquet: Caminho para o diretório dos arquivos parquet
        uf_subset (str | None, optional): Sigla da UF para criar um subset. Defaults to None.
    """
    zip_filename_prefix = os.path.splitext(zip_file)[0] # Prefixo para nomes de arquivo
    path_extracao = ""
    try:
        # --- 1. Extração --- (Adaptado da versão Polars/Empresa)
        logger.debug(f"[{os.getpid()}] Dask Fase 1: Extração para {zip_file}")
        nome_arquivo = os.path.splitext(zip_file)[0]
        path_extracao = os.path.join(path_unzip, nome_arquivo)
        if os.path.exists(path_extracao): shutil.rmtree(path_extracao)
        os.makedirs(path_extracao, exist_ok=True)
        
        path_zip_file = os.path.join(path_zip, zip_file)
        with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
            zip_ref.extractall(path_extracao)
        logger.info(f"[{os.getpid()}] Dask Extração de {zip_file} concluída para {path_extracao}")

        # --- 2. Leitura e Processamento CSV com Dask ---
        all_files_in_extraction = [
            os.path.join(path_extracao, f)
            for f in os.listdir(path_extracao)
            if os.path.isfile(os.path.join(path_extracao, f))
        ]
        
        if not all_files_in_extraction:
            logger.warning(f"[{os.getpid()}] Dask Nenhum arquivo de dados encontrado em {path_extracao} para {zip_file}.")
            # Limpeza no finally cuidará da pasta vazia
            return True # Sucesso se não há o que processar
        
        logger.info(f"[{os.getpid()}] Dask Arquivos encontrados: {all_files_in_extraction}")
        # Assume que todos os arquivos na pasta são do mesmo tipo (estabelecimento)
        # Usa a função genérica process_csv_to_df que retorna Dask DF
        # IMPORTANTE: process_csv_to_df precisa ser robusta para lidar com múltiplos arquivos aqui
        # Idealmente, Dask leria todos diretamente com wildcards, mas vamos manter a leitura individual por enquanto
        # para alinhar com Polars, assumindo que process_csv_to_df lida com um path e retorna ddf
        
        # Criar lista de dataframes dask para cada arquivo e depois concatenar
        ddf_list = []
        for data_path in all_files_in_extraction:
            logger.info(f"[{os.getpid()}] Dask Tentando processar: {os.path.basename(data_path)}")
            ddf_part = process_csv_file(data_path) # Usa a função Dask existente
            if ddf_part is not None:
                # Aqui podemos ter um problema se process_csv_file já retorna um ddf grande.
                # Idealmente, Dask leria múltiplos arquivos de uma vez.
                # dd.read_csv(os.path.join(path_extracao, '*.ESTABELE'), ...) seria mais eficiente.
                # Por ora, concatenamos os DFs Dask resultantes.
                ddf_list.append(ddf_part)
            else:
                logger.warning(f"[{os.getpid()}] Dask Falha ao processar {os.path.basename(data_path)}, pulando.")

        if not ddf_list:
            logger.error(f"[{os.getpid()}] Dask Nenhum DataFrame Dask válido gerado para {zip_file}.")
            return False
            
        # Concatenar os DataFrames Dask
        ddf = dd.concat(ddf_list, ignore_index=True) if len(ddf_list) > 1 else ddf_list[0]

        # Aplicar transformações Dask
        ddf = apply_estabelecimento_transformations(ddf)
        
        # --- 3. Salvar Parquet (Principal) ---
        main_saved = False
        try:
            create_parquet(ddf, 'estabelecimentos', path_parquet, zip_filename_prefix)
            main_saved = True
        except Exception as e_parq_main:
            logger.error(f"[{os.getpid()}] Dask Erro ao salvar Parquet principal para {zip_file}: {e_parq_main}")
            # Continua para tentar salvar subset se houver

        # --- 4. Salvar Parquet (Subset UF - Condicional) ---
        subset_saved = True # Assume sucesso se não for criar ou se falhar principal
        if main_saved and uf_subset and 'uf' in ddf.columns:
            subset_table_name = f"estabelecimentos_{uf_subset.lower()}"
            logger.info(f"[{os.getpid()}] Dask Criando subset {subset_table_name}...")
            ddf_subset = ddf[ddf['uf'] == uf_subset]
            # Apenas tentamos salvar.
            try:
                create_parquet(ddf_subset, subset_table_name, path_parquet, zip_filename_prefix)
            except Exception as e_parq_subset:
                 logger.error(f"[{os.getpid()}] Dask Falha ao salvar subset {subset_table_name} para {zip_file}: {e_parq_subset}")
                 subset_saved = False
            # del ddf_subset # Dask lida com memória
        elif main_saved and uf_subset:
             logger.warning(f"[{os.getpid()}] Dask Coluna 'uf' não encontrada, não é possível criar subset para UF {uf_subset}.")
             
        success = main_saved and subset_saved
        # del ddf # Dask lida com a memória
        return success
        
    except Exception as e:
        logger.error(f'[{os.getpid()}] Dask Erro GERAL processando {zip_file}: {str(e)}')
        # Adicionar traceback para depuração
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        # --- 5. Limpeza ---
        if path_extracao and os.path.exists(path_extracao):
            try: 
                shutil.rmtree(path_extracao)
                logger.info(f"[{os.getpid()}] Dask Diretório de extração {path_extracao} limpo.")
            except Exception as e_clean:
                 logger.warning(f"[{os.getpid()}] Dask Falha ao limpar diretório {path_extracao}: {e_clean}")


def process_estabelecimento(path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa os dados de estabelecimentos usando Dask.
    
    Args:
        path_zip: Caminho para o diretório dos arquivos ZIP
        path_unzip: Caminho para o diretório de extração
        path_parquet: Caminho para o diretório dos arquivos parquet
        uf_subset (str | None, optional): Sigla da UF para criar um subset. Defaults to None.
    """
    logger.info('=' * 50)
    log_uf = f" (Subset UF: {uf_subset})" if uf_subset else ""
    logger.info(f'Iniciando processamento Dask de ESTABELECIMENTOS{log_uf}')
    logger.info('=' * 50)
    
    try:
        client = DaskManager.get_instance().client
        
        zip_files = [f for f in os.listdir(path_zip) 
                    if f.startswith('Estabelecimento') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Estabelecimentos encontrado.')
            return True
        
        # LIMPEZA PRÉVIA dos diretórios de saída
        output_dir_main = os.path.join(path_parquet, 'estabelecimentos')
        # Limpa também o diretório do subset se a UF foi especificada
        if uf_subset:
            output_dir_subset = os.path.join(path_parquet, f"estabelecimentos_{uf_subset.lower()}")
        else:
            output_dir_subset = None

        try:
            file_delete(output_dir_main)
            logger.info(f'Diretório {output_dir_main} limpo antes do processamento Dask.')
            if output_dir_subset:
                 file_delete(output_dir_subset)
                 logger.info(f'Diretório {output_dir_subset} limpo antes do processamento Dask.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretórios de saída antes do processamento Dask: {str(e)}')
            
        futures = [
            client.submit(
                process_single_zip,
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet,
                uf_subset=uf_subset # Passa a UF para a função Dask
            )
            for zip_file in zip_files
        ]
        
        results = client.gather(futures)
        return all(results)
            
    except Exception as e:
        logger.error(f'Erro no processamento principal: {str(e)}')
        return False


# ----- Implementação para Polars -----

def process_csv_file_polars(csv_path: str):
    """Lê um arquivo CSV de estabelecimento usando Polars.
    
    Args:
        csv_path: Caminho para o arquivo CSV.
        
    Returns:
        DataFrame Polars ou None em caso de erro.
    """
    if not verify_csv_integrity(csv_path):
        return None

    original_column_names = config.estabelecimento_columns

    try:
        df = pl.read_csv(
            csv_path,
            separator=config.file.separator,
            encoding=config.file.encoding,
            has_header=False,
            new_columns=original_column_names,
            infer_schema_length=0,  # Não inferir schema
            dtypes={col: pl.Utf8 for col in original_column_names}, # Ler tudo como string
            ignore_errors=True # Tentar ignorar linhas mal formadas
        )
        return df
    except Exception as e:
        logger.error(f'Erro ao processar o arquivo {os.path.basename(csv_path)} com Polars: {str(e)}')
        return None

def apply_estabelecimento_transformations_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica transformações específicas para estabelecimentos usando Polars."""
    logger.info("Aplicando transformações em Estabelecimentos com Polars...")
    
    # Renomeação de colunas (ignora colunas que não existem no DataFrame)
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico',
        'identificador_matriz_filial': 'matriz_filial',
        'nome_fantasia': 'nome_fantasia',
        'situacao_cadastral': 'codigo_situacao_cadastral',
        'data_situacao_cadastral': 'data_situacao_cadastral',
        'motivo_situacao_cadastral': 'codigo_motivo_situacao_cadastral',
        'data_inicio_atividade': 'data_inicio_atividades',
        'cnae_fiscal_principal': 'codigo_cnae',
        # Adicionar outras se necessário, ex: 'uf', 'municipio'?
        'municipio': 'codigo_municipio'
    }
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in df.columns}
    df = df.rename(actual_rename_mapping)
    
    # Conversão numérica
    int_cols = [
        'matriz_filial', 'codigo_situacao_cadastral', 
        'codigo_motivo_situacao_cadastral', 'codigo_cnae', 
        'codigo_municipio'
    ]
    for col in int_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))
    
    # Conversão de datas
    date_cols = ['data_situacao_cadastral', 'data_inicio_atividades']
    for col in date_cols:
        if col in df.columns:
            # Tratar valores inválidos ('0', '', None) antes de converter
            df = df.with_columns(
                pl.when(
                    pl.col(col).is_in(['0', '00000000']) | 
                    pl.col(col).is_null() | 
                    (pl.col(col).str.len_chars() == 0)
                )
                  .then(None)
                  .otherwise(pl.col(col))
                  .str.strptime(pl.Date, format="%Y%m%d", strict=False)
                  .alias(col)
            )
            
    # Criação do CNPJ completo
    if all(col in df.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
        df = df.with_columns(
            pl.format("{}{}{}", 
                pl.col('cnpj_basico').cast(pl.Utf8).str.pad_start(8, '0'), 
                pl.col('cnpj_ordem').cast(pl.Utf8).str.pad_start(4, '0'), 
                pl.col('cnpj_dv').cast(pl.Utf8).str.pad_start(2, '0')
            ).alias('cnpj')
        )
    
    # Remoção de colunas
    cols_to_drop = [
        'cnpj_ordem', 'cnpj_dv', 'tipo_logradouro', 'logradouro', 
        'numero', 'complemento', 'bairro', 'ddd1', 'telefone1', 
        'ddd2', 'telefone2', 'ddd_fax', 'fax', 'pais',
        'correio_eletronico', 'situacao_especial', 
        'data_situacao_especial', 'nome_cidade_exterior'
    ]
    actual_cols_to_drop = [col for col in cols_to_drop if col in df.columns]
    df = df.drop(actual_cols_to_drop)
    
    logger.info("Transformações Polars aplicadas em Estabelecimentos.")
    return df

def create_parquet_polars(df: pl.DataFrame, table_name: str, path_parquet: str, zip_filename_prefix: str):
    """Salva um DataFrame Polars como múltiplos arquivos parquet com prefixo."""
    try:
        output_dir = os.path.join(path_parquet, table_name)
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Colunas do DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) antes de salvar em Parquet: {df.columns}")
        
        total_rows = df.height
        if total_rows == 0:
            logger.warning(f"DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) está vazio. Nenhum Parquet será salvo.")
            return True # Considera sucesso se não há o que salvar
            
        chunk_size = 500000  # Definir tamanho do chunk
        num_chunks = max(1, int(np.ceil(total_rows / chunk_size))) # Pelo menos 1 chunk
        adjusted_chunk_size = int(np.ceil(total_rows / num_chunks))
        
        logger.info(f"Salvando '{table_name}' (Origem: {zip_filename_prefix}) com {total_rows} linhas em {num_chunks} chunks Parquet...")
        
        for i in range(num_chunks):
            start_idx = i * adjusted_chunk_size
            # slice(offset, length)
            df_chunk = df.slice(start_idx, adjusted_chunk_size)
            
            file_name = f"{zip_filename_prefix}_{table_name}_{i:03d}.parquet"
            file_path = os.path.join(output_dir, file_name)
            
            df_chunk.write_parquet(file_path, compression="snappy")
            logger.debug(f"Chunk {i+1}/{num_chunks} salvo como {file_name} ({df_chunk.height} linhas)")
            
        logger.info(f"DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) salvo com sucesso em {num_chunks} arquivo(s) Parquet.")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar DataFrame Polars '{table_name}' (Origem: {zip_filename_prefix}) como parquet: {str(e)}")
        return False

def process_single_zip_polars(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa um único arquivo ZIP de estabelecimentos usando Polars.

    Args:
        ...
        uf_subset (str | None, optional): Sigla da UF para criar um subset. Defaults to None.
    """
    logger = logging.getLogger() # Obter o logger configurado
    pid = os.getpid() # Para logs, se necessário
    zip_filename_prefix = os.path.splitext(zip_file)[0]
    logger.info(f"[{pid}] Iniciando processamento Polars para: {zip_file}")
    path_extracao = ""
    success = False
    
    try:
        # --- 1. Extração ---
        nome_arquivo_sem_ext = os.path.splitext(zip_file)[0]
        path_extracao = os.path.join(path_unzip, nome_arquivo_sem_ext)
        if os.path.exists(path_extracao): 
            shutil.rmtree(path_extracao)
        os.makedirs(path_extracao, exist_ok=True)
        
        path_zip_file = os.path.join(path_zip, zip_file)
        with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
            zip_ref.extractall(path_extracao)
        logger.info(f"[{pid}] Extração de {zip_file} concluída para {path_extracao}")

        # --- 2. Leitura e Processamento CSV com Polars ---
        # Busca todos os arquivos dentro da pasta de extração (mais robusto)
        all_files_in_extraction = [
            os.path.join(path_extracao, f)
            for f in os.listdir(path_extracao)
            if os.path.isfile(os.path.join(path_extracao, f)) # Garante que é um arquivo
        ]
        
        if not all_files_in_extraction:
            logger.warning(f"[{pid}] Nenhum arquivo de dados encontrado em {path_extracao} para {zip_file}.")
            return True # Sucesso, pois não há o que processar

        dataframes = []
        logger.debug(f"[{pid}] Arquivos encontrados em {path_extracao}: {all_files_in_extraction}")
        # Tenta processar todos os arquivos encontrados como CSV/dados
        for data_path in all_files_in_extraction:
            file_name = os.path.basename(data_path)
            logger.debug(f"[{pid}] Tentando processar arquivo: {file_name}")
            df_polars = process_csv_file_polars(data_path)
            if df_polars is not None and not df_polars.is_empty():
                logger.debug(f"[{pid}] Arquivo {file_name} processado com sucesso ({df_polars.height} linhas).")
                dataframes.append(df_polars)
            elif df_polars is not None and df_polars.is_empty():
                 logger.warning(f"[{pid}] Arquivo {file_name} resultou em DataFrame vazio.")
            # Se df_polars for None, o erro já foi logado em process_csv_file_polars
        
        if not dataframes:
            logger.error(f"[{pid}] Nenhum DataFrame Polars válido gerado a partir do ZIP {zip_file}.")
            return False

        # Concatenar e aplicar transformações
        df_final = pl.concat(dataframes) if len(dataframes) > 1 else dataframes[0]
        df_final = apply_estabelecimento_transformations_polars(df_final)
        
        # --- 3. Salvar Parquet (Principal) ---
        main_saved = create_parquet_polars(df_final, 'estabelecimentos', path_parquet, zip_filename_prefix)
        
        # --- 4. Salvar Parquet (Subset UF - Condicional) ---
        subset_saved = True # Assume sucesso se não for criar ou se falhar principal
        if main_saved and uf_subset and 'uf' in df_final.columns:
            subset_table_name = f"estabelecimentos_{uf_subset.lower()}"
            logger.debug(f"[{pid}] Polars Criando subset {subset_table_name}...")
            df_subset = df_final.filter(pl.col('uf') == uf_subset)
            if not df_subset.is_empty():
                subset_saved = create_parquet_polars(df_subset, subset_table_name, path_parquet, zip_filename_prefix)
                if not subset_saved:
                     logger.error(f"[{pid}] Polars Falha ao salvar subset {subset_table_name} para {zip_file}.")
            else:
                logger.debug(f"[{pid}] Polars Subset {subset_table_name} vazio para {zip_file}, não será salvo.")
            del df_subset # Liberar memória
        elif main_saved and uf_subset:
             logger.warning(f"[{pid}] Polars Coluna 'uf' não encontrada, não é possível criar subset para UF {uf_subset}.")
             
        success = main_saved and subset_saved
        del df_final # Liberar memória
        
    except Exception as e:
        logger.exception(f"[{pid}] Erro GERAL no processamento Polars de {zip_file}")
        success = False
    finally:
        # --- 5. Limpeza ---
        if path_extracao and os.path.exists(path_extracao):
            try: 
                shutil.rmtree(path_extracao)
                logger.info(f"[{pid}] Diretório de extração {path_extracao} limpo.")
            except Exception as e_clean:
                 logger.warning(f"[{pid}] Falha ao limpar diretório de extração {path_extracao}: {e_clean}")

    logger.info(f"[{pid}] Processamento Polars para {zip_file} concluído com status: {success}")
    return success

def process_estabelecimento_with_polars(path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None) -> bool:
    """Processa os dados de estabelecimentos usando Polars.
    
     Args:
        ...
        uf_subset (str | None, optional): Sigla da UF para criar um subset. Defaults to None.
    """
    logger.info('=' * 50)
    log_uf = f" (Subset UF: {uf_subset})" if uf_subset else ""
    logger.info(f'Iniciando processamento de ESTABELECIMENTOS com Polars{log_uf}')
    logger.info('=' * 50)
    
    try:
        zip_files = [f for f in os.listdir(path_zip) 
                    if f.startswith('Estabelecimento') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Estabelecimentos encontrado.')
            return True

        # LIMPEZA PRÉVIA dos diretórios de saída
        output_dir_main = os.path.join(path_parquet, 'estabelecimentos')
        # Limpa também o diretório do subset se a UF foi especificada
        if uf_subset:
            output_dir_subset = os.path.join(path_parquet, f"estabelecimentos_{uf_subset.lower()}")
        else:
            output_dir_subset = None

        try:
            file_delete(output_dir_main)
            logger.info(f'Diretório {output_dir_main} limpo antes do processamento Polars.')
            if output_dir_subset:
                file_delete(output_dir_subset)
                logger.info(f'Diretório {output_dir_subset} limpo antes do processamento Polars.')
        except Exception as e:
            logger.warning(f'Não foi possível limpar diretórios de saída antes do processamento Polars: {str(e)}')

        overall_success = False
        # Usar rich.progress.track para visualização
        for zip_file in track(zip_files, description="[cyan]Processing Estabelecimentos ZIPs (Polars)..."): 
            result = process_single_zip_polars(
                zip_file=zip_file,
                path_zip=path_zip,
                path_unzip=path_unzip,
                path_parquet=path_parquet,
                uf_subset=uf_subset # Passa a UF para a função Polars
            )
            if result:
                overall_success = True # Marca que pelo menos um teve sucesso
                # Log individual já é feito dentro de process_single_zip_polars
        
        if not overall_success:
             logger.warning("Nenhum arquivo ZIP de Estabelecimentos foi processado com sucesso usando Polars.")
             
        return overall_success
            
    except Exception as e:
        logger.error(f'Erro no processamento principal com Polars: {str(e)}')
        return False
