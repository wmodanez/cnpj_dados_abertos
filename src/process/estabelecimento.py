import logging
import logging
import os
import zipfile

import dask.dataframe as dd
import numpy as np
import pandas as pd

from ..config import config
from ..utils import file_delete, check_disk_space, estimate_zip_extracted_size
from ..utils import process_csv_files_parallel, process_csv_to_df, verify_csv_integrity, create_parquet_filename

logger = logging.getLogger(__name__)


def create_parquet(df, table_name, path_parquet):
    """Converte um DataFrame para formato parquet.
    
    Args:
        df: DataFrame Dask a ser convertido
        table_name: Nome da tabela
        path_parquet: Caminho base para os arquivos parquet
    """
    output_dir = os.path.join(path_parquet, table_name)

    # Limpa o diretório antes de criar os novos arquivos
    try:
        file_delete(output_dir)
        logger.info(f'Diretório {output_dir} limpo antes de criar novos arquivos parquet')
    except Exception as e:
        logger.warning(f'Não foi possível limpar diretório {output_dir}: {str(e)}')

    os.makedirs(output_dir, exist_ok=True)

    # Log das colunas antes de salvar
    logger.info(f"Colunas do DataFrame '{table_name}' antes de salvar em Parquet: {list(df.columns)}")

    # Configura o nome dos arquivos parquet com prefixo da tabela
    df.to_parquet(
        output_dir,
        engine='pyarrow',  # Especifica o engine
        write_index=False,
        name_function=lambda i: create_parquet_filename(table_name, i)
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
    """Aplica as transformações específicas para estabelecimentos."""
    logger.info("Aplicando transformações em Estabelecimentos...")
    
    # 1. Renomear colunas (Ajustar conforme nomes na config)
    rename_mapping = {
        'cnpj_basico': 'cnpj_basico', # Mantém
        'cnpj_ordem': 'cnpj_ordem',
        'cnpj_dv': 'cnpj_dv',
        'identificador_matriz_filial': 'matriz_filial',
        'nome_fantasia': 'nome_fantasia',
        'situacao_cadastral': 'codigo_situacao_cadastral', # Renomear
        'data_situacao_cadastral': 'data_situacao_cadastral',
        'motivo_situacao_cadastral': 'codigo_motivo_situacao_cadastral', # Renomear
        'nome_cidade_exterior': 'nome_cidade_exterior',
        'pais': 'pais',
        'data_inicio_atividade': 'data_inicio_atividades', # Renomear
        'cnae_fiscal_principal': 'codigo_cnae', # Renomear
        'cnae_fiscal_secundaria': 'cnae_secundaria',
        'tipo_logradouro': 'tipo_logradouro',
        'logradouro': 'logradouro',
        'numero': 'numero',
        'complemento': 'complemento',
        'bairro': 'bairro',
        'cep': 'cep',
        'uf': 'uf',
        'municipio': 'codigo_municipio', # Renomear
        'ddd_1': 'ddd1',
        'telefone_1': 'telefone1',
        'ddd_2': 'ddd2',
        'telefone_2': 'telefone2',
        'ddd_fax': 'ddd_fax',
        'fax': 'fax',
        'correio_eletronico': 'correio_eletronico',
        'situacao_especial': 'situacao_especial',
        'data_situacao_especial': 'data_situacao_especial'
    }
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in ddf.columns}
    ddf = ddf.rename(columns=actual_rename_mapping)
    logger.info(f"Colunas após renomear: {list(ddf.columns)}")

    # 2. Converter tipos (antes de outras operações)
    # 2.1 Campos numéricos inteiros - convertendo com segurança para Int64
    int_cols = ['matriz_filial', 'codigo_situacao_cadastral', 'codigo_motivo_situacao_cadastral', 
                'codigo_cnae', 'codigo_municipio']
    for col in int_cols:
        if col in ddf.columns:
            logger.info(f"Convertendo coluna '{col}' para Int64")
            ddf[col] = ddf[col].map_partitions(
                pd.to_numeric, errors='coerce', meta=(col, 'float64')
            ).astype('Int64')  # Int64 permite valores nulos
    
    # 2.2 Converta o cnpj_basico para Int64
    if 'cnpj_basico' in ddf.columns:
        ddf['cnpj_basico'] = ddf['cnpj_basico'].map_partitions(
            pd.to_numeric, errors='coerce', meta=('cnpj_basico', 'float64')
        ).astype('Int64')
        logger.info("Coluna 'cnpj_basico' convertida para Int64.")

    # 2.3 Converter colunas de data
    date_cols_to_convert = {
        'data_situacao_cadastral': None, # Meta inferida pelo Dask
        'data_inicio_atividades': None
    }
    for col, meta_type in date_cols_to_convert.items():
        if col in ddf.columns:
            logger.info(f"Convertendo coluna de data: {col}")
            # Substituir '0' ou '00000000' por NaN antes de converter
            # Usar map_partitions para aplicar a substituição e conversão
            def convert_date_partition(series):
                # Trata strings vazias e zeros antes da conversão
                try:
                    # Converte para string primeiro caso seja outro tipo
                    series = series.astype(str)
                    # Substitui valores inválidos por vazio
                    series = series.replace(['0', '00000000', 'nan', 'None', 'NaN'], '')
                    # Converte para datetime, com erros como NaT
                    return pd.to_datetime(series, errors='coerce', format='%Y%m%d')
                except Exception as e:
                    logger.warning(f"Erro ao converter coluna de data {col}: {str(e)}")
                    return pd.Series(pd.NaT, index=series.index)

            meta = (col, 'datetime64[ns]') if meta_type is None else (col, meta_type)
            ddf[col] = ddf[col].map_partitions(convert_date_partition, meta=meta)
            logger.info(f"Coluna '{col}' convertida para datetime.")

    # 3. Remover colunas
    cols_to_drop = [
        'cnpj_ordem', 'cnpj_dv', 'tipo_logradouro', 'logradouro', 'numero', 'complemento',
        'bairro', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'pais',
        'correio_eletronico', 'situacao_especial', 'data_situacao_especial',
        'nome_cidade_exterior'
    ]
    
    # 4. Criar coluna 'cnpj' completa (se necessário)
    # TODO: Implementar esta funcionalidade conforme necessidade
    
    # 5. Criar coluna 'tipo_situacao_cadastral'
    if 'codigo_situacao_cadastral' in ddf.columns and 'codigo_motivo_situacao_cadastral' in ddf.columns:
        logger.info("Criando coluna 'tipo_situacao_cadastral'...")
        # Usar diretamente as colunas já convertidas para Int64
        
        # Aplicar lógica com dask.dataframe.assign ou where
        ddf = ddf.assign(tipo_situacao_cadastral=1) # Default para ATIVA
        ddf['tipo_situacao_cadastral'] = ddf['tipo_situacao_cadastral'].where(
            ddf['codigo_situacao_cadastral'] != 2, 2) # INATIVA
        
        # Para BAIXADA, precisamos verificar se ambos os campos não são nulos
        baixada_condition = (
            (ddf['codigo_situacao_cadastral'] == 8) & 
            (ddf['codigo_motivo_situacao_cadastral'] == 1)
        )
        ddf['tipo_situacao_cadastral'] = ddf['tipo_situacao_cadastral'].where(
            ~baixada_condition, 3) # BAIXADA
        
        logger.info("Coluna 'tipo_situacao_cadastral' criada.")
    else:
        logger.warning("Colunas 'codigo_situacao_cadastral' ou 'codigo_motivo_situacao_cadastral' não encontradas. Pulando criação de 'tipo_situacao_cadastral'.")
        
    # Removendo colunas no final, após utilizar todos os dados necessários
    actual_cols_to_drop = [col for col in cols_to_drop if col in ddf.columns]
    if actual_cols_to_drop:
        ddf = ddf.drop(columns=actual_cols_to_drop)
        logger.info(f"Colunas removidas (final): {actual_cols_to_drop}")
    logger.info(f"Colunas finais após transformações e drop: {list(ddf.columns)}")

    return ddf


def process_estabelecimento(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de estabelecimentos."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento de ESTABELECIMENTOS')
    logger.info('=' * 50)

    # Verifica espaço em disco (Manter)
    logger.info("Verificando espaço em disco necessário para descompactação...")

    # Limpa o diretório de descompactação antes de começar
    try:
        file_delete(path_unzip)
    except PermissionError as e:
        logger.error(f'Sem permissão para limpar diretório de descompactação: {str(e)}')
        return False
    except Exception as e:
        logger.error(f'Erro inesperado ao limpar diretório de descompactação: {str(e)}')
        return False

    # Processa um arquivo ZIP por vez
    logger.info(f'Iniciando processamento de arquivos ZIP existentes em {path_zip}...')
    success = False
    try:
        # Lista todos os arquivos ZIP de estabelecimentos
        zip_files = [f for f in os.listdir(path_zip) if f.startswith('Estabelecimento') and f.endswith('.zip')]
        if not zip_files:
            logger.warning(f'Nenhum arquivo ZIP de Estabelecimentos encontrado em {path_zip} para processar.')
            return True  # Não é erro

        all_dfs = []
        for zip_file in zip_files:
            # Mover verificação de espaço para cá
            zip_path = os.path.join(path_zip, zip_file)
            logger.info(f'Processando arquivo ZIP: {zip_file}')
            estimated_size_mb = estimate_zip_extracted_size(zip_path)
            logger.info(f"Tamanho estimado após descompactação: {estimated_size_mb:.2f}MB")
            has_space_for_file, available_mb_now = check_disk_space(path_unzip, estimated_size_mb * 1.2)
            if not has_space_for_file:
                logger.error(
                    f"Espaço insuficiente para descompactar {zip_file}. Disponível: {available_mb_now:.2f}MB, necessário: {estimated_size_mb * 1.2:.2f}MB")
                continue

            # Descompacta apenas este arquivo ZIP
            try:
                logger.info(f'Descompactando arquivo: {zip_path}')
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(path_unzip)
                logger.info('Descompactação concluída com sucesso')
            except zipfile.BadZipFile as e:
                logger.error(f'Arquivo ZIP corrompido ou inválido {zip_path}: {str(e)}')
                continue
            except zipfile.LargeZipFile as e:
                logger.error(f'Arquivo ZIP muito grande para processamento {zip_path}: {str(e)}')
                continue
            except PermissionError as e:
                logger.error(f'Sem permissão para extrair arquivo ZIP {zip_path}: {str(e)}')
                continue
            except MemoryError as e:
                logger.error(f'Memória insuficiente para extrair arquivo ZIP {zip_path}: {str(e)}')
                continue
            except Exception as e:
                logger.error(f'Erro inesperado ao descompactar arquivo {zip_path}: {str(e)}')
                continue

            # Processa os dados deste arquivo
            try:
                # Lê todos os arquivos CSV descompactados
                try:
                    csv_files = [f for f in os.listdir(path_unzip) if 'CSV' in f]
                    if not csv_files:
                        logger.warning(f'Nenhum arquivo CSV encontrado após descompactar {zip_file}')
                        continue
                except FileNotFoundError as e:
                    logger.error(f'Diretório de descompactação não encontrado: {str(e)}')
                    continue
                except PermissionError as e:
                    logger.error(f'Sem permissão para acessar o diretório de descompactação: {str(e)}')
                    continue
                except Exception as e:
                    logger.error(f'Erro inesperado ao listar arquivos CSV: {str(e)}')
                    continue

                # Processa todos os arquivos CSV em paralelo
                logger.info(f'Processando {len(csv_files)} arquivos CSV em paralelo...')

                dfs = process_csv_files_parallel(
                    csv_files=csv_files,
                    base_path=path_unzip,
                    process_function=process_csv_file,
                    max_workers=config.dask.n_workers
                )

                # Filtra DataFrames vazios ou inválidos
                dfs = [df for df in dfs if df is not None]

                if dfs:
                    all_dfs.extend(dfs)
                    logger.info(f'Processamento de {len(dfs)} arquivos CSV concluído com sucesso')
                else:
                    logger.warning(f'Nenhum DataFrame válido foi gerado a partir dos arquivos CSV')

                success = True

            except Exception as e:
                logger.error(f'Erro inesperado ao processar dados do arquivo {zip_file}: {str(e)}')
                continue

        # Se temos DataFrames para processar, concatena todos e cria o parquet
        if all_dfs:
            logger.info('Concatenando todos os DataFrames...')
            try:
                # Verifica espaço para criação do arquivo parquet
                # Estima o tamanho como 50% do tamanho dos DataFrames em memória (compressão)
                parquet_size_estimate = sum([df.memory_usage(deep=True).sum().compute() for df in all_dfs]) * 0.5 / (
                            1024 * 1024)
                has_space, available_mb = check_disk_space(path_parquet, parquet_size_estimate)

                if not has_space:
                    logger.error(
                        f"Espaço insuficiente para criar arquivo parquet. Disponível: {available_mb:.2f}MB, estimado: {parquet_size_estimate:.2f}MB")
                    return False

                dd_estabelecimento_raw = dd.concat(all_dfs)
                
                # --- Adicionar criação do CNPJ aqui, antes do drop --- 
                if all(col in dd_estabelecimento_raw.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']): 
                    logger.info("Criando coluna 'cnpj' completa...")
                    dd_estabelecimento_raw['cnpj'] = dd_estabelecimento_raw['cnpj_basico'] + dd_estabelecimento_raw['cnpj_ordem'] + dd_estabelecimento_raw['cnpj_dv']
                    # Converter para numérico
                    dd_estabelecimento_raw['cnpj'] = dd_estabelecimento_raw['cnpj'].map_partitions(pd.to_numeric, errors='coerce', meta=('cnpj', 'float64')).astype('Int64')
                    logger.info("Coluna 'cnpj' criada e convertida.")
                else:
                     logger.warning("Colunas originais para CNPJ completo não encontradas.")

                # Aplicar transformações
                dd_estabelecimento = apply_estabelecimento_transformations(dd_estabelecimento_raw)
                
                # --- Salvar Parquet Principal --- 
                table_name_main = 'estabelecimentos' # Nome ajustado
                logger.info(f'Criando arquivo parquet {table_name_main}...')
                try:
                    create_parquet(dd_estabelecimento, table_name_main, path_parquet)
                    logger.info(f'Parquet {table_name_main} criado com sucesso')
                    success = True
                except Exception as e:
                    logger.error(f'Erro ao criar parquet {table_name_main}: {str(e)}')
                    success = False # Falha na principal, falha geral
                
                # --- Salvar Parquet Estabelecimentos GO --- 
                if success and 'uf' in dd_estabelecimento.columns:
                    logger.info("Iniciando criação do parquet para estabelecimentos_go...")
                    try:
                        dd_estabelecimento_go = dd_estabelecimento[dd_estabelecimento['uf'] == 'GO'].copy()
                        
                        # Verificar se o dataframe filtrado não está vazio
                        if not dd_estabelecimento_go.compute().empty:
                           table_name_go = 'estabelecimentos_go'
                           logger.info(f'Criando arquivo parquet {table_name_go}...')
                           create_parquet(dd_estabelecimento_go, table_name_go, path_parquet)
                           logger.info(f'Parquet {table_name_go} criado com sucesso')
                        else:
                           logger.warning("Nenhum estabelecimento encontrado em GO. Parquet 'estabelecimentos_go' não será criado.")
                           
                    except Exception as e:
                        logger.error(f'Erro ao processar/criar parquet para estabelecimentos_go: {str(e)}')
                        # Não falha o processo principal se este falhar
                elif 'uf' not in dd_estabelecimento.columns:
                    logger.warning("Coluna 'uf' não encontrada. Pulando criação de 'estabelecimentos_go'.")
                    
                # Limpa o diretório após criar os arquivos parquet
                try:
                    file_delete(path_unzip)
                    logger.info('Diretório de descompactação limpo após processamento')
                except Exception as e:
                    logger.warning(f'Não foi possível limpar diretório após processamento: {str(e)}')

            except MemoryError as e:
                logger.error(f'Memória insuficiente para concatenar DataFrames: {str(e)}')
                success = False
            except Exception as e:
                logger.error(f'Erro inesperado ao concatenar DataFrames: {str(e)}')
                success = False
        else:
            logger.error('Nenhum dado foi processado com sucesso')

        return success

    except FileNotFoundError:
        logger.error(f"Diretório de origem dos ZIPs não encontrado: {path_zip}")
        return False
    except Exception as e:
        logger.exception(f'Erro inesperado no processo principal de estabelecimentos: {e}')
        return False
