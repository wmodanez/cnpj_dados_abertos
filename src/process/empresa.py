import logging
import os
import zipfile
import dask.dataframe as dd
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


def apply_empresa_transformations(ddf):
    """Aplica as transformações específicas para Empresas."""
    logger.info("Aplicando transformações em Empresas...")
    
    # 1. Renomear colunas 
    rename_mapping = {
        'cnpj_basico': 'cnpj',
        'razao_social_nome_empresarial': 'razao_social',
        'natureza_juridica': 'natureza_juridica',
        'qualificacao_do_responsavel': 'qualificacao_responsavel',
        'capital_social_da_empresa': 'capital_social',
        'porte_da_empresa': 'porte_empresa',
        'ente_federativo_responsavel': 'ente_federativo_responsavel'
    }
    # Filtra o mapeamento para incluir apenas colunas existentes no DataFrame
    actual_rename_mapping = {k: v for k, v in rename_mapping.items() if k in ddf.columns}
    ddf = ddf.rename(columns=actual_rename_mapping)
    logger.info(f"Colunas após renomear: {list(ddf.columns)}")
    
    # 2. Converter colunas para tipos corretos (após serem lidas como string)
    # Campos que precisam ser numéricos para operações posteriores
    int_cols = ['natureza_juridica', 'qualificacao_responsavel', 'porte_empresa']
    for col in int_cols:
        if col in ddf.columns:
            logger.info(f"Convertendo coluna '{col}' para Int64")
            try:
                # Tenta converter para numérico
                ddf[col] = ddf[col].map_partitions(
                    pd.to_numeric, errors='coerce', meta=(col, 'float64')
                ).astype('Int64')  # Int64 permite valores nulos
                
                # Verifica o tipo depois da conversão
                coluna_tipo = ddf[col].dtype
                logger.info(f"Coluna '{col}' convertida com sucesso. Tipo atual: {coluna_tipo}")
                
                # Se for natureza_juridica, verifica se existem valores entre 2046 e 2348
                if col == 'natureza_juridica':
                    # Verifica a distribuição de valores para confirmar a conversão
                    try:
                        # Tenta acessar apenas a primeira partição para evitar computação pesada
                        primeira_particao = ddf[col].partitions[0].compute()
                        if len(primeira_particao) > 0:
                            valores_unicos = primeira_particao.unique()
                            logger.info(f"Valores únicos em primeira_particao de {col} (amostra): {valores_unicos[:10] if len(valores_unicos) > 10 else valores_unicos}")
                            
                            # Verifica se existem valores no intervalo de interesse
                            valores_privados = [v for v in valores_unicos if isinstance(v, (int, float)) and 2046 <= v <= 2348]
                            if valores_privados:
                                logger.info(f"Encontrados valores de empresa privada em {col}: {valores_privados[:5]}")
                            else:
                                logger.warning(f"Não foram encontrados valores no intervalo de empresas privadas (2046-2348) em {col}")
                    except Exception as e:
                        logger.warning(f"Não foi possível verificar distribuição de valores em {col}: {e}")
            except Exception as e:
                logger.error(f"Erro ao converter coluna '{col}' para Int64: {str(e)}")
        else:
            logger.warning(f"Coluna '{col}' não encontrada para conversão.")
            
    # Conversão do campo capital_social (manuseia vírgulas como separador decimal)
    if 'capital_social' in ddf.columns:
        logger.info("Convertendo capital_social (tratando vírgulas como separador decimal)")
        # Função para converter valores monetários com vírgula
        def convert_monetary_value(series):
            try:
                # Converte para string primeiro caso seja outro tipo
                series = series.astype(str)
                # Substitui vírgula por ponto e converte para float
                return series.str.replace(',', '.', regex=False).astype(float, errors='ignore')
            except Exception as e:
                logger.warning(f"Erro no tratamento monetário: {e}")
                return series
            
        # Aplicar a conversão
        try:
            ddf['capital_social'] = ddf['capital_social'].map_partitions(convert_monetary_value, meta=('capital_social', 'float64'))
            logger.info("Conversão de capital_social concluída com sucesso")
        except Exception as e:
            logger.warning(f"Erro ao converter capital_social: {str(e)}. Mantendo como está.")
    
    # Log dos tipos de dados após transformações
    tipos_colunas = {col: str(ddf[col].dtype) for col in ddf.columns}
    logger.info(f"Tipos de dados após transformações: {tipos_colunas}")
    
    return ddf


def process_empresa(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de empresas."""
    logger.info('=' * 50)
    logger.info('Iniciando processamento de EMPRESAS')
    logger.info('=' * 50)

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
        # Lista todos os arquivos ZIP de empresas
        zip_files = [f for f in os.listdir(path_zip) if f.startswith('Empresa') and f.endswith('.zip')]
        if not zip_files:
            logger.warning(f'Nenhum arquivo ZIP de Empresas encontrado em {path_zip} para processar.')
            return True  # Retorna True pois não é um erro, apenas não há nada a fazer

        all_dfs = []

        # Processa cada arquivo ZIP individualmente
        for zip_file in zip_files:
            zip_path = os.path.join(path_zip, zip_file)
            logger.info(f'Processando arquivo ZIP: {zip_file}')

            # Estima o tamanho que o arquivo ocupará quando descompactado
            estimated_size_mb = estimate_zip_extracted_size(zip_path)
            logger.info(f"Tamanho estimado após descompactação: {estimated_size_mb:.2f}MB")

            # Verifica se há espaço suficiente PARA ESTE ARQUIVO
            has_space_for_file, available_mb_now = check_disk_space(path_unzip, estimated_size_mb * 1.2)  # 20% margem
            if not has_space_for_file:
                logger.error(
                    f"Espaço insuficiente para descompactar {zip_file}. Disponível: {available_mb_now:.2f}MB, necessário: {estimated_size_mb * 1.2:.2f}MB")
                continue  # Pula para o próximo arquivo ZIP

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

                dd_empresa_raw = dd.concat(all_dfs)

                # Aplicar transformações
                dd_empresa = apply_empresa_transformations(dd_empresa_raw)

                # --- Criação do Parquet Principal ('empresas') ---
                table_name_main = 'empresas' # Nome ajustado
                logger.info(f'Criando arquivo parquet {table_name_main}...')
                try:
                    create_parquet(dd_empresa, table_name_main, path_parquet)
                    logger.info(f'Parquet {table_name_main} criado com sucesso')
                    success = True
                except PermissionError as e:
                    logger.error(f'Sem permissão para criar arquivo parquet {table_name_main}: {str(e)}')
                    success = False
                except IOError as e:
                    logger.error(f'Erro de I/O ao criar arquivo parquet {table_name_main}: {str(e)}')
                    success = False
                except Exception as e:
                    logger.error(f'Erro inesperado ao criar arquivo parquet {table_name_main}: {str(e)}')
                    success = False
                
                # --- Criação do Parquet de Empresas Privadas ('empresa_privada') ---
                if success and 'natureza_juridica' in dd_empresa.columns: # Procede apenas se o parquet principal foi criado e a coluna existe
                    logger.info("Iniciando criação do parquet para empresas privadas...")
                    try:
                        # Verifica se a coluna natureza_juridica é do tipo numérico (Int64)
                        coluna_tipo = dd_empresa['natureza_juridica'].dtype
                        logger.info(f"Tipo atual da coluna natureza_juridica: {coluna_tipo}")
                        
                        # Tenta obter alguns valores para diagnóstico
                        try:
                            # Pega alguns valores da primeira partição para verificar
                            amostra = dd_empresa['natureza_juridica'].partitions[0].compute().head(10)
                            logger.info(f"Amostra de valores em natureza_juridica: {amostra.tolist()}")
                            
                            # Verifica se há valores nulos
                            qtd_nulos = dd_empresa['natureza_juridica'].isna().sum().compute()
                            logger.info(f"Quantidade de valores nulos em natureza_juridica: {qtd_nulos}")
                        except Exception as e:
                            logger.warning(f"Não foi possível obter amostra de valores: {e}")
                        
                        # Verifica se o tipo é numérico ou se precisa converter
                        if 'int' in str(coluna_tipo).lower() or 'float' in str(coluna_tipo).lower():
                            # A coluna já está como numérica, então podemos filtrar diretamente
                            logger.info("Utilizando coluna 'natureza_juridica' já convertida para numérico")
                            
                            # Tenta fazer a filtragem usando o método between
                            try:
                                dd_empresa_privada = dd_empresa[
                                    dd_empresa['natureza_juridica'].between(2046, 2348, inclusive='both')
                                ].copy()
                                logger.info("Filtragem com between realizada com sucesso")
                            except Exception as e:
                                logger.warning(f"Erro ao usar between para filtrar: {e}")
                                # Tenta uma alternativa usando comparação direta
                                try:
                                    dd_empresa_privada = dd_empresa[
                                        (dd_empresa['natureza_juridica'] >= 2046) & 
                                        (dd_empresa['natureza_juridica'] <= 2348)
                                    ].copy()
                                    logger.info("Filtragem com comparação direta realizada com sucesso")
                                except Exception as e2:
                                    logger.error(f"Todas as tentativas de filtragem falharam: {e2}")
                                    raise
                        else:
                            # Precisamos converter para numérico antes de filtrar
                            logger.warning(f"Coluna 'natureza_juridica' não está em formato numérico (tipo: {coluna_tipo}). Convertendo para filtragem.")
                            
                            # Criar coluna temporária para filtragem com log detalhado do processo
                            try:
                                # Primeiro, converte para numérico
                                logger.info("Criando coluna temporária natureza_juridica_num...")
                                dd_empresa['natureza_juridica_num'] = dd_empresa['natureza_juridica'].map_partitions(
                                    pd.to_numeric, errors='coerce', meta=('natureza_juridica', 'float64') 
                                ).astype('Int64')
                                
                                # Verifica se a conversão foi bem-sucedida
                                temp_coluna_tipo = dd_empresa['natureza_juridica_num'].dtype
                                logger.info(f"Coluna temporária criada. Tipo: {temp_coluna_tipo}")
                                
                                # Tenta ver uma amostra da nova coluna
                                try:
                                    amostra_temp = dd_empresa['natureza_juridica_num'].partitions[0].compute().head(10)
                                    logger.info(f"Amostra de valores em natureza_juridica_num: {amostra_temp.tolist()}")
                                except Exception as e:
                                    logger.warning(f"Não foi possível obter amostra da coluna temporária: {e}")
                                
                                # Filtra utilizando a coluna temporária
                                dd_empresa_privada = dd_empresa[
                                    dd_empresa['natureza_juridica_num'].between(2046, 2348, inclusive='both')
                                ].copy()
                                logger.info("Filtragem com coluna temporária realizada com sucesso")
                            except Exception as e:
                                logger.error(f"Erro durante a criação/filtragem com coluna temporária: {e}")
                                raise

                        # Log do número de registros filtrados
                        try:
                            num_registros = dd_empresa_privada.shape[0].compute()
                            logger.info(f"Número de empresas privadas filtradas: {num_registros}")
                        except Exception as e:
                            logger.warning(f"Não foi possível contar o número de registros filtrados: {e}")
                        
                        # Colunas a serem removidas (verificar se existem antes de dropar)
                        cols_to_drop = ['qualificacao_responsavel', 'ente_federativo_responsavel']
                        # Adicionar natureza_juridica_num à lista se ela existe
                        if 'natureza_juridica_num' in dd_empresa_privada.columns:
                            cols_to_drop.append('natureza_juridica_num')
                            
                        actual_cols_to_drop = [col for col in cols_to_drop if col in dd_empresa_privada.columns]
                        
                        if actual_cols_to_drop:
                           dd_empresa_privada = dd_empresa_privada.drop(columns=actual_cols_to_drop)
                           logger.info(f"Colunas removidas para empresa_privada: {actual_cols_to_drop}")
                        else:
                            logger.warning("Nenhuma das colunas esperadas para drop ('qualificacao_responsavel', 'ente_federativo_responsavel') encontrada em empresa_privada.")

                        # Criar o parquet para empresas privadas
                        table_name_privada = 'empresa_privada'
                        logger.info(f'Criando arquivo parquet {table_name_privada}...')
                        create_parquet(dd_empresa_privada, table_name_privada, path_parquet)
                        logger.info(f'Parquet {table_name_privada} criado com sucesso')

                    except Exception as e:
                        logger.error(f'Erro ao processar/criar parquet para {table_name_privada}: {str(e)}')
                        # Não definimos success como False aqui, pois o parquet principal pode ter sido criado
                elif 'natureza_juridica' not in dd_empresa.columns:
                     logger.warning("Coluna 'natureza_juridica' não encontrada. Pulando criação do parquet 'empresa_privada'.")


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
        logger.exception(f'Erro inesperado no processo principal de empresas: {e}')
        return False
