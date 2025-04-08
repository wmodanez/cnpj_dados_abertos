import datetime
import logging
import dask.dataframe as dd
from ..config import config
from ..utils import file_delete, check_disk_space, estimate_zip_extracted_size
from ..utils import process_csv_files_parallel, process_csv_to_df, verify_csv_integrity, create_parquet_filename
import os
import zipfile

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
    
    # Configura o nome dos arquivos parquet com prefixo da tabela
    df.to_parquet(
        output_dir,
        write_index=False,
        name_function=lambda i: create_parquet_filename(table_name, i)
    )

def process_csv_file(csv_path):
    """
    Função para processar um único arquivo CSV de empresas.
    
    Args:
        csv_path: Caminho do arquivo CSV a ser processado
        
    Returns:
        DataFrame Dask com os dados processados ou None em caso de erro
    """
    # Verifica a integridade do CSV
    if not verify_csv_integrity(csv_path):
        logger.error(f'Falha na verificação de integridade do arquivo {os.path.basename(csv_path)}')
        return None
    
    # Define os tipos de dados para as colunas
    dtype_dict = {
        'cnpj_basico': 'object',
        'razao_social': 'object',
        'natureza_juridica': 'object',
        'qualificacao_responsavel': 'object',
        'capital_social': 'object',
        'porte_empresa': 'object',
        'ente_federativo_responsavel': 'object'
    }
    
    try:
        df = process_csv_to_df(csv_path, dtype=dtype_dict)
        return df
    except Exception as e:
        logger.error(f'Erro ao processar arquivo {os.path.basename(csv_path)}: {str(e)}')
        return None

def process_empresa(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de empresas."""
    logger.info('='*50)
    logger.info('Iniciando processamento de EMPRESAS')
    logger.info('='*50)
    
    # Verifica espaço em disco para o diretório de trabalho
    # Requisito mínimo: 5GB para trabalhar com segurança
    has_space, available_mb = check_disk_space(path_unzip, 5000)
    if not has_space:
        logger.error(f"Espaço em disco insuficiente para processar os dados. Disponível: {available_mb:.2f}MB, necessário: 5000MB")
        return False
    logger.info(f"Verificação de espaço em disco concluída: {available_mb:.2f}MB disponível")
    
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
            return True # Retorna True pois não é um erro, apenas não há nada a fazer

        all_dfs = []

        # Processa cada arquivo ZIP individualmente
        for zip_file in zip_files:
            zip_path = os.path.join(path_zip, zip_file)
            logger.info(f'Processando arquivo ZIP: {zip_file}')

            # Estima o tamanho que o arquivo ocupará quando descompactado
            estimated_size_mb = estimate_zip_extracted_size(zip_path)
            logger.info(f"Tamanho estimado após descompactação: {estimated_size_mb:.2f}MB")

            # Verifica se há espaço suficiente PARA ESTE ARQUIVO
            has_space_for_file, available_mb_now = check_disk_space(path_unzip, estimated_size_mb * 1.2) # 20% margem
            if not has_space_for_file:
                logger.error(f"Espaço insuficiente para descompactar {zip_file}. Disponível: {available_mb_now:.2f}MB, necessário: {estimated_size_mb * 1.2:.2f}MB")
                continue # Pula para o próximo arquivo ZIP
            
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
                parquet_size_estimate = sum([df.memory_usage(deep=True).sum().compute() for df in all_dfs]) * 0.5 / (1024 * 1024)
                has_space, available_mb = check_disk_space(path_parquet, parquet_size_estimate)
                
                if not has_space:
                    logger.error(f"Espaço insuficiente para criar arquivo parquet. Disponível: {available_mb:.2f}MB, estimado: {parquet_size_estimate:.2f}MB")
                    return False
                
                dd_empresa = dd.concat(all_dfs)
                
                # Renomeia as colunas
                dd_empresa = dd_empresa.rename(columns={
                    'cnpj_basico': 'cnpj',
                    'razao_social': 'razao_social',
                    'natureza_juridica': 'natureza_juridica',
                    'qualificacao_responsavel': 'qualificacao_responsavel',
                    'capital_social': 'capital_social',
                    'porte_empresa': 'porte_empresa',
                    'ente_federativo_responsavel': 'ente_federativo_responsavel'
                })
                
                # Converte para parquet
                table_name = 'empresa'
                logger.info(f'Criando arquivo parquet {table_name}...')
                try:
                    create_parquet(dd_empresa, table_name, path_parquet)
                    logger.info('Processamento concluído com sucesso')
                    success = True
                except PermissionError as e:
                    logger.error(f'Sem permissão para criar arquivo parquet: {str(e)}')
                    success = False
                except IOError as e:
                    logger.error(f'Erro de I/O ao criar arquivo parquet: {str(e)}')
                    success = False
                except Exception as e:
                    logger.error(f'Erro inesperado ao criar arquivo parquet: {str(e)}')
                    success = False

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