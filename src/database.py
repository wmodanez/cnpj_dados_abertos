import logging
import os
import shutil

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .config import config

logger = logging.getLogger(__name__)


def verify_parquet_file(file_path):
    """Verifica se um arquivo parquet é válido.
    
    Args:
        file_path: Caminho do arquivo parquet
        
    Returns:
        bool: True se o arquivo é válido, False caso contrário
    """
    try:
        # Tenta abrir o arquivo para verificar se é um parquet válido
        pq.read_metadata(file_path)
        return True
    except Exception as e:
        logger.error(f"Arquivo parquet inválido: {file_path} - {str(e)}")
        return False


def create_duckdb_file(path_parquet_folder: str, file_db_parquet: str, path_remote_parquet: str) -> bool:
    """Cria um arquivo DuckDB a partir dos arquivos parquet."""
    logger.info(f"Iniciando criação do banco de dados DuckDB em {path_parquet_folder}...")

    parquet_files = {}

    if os.path.exists(path_parquet_folder):
        for root, dirs, files in os.walk(path_parquet_folder):
            if root == path_parquet_folder:
                continue

            # Pega o nome da subpasta (simples, socio, etc)
            table_name = os.path.basename(root)

            # Busca arquivos parquet nesta pasta
            parquet_files_in_dir = []
            for f in files:
                if f.endswith('.parquet'):
                    file_path = os.path.join(root, f)
                    # Verificar se o arquivo parquet é válido
                    if verify_parquet_file(file_path):
                        # Normaliza as barras invertidas para o SQL
                        parquet_files_in_dir.append(file_path.replace('\\', '/'))
                    else:
                        logger.warning(f"Arquivo parquet inválido ignorado: {file_path}")

            if parquet_files_in_dir:
                parquet_files[table_name] = parquet_files_in_dir
                logger.info(f"Encontrados {len(parquet_files_in_dir)} arquivos parquet válidos na pasta {table_name}")

    # Busca os arquivos parquet na pasta base (diretamente na raiz de PATH_PARQUET) e cria uma tabela para cada um
    # Presume que path_parquet_folder é algo como './parquet/<subpasta_processamento>'
    # Queremos buscar em './parquet/base'
    path_parquet_root = os.path.dirname(path_parquet_folder) # Obtém o diretório pai (ex: ./parquet)
    base_path = os.path.join(path_parquet_root, 'base') # Constrói o caminho para ./parquet/base

    if os.path.exists(base_path):
        logger.info(f"Procurando arquivos Parquet na pasta base: {base_path}")
        files_in_base_count = 0
        for f in os.listdir(base_path):
            if f.endswith('.parquet'):
                file_path = os.path.join(base_path, f)
                if verify_parquet_file(file_path):
                    # Normaliza as barras invertidas para o SQL
                    normalized_file_path = file_path.replace('\\\\', '/')
                    # Deriva o nome da tabela do nome do arquivo (sem extensão)
                    table_name_from_file = os.path.splitext(f)[0]
                    # Adiciona uma entrada separada para este arquivo no dicionário
                    parquet_files[table_name_from_file] = [normalized_file_path]
                    logger.info(f"Arquivo parquet base válido encontrado: {f}. Será criada a tabela '{table_name_from_file}'.")
                    files_in_base_count += 1
                else:
                    logger.warning(f"Arquivo parquet base inválido ignorado: {file_path}")
        if files_in_base_count > 0:
             logger.info(f"Encontrados {files_in_base_count} arquivos parquet válidos na pasta base para criar tabelas individuais.")
        else:
             logger.info(f"Nenhum arquivo parquet válido encontrado na pasta base ({base_path}).")
    else:
        logger.warning(f"Pasta base não encontrada em: {base_path}")

    if not parquet_files:
        logger.warning(f"Nenhum arquivo parquet válido encontrado em {path_parquet_folder} ou em sua subpasta 'base'")
        return False

    # Cria o banco de dados
    db_path = os.path.join(path_parquet_folder, file_db_parquet)

    # Verifica se o arquivo de banco de dados já existe e o remove
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            logger.info(f"Arquivo de banco de dados existente removido: {db_path}")
        except OSError as e:
            logger.error(f"Erro ao remover arquivo de banco de dados existente {db_path}: {e}")
            # Decide se quer parar ou continuar. Por enquanto, vamos parar.
            return False

    try:
        # Conecta ao banco de dados com configurações otimizadas
        conn = duckdb.connect(db_path, config={
            'threads': config.database.threads,
            'memory_limit': config.dask.memory_limit,
            'checkpoint_threshold': '1GB'
        })
        logger.info(f"Banco de dados criado em {db_path}")

        # Para cada pasta (simples, empresa, estabelecimento, etc.), cria uma tabela
        success_count = 0
        for table_name, files in parquet_files.items():
            logger.info(f"Criando tabela {table_name} com {len(files)} arquivos...")

            # Cria uma lista SQL literal: ['path/file1.parquet', 'path/file2.parquet', ...]
            sql_file_list = '[' + ', '.join([f"'{f}'" for f in files]) + ']'

            try:
                # Cria a tabela combinando todos os arquivos da pasta usando a lista SQL
                conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM read_parquet({sql_file_list})
                """)
                logger.info(f"Tabela {table_name} criada com sucesso")
                success_count += 1
            except Exception as e:
                logger.error(f"Erro ao criar tabela {table_name}: {e}")

        # Fecha a conexão
        conn.close()
        
        if success_count > 0:
            logger.info(f"Banco de dados criado com sucesso com {success_count} tabelas")
            
            # Se houver caminho remoto, faz backup
            if path_remote_parquet:
                remote_path = os.path.join(path_remote_parquet, os.path.basename(path_parquet_folder))
                logger.info(f"Fazendo backup para {remote_path}...")

                # Cria o diretório remoto se não existir
                os.makedirs(remote_path, exist_ok=True)

                # Copia o arquivo do banco de dados
                shutil.copy2(db_path, os.path.join(remote_path, file_db_parquet))
                logger.info("Backup concluído com sucesso")
            
            return True
        else:
            logger.error("Nenhuma tabela foi criada com sucesso")
            return False

    except Exception as e:
        logger.error(f"Erro ao criar banco de dados: {e}")
        return False
