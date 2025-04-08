import logging
import os
import shutil

import duckdb

from .config import config

logger = logging.getLogger(__name__)


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
                    # Normaliza as barras invertidas para o SQL
                    parquet_files_in_dir.append(os.path.join(root, f).replace('\\', '/'))

            if parquet_files_in_dir:
                parquet_files[table_name] = parquet_files_in_dir
                logger.info(f"Encontrados {len(parquet_files_in_dir)} arquivos parquet na pasta {table_name}")

    # Busca os arquivos parquet na pasta base
    base_path = os.path.join(path_parquet_folder, 'base')
    if os.path.exists(base_path):
        base_files = []
        for f in os.listdir(base_path):
            if f.endswith('.parquet'):
                # Normaliza as barras invertidas para o SQL
                base_files.append(os.path.join(base_path, f).replace('\\', '/'))
        if base_files:
            parquet_files['base'] = base_files
            logger.info(f"Encontrados {len(base_files)} arquivos parquet na pasta base")

    if not parquet_files:
        logger.warning(f"Nenhum arquivo parquet encontrado em {path_parquet_folder}")
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
        for table_name, files in parquet_files.items():
            logger.info(f"Criando tabela {table_name} com {len(files)} arquivos...")

            # Cria uma lista SQL literal: ['path/file1.parquet', 'path/file2.parquet', ...]
            sql_file_list = '[' + ', '.join([f"'{f}'" for f in files]) + ']'

            # Cria a tabela combinando todos os arquivos da pasta usando a lista SQL
            conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_parquet({sql_file_list})
            """)
            logger.info(f"Tabela {table_name} criada com sucesso")

        # Fecha a conexão
        conn.close()
        logger.info("Banco de dados criado com sucesso")

        # Se houver caminho remoto, faz backup
        if path_remote_parquet:
            remote_path = os.path.join(path_remote_parquet, os.path.basename(path_parquet_folder))
            logger.info(f"Fazendo backup para {remote_path}...")

            # Cria o diretório remoto se não existir
            os.makedirs(remote_path, exist_ok=True)

            # Copia o arquivo do banco de dados
            shutil.copy2(db_path, os.path.join(remote_path, file_db_parquet))
            logger.info("Backup concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro ao criar banco de dados: {e}")
        raise
