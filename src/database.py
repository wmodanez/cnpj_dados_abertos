import datetime
import os
import shutil
import logging
import duckdb
from config import config

logger = logging.getLogger(__name__)

def create_duckdb_file(path_parquet: str, file_db_parquet: str, path_remote_parquet: str, yyyymm: str) -> None:
    """Cria um arquivo DuckDB com as tabelas parquet."""
    logger.info('Início da criação das tabelas parquet')
    inter_time: datetime = datetime.datetime.now()
    
    # Verifica se o diretório de destino existe e tem permissões
    try:
        if not os.path.exists(path_remote_parquet):
            logger.info(f'Criando diretório de destino: {path_remote_parquet}')
            os.makedirs(path_remote_parquet, exist_ok=True)
    except PermissionError:
        logger.error(f'Erro: Sem permissão para criar/acessar o diretório {path_remote_parquet}')
        return
    except Exception as e:
        logger.error(f'Erro ao verificar diretório de destino: {str(e)}')
        return

    # Remove arquivo existente se houver
    try:
        if os.path.exists(path_parquet + file_db_parquet):
            logger.info(f'Removendo arquivo existente: {path_parquet + file_db_parquet}')
            os.remove(path_parquet + file_db_parquet)
    except Exception as e:
        logger.warning(f'Não foi possível remover arquivo existente: {str(e)}')

    try:
        logger.info('Iniciando conexão com DuckDB')
        conn: duckdb.connect = duckdb.connect(
            path_parquet + file_db_parquet,
            config={
                'threads': config.database.threads,
                'memory_limit': config.dask.memory_limit,
                'use_direct_io': True,
                'compression': True,
                'checkpoint_threshold': '1GB'
            }
        )

        # Lista de tabelas principais
        list_tabela: list = ['empresas', 'estabelecimentos', 'estabelecimentos_go', 'simples', 'socios']
        
        # Processa tabelas principais em paralelo
        for tabela in list_tabela:
            path_parquet: str = path_parquet + yyyymm + '/' + tabela + '/*.parquet'
            logger.info(f'Processando tabela: {tabela}')
            add_table_parquet(conn, tabela, path_parquet)

        # Lista de tabelas fixas
        list_tabela_fixa: list = ['cnae', 'motivo', 'municipio']
        
        # Processa tabelas fixas
        for tabela in list_tabela_fixa:
            path_parquet = path_parquet + 'base/' + tabela + '.parquet'
            logger.info(f'Processando tabela fixa: {tabela}')
            add_table_parquet(conn, tabela, path_parquet)
        
        logger.info(f'Tempo total de carga das tabelas: {str(datetime.datetime.now() - inter_time)}')

        # Tenta copiar o arquivo para o destino remoto
        try:
            if not os.path.exists(path_parquet + file_db_parquet):
                logger.error(f'Arquivo de origem não encontrado: {path_parquet + file_db_parquet}')
                return

            logger.info(f'Iniciando cópia do arquivo para: {path_remote_parquet + file_db_parquet}')
            shutil.copy2(path_parquet + file_db_parquet, path_remote_parquet + file_db_parquet)
            logger.info('Arquivo copiado com sucesso')
            logger.info(f'Tempo gasto para cópia do arquivo: {str(datetime.datetime.now() - inter_time)}')
            
        except PermissionError:
            logger.error(f'Erro: Sem permissão para copiar arquivo para {path_remote_parquet}')
        except Exception as e:
            logger.error(f'Erro ao copiar o arquivo: {str(e)}')

    except Exception as e:
        logger.error(f'Erro ao criar banco de dados: {str(e)}')
    finally:
        try:
            conn.close()
            logger.info('Conexão com DuckDB fechada')
        except Exception as e:
            logger.warning(f'Erro ao fechar conexão: {str(e)}')

def add_table_parquet(conn: duckdb.connect, table: str, path_parquet: str) -> None:
    """Adiciona uma tabela ao DuckDB a partir de arquivos parquet."""
    inter_timer: datetime = datetime.datetime.now()
    try:
        logger.info(f'Iniciando carregamento da tabela {table}')
        
        # Otimiza a query para melhor performance
        sql: str = f'''
            CREATE OR REPLACE TABLE {table} AS 
            SELECT * FROM '{path_parquet}'
            USING SAMPLE 100 ROWS
        '''
        
        # Executa a query com otimizações
        conn.execute("PRAGMA enable_progress_bar")
        conn.execute("PRAGMA threads=" + str(config.database.threads))
        conn.execute(sql)
        
        # Cria índices para melhorar performance de consultas
        if table in ['empresas', 'estabelecimentos', 'estabelecimentos_go']:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_cnpj ON {table}(cnpj_basico)")
        
        logger.info(f'Tabela {table} carregada com sucesso')
        logger.info(f'Tempo de carregamento da tabela {table}: {str(datetime.datetime.now() - inter_timer)}')
    except Exception as e:
        logger.error(f'Erro ao carregar tabela {table}: {str(e)}')
        raise 