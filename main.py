import datetime
import logging
import os
from multiprocessing import freeze_support
import requests
from bs4 import BeautifulSoup
from dask.distributed import Client, LocalCluster
from dotenv import load_dotenv
from config import config
from src.utils import check_basic_folders
from src.process.empresa import process_empresa
from src.process.estabelecimento import process_estabelecimento
from src.process.simples import process_simples
from src.process.socio import process_socio
from src.database import create_duckdb_file

def setup_logging():
    """Configura o sistema de logging."""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    log_filename = f'logs/cnpj_process_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def main():
    """Função principal que orquestra todo o processo."""
    logger = setup_logging()
    logger.info(f'Início da execução dia: {datetime.datetime.now():%d/%m/%Y} às {datetime.datetime.now():%H:%M:%S}')
    start_time: datetime = datetime.datetime.now()
    
    # Carrega variáveis de ambiente
    load_dotenv('.env.local')
    
    # Configurações de diretórios
    URL: str = os.getenv('URL_ORIGIN')
    PATH_ZIP: str = os.getenv('PATH_ZIP')
    PATH_UNZIP: str = os.getenv('PATH_UNZIP')
    PATH_PARQUET: str = os.getenv('PATH_PARQUET')
    FILE_DB_PARQUET: str = os.getenv('FILE_DB_PARQUET')
    PATH_REMOTE_PARQUET: str = os.getenv('PATH_REMOTE_PARQUET')
    
    # Cria diretórios necessários
    list_folders: list = [PATH_ZIP, PATH_UNZIP, PATH_PARQUET]
    for folder in list_folders:
        check_basic_folders(folder)

    # Configuração do Dask
    freeze_support()
    cluster: LocalCluster = LocalCluster(
        n_workers=config.dask.n_workers,
        threads_per_worker=config.dask.threads_per_worker,
        memory_limit=config.dask.memory_limit,
        dashboard_address=config.dask.dashboard_address
    )
    client: Client = Client(cluster)
    logger.info(f'Cliente Dask inicializado: {client}')

    # Obtém a URL mais recente dos dados
    soup: BeautifulSoup = BeautifulSoup(requests.get(URL).text, 'html.parser')
    list_folders = []
    for element in soup.find_all('a'):
        if '-' in element.get('href'):
            list_folders.append(element.get('href'))
    URL += max(list_folders)
    soup: BeautifulSoup = BeautifulSoup(requests.get(URL).text, 'html.parser')

    # Processa os dados
    is_create_db_parquet: bool = False
    yyyymm: str = datetime.date.today().strftime('%Y%m')

    # Processa cada tipo de dado
    if process_empresa(soup, URL, PATH_ZIP, PATH_UNZIP):
        is_create_db_parquet = True
    if process_estabelecimento(soup, URL, PATH_ZIP, PATH_UNZIP):
        is_create_db_parquet = True
    if process_simples(soup, URL, PATH_ZIP, PATH_UNZIP):
        is_create_db_parquet = True
    if process_socio(soup, URL, PATH_ZIP, PATH_UNZIP):
        is_create_db_parquet = True

    # Cria o banco de dados se necessário
    if is_create_db_parquet:
        create_duckdb_file(PATH_PARQUET, FILE_DB_PARQUET, PATH_REMOTE_PARQUET, yyyymm)

    logger.info(f'Tempo total: {str(datetime.datetime.now() - start_time)}')
    client.shutdown()

if __name__ == '__main__':
    main() 