import datetime
import logging
import os
from multiprocessing import freeze_support
import requests
from bs4 import BeautifulSoup
from dask.distributed import Client, LocalCluster
from dotenv import load_dotenv
from src.config import config, IGNORED_FILES
from src.utils import check_basic_folders, check_internet_connection
from src.process.empresa import process_empresa
from src.process.estabelecimento import process_estabelecimento
from src.process.simples import process_simples
from src.process.socio import process_socio
from src.database import create_duckdb_file
from src.download import download_files_parallel

# Cores para o console
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class ColoredFormatter(logging.Formatter):
    """Formatação colorida para o console."""
    def format(self, record):
        if record.levelname == 'INFO':
            color = Colors.GREEN
        elif record.levelname == 'WARNING':
            color = Colors.YELLOW
        elif record.levelname == 'ERROR':
            color = Colors.RED
        elif record.levelname == 'CRITICAL':
            color = Colors.RED + Colors.BOLD
        else:
            color = Colors.END
            
        record.msg = f"{color}{record.msg}{Colors.END}"
        return super().format(record)

def setup_logging():
    """Configura o sistema de logging."""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    log_filename = f'logs/cnpj_process_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Configuração do logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Handler para arquivo (sem cores)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    logger.addHandler(file_handler)
    
    # Handler para console (com cores)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter(log_format, date_format))
    logger.addHandler(console_handler)
    
    return logger

def print_header(text: str):
    """Imprime um cabeçalho formatado."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*50}")
    print(f"{text}")
    print(f"{'='*50}{Colors.END}\n")

def print_section(text: str):
    """Imprime uma seção formatada."""
    print(f"\n{Colors.BLUE}{Colors.BOLD}▶ {text}{Colors.END}")

def print_success(text: str):
    """Imprime uma mensagem de sucesso formatada."""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")

def print_warning(text: str):
    """Imprime uma mensagem de aviso formatada."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")

def print_error(text: str):
    """Imprime uma mensagem de erro formatada."""
    print(f"{Colors.RED}✗ {text}{Colors.END}")

def main():
    """Função principal que orquestra todo o processo."""
    logger = setup_logging()
    print_header(f'Início da execução: {datetime.datetime.now():%d/%m/%Y às %H:%M:%S}')
    start_time: datetime = datetime.datetime.now()
    
    # Carrega variáveis de ambiente
    print_section("Carregando variáveis de ambiente...")
    load_dotenv('.env.local')
    print_success("Variáveis de ambiente carregadas com sucesso")
    
    # Exibe os arquivos ignorados
    print_section("Arquivos auxiliares que serão ignorados no download:")
    file_descriptions = {}
    
    # Tenta obter as descrições do arquivo config.py
    try:
        with open("config.py", "r", encoding="utf-8") as config_file:
            for line in config_file:
                for ignored_file in IGNORED_FILES:
                    if f"'{ignored_file}'," in line and "#" in line:
                        file_descriptions[ignored_file] = line.split('#')[1].strip()
    except Exception as e:
        logger.warning(f"Não foi possível ler as descrições dos arquivos ignorados: {str(e)}")
    
    # Exibe os arquivos ignorados com suas descrições
    for ignored_file in IGNORED_FILES:
        if ignored_file in file_descriptions:
            print_warning(f"- {ignored_file} ({file_descriptions[ignored_file]})")
        else:
            print_warning(f"- {ignored_file}")
    
    # Configurações de diretórios
    URL: str = os.getenv('URL_ORIGIN')
    PATH_ZIP: str = os.getenv('PATH_ZIP')
    PATH_UNZIP: str = os.getenv('PATH_UNZIP')
    PATH_PARQUET: str = os.getenv('PATH_PARQUET')
    FILE_DB_PARQUET: str = os.getenv('FILE_DB_PARQUET')
    PATH_REMOTE_PARQUET: str = os.getenv('PATH_REMOTE_PARQUET')
    
    # Cria diretórios necessários
    print_section("Criando diretórios necessários...")
    list_folders: list = [PATH_ZIP, PATH_UNZIP, PATH_PARQUET]
    for folder in list_folders:
        check_basic_folders(folder)
    print_success("Diretórios criados com sucesso")

    # Configuração do Dask
    print_section("Iniciando configuração do Dask...")
    freeze_support()
    cluster: LocalCluster = LocalCluster(
        n_workers=config.dask.n_workers,
        threads_per_worker=config.dask.threads_per_worker,
        memory_limit=config.dask.memory_limit,
        dashboard_address=config.dask.dashboard_address
    )
    client: Client = Client(cluster)
    print_success(f"Cliente Dask inicializado com sucesso: {client}")

    # Obtém a URL mais recente dos dados
    print_section("Buscando URL mais recente dos dados...")
    soup: BeautifulSoup = BeautifulSoup(requests.get(URL).text, 'html.parser')
    list_folders = []
    for element in soup.find_all('a'):
        if '-' in element.get('href'):
            list_folders.append(element.get('href'))
    URL += max(list_folders)
    soup: BeautifulSoup = BeautifulSoup(requests.get(URL).text, 'html.parser')
    print_success(f"URL mais recente encontrada: {URL}")

    # Processa os dados
    is_create_db_parquet: bool = False
    yyyymm: str = datetime.date.today().strftime('%Y%m')

    # Processa cada tipo de dado
    print_header("Iniciando processamento dos dados...")

    print_section("Processando dados de EMPRESAS...")
    if process_empresa(soup, URL, PATH_ZIP, PATH_UNZIP, PATH_PARQUET):
        is_create_db_parquet = True
        print_success("Dados de EMPRESAS processados com sucesso")
    else:
        print_warning("Nenhum dado novo de EMPRESAS para processar")

    print_section("Processando dados de ESTABELECIMENTOS...")
    if process_estabelecimento(soup, URL, PATH_ZIP, PATH_UNZIP, PATH_PARQUET):
        is_create_db_parquet = True
        print_success("Dados de ESTABELECIMENTOS processados com sucesso")
    else:
        print_warning("Nenhum dado novo de ESTABELECIMENTOS para processar")

    print_section("Processando dados do SIMPLES NACIONAL...")
    if process_simples(soup, URL, PATH_ZIP, PATH_UNZIP, PATH_PARQUET):
        is_create_db_parquet = True
        print_success("Dados do SIMPLES NACIONAL processados com sucesso")
    else:
        print_warning("Nenhum dado novo do SIMPLES NACIONAL para processar")

    print_section("Processando dados de SÓCIOS...")
    if process_socio(soup, URL, PATH_ZIP, PATH_UNZIP, PATH_PARQUET):
        is_create_db_parquet = True
        print_success("Dados de SÓCIOS processados com sucesso")
    else:
        print_warning("Nenhum dado novo de SÓCIOS para processar")

    # Cria o banco de dados se necessário
    if is_create_db_parquet:
        print_header("Criando banco de dados DuckDB...")
        create_duckdb_file(PATH_PARQUET, FILE_DB_PARQUET, PATH_REMOTE_PARQUET, yyyymm)
        print_success("Banco de dados DuckDB criado com sucesso")
    else:
        print_warning("Nenhum dado novo para criar banco de dados")

    print_header(f"Tempo total de execução: {str(datetime.datetime.now() - start_time)}")
    
    print_section("Encerrando cliente Dask...")
    client.shutdown()
    print_success("Processo finalizado com sucesso!")

if __name__ == '__main__':
    main() 