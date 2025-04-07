import datetime
import logging
import os
import asyncio
from multiprocessing import freeze_support
from dask.distributed import Client, LocalCluster
from dotenv import load_dotenv
from rich.logging import RichHandler
from src.config import config
from src.utils import check_basic_folders, check_internet_connection
from src.process.empresa import process_empresa
from src.process.estabelecimento import process_estabelecimento
from src.process.simples import process_simples
from src.process.socio import process_socio
from src.database import create_duckdb_file
from src.async_downloader import get_latest_month_zip_urls, download_multiple_files, _filter_urls_by_type

def setup_logging():
    """Configura o sistema de logging."""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    log_filename = f'logs/cnpj_process_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Configuração do logger raiz para capturar tudo
    # (Necessário para que os logs de async_downloader sejam pegos)
    root_logger = logging.getLogger() 
    root_logger.setLevel(logging.INFO) 
    
    # Handler para arquivo (sem cores)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(file_handler)
    
    # Handler para console (com RichHandler)
    console_handler = RichHandler(rich_tracebacks=True)
    # O formatter do RichHandler é configurado por ele mesmo, não precisa setFormatter
    root_logger.addHandler(console_handler)
    
    # Retorna um logger específico para main, se desejar, mas a configuração é global
    return logging.getLogger(__name__)

def print_header(text: str):
    """Imprime um cabeçalho formatado."""
    print(f"\n{'='*50}")
    print(f"{text}")
    print(f"{'='*50}\n")

def print_section(text: str):
    """Imprime uma seção formatada."""
    print(f"\n▶ {text}")

def print_success(text: str):
    """Imprime uma mensagem de sucesso formatada."""
    print(f"✓ {text}")

def print_warning(text: str):
    """Imprime uma mensagem de aviso formatada."""
    print(f"⚠ {text}")

def print_error(text: str):
    """Imprime uma mensagem de erro formatada."""
    print(f"✗ {text}")

async def run_download_process():
    """Executa todo o processo de download de forma assíncrona."""
    logger = logging.getLogger(__name__) # Pega o logger configurado
    logger.info("Iniciando processo de download centralizado...")
    base_url = os.getenv('URL_ORIGIN')
    download_folder = os.getenv('PATH_ZIP')

    if not base_url or not download_folder:
        logger.error("Variáveis de ambiente URL_ORIGIN ou PATH_ZIP não definidas.")
        return False # Indica falha no download

    # 1. Buscar URLs mais recentes
    all_zip_urls = get_latest_month_zip_urls(base_url)
    if not all_zip_urls:
        logger.warning("Nenhuma URL .zip encontrada na origem.")
        return False # Indica falha no download

    # 2. Filtrar URLs desejadas
    tipos_desejados = ("Empresas", "Estabelecimentos", "Simples", "Socios")
    zip_urls_to_download, ignored_count = _filter_urls_by_type(all_zip_urls, tipos_desejados)
    logger.info(f"{ignored_count} arquivos ignorados com base nos tipos não desejados.")

    if not zip_urls_to_download:
         logger.warning(f"Nenhuma URL relevante para download encontrada após filtrar por tipos.")
         return False # Indica falha no download

    logger.info(f"Iniciando download de {len(zip_urls_to_download)} arquivos relevantes para {download_folder}...")

    # 3. Baixar os arquivos
    max_concurrent_downloads = config.dask.n_workers # Usando config para concorrência
    downloaded, failed = await download_multiple_files(
        zip_urls_to_download,
        download_folder,
        max_concurrent=max_concurrent_downloads
    )

    logger.info("Processo de download concluído.")
    if failed:
        logger.error(f"{len(failed)} downloads falharam. Verifique os logs acima.")
        # Decide se quer continuar o processamento mesmo com falhas
        # return False # Descomente para parar se houver falhas
    
    # Retorna True se pelo menos um arquivo foi baixado/pulado com sucesso (ou se não havia nada para baixar)
    # Ou pode retornar a lista de `downloaded` se as próximas etapas precisarem dela
    return True 

def main():
    """Função principal que orquestra todo o processo."""
    logger = setup_logging()
    print_header(f'Início da execução: {datetime.datetime.now():%d/%m/%Y às %H:%M:%S}')
    start_time: datetime = datetime.datetime.now()
    
    # Carrega variáveis de ambiente
    print_section("Carregando variáveis de ambiente...")
    load_dotenv('.env.local')
    print_success("Variáveis de ambiente carregadas com sucesso")
    
    # Configurações de diretórios
    PATH_ZIP: str = os.getenv('PATH_ZIP')
    PATH_UNZIP: str = os.getenv('PATH_UNZIP')
    PATH_PARQUET: str = os.getenv('PATH_PARQUET')
    FILE_DB_PARQUET: str = os.getenv('FILE_DB_PARQUET')
    PATH_REMOTE_PARQUET: str = os.getenv('PATH_REMOTE_PARQUET')
    
    # Cria diretórios necessários
    print_section("Criando diretórios necessários...")
    list_folders: list = [PATH_ZIP, PATH_UNZIP, PATH_PARQUET]
    # Adiciona o diretório de cache para garantir que seja criado no início, se necessário
    # Embora config.py e cache.py também tentem criar
    list_folders.append(config.cache.cache_dir) 
    for folder in list_folders:
        if folder: # Verifica se a variável de ambiente não está vazia
             check_basic_folders(folder)
    print_success("Diretórios criados com sucesso")

    # --- Etapa de Download Centralizada ---
    print_header("Iniciando Etapa de Download...")
    download_successful = asyncio.run(run_download_process())

    if not download_successful:
        print_error("A etapa de download falhou ou não encontrou arquivos. Abortando processamento subsequente.")
        # Opcional: encerrar Dask se já foi iniciado, ou sair
        # client.shutdown()
        return # Sai da função main
    
    print_success("Etapa de Download concluída.")
    # -----------------------------------------

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

    # Processa os dados
    is_create_db_parquet: bool = False
    yyyymm: str = datetime.date.today().strftime('%Y%m')

    # Processa cada tipo de dado
    print_header("Iniciando processamento dos dados...")

    print_section("Processando dados de EMPRESAS...")
    if process_empresa(PATH_ZIP, PATH_UNZIP, PATH_PARQUET):
        is_create_db_parquet = True
        print_success("Dados de EMPRESAS processados com sucesso")
    else:
        print_warning("Nenhum dado novo de EMPRESAS para processar")

    print_section("Processando dados de ESTABELECIMENTOS...")
    if process_estabelecimento(PATH_ZIP, PATH_UNZIP, PATH_PARQUET):
        is_create_db_parquet = True
        print_success("Dados de ESTABELECIMENTOS processados com sucesso")
    else:
        print_warning("Nenhum dado novo de ESTABELECIMENTOS para processar")

    print_section("Processando dados do SIMPLES NACIONAL...")
    if process_simples(PATH_ZIP, PATH_UNZIP, PATH_PARQUET):
        is_create_db_parquet = True
        print_success("Dados do SIMPLES NACIONAL processados com sucesso")
    else:
        print_warning("Nenhum dado novo do SIMPLES NACIONAL para processar")

    print_section("Processando dados de SÓCIOS...")
    if process_socio(PATH_ZIP, PATH_UNZIP, PATH_PARQUET):
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