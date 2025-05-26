"""
Exemplos de uso:

1. Execução padrão (Baixa todos os tipos na pasta mais recente, processa todos com Polars, salva em subpasta com nome da data baixada):
   python main.py

2. Baixa e processa apenas Empresas e Sócios com Polars (salva em subpasta com nome da data baixada):
   python main.py --tipos empresas socios

3. Baixa e processa todos os tipos (salva em subpasta com nome da data baixada):
   python main.py

4. Baixa e processa apenas Estabelecimentos usando Polars (salva em subpasta com nome da data baixada):
   python main.py --tipos estabelecimentos --engine polars

5. Pular o download e processar todos os tipos da pasta ZIP '../dados-abertos-zip/2023-05', salvando Parquet na subpasta 'meu_processamento_manual' (dentro de PATH_PARQUET):
   python main.py --step process --source-zip-folder ../dados-abertos-zip/2023-05 --output-subfolder meu_processamento_manual

6. Pular o download, processar apenas Simples e Sócios da pasta ZIP 'D:/MeusDownloads/CNPJ_ZIPs/2023-01', salvando Parquet na subpasta 'simples_socios' (dentro de PATH_PARQUET):
   python main.py --step process --source-zip-folder "D:/MeusDownloads/CNPJ_ZIPs/2023-01" --output-subfolder simples_socios --tipos simples socios

7. Baixa e processa apenas Empresas (salva em subpasta com nome da data baixada):
   python main.py --tipos empresas

8. Baixa e processa apenas Empresas usando Polars, salvando na subpasta 'apenas_empresas_polars' (dentro de PATH_PARQUET):
   python main.py --tipos empresas --engine polars --output-subfolder apenas_empresas_polars

9. Como o exemplo 8, mas também cria o subconjunto 'empresa_privada' no diretório de saída:
   python main.py --tipos empresas --engine polars --output-subfolder apenas_empresas_polars --criar-empresa-privada

10. Pular download E processamento, criando apenas o arquivo DuckDB a partir dos Parquets existentes na subpasta 'processamento_anterior' (dentro de PATH_PARQUET):
    python main.py --step database --output-subfolder processamento_anterior

11. Pular download, processar, e depois criar o DuckDB, usando a pasta de origem 'meus_zips/2023-05' e salvando na subpasta 'resultado':
    python main.py --step process --source-zip-folder meus_zips/2023-05 --output-subfolder resultado

12. Processar apenas estabelecimentos com Polars, criando também um subset para São Paulo (SP) na saída 'parquet/process_sp/estabelecimentos_sp':
    python main.py --tipos estabelecimentos --engine polars --output-subfolder process_sp --criar-subset-uf SP

13. Baixar e processar dados de uma pasta remota específica (2023-05) em vez da pasta mais recente:
    python main.py --remote-folder 2023-05

14. Baixar arquivos de TODOS os diretórios remotos disponíveis (salvando em subpastas separadas):
    python main.py --all-folders --step download

15. Processar dados de uma pasta baixada anteriormente (aponta diretamente para a subpasta com arquivos):
    python main.py --step process --source-zip-folder pasta_zips/2023-05 --output-subfolder processados_2023_05

16. Baixar arquivos forçando download mesmo que já existam localmente ou no cache:
    python main.py --remote-folder 2023-05 --force-download

17. Processar todas as pastas no formato AAAA-MM encontradas dentro de PATH_ZIP (útil após download com --all-folders):
    python main.py --step process --process-all-folders --output-subfolder processados_completos

NOTA: O download sempre salvará os arquivos em uma subpasta com o nome da pasta remota.
      Exemplo: se --remote-folder=2023-05, os arquivos serão salvos em PATH_ZIP/2023-05/.
      Ao usar --source-zip-folder, aponte diretamente para o diretório que contém os arquivos ZIP.
"""
import argparse
import asyncio
import datetime
import logging
import os
from multiprocessing import freeze_support
import re
import time
import socket
import requests

import aiohttp
from dotenv import load_dotenv
from rich.logging import RichHandler

from src.async_downloader import (
    download_multiple_files, 
    get_latest_month_zip_urls, 
    get_remote_folders, 
    get_latest_remote_folder,
    _filter_urls_by_type
)
from src.config import config
from src.database import create_duckdb_file
from src.process.empresa import process_empresa_files
from src.process.estabelecimento import process_estabelecimento_files
from src.process.simples import process_simples_files
from src.process.socio import process_socio_files
from src.utils import check_basic_folders
from src.utils.time_utils import format_elapsed_time

# Configurar logger global
logger = logging.getLogger(__name__)

def check_internet_connection() -> bool:
    """
    Verifica se há conexão com a internet.
    
    Returns:
        bool: True se houver conexão, False caso contrário
    """
    try:
        # Tenta fazer uma requisição para um servidor confiável
        requests.get("http://www.google.com", timeout=5)
        return True
    except requests.RequestException:
        try:
            # Tenta resolver um domínio conhecido
            socket.create_connection(("8.8.8.8", 53), timeout=5)
            return True
        except OSError:
            return False

def check_disk_space() -> bool:
    """
    Verifica se há espaço suficiente em disco.
    
    Returns:
        bool: True se houver espaço suficiente, False caso contrário
    """
    try:
        # Obtém o diretório de destino
        path_zip = os.getenv('PATH_ZIP')
        if not path_zip:
            logger.error("PATH_ZIP não definido no arquivo .env")
            return False
            
        # Obtém o espaço livre em bytes usando ctypes para Windows
        import ctypes
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(
            ctypes.c_wchar_p(path_zip), None, None, ctypes.pointer(free_bytes)
        )
        
        # Define um limite mínimo de 10GB
        min_space = 10 * 1024 * 1024 * 1024  # 10GB em bytes
        
        if free_bytes.value < min_space:
            logger.warning(f"Espaço livre insuficiente: {free_bytes.value / (1024*1024*1024):.2f}GB")
            return False
            
        return True
    except Exception as e:
        logger.error(f"Erro ao verificar espaço em disco: {e}")
        return False

def setup_logging(log_level_str: str):
    """Configura o sistema de logging com base no nível fornecido."""
    if not os.path.exists('logs'):
        os.makedirs('logs')

    log_filename = f'logs/cnpj_process_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Converte a string do argumento para o nível de log correspondente
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)

    # Configuração do logger raiz para capturar tudo
    root_logger = logging.getLogger()
    # root_logger.setLevel(logging.INFO) # Nível fixo removido
    root_logger.setLevel(log_level) # Define o nível dinamicamente

    # Limpa handlers existentes para evitar duplicação em re-execuções (ex: testes)
    # CUIDADO: Isso pode remover handlers adicionados por outras bibliotecas.
    # Alternativa: verificar se handlers específicos já existem antes de adicionar.
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        print("[setup_logging] Handlers de log anteriores removidos.") # Log temporário para depuração

    # Handler para arquivo (sem cores)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(file_handler)

    # Handler para console (com RichHandler)
    console_handler = RichHandler(rich_tracebacks=True)
    root_logger.addHandler(console_handler)

    logger_instance = logging.getLogger(__name__)
    logger_instance.info(f"Nível de log configurado para: {logging.getLevelName(log_level)}")
    return logger_instance


def print_header(text: str):
    """Imprime um cabeçalho formatado."""
    print(f"\n{'=' * 50}")
    print(f"{text}")
    print(f"{'=' * 50}\n")


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


async def run_download_process(tipos_desejados: list[str] | None = None, remote_folder: str | None = None, all_folders: bool = False):
    """Executa todo o processo de download de forma assíncrona.
    
    Args:
        tipos_desejados: Lista de tipos de arquivos para baixar (ex: ['Empresas', 'Estabelecimentos'])
        remote_folder: Nome da pasta remota específica para baixar (ex: '2024-01')
        all_folders: Se True, baixa de todas as pastas remotas disponíveis
    """
    try:
        # Se nenhum tipo foi especificado, usar a lista padrão
        if tipos_desejados is None:
            tipos_desejados = ['Empresas', 'Estabelecimentos', 'Simples', 'Socios']
            logger.info("Nenhum tipo especificado. Usando tipos padrão: Empresas, Estabelecimentos, Simples, Socios")

        # Obter variáveis de ambiente
        PATH_ZIP = os.getenv('PATH_ZIP')
        PATH_UNZIP = os.getenv('PATH_UNZIP')
        PATH_PARQUET = os.getenv('PATH_PARQUET')

        if not all([PATH_ZIP, PATH_UNZIP, PATH_PARQUET]):
            logger.error("Variáveis de ambiente PATH_ZIP, PATH_UNZIP ou PATH_PARQUET não definidas")
            return False, ""

        # Verificar conexão com internet
        if not check_internet_connection():
            logger.error("Sem conexão com a internet. Verifique sua conexão e tente novamente.")
            return False, ""

        # Verificar espaço em disco
        if not check_disk_space():
            logger.error("Espaço em disco insuficiente. Libere espaço e tente novamente.")
            return False, ""

        # Criar diretórios básicos
        check_basic_folders([PATH_ZIP, PATH_UNZIP, PATH_PARQUET])

        # Obter URLs base
        base_url = os.getenv('BASE_URL')
        if not base_url:
            logger.error("BASE_URL não definida no arquivo .env")
            return False, ""

        # Obter pasta de download
        download_folder = os.getenv('PATH_ZIP')
        if not download_folder:
            logger.error("PATH_ZIP não definido no arquivo .env")
            return False, ""

        # Lista para armazenar resultados
        downloaded_files_count = 0
        failed_downloads_count = 0
        all_successful = True

        if all_folders:
            # Modo: Baixar de todas as pastas
            logger.info("Modo: Baixar de todas as pastas disponíveis")
            
            # Obter lista de pastas remotas
            remote_folders = await get_remote_folders(base_url)
            if not remote_folders:
                logger.error("Não foi possível obter a lista de pastas remotas")
                return False, ""

            logger.info(f"Encontradas {len(remote_folders)} pastas remotas")
            
            # Processar cada pasta remota
            for folder_name in remote_folders:
                logger.info(f"Processando pasta remota: {folder_name}")
                
                # Atualizar pasta remota no cache
                if config.cache.enabled:
                    config.cache.set_remote_folder(folder_name)
                    logger.info(f"Cache configurado para pasta remota: {folder_name}")
                
                # Criar subdiretório para esta pasta remota
                folder_download_path = os.path.join(download_folder, folder_name)
                os.makedirs(folder_download_path, exist_ok=True)
                logger.info(f"Criando subdiretório para a pasta remota: {folder_download_path}")
                
                # Obter URLs dos arquivos
                zip_urls, folder_name = get_latest_month_zip_urls(base_url, folder_name)
                if not zip_urls:
                    logger.warning(f"Nenhuma URL relevante para download encontrada em {folder_name}.")
                    continue
                
                # Filtrar URLs por tipos desejados
                if tipos_desejados:
                    zip_urls, ignored = _filter_urls_by_type(zip_urls, tuple(tipos_desejados))
                    if not zip_urls:
                        logger.warning(f"Nenhuma URL relevante para download encontrada após filtrar por tipos em {folder_name}.")
                        continue
                    logger.info(f"Filtrados {ignored} URLs não desejadas. Restaram {len(zip_urls)} URLs para download.")
                
                # Log adicional para verificação de cache
                logger.info(f"Iniciando download para {folder_name} - Sistema de cache está {'ativado' if config.cache.enabled else 'desativado'}")
                
                # Verificar se o diretório do cache existe
                if config.cache.enabled and not os.path.exists(config.cache.cache_dir):
                    os.makedirs(config.cache.cache_dir, exist_ok=True)
                    logger.info(f"Diretório de cache criado: {config.cache.cache_dir}")
                
                # Baixar os arquivos deste diretório
                try:
                    max_concurrent_downloads = config.n_workers
                    downloaded, failed = await download_multiple_files(
                        zip_urls,
                        folder_download_path,
                        max_concurrent=max_concurrent_downloads
                    )
                    
                    downloaded_files_count += len(downloaded)
                    failed_downloads_count += len(failed)
                    
                    if failed:
                        logger.warning(f"{len(failed)} downloads falharam em {folder_name}.")
                        all_successful = False
                except Exception as e:
                    logger.error(f"Erro durante downloads em {folder_name}: {e}")
                    all_successful = False
            
            # Resumo final
            logger.info(f"Download de múltiplos diretórios concluído.")
            
        else:
            # Modo: Baixar de uma pasta específica ou a mais recente
            if remote_folder:
                # Usar pasta remota especificada
                latest_folder = remote_folder
                logger.info(f"Usando pasta remota especificada: {latest_folder}")
            else:
                # Obter pasta mais recente
                latest_folder = await get_latest_remote_folder(base_url)
                if not latest_folder:
                    logger.error("Não foi possível determinar a pasta remota mais recente")
                    return False, ""
                logger.info(f"Pasta remota mais recente: {latest_folder}")

            # Atualizar pasta remota no cache
            if config.cache.enabled:
                config.cache.set_remote_folder(latest_folder)
                logger.info(f"Cache configurado para pasta remota: {latest_folder}")

            # Criar subdiretório para esta pasta remota
            folder_download_path = os.path.join(download_folder, latest_folder)
            os.makedirs(folder_download_path, exist_ok=True)
            logger.info(f"Criando subdiretório para a pasta remota: {folder_download_path}")
            
            # Obter URLs dos arquivos
            zip_urls, folder_name = get_latest_month_zip_urls(base_url, latest_folder)
            if not zip_urls:
                logger.warning(f"Nenhuma URL relevante para download encontrada em {latest_folder}.")
                return False, ""
            
            # Filtrar URLs por tipos desejados
            if tipos_desejados:
                zip_urls, ignored = _filter_urls_by_type(zip_urls, tuple(tipos_desejados))
                if not zip_urls:
                    logger.warning(f"Nenhuma URL relevante para download encontrada após filtrar por tipos em {latest_folder}.")
                    return False, ""
                logger.info(f"Filtrados {ignored} URLs não desejadas. Restaram {len(zip_urls)} URLs para download.")
            
            # Log para depuração
            logger.info(f"Iniciando download de {len(zip_urls)} arquivos relevantes para {folder_download_path}...")
            logger.info(f"Sistema de cache está {'ativado' if config.cache.enabled else 'desativado'}")
            
            # Verificar se o diretório do cache existe
            if config.cache.enabled and not os.path.exists(config.cache.cache_dir):
                os.makedirs(config.cache.cache_dir, exist_ok=True)
                logger.info(f"Diretório de cache criado: {config.cache.cache_dir}")

            # Baixar os arquivos
            try:
                max_concurrent_downloads = config.n_workers
                downloaded, failed = await download_multiple_files(
                    zip_urls,
                    folder_download_path,
                    max_concurrent=max_concurrent_downloads
                )
                
                downloaded_files_count = len(downloaded)
                failed_downloads_count = len(failed)
                
                if failed:
                    logger.warning(f"{len(failed)} downloads falharam.")
                    all_successful = False
                    
            except aiohttp.ClientError as e:
                logger.error(f"Erro de conexão durante downloads: {e}")
                return False, ""
            except asyncio.TimeoutError as e:
                logger.error(f"Timeout durante downloads: {e}")
                return False, ""
            except Exception as e:
                logger.error(f"Erro inesperado durante downloads: {e}")
                return False, ""

            logger.info("Processo de download concluído.")

            if failed:
                logger.error(f"{len(failed)} downloads falharam. Verifique os logs acima.")
                if not downloaded:  # Se nenhum arquivo foi baixado com sucesso
                    return False, ""
                logger.warning("Continuando processamento com os arquivos baixados com sucesso.")

        # Resumo final
        logger.info("=" * 50)
        logger.info("RESUMO DO PROCESSO DE DOWNLOAD:")
        logger.info("=" * 50)
        logger.info(f"Total de arquivos baixados com sucesso: {downloaded_files_count}")
        logger.info(f"Total de downloads com falha: {failed_downloads_count}")
        logger.info(f"Processo {'completo' if all_successful else 'com falhas'}")
        logger.info("=" * 50)

        return all_successful, latest_folder if not all_folders else ""

    except Exception as e:
        logger.error(f"Erro durante o processo de download: {str(e)}")
        return False, ""


def process_folder(source_zip_path, unzip_path, output_parquet_path, 
                 tipos_list, engine, criar_empresa_privada, criar_subset_uf,
                 tipos_a_processar) -> dict:
    """Processa os arquivos da pasta, usando o engine especificado.
    
    Args:
        source_zip_path: Caminho para os ZIPs
        unzip_path: Caminho para extrair arquivos
        output_parquet_path: Caminho para salvar parquets
        tipos_list: Lista de tipos a serem processados
        engine: Engine a ser usado (polars)
        criar_empresa_privada: Flag para criar subset empresas privadas
        criar_subset_uf: Flag para criar subset por UF
        tipos_a_processar: Lista de tipos a processar
        
    Returns:
        dict: Dicionário com o status de cada tipo e tempo de processamento
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Engine selecionado: {engine}")
    logger.info(f"Caminho para salvar parquets: {output_parquet_path}")
    
    # Extrair a pasta remota do caminho dos ZIPs (para usar na estrutura de diretórios)
    remote_folder = os.path.basename(os.path.normpath(source_zip_path))
    logger.info(f"Pasta remota extraída do caminho: {remote_folder}")
    
    all_ok = True
    processing_results = {}
    total_start_time = time.time()
    
    # Processa Empresas
    if 'empresas' in tipos_a_processar:
        if 'Empresas' in tipos_list or 'empresas' in tipos_list:
            start_time = time.time()
            # Agora só usamos polars
            empresas_ok = process_empresa_files(
                source_zip_path, unzip_path, output_parquet_path, criar_empresa_privada
            )
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            processing_results['empresas'] = {
                'success': empresas_ok,
                'time': elapsed_time
            }
                
            if not empresas_ok:
                logger.error("Erro no processamento de empresas.")
                all_ok = False
    
    # Processa Estabelecimentos
    if 'estabelecimentos' in tipos_a_processar:
        if 'Estabelecimentos' in tipos_list or 'estabelecimentos' in tipos_list:
            start_time = time.time()
            uf_subset = None
            
            # Se criar_subset_uf foi especificado, extrai a sigla da UF
            if criar_subset_uf:
                if isinstance(criar_subset_uf, str) and len(criar_subset_uf) == 2:
                    uf_subset = criar_subset_uf.upper()
                    logger.info(f"Criando subset de estabelecimentos para UF: {uf_subset}")
                else:
                    logger.warning(f"Valor inválido para subset UF: {criar_subset_uf}. Ignorando.")
            
            estab_ok = process_estabelecimento_files(
                source_zip_path, unzip_path, output_parquet_path, uf_subset
            )
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            processing_results['estabelecimentos'] = {
                'success': estab_ok,
                'time': elapsed_time
            }
            
            if not estab_ok:
                logger.error("Erro no processamento de estabelecimentos.")
                all_ok = False
    
    # Processa Simples Nacional
    if 'simples' in tipos_a_processar:
        if 'Simples' in tipos_list or 'simples' in tipos_list:
            start_time = time.time()
            simples_ok = process_simples_files(
                source_zip_path, unzip_path, output_parquet_path
            )
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            processing_results['simples'] = {
                'success': simples_ok,
                'time': elapsed_time
            }
            
            if not simples_ok:
                logger.error("Erro no processamento do simples nacional.")
                all_ok = False
    
    # Processa Sócios
    if 'socios' in tipos_a_processar:
        if 'Socios' in tipos_list or 'socios' in tipos_list:
            start_time = time.time()
            socios_ok = process_socio_files(
                source_zip_path, unzip_path, output_parquet_path
            )
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            processing_results['socios'] = {
                'success': socios_ok,
                'time': elapsed_time
            }
            
            if not socios_ok:
                logger.error("Erro no processamento de sócios.")
                all_ok = False
    
    # Calcular tempo total
    total_elapsed_time = time.time() - total_start_time
    processing_results['total_time'] = total_elapsed_time
    processing_results['all_ok'] = all_ok
    
    # Logar resumo de processamento
    logger.info("=" * 50)
    logger.info("RESUMO DO PROCESSAMENTO:")
    logger.info("=" * 50)
    for tipo, resultado in processing_results.items():
        if tipo != 'total_time' and tipo != 'all_ok':
            status = "SUCESSO" if resultado['success'] else "FALHA"
            logger.info(f"{tipo.upper()}: {status} - Tempo: {format_elapsed_time(resultado['time'])}")
    logger.info("-" * 50)
    logger.info(f"TEMPO TOTAL DE PROCESSAMENTO: {format_elapsed_time(total_elapsed_time)}")
    logger.info(f"STATUS GERAL: {'SUCESSO' if all_ok else 'FALHA'}")
    logger.info("=" * 50)
    
    return processing_results


def main():
    """Função principal de execução."""
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Realizar download e processamento dos dados de CNPJ')
    parser.add_argument('--tipos', '-t', nargs='+', choices=['empresas', 'estabelecimentos', 'simples', 'socios'],
                        help='Tipos de dados a serem processados. Se não especificado, processa todos (relevante para steps \'process\' e \'all\').')
    parser.add_argument('--engine', '-e', choices=['polars'], default='polars',
                        help='Motor de processamento a ser utilizado (relevante para steps \'process\' e \'all\'). Padrão: polars')
    parser.add_argument('--source-zip-folder', '-z', type=str, default=None,
                        help='Caminho para o diretório contendo os arquivos ZIP ou suas subpastas. No modo "all", usa automaticamente a subpasta com nome da pasta remota dentro de PATH_ZIP.')
    parser.add_argument('--output-subfolder', '-o', type=str, default=None,
                        help='Nome da subpasta dentro de PATH_PARQUET onde os arquivos Parquet serão salvos ou lidos (Obrigatório para --step process e --step database).')
    parser.add_argument('--criar-empresa-privada', '-priv', action='store_true',
                        help='Se presente (com --step process ou --step all), cria um subconjunto Parquet adicional para empresas privadas.')
    parser.add_argument('--log-level', '-l', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO',
                        help='Define o nível mínimo de log a ser exibido/salvo. Padrão: INFO.')
    parser.add_argument(
        '--criar-subset-uf', '-uf',
        type=str,
        default=None,
        metavar='UF',
        help="Opcional (com --step process ou --step all). Cria um subconjunto Parquet adicional para estabelecimentos da UF especificada."
    )
    parser.add_argument(
        '--remote-folder', '-rf',
        type=str,
        default=None,
        metavar='PASTA',
        help="Opcional (com --step download ou --step all). Especifica a pasta remota para download no formato AAAA-MM (ex: 2023-01). Se não especificado, usa a pasta mais recente."
    )
    parser.add_argument(
        '--all-folders', '-af',
        action='store_true',
        help="Opcional (com --step download ou --step all). Quando presente, baixa os arquivos de TODOS os diretórios remotos disponíveis. Tem prioridade sobre --remote-folder."
    )
    parser.add_argument(
        '--force-download', '-fd',
        action='store_true',
        help="Força o download de todos os arquivos, mesmo que já existam localmente ou no cache."
    )
    parser.add_argument(
        '--process-all-folders', '-paf',
        action='store_true',
        help="Processa todas as pastas no formato AAAA-MM encontradas no diretório ZIP. Útil após download com --all-folders."
    )
    parser.add_argument(
        '--step', '-s',
        choices=['download', 'process', 'database', 'all'], 
        default='all',
        help="""Especifica qual(is) etapa(s) executar:
                        'download': Apenas baixa os arquivos ZIP mais recentes (ou da pasta especificada em --remote-folder, ou de todos os diretórios com --all-folders).
                        'process': Apenas processa ZIPs existentes (--source-zip-folder) para Parquet (--output-subfolder).
                        'database': Apenas cria o DuckDB a partir de Parquets existentes (--output-subfolder).
                        'all': Executa todas as etapas: download -> process -> database (padrão)."""
    )
    args = parser.parse_args()

    # Configurando o logging
    logger = setup_logging(args.log_level)

    # Carregar variáveis de ambiente do arquivo .env
    load_dotenv()
    print_section("Carregando variáveis de ambiente...")
    load_dotenv()
    print_success("Variáveis de ambiente carregadas com sucesso")

    PATH_ZIP = os.getenv('PATH_ZIP')
    PATH_UNZIP = os.getenv('PATH_UNZIP')
    PATH_PARQUET = os.getenv('PATH_PARQUET')
    FILE_DB_PARQUET = os.getenv('FILE_DB_PARQUET')
    PATH_REMOTE_PARQUET = os.getenv('PATH_REMOTE_PARQUET')
    
    # Garantir que PATH_UNZIP e PATH_PARQUET estão definidos
    if not PATH_UNZIP:
        PATH_UNZIP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados-unzip")
        os.environ['PATH_UNZIP'] = PATH_UNZIP
        logger.info(f"Variável PATH_UNZIP não encontrada no arquivo .env. Usando valor padrão: {PATH_UNZIP}")
    
    if not PATH_PARQUET:
        PATH_PARQUET = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados-parquet")
        os.environ['PATH_PARQUET'] = PATH_PARQUET
        logger.info(f"Variável PATH_PARQUET não encontrada no arquivo .env. Usando valor padrão: {PATH_PARQUET}")
    
    # Mostrar valores para debug
    logger.info(f"PATH_ZIP = {PATH_ZIP}")
    logger.info(f"PATH_UNZIP = {PATH_UNZIP}")
    logger.info(f"PATH_PARQUET = {PATH_PARQUET}")
    logger.info(f"FILE_DB_PARQUET = {FILE_DB_PARQUET}")
    logger.info(f"PATH_REMOTE_PARQUET = {PATH_REMOTE_PARQUET}")

    # Verificando pastas básicas
    check_basic_folders([PATH_ZIP, PATH_UNZIP, PATH_PARQUET])
    
    # Determinar o modo de processamento
    if args.step == 'process':
        print_header("Modo de Processamento")
        
        # Verificar se temos o source-zip-folder
        if args.source_zip_folder is not None:
            # Processa a pasta de origem especificada
            logger.info(f"Processando pasta de origem: {args.source_zip_folder}")
            source_folder_path = args.source_zip_folder
            
            if not os.path.exists(source_folder_path):
                logger.error(f"Pasta de origem especificada não existe: {source_folder_path}")
                print_error(f"Pasta {args.source_zip_folder} não encontrada")
                return
            
            # Determinar pasta de saída - usando o nome do diretório como subpasta
            if args.output_subfolder:
                output_subfolder = args.output_subfolder
            else:
                # Usa o nome da pasta de origem como nome da subpasta de saída
                output_subfolder = os.path.basename(os.path.normpath(source_folder_path))
                logger.info(f"Usando nome da pasta de origem como subpasta de saída: {output_subfolder}")
            
            output_parquet_path = os.path.join(PATH_PARQUET, output_subfolder)
            os.makedirs(output_parquet_path, exist_ok=True)
            
            logger.info(f"Processando arquivos de: {source_folder_path}")
            logger.info(f"Salvando Parquets em: {output_parquet_path}")
            
            # Definir tipos a processar (todos ou filtrados)
            if args.tipos:
                tipos_a_processar = args.tipos
            else:
                tipos_a_processar = ['empresas', 'estabelecimentos', 'simples', 'socios']
            
            # Iniciar processamento
            process_results = process_folder(
                source_folder_path, PATH_UNZIP, output_parquet_path, 
                args.tipos if args.tipos else ['empresas', 'estabelecimentos', 'simples', 'socios'],
                args.engine, args.criar_empresa_privada, args.criar_subset_uf,
                tipos_a_processar
            )
            
            if process_results['all_ok']:
                print_success(f"Processamento concluído com sucesso na pasta: {output_parquet_path}")
            else:
                print_warning("Alguns erros ocorreram durante o processamento. Verifique os logs.")
            
            # Se process-all-folders estiver habilitado, busca todas as pastas no formato AAAA-MM
            if args.process_all_folders:
                print_section("Processando todas as pastas no formato AAAA-MM")
                # Implementação para process_all_folders
                # ...
        else:
            print_error("É necessário especificar a pasta de origem dos ZIPs com --source-zip-folder")
            logger.error("Parâmetro --source-zip-folder é obrigatório para o step 'process'.")
            return
    
    # Resto do código do step 'download'
    elif args.step == 'download':
        print_header("Modo de Download")
        
        # Configurar para forçar o download se necessário
        if args.force_download:
            # Definir variável de ambiente para o módulo async_downloader
            os.environ['FORCE_DOWNLOAD'] = 'True'
            logger.info("Download forçado ativado: sobrescreverá arquivos existentes.")
        
        # Iniciar o download assíncrono
        tipos_desejados = args.tipos if args.tipos else None
        download_ok, latest_folder = asyncio.run(run_download_process(
            tipos_desejados=tipos_desejados,
            remote_folder=args.remote_folder,
            all_folders=args.all_folders
        ))
        
        if not download_ok:
            print_error("Falha no processo de download. Verifique os logs para mais detalhes.")
            return
        
        print_success(f"Download concluído com sucesso. Arquivos salvos em: {os.path.join(PATH_ZIP, latest_folder)}")
    
    # Código para o step 'database'
    elif args.step == 'database':
        print_header("Modo de Criação de Banco de Dados")
        
        # Verificar se temos a subpasta de saída especificada
        if args.output_subfolder is None:
            logger.error("Parâmetro --output-subfolder é obrigatório para o step 'database'.")
            print_error("Especifique a subpasta dos Parquets com --output-subfolder")
            return
        
        # Caminho completo para a pasta de parquets
        parquet_folder = os.path.join(PATH_PARQUET, args.output_subfolder)
        if not os.path.exists(parquet_folder):
            logger.error(f"Pasta de Parquets não encontrada: {parquet_folder}")
            print_error(f"Pasta {parquet_folder} não existe. Execute o processamento primeiro.")
            return
        
        # Criar o arquivo DuckDB
        try:
            db_file = os.path.join(parquet_folder, FILE_DB_PARQUET)
            logger.info(f"Criando arquivo DuckDB em: {db_file}")
            create_duckdb_file(parquet_folder, db_file, PATH_REMOTE_PARQUET)
            print_success(f"Banco de dados DuckDB criado com sucesso em: {db_file}")
        except Exception as e:
            logger.exception(f"Erro ao criar banco de dados: {e}")
            print_error(f"Falha ao criar banco de dados: {str(e)}")
            return
    
    # Código para o step 'all' (executa todos os passos em sequência)
    elif args.step == 'all':
        print_header("Modo Completo: Download -> Processamento -> Banco de Dados")
        
        # 1. Download
        print_section("Etapa 1: Download dos arquivos")
        download_start_time = time.time()
        
        # Configurar para forçar o download se necessário
        if args.force_download:
            os.environ['FORCE_DOWNLOAD'] = 'True'
            logger.info("Download forçado ativado: sobrescreverá arquivos existentes.")
        
        # Iniciar o download assíncrono
        tipos_desejados = args.tipos if args.tipos else None
        download_ok, latest_folder = asyncio.run(run_download_process(
            tipos_desejados=tipos_desejados,
            remote_folder=args.remote_folder,
            all_folders=args.all_folders
        ))
        
        download_time = time.time() - download_start_time
        logger.info("=" * 50)
        logger.info(f"Tempo de download: {format_elapsed_time(download_time)}")
        
        if not download_ok:
            print_error("Falha no processo de download. Verifique os logs para mais detalhes.")
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return
        
        print_success(f"Download concluído. Arquivos salvos em: {os.path.join(PATH_ZIP, latest_folder)}")
        
        # 2. Processamento
        print_section("Etapa 2: Processamento dos arquivos")
        process_start_time = time.time()
        
        # Definir o diretório de origem para ZIPs
        source_zip_path = os.path.join(PATH_ZIP, latest_folder)
        
        # Definir o diretório de saída para Parquets - USANDO O MESMO NOME DA PASTA REMOTA
        output_subfolder = args.output_subfolder if args.output_subfolder else latest_folder
        output_parquet_path = os.path.join(PATH_PARQUET, output_subfolder)
        os.makedirs(output_parquet_path, exist_ok=True)
        
        logger.info(f"Processando arquivos de: {source_zip_path}")
        logger.info(f"Salvando Parquets em: {output_parquet_path}")
        
        # Lista de tipos a processar (todos ou filtrados)
        tipos_a_processar = args.tipos if args.tipos else ['empresas', 'estabelecimentos', 'simples', 'socios']
        
        # Iniciar processamento
        process_results = process_folder(
            source_zip_path, PATH_UNZIP, output_parquet_path,
            args.tipos if args.tipos else ['empresas', 'estabelecimentos', 'simples', 'socios'],
            args.engine, args.criar_empresa_privada, args.criar_subset_uf,
            tipos_a_processar
        )
        
        process_time = time.time() - process_start_time
        logger.info("=" * 50)
        logger.info(f"Tempo de processamento: {format_elapsed_time(process_time)}")
        
        if not process_results['all_ok']:
            print_warning("Alguns erros ocorreram durante o processamento. O banco de dados NÃO será criado.")
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return
        else:
            print_success("Processamento concluído com sucesso.")
        
        # 3. Criação do banco de dados
        print_section("Etapa 3: Criação do banco de dados DuckDB")
        db_start_time = time.time()
        
        try:
            db_file = os.path.join(output_parquet_path, FILE_DB_PARQUET)
            logger.info(f"Criando arquivo DuckDB em: {db_file}")
            create_duckdb_file(output_parquet_path, db_file, PATH_REMOTE_PARQUET)
            db_time = time.time() - db_start_time
            logger.info("=" * 50)
            logger.info(f"Tempo de processamento do banco: {format_elapsed_time(db_time)}")
            print_success(f"Banco de dados DuckDB criado com sucesso em: {db_file}")
        except Exception as e:
            logger.exception(f"Erro ao criar banco de dados: {e}")
            print_error(f"Falha ao criar banco de dados: {str(e)}")
            db_time = time.time() - db_start_time
            logger.info(f"Tempo de criação do banco (falhou): {format_elapsed_time(db_time)}")
    
    total_time = time.time() - start_time
    
    # Resumo final
    print_header("Processamento concluído")
    logger.info("=" * 50)
    logger.info("RESUMO FINAL DE EXECUÇÃO:")
    logger.info("=" * 50)
    
    if args.step == 'all':
        logger.info(f"Download: {format_elapsed_time(download_time)}")
        logger.info(f"Processamento: {format_elapsed_time(process_time)}")
        logger.info(f"Criação do banco: {format_elapsed_time(db_time)}")
    
    logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
    logger.info("=" * 50)
    logger.info("Execução concluída.")
    return


if __name__ == '__main__':
    main()
