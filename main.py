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

import aiohttp
from dotenv import load_dotenv
from rich.logging import RichHandler

from src.async_downloader import get_latest_month_zip_urls, download_multiple_files, _filter_urls_by_type
from src.config import config
from src.database import create_duckdb_file
from src.process.empresa import process_empresa_with_polars
from src.process.estabelecimento import process_estabelecimento_with_polars
from src.process.simples import process_simples_with_polars
from src.process.socio import process_socio_with_polars
from src.utils import check_basic_folders


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
        tipos_desejados: Lista de tipos de arquivos a serem baixados. Se None, baixa todos.
        remote_folder: Pasta remota específica a ser baixada no formato AAAA-MM. Se None, usa a mais recente.
        all_folders: Se True, baixa arquivos de todos os diretórios remotos disponíveis.

    Returns:
        tuple[bool, str]: (sucesso do download, pasta mais recente)
            - Se sucesso for False, a pasta mais recente será uma string vazia
            - Se all_folders=True, a segunda posição conterá "all_folders"
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando processo de download centralizado...")

    try:
        base_url = os.getenv('URL_ORIGIN')
        download_folder = os.getenv('PATH_ZIP')

        if not base_url or not download_folder:
            logger.error("Variáveis de ambiente URL_ORIGIN ou PATH_ZIP não definidas.")
            return False, ""

        if all_folders:
            # Modo especial: baixar de todos os diretórios disponíveis
            logger.info("Modo ALL_FOLDERS ativado: baixando arquivos de todos os diretórios disponíveis.")
            
            # 1. Primeiro, obtemos a lista de todos os diretórios disponíveis
            try:
                # Vamos reutilizar parte da função get_latest_month_zip_urls para obter os diretórios
                from src.async_downloader import _fetch_and_parse, _find_links
                
                base_soup = _fetch_and_parse(base_url)
                if not base_soup:
                    logger.error("Falha ao buscar diretórios remotos.")
                    return False, ""
                
                directory_links = _find_links(base_soup, base_url, ends_with=None)
                
                year_month_folders = []
                for dir_url in directory_links:
                    folder_name = dir_url.strip('/').split('/')[-1]
                    match = re.fullmatch(r'(\d{4})-(\d{2})', folder_name)
                    if match:
                        year_month_folders.append((folder_name, dir_url))
                        logger.debug(f"Encontrado diretório AAAA-MM: {folder_name} -> {dir_url}")
                
                if not year_month_folders:
                    logger.warning(f"Nenhum diretório no formato AAAA-MM encontrado em {base_url}")
                    return False, ""
                
                # Ordenamos os diretórios do mais recente para o mais antigo
                year_month_folders.sort(key=lambda x: x[0], reverse=True)
                
                logger.info(f"Encontrados {len(year_month_folders)} diretórios remotos para download.")
                
                # 2. Agora, vamos baixar de cada diretório
                all_successful = True
                downloaded_files_count = 0
                failed_downloads_count = 0
                
                for folder_name, folder_url in year_month_folders:
                    logger.info(f"Processando diretório: {folder_name}")
                    
                    # Criar subdiretório para esta pasta se não existir
                    folder_download_path = os.path.join(download_folder, folder_name)
                    os.makedirs(folder_download_path, exist_ok=True)
                    
                    # Obter URLs dos arquivos .zip neste diretório
                    folder_soup = _fetch_and_parse(folder_url)
                    if not folder_soup:
                        logger.warning(f"Falha ao buscar arquivos em {folder_url}. Pulando.")
                        all_successful = False
                        continue
                    
                    zip_urls = _find_links(folder_soup, folder_url, ends_with='.zip')
                    
                    if not zip_urls:
                        logger.warning(f"Nenhum arquivo .zip encontrado em {folder_url}")
                        continue
                    
                    # Filtrar por tipos desejados
                    if tipos_desejados is not None:
                        try:
                            filtered_urls, ignored_count = _filter_urls_by_type(zip_urls, tipos_desejados)
                            logger.info(f"{ignored_count} arquivos ignorados com base nos tipos não desejados em {folder_name}.")
                            zip_urls = filtered_urls
                        except Exception as e:
                            logger.error(f"Erro ao filtrar URLs em {folder_name}: {e}")
                            all_successful = False
                            continue
                    
                    if not zip_urls:
                        logger.warning(f"Nenhuma URL relevante para download encontrada após filtrar por tipos em {folder_name}.")
                        continue
                    
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
                logger.info(f"Total de arquivos baixados: {downloaded_files_count}")
                if failed_downloads_count > 0:
                    logger.warning(f"Total de downloads falhos: {failed_downloads_count}")
                
                return downloaded_files_count > 0, "all_folders"
                
            except Exception as e:
                logger.exception(f"Erro no modo ALL_FOLDERS: {e}")
                return False, ""
        
        # Modo normal: baixar de uma pasta específica ou a mais recente
        # 1. Buscar URLs mais recentes ou da pasta específica
        try:
            all_zip_urls, latest_folder = get_latest_month_zip_urls(base_url, remote_folder)
            if not all_zip_urls:
                if remote_folder:
                    logger.warning(f"Nenhuma URL .zip encontrada na pasta remota especificada: {remote_folder}.")
                else:
                    logger.warning("Nenhuma URL .zip encontrada na origem.")
                return False, ""
        except aiohttp.ClientError as e:
            logger.error(f"Erro de conexão ao buscar URLs: {e}")
            return False, ""
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar URLs: {e}")
            return False, ""

        # 2. Filtrar URLs desejadas
        if tipos_desejados is None:
            tipos_desejados = ("Empresas", "Estabelecimentos", "Simples", "Socios")
        try:
            zip_urls_to_download, ignored_count = _filter_urls_by_type(all_zip_urls, tipos_desejados)
            logger.info(f"{ignored_count} arquivos ignorados com base nos tipos não desejados.")
        except Exception as e:
            logger.error(f"Erro ao filtrar URLs: {e}")
            return False, ""

        if not zip_urls_to_download:
            logger.warning(f"Nenhuma URL relevante para download encontrada após filtrar por tipos.")
            return False, ""

        # Criar subdiretório para esta pasta remota (igual ao modo --all-folders)
        folder_download_path = os.path.join(download_folder, latest_folder)
        os.makedirs(folder_download_path, exist_ok=True)
        logger.info(f"Criando subdiretório para a pasta remota: {folder_download_path}")
        
        # Log para depuração
        logger.info(f"Iniciando download de {len(zip_urls_to_download)} arquivos relevantes para {folder_download_path}...")
        logger.info(f"Sistema de cache está {'ativado' if config.cache.enabled else 'desativado'}")
        
        # Verificar se o diretório do cache existe
        if config.cache.enabled and not os.path.exists(config.cache.cache_dir):
            os.makedirs(config.cache.cache_dir, exist_ok=True)
            logger.info(f"Diretório de cache criado: {config.cache.cache_dir}")

        # 3. Baixar os arquivos
        try:
            max_concurrent_downloads = config.n_workers
            downloaded, failed = await download_multiple_files(
                zip_urls_to_download,
                folder_download_path,
                max_concurrent=max_concurrent_downloads
            )
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

        return True, latest_folder

    except Exception as e:
        logger.exception(f"Erro crítico no processo de download: {e}")
        return False, ""


def process_folder(source_zip_path, unzip_path, output_parquet_path, 
                 tipos_list, engine, criar_empresa_privada, criar_subset_uf,
                 tipos_a_processar) -> bool:
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
        bool: Sucesso ou falha
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Engine selecionado: {engine}")
    
    all_ok = True
    
    # Processa Empresas
    if 'empresas' in tipos_a_processar:
        if 'Empresas' in tipos_list or 'empresas' in tipos_list:
            # Agora só usamos polars
            empresas_ok = process_empresa_with_polars(
                source_zip_path, unzip_path, output_parquet_path, criar_empresa_privada
            )
                
            if not empresas_ok:
                logger.error("Erro no processamento de empresas.")
                all_ok = False
    
    # Processa Estabelecimentos
    if 'estabelecimentos' in tipos_a_processar:
        if 'Estabelecimentos' in tipos_list or 'estabelecimentos' in tipos_list:
            uf_subset = None
            
            # Se criar_subset_uf foi especificado, extrai a sigla da UF
            if criar_subset_uf:
                if isinstance(criar_subset_uf, str) and len(criar_subset_uf) == 2:
                    uf_subset = criar_subset_uf.upper()
                    logger.info(f"Criando subset de estabelecimentos para UF: {uf_subset}")
                else:
                    logger.warning(f"Valor inválido para subset UF: {criar_subset_uf}. Ignorando.")
            
            estab_ok = process_estabelecimento_with_polars(
                source_zip_path, unzip_path, output_parquet_path, uf_subset
            )
            
            if not estab_ok:
                logger.error("Erro no processamento de estabelecimentos.")
                all_ok = False
    
    # Processa Simples Nacional
    if 'simples' in tipos_a_processar:
        if 'Simples' in tipos_list or 'simples' in tipos_list:
            simples_ok = process_simples_with_polars(
                source_zip_path, unzip_path, output_parquet_path
            )
            
            if not simples_ok:
                logger.error("Erro no processamento do simples nacional.")
                all_ok = False
    
    # Processa Sócios
    if 'socios' in tipos_a_processar:
        if 'Socios' in tipos_list or 'socios' in tipos_list:
            socios_ok = process_socio_with_polars(
                source_zip_path, unzip_path, output_parquet_path
            )
            
            if not socios_ok:
                logger.error("Erro no processamento de sócios.")
                all_ok = False
    
    return all_ok


def main():
    """Função principal de execução."""
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
        
        # Verificar se temos pastas/subpastas de dados
        if args.subfolder is not None:
            # Processa apenas a subpasta especificada
            logger.info(f"Processando subpasta: {args.subfolder}")
            subfolder_path = os.path.join(PATH_ZIP, args.subfolder)
            if not os.path.exists(subfolder_path):
                logger.error(f"Subpasta especificada não existe: {subfolder_path}")
                print_error(f"Subpasta {args.subfolder} não encontrada em {PATH_ZIP}")
                return
                
            subfolder_output_path = os.path.join(PATH_PARQUET, args.subfolder)
            os.makedirs(subfolder_output_path, exist_ok=True)
            
            # Definir tipos a processar
            tipos_a_processar = {
                'empresas': 'Empresas',
                'estabelecimentos': 'Estabelecimentos',
                'simples': 'Simples Nacional',
                'socios': 'Sócios'
            }
            
            # Iniciar processamento
            processed_ok = process_folder(
                subfolder_path, PATH_UNZIP, subfolder_output_path, 
                args.tipos, args.engine, args.criar_empresa_privada, args.criar_subset_uf,
                tipos_a_processar
            )
            
        elif args.folder is not None:
            # Processa a pasta especificada
            logger.info(f"Processando pasta: {args.folder}")
            folder_path = args.folder
            
            if not os.path.exists(folder_path):
                logger.error(f"Pasta especificada não existe: {folder_path}")
                print_error(f"Pasta {args.folder} não encontrada")
                return
                
            output_path = PATH_PARQUET
            if args.output:
                output_path = args.output
                os.makedirs(output_path, exist_ok=True)
            
            # Definir tipos a processar
            tipos_a_processar = {
                'empresas': 'Empresas',
                'estabelecimentos': 'Estabelecimentos',
                'simples': 'Simples Nacional',
                'socios': 'Sócios'
            }
            
            # Iniciar processamento
            processed_ok = process_folder(
                folder_path, PATH_UNZIP, output_path, 
                args.tipos, args.engine, args.criar_empresa_privada, args.criar_subset_uf,
                tipos_a_processar
            )
        else:
            # Processa a pasta padrão PATH_ZIP
            logger.info(f"Processando pasta padrão: {PATH_ZIP}")
            
            # Definir tipos a processar
            tipos_a_processar = {
                'empresas': 'Empresas',
                'estabelecimentos': 'Estabelecimentos',
                'simples': 'Simples Nacional', 
                'socios': 'Sócios'
            }
            
            # Definir pasta de saída (padrão ou especificada)
            target_parquet_output_path = PATH_PARQUET
            if args.output:
                target_parquet_output_path = args.output
                os.makedirs(target_parquet_output_path, exist_ok=True)
                
            # Qual pasta ZIP usar (default PATH_ZIP)
            path_zip_to_use = PATH_ZIP
            
            # Iniciar processamento
            processed_ok = process_folder(
                path_zip_to_use, PATH_UNZIP, target_parquet_output_path, 
                args.tipos, args.engine, args.criar_empresa_privada, args.criar_subset_uf,
                tipos_a_processar
            )
    
    # Resto do código do step 'download'
    elif args.step == 'download':
        # ... (código existente para download)
        pass
    # ... (outros steps)
    
    print_header("Processamento concluído")
    logger.info("Execução concluída.")
    return


if __name__ == '__main__':
    main()
