"""
Exemplos de uso:

1. Execução padrão (Baixa todos os tipos na pasta mais recente, processa todos com Pandas, salva em subpasta com nome da data baixada):
   python main.py

2. Baixa e processa apenas Empresas e Sócios com Pandas (salva em subpasta com nome da data baixada):
   python main.py --tipos empresas socios

3. Baixa e processa todos os tipos usando o motor Dask (salva em subpasta com nome da data baixada):
   python main.py --engine dask

4. Baixa e processa apenas Estabelecimentos usando Polars (salva em subpasta com nome da data baixada):
   python main.py --tipos estabelecimentos --engine polars

5. Pular o download e processar todos os tipos da pasta ZIP '../dados-abertos-zip', salvando Parquet na subpasta 'meu_processamento_manual' (dentro de PATH_PARQUET):
   python main.py --skip-download --source-zip-folder ../dados-abertos-zip --output-subfolder meu_processamento_manual

6. Pular o download, processar apenas Simples e Sócios da pasta ZIP 'D:/MeusDownloads/CNPJ_ZIPs', usando Dask, salvando Parquet na subpasta 'simples_socios_dask' (dentro de PATH_PARQUET):
   python main.py --skip-download --source-zip-folder "D:/MeusDownloads/CNPJ_ZIPs" --output-subfolder simples_socios_dask --tipos simples socios --engine dask

7. Baixa e processa apenas Empresas usando Pandas (salva em subpasta com nome da data baixada):
   python main.py --tipos empresas --engine pandas

8. Baixa e processa apenas Empresas usando Polars, salvando na subpasta 'apenas_empresas_polars' (dentro de PATH_PARQUET):
   python main.py --tipos empresas --engine polars --output-subfolder apenas_empresas_polars

9. Como o exemplo 8, mas também cria o subconjunto 'empresa_privada' no diretório de saída:
   python main.py --tipos empresas --engine polars --output-subfolder apenas_empresas_polars --criar-empresa-privada

10. Pular download E processamento, criando apenas o arquivo DuckDB a partir dos Parquets existentes na subpasta 'processamento_anterior' (dentro de PATH_PARQUET):
    python main.py --skip-processing --output-subfolder processamento_anterior

11. Pular download, processar com Dask, e depois criar o DuckDB, usando a pasta de origem 'meus_zips' e salvando na subpasta 'resultado_dask':
    python main.py --skip-download --source-zip-folder meus_zips --engine dask --output-subfolder resultado_dask

12. Processar apenas estabelecimentos com Polars, criando também um subset para São Paulo (SP) na saída 'parquet/process_sp/estabelecimentos_sp':
    python main.py --tipos estabelecimentos --engine polars --output-subfolder process_sp --criar-subset-uf SP

13. Baixar e processar dados de uma pasta remota específica (2023-05) em vez da pasta mais recente:
    python main.py --remote-folder 2023-05
"""
import argparse
import asyncio
import datetime
import logging
import os
from multiprocessing import freeze_support
import re

import aiohttp
from dask.distributed import Client, LocalCluster
from dotenv import load_dotenv
from rich.logging import RichHandler

from src.async_downloader import get_latest_month_zip_urls, download_multiple_files, _filter_urls_by_type
from src.config import config
from src.database import create_duckdb_file
from src.process.empresa import process_empresa, process_empresa_with_pandas as process_empresa_pandas_impl, process_empresa_with_polars as process_empresa_polars_impl
from src.process.estabelecimento import process_estabelecimento
from src.process.simples import process_simples, process_single_zip_pandas, process_single_zip_polars
from src.process.socio import process_socio
from src.utils import check_basic_folders
import dask
from src.utils.dask_manager import DaskManager


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


async def run_download_process(tipos_desejados: list[str] | None = None, remote_folder: str | None = None):
    """Executa todo o processo de download de forma assíncrona.

    Args:
        tipos_desejados: Lista de tipos de arquivos a serem baixados. Se None, baixa todos.
        remote_folder: Pasta remota específica a ser baixada no formato AAAA-MM. Se None, usa a mais recente.

    Returns:
        tuple[bool, str]: (sucesso do download, pasta mais recente)
            - Se sucesso for False, a pasta mais recente será uma string vazia
    """
    logger = logging.getLogger(__name__)
    logger.info("Iniciando processo de download centralizado...")

    try:
        base_url = os.getenv('URL_ORIGIN')
        download_folder = os.getenv('PATH_ZIP')

        if not base_url or not download_folder:
            logger.error("Variáveis de ambiente URL_ORIGIN ou PATH_ZIP não definidas.")
            return False, ""

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

        logger.info(f"Iniciando download de {len(zip_urls_to_download)} arquivos relevantes para {download_folder}...")

        # 3. Baixar os arquivos
        try:
            max_concurrent_downloads = config.dask.n_workers
            downloaded, failed = await download_multiple_files(
                zip_urls_to_download,
                download_folder,
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


def setup_dask_cluster():
    """Configura e retorna um cluster Dask otimizado."""
    dask.config.set({
        'distributed.worker.memory.target': 0.6,
        'distributed.worker.memory.spill': 0.7,
        'distributed.worker.memory.pause': 0.8,
        'distributed.worker.memory.terminate': 0.95,
        'distributed.comm.timeouts.connect': '30s',
        'distributed.comm.timeouts.tcp': '30s',
        'distributed.nanny.environ.MALLOC_TRIM_THRESHOLD_': '65536',
    })

    return LocalCluster(
        n_workers=config.dask.n_workers,
        threads_per_worker=config.dask.threads_per_worker,
        memory_limit=config.dask.memory_limit,
        dashboard_address=config.dask.dashboard_address
    )

def main():
    """Função principal que orquestra todo o processo."""
    parser = argparse.ArgumentParser(description='Processa dados do CNPJ')
    parser.add_argument('--tipos', '-t', nargs='+', choices=['empresas', 'estabelecimentos', 'simples', 'socios'],
                        help='Tipos de dados a serem processados. Se não especificado, processa todos (relevante para steps \'process\' e \'all\').')
    parser.add_argument('--engine', '-e', choices=['pandas', 'dask', 'polars'], default='polars',
                        help='Motor de processamento a ser utilizado (relevante para steps \'process\' e \'all\'). Padrão: polars')
    parser.add_argument('--source-zip-folder', '-z', type=str, default=None,
                        help='Caminho para a pasta contendo os arquivos ZIP a serem processados (Obrigatório para --step process).')
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
        '--step', '-s',
        choices=['download', 'process', 'database', 'all'], 
        default='all',
        help="""Especifica qual(is) etapa(s) executar:
                        'download': Apenas baixa os arquivos ZIP mais recentes (ou da pasta especificada em --remote-folder).
                        'process': Apenas processa ZIPs existentes (--source-zip-folder) para Parquet (--output-subfolder).
                        'database': Apenas cria o DuckDB a partir de Parquets existentes (--output-subfolder).
                        'all': Executa todas as etapas: download -> process -> database (padrão)."""
    )
    args = parser.parse_args()

    # --- Validação de Argumentos --- 
    # Validação baseada no --step escolhido
    if args.step == 'process' and (not args.source_zip_folder or not args.output_subfolder):
        parser.error("--step 'process' requer que --source-zip-folder e --output-subfolder sejam especificados.")
    if args.step == 'database' and not args.output_subfolder:
        parser.error("--step 'database' requer que --output-subfolder seja especificado.")
    if args.source_zip_folder and not os.path.isdir(args.source_zip_folder):
         parser.error(f"A pasta de origem especificada (--source-zip-folder) não existe ou não é um diretório: {args.source_zip_folder}")

    # Validação da UF (mantida)
    if args.criar_subset_uf and len(args.criar_subset_uf) != 2:
        parser.error("--criar-subset-uf deve receber uma sigla de UF com 2 caracteres (ex: SP, RJ).")
    if args.criar_subset_uf:
        args.criar_subset_uf = args.criar_subset_uf.upper()
        
    # Validação da pasta remota
    if args.remote_folder and not re.fullmatch(r'(\d{4})-(\d{2})', args.remote_folder):
        parser.error("--remote-folder deve estar no formato AAAA-MM (ex: 2023-01).")

    tipo_para_nome = {
        'empresas': 'Empresas',
        'estabelecimentos': 'Estabelecimentos',
        'simples': 'Simples',
        'socios': 'Socios'
    }

    logger = setup_logging(args.log_level)
    print_header(f'Início da execução: {datetime.datetime.now():%d/%m/%Y às %H:%M:%S}')
    start_time = datetime.datetime.now()

    print_section("Carregando variáveis de ambiente...")
    load_dotenv('.env.local')
    print_success("Variáveis de ambiente carregadas com sucesso")

    PATH_ZIP = os.getenv('PATH_ZIP')
    PATH_UNZIP = os.getenv('PATH_UNZIP')
    PATH_PARQUET = os.getenv('PATH_PARQUET')
    FILE_DB_PARQUET = os.getenv('FILE_DB_PARQUET')
    PATH_REMOTE_PARQUET = os.getenv('PATH_REMOTE_PARQUET')

    print_section("Criando diretórios necessários...")
    list_folders = [PATH_ZIP, PATH_UNZIP, PATH_PARQUET]
    list_folders.append(config.cache.cache_dir)
    for folder in list_folders:
        if folder:
            check_basic_folders(folder)
    print_success("Diretórios criados com sucesso")

    tipos_a_processar = {
        'empresas': (process_empresa, "EMPRESAS"),
        'estabelecimentos': (process_estabelecimento, "ESTABELECIMENTOS"),
        'simples': (process_simples, "SIMPLES NACIONAL"),
        'socios': (process_socio, "SOCIOS")
    }
    tipo_para_nome = {k: v[1].split()[0].capitalize() for k, v in tipos_a_processar.items()}

    if not args.tipos:
        args.tipos = list(tipos_a_processar.keys())

    # --- Definição das Etapas a Executar e Caminhos --- 
    run_download = args.step in ['download', 'all']
    run_process = args.step in ['process', 'all']
    run_database = args.step in ['database', 'all']

    path_zip_to_use = None
    target_parquet_output_path = None
    latest_folder = None # Nome da subpasta para Parquet

    if run_download:
        path_zip_to_use = PATH_ZIP # Onde salvar os downloads
    elif run_process:
        path_zip_to_use = args.source_zip_folder # De onde ler os ZIPs

    if run_process or run_database:
        # Determina a pasta de saída/leitura Parquet
        if args.output_subfolder:
            latest_folder = args.output_subfolder
        elif run_process and args.source_zip_folder:
            # Se processando e sem output_subfolder, usa nome da pasta de origem
            latest_folder = os.path.basename(os.path.normpath(args.source_zip_folder))
            print_warning(f"Nenhuma --output-subfolder especificada para 'process'. Usando nome da pasta de origem ZIP: '{latest_folder}'")
        # Se for 'all' e sem output_subfolder, será definido após o download
        # Se for 'database' sem output_subfolder, já deu erro na validação inicial
        
        if latest_folder:
             target_parquet_output_path = os.path.join(PATH_PARQUET, latest_folder)
             logger.info(f"Caminho Parquet a ser usado/gerado: {target_parquet_output_path}")
             # Garante que exista se formos processar
             if run_process:
                 os.makedirs(target_parquet_output_path, exist_ok=True)
        # else: Se latest_folder ainda é None, será definido após download (no caso 'all')

    # Inicializa Dask se necessário (antes das etapas que o usam)
    dask_manager = None
    if args.engine == 'dask' and (run_process or run_database): # Adaptação: só inicializa se for usar
        print_section("Iniciando configuração do Dask...")
        freeze_support()
        try:
            dask_manager = DaskManager.initialize(
                n_workers=config.dask.n_workers,
                memory_limit=config.dask.memory_limit,
                dashboard_address=config.dask.dashboard_address
            )
            print_success(f"Cliente Dask inicializado com sucesso: {dask_manager.client}")
        except Exception as e:
            logger.error(f"Falha ao inicializar o Dask: {e}")
            print_error("Não foi possível iniciar o Dask. Abortando.")
            return # Não continuar sem Dask se ele for necessário

    # --- Etapa 1: Download --- 
    if run_download:
        print_header("Etapa 1: Download")
        tipos_desejados_dl = [tipo_para_nome[t] for t in args.tipos] if args.tipos else None
        download_successful, latest_folder_from_dl = asyncio.run(run_download_process(tipos_desejados_dl, args.remote_folder))
        
        if not download_successful:
            print_error("Download falhou ou não encontrou arquivos.")
            if args.step == 'download': # Se era só download, para aqui
                 if dask_manager: dask_manager.shutdown()
                 return 
            else: # Se era 'all', avisa mas tenta continuar com o que tem
                print_warning("Tentando continuar com arquivos possivelmente incompletos...")
        else:
             print_success("Download concluído com sucesso.")

        # Define a pasta de saída para o processamento (caso 'all' e sem --output-subfolder)
        if not latest_folder and latest_folder_from_dl:
            latest_folder = latest_folder_from_dl
            target_parquet_output_path = os.path.join(PATH_PARQUET, latest_folder)
            logger.info(f"Usando subpasta de saída padrão (data baixada): '{latest_folder}'. Caminho Parquet: {target_parquet_output_path}")
            os.makedirs(target_parquet_output_path, exist_ok=True)
        elif not target_parquet_output_path:
            # Caso raro: download ok, mas sem nome de pasta e sem --output-subfolder
            latest_folder = f"processamento_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            target_parquet_output_path = os.path.join(PATH_PARQUET, latest_folder)
            logger.warning(f"Não foi possível determinar a subpasta de saída. Usando nome padrão: '{latest_folder}'")
            os.makedirs(target_parquet_output_path, exist_ok=True)

        # Se a etapa era APENAS download, encerra aqui.
        if args.step == 'download':
            print_success("Etapa 'download' concluída.")
            if dask_manager:
                dask_manager.shutdown()
            return
            
    # --- Etapa 2: Processamento para Parquet --- 
    processing_performed = False
    if run_process:
        print_header("Etapa 2: Processamento para Parquet")
        # Verifica se os caminhos necessários estão definidos
        if not path_zip_to_use or not target_parquet_output_path:
             print_error("Erro interno: Caminhos de entrada ZIP ou saída Parquet não definidos para a etapa de processamento.")
             if dask_manager: dask_manager.shutdown()
             return
        
        logger.info(f"Lendo ZIPs de: {path_zip_to_use}")
        logger.info(f"Salvando Parquets em: {target_parquet_output_path}")
        
        processing_success_flag = False
        try:
            for tipo in args.tipos:
                process_func_base, nome_tipo = tipos_a_processar[tipo]
                print_section(f"Processando dados de {nome_tipo} com engine '{args.engine}'...")
                
                kwargs = {}
                if tipo == 'empresas':
                    kwargs['create_private'] = args.criar_empresa_privada
                if tipo == 'estabelecimentos' and args.criar_subset_uf:
                    kwargs['uf_subset'] = args.criar_subset_uf
                
                # --- Lógica de Seleção de Função (mantida) ---
                specific_process_func = None
                if tipo == 'empresas':
                    if args.engine == 'pandas': specific_process_func = process_empresa_pandas_impl
                    elif args.engine == 'polars': specific_process_func = process_empresa_polars_impl
                    elif args.engine == 'dask': specific_process_func = process_empresa
                elif tipo == 'simples':
                    if args.engine == 'pandas':
                        try:
                            from src.process.simples import process_simples_with_pandas
                            specific_process_func = process_simples_with_pandas
                        except ImportError: logger.warning("Função 'process_simples_with_pandas' não encontrada.")
                    elif args.engine == 'polars':
                        try:
                            from src.process.simples import process_simples_with_polars
                            specific_process_func = process_simples_with_polars
                        except ImportError: logger.warning("Função 'process_simples_with_polars' não encontrada.")
                    elif args.engine == 'dask': specific_process_func = process_simples
                elif tipo == 'socios':
                    if args.engine == 'pandas':
                        try:
                            from src.process.socio import process_socio_with_pandas
                            specific_process_func = process_socio_with_pandas
                        except ImportError: logger.warning("Função 'process_socio_with_pandas' não encontrada.")
                    elif args.engine == 'polars':
                        try:
                            from src.process.socio import process_socio_with_polars
                            specific_process_func = process_socio_with_polars
                        except ImportError: logger.warning("Função 'process_socio_with_polars' não encontrada.")
                    elif args.engine == 'dask': specific_process_func = process_socio
                elif tipo == 'estabelecimentos':
                    if args.engine == 'pandas':
                         try:
                             from src.process.estabelecimento import process_estabelecimento_with_pandas
                             specific_process_func = process_estabelecimento_with_pandas
                         except ImportError: logger.warning("Função 'process_estabelecimento_with_pandas' não encontrada.")
                    elif args.engine == 'polars':
                         try:
                             from src.process.estabelecimento import process_estabelecimento_with_polars
                             specific_process_func = process_estabelecimento_with_polars
                         except ImportError: logger.warning("Função 'process_estabelecimento_with_polars' não encontrada.")
                    elif args.engine == 'dask': specific_process_func = process_estabelecimento
                
                func_to_call = specific_process_func if specific_process_func else process_func_base
                
                if not func_to_call:
                     logger.error(f"Nenhuma função de processamento válida encontrada para tipo '{tipo}' e engine '{args.engine}'. Pulando...")
                     continue
                     
                # --- Chamada da Função (mantida) ---
                import inspect
                sig = inspect.signature(func_to_call)
                valid_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
                
                success = func_to_call(path_zip_to_use, PATH_UNZIP, target_parquet_output_path, **valid_kwargs)
                processing_performed = True # Marca que tentamos processar algo
                
                if success:
                    processing_success_flag = True
                    print_success(f"Dados de {nome_tipo} processados com sucesso.")
                else:
                    print_warning(f"Processamento de {nome_tipo} não foi bem-sucedido ou não encontrou/gerou dados.")
                    
            if not processing_performed:
                 print_warning("Nenhum tipo de dado foi efetivamente processado.")
            elif not processing_success_flag:
                 print_warning("Nenhuma etapa de processamento Parquet foi concluída com sucesso.")
                 
        except Exception as e:
            logger.exception(f"Erro GERAL durante a fase de processamento para Parquet: {e}")
            print_error("Erro crítico na fase de processamento. Verifique os logs.")
            # Mesmo com erro, podemos querer criar o DB se a etapa for 'all' ou 'database'

        # Se a etapa era APENAS processamento, encerra aqui.
        if args.step == 'process':
             print_success("Etapa 'process' concluída.")
             if dask_manager: dask_manager.shutdown()
             return

    # --- Etapa 3: Criação do DuckDB --- 
    if run_database:
        print_header("Etapa 3: Criação do Banco de Dados DuckDB")
        # Verifica se o caminho Parquet está definido (importante se pulou etapas)
        if not target_parquet_output_path:
            print_error("Caminho para arquivos Parquet não definido. Não é possível criar DuckDB.")
            # Tenta pegar do argumento se a etapa for SÓ database
            if args.step == 'database' and args.output_subfolder:
                 target_parquet_output_path = os.path.join(PATH_PARQUET, args.output_subfolder)
                 logger.info(f"Usando caminho Parquet de --output-subfolder: {target_parquet_output_path}")
            else:
                if dask_manager: dask_manager.shutdown()
                return

        # Verifica se o caminho Parquet existe
        if not os.path.isdir(target_parquet_output_path):
            print_error(f"Caminho para arquivos Parquet não existe ou não é um diretório: {target_parquet_output_path}.")
            if dask_manager: dask_manager.shutdown()
            return
            
        logger.info(f"Lendo Parquets de: {target_parquet_output_path}")
        db_creation_success = create_duckdb_file(
            path_parquet_folder=target_parquet_output_path,
            file_db_parquet=FILE_DB_PARQUET,
            path_remote_parquet=PATH_REMOTE_PARQUET
        )
        if db_creation_success:
            print_success("Banco de dados DuckDB criado/atualizado com sucesso.")
        else:
            print_error("Falha ao criar/atualizar banco de dados DuckDB.")
            
        # Se a etapa era APENAS database, encerra aqui.
        if args.step == 'database':
             print_success("Etapa 'database' concluída.")
             if dask_manager: dask_manager.shutdown()
             return

    # --- Finalização --- 
    print_header(f"Tempo total de execução: {str(datetime.datetime.now() - start_time)}")
    if dask_manager:
        print_section("Encerrando cliente Dask...")
        try:
            dask_manager.shutdown()
            print_success("Cliente Dask encerrado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao encerrar cliente Dask: {e}")
    print_success("Processo finalizado!")


def process_empresa_with_pandas(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Wrapper para processar empresas com Pandas, aceitando create_private."""
    logger = logging.getLogger(__name__)
    logger.info(f"Iniciando processamento de EMPRESAS com Pandas (create_private={create_private})...")
    return process_empresa_pandas_impl(path_zip, path_unzip, path_parquet, create_private)

def process_empresa_with_polars(path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False) -> bool:
    """Wrapper para processar empresas com Polars, aceitando create_private."""
    logger = logging.getLogger(__name__)
    logger.info(f"Iniciando processamento de EMPRESAS com Polars (create_private={create_private})...")
    return process_empresa_polars_impl(path_zip, path_unzip, path_parquet, create_private)


if __name__ == '__main__':
    main()
