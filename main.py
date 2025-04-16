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
"""
import argparse
import asyncio
import datetime
import logging
import os
from multiprocessing import freeze_support

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


async def run_download_process(tipos_desejados: list[str] | None = None):
    """Executa todo o processo de download de forma assíncrona.

    Args:
        tipos_desejados: Lista de tipos de arquivos a serem baixados. Se None, baixa todos.

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

        # 1. Buscar URLs mais recentes
        try:
            all_zip_urls, latest_folder = get_latest_month_zip_urls(base_url)
            if not all_zip_urls:
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
    parser.add_argument('--tipos', nargs='+', choices=['empresas', 'estabelecimentos', 'simples', 'socios'],
                        help='Tipos de dados a serem processados. Se não especificado, processa todos.')
    parser.add_argument('--engine', choices=['pandas', 'dask', 'polars'], default='pandas',
                        help='Motor de processamento a ser utilizado. Padrão: pandas')
    parser.add_argument('--skip-download', action='store_true',
                        help='Pula a etapa de download.')
    parser.add_argument('--source-zip-folder', type=str, default=None,
                        help='Caminho para a pasta contendo os arquivos ZIP a serem processados (obrigatório e usado apenas com --skip-download).')
    parser.add_argument('--output-subfolder', type=str, default=None,
                        help='Nome da subpasta dentro de PATH_PARQUET onde os arquivos Parquet serão salvos. Se omitido, um nome padrão será usado (baseado na data/hora ou pasta de origem).')
    parser.add_argument('--criar-empresa-privada', action='store_true',
                        help='Se presente, cria um subconjunto Parquet adicional contendo apenas empresas privadas (natureza jurídica 2046-2348) na pasta \'empresa_privada\'.')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO',
                        help='Define o nível mínimo de log a ser exibido/salvo. Padrão: INFO.')
    parser.add_argument(
        '--skip-processing',
        action='store_true',
        help="Pular a fase de processamento para Parquet e ir direto para a criação do DuckDB (requer que os arquivos Parquet já existam)."
    )
    parser.add_argument(
        '--criar-subset-uf',
        type=str,
        default=None,
        metavar='UF',
        help="Opcional. Cria um subconjunto Parquet adicional contendo apenas estabelecimentos da UF especificada (ex: GO, SP, RJ) na pasta 'estabelecimentos_UF'."
    )
    args = parser.parse_args()

    # --- Validação de Argumentos ---
    if args.skip_processing and not args.output_subfolder:
        print_error("--output-subfolder é obrigatório ao usar --skip-processing.")
        # Idealmente, encerrar aqui ou garantir que o código abaixo lide com isso
        # Por segurança, vamos retornar para evitar erros posteriores
        return
    if args.skip_download and not args.source_zip_folder:
        # Esta validação já existe mais abaixo, mas podemos centralizar se preferir
        pass
    # Validação adicional para a nova flag UF
    if args.criar_subset_uf and len(args.criar_subset_uf) != 2:
        print_error("--criar-subset-uf deve receber uma sigla de UF com 2 caracteres (ex: SP, RJ).")
        return
    # Converter para maiúsculas para consistência
    if args.criar_subset_uf:
        args.criar_subset_uf = args.criar_subset_uf.upper()

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
        'socios': (process_socio, "SÓCIOS")
    }

    if not args.tipos:
        args.tipos = list(tipos_a_processar.keys())

    tipos_desejados = [tipo_para_nome[t] for t in args.tipos] if args.tipos else None

    dask_manager = None
    if args.engine == 'dask':
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
            print_error("Não foi possível iniciar o Dask. Verifique a configuração e os logs.")

    latest_folder = None
    path_zip_to_use = None # Inicializa como None
    target_parquet_output_path = None # Inicializa como None
    process_parquet_needed = not args.skip_processing # Flag para clareza

    # --- Etapa 1: Download e Determinação de Caminhos (APENAS SE process_parquet_needed) ---
    if process_parquet_needed:
        if args.skip_download:
            print_warning("A etapa de download será pulada (--skip-download).")
            if not args.source_zip_folder:
                print_error("Com --skip-download, é obrigatório especificar a pasta de origem dos ZIPs com --source-zip-folder.")
                if dask_manager: dask_manager.shutdown()
                return
            if not os.path.exists(args.source_zip_folder) or not os.path.isdir(args.source_zip_folder):
                print_error(f"A pasta de origem especificada (--source-zip-folder) não existe ou não é um diretório: {args.source_zip_folder}")
                if dask_manager: dask_manager.shutdown()
                return

            path_zip_to_use = args.source_zip_folder
            print_success(f"Usando pasta de origem ZIP: {path_zip_to_use}")

            # Determina a pasta de saída
            if args.output_subfolder:
                latest_folder = args.output_subfolder
            else:
                latest_folder = os.path.basename(os.path.normpath(args.source_zip_folder))
                print_warning(f"Nenhuma --output-subfolder especificada. Usando nome da pasta de origem como subpasta de saída: '{latest_folder}'")
        else:
            # Executa o download
            print_header("Iniciando Etapa de Download...")
            path_zip_to_use = PATH_ZIP # Usa o path padrão para downloads
            download_successful, latest_folder_from_dl = asyncio.run(run_download_process(tipos_desejados))

            if not download_successful:
                print_error("A etapa de download falhou ou não encontrou arquivos. Abortando processamento subsequente.")
                if dask_manager: dask_manager.shutdown()
                return

            # Determina a pasta de saída
            if args.output_subfolder:
                latest_folder = args.output_subfolder
                print_success(f"Etapa de Download concluída. Usando subpasta de saída especificada: '{latest_folder}'")
            else:
                latest_folder = latest_folder_from_dl
                print_success(f"Etapa de Download concluída. Usando subpasta de saída padrão (data baixada): '{latest_folder}'")

        # Define o caminho final de saída Parquet para esta execução
        if not latest_folder:
            latest_folder = f"processamento_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            print_warning(f"Não foi possível determinar a subpasta de saída Parquet. Usando nome padrão: '{latest_folder}'")
        target_parquet_output_path = os.path.join(PATH_PARQUET, latest_folder)
        logger.info(f"Caminho de saída Parquet definido como: {target_parquet_output_path}")
        os.makedirs(target_parquet_output_path, exist_ok=True) # Garante que exista

    else: # Se skip_processing for True
        print_warning("Fases de Download e Processamento Parquet PULADAS (--skip-processing ativo).")
        # Define o caminho Parquet usando a subpasta obrigatória
        target_parquet_output_path = os.path.join(PATH_PARQUET, args.output_subfolder)
        logger.info(f"Usando caminho Parquet existente: {target_parquet_output_path}")
        # Validação adicional: Verifica se o caminho realmente existe
        if not os.path.exists(target_parquet_output_path) or not os.path.isdir(target_parquet_output_path):
            print_error(f"A pasta Parquet especificada via --output-subfolder ('{args.output_subfolder}') não existe em {PATH_PARQUET}. Necessária para --skip-processing.")
            if dask_manager: dask_manager.shutdown()
            return

    # --- Etapa 2: Processamento para Parquet (APENAS SE process_parquet_needed) ---
    is_create_db_parquet = False # Tentaremos criar o DB se skip_processing OU se o processamento ocorrer (mesmo que falhe)

    if process_parquet_needed:
        print_header("Iniciando processamento dos dados...")
        processing_success_flag = False # Flag para saber se ALGUM processamento funcionou
        try:
            for tipo in args.tipos:
                process_func, nome = tipos_a_processar[tipo]
                print_section(f"Processando dados de {nome}...")

                kwargs = {}
                if tipo == 'empresas':
                    kwargs['create_private'] = args.criar_empresa_privada
                # Passa a UF desejada para as funções de processamento, se fornecida
                if tipo == 'estabelecimentos' and args.criar_subset_uf:
                     kwargs['uf_subset'] = args.criar_subset_uf

                if args.engine == 'dask' and dask_manager:
                    pass

                specific_process_func = None
                if tipo == 'empresas':
                    if args.engine == 'pandas':
                        specific_process_func = process_empresa_pandas_impl
                    elif args.engine == 'polars':
                        specific_process_func = process_empresa_polars_impl
                    elif args.engine == 'dask':
                        specific_process_func = process_empresa
                elif tipo == 'simples':
                    if args.engine == 'pandas':
                        try:
                            from src.process.simples import process_simples_with_pandas
                            specific_process_func = process_simples_with_pandas
                        except ImportError:
                            logger.warning(f"Função 'process_simples_with_pandas' não encontrada. Usando a função padrão.")
                            specific_process_func = process_func
                    elif args.engine == 'polars':
                        try:
                            from src.process.simples import process_simples_with_polars
                            specific_process_func = process_simples_with_polars
                        except ImportError:
                            logger.warning(f"Função 'process_simples_with_polars' não encontrada. Usando a função padrão.")
                            specific_process_func = process_func
                    elif args.engine == 'dask':
                        specific_process_func = process_simples
                elif tipo == 'socios':
                    if args.engine == 'pandas':
                        try:
                            from src.process.socio import process_socio_with_pandas
                            specific_process_func = process_socio_with_pandas
                        except ImportError:
                            logger.warning(f"Função 'process_socio_with_pandas' não encontrada. Usando a função padrão.")
                            specific_process_func = process_func
                    elif args.engine == 'polars':
                        try:
                            from src.process.socio import process_socio_with_polars
                            specific_process_func = process_socio_with_polars
                        except ImportError:
                            logger.warning(f"Função 'process_socio_with_polars' não encontrada. Usando a função padrão.")
                            specific_process_func = process_func
                    elif args.engine == 'dask':
                        specific_process_func = process_socio
                elif tipo == 'estabelecimentos':
                    # Seleciona a implementação específica para estabelecimentos
                    specific_process_func = None # Reseta para garantir que a lógica abaixo funcione
                    if args.engine == 'pandas':
                         try:
                             from src.process.estabelecimento import process_estabelecimento_with_pandas
                             specific_process_func = process_estabelecimento_with_pandas
                         except ImportError:
                              logger.warning(f"Função 'process_estabelecimento_with_pandas' não encontrada. Usando a função padrão.")
                              # Se não achar específica, não define specific_process_func, cairá no fallback geral
                    elif args.engine == 'polars':
                         try:
                             # Importa a função Polars que acabamos de adicionar
                             from src.process.estabelecimento import process_estabelecimento_with_polars
                             specific_process_func = process_estabelecimento_with_polars
                         except ImportError:
                              logger.warning(f"Função 'process_estabelecimento_with_polars' não encontrada. Usando a função padrão.")
                              # Se não achar específica, não define specific_process_func, cairá no fallback geral
                    elif args.engine == 'dask':
                         # Assume que process_estabelecimento é a função Dask
                         specific_process_func = process_estabelecimento 

                # Se nenhuma função específica foi encontrada para o engine/tipo, usa a padrão selecionada antes do loop
                func_to_call = specific_process_func if specific_process_func else process_func

                import inspect
                sig = inspect.signature(func_to_call)
                valid_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}

                success = func_to_call(path_zip_to_use, PATH_UNZIP, target_parquet_output_path, **valid_kwargs)

                if success:
                    # is_create_db_parquet = True # Não seta mais aqui
                    processing_success_flag = True # Marca que pelo menos um funcionou
                    print_success(f"Dados de {nome} processados com sucesso")
                else:
                    print_warning(f"Processamento de {nome} não foi bem-sucedido ou não encontrou dados novos.")

            # Após o loop, verificamos se algo foi processado
            if not processing_success_flag:
                print_warning("Nenhuma etapa de processamento Parquet foi concluída com sucesso ou gerou dados novos.")
            # Independentemente do sucesso do processamento, tentaremos criar o DuckDB
            is_create_db_parquet = True

        except Exception as e:
            logger.exception(f"Erro durante a fase de processamento para Parquet: {e}")
            print_error("Erro na fase de processamento. Verifique os logs.")
            # Mesmo com erro, tentamos criar o DB a partir do que pode ter sido gerado
            is_create_db_parquet = True

    else: # Se skip_processing for True
        # Já avisamos que pulou. Marcamos para criar o DB.
        is_create_db_parquet = True

    # --- Etapa 3: Criação do DuckDB (Condicional) ---
    if is_create_db_parquet:
        # Verifica se o caminho de destino existe antes de tentar criar
        if target_parquet_output_path and os.path.exists(target_parquet_output_path):
            print_header("Criando banco de dados DuckDB...")
            db_creation_success = create_duckdb_file(
                path_parquet_folder=target_parquet_output_path,
                file_db_parquet=FILE_DB_PARQUET,
                path_remote_parquet=PATH_REMOTE_PARQUET
            )
            if db_creation_success:
                print_success("Banco de dados DuckDB criado com sucesso")
            else:
                print_error("Falha ao criar banco de dados DuckDB.")
        else:
            print_error(f"Caminho para arquivos Parquet inválido ou não encontrado: {target_parquet_output_path}. Não é possível criar DuckDB.")
    else:
        # Este caso não deve ocorrer com a lógica atual, mas mantemos por segurança
        print_warning("Criação do DuckDB não será realizada (nenhum processamento iniciado ou skip ativo sem sucesso?).")

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
