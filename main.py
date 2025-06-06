"""
Exemplos de uso:

1. Execução padrão (Baixa todos os tipos na pasta mais recente e salva em subpasta com nome da data baixada):
   python main.py

2. Baixa e processa apenas Empresas e Sócios (salva em subpasta com nome da data baixada):
   python main.py --tipos empresas socios

3. Baixa e processa todos os tipos (salva em subpasta com nome da data baixada):
   python main.py

4. Pular o download e processar todos os tipos da pasta ZIP '../dados-abertos-zip/2023-05', salvando Parquet na subpasta 'meu_processamento_manual' (dentro de PATH_PARQUET):
   python main.py --step process --source-zip-folder ../dados-abertos-zip/2023-05 --output-subfolder meu_processamento_manual

5. Pular o download, processar apenas Simples e Sócios da pasta ZIP 'D:/MeusDownloads/CNPJ_ZIPs/2023-01', salvando Parquet na subpasta 'simples_socios' (dentro de PATH_PARQUET):
   python main.py --step process --source-zip-folder "D:/MeusDownloads/CNPJ_ZIPs/2023-01" --output-subfolder simples_socios --tipos simples socios

6. Baixa e processa apenas Empresas (salva em subpasta com nome da data baixada):
   python main.py --tipos empresas

7. Baixa e processa apenas Empresas e salvando na subpasta 'apenas_empresas' (dentro de PATH_PARQUET):
   python main.py --tipos empresas --output-subfolder apenas_empresa

8. Como o exemplo 7, mas também cria o subconjunto 'empresa_privada' no diretório de saída:
   python main.py --tipos empresas --output-subfolder apenas_empresas --criar-empresa-privada

9. Pular download E processamento, criando apenas o arquivo DuckDB a partir dos Parquets existentes na subpasta 'processamento_anterior' (dentro de PATH_PARQUET):
    python main.py --step database --output-subfolder processamento_anterior

10. Pular download, processar, e depois criar o DuckDB, usando a pasta de origem 'meus_zips/2023-05' e salvando na subpasta 'resultado':
    python main.py --step process --source-zip-folder meus_zips/2023-05 --output-subfolder resultado

11. Processar apenas estabelecimentos, criando também um subset para São Paulo (SP) na saída 'parquet/process_sp/estabelecimentos_sp':
    python main.py --tipos estabelecimentos --output-subfolder process_sp --criar-subset-uf SP

12. Baixar e processar dados de uma pasta remota específica (2023-05) em vez da pasta mais recente:
    python main.py --remote-folder 2023-05

13. Baixar arquivos de TODOS os diretórios remotos disponíveis (salvando em subpastas separadas):
    python main.py --all-folders --step download

14. Processar dados de uma pasta baixada anteriormente (aponta diretamente para a subpasta com arquivos):
    python main.py --step process --source-zip-folder pasta_zips/2023-05 --output-subfolder processados_2023_05

15. Baixar arquivos forçando download mesmo que já existam localmente ou no cache:
    python main.py --remote-folder 2023-05 --force-download

16. Processar todas as pastas no formato AAAA-MM encontradas dentro de PATH_ZIP (útil após download com --all-folders):
    python main.py --step process --process-all-folders --output-subfolder processados_completos

17. Baixar arquivos de todas as pastas remotas a partir de 2023-01 até a mais atual:
    python main.py --all-folders --from-folder 2023-01 --step download

18. Baixar e processar arquivos de todas as pastas remotas desde a mais antiga até a mais atual:
    python main.py --all-folders

19. Baixar e processar arquivos a partir de uma pasta específica (2023-06) até a mais atual:
    python main.py --all-folders --from-folder 2023-06

20. Processar todas as pastas locais no formato AAAA-MM a partir de 2023-03:
    python main.py --step process --process-all-folders --from-folder 2023-03 --output-subfolder processados_desde_2023_03

21. Baixar sequencialmente da pasta mais antiga até a mais atual, processando cada uma:
    python main.py --all-folders --from-folder 2022-01

22. Processar dados deletando os ZIPs após extração para economizar espaço:
    python main.py --tipos empresas --delete-zips-after-extract

23. Baixar e processar dados de 2023-01 até atual, deletando ZIPs após processamento:
    python main.py --all-folders --from-folder 2023-01 --delete-zips-after-extract

24. Processar todas as pastas locais deletando ZIPs para economizar espaço:
    python main.py --step process --process-all-folders --output-subfolder economizando_espaco --delete-zips-after-extract

25. Processamento conservador de espaço - apenas estabelecimentos com deleção de ZIPs:
    python main.py --tipos estabelecimentos --delete-zips-after-extract --output-subfolder estabelecimentos_sem_zips

EXEMPLOS COM CONTROLE DE INTERFACE VISUAL:

26. Download em modo silencioso (sem barras de progresso nem lista de pendentes):
    python main.py --quiet

27. Download com interface completa (barras de progresso + lista de pendentes):
    python main.py --verbose-ui

28. Download ocultando apenas as barras de progresso:
    python main.py --hide-progress

29. Download mostrando apenas as barras de progresso (oculta lista de pendentes):
    python main.py --show-progress --hide-pending

30. Processamento em modo verboso com todas as informações visuais:
    python main.py --step process --source-zip-folder ../dados/2023-05 --output-subfolder teste --verbose-ui

31. Download de todas as pastas em modo silencioso para logs limpos:
    python main.py --all-folders --quiet

32. Processamento mostrando lista de arquivos pendentes mas sem barras de progresso:
    python main.py --tipos empresas --show-pending --hide-progress

33. Download forçado com interface mínima (apenas lista de pendentes):
    python main.py --force-download --hide-progress --show-pending

34. Processamento de múltiplas pastas em modo silencioso:
    python main.py --step process --process-all-folders --output-subfolder batch_silent --quiet

35. Download de pasta específica com barras de progresso ativadas:
    python main.py --remote-folder 2024-01 --show-progress

EXEMPLOS COM LIMPEZA DE ARQUIVOS:

36. Processar dados e criar banco DuckDB, removendo arquivos parquet após criação:
    python main.py --step all --tipos empresas --cleanup-after-db

37. Processar dados e criar banco DuckDB, removendo arquivos parquet E ZIP após criação:
    python main.py --step all --tipos empresas --cleanup-all-after-db

38. Criar banco DuckDB a partir de parquets existentes e remover os parquets:
    python main.py --step database --output-subfolder processados_2023_05 --cleanup-after-db

39. Download, processamento e banco completo com limpeza total (economiza máximo espaço):
    python main.py --all-folders --from-folder 2023-01 --cleanup-all-after-db

40. Processamento conservador com deleção de ZIPs durante extração e limpeza final:
    python main.py --tipos estabelecimentos --delete-zips-after-extract --cleanup-after-db

NOTA: O download sempre salvará os arquivos em uma subpasta com o nome da pasta remota.
      Exemplo: se --remote-folder=2023-05, os arquivos serão salvos em PATH_ZIP/2023-05/.
      Ao usar --source-zip-folder, aponte diretamente para o diretório que contém os arquivos ZIP.
      
NOVO COMPORTAMENTO:
      - --from-folder especifica a pasta inicial para download/processamento sequencial
      - Sem --from-folder + --all-folders: baixa/processa da mais antiga até a mais atual
      - --process-all-folders agora suporta --from-folder para processamento sequencial local
      - --delete-zips-after-extract deleta arquivos ZIP após extração bem-sucedida (economiza espaço)
      - --cleanup-after-db deleta arquivos parquet após criação do banco DuckDB (economiza espaço)
      - --cleanup-all-after-db deleta arquivos parquet E ZIP após criação do banco (máxima economia)
      - A deleção só ocorre após verificação de que as operações foram realizadas com sucesso

CONTROLE DE INTERFACE VISUAL:
      - --quiet (-q): Modo silencioso - desativa barras de progresso e lista de pendentes
      - --verbose-ui (-v): Modo verboso - ativa barras de progresso e lista de pendentes
      - --show-progress (-pb): Força exibição de barras de progresso
      - --hide-progress (-hp): Força ocultação de barras de progresso
      - --show-pending (-sp): Força exibição da lista de arquivos pendentes
      - --hide-pending (-hf): Força ocultação da lista de arquivos pendentes
      - Argumentos específicos têm prioridade sobre modos gerais (quiet/verbose-ui)
      - Modo silencioso tem prioridade máxima sobre todos os outros argumentos
"""
import argparse
import asyncio
import datetime
import logging
import os
from multiprocessing import freeze_support
import psutil
import re
import sys
import time
import socket
import requests
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from rich.logging import RichHandler

# CARREGAR VARIÁVEIS DE AMBIENTE ANTES DAS IMPORTAÇÕES QUE DEPENDEM DELAS
load_dotenv()

from src.async_downloader import (
    download_multiple_files, 
    get_latest_month_zip_urls, 
    get_remote_folders, 
    get_latest_remote_folder,
    _filter_urls_by_type,
    download_only_files,
    get_network_test_results
)
from src.config import config
from src.database import create_duckdb_file
# Importações da nova arquitetura refatorada (versão 3.0.0)
from src.process.base.factory import ProcessorFactory
from src.process.processors.empresa_processor import EmpresaProcessor
from src.process.processors.estabelecimento_processor import EstabelecimentoProcessor
from src.process.processors.simples_processor import SimplesProcessor
from src.process.processors.socio_processor import SocioProcessor
from src.utils import check_basic_folders
from src.utils.time_utils import format_elapsed_time
from src.utils.statistics import global_stats

# Configurar logger global
logger = logging.getLogger(__name__)

# Imports do circuit breaker
from src.utils.global_circuit_breaker import (
    circuit_breaker,
    FailureType,
    CriticalityLevel,
    should_continue_processing,
    report_critical_failure,
    report_fatal_failure,
    register_stop_callback
)

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
        bool: True se há espaço suficiente, False caso contrário
    """
    try:
        total, used, free = psutil.disk_usage("/")
        
        # Verificar se há pelo menos 5GB livres
        min_free_gb = 5
        free_gb = free / (1024**3)
        
        if free_gb < min_free_gb:
            logger.error(f"Espaço em disco insuficiente. Disponível: {free_gb:.2f}GB, Mínimo: {min_free_gb}GB")
            return False
        
        logger.info(f"Espaço em disco verificado: {free_gb:.2f}GB disponíveis")
        return True
        
    except Exception as e:
        logger.warning(f"Erro ao verificar espaço em disco: {e}")
        return True  # Assumir que está OK se não conseguir verificar

def setup_logging(log_level_str: str):
    """Configura o sistema de logging com base no nível fornecido."""
    if not os.path.exists('logs'):
        os.makedirs('logs')

    log_filename = f'logs/cnpj_process_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Comando de execução
    cmd_line = ' '.join(sys.argv)
    # Escreve o comando como primeira linha do log
    with open(log_filename, 'w', encoding='utf-8') as f:
        f.write(f"# Linha de comando: {cmd_line}\n")

    # Converte a string do argumento para o nível de log correspondente
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)

    # Configuração do logger raiz para capturar tudo
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        print("[setup_logging] Handlers de log anteriores removidos.")

    # Handler para arquivo (sem cores)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(file_handler)

    # Handler para console (com RichHandler)
    console_handler = RichHandler(rich_tracebacks=True)
    root_logger.addHandler(console_handler)

    logger_instance = logging.getLogger(__name__)
    logger_instance.info(f"Nível de log configurado para: {logging.getLevelName(log_level)}")
    logger_instance.info(f"Linha de comando: {cmd_line}")
    return logger_instance


def print_header(text: str):
    """Imprime um cabeçalho formatado."""
    print(f"\n{'=' * 50}")
    print(f"{text}")
    print(f"{'=' * 50}\n")
    # Também logar
    logger.info("=" * 50)
    logger.info(text)
    logger.info("=" * 50)


def print_section(text: str):
    """Imprime uma seção formatada."""
    print(f"\n▶ {text}")
    # Também logar
    logger.info(f"▶ {text}")


def print_success(text: str):
    """Imprime uma mensagem de sucesso formatada."""
    print(f"✓ {text}")
    # Também logar
    logger.info(f"✓ {text}")


def print_warning(text: str):
    """Imprime uma mensagem de aviso formatada."""
    print(f"⚠ {text}")
    # Também logar
    logger.warning(f"⚠ {text}")


def print_error(text: str):
    """Imprime uma mensagem de erro formatada."""
    print(f"✗ {text}")
    # Também logar
    logger.error(f"✗ {text}")


async def run_download_process(tipos_desejados: list[str] | None = None, remote_folder: str | None = None, all_folders: bool = False, from_folder: str | None = None, quiet: bool = False, verbose_ui: bool = False, show_progress: bool = False, hide_progress: bool = False, show_pending: bool = False, hide_pending: bool = False):
    """Executa todo o processo de download de forma assíncrona.
    
    Args:
        tipos_desejados: Lista de tipos de arquivo desejados (opcional)
        remote_folder: Pasta remota específica para download (opcional)
        all_folders: Se True, baixa de todas as pastas remotas
        from_folder: Pasta local para processar arquivos já baixados
        quiet: Se True, reduz output no console
        verbose_ui: Se True, mostra interface detalhada
        show_progress: Se True, força exibição da barra de progresso
        hide_progress: Se True, força ocultação da barra de progresso
        show_pending: Se True, força exibição da lista de arquivos pendentes
        hide_pending: Se True, força ocultação da lista de arquivos pendentes
    """
    try:
        # Importar e executar teste de rede adaptativo
        try:
            network_results = await get_network_test_results()
            if network_results and not network_results.get("connected"):
                logger.warning("⚠️ Teste de rede indicou problemas de conectividade")
        except Exception as e:
            logger.warning(f"⚠️ Erro no teste de rede adaptativo: {e}. Continuando sem otimizações de rede.")
            network_results = None
        
        # Se nenhum tipo foi especificado ou lista vazia, usar a lista padrão
        if not tipos_desejados:
            tipos_desejados = ['Empresas', 'Estabelecimentos', 'Simples', 'Socios']
            logger.info("Nenhum tipo especificado. Usando tipos padrão: Empresas, Estabelecimentos, Simples, Socios")

        # Obter variáveis de ambiente
        PATH_ZIP = os.getenv('PATH_ZIP', './dados-zip')
        PATH_UNZIP = os.getenv('PATH_UNZIP', './dados-unzip')
        PATH_PARQUET = os.getenv('PATH_PARQUET', './dados-parquet')

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

        # Verificando pastas básicas
        check_basic_folders([PATH_ZIP, PATH_UNZIP, PATH_PARQUET])
        
        # 🆕 Versão 3.0.0: Inicializar nova arquitetura de processadores
        print_section("Inicializando arquitetura refatorada (v3.0.0)...")
        if not initialize_processors():
            print_error("Falha ao inicializar processadores da nova arquitetura")
            return False, ""
        print_success("Arquitetura refatorada inicializada com sucesso")
        
        # Processar argumentos de interface (barra de progresso e arquivos pendentes)
        # Determinar configurações de interface baseadas nos argumentos
        show_progress_bar = config.pipeline.show_progress_bar  # Valor padrão
        show_pending_files = config.pipeline.show_pending_files  # Valor padrão
        
        # Modo silencioso tem prioridade máxima
        if quiet:
            show_progress_bar = False
            show_pending_files = False
            logger.info("🔇 Modo silencioso ativado: interface simplificada")
        
        # Modo verboso sobrescreve padrão, mas não o modo silencioso
        elif verbose_ui:
            show_progress_bar = True
            show_pending_files = True
            logger.info("📊 Modo verboso ativado: interface completa")
        
        # Argumentos específicos têm prioridade sobre modos
        else:
            if show_progress:
                show_progress_bar = True
            elif hide_progress:
                show_progress_bar = False
            
            if show_pending:
                show_pending_files = True
            elif hide_pending:
                show_pending_files = False
        
        # Sobrescrever configurações no objeto config para uso nos downloaders
        config.pipeline.show_progress_bar = show_progress_bar
        config.pipeline.show_pending_files = show_pending_files
        
        # Log das configurações finais de interface
        logger.info(f"📊 Barra de progresso: {'✅ ativada' if show_progress_bar else '❌ desativada'}")
        logger.info(f"📋 Lista de arquivos pendentes: {'✅ ativada' if show_pending_files else '❌ desativada'}")
        
        # Obter URLs base
        base_url = os.getenv('BASE_URL', 'https://dados.rfb.gov.br/CNPJ/')
        if not base_url:
            logger.error("BASE_URL não definida no arquivo .env")
            return False, ""

        # Obter pasta de download
        download_folder = os.getenv('PATH_ZIP', './dados-zip')
        if not download_folder:
            logger.error("PATH_ZIP não definido no arquivo .env")
            return False, ""

        # Lista para armazenar resultados
        downloaded_files_count = 0
        failed_downloads_count = 0
        all_successful = True
        latest_folder = ""

        if all_folders:
            # Modo: Baixar de todas as pastas
            logger.info("Modo: Baixar de todas as pastas disponíveis")
            
            # Obter lista de pastas remotas
            remote_folders = await get_remote_folders(base_url)
            if not remote_folders:
                logger.error("Não foi possível obter a lista de pastas remotas")
                return False, ""

            logger.info(f"Encontradas {len(remote_folders)} pastas remotas")
            
            # Filtrar pastas com base no from_folder se especificado
            if from_folder:
                logger.info(f"Filtrando pastas a partir de: {from_folder}")
                # Ordenar as pastas e filtrar a partir da pasta especificada
                remote_folders_sorted = sorted(remote_folders)
                if from_folder in remote_folders_sorted:
                    start_index = remote_folders_sorted.index(from_folder)
                    remote_folders = remote_folders_sorted[start_index:]
                    logger.info(f"Baixando {len(remote_folders)} pastas a partir de {from_folder}: {', '.join(remote_folders)}")
                else:
                    logger.warning(f"Pasta inicial '{from_folder}' não encontrada. Disponíveis: {', '.join(sorted(remote_folders))}")
                    logger.info("Baixando todas as pastas disponíveis")
            else:
                # Se from_folder não especificado, ordenar da mais antiga para a mais nova
                remote_folders = sorted(remote_folders)
                logger.info(f"Baixando todas as {len(remote_folders)} pastas em ordem cronológica: {', '.join(remote_folders)}")
            
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
                
                # Usar a nova função apenas para download (sem processamento)
                try:
                    downloaded, failed = await download_only_files(
                        zip_urls,
                        folder_download_path,  # path_zip
                        force_download=os.getenv('FORCE_DOWNLOAD', '').lower() == 'true',
                        max_concurrent_downloads=config.n_workers,
                        show_progress_bar=config.pipeline.show_progress_bar,
                        show_pending_files=config.pipeline.show_pending_files
                    )
                    
                    downloaded_files_count += len(downloaded)
                    failed_downloads_count += len(failed)
                    
                    if failed:
                        logger.warning(f"{len(failed)} downloads/processamentos falharam em {folder_name}.")
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

            # Usar a nova função apenas para download (sem processamento)
            downloaded, failed = await download_only_files(
                zip_urls,
                folder_download_path,  # path_zip
                force_download=os.getenv('FORCE_DOWNLOAD', '').lower() == 'true',
                max_concurrent_downloads=config.n_workers,
                show_progress_bar=config.pipeline.show_progress_bar,
                show_pending_files=config.pipeline.show_pending_files
            )
            
            downloaded_files_count = len(downloaded)
            failed_downloads_count = len(failed)
            
            if failed:
                logger.warning(f"{len(failed)} downloads/processamentos falharam.")
                all_successful = False
                    
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


def initialize_processors():
    """Inicializa todos os processadores da nova arquitetura (versão 3.0.0)."""
    try:
        # Registrar todos os processadores na factory
        ProcessorFactory.register("empresa", EmpresaProcessor)
        ProcessorFactory.register("estabelecimento", EstabelecimentoProcessor)
        ProcessorFactory.register("simples", SimplesProcessor)
        ProcessorFactory.register("socio", SocioProcessor)
        
        registered = ProcessorFactory.get_registered_processors()
        logger.info(f"✅ Processadores registrados: {', '.join(registered)}")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar processadores: {e}")
        return False

def process_with_new_architecture(processor_type: str, source_zip_path: str, unzip_path: str, 
                                 output_parquet_path: str, delete_zips_after_extract: bool = False, **options) -> bool:
    """
    Processa dados usando a nova arquitetura refatorada (versão 3.0.0).
    
    Args:
        processor_type: Tipo do processador ('empresa', 'estabelecimento', 'simples', 'socio')
        source_zip_path: Caminho dos arquivos ZIP
        unzip_path: Caminho temporário para extração
        output_parquet_path: Caminho de saída dos parquets
        delete_zips_after_extract: Se True, deleta ZIPs após extração bem-sucedida
        **options: Opções específicas do processador
    
    Returns:
        bool: True se processamento foi bem-sucedido
    """
    try:
        # Criar processador usando factory
        processor = ProcessorFactory.create(
            processor_type,
            source_zip_path,
            unzip_path,
            output_parquet_path,
            delete_zips_after_extract=delete_zips_after_extract,
            **options
        )
        
        # Encontrar arquivos ZIP relevantes
        if processor_type == "empresa":
            pattern = "Empr"
        elif processor_type == "estabelecimento":
            pattern = "Estabele"
        elif processor_type == "simples":
            pattern = "Simples"
        elif processor_type == "socio":
            pattern = "Socio"
        else:
            logger.error(f"Tipo de processador desconhecido: {processor_type}")
            return False
        
        zip_files = [f for f in os.listdir(source_zip_path) 
                    if f.startswith(pattern) and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning(f"Nenhum arquivo ZIP encontrado para {processor_type}")
            return True  # Não é erro se não há arquivos
        
        logger.info(f"Arquivos {processor_type} encontrados: {len(zip_files)}")
        
        # Processar cada arquivo ZIP
        success_count = 0
        for zip_file in zip_files:
            try:
                file_path = os.path.join(source_zip_path, zip_file)
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"Processando {zip_file}: {file_size_mb:.1f}MB")
                
                # Processar arquivo usando o novo sistema com todos os argumentos necessários
                success = processor.process_single_zip(zip_file, source_zip_path, unzip_path, output_parquet_path, **options)
                
                if success:
                    success_count += 1
                    logger.info(f"✅ {zip_file} processado com sucesso")
                else:
                    logger.error(f"❌ Falha ao processar {zip_file}")
                    
            except Exception as e:
                logger.error(f"❌ Erro ao processar {zip_file}: {e}")
        
        # Verificar se todos foram processados com sucesso
        all_success = success_count == len(zip_files)
        logger.info(f"Processamento {processor_type}: {success_count}/{len(zip_files)} arquivos processados com sucesso")
        
        return all_success
        
    except Exception as e:
        logger.error(f"❌ Erro no processamento {processor_type}: {e}")
        return False

def find_date_folders(base_path: str, from_folder: str | None = None) -> list[str]:
    """
    Encontra todas as pastas no formato AAAA-MM no diretório especificado.
    
    Args:
        base_path: Caminho base para procurar as pastas
        from_folder: Pasta inicial para filtrar (formato AAAA-MM), se None, inclui todas
        
    Returns:
        Lista de pastas no formato AAAA-MM ordenadas cronologicamente
    """
    if not os.path.exists(base_path):
        return []
    
    # Padrão regex para pastas no formato AAAA-MM
    date_pattern = re.compile(r'^\d{4}-\d{2}$')
    
    # Encontrar todas as pastas que correspondem ao padrão
    date_folders = []
    for item in os.listdir(base_path):
        item_path = os.path.join(base_path, item)
        if os.path.isdir(item_path) and date_pattern.match(item):
            date_folders.append(item)
    
    # Ordenar cronologicamente
    date_folders.sort()
    
    # Filtrar a partir da pasta especificada se fornecida
    if from_folder and from_folder in date_folders:
        start_index = date_folders.index(from_folder)
        date_folders = date_folders[start_index:]
        logger.info(f"Processando pastas a partir de {from_folder}: {', '.join(date_folders)}")
    elif from_folder:
        logger.warning(f"Pasta inicial '{from_folder}' não encontrada. Disponíveis: {', '.join(date_folders)}")
        logger.info("Processando todas as pastas encontradas")
    
    return date_folders

def process_folder(source_zip_path, unzip_path, output_parquet_path, 
                 tipos_list, criar_empresa_privada, criar_subset_uf,
                 tipos_a_processar, delete_zips_after_extract: bool = False) -> dict:
    """Processa os arquivos da pasta usando a nova arquitetura v3.0.0.
    
    Args:
        source_zip_path: Caminho para os ZIPs
        unzip_path: Caminho para extrair arquivos
        output_parquet_path: Caminho para salvar parquets
        tipos_list: Lista de tipos a serem processados
        criar_empresa_privada: Flag para criar subset empresas privadas
        criar_subset_uf: Flag para criar subset por UF
        tipos_a_processar: Lista de tipos a processar
        delete_zips_after_extract: Se True, deleta ZIPs após extração
        
    Returns:
        dict: Dicionário com o status de cada tipo e tempo de processamento
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Caminho para salvar parquets: {output_parquet_path}")
    
    # Extrair a pasta remota do caminho dos ZIPs (para usar na estrutura de diretórios)
    remote_folder = os.path.basename(os.path.normpath(source_zip_path))
    logger.info(f"Pasta remota extraída do caminho: {remote_folder}")
    
    all_ok = True
    processing_results = {}
    total_start_time = time.time()
    
    # Mostrar informações sobre recursos do sistema antes de iniciar
    cpu_count = os.cpu_count() or 4
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage(output_parquet_path)
    
    logger.info("=" * 60)
    logger.info("INFORMAÇÕES DO SISTEMA PARA PROCESSAMENTO:")
    logger.info("=" * 60)
    logger.info(f"CPUs disponíveis: {cpu_count}")
    logger.info(f"Memória total: {memory.total / (1024**3):.1f}GB")
    logger.info(f"Memória disponível: {memory.available / (1024**3):.1f}GB ({memory.percent:.1f}% em uso)")
    logger.info(f"Espaço em disco disponível: {disk.free / (1024**3):.1f}GB")
    logger.info(f"Tipos a processar: {', '.join(tipos_a_processar)}")
    logger.info("=" * 60)
    
    # Contador de estatísticas antes do processamento
    stats_before = len(global_stats.processing_stats)
    logger.info(f"Estatísticas de processamento antes: {stats_before} registros")
    
    # Processa Empresas
    if 'empresas' in tipos_a_processar:
        if 'Empresas' in tipos_list or 'empresas' in tipos_list:
            logger.info("🏢 INICIANDO PROCESSAMENTO DE EMPRESAS")
            logger.info("=" * 50)
            start_time = time.time()
            
            # Mostrar informações sobre arquivos a processar
            zip_files = [f for f in os.listdir(source_zip_path) if f.startswith('Empr') and f.endswith('.zip')]
            logger.info(f"Arquivos de empresas encontrados: {len(zip_files)}")
            for i, zip_file in enumerate(zip_files, 1):
                file_path = os.path.join(source_zip_path, zip_file)
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"  {i:2d}. {zip_file}: {file_size_mb:.1f}MB")
            
            empresas_ok = process_with_new_architecture(
                "empresa", source_zip_path, unzip_path, output_parquet_path, 
                delete_zips_after_extract=delete_zips_after_extract,
                create_private=criar_empresa_privada
            )
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            processing_results['empresas'] = {
                'success': empresas_ok,
                'time': elapsed_time
            }
            
            status_emoji = "✅" if empresas_ok else "❌"
            logger.info("=" * 50)
            logger.info(f"{status_emoji} EMPRESAS CONCLUÍDO - Tempo: {format_elapsed_time(elapsed_time)}")
            logger.info("=" * 50)
                
            if not empresas_ok:
                logger.error("Erro no processamento de empresas.")
                all_ok = False
    
    # Processa Estabelecimentos
    if 'estabelecimentos' in tipos_a_processar:
        if 'Estabelecimentos' in tipos_list or 'estabelecimentos' in tipos_list:
            logger.info("🏪 INICIANDO PROCESSAMENTO DE ESTABELECIMENTOS")
            logger.info("=" * 50)
            start_time = time.time()
            uf_subset = None
            
            # Mostrar informações sobre arquivos a processar
            zip_files = [f for f in os.listdir(source_zip_path) if f.startswith('Estabele') and f.endswith('.zip')]
            logger.info(f"Arquivos de estabelecimentos encontrados: {len(zip_files)}")
            total_size_mb = 0
            for i, zip_file in enumerate(zip_files, 1):
                file_path = os.path.join(source_zip_path, zip_file)
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                total_size_mb += file_size_mb
                logger.info(f"  {i:2d}. {zip_file}: {file_size_mb:.1f}MB")
            logger.info(f"Tamanho total dos arquivos: {total_size_mb:.1f}MB ({total_size_mb/1024:.1f}GB)")
            
            # Se criar_subset_uf foi especificado, extrai a sigla da UF
            if criar_subset_uf:
                if isinstance(criar_subset_uf, str) and len(criar_subset_uf) == 2:
                    uf_subset = criar_subset_uf.upper()
                    logger.info(f"Criando subset de estabelecimentos para UF: {uf_subset}")
                else:
                    logger.warning(f"Valor inválido para subset UF: {criar_subset_uf}. Ignorando.")
            
            estab_ok = process_with_new_architecture(
                "estabelecimento", source_zip_path, unzip_path, output_parquet_path, 
                delete_zips_after_extract=delete_zips_after_extract,
                uf_subset=uf_subset
            )
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            processing_results['estabelecimentos'] = {
                'success': estab_ok,
                'time': elapsed_time
            }
            
            status_emoji = "✅" if estab_ok else "❌"
            logger.info("=" * 50)
            logger.info(f"{status_emoji} ESTABELECIMENTOS CONCLUÍDO - Tempo: {format_elapsed_time(elapsed_time)}")
            logger.info("=" * 50)
            
            if not estab_ok:
                logger.error("Erro no processamento de estabelecimentos.")
                all_ok = False
    
    # Processa Simples Nacional
    if 'simples' in tipos_a_processar:
        if 'Simples' in tipos_list or 'simples' in tipos_list:
            logger.info("📋 INICIANDO PROCESSAMENTO DO SIMPLES NACIONAL")
            logger.info("=" * 50)
            start_time = time.time()
            
            # Mostrar informações sobre arquivos a processar
            zip_files = [f for f in os.listdir(source_zip_path) if f.startswith('Simples') and f.endswith('.zip')]
            logger.info(f"Arquivos do Simples Nacional encontrados: {len(zip_files)}")
            for i, zip_file in enumerate(zip_files, 1):
                file_path = os.path.join(source_zip_path, zip_file)
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"  {i:2d}. {zip_file}: {file_size_mb:.1f}MB")
            
            simples_ok = process_with_new_architecture(
                "simples", source_zip_path, unzip_path, output_parquet_path,
                delete_zips_after_extract=delete_zips_after_extract
            )
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            processing_results['simples'] = {
                'success': simples_ok,
                'time': elapsed_time
            }
            
            status_emoji = "✅" if simples_ok else "❌"
            logger.info("=" * 50)
            logger.info(f"{status_emoji} SIMPLES NACIONAL CONCLUÍDO - Tempo: {format_elapsed_time(elapsed_time)}")
            logger.info("=" * 50)
            
            if not simples_ok:
                logger.error("Erro no processamento do simples nacional.")
                all_ok = False
    
    # Processa Sócios
    if 'socios' in tipos_a_processar:
        if 'Socios' in tipos_list or 'socios' in tipos_list:
            logger.info("👥 INICIANDO PROCESSAMENTO DE SÓCIOS")
            logger.info("=" * 50)
            start_time = time.time()
            
            # Mostrar informações sobre arquivos a processar
            zip_files = [f for f in os.listdir(source_zip_path) if f.startswith('Socio') and f.endswith('.zip')]
            logger.info(f"Arquivos de sócios encontrados: {len(zip_files)}")
            for i, zip_file in enumerate(zip_files, 1):
                file_path = os.path.join(source_zip_path, zip_file)
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"  {i:2d}. {zip_file}: {file_size_mb:.1f}MB")
            
            socios_ok = process_with_new_architecture(
                "socio", source_zip_path, unzip_path, output_parquet_path,
                delete_zips_after_extract=delete_zips_after_extract
            )
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            processing_results['socios'] = {
                'success': socios_ok,
                'time': elapsed_time
            }
            
            status_emoji = "✅" if socios_ok else "❌"
            logger.info("=" * 50)
            logger.info(f"{status_emoji} SÓCIOS CONCLUÍDO - Tempo: {format_elapsed_time(elapsed_time)}")
            logger.info("=" * 50)
            
            if not socios_ok:
                logger.error("Erro no processamento de sócios.")
                all_ok = False
    
    # Calcular tempo total
    total_elapsed_time = time.time() - total_start_time
    processing_results['total_time'] = total_elapsed_time
    processing_results['all_ok'] = all_ok
    
    # Verificar estatísticas coletadas
    stats_after = len(global_stats.processing_stats)
    stats_collected = stats_after - stats_before
    logger.info(f"Estatísticas de processamento coletadas: {stats_collected} novos registros")
    logger.info(f"Total de estatísticas de processamento: {stats_after} registros")
    
    # Logar resumo de processamento
    logger.info("=" * 60)
    logger.info("📊 RESUMO FINAL DO PROCESSAMENTO:")
    logger.info("=" * 60)
    for tipo, resultado in processing_results.items():
        if tipo != 'total_time' and tipo != 'all_ok':
            status = "✅ SUCESSO" if resultado['success'] else "❌ FALHA"
            logger.info(f"{tipo.upper()}: {status} - Tempo: {format_elapsed_time(resultado['time'])}")
    logger.info("-" * 60)
    logger.info(f"⏱️  TEMPO DE PROCESSAMENTO DOS DADOS: {format_elapsed_time(total_elapsed_time)}")
    status_final = "✅ SUCESSO COMPLETO" if all_ok else "❌ FALHA PARCIAL/TOTAL"
    logger.info(f"🎯 STATUS GERAL: {status_final}")
    logger.info("=" * 60)
    
    return processing_results

def cleanup_after_database(parquet_folder: str, zip_folder: str = "", cleanup_parquet: bool = False, cleanup_zip: bool = False) -> bool:
    """
    Realiza limpeza de arquivos após criação bem-sucedida do banco DuckDB.
    
    Args:
        parquet_folder: Pasta contendo os arquivos parquet
        zip_folder: Pasta contendo os arquivos ZIP (opcional)
        cleanup_parquet: Se deve deletar os arquivos parquet
        cleanup_zip: Se deve deletar os arquivos ZIP
        
    Returns:
        bool: True se limpeza foi bem-sucedida, False caso contrário
    """
    success = True
    
    if not cleanup_parquet and not cleanup_zip:
        logger.debug("Nenhuma limpeza solicitada")
        return True
    
    print_section("Realizando limpeza de arquivos")
    
    try:
        # Contadores para estatísticas
        parquet_files_deleted = 0
        parquet_size_freed = 0
        zip_files_deleted = 0
        zip_size_freed = 0
        
        # Limpar arquivos parquet se solicitado
        if cleanup_parquet and os.path.exists(parquet_folder):
            logger.info(f"Iniciando limpeza de arquivos parquet em: {parquet_folder}")
            
            for root, dirs, files in os.walk(parquet_folder):
                for file in files:
                    if file.endswith('.parquet'):
                        file_path = os.path.join(root, file)
                        try:
                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            parquet_files_deleted += 1
                            parquet_size_freed += file_size
                            logger.debug(f"Arquivo parquet deletado: {file}")
                        except Exception as e:
                            logger.error(f"Erro ao deletar arquivo parquet {file}: {e}")
                            success = False
            
            # Remover diretórios vazios
            try:
                for root, dirs, files in os.walk(parquet_folder, topdown=False):
                    for dir_name in dirs:
                        dir_path = os.path.join(root, dir_name)
                        try:
                            if not os.listdir(dir_path):  # Se diretório está vazio
                                os.rmdir(dir_path)
                                logger.debug(f"Diretório vazio removido: {dir_name}")
                        except OSError:
                            pass  # Diretório não está vazio ou erro de permissão
            except Exception as e:
                logger.warning(f"Erro ao remover diretórios vazios: {e}")
        
        # Limpar arquivos ZIP se solicitado
        if cleanup_zip and zip_folder and os.path.exists(zip_folder):
            logger.info(f"Iniciando limpeza de arquivos ZIP em: {zip_folder}")
            
            for root, dirs, files in os.walk(zip_folder):
                for file in files:
                    if file.endswith('.zip'):
                        file_path = os.path.join(root, file)
                        try:
                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            zip_files_deleted += 1
                            zip_size_freed += file_size
                            logger.debug(f"Arquivo ZIP deletado: {file}")
                        except Exception as e:
                            logger.error(f"Erro ao deletar arquivo ZIP {file}: {e}")
                            success = False
        
        # Exibir estatísticas da limpeza
        total_size_freed = parquet_size_freed + zip_size_freed
        total_files_deleted = parquet_files_deleted + zip_files_deleted
        
        if total_files_deleted > 0:
            size_freed_mb = total_size_freed / (1024 * 1024)
            size_freed_gb = size_freed_mb / 1024
            
            logger.info(f"Limpeza concluída:")
            if parquet_files_deleted > 0:
                parquet_mb = parquet_size_freed / (1024 * 1024)
                logger.info(f"  - Arquivos parquet: {parquet_files_deleted} arquivos, {parquet_mb:.2f} MB liberados")
            
            if zip_files_deleted > 0:
                zip_mb = zip_size_freed / (1024 * 1024)
                logger.info(f"  - Arquivos ZIP: {zip_files_deleted} arquivos, {zip_mb:.2f} MB liberados")
            
            if size_freed_gb >= 1:
                print_success(f"Limpeza concluída: {total_files_deleted} arquivos removidos, {size_freed_gb:.2f} GB liberados")
            else:
                print_success(f"Limpeza concluída: {total_files_deleted} arquivos removidos, {size_freed_mb:.2f} MB liberados")
        else:
            print_warning("Nenhum arquivo foi removido durante a limpeza")
        
        return success
        
    except Exception as e:
        logger.error(f"Erro durante limpeza de arquivos: {e}")
        print_error(f"Erro durante limpeza: {e}")
        return False

def main():
    """Função principal de execução."""
    start_time = time.time()  # Inicialização da variável aqui
    
    # Inicializar coleta de estatísticas
    global_stats.start_session()
    
    # Inicializar variáveis de controle que podem ser usadas no retorno
    overall_success = True
    date_folders = []
    latest_folder = ""
    
    # Registrar callbacks de parada de emergência para componentes assíncronos
    async_downloads_stop_flag = {'stop': False}
    
    def emergency_stop_main():
        """Callback de parada de emergência para o main."""
        logger.critical("🚨 PARADA DE EMERGÊNCIA ACIONADA NO MAIN!")
        async_downloads_stop_flag['stop'] = True
        # Aqui podemos adicionar mais lógica de parada se necessário
    
    # Registrar callback no circuit breaker
    register_stop_callback(emergency_stop_main)
    
    parser = argparse.ArgumentParser(description='Realizar download e processamento dos dados de CNPJ')
    parser.add_argument('--tipos', '-t', nargs='+', choices=['empresas', 'estabelecimentos', 'simples', 'socios'],
                        default=[], help='Tipos de dados a serem processados. Se não especificado, processa todos (relevante para steps \'process\' e \'all\').')
    parser.add_argument('--source-zip-folder', '-z', type=str, default='',
                        help='Caminho para o diretório contendo os arquivos ZIP ou suas subpastas. No modo "all", usa automaticamente a subpasta com nome da pasta remota dentro de PATH_ZIP.')
    parser.add_argument('--output-subfolder', '-o', type=str, default='',
                        help='Nome da subpasta dentro de PATH_PARQUET onde os arquivos Parquet serão salvos ou lidos (Obrigatório para --step process e --step database).')
    parser.add_argument('--criar-empresa-privada', '-priv', action='store_true',
                        help='Se presente (com --step process ou --step all), cria um subconjunto Parquet adicional para empresas privadas.')
    parser.add_argument('--log-level', '-l', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO',
                        help='Define o nível mínimo de log a ser exibido/salvo. Padrão: INFO.')
    parser.add_argument(
        '--criar-subset-uf', '-uf',
        type=str,
        default='',
        metavar='UF',
        help="Opcional (com --step process ou --step all). Cria um subconjunto Parquet adicional para estabelecimentos da UF especificada."
    )
    parser.add_argument(
        '--remote-folder', '-rf',
        type=str,
        default='',
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
        '--from-folder', '-ff',
        type=str,
        default='',
        metavar='PASTA_INICIAL',
        help="Opcional (com --step download ou --step all). Especifica a pasta inicial para download sequencial no formato AAAA-MM (ex: 2023-01). Se não especificado com --all-folders, baixa desde a pasta mais antiga. Baixa da pasta especificada até a mais atual."
    )
    parser.add_argument(
        '--delete-zips-after-extract', '-dz',
        action='store_true',
        help="Se presente, deleta os arquivos ZIP após extração bem-sucedida para economizar espaço em disco. Use com cautela!"
    )
    parser.add_argument(
        '--cleanup-after-db', '-cdb',
        action='store_true',
        help="Se presente, deleta os arquivos parquet após criação bem-sucedida do banco DuckDB. Use com cautela!"
    )
    parser.add_argument(
        '--cleanup-all-after-db', '-cadb',
        action='store_true',
        help="Se presente, deleta os arquivos parquet E os arquivos ZIP após criação bem-sucedida do banco DuckDB. Implica --cleanup-after-db. Use com extrema cautela!"
    )
    parser.add_argument(
        '--show-progress', '-pb',
        action='store_true',
        default=None,
        help="Exibe barras de progresso visuais durante downloads e processamento. Sobrescreve configuração padrão."
    )
    parser.add_argument(
        '--hide-progress', '-hp',
        action='store_true',
        help="Oculta barras de progresso visuais. Útil para logs limpos ou execução em background."
    )
    parser.add_argument(
        '--show-pending', '-sp',
        action='store_true',
        default=None,
        help="Exibe lista de arquivos pendentes/em progresso. Sobrescreve configuração padrão."
    )
    parser.add_argument(
        '--hide-pending', '-hf',
        action='store_true',
        help="Oculta lista de arquivos pendentes. Útil para logs mais limpos."
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help="Modo silencioso: desativa tanto barra de progresso quanto lista de arquivos pendentes."
    )
    parser.add_argument(
        '--verbose-ui', '-v',
        action='store_true',
        help="Modo verboso: ativa tanto barra de progresso quanto lista de arquivos pendentes."
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

    PATH_ZIP = os.getenv('PATH_ZIP', './dados-zip')
    PATH_UNZIP = os.getenv('PATH_UNZIP', './dados-unzip') 
    PATH_PARQUET = os.getenv('PATH_PARQUET', './dados-parquet')
    FILE_DB_PARQUET = os.getenv('FILE_DB_PARQUET', 'cnpj.duckdb')
    PATH_REMOTE_PARQUET = os.getenv('PATH_REMOTE_PARQUET', '')
    
    # Garantir que PATH_UNZIP e PATH_PARQUET estão definidos
    if not PATH_UNZIP:
        PATH_UNZIP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados-unzip")
        os.environ['PATH_UNZIP'] = PATH_UNZIP
        logger.info(f"Variável PATH_UNZIP não encontrada no arquivo .env. Usando valor padrão: {PATH_UNZIP}")
    
    if not PATH_PARQUET:
        PATH_PARQUET = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados-parquet")
        os.environ['PATH_PARQUET'] = PATH_PARQUET
        logger.info(f"Variável PATH_PARQUET não encontrada no arquivo .env. Usando valor padrão: {PATH_PARQUET}")
    
    # Garantir que PATH_ZIP está definido
    if not PATH_ZIP:
        PATH_ZIP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados-zip")
        os.environ['PATH_ZIP'] = PATH_ZIP
        logger.info(f"Variável PATH_ZIP não encontrada no arquivo .env. Usando valor padrão: {PATH_ZIP}")
    
    # Garantir que FILE_DB_PARQUET está definido
    if not FILE_DB_PARQUET:
        FILE_DB_PARQUET = "cnpj.duckdb"
        logger.info(f"Variável FILE_DB_PARQUET não encontrada no arquivo .env. Usando valor padrão: {FILE_DB_PARQUET}")
        
    # Garantir que PATH_REMOTE_PARQUET está definido
    if not PATH_REMOTE_PARQUET:
        PATH_REMOTE_PARQUET = ""
        logger.info(f"Variável PATH_REMOTE_PARQUET não encontrada no arquivo .env. Usando valor padrão vazio")
    
    # Mostrar valores para debug
    logger.info(f"PATH_ZIP = {PATH_ZIP}")
    logger.info(f"PATH_UNZIP = {PATH_UNZIP}")
    logger.info(f"PATH_PARQUET = {PATH_PARQUET}")
    logger.info(f"FILE_DB_PARQUET = {FILE_DB_PARQUET}")
    logger.info(f"PATH_REMOTE_PARQUET = {PATH_REMOTE_PARQUET}")

    # Verificando pastas básicas
    check_basic_folders([PATH_ZIP, PATH_UNZIP, PATH_PARQUET])
    
    # 🆕 Versão 3.0.0: Inicializar nova arquitetura de processadores
    print_section("Inicializando arquitetura refatorada (v3.0.0)...")
    if not initialize_processors():
        print_error("Falha ao inicializar processadores da nova arquitetura")
        return False, ""
    print_success("Arquitetura refatorada inicializada com sucesso")
    
    # Inicializar variáveis para controle de escopo
    download_time = 0.0
    process_time = 0.0
    db_time = 0.0
    
    # Determinar o modo de processamento
    if args.step == 'process':
        print_header("Modo de Processamento")
        
        # Verificar se --process-all-folders foi especificado
        if args.process_all_folders:
            print_section("Processando todas as pastas no formato AAAA-MM")
            
            # Verificar se output-subfolder foi especificado
            if not args.output_subfolder:
                logger.error("Parâmetro --output-subfolder é obrigatório para --process-all-folders")
                print_error("Especifique a subpasta base de saída com --output-subfolder")
                return False, ""
            
            # Encontrar todas as pastas de data no PATH_ZIP
            from_folder_param = args.from_folder if args.from_folder else None
            date_folders = find_date_folders(PATH_ZIP, from_folder_param)
            
            if not date_folders:
                if args.from_folder:
                    print_error(f"Nenhuma pasta no formato AAAA-MM encontrada a partir de {args.from_folder} em {PATH_ZIP}")
                else:
                    print_error(f"Nenhuma pasta no formato AAAA-MM encontrada em {PATH_ZIP}")
                return False, ""
            
            logger.info(f"Encontradas {len(date_folders)} pastas para processamento: {', '.join(date_folders)}")
            
            # Processar cada pasta sequencialmente
            overall_success = True
            total_process_time = 0
            
            for folder_name in date_folders:
                print_section(f"Processando pasta: {folder_name}")
                
                source_folder_path = os.path.join(PATH_ZIP, folder_name)
                
                if not os.path.exists(source_folder_path):
                    logger.warning(f"Pasta não encontrada: {source_folder_path}, pulando...")
                    continue
                
                # Verificar se há arquivos ZIP na pasta
                zip_files = [f for f in os.listdir(source_folder_path) if f.endswith('.zip')]
                if not zip_files:
                    logger.warning(f"Nenhum arquivo ZIP encontrado em {source_folder_path}, pulando...")
                    continue
                
                # Criar subpasta de saída específica para esta pasta
                output_subfolder_name = f"{args.output_subfolder}_{folder_name}"
                output_parquet_path = os.path.join(PATH_PARQUET, output_subfolder_name)
                os.makedirs(output_parquet_path, exist_ok=True)
                
                logger.info(f"Processando arquivos de: {source_folder_path}")
                logger.info(f"Salvando Parquets em: {output_parquet_path}")
                
                # Definir tipos a processar (todos ou filtrados)
                tipos_a_processar = args.tipos if args.tipos else ['empresas', 'estabelecimentos', 'simples', 'socios']
                
                # Processar esta pasta
                folder_start_time = time.time()
                process_results = process_folder(
                    source_folder_path, PATH_UNZIP, output_parquet_path, 
                    args.tipos if args.tipos else ['empresas', 'estabelecimentos', 'simples', 'socios'],
                    args.criar_empresa_privada, args.criar_subset_uf or '',
                    tipos_a_processar, args.delete_zips_after_extract
                )
                folder_time = time.time() - folder_start_time
                total_process_time += folder_time
                
                if process_results['all_ok']:
                    print_success(f"Pasta {folder_name} processada com sucesso em {format_elapsed_time(folder_time)}")
                else:
                    print_warning(f"Alguns erros ocorreram ao processar pasta {folder_name}")
                    overall_success = False
            
            # Resumo final do processamento de múltiplas pastas
            logger.info("=" * 60)
            logger.info("RESUMO DO PROCESSAMENTO DE MÚLTIPLAS PASTAS:")
            logger.info("=" * 60)
            logger.info(f"Pastas processadas: {len(date_folders)}")
            logger.info(f"Tempo total de processamento: {format_elapsed_time(total_process_time)}")
            logger.info(f"Status geral: {'✅ SUCESSO' if overall_success else '❌ FALHAS DETECTADAS'}")
            logger.info("=" * 60)
            
            return overall_success, date_folders[-1] if date_folders else ""
        
        # Verificar se temos o source-zip-folder para processamento de pasta única
        if args.source_zip_folder:
            # Processa a pasta de origem especificada
            logger.info(f"Processando pasta de origem: {args.source_zip_folder}")
            source_folder_path = args.source_zip_folder
            
            if not os.path.exists(source_folder_path):
                logger.error(f"Pasta de origem especificada não existe: {source_folder_path}")
                print_error(f"Pasta {args.source_zip_folder} não encontrada")
                return False, ""
            
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
                args.criar_empresa_privada, args.criar_subset_uf or '',
                tipos_a_processar, args.delete_zips_after_extract
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
            return False, ""
    
    # Resto do código do step 'download'
    elif args.step == 'download':
        print_header("Modo de Download")
        
        # Verificar se aplicação deve continuar antes de iniciar download
        if not should_continue_processing():
            logger.critical("🛑 Download cancelado pelo circuit breaker")
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return False, ""
        
        # Verificar conectividade antes de iniciar
        if not check_internet_connection():
            report_critical_failure(
                FailureType.CONNECTIVITY,
                "Sem conexão com a internet para download",
                "MAIN_DOWNLOAD"
            )
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return False, ""
        
        # Configurar para forçar o download se necessário
        if args.force_download:
            # Definir variável de ambiente para o módulo async_downloader
            os.environ['FORCE_DOWNLOAD'] = 'True'
            logger.info("Download forçado ativado: sobrescreverá arquivos existentes.")
        
        # Iniciar o download assíncrono
        tipos_desejados = args.tipos if args.tipos else []
        remote_folder_param = args.remote_folder if args.remote_folder else None
        from_folder_param = args.from_folder if args.from_folder else None
        
        download_ok, latest_folder = asyncio.run(run_download_process(
            tipos_desejados=tipos_desejados,
            remote_folder=remote_folder_param,
            all_folders=args.all_folders,
            from_folder=from_folder_param,
            quiet=args.quiet,
            verbose_ui=args.verbose_ui,
            show_progress=args.show_progress,
            hide_progress=args.hide_progress,
            show_pending=args.show_pending,
            hide_pending=args.hide_pending
        ))
        
        if not download_ok:
            print_error("Falha no processo de download. Verifique os logs para mais detalhes.")
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return False, ""
        
        if not latest_folder:
            print_error("Erro: não foi possível determinar a pasta de download para processamento")
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return False, ""
        
        print_success(f"Download concluído. Arquivos salvos em: {os.path.join(PATH_ZIP, latest_folder)}")
    
    # Código para o step 'database'
    elif args.step == 'database':
        print_header("Modo de Criação de Banco de Dados")
        
        # Verificar se temos a subpasta de saída especificada
        if not args.output_subfolder:
            logger.error("Parâmetro --output-subfolder é obrigatório para o step 'database'.")
            print_error("Especifique a subpasta dos Parquets com --output-subfolder")
            return False, ""
        
        # Caminho completo para a pasta de parquets
        parquet_folder = os.path.join(PATH_PARQUET, args.output_subfolder)
        if not os.path.exists(parquet_folder):
            logger.error(f"Pasta de Parquets não encontrada: {parquet_folder}")
            print_error(f"Pasta {parquet_folder} não existe. Execute o processamento primeiro.")
            return False, ""
        
        # Criar o arquivo DuckDB
        try:
            logger.info(f"Criando arquivo DuckDB em: {parquet_folder}")
            db_success = create_duckdb_file(parquet_folder, FILE_DB_PARQUET, PATH_REMOTE_PARQUET)
            if db_success:
                db_file = os.path.join(parquet_folder, FILE_DB_PARQUET)
                print_success(f"Banco de dados DuckDB criado com sucesso em: {db_file}")
                
                # Realizar limpeza se solicitada
                if args.cleanup_after_db or args.cleanup_all_after_db:
                    # Determinar se deve limpar ZIP também
                    cleanup_zip = args.cleanup_all_after_db
                    
                    # Se não foi especificado um zip_folder específico, usar o padrão
                    zip_folder = ""
                    if cleanup_zip:
                        # Tentar determinar a pasta ZIP correspondente
                        # No modo database, não sabemos exatamente qual pasta ZIP foi usada
                        # então vamos avisar que a limpeza de ZIP não é recomendada neste modo
                        logger.warning("Limpeza de ZIP no modo 'database' não é recomendada pois não sabemos qual pasta ZIP foi usada originalmente")
                        print_warning("Limpeza de ZIP ignorada no modo 'database' - use no modo 'all' para limpeza completa")
                        cleanup_zip = False
                    
                    cleanup_success = cleanup_after_database(
                        parquet_folder=parquet_folder,
                        zip_folder=zip_folder,
                        cleanup_parquet=True,  # Sempre limpar parquet se foi solicitado
                        cleanup_zip=cleanup_zip
                    )
                    
                    if not cleanup_success:
                        print_warning("Alguns arquivos podem não ter sido removidos durante a limpeza")
                
            else:
                print_error("Falha ao criar banco de dados. Verifique os logs para mais detalhes.")
                logger.error("Criação do banco de dados falhou")
                return False, ""
        except Exception as e:
            logger.exception(f"Erro ao criar banco de dados: {e}")
            print_error(f"Falha ao criar banco de dados: {str(e)}")
            return False, ""
    
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
        tipos_desejados = args.tipos if args.tipos else []
        remote_folder_param = args.remote_folder if args.remote_folder else None
        from_folder_param = args.from_folder if args.from_folder else None
        
        download_ok, latest_folder = asyncio.run(run_download_process(
            tipos_desejados=tipos_desejados,
            remote_folder=remote_folder_param,
            all_folders=args.all_folders,
            from_folder=from_folder_param,
            quiet=args.quiet,
            verbose_ui=args.verbose_ui,
            show_progress=args.show_progress,
            hide_progress=args.hide_progress,
            show_pending=args.show_pending,
            hide_pending=args.hide_pending
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
            return False, ""
        
        if not latest_folder:
            print_error("Erro: não foi possível determinar a pasta de download para processamento")
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return False, ""
        
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
            args.criar_empresa_privada, args.criar_subset_uf or '',
            tipos_a_processar, args.delete_zips_after_extract
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
            return False, ""
        else:
            print_success("Processamento concluído com sucesso.")
        
        # 3. Criação do banco de dados
        print_section("Etapa 3: Criação do banco de dados DuckDB")
        db_start_time = time.time()
        
        try:
            logger.info(f"Criando arquivo DuckDB em: {output_parquet_path}")
            db_success = create_duckdb_file(output_parquet_path, FILE_DB_PARQUET, PATH_REMOTE_PARQUET)
            db_time = time.time() - db_start_time
            
            if db_success:
                logger.info("=" * 50)
                logger.info(f"Tempo de processamento do banco: {format_elapsed_time(db_time)}")
                db_file = os.path.join(output_parquet_path, FILE_DB_PARQUET)
                print_success(f"Banco de dados DuckDB criado com sucesso em: {db_file}")
                
                # Realizar limpeza se solicitada
                if args.cleanup_after_db or args.cleanup_all_after_db:
                    cleanup_zip = args.cleanup_all_after_db
                    
                    cleanup_success = cleanup_after_database(
                        parquet_folder=output_parquet_path,
                        zip_folder=source_zip_path if cleanup_zip else "",
                        cleanup_parquet=True,  # Sempre limpar parquet se foi solicitado
                        cleanup_zip=cleanup_zip
                    )
                    
                    if not cleanup_success:
                        print_warning("Alguns arquivos podem não ter sido removidos durante a limpeza")
                
            else:
                logger.info("=" * 50)
                logger.info(f"Tempo de processamento do banco (falhou): {format_elapsed_time(db_time)}")
                print_error("Falha ao criar banco de dados. Verifique os logs para mais detalhes.")
                logger.error("Criação do banco de dados falhou")
                total_time = time.time() - start_time
                logger.info("=" * 50)
                logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
                logger.info("STATUS FINAL: FALHA")
                logger.info("=" * 50)
                return False, ""
        except Exception as e:
            db_time = time.time() - db_start_time
            logger.exception(f"Erro ao criar banco de dados: {e}")
            logger.info("=" * 50)
            logger.info(f"Tempo de processamento do banco (erro): {format_elapsed_time(db_time)}")
            print_error(f"Falha ao criar banco de dados: {str(e)}")
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return False, ""
    
    total_time = time.time() - start_time
    
    # Finalizar coleta de estatísticas
    global_stats.end_session()
    
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
    
    # Exibir relatório detalhado de estatísticas
    global_stats.print_detailed_report()
    
    # Salvar estatísticas em arquivo
    try:
        stats_filename = f"estatisticas_cnpj_{time.strftime('%Y%m%d_%H%M%S')}.json"
        stats_path = os.path.join("logs", stats_filename)
        os.makedirs("logs", exist_ok=True)
        global_stats.save_to_json(stats_path)
        print(f"\n📄 Estatísticas detalhadas salvas em: {stats_path}")
        logger.info(f"📄 Estatísticas detalhadas salvas em: {stats_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar estatísticas: {e}")
    
    return overall_success, latest_folder


if __name__ == '__main__':
    main()
