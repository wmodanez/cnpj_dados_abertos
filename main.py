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
import signal
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

from typing import List, Tuple

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
        disk_usage = psutil.disk_usage("/")
        
        # Verificar se há pelo menos 5GB livres
        min_free_gb = 5
        free_gb = disk_usage.free / (1024**3)
        
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

async def optimized_download_and_process_pipeline(
    urls: List[str], 
    source_zip_path: str, 
    unzip_path: str, 
    output_parquet_path: str,
    tipos_a_processar: List[str],
    delete_zips_after_extract: bool = False,
    force_download: bool = False,
    **processing_options
) -> dict:
    """
    Pipeline otimizado que inicia o processamento assim que cada arquivo é verificado/baixado.
    
    Args:
        urls: Lista de URLs para download
        source_zip_path: Diretório onde salvar os arquivos baixados
        unzip_path: Diretório temporário para extração
        output_parquet_path: Diretório de saída dos arquivos Parquet
        tipos_a_processar: Lista de tipos a processar
        delete_zips_after_extract: Se True, deleta ZIPs após extração
        force_download: Se True, força o download mesmo que o arquivo já exista
        **processing_options: Opções específicas de processamento
        
    Returns:
        dict: Resultado do processamento com informações de cada tipo
    """
    import asyncio
    import aiohttp
    import time
    import os
    from src.process.base.factory import ProcessorFactory
    from src.async_downloader import download_file, get_network_test_results
    
    logger.info("🚀 Iniciando pipeline otimizado de download e processamento")
    
    # Inicializar processadores
    processors = {}
    processing_results = {}
    
    for tipo in tipos_a_processar:
        processor_key = {
            'empresas': 'empresa',
            'estabelecimentos': 'estabelecimento', 
            'simples': 'simples',
            'socios': 'socio'
        }.get(tipo.lower())
        
        if processor_key:
            try:
                processor = ProcessorFactory.create(
                    processor_key,
                    source_zip_path,
                    unzip_path, 
                    output_parquet_path,
                    delete_zips_after_extract=delete_zips_after_extract,
                    **processing_options
                )
                processors[processor_key] = processor
                processing_results[tipo] = {'success': False, 'time': 0, 'files_processed': 0}
                logger.info(f"✅ Processador '{processor_key}' criado com sucesso")
            except Exception as e:
                logger.error(f"❌ Erro ao criar processador '{processor_key}': {e}")
                processing_results[tipo] = {'success': False, 'time': 0, 'error': str(e)}
    
    # Obter configurações de rede
    network_results = await get_network_test_results()
    max_concurrent_downloads = min(6, network_results["recommendations"]["max_concurrent_downloads"])
    
    logger.info(f"🌐 Rede: {network_results['quality']['connection_quality']}")
    logger.info(f"🔧 Downloads simultâneos: {max_concurrent_downloads}")
    
    # Configurar semáforos
    download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
    
    # Listas para rastreamento
    successful_downloads = []
    failed_downloads = []
    processed_files = {}
    
    # Função para processar arquivo imediatamente após download/verificação
    async def process_file_immediately(file_path: str, filename: str) -> bool:
        """Processa um arquivo assim que ele está disponível."""
        # Determinar tipo do processador baseado no nome do arquivo
        processor_key = None
        tipo_original = None
        
        if filename.startswith('Empr'):
            processor_key = 'empresa'
            tipo_original = 'empresas'
        elif filename.startswith('Estabele'):
            processor_key = 'estabelecimento'
            tipo_original = 'estabelecimentos'
        elif filename.startswith('Simples'):
            processor_key = 'simples'
            tipo_original = 'simples'
        elif filename.startswith('Socio'):
            processor_key = 'socio'
            tipo_original = 'socios'
        
        if not processor_key or processor_key not in processors:
            logger.warning(f"⚠️ Processador não encontrado para {filename}")
            return False
        
        try:
            start_time = time.time()
            logger.info(f"🔄 Iniciando processamento de {filename}")
            
            processor = processors[processor_key]
            success = processor.process_single_zip(filename, source_zip_path, unzip_path, output_parquet_path)
            
            elapsed_time = time.time() - start_time
            
            if success:
                logger.info(f"✅ {filename} processado com sucesso em {elapsed_time:.1f}s")
                if processor_key not in processed_files:
                    processed_files[processor_key] = []
                processed_files[processor_key].append(filename)
                
                # Atualizar resultados
                processing_results[tipo_original]['files_processed'] += 1
                return True
            else:
                logger.error(f"❌ Falha ao processar {filename}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro no processamento de {filename}: {e}")
            return False
    
    # Função para verificar/baixar e processar imediatamente
    async def download_and_process_immediately(url: str, session: aiohttp.ClientSession):
        """Baixa/verifica um arquivo e o processa imediatamente."""
        filename = os.path.basename(url)
        destination_path = os.path.join(source_zip_path, filename)
        
        try:
            async with download_semaphore:
                # Verificar se arquivo já existe
                if os.path.exists(destination_path) and not force_download:
                    logger.info(f"✅ Arquivo {filename} já existe. Processando imediatamente...")
                    successful_downloads.append(destination_path)
                    
                    # Processar imediatamente
                    await process_file_immediately(destination_path, filename)
                    return
                
                # Fazer download diretamente sem usar download_file problemático
                logger.info(f"📥 Baixando {filename}...")
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            with open(destination_path, 'wb') as f:
                                async for chunk in response.content.iter_chunked(8192):
                                    f.write(chunk)
                            logger.info(f"✅ Download de {filename} concluído")
                            successful_downloads.append(destination_path)
                            
                            # Processar imediatamente após download
                            await process_file_immediately(destination_path, filename)
                        else:
                            error_msg = f"HTTP {response.status}"
                            logger.error(f"❌ Erro no download de {filename}: {error_msg}")
                            failed_downloads.append((filename, error_msg))
                except Exception as download_error:
                    logger.error(f"❌ Erro no download de {filename}: {download_error}")
                    failed_downloads.append((filename, str(download_error)))
            
        except Exception as e:
            logger.error(f"❌ Erro inesperado com {filename}: {e}")
            failed_downloads.append((filename, e))
    
    # Executar downloads e processamentos em paralelo
    start_time = time.time()
    
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=3600, connect=30),
        connector=aiohttp.TCPConnector(limit=100, limit_per_host=20)
    ) as session:
        
        # Criar tasks para todos os URLs
        tasks = [download_and_process_immediately(url, session) for url in urls]
        
        logger.info(f"🚀 Iniciando pipeline com {len(tasks)} arquivos...")
        logger.info("📊 Cada arquivo será processado assim que for verificado/baixado")
        
        # Executar todas as tasks em paralelo
        await asyncio.gather(*tasks, return_exceptions=True)
    
    total_time = time.time() - start_time
    
    # Calcular estatísticas finais
    for tipo in tipos_a_processar:
        processor_key = {
            'empresas': 'empresa',
            'estabelecimentos': 'estabelecimento', 
            'simples': 'simples',
            'socios': 'socio'
        }.get(tipo.lower())
        
        if processor_key in processed_files:
            files_count = len(processed_files[processor_key])
            processing_results[tipo]['success'] = files_count > 0
            processing_results[tipo]['files_processed'] = files_count
        
        processing_results[tipo]['time'] = total_time
    
    # Log do resumo
    logger.info("=" * 60)
    logger.info("📊 RESUMO DO PIPELINE OTIMIZADO:")
    logger.info("=" * 60)
    logger.info(f"⏱️  Tempo total: {format_elapsed_time(total_time)}")
    logger.info(f"📥 Downloads bem-sucedidos: {len(successful_downloads)}")
    logger.info(f"❌ Downloads com falha: {len(failed_downloads)}")
    
    for tipo, result in processing_results.items():
        if isinstance(result, dict) and 'success' in result:
            status = "✅ SUCESSO" if result['success'] else "❌ FALHA"
            files_processed = result.get('files_processed', 0)
            logger.info(f"{tipo.upper()}: {status} - {files_processed} arquivos processados")
    
    logger.info("=" * 60)
    
    # Determinar sucesso geral
    all_success = all(result.get('success', False) for result in processing_results.values() if isinstance(result, dict) and 'success' in result)
    processing_results['all_ok'] = all_success
    processing_results['total_time'] = total_time
    processing_results['downloads_successful'] = len(successful_downloads)
    processing_results['downloads_failed'] = len(failed_downloads)
    
    return processing_results

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
    return asyncio.run(async_main())

async def async_main():
    """Função principal assíncrona de execução."""
    global overall_success
    overall_success = True
    
    start_time = time.time()  # Definir start_time no início
    
    # Inicializar variáveis de tempo para evitar erros
    download_time = 0.0
    process_time = 0.0
    db_time = 0.0
    latest_folder = ""
    
    # Parser de argumentos
    parser = argparse.ArgumentParser(
        description="Sistema de Processamento de Dados CNPJ v3.0.0"
    )
    
    parser.add_argument('--tipos', '-t', nargs='+', choices=['empresas', 'estabelecimentos', 'simples', 'socios'],
                         default=[], help='Tipos de dados a serem processados. Se não especificado, processa todos (relevante para steps \'process\' e \'all\').')
    parser.add_argument('--step', choices=['download', 'process', 'database', 'all'], default='all',
                         help='Etapa a ser executada. Padrão: all')
    parser.add_argument('--quiet', '-q', action='store_true',
                         help='Modo silencioso - reduz drasticamente as saídas no console')
    parser.add_argument('--verbose-ui', '-v', action='store_true',
                         help='Interface visual mais completa - só funciona com UI interativo')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                         help='Nível de logging. Padrão: INFO')
    parser.add_argument('--remote-folder', type=str, 
                         help='Usar uma pasta específica do servidor (formato AAAA-MM). Exemplo: 2024-05')
    parser.add_argument('--all-folders', action='store_true',
                         help='Baixar de todas as pastas disponíveis do servidor. Sobrescreve --remote-folder')
    parser.add_argument('--from-folder', type=str,
                         help='Iniciar download/processamento a partir de uma pasta específica (formato AAAA-MM)')
    parser.add_argument('--force-download', action='store_true',
                         help='Forçar download mesmo que arquivo já exista')
    parser.add_argument('--criar-empresa-privada', action='store_true',
                         help='Criar subconjunto de empresas privadas (apenas para empresas)')
    parser.add_argument('--criar-subset-uf', type=str, metavar='UF',
                         help='Criar subconjunto por UF (apenas para estabelecimentos). Ex: --criar-subset-uf SP')
    parser.add_argument('--output-subfolder', type=str,
                         help='Nome da subpasta onde salvar os arquivos parquet')
    parser.add_argument('--source-zip-folder', type=str,
                         help='Pasta de origem dos arquivos ZIP (para step \'process\')')
    parser.add_argument('--process-all-folders', action='store_true',
                         help='Processar todas as pastas de data (formato AAAA-MM) em PATH_ZIP')
    parser.add_argument('--delete-zips-after-extract', action='store_true',
                         help='Deletar arquivos ZIP após extração bem-sucedida (economiza espaço)')
    parser.add_argument('--cleanup-after-db', action='store_true',
                         help='Deletar arquivos parquet após criação do banco DuckDB (economiza espaço)')
    parser.add_argument('--cleanup-all-after-db', action='store_true',
                         help='Deletar arquivos parquet E ZIP após criação do banco (máxima economia)')
    parser.add_argument('--show-progress', action='store_true',
                         help='Forçar exibição da barra de progresso (sobrescreve config)')
    parser.add_argument('--hide-progress', action='store_true',
                         help='Forçar ocultação da barra de progresso (sobrescreve config)')
    parser.add_argument('--show-pending', action='store_true',
                         help='Forçar exibição da lista de arquivos pendentes (sobrescreve config)')
    parser.add_argument('--hide-pending', action='store_true',
                         help='Forçar ocultação da lista de arquivos pendentes (sobrescreve config)')

    args = parser.parse_args()
    
    # Configurar logging
    logger = setup_logging(args.log_level)
    
    # Configurar manipulador de sinal de emergência
    def emergency_stop_main():
        """Manipulador de emergência para sinais críticos."""
        print("\n🛑 SINAL DE EMERGÊNCIA RECEBIDO!")
        print("⚠️ Interrompendo execução...")
        logger.critical("🛑 Execução interrompida por sinal de emergência")
        global overall_success
        overall_success = False
        sys.exit(1)

    # Registrar manipulador de sinal
    signal.signal(signal.SIGINT, lambda s, f: emergency_stop_main())
    signal.signal(signal.SIGTERM, lambda s, f: emergency_stop_main())
    
    # Carregar variáveis de ambiente
    load_dotenv()
    print_header("Carregando variáveis de ambiente...")
    PATH_ZIP = os.getenv('PATH_ZIP', './dados-zip')
    PATH_UNZIP = os.getenv('PATH_UNZIP', './dados-unzip')
    PATH_PARQUET = os.getenv('PATH_PARQUET', './dados-parquet')
    FILE_DB_PARQUET = os.getenv('FILE_DB_PARQUET', 'cnpj.duckdb')
    PATH_REMOTE_PARQUET = os.getenv('PATH_REMOTE_PARQUET', 'destino/')
    
    if PATH_ZIP and PATH_UNZIP and PATH_PARQUET:
        print_success("Variáveis de ambiente carregadas com sucesso")
        logger.info(f"PATH_ZIP = {PATH_ZIP}")
        logger.info(f"PATH_UNZIP = {PATH_UNZIP}")
        logger.info(f"PATH_PARQUET = {PATH_PARQUET}")
        logger.info(f"FILE_DB_PARQUET = {FILE_DB_PARQUET}")
        logger.info(f"PATH_REMOTE_PARQUET = {PATH_REMOTE_PARQUET}")
    else:
        print_error("Erro ao carregar variáveis de ambiente. Verifique o arquivo .env")
        logger.error("Variáveis de ambiente PATH_ZIP, PATH_UNZIP ou PATH_PARQUET não definidas")
        return False, ""
    
    if not initialize_processors():
        print_error("Falha ao inicializar nova arquitetura. Verifique os logs.")
        return False, ""
    print_success("Arquitetura refatorada inicializada com sucesso")
    
    # Verificar conectividade de rede antes de qualquer operação
    if not check_internet_connection():
        logger.warning("⚠️ Sem conectividade de rede. Algumas funcionalidades podem estar limitadas.")
        report_critical_failure(
            FailureType.CONNECTIVITY,
            "Sem conexão com a internet",
            "MAIN_CONNECTIVITY"
        )
    
    # Verificar espaço em disco
    if not check_disk_space():
        logger.warning("⚠️ Espaço em disco limitado. Monitorando recursos durante execução.")
    
    # Inicializar sistema de estatísticas
    global_stats.start_session()
    
    # Se chegou até aqui após processamento bem-sucedido, usar pipeline otimizado
    if args.step == 'all':
        print_header("Modo Completo: Download -> Processamento Otimizado -> Banco de Dados")
        
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
        
        download_ok, latest_folder = await run_download_process(
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
        )
        
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
        
        # 2. Processamento com Pipeline Otimizado
        print_section("Etapa 2: Processamento Otimizado dos arquivos")
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
        
        # 🆕 Usar o novo pipeline otimizado que processa arquivos já existentes imediatamente
        logger.info("🚀 Iniciando pipeline otimizado: processamento imediato de arquivos já baixados")
        
        # Obter URLs dos arquivos da pasta remota (mesmos que foram baixados)
        from src.async_downloader import get_latest_month_zip_urls, _filter_urls_by_type
        
        base_url = os.getenv('BASE_URL')
        if not base_url:
            logger.error("BASE_URL não definida no arquivo .env")
            return False, ""
            
        zip_urls, _ = get_latest_month_zip_urls(base_url, latest_folder)
        
        # Filtrar URLs por tipos desejados
        tipos_desejados = args.tipos if args.tipos else []
        if tipos_desejados:
            zip_urls, ignored = _filter_urls_by_type(zip_urls, tuple(tipos_desejados))
            logger.info(f"Filtrados {ignored} URLs não desejadas para processamento. Restaram {len(zip_urls)} URLs.")
        
        # Preparar opções de processamento
        processing_options = {}
        if hasattr(args, 'criar_empresa_privada') and args.criar_empresa_privada:
            processing_options['create_private'] = True
        if hasattr(args, 'criar_subset_uf') and args.criar_subset_uf:
            processing_options['uf_subset'] = args.criar_subset_uf
        
        # Executar pipeline otimizado
        process_results = await optimized_download_and_process_pipeline(
            urls=zip_urls,
            source_zip_path=source_zip_path,
            unzip_path=PATH_UNZIP,
            output_parquet_path=output_parquet_path,
            tipos_a_processar=tipos_a_processar,
            delete_zips_after_extract=args.delete_zips_after_extract,
            force_download=False,  # Não forçar download pois já foi feito na etapa anterior
            **processing_options
        )
        
        process_time = time.time() - process_start_time
        logger.info("=" * 50)
        logger.info(f"Tempo de processamento: {format_elapsed_time(process_time)}")
        
        if not process_results.get('all_ok', False):
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
