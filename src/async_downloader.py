import asyncio
import datetime
import logging
import os
import re
import time
from typing import List, Tuple, Callable, Dict, Any
from urllib.parse import urljoin

# Bibliotecas de terceiros
import aiofiles
import aiohttp
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rich.logging import RichHandler
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
)

# Importações locais do projeto
from .config import config
from .utils import DownloadCache
from .process.empresa import process_single_zip as process_empresa_zip
from .process.estabelecimento import process_single_zip as process_estabelecimento_zip
from .process.simples import process_single_zip as process_simples_zip
from .process.socio import process_single_zip as process_socio_zip

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Instanciar o cache
# TODO: Considerar injetar a instância ao invés de criar globalmente, se necessário
download_cache = None  # Será inicializado quando necessário

def get_download_cache():
    """Obtém a instância do cache de downloads."""
    global download_cache
    if download_cache is None:
        download_cache = DownloadCache(config.cache.cache_path)
    return download_cache

logger = logging.getLogger(__name__)

# Mapeamento de tipos de arquivo para funções de processamento
PROCESSOR_MAP = {
    'Empresas': process_empresa_zip,
    'Estabelecimentos': process_estabelecimento_zip,
    'Simples': process_simples_zip,
    'Socios': process_socio_zip
}

# Semáforo para limitar o número de processamentos simultâneos
process_semaphore = asyncio.Semaphore(os.cpu_count() // 2 or 1)  # Metade do número de CPUs


def _fetch_and_parse(url: str) -> BeautifulSoup | None:
    """Busca HTML de uma URL e parseia com BeautifulSoup."""
    try:
        logger.debug(f"Fetching: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Verifica erros HTTP
        return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        logger.error(f"Erro ao buscar {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao parsear {url}: {e}")
        return None


def _find_links(soup: BeautifulSoup, base_url: str, ends_with: str | None = None) -> List[str]:
    """Encontra links em um objeto BeautifulSoup que terminam com um sufixo específico."""
    found_urls = []
    if not soup:
        return found_urls

    for link in soup.find_all('a'):
        href = link.get('href')
        if not href or href.startswith('?') or href == '../':
            continue

        # Verifica se o link termina com o sufixo desejado (case-insensitive)
        if ends_with and href.lower().endswith(ends_with.lower()):
            full_url = urljoin(base_url, href)  # Constrói URL completa
            if full_url not in found_urls:
                found_urls.append(full_url)
        # Se não busca por sufixo, pega todos os links válidos (usado para diretórios)
        elif ends_with is None and href.endswith('/'):  # Considera apenas diretórios se ends_with não for especificado
            full_url = urljoin(base_url, href)
            if full_url not in found_urls:
                found_urls.append(full_url)

    return found_urls


def _filter_urls_by_type(urls: List[str], tipos: Tuple[str, ...]) -> Tuple[List[str], int]:
    """Filtra uma lista de URLs, mantendo apenas aquelas cujo nome de arquivo começa com um dos tipos fornecidos."""
    filtered_urls = []
    ignored_count = 0
    for url in urls:
        filename = url.split('/')[-1]
        filename_lower = filename.lower()
        
        # Primeiro verificar se o arquivo está na lista de ignorados
        should_ignore = False
        for ignored in config.ignored_files:
            if filename_lower.startswith(ignored.lower()):
                logger.debug(f"Ignorando arquivo de referência: {filename}")
                should_ignore = True
                ignored_count += 1
                break
        
        if should_ignore:
            continue
            
        # Se não está na lista de ignorados, verificar se é um dos tipos desejados
        match_found = False
        for tipo in tipos:
            tipo_lower = tipo.lower()
            if filename_lower.startswith(tipo_lower):
                match_found = True
                filtered_urls.append(url)
                break
        
        if not match_found:
            logger.debug(f"Ignorando URL (tipo não desejado): {filename}")
            ignored_count += 1
            
    return filtered_urls, ignored_count


def get_latest_month_zip_urls(base_url: str, remote_folder: str | None = None) -> Tuple[List[str], str]:
    """Busca URLs de arquivos .zip na pasta AAAA-MM mais recente ou na pasta especificada.

    1. Busca e parseia a URL base.
    2. Encontra links de diretórios.
    3. Filtra diretórios no formato AAAA-MM e encontra o mais recente ou usa o especificado.
    4. Busca e parseia a URL do diretório escolhido.
    5. Encontra links .zip nesse diretório.
    
    Args:
        base_url: URL base para buscar diretórios
        remote_folder: Pasta específica a ser usada (formato AAAA-MM), se None usa a mais recente

    Returns:
        Tuple[List[str], str]: Lista de URLs e nome da pasta escolhida
    """
    zip_urls = []
    folder_url = None
    year_month_folders = []

    # 1 & 2: Buscar e encontrar links de diretórios na URL base
    logger.info(f"Buscando diretórios em: {base_url}")
    base_soup = _fetch_and_parse(base_url)
    if not base_soup:
        return [], ""

    directory_links = _find_links(base_soup, base_url,
                                  ends_with=None)  # ends_with=None busca diretórios terminados em /

    # 3: Filtrar diretórios AAAA-MM e encontrar o mais recente ou o especificado
    for dir_url in directory_links:
        folder_name = dir_url.strip('/').split('/')[-1]
        match = re.fullmatch(r'(\d{4})-(\d{2})', folder_name)
        if match:
            # Se existe um remote_folder específico e esse é o diretório
            if remote_folder and folder_name == remote_folder:
                folder_url = dir_url
                logger.info(f"Diretório especificado encontrado: {folder_name} ({folder_url})")
                break
            
            year_month_folders.append((folder_name, dir_url))
            logger.debug(f"Encontrado diretório AAAA-MM: {folder_name} -> {dir_url}")

    # Se não temos um folder_url específico (porque remote_folder não foi encontrado ou não foi especificado)
    if not folder_url:
        if remote_folder:
            # Se o usuário solicitou uma pasta específica, mas não foi encontrada
            logger.warning(f"Diretório solicitado '{remote_folder}' não foi encontrado.")
            return [], ""
            
        if not year_month_folders:
            logger.warning(f"Nenhum diretório no formato AAAA-MM encontrado em {base_url}")
            return [], ""

        # Ordena pela chave (nome da pasta AAAA-MM) para encontrar o mais recente
        year_month_folders.sort(key=lambda x: x[0], reverse=True)
        folder_name, folder_url = year_month_folders[0]
        logger.info(f"Diretório mais recente encontrado: {folder_name} ({folder_url})")
    else:
        # Usamos a pasta específica encontrada
        folder_name = folder_url.strip('/').split('/')[-1]

    # 4 & 5: Buscar e encontrar links .zip no diretório escolhido
    logger.info(f"Buscando arquivos .zip em: {folder_url}")
    folder_soup = _fetch_and_parse(folder_url)
    if not folder_soup:
        return [], folder_name  # Erro já logado

    zip_urls = _find_links(folder_soup, folder_url, ends_with='.zip')

    if not zip_urls:
        logger.warning(f"Nenhum arquivo .zip encontrado em {folder_url}")

    logger.info(f"Total de {len(zip_urls)} URLs .zip encontradas na pasta {folder_name}. ")
    return zip_urls, folder_name  # Retorna URLs e nome da pasta


async def _process_download_response(response: aiohttp.ClientResponse, destination_path: str, file_mode: str,
                                     progress: Progress, task_id: int, expected_size: int, initial_size: int,
                                     filename: str, remote_last_modified: int | None):
    """Processa o corpo da resposta HTTP e escreve no arquivo, atualizando a barra Rich."""
    downloaded_size_since_start = 0
    try:
        # Verifica o tamanho real do conteúdo a ser baixado (útil se for 200 OK em vez de 206)
        content_length_header = response.headers.get('Content-Length')
        if content_length_header is not None:
            size_to_download = int(content_length_header)
            # Ajusta o total na barra Rich
            progress.update(task_id, total=size_to_download, completed=0)
            initial_size = 0
        else:
            size_to_download = None

        async with aiofiles.open(destination_path, mode=file_mode) as f:
            # Inicia a tarefa na barra de progresso se ainda não estiver iniciada
            progress.start_task(task_id)
            async for chunk in response.content.iter_chunked(8192):
                await f.write(chunk)
                chunk_len = len(chunk)
                downloaded_size_since_start += chunk_len
                # Avança a barra Rich
                progress.update(task_id, advance=chunk_len)

        # Verificação final de tamanho
        final_local_size = initial_size + downloaded_size_since_start
        current_task = progress.tasks[task_id]

        if current_task.total and final_local_size < current_task.total:
            logger.warning(
                f"Download de {filename} parece incompleto. Esperado: {current_task.total}, Obtido: {final_local_size}")
            # Marca a barra como erro (vermelho)
            progress.update(task_id, description=f"[red]{filename[:30]} (incompleto)[/red]", style="red")
            
            # Registra o erro no cache
            if config.cache.enabled:
                error_msg = f"Download incompleto: Esperado {current_task.total}, Obtido {final_local_size}"
                get_download_cache().register_file_error(filename, error_msg)

        elif current_task.total and final_local_size > current_task.total:
            logger.warning(
                f"Download de {filename} maior que o esperado? Esperado: {current_task.total}, Obtido: {final_local_size}")
            # Marca a barra como aviso (amarelo)
            progress.update(task_id, description=f"[yellow]{filename[:30]} (tamanho>esperado)[/yellow]", style="yellow")
            
            # Registra o aviso no cache
            if config.cache.enabled:
                error_msg = f"Tamanho maior que o esperado: Esperado {current_task.total}, Obtido {final_local_size}"
                get_download_cache().register_file_error(filename, error_msg)

        # Garante que a barra chegue a 100% se o total era conhecido e foi atingido
        # Rich faz isso automaticamente se advance cobrir o total.

        # Marca a barra como concluída (verde)
        progress.update(task_id, description=f"[green]{filename[:30]} (concluído)[/green]", style="green")
        logger.info(
            f"Escrita do arquivo {filename} concluída (modo: {file_mode}). Tamanho final local: {final_local_size}")

        # 4. Atualizar data de modificação local e cache
        if remote_last_modified is not None:
            try:
                os.utime(destination_path, (time.time(), remote_last_modified))
            except Exception as e_utime:
                logger.warning(f"Não foi possível definir data de modificação para {filename}: {e_utime}")

        if config.cache.enabled:
            if final_local_size == expected_size:
                get_download_cache().update_file_cache(filename, expected_size, remote_last_modified)
                logger.debug(f"Cache atualizado para {filename}")
            else:
                error_msg = f"Tamanho final ({final_local_size}) difere do remoto esperado ({expected_size})"
                logger.error(f"Arquivo {filename}: {error_msg}. Cache não atualizado.")
                # Registra o erro no cache
                get_download_cache().register_file_error(filename, error_msg)
                # Marca a barra como erro se o cache falhar devido ao tamanho
                progress.update(task_id, description=f"[red]{filename[:30]} (erro cache)[/red]", style="red")

        return destination_path, None

    except Exception as e_proc:
        error_msg = f"Erro durante o processamento da resposta/escrita: {e_proc}"
        logger.error(f"{filename}: {error_msg}")
        # Marca a barra como erro
        progress.update(task_id, description=f"[red]{filename[:30]} (ERRO escrita)[/red]", style="red")
        
        # Registra o erro no cache
        if config.cache.enabled:
            get_download_cache().register_file_error(filename, error_msg)
            
        return filename, e_proc


async def download_file(session: aiohttp.ClientSession | None, url: str, destination_path: str, semaphore: asyncio.Semaphore,
                        progress: Progress, task_id: int) -> Tuple[str, Exception | None, str | None]:
    """
    Downloads a single file asynchronously, updating a Rich progress bar.
    Attempts to resume download if local file is partial and server supports Range.
    Otherwise, downloads the entire file. Checks cache and metadata.
    Applies styles (colors) to the progress bar based on status.
    Returns:
        Tuple[str, Exception | None, str | None]: (Path/URL, Error or None, Skip Reason or None)
    """
    filename = os.path.basename(destination_path)
    skip_reason: str | None = None  # Variável para armazenar o motivo do skip
    created_session = False
    async with semaphore:
        file_mode = 'wb'
        resume_header = {}
        initial_size = 0
        skip_download = False
        force_download = False

        try:
            # Criar sessão se não for fornecida
            if session is None:
                session = aiohttp.ClientSession()
                created_session = True

            # Verificar se o arquivo tem erros registrados no cache
            if config.cache.enabled and get_download_cache().has_file_error(filename):
                logger.warning(f"Arquivo {filename} tem erros registrados no cache. Forçando download completo.")
                force_download = True
                # Atualiza barra Rich (laranja para forçar download)
                progress.update(task_id, description=f"[orange]{filename[:30]} (forçando download)[/orange]",
                               style="yellow")
                file_mode = 'wb'
                initial_size = 0

            # 1. Obter metadados remotos - Atualiza descrição na barra Rich
            progress.update(task_id, description=f"[cyan]{filename[:30]} (verificando...)[/cyan]")
            remote_size, remote_last_modified = await get_remote_file_metadata(session, url)

            if remote_size is None:
                raise ValueError(f"Não foi possível obter metadados remotos para {url}")

            # Atualiza o total da barra assim que conhecido
            progress.update(task_id, total=remote_size)

            # 2. Verificar cache e arquivo local
            file_exists = os.path.exists(destination_path)
            local_size = os.path.getsize(destination_path) if file_exists else 0
            attempt_resume = False

            if file_exists and not force_download:
                # Checa cache primeiro (se habilitado)
                if config.cache.enabled and get_download_cache().is_file_cached(filename, remote_size, remote_last_modified):
                    if local_size == remote_size:
                        progress.update(task_id, description=f"[green]{filename[:30]} (cache)[/green]",
                                        completed=remote_size, style="green")
                        skip_download = True
                        skip_reason = "cache"
                    else:
                        # Cache diz que está ok, mas tamanho local difere -> Baixar completo
                        logger.warning(f"Arquivo {filename} em cache, mas tamanho local difere. Baixando completo.")
                        # Atualiza barra Rich (amarelo para cache inválido)
                        progress.update(task_id, description=f"[yellow]{filename[:30]} (cache inválido)[/yellow]",
                                        style="yellow")
                        file_mode = 'wb'
                        initial_size = 0
                # Sem cache ou cache desatualizado, verifica tamanho e data local
                elif local_size < remote_size:
                    # Arquivo parcial. Verifica data se possível
                    if remote_last_modified is not None:
                        local_last_modified = int(os.path.getmtime(destination_path))
                        if local_last_modified >= remote_last_modified:
                            # Parcial, mas local é mais novo ou igual? Estranho. Baixar completo.
                            logger.warning(
                                f"Arquivo local parcial {filename} é mais recente que o remoto? Baixando completo.")
                            file_mode = 'wb'
                            initial_size = 0
                        else:
                            # Parcial e mais antigo que remoto -> Tentar retomar
                            logger.info(f"Arquivo local parcial {filename} encontrado. Tentando retomar download.")
                            # Atualiza barra Rich (ciano para retomar)
                            progress.update(task_id, description=f"[cyan]{filename[:30]} (retomando?)[/cyan]",
                                            style="cyan")
                            attempt_resume = True
                            file_mode = 'ab'
                            initial_size = local_size
                            resume_header = {'Range': f'bytes={local_size}-'}
                    else:
                        # Parcial, mas sem data remota para comparar -> Tentar retomar (otimista)
                        logger.info(f"Arquivo local parcial {filename} encontrado (sem data remota). Tentando retomar.")
                        attempt_resume = True
                        file_mode = 'ab'
                        initial_size = local_size
                        resume_header = {'Range': f'bytes={local_size}-'}

                elif local_size == remote_size:
                    if remote_last_modified is not None:
                        local_last_modified = int(os.path.getmtime(destination_path))
                        if local_last_modified >= remote_last_modified:
                            progress.update(task_id, description=f"[green]{filename[:30]} (local)[/green]",
                                            completed=remote_size, style="green")
                            skip_download = True
                            skip_reason = "local"
                        else:
                            logger.info(f"Arquivo local {filename} completo mas desatualizado. Baixando completo.")
                            # Atualiza barra Rich (amarelo para desatualizado)
                            progress.update(task_id, description=f"[yellow]{filename[:30]} (desatualizado)[/yellow]",
                                            style="yellow")
                            file_mode = 'wb'
                            initial_size = 0
                    else:
                        progress.update(task_id, description=f"[green]{filename[:30]} (local s/ data)[/green]",
                                        completed=remote_size, style="green")
                        skip_download = True
                        skip_reason = "local (sem data)"
                else:  # local_size > remote_size
                    logger.warning(
                        f"Arquivo local {filename} maior que o remoto ({local_size} > {remote_size}). Baixando completo.")
                    # Atualiza barra Rich (amarelo para tamanho inválido)
                    progress.update(task_id, description=f"[yellow]{filename[:30]} (tamanho inválido)[/yellow]",
                                    style="yellow")
                    file_mode = 'wb'
                    initial_size = 0
            else:
                # Arquivo não existe localmente ou foi forçado o download -> Baixar completo
                if force_download:
                    logger.info(f"Download forçado para {filename} devido a erros anteriores.")
                else:
                    logger.info(f"Arquivo {filename} não encontrado localmente. Baixando completo.")
                # Atualiza barra Rich (cinza para novo download)
                progress.update(task_id, description=f"[grey]{filename[:30]} (novo download)[/grey]",
                                style="white")  # Usando branco como cor padrão
                file_mode = 'wb'
                initial_size = 0

            if skip_download and not force_download:
                progress.update(task_id, completed=remote_size)
                if config.cache.enabled and not get_download_cache().is_file_cached(filename, remote_size,
                                                                              remote_last_modified):
                    get_download_cache().update_file_cache(filename, remote_size, remote_last_modified)
                # Retorna com o motivo do skip
                return destination_path, None, skip_reason

            # 3. Executar Download
            # Reinicia o progresso (completed=initial_size) e define a descrição/estilo
            progress.update(task_id,
                            description=f"[cyan]{filename[:30]} ({'retomando' if attempt_resume else 'baixando'}...)[/cyan]",
                            completed=initial_size,  # Define o ponto de partida
                            style="cyan" if attempt_resume else "white")  # Ciano para retomada, branco para normal

            async with session.get(url, headers=resume_header,
                                   timeout=aiohttp.ClientTimeout(total=None, sock_connect=30,
                                                                 sock_read=3600)) as response:
                if attempt_resume:
                    if response.status == 206:
                        logger.info(f"Servidor aceitou retomar download para {filename}.")
                        # Descrição e estilo já definidos acima
                    elif response.status == 200:
                        logger.warning(f"Servidor ignorou Range para {filename}. Baixando arquivo completo novamente.")
                        file_mode = 'wb'
                        initial_size = 0
                        # Reinicia progresso e atualiza descrição/estilo (amarelo)
                        progress.update(task_id, completed=0,
                                        description=f"[yellow]{filename[:30]} (baixando completo*)[/yellow]",
                                        style="yellow")
                    else:
                        logger.error(
                            f"Falha ao tentar retomar {filename} (Status: {response.status}). Baixando arquivo completo como fallback.")
                        file_mode = 'wb'
                        initial_size = 0
                        # Reinicia progresso e atualiza descrição/estilo (vermelho)
                        progress.update(task_id, completed=0,
                                        description=f"[red]{filename[:30]} (fallback download)[/red]", style="red")
                        response.release()
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=None, sock_connect=30,
                                                                                  sock_read=3600)) as response_fallback:
                            response_fallback.raise_for_status()
                            return await _process_download_response(response_fallback, destination_path, file_mode,
                                                                    progress, task_id, remote_size, initial_size,
                                                                    filename, remote_last_modified)

                response.raise_for_status()
                # Download ocorreu, retorna sem motivo de skip
                dest_path, error = await _process_download_response(response, destination_path, file_mode, progress,
                                                                    task_id, remote_size, initial_size, filename,
                                                                    remote_last_modified)
                return dest_path, error, None  # Adiciona None para skip_reason

        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            logger.error(f"Erro ao processar {filename} ({url}): {e}")
            # Atualiza barra Rich (vermelho para erro)
            progress.update(task_id, description=f"[red]{filename[:30]} (ERRO)[/red]", style="red")
            # Remove do cache em caso de erro
            if config.cache.enabled:
                get_download_cache().remove_file_from_cache(filename)
            # Retorna erro, sem motivo de skip
            return url, e, None  # Adiciona None para skip_reason
        except Exception as e:
            logger.error(f"Erro inesperado ao processar {filename} ({url}): {e}")
            # Atualiza barra Rich (vermelho para erro inesperado)
            progress.update(task_id, description=f"[red]{filename[:30]} (ERRO Inesp.)[/red]", style="red")
            if config.cache.enabled:
                get_download_cache().remove_file_from_cache(filename)
            # Retorna erro, sem motivo de skip
            return url, e, None  # Adiciona None para skip_reason
        finally:
            # Fechar a sessão se foi criada por nós
            if created_session and session:
                await session.close()


async def get_remote_file_metadata(session: aiohttp.ClientSession, url: str) -> Tuple[int | None, int | None]:
    """Obtém tamanho e timestamp de modificação de um arquivo remoto via HEAD request."""
    try:
        async with session.head(url, timeout=30, allow_redirects=True) as response:
            response.raise_for_status()
            remote_size = int(response.headers.get('Content-Length', 0))
            last_modified_str = response.headers.get('Last-Modified')
            if last_modified_str:
                # Parseia o timestamp - Exemplo: 'Wed, 21 Oct 2015 07:28:00 GMT'
                # Nota: O formato pode variar, ajuste se necessário.
                try:
                    # Tenta formato RFC 1123 (mais comum)
                    dt_obj = datetime.datetime.strptime(last_modified_str, '%a, %d %b %Y %H:%M:%S GMT')
                except ValueError:
                    # Tentar outros formatos se necessário ou logar um erro
                    logger.warning(f"Formato inesperado de Last-Modified: {last_modified_str} para {url}")
                    return remote_size, None
                timestamp_last_modified = int(dt_obj.timestamp())
                return remote_size, timestamp_last_modified
            else:
                logger.warning(f"Cabeçalho Last-Modified não encontrado para {url}")
                return remote_size, None
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
        logger.error(f"Erro ao obter metadados de {url}: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Erro inesperado ao obter metadados de {url}: {e}")
        return None, None


async def process_downloaded_file(filename: str, destination_path: str, path_unzip: str, path_parquet: str) -> Tuple[str, Exception | None]:
    """
    Processa um arquivo ZIP baixado de forma assíncrona.
    Descompacta e converte para parquet usando Polars.
    
    Args:
        filename: Nome do arquivo baixado
        destination_path: Caminho completo do arquivo ZIP
        path_unzip: Diretório para descompactar
        path_parquet: Diretório onde salvar os arquivos parquet
        
    Returns:
        Tuple[str, Exception | None]: Nome do arquivo e erro (se ocorrer)
    """
    try:
        logger.info(f"Iniciando processamento paralelo de: {filename}")
        async with process_semaphore:
            # Determinar qual processador usar com base no nome do arquivo
            processo_encontrado = False
            for tipo_prefixo, processor_fn in PROCESSOR_MAP.items():
                if filename.lower().startswith(tipo_prefixo.lower()):
                    logger.info(f"Processando {filename} como {tipo_prefixo}")
                    
                    # Chamada do processador apropriado
                    try:
                        # Criar argumentos específicos para cada tipo de processador
                        kwargs = {}
                        if tipo_prefixo == "Empresas":
                            kwargs = {
                                "zip_file": filename,
                                "path_zip": os.path.dirname(destination_path),
                                "path_unzip": path_unzip,
                                "path_parquet": path_parquet,
                                "create_private": False  # Configuração padrão
                            }
                        else:
                            # Para os outros tipos, os parâmetros são os mesmos
                            kwargs = {
                                "zip_file": filename,
                                "path_zip": os.path.dirname(destination_path),
                                "path_unzip": path_unzip,
                                "path_parquet": path_parquet
                            }
                            
                        # Executar o processamento em um thread separado para não bloquear o event loop
                        loop = asyncio.get_event_loop()
                        
                        # Todas as funções agora são do tipo polars e retornam bool
                        result = await loop.run_in_executor(
                            None,
                            lambda: processor_fn(**kwargs)
                        )
                        
                        # Verificar o resultado (agora é sempre um bool)
                        if result is True:
                            logger.info(f"Processamento de {filename} concluído com sucesso")
                        else:
                            logger.warning(f"Processamento de {filename} falhou")
                    except Exception as e:
                        logger.error(f"Erro durante o processamento de {filename}: {str(e)}")
                        return filename, e
                    
                    processo_encontrado = True
                    break
            
            if not processo_encontrado:
                logger.warning(f"Não foi possível determinar o tipo de processamento para {filename}")
                
        return filename, None
    except Exception as e:
        logger.error(f"Erro geral durante o processamento de {filename}: {str(e)}")
        return filename, e


async def download_multiple_files(urls: List[str], destination_folder: str, max_concurrent: int = 5):
    """
    Downloads multiple files asynchronously with progress bars using Rich.
    Also processes each file immediately after download is completed or identified as already existing.
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
        logger.info(f"Diretório criado: {destination_folder}")

    semaphore = asyncio.Semaphore(max_concurrent)
    download_tasks = []
    processing_tasks = []
    downloaded_files = []
    failed_downloads = []
    skipped_files = []  # Lista para guardar arquivos pulados e motivo
    processed_files = []
    failed_processing = []
    
    # Verificar se há arquivos com erros no cache
    files_with_errors = []
    if config.cache.enabled:
        cache = get_download_cache()
        files_with_errors = cache.get_files_with_errors()
        if files_with_errors:
            logger.warning(f"Encontrados {len(files_with_errors)} arquivos com erros no cache. Eles serão baixados novamente.")
            for error_file in files_with_errors:
                logger.info(f"Arquivo com erro: {error_file}")
    
    # Fila para comunicação entre downloads e processamento
    processing_queue = asyncio.Queue()
    
    # Pegar caminhos para processamento dos arquivos
    path_unzip = os.getenv('PATH_UNZIP')
    path_parquet = os.getenv('PATH_PARQUET')
    
    if not path_unzip or not path_parquet:
        logger.error("PATH_UNZIP ou PATH_PARQUET não definidos, o processamento paralelo será desabilitado.")
        # Continua com os downloads, mas não vai processar

    # Configurar colunas do Rich Progress
    progress_columns = [
        TextColumn("[progress.description]{task.description}", justify="left"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",  # Readicionar separador
        DownloadColumn(),
        "•",  # Readicionar separador
        TransferSpeedColumn(),
        "•",  # Readicionar separador
        TimeRemainingColumn(),
    ]
    
    # Função que processa os arquivos da fila
    async def process_files_from_queue():
        """Consome a fila de processamento e processa os arquivos em tempo real."""
        while True:
            try:
                file_path = await processing_queue.get()
                
                # Verifica se recebemos o sinal de encerramento (None)
                if file_path is None:
                    logger.debug("Sinal de encerramento recebido pelo consumidor da fila de processamento")
                    processing_queue.task_done()
                    break
                
                filename = os.path.basename(file_path)
                logger.info(f"Iniciando processamento paralelo para {filename}")
                
                # Verificar se arquivo tem erros no cache
                if config.cache.enabled:
                    cache = get_download_cache()
                    if cache.has_file_error(filename):
                        logger.warning(f"Arquivo {filename} tem erros registrados no cache. Pulando processamento.")
                        processing_queue.task_done()
                        continue
                
                try:
                    result, error = await process_downloaded_file(filename, file_path, path_unzip, path_parquet)
                    if error is None:
                        processed_files.append(result)
                        # Atualiza o cache para indicar processamento bem-sucedido
                        if config.cache.enabled:
                            cache = get_download_cache()
                            # Obtém informações atuais do arquivo
                            file_info = cache.get_file_info(filename)
                            if file_info:
                                cache.update_file_cache(
                                    filename, 
                                    file_info.get("size", 0), 
                                    file_info.get("modified", 0),
                                    status="success"
                                )
                    else:
                        failed_processing.append((result, error))
                        logger.warning(f"Falha ao processar {filename}: {error}")
                        # Registra o erro de processamento no cache
                        if config.cache.enabled:
                            cache = get_download_cache()
                            error_msg = f"Erro de processamento: {str(error)}"
                            cache.register_file_error(filename, error_msg)
                except Exception as e:
                    logger.error(f"Erro ao processar {filename}: {e}")
                    failed_processing.append((filename, e))
                    # Registra o erro no cache
                    if config.cache.enabled:
                        cache = get_download_cache()
                        error_msg = f"Erro não tratado: {str(e)}"
                        cache.register_file_error(filename, error_msg)
                
                processing_queue.task_done()
            except Exception as e:
                logger.error(f"Erro no processamento da fila: {e}")
                processing_queue.task_done()

    # Iniciar o processador de fila
    processor_task = asyncio.create_task(process_files_from_queue())

    # Configurar barra de progresso
    with Progress(*progress_columns, expand=True) as progress:
        # Criar tarefas de download
        for url in urls:
            filename = os.path.basename(url)
            destination_path = os.path.join(destination_folder, filename)
            
            # Criar tarefa de download
            task_id = progress.add_task(f"[cyan]{filename[:30]}...", total=None)
            download_task = asyncio.create_task(
                download_file(
                    None,  # session será criado dentro da função
                    url,
                    destination_path,
                    semaphore,
                    progress,
                    task_id
                )
            )
            download_tasks.append(download_task)

        # Aguardar todas as tarefas de download
        for task in asyncio.as_completed(download_tasks):
            try:
                result, error, skip_reason = await task
                if error is None:
                    if skip_reason:
                        skipped_files.append((result, skip_reason))
                    else:
                        downloaded_files.append(result)
                        # Adicionar à fila de processamento
                        await processing_queue.put(result)
                else:
                    failed_downloads.append((result, error))
            except Exception as e:
                logger.error(f"Erro na tarefa de download: {e}")
                failed_downloads.append((None, e))

        # Sinalizar fim do processamento
        await processing_queue.put(None)
        await processor_task

    # Resumo final
    logger.info("=" * 50)
    logger.info("RESUMO DO DOWNLOAD:")
    logger.info("=" * 50)
    logger.info(f"Arquivos baixados com sucesso: {len(downloaded_files)}")
    logger.info(f"Arquivos pulados: {len(skipped_files)}")
    logger.info(f"Downloads com falha: {len(failed_downloads)}")
    logger.info(f"Arquivos processados com sucesso: {len(processed_files)}")
    logger.info(f"Processamentos com falha: {len(failed_processing)}")
    logger.info("=" * 50)

    return downloaded_files, failed_downloads


def get_remote_folders(base_url: str) -> List[str]:
    """Busca todas as pastas remotas disponíveis no formato AAAA-MM.
    
    Args:
        base_url: URL base para buscar diretórios
        
    Returns:
        List[str]: Lista de nomes de pastas no formato AAAA-MM
    """
    year_month_folders = []
    
    # Buscar e encontrar links de diretórios na URL base
    logger.info(f"Buscando diretórios em: {base_url}")
    base_soup = _fetch_and_parse(base_url)
    if not base_soup:
        return []
    
    directory_links = _find_links(base_soup, base_url, ends_with=None)
    
    # Filtrar diretórios AAAA-MM
    for dir_url in directory_links:
        folder_name = dir_url.strip('/').split('/')[-1]
        if re.fullmatch(r'(\d{4})-(\d{2})', folder_name):
            year_month_folders.append(folder_name)
            logger.debug(f"Encontrado diretório AAAA-MM: {folder_name}")
    
    # Ordenar pastas por data (mais recente primeiro)
    year_month_folders.sort(reverse=True)
    
    logger.info(f"Total de {len(year_month_folders)} pastas remotas encontradas")
    return year_month_folders


async def get_latest_remote_folder(base_url: str) -> str:
    """Busca a pasta remota mais recente no formato AAAA-MM.
    
    Args:
        base_url: URL base para buscar diretórios
        
    Returns:
        str: Nome da pasta mais recente no formato AAAA-MM
    """
    folders = get_remote_folders(base_url)
    if not folders:
        logger.error("Nenhuma pasta remota encontrada")
        return ""
    
    latest_folder = folders[0]  # A lista já está ordenada por data (mais recente primeiro)
    logger.info(f"Pasta remota mais recente: {latest_folder}")
    return latest_folder
