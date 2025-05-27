import asyncio
import datetime
import logging
import os
import re
import time
import zipfile
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
from .process.empresa import process_single_zip as process_empresa_zip, extract_zip_parallel
from .process.estabelecimento import process_single_zip as process_estabelecimento_zip
from .process.simples import process_single_zip as process_simples_zip
from .process.socio import process_single_zip as process_socio_zip
from .utils.time_utils import format_elapsed_time

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
                # Atualiza o cache silenciosamente (sem log duplicado)
                get_download_cache().update_file_cache(filename, expected_size, remote_last_modified, log_update=False)
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
    skip_reason: str | None = None
    created_session = False
    max_retries = 3
    retry_count = 0
    last_error = None

    while retry_count < max_retries:
        try:
            async with semaphore:
                file_mode = 'wb'
                resume_header = {}
                initial_size = 0
                skip_download = False
                force_download = False

                # Criar sessão se não for fornecida
                if session is None:
                    session = aiohttp.ClientSession()
                    created_session = True

                # Verificar se o arquivo tem erros registrados no cache
                if config.cache.enabled and get_download_cache().has_file_error(filename):
                    logger.warning(f"Arquivo {filename} tem erros registrados no cache. Forçando download completo.")
                    force_download = True
                    progress.update(task_id, description=f"[orange]{filename[:30]} (forçando download)[/orange]",
                                   style="yellow")
                    file_mode = 'wb'
                    initial_size = 0

                # 1. Obter metadados remotos
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
                            logger.warning(f"Arquivo {filename} em cache, mas tamanho local difere. Baixando completo.")
                            progress.update(task_id, description=f"[yellow]{filename[:30]} (cache inválido)[/yellow]",
                                            style="yellow")
                            file_mode = 'wb'
                            initial_size = 0
                    elif local_size < remote_size:
                        if remote_last_modified is not None:
                            local_last_modified = int(os.path.getmtime(destination_path))
                            if local_last_modified >= remote_last_modified:
                                logger.warning(
                                    f"Arquivo local parcial {filename} é mais recente que o remoto? Baixando completo.")
                                file_mode = 'wb'
                                initial_size = 0
                            else:
                                logger.info(f"Arquivo local parcial {filename} encontrado. Tentando retomar download.")
                                progress.update(task_id, description=f"[cyan]{filename[:30]} (retomando?)[/cyan]",
                                                style="cyan")
                                attempt_resume = True
                                file_mode = 'ab'
                                initial_size = local_size
                                resume_header = {'Range': f'bytes={local_size}-'}
                        else:
                            logger.info(f"Arquivo local parcial {filename} encontrado (sem data remota). Tentando retomar.")
                            progress.update(task_id, description=f"[cyan]{filename[:30]} (retomando?)[/cyan]",
                                            style="cyan")
                            attempt_resume = True
                            file_mode = 'ab'
                            initial_size = local_size
                            resume_header = {'Range': f'bytes={local_size}-'}
                    elif local_size > remote_size:
                        logger.warning(
                            f"Arquivo local {filename} maior que remoto? Local: {local_size}, Remoto: {remote_size}")
                        file_mode = 'wb'
                        initial_size = 0
                    elif local_size == remote_size:
                        if remote_last_modified is not None:
                            local_last_modified = int(os.path.getmtime(destination_path))
                            if local_last_modified >= remote_last_modified:
                                progress.update(task_id, description=f"[green]{filename[:30]} (atualizado)[/green]",
                                                completed=remote_size, style="green")
                                skip_download = True
                                skip_reason = "up-to-date"
                            else:
                                logger.info(f"Arquivo {filename} desatualizado. Baixando novamente.")
                                file_mode = 'wb'
                                initial_size = 0

                if not skip_download:
                    # 3. Baixar arquivo
                    if attempt_resume:
                        try:
                            async with session.get(url, headers=resume_header) as response:
                                if response.status == 206:  # Partial Content
                                    logger.info(f"Retomando download de {filename} a partir do byte {initial_size}")
                                    result, error = await _process_download_response(
                                        response, destination_path, file_mode, progress, task_id,
                                        remote_size, initial_size, filename, remote_last_modified)
                                    if error is None:
                                        return result, None, skip_reason
                                else:
                                    logger.warning(f"Servidor não suporta retomada para {filename}. Baixando completo.")
                                    file_mode = 'wb'
                                    initial_size = 0
                        except Exception as e_resume:
                            logger.warning(f"Erro ao tentar retomar {filename}: {e_resume}. Baixando completo.")
                            file_mode = 'wb'
                            initial_size = 0

                    # Download completo (seja porque resume falhou ou não era necessário)
                    if file_mode == 'wb':
                        progress.update(task_id, description=f"[cyan]{filename[:30]} (baixando...)[/cyan]")
                        async with session.get(url) as response:
                            if response.status == 200:
                                result, error = await _process_download_response(
                                    response, destination_path, file_mode, progress, task_id,
                                    remote_size, initial_size, filename, remote_last_modified)
                                if error is None:
                                    return result, None, skip_reason
                            else:
                                raise aiohttp.ClientError(
                                    f"Status inesperado ao baixar {filename}: {response.status}")
                else:
                    return destination_path, None, skip_reason

        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as e:
            last_error = e
            retry_count += 1
            if retry_count < max_retries:
                wait_time = 2 ** retry_count  # Backoff exponencial: 2, 4, 8 segundos
                logger.warning(f"Tentativa {retry_count} de {max_retries} para {filename} falhou. "
                             f"Aguardando {wait_time} segundos antes de tentar novamente. Erro: {e}")
                progress.update(task_id, description=f"[yellow]{filename[:30]} (tentativa {retry_count})[/yellow]")
                await asyncio.sleep(wait_time)
            continue
        except Exception as e:
            last_error = e
            break
        finally:
            if created_session:
                await session.close()

    # Se chegou aqui, todas as tentativas falharam
    error_msg = f"Todas as tentativas falharam para {filename}. Último erro: {last_error}"
    logger.error(error_msg)
    progress.update(task_id, description=f"[red]{filename[:30]} (ERRO final)[/red]", style="red")
    
    if config.cache.enabled:
        get_download_cache().register_file_error(filename, str(error_msg))
    
    return filename, last_error, None


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


async def process_downloaded_file(filename: str, file_path: str, path_unzip: str, path_parquet: str) -> Tuple[str, Exception | None]:
    """
    Processa um arquivo baixado, extraindo e convertendo para parquet.
    
    Args:
        filename: Nome do arquivo
        file_path: Caminho do arquivo zip
        path_unzip: Diretório onde extrair o arquivo
        path_parquet: Diretório onde salvar o arquivo parquet
        
    Returns:
        Tuple[str, Exception | None]: (Caminho do arquivo, Erro se houver)
    """
    try:
        # Extrair pasta remota do caminho zip (geralmente algo como 2025-03)
        remote_folder = os.path.basename(os.path.dirname(file_path))
        # Verificar se o formato é AAAA-MM
        if not re.match(r'^\d{4}-\d{2}$', remote_folder):
            # Se não for uma pasta no formato esperado, tentar extrair do caminho
            match = re.search(r'(20\d{2}-\d{2})', file_path)
            if match:
                remote_folder = match.group(1)
            else:
                # Último recurso: usar um valor padrão
                remote_folder = datetime.datetime.now().strftime("%Y-%m")
        
        logger.info(f"Pasta remota identificada para processamento: {remote_folder}")

        # Verifica se o arquivo já foi processado com sucesso
        if config.cache.enabled:
            cache = get_download_cache()
            if cache.is_file_processed(filename):
                processing_info = cache.get_processing_status(filename)
                parquet_path = processing_info.get("parquet_path")
                if parquet_path and os.path.exists(parquet_path):
                    logger.info(f"Arquivo {filename} já foi processado anteriormente. Pulando processamento.")
                    return parquet_path, None

        # Atualiza status para extraindo
        if config.cache.enabled:
            cache = get_download_cache()
            cache.update_processing_status(filename, "extracting")

        # Extrair o arquivo
        logger.info(f"Extraindo {filename}...")
        print(f"📦 Extraindo {filename}...")
        
        # Determinar o diretório de extração específico para este arquivo
        extract_dir = os.path.join(path_unzip, os.path.splitext(filename)[0])
        
        # Usar a função de extração paralela do módulo empresa
        success = extract_zip_parallel(file_path, extract_dir)
        
        if not success:
            error_msg = f"Falha na extração de {filename}"
            logger.error(error_msg)
            return filename, Exception(error_msg)
        
        logger.info(f"Extração paralela concluída para {filename}")
        print(f"✅ Extração de {filename} concluída")

        # Atualiza status para processando
        if config.cache.enabled:
            cache.update_processing_status(filename, "processing")

        # Processa o arquivo baseado no tipo
        parquet_file = None
        success = False
        
        # Determina o tipo de arquivo e usa a função de processamento apropriada
        for tipo_prefixo, processor_fn in PROCESSOR_MAP.items():
            if filename.lower().startswith(tipo_prefixo.lower()):
                logger.info(f"Processando {filename} como {tipo_prefixo}")
                print(f"🔄 Processando {filename} como {tipo_prefixo}")
                try:
                    # Criar argumentos específicos para cada tipo de processador
                    kwargs = {
                        "zip_file": filename,
                        "path_zip": os.path.dirname(file_path),
                        "path_unzip": path_unzip,
                        "path_parquet": path_parquet,
                    }
                    
                    # Adicionar argumentos específicos baseados na função
                    if "remote_folder" in processor_fn.__code__.co_varnames:
                        kwargs["remote_folder"] = remote_folder
                    if "create_private" in processor_fn.__code__.co_varnames:
                        kwargs["create_private"] = False
                    
                    # Executar a função de processamento correta
                    logger.info(f"Chamando processador {processor_fn.__name__} para {filename}")
                    print(f"⚙️  Executando processador {processor_fn.__name__} para {filename}")
                    result = processor_fn(**kwargs)
                    
                    if result:
                        success = True
                        # Construir caminho do parquet baseado no tipo e pasta remota
                        if remote_folder:
                            parquet_file = os.path.join(path_parquet, remote_folder, tipo_prefixo.lower())
                        else:
                            parquet_file = os.path.join(path_parquet, tipo_prefixo.lower())
                        
                        logger.info(f"Processamento de {filename} concluído com sucesso. Parquet salvo em: {parquet_file}")
                        print(f"💾 Parquet salvo em: {parquet_file}")
                        break
                    else:
                        logger.warning(f"Processador {processor_fn.__name__} retornou None para {filename}")
                        print(f"⚠️  Processador retornou None para {filename}")
                except Exception as e:
                    logger.error(f"Erro no processador {processor_fn.__name__} para {filename}: {e}")
                    print(f"❌ Erro no processador para {filename}: {e}")
                    continue
                    
                break  # Sai do loop após encontrar o tipo correto

        if success and parquet_file:
            logger.info(f"Arquivo {filename} processado com sucesso. Parquet: {parquet_file}")
            # Atualiza cache com sucesso
            if config.cache.enabled:
                cache.update_processing_status(filename, "completed", parquet_file)
            return parquet_file, None
        else:
            error_msg = f"Falha ao processar {filename} - nenhum processador adequado encontrado ou processamento falhou"
            logger.error(error_msg)
            if config.cache.enabled:
                cache = get_download_cache()
                cache.register_file_error(filename, error_msg)
            return file_path, ValueError(error_msg)

    except Exception as e:
        error_msg = f"Erro ao processar {filename}: {e}"
        logger.error(error_msg)
        if config.cache.enabled:
            cache = get_download_cache()
            cache.register_file_error(filename, error_msg)
        return file_path, e


def format_elapsed_time(seconds: float) -> str:
    """
    Formata o tempo decorrido em horas, minutos e segundos.
    
    Args:
        seconds: Tempo em segundos
        
    Returns:
        str: Tempo formatado (ex: "2h 15min 30s" ou "15min 30s" ou "30s")
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:  # Mostra minutos se tiver horas
        parts.append(f"{minutes}min")
    parts.append(f"{secs}s")
    
    return " ".join(parts)


async def download_multiple_files(urls: List[str], destination_folder: str, max_concurrent: int = 5):
    """
    Downloads multiple files asynchronously with progress bars using Rich.
    Also processes each file immediately after download is completed or identified as already existing.
    """
    start_time = time.time()
    
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
        logger.info(f"Diretório criado: {destination_folder}")

    semaphore = asyncio.Semaphore(max_concurrent)
    download_tasks = []
    processing_tasks = []
    downloaded_files = []
    failed_downloads = []
    skipped_files = []
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

    # Configurar colunas do Rich Progress
    progress_columns = [
        TextColumn("[progress.description]{task.description}", justify="left"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
    ]

    # Função que processa os arquivos da fila em paralelo
    async def process_files_from_queue():
        """Consome a fila de processamento e processa os arquivos em tempo real."""
        processor_id = id(asyncio.current_task())
        processor_name = f"Worker-{processor_id % 1000}"  # Nome mais legível
        
        # Log visível no console
        print(f"\n🔧 {processor_name} iniciado e aguardando arquivos...")
        logger.info(f"Iniciando processador {processor_id}")
        
        while True:
            try:
                logger.debug(f"Processador {processor_id} aguardando arquivo...")
                file_path = await processing_queue.get()
                
                # Verifica se recebemos o sinal de encerramento (None)
                if file_path is None:
                    print(f"\n🏁 {processor_name} finalizando...")
                    logger.debug(f"Processador {processor_id} recebeu sinal de encerramento")
                    processing_queue.task_done()
                    break
                
                filename = os.path.basename(file_path)
                
                # Log visível no console
                print(f"\n⚙️  {processor_name} processando: {filename}")
                logger.info(f"Processador {processor_id} iniciando processamento de {filename}")
                
                # Verificar se arquivo tem erros no cache
                if config.cache.enabled:
                    cache = get_download_cache()
                    if cache.has_file_error(filename):
                        print(f"⚠️  {processor_name}: {filename} tem erros no cache - pulando")
                        logger.warning(f"Processador {processor_id}: Arquivo {filename} tem erros registrados no cache. Pulando processamento.")
                        processing_queue.task_done()
                        continue
                
                try:
                    # Usar o semáforo para limitar processamento paralelo
                    logger.debug(f"Processador {processor_id} aguardando semáforo para {filename}")
                    async with process_semaphore:
                        print(f"🔄 {processor_name}: Iniciando extração e processamento de {filename}")
                        logger.info(f"Processador {processor_id} obteve semáforo para {filename}")
                        
                        # Executar o processamento diretamente (função já é assíncrona)
                        result, error = await process_downloaded_file(filename, file_path, path_unzip, path_parquet)
                        
                        if error is None:
                            processed_files.append(result)
                            print(f"✅ {processor_name}: {filename} processado com sucesso!")
                            logger.info(f"Processador {processor_id} concluiu processamento de {filename} com sucesso")
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
                                        status="success",
                                        log_update=False
                                    )
                                    logger.info(f"Processador {processor_id}: Cache atualizado para {filename}")
                        else:
                            failed_processing.append((result, error))
                            print(f"❌ {processor_name}: Falha ao processar {filename} - {error}")
                            logger.warning(f"Processador {processor_id}: Falha ao processar {filename}: {error}")
                            # Registra o erro de processamento no cache
                            if config.cache.enabled:
                                cache = get_download_cache()
                                error_msg = f"Erro de processamento: {str(error)}"
                                cache.register_file_error(filename, error_msg)
                except Exception as e:
                    print(f"💥 {processor_name}: Erro crítico ao processar {filename} - {e}")
                    logger.error(f"Processador {processor_id}: Erro ao processar {filename}: {e}")
                    failed_processing.append((filename, e))
                    # Registra o erro no cache
                    if config.cache.enabled:
                        cache = get_download_cache()
                        error_msg = f"Erro não tratado: {str(e)}"
                        cache.register_file_error(filename, error_msg)
                
                logger.debug(f"Processador {processor_id} finalizando tarefa para {filename}")
                processing_queue.task_done()
            except Exception as e:
                print(f"💥 {processor_name}: Erro na fila de processamento - {e}")
                logger.error(f"Processador {processor_id}: Erro no processamento da fila: {e}")
                processing_queue.task_done()

        print(f"🏁 {processor_name} encerrado")
        logger.info(f"Processador {processor_id} encerrado")

    # Criar múltiplos processadores para trabalhar em paralelo
    num_processors = min(3, os.cpu_count() or 1)  # Limitar a 3 processadores ou número de CPUs
    logger.info(f"Iniciando {num_processors} processadores paralelos")
    processor_tasks = [
        asyncio.create_task(process_files_from_queue())
        for _ in range(num_processors)
    ]

    # Verificar arquivos já existentes e adicioná-los à fila de processamento
    if path_unzip and path_parquet:
        print(f"\n📋 Verificando arquivos já baixados para processamento...")
        logger.info("Verificando arquivos já baixados para processamento...")
        for url in urls:
            filename = os.path.basename(url)
            file_path = os.path.join(destination_folder, filename)
            
            # Se o arquivo já existe e não tem erros no cache
            if os.path.exists(file_path):
                if config.cache.enabled:
                    cache = get_download_cache()
                    if cache.has_file_error(filename):
                        logger.debug(f"Arquivo {filename} tem erros no cache, será baixado novamente")
                        continue
                
                print(f"📁 Adicionando arquivo existente {filename} à fila de processamento")
                logger.info(f"Adicionando arquivo já existente {filename} à fila de processamento")
                await processing_queue.put(file_path)

    # Criar sessão compartilhada para todos os downloads
    async with aiohttp.ClientSession() as session:
        # Configurar barra de progresso
        with Progress(*progress_columns, expand=True) as progress:
            # Criar tarefas de download
            tasks_info = []
            for url in urls:
                filename = os.path.basename(url)
                destination_path = os.path.join(destination_folder, filename)
                task_id = progress.add_task(f"[cyan]{filename[:30]}...", total=None)
                tasks_info.append((url, destination_path, task_id))

            # Criar e iniciar todas as tarefas de download simultaneamente
            download_tasks = [
                asyncio.create_task(
                    download_file(
                        session,  # Usar a mesma sessão para todos os downloads
                        url,
                        destination_path,
                        semaphore,
                        progress,
                        task_id
                    )
                )
                for url, destination_path, task_id in tasks_info
            ]

            # Aguardar todas as tarefas SIMULTANEAMENTE
            results = await asyncio.gather(*download_tasks, return_exceptions=True)

            # Processar resultados
            for result in results:
                if isinstance(result, tuple):
                    path, error, skip_reason = result
                    if error is None:
                        if skip_reason:
                            skipped_files.append((path, skip_reason))
                            logger.info(f"Arquivo {os.path.basename(path)} pulado: {skip_reason}")
                        else:
                            downloaded_files.append(path)
                            filename = os.path.basename(path)
                            print(f"📥 Download concluído: {filename} → Adicionando à fila de processamento")
                            logger.info(f"Adicionando {filename} à fila de processamento")
                            # Adicionar à fila de processamento
                            await processing_queue.put(path)
                    else:
                        failed_downloads.append((path, error))
                        logger.error(f"Download falhou para {os.path.basename(path)}: {error}")
                else:
                    # Se não é uma tupla, é uma exceção não tratada
                    failed_downloads.append((None, result))
                    logger.error(f"Erro não tratado durante download: {result}")

            # Sinalizar fim do processamento para todos os processadores
            logger.info("Todos os downloads concluídos. Sinalizando encerramento para os processadores...")
            for _ in range(num_processors):
                await processing_queue.put(None)

            # Aguardar todos os processadores terminarem
            logger.info("Aguardando conclusão de todos os processadores...")
            await asyncio.gather(*processor_tasks)
            logger.info("Todos os processadores concluídos")

    # Resumo final
    logger.info("=" * 50)
    logger.info("RESUMO DO DOWNLOAD:")
    logger.info("=" * 50)
    logger.info(f"Arquivos baixados com sucesso: {len(downloaded_files)}")
    logger.info(f"Arquivos pulados: {len(skipped_files)}")
    logger.info(f"Downloads com falha: {len(failed_downloads)}")
    logger.info(f"Arquivos processados com sucesso: {len(processed_files)}")
    logger.info(f"Processamentos com falha: {len(failed_processing)}")
    
    # Calcular tempo total
    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Tempo total: {format_elapsed_time(total_time)}")
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
