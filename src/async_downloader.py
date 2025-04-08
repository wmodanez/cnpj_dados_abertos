import asyncio
import os
import logging
import re
import datetime
import time
from typing import List, Tuple
from urllib.parse import urljoin

# Bibliotecas de terceiros
import aiofiles
import aiohttp
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
)
from rich.logging import RichHandler

# Importações locais do projeto
from .config import config
from .utils import DownloadCache
import datetime
import time

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Instanciar o cache
# TODO: Considerar injetar a instância ao invés de criar globalmente, se necessário
download_cache = DownloadCache(config.cache.cache_path)

# Remover configuração de logging daqui - será feita em main.py
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(message)s",
#     datefmt="[%X]",
#     handlers=[RichHandler(rich_tracebacks=True)]
# )
logger = logging.getLogger(__name__)

def _fetch_and_parse(url: str) -> BeautifulSoup | None:
    """Busca HTML de uma URL e parseia com BeautifulSoup."""
    try:
        logger.debug(f"Fetching: {url}")
        response = requests.get(url)
        response.raise_for_status() # Verifica erros HTTP
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
        if not href or href.startswith('?') or href == '../': # Ignora links inválidos, de query ou pai
            continue

        # Verifica se o link termina com o sufixo desejado (case-insensitive)
        if ends_with and href.lower().endswith(ends_with.lower()):
            full_url = urljoin(base_url, href) # Constrói URL completa
            if full_url not in found_urls:
                found_urls.append(full_url)
        # Se não busca por sufixo, pega todos os links válidos (usado para diretórios)
        elif ends_with is None and href.endswith('/'): # Considera apenas diretórios se ends_with não for especificado
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
        if any(filename.lower().startswith(tipo.lower()) for tipo in tipos):
            filtered_urls.append(url)
        else:
            logger.debug(f"Ignorando URL (tipo não desejado): {filename}")
            ignored_count += 1
    return filtered_urls, ignored_count

def get_latest_month_zip_urls(base_url: str) -> Tuple[List[str], str]:
    """Busca URLs de arquivos .zip na pasta AAAA-MM mais recente.

    1. Busca e parseia a URL base.
    2. Encontra links de diretórios.
    3. Filtra diretórios no formato AAAA-MM e encontra o mais recente.
    4. Busca e parseia a URL do diretório mais recente.
    5. Encontra links .zip nesse diretório.
    
    Returns:
        Tuple[List[str], str]: Lista de URLs e nome da pasta mais recente
    """
    zip_urls = []
    latest_month_folder_url = None
    year_month_folders = []

    # 1 & 2: Buscar e encontrar links de diretórios na URL base
    logger.info(f"Buscando diretórios em: {base_url}")
    base_soup = _fetch_and_parse(base_url)
    if not base_soup:
        return [], "" # Erro já logado em _fetch_and_parse

    directory_links = _find_links(base_soup, base_url, ends_with=None) # ends_with=None busca diretórios terminados em /

    # 3: Filtrar diretórios AAAA-MM e encontrar o mais recente
    for dir_url in directory_links:
        folder_name = dir_url.strip('/').split('/')[-1] # Pega o último componente do path
        match = re.fullmatch(r'(\d{4})-(\d{2})', folder_name)
        if match:
            year_month_folders.append((folder_name, dir_url))
            logger.debug(f"Encontrado diretório AAAA-MM: {folder_name} -> {dir_url}")

    if not year_month_folders:
        logger.warning(f"Nenhum diretório no formato AAAA-MM encontrado em {base_url}")
        return [], ""

    # Ordena pela chave (nome da pasta AAAA-MM) para encontrar o mais recente
    year_month_folders.sort(key=lambda x: x[0], reverse=True)
    latest_folder_name, latest_month_folder_url = year_month_folders[0]
    logger.info(f"Diretório mais recente encontrado: {latest_folder_name} ({latest_month_folder_url})")

    # 4 & 5: Buscar e encontrar links .zip no diretório mais recente
    logger.info(f"Buscando arquivos .zip em: {latest_month_folder_url}")
    latest_soup = _fetch_and_parse(latest_month_folder_url)
    if not latest_soup:
        return [], latest_folder_name # Erro já logado

    zip_urls = _find_links(latest_soup, latest_month_folder_url, ends_with='.zip')

    if not zip_urls:
        logger.warning(f"Nenhum arquivo .zip encontrado em {latest_month_folder_url}")

    logger.info(f"Total de {len(zip_urls)} URLs .zip encontradas na pasta {latest_folder_name}. ")
    return zip_urls, latest_folder_name # Retorna URLs e nome da pasta

async def _process_download_response(response: aiohttp.ClientResponse, destination_path: str, file_mode: str, progress: Progress, task_id: int, expected_size: int, initial_size: int, filename: str, remote_last_modified: int | None):
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
             size_to_download = None # Tamanho desconhecido, a barra Rich lidará com total=None

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
             logger.warning(f"Download de {filename} parece incompleto. Esperado: {current_task.total}, Obtido: {final_local_size}")
             # Marca a barra como erro (vermelho)
             progress.update(task_id, description=f"[red]{filename[:30]} (incompleto)[/red]", style="red")

        elif current_task.total and final_local_size > current_task.total:
             logger.warning(f"Download de {filename} maior que o esperado? Esperado: {current_task.total}, Obtido: {final_local_size}")
             # Marca a barra como aviso (amarelo)
             progress.update(task_id, description=f"[yellow]{filename[:30]} (tamanho>esperado)[/yellow]", style="yellow")

        # Garante que a barra chegue a 100% se o total era conhecido e foi atingido
        # Rich faz isso automaticamente se advance cobrir o total.

        # Marca a barra como concluída (verde)
        progress.update(task_id, description=f"[green]{filename[:30]} (concluído)[/green]", style="green")
        logger.info(f"Escrita do arquivo {filename} concluída (modo: {file_mode}). Tamanho final local: {final_local_size}")

        # 4. Atualizar data de modificação local e cache
        if remote_last_modified is not None:
            try:
                os.utime(destination_path, (time.time(), remote_last_modified))
            except Exception as e_utime:
                logger.warning(f"Não foi possível definir data de modificação para {filename}: {e_utime}")

        if config.cache.enabled:
            if final_local_size == expected_size:
                download_cache.update_file_cache(filename, expected_size, remote_last_modified)
                logger.debug(f"Cache atualizado para {filename}")
            else:
                 logger.error(f"Tamanho final do arquivo {filename} ({final_local_size}) difere do remoto esperado ({expected_size}). Cache não atualizado.")
                 # Marca a barra como erro se o cache falhar devido ao tamanho
                 progress.update(task_id, description=f"[red]{filename[:30]} (erro cache)[/red]", style="red")

        return destination_path, None

    except Exception as e_proc:
        logger.error(f"Erro durante o processamento da resposta/escrita para {filename}: {e_proc}")
        # Marca a barra como erro
        progress.update(task_id, description=f"[red]{filename[:30]} (ERRO escrita)[/red]", style="red")
        return filename, e_proc

async def download_file(session: aiohttp.ClientSession, url: str, destination_path: str, semaphore: asyncio.Semaphore, progress: Progress, task_id: int) -> Tuple[str, Exception | None, str | None]:
    """
    Downloads a single file asynchronously, updating a Rich progress bar.
    Attempts to resume download if local file is partial and server supports Range.
    Otherwise, downloads the entire file. Checks cache and metadata.
    Applies styles (colors) to the progress bar based on status.
    Returns:
        Tuple[str, Exception | None, str | None]: (Path/URL, Error or None, Skip Reason or None)
    """
    filename = os.path.basename(destination_path)
    skip_reason: str | None = None # Variável para armazenar o motivo do skip
    async with semaphore:
        file_mode = 'wb'
        resume_header = {}
        initial_size = 0
        skip_download = False

        try:
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

            if file_exists:
                # Checa cache primeiro (se habilitado)
                if config.cache.enabled and download_cache.is_file_cached(filename, remote_size, remote_last_modified):
                     if local_size == remote_size:
                         # logger.info(f"Arquivo {filename} já existe, atualizado e em cache. Pulando download.") # REMOVIDO
                         progress.update(task_id, description=f"[green]{filename[:30]} (cache)[/green]", completed=remote_size, style="green")
                         skip_download = True
                         skip_reason = "cache"
                     else:
                         # Cache diz que está ok, mas tamanho local difere -> Baixar completo
                         logger.warning(f"Arquivo {filename} em cache, mas tamanho local difere. Baixando completo.")
                         # Atualiza barra Rich (amarelo para cache inválido)
                         progress.update(task_id, description=f"[yellow]{filename[:30]} (cache inválido)[/yellow]", style="yellow")
                         file_mode = 'wb'
                         initial_size = 0
                # Sem cache ou cache desatualizado, verifica tamanho e data local
                elif local_size < remote_size:
                    # Arquivo parcial. Verifica data se possível
                    if remote_last_modified is not None:
                        local_last_modified = int(os.path.getmtime(destination_path))
                        if local_last_modified >= remote_last_modified:
                             # Parcial, mas local é mais novo ou igual? Estranho. Baixar completo.
                             logger.warning(f"Arquivo local parcial {filename} é mais recente que o remoto? Baixando completo.")
                             file_mode = 'wb'
                             initial_size = 0
                        else:
                             # Parcial e mais antigo que remoto -> Tentar retomar
                             logger.info(f"Arquivo local parcial {filename} encontrado. Tentando retomar download.")
                             # Atualiza barra Rich (ciano para retomar)
                             progress.update(task_id, description=f"[cyan]{filename[:30]} (retomando?)[/cyan]", style="cyan")
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
                              # logger.info(f"Arquivo local {filename} completo e atualizado. Pulando download.") # REMOVIDO
                              progress.update(task_id, description=f"[green]{filename[:30]} (local)[/green]", completed=remote_size, style="green")
                              skip_download = True
                              skip_reason = "local"
                          else:
                              logger.info(f"Arquivo local {filename} completo mas desatualizado. Baixando completo.")
                              # Atualiza barra Rich (amarelo para desatualizado)
                              progress.update(task_id, description=f"[yellow]{filename[:30]} (desatualizado)[/yellow]", style="yellow")
                              file_mode = 'wb'
                              initial_size = 0
                     else:
                          # logger.info(f"Arquivo local {filename} completo (sem data remota). Pulando download.") # REMOVIDO
                          progress.update(task_id, description=f"[green]{filename[:30]} (local s/ data)[/green]", completed=remote_size, style="green")
                          skip_download = True
                          skip_reason = "local (sem data)"
                else: # local_size > remote_size
                     logger.warning(f"Arquivo local {filename} maior que o remoto ({local_size} > {remote_size}). Baixando completo.")
                     # Atualiza barra Rich (amarelo para tamanho inválido)
                     progress.update(task_id, description=f"[yellow]{filename[:30]} (tamanho inválido)[/yellow]", style="yellow")
                     file_mode = 'wb'
                     initial_size = 0
            else:
                 # Arquivo não existe localmente -> Baixar completo
                 logger.info(f"Arquivo {filename} não encontrado localmente. Baixando completo.")
                 # Atualiza barra Rich (cinza para novo download)
                 progress.update(task_id, description=f"[grey]{filename[:30]} (novo download)[/grey]", style="white") # Usando branco como cor padrão
                 file_mode = 'wb'
                 initial_size = 0

            if skip_download:
                progress.update(task_id, completed=remote_size)
                if config.cache.enabled and not download_cache.is_file_cached(filename, remote_size, remote_last_modified):
                     download_cache.update_file_cache(filename, remote_size, remote_last_modified)
                # Retorna com o motivo do skip
                return destination_path, None, skip_reason

            # 3. Executar Download
            # Reinicia o progresso (completed=initial_size) e define a descrição/estilo
            progress.update(task_id,
                            description=f"[cyan]{filename[:30]} ({'retomando' if attempt_resume else 'baixando'}...)[/cyan]",
                            completed=initial_size, # Define o ponto de partida
                            style="cyan" if attempt_resume else "white") # Ciano para retomada, branco para normal

            async with session.get(url, headers=resume_header, timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)) as response:
                 if attempt_resume:
                     if response.status == 206:
                         logger.info(f"Servidor aceitou retomar download para {filename}.")
                         # Descrição e estilo já definidos acima
                     elif response.status == 200:
                         logger.warning(f"Servidor ignorou Range para {filename}. Baixando arquivo completo novamente.")
                         file_mode = 'wb'
                         initial_size = 0
                         # Reinicia progresso e atualiza descrição/estilo (amarelo)
                         progress.update(task_id, completed=0, description=f"[yellow]{filename[:30]} (baixando completo*)[/yellow]", style="yellow")
                     else:
                         logger.error(f"Falha ao tentar retomar {filename} (Status: {response.status}). Baixando arquivo completo como fallback.")
                         file_mode = 'wb'
                         initial_size = 0
                         # Reinicia progresso e atualiza descrição/estilo (vermelho)
                         progress.update(task_id, completed=0, description=f"[red]{filename[:30]} (fallback download)[/red]", style="red")
                         response.release()
                         async with session.get(url, timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)) as response_fallback:
                             response_fallback.raise_for_status()
                             return await _process_download_response(response_fallback, destination_path, file_mode, progress, task_id, remote_size, initial_size, filename, remote_last_modified)

                 response.raise_for_status()
                 # Download ocorreu, retorna sem motivo de skip
                 dest_path, error = await _process_download_response(response, destination_path, file_mode, progress, task_id, remote_size, initial_size, filename, remote_last_modified)
                 return dest_path, error, None # Adiciona None para skip_reason

        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            logger.error(f"Erro ao processar {filename} ({url}): {e}")
            # Atualiza barra Rich (vermelho para erro)
            progress.update(task_id, description=f"[red]{filename[:30]} (ERRO)[/red]", style="red")
            # Remove do cache em caso de erro
            if config.cache.enabled:
                download_cache.remove_file_from_cache(filename)
            # Considerar remover arquivo parcial?
            # if os.path.exists(destination_path):
            #    try: os.remove(destination_path) except OSError:
            # Retorna erro, sem motivo de skip
            return url, e, None # Adiciona None para skip_reason
        except Exception as e:
            logger.error(f"Erro inesperado ao processar {filename} ({url}): {e}")
            # Atualiza barra Rich (vermelho para erro inesperado)
            progress.update(task_id, description=f"[red]{filename[:30]} (ERRO Inesp.)[/red]", style="red")
            if config.cache.enabled:
                download_cache.remove_file_from_cache(filename)
            # Retorna erro, sem motivo de skip
            return url, e, None # Adiciona None para skip_reason

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

async def download_multiple_files(urls: List[str], destination_folder: str, max_concurrent: int = 5):
    """
    Downloads multiple files asynchronously with progress bars using Rich.
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
        logger.info(f"Diretório criado: {destination_folder}")

    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = []
    downloaded_files = []
    failed_downloads = []
    skipped_files = [] # Lista para guardar arquivos pulados e motivo

    # Configurar colunas do Rich Progress
    progress_columns = [
        TextColumn("[progress.description]{task.description}", justify="left"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•", # Readicionar separador
        DownloadColumn(),
        "•", # Readicionar separador
        TransferSpeedColumn(),
        "•", # Readicionar separador
        TimeRemainingColumn(),
    ]

    # Usar uma única sessão para todos os downloads
    async with aiohttp.ClientSession() as session:
        # Envolver o processo com o Progress do Rich
        with Progress(*progress_columns, transient=False) as progress: # transient=False para manter as barras após concluir
            for url in urls:
                filename = url.split('/')[-1]
                destination_path = os.path.join(destination_folder, filename)

                # Adicionar uma tarefa para este arquivo
                task_id = progress.add_task(f"[grey]{filename[:30]}", total=None, start=False) # Inicia sem total definido e não iniciada

                # Passar o progress e task_id para a função de download
                tasks.append(download_file(session, url, destination_path, semaphore, progress, task_id))

            results = await asyncio.gather(*tasks)
            for result in results:
                file_or_url, error, skip_reason = result # Desempacota o skip_reason
                if error is None:
                    # Se não houve erro, verifica se foi pulado
                    if skip_reason:
                        filename = os.path.basename(file_or_url) # Pega nome do arquivo do path
                        skipped_files.append((filename, skip_reason))
                    else:
                        # Download bem-sucedido (não pulado)
                        downloaded_files.append(file_or_url)
                else:
                    # Download falhou
                    failed_downloads.append((file_or_url, error))

    # --- Resumo dos Arquivos Pulados ---
    if skipped_files:
        logger.info(f"\n--- Resumo dos Arquivos Pulados ({len(skipped_files)}) ---")
        for filename, reason in skipped_files:
            if reason == "cache":
                logger.info(f"  - {filename}: Utilizado do cache (atualizado)")
            elif reason == "local":
                logger.info(f"  - {filename}: Já existe localmente (atualizado)")
            elif reason == "local (sem data)":
                 logger.info(f"  - {filename}: Já existe localmente (sem data remota para comparar)")
            else:
                 logger.info(f"  - {filename}: Pulado (motivo: {reason})")

    # --- Resumo Final --- (Movido para depois do resumo de skips)
    logger.info(f"\n--- Resumo Final dos Downloads ---")
    logger.info(f"Arquivos baixados/processados nesta execução: {len(downloaded_files)}")
    # Removido log individual de sucesso aqui, a barra já indica

    if failed_downloads:
        logger.warning(f"Total de downloads falhados: {len(failed_downloads)}")
        # Removido log individual de falha aqui, a barra já indica e foi logado na função

    # Mantém a contagem total
    logger.info(f"Total de arquivos pulados (cache/local): {len(skipped_files)}")

    return downloaded_files, failed_downloads

# Remover função de exemplo e bloco de execução direta
# async def main_example():
#     ...
#
# if __name__ == "__main__":
#     ... 