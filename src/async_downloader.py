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
from tqdm.asyncio import tqdm

# Importações locais do projeto
from .config import config, IGNORED_FILES # Importar config - Ajuste para relativo
from .utils import DownloadCache # Importar cache
import datetime # Para lidar com timestamps
import time # Para time.strptime

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Instanciar o cache
# TODO: Considerar injetar a instância ao invés de criar globalmente, se necessário
download_cache = DownloadCache(config.cache.cache_dir)

# Configuração básica de logging (pode ser ajustada conforme necessário)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

def get_latest_month_zip_urls(base_url: str) -> List[str]:
    """Busca URLs de arquivos .zip na pasta AAAA-MM mais recente.

    1. Busca e parseia a URL base.
    2. Encontra links de diretórios.
    3. Filtra diretórios no formato AAAA-MM e encontra o mais recente.
    4. Busca e parseia a URL do diretório mais recente.
    5. Encontra links .zip nesse diretório.
    """
    zip_urls = []
    latest_month_folder_url = None
    year_month_folders = []

    # 1 & 2: Buscar e encontrar links de diretórios na URL base
    logger.info(f"Buscando diretórios em: {base_url}")
    base_soup = _fetch_and_parse(base_url)
    if not base_soup:
        return [] # Erro já logado em _fetch_and_parse

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
        return []

    # Ordena pela chave (nome da pasta AAAA-MM) para encontrar o mais recente
    year_month_folders.sort(key=lambda x: x[0], reverse=True)
    latest_folder_name, latest_month_folder_url = year_month_folders[0]
    logger.info(f"Diretório mais recente encontrado: {latest_folder_name} ({latest_month_folder_url})")

    # 4 & 5: Buscar e encontrar links .zip no diretório mais recente
    logger.info(f"Buscando arquivos .zip em: {latest_month_folder_url}")
    latest_soup = _fetch_and_parse(latest_month_folder_url)
    if not latest_soup:
        return [] # Erro já logado

    zip_urls = _find_links(latest_soup, latest_month_folder_url, ends_with='.zip')

    if not zip_urls:
        logger.warning(f"Nenhum arquivo .zip encontrado em {latest_month_folder_url}")

    logger.info(f"Total de {len(zip_urls)} URLs .zip encontradas na pasta {latest_folder_name}. ")
    return zip_urls # Retorna todas as URLs encontradas

async def _process_download_response(response: aiohttp.ClientResponse, destination_path: str, file_mode: str, progress_bar: tqdm, expected_size: int, initial_size: int, filename: str, remote_last_modified: int | None):
    """Processa o corpo da resposta HTTP e escreve no arquivo."""
    downloaded_size_since_start = 0 # Bytes baixados *nesta* execução
    try:
        # Verifica o tamanho real do conteúdo a ser baixado (útil se for 200 OK em vez de 206)
        content_length_header = response.headers.get('Content-Length')
        if content_length_header is not None:
            size_to_download = int(content_length_header)
            # Ajusta o total da barra se o servidor enviou algo diferente do esperado no HEAD
            # especialmente se estivermos no modo 'wb' após uma tentativa de Range falha
            if file_mode == 'wb' and size_to_download != expected_size:
                 logger.warning(f"Tamanho do conteúdo GET ({size_to_download}) para {filename} difere do HEAD ({expected_size}). Ajustando barra.")
                 progress_bar.total = size_to_download
                 progress_bar.reset() # Reseta contador para 0
                 initial_size = 0 # Garante que começamos a contar do zero
        else:
             size_to_download = None # Tamanho desconhecido

        async with aiofiles.open(destination_path, mode=file_mode) as f:
            async for chunk in response.content.iter_chunked(8192):
                await f.write(chunk)
                chunk_len = len(chunk)
                downloaded_size_since_start += chunk_len
                progress_bar.update(chunk_len)

        # Verificação final de tamanho
        final_local_size = initial_size + downloaded_size_since_start
        # Se o tamanho total esperado era conhecido (remote_size do HEAD original)
        if progress_bar.total and final_local_size < progress_bar.total:
             logger.warning(f"Download de {filename} parece incompleto. Esperado: {progress_bar.total}, Obtido: {final_local_size}")
             # Poderia lançar erro aqui ou tentar de novo? Por ora, só log.
             # raise ValueError("Download incompleto")
        elif progress_bar.total and final_local_size > progress_bar.total:
             logger.warning(f"Download de {filename} maior que o esperado? Esperado: {progress_bar.total}, Obtido: {final_local_size}")

        # Garante que a barra chegue a 100% se o total era conhecido e foi atingido
        if progress_bar.total and progress_bar.n < progress_bar.total:
            # Evita atualizar demais se o download foi maior que o esperado
            update_amount = max(0, progress_bar.total - progress_bar.n)
            progress_bar.update(update_amount)
        elif not progress_bar.total: # Se o total não era conhecido, apenas fecha
             pass # Barra indeterminada

        logger.info(f"Escrita do arquivo {filename} concluída (modo: {file_mode}). Tamanho final local: {final_local_size}")

        # 4. Atualizar data de modificação local e cache
        if remote_last_modified is not None:
            try:
                os.utime(destination_path, (time.time(), remote_last_modified))
            except Exception as e_utime:
                logger.warning(f"Não foi possível definir data de modificação para {filename}: {e_utime}")

        if config.cache.enabled:
            # Usa o tamanho esperado do HEAD original para o cache, assumindo que é o tamanho correto do arquivo completo
            if final_local_size == expected_size:
                download_cache.update_file_cache(filename, expected_size, remote_last_modified)
                logger.debug(f"Cache atualizado para {filename}")
            else:
                 logger.error(f"Tamanho final do arquivo {filename} ({final_local_size}) difere do remoto esperado ({expected_size}). Cache não atualizado.")
                 # Considerar remover o arquivo local e do cache se o tamanho estiver errado?
                 # download_cache.remove_file_from_cache(filename)
                 # if os.path.exists(destination_path): os.remove(destination_path)
                 # raise ValueError("Tamanho do arquivo baixado difere do remoto")


        return destination_path, None

    except Exception as e_proc:
        logger.error(f"Erro durante o processamento da resposta/escrita para {filename}: {e_proc}")
        # Retorna a URL e o erro para ser tratado no gather
        return filename, e_proc # Retorna filename pois destination_path pode não ser útil aqui

async def download_file(session: aiohttp.ClientSession, url: str, destination_path: str, semaphore: asyncio.Semaphore, progress_bar: tqdm):
    """
    Downloads a single file asynchronously, updating a progress bar.
    Attempts to resume download if local file is partial and server supports Range.
    Otherwise, downloads the entire file. Checks cache and metadata.
    """
    filename = os.path.basename(destination_path)
    async with semaphore:
        file_mode = 'wb'  # Default to overwrite
        resume_header = {}
        initial_size = 0
        skip_download = False # Flag para pular o bloco de download

        try:
            # 1. Obter metadados remotos
            progress_bar.set_description(f"{filename[:20]} (verificando...)", refresh=True)
            remote_size, remote_last_modified = await get_remote_file_metadata(session, url)

            if remote_size is None:
                 raise ValueError(f"Não foi possível obter metadados remotos para {url}")

            # 2. Verificar cache e arquivo local
            file_exists = os.path.exists(destination_path)
            local_size = os.path.getsize(destination_path) if file_exists else 0

            # Decidir se tenta retomar ou baixar completo
            attempt_resume = False

            if file_exists:
                # Checa cache primeiro (se habilitado)
                if config.cache.enabled and download_cache.is_file_cached(filename, remote_size, remote_last_modified):
                     if local_size == remote_size:
                         logger.info(f"Arquivo {filename} já existe, atualizado e em cache. Pulando download.")
                         progress_bar.set_description(f"{filename[:20]} (cache)", refresh=True)
                         skip_download = True
                     else:
                         # Cache diz que está ok, mas tamanho local difere -> Baixar completo
                         logger.warning(f"Arquivo {filename} em cache, mas tamanho local difere. Baixando completo.")
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
                     # Tamanho igual. Verifica data se possível.
                     if remote_last_modified is not None:
                          local_last_modified = int(os.path.getmtime(destination_path))
                          if local_last_modified >= remote_last_modified:
                              logger.info(f"Arquivo local {filename} completo e atualizado. Pulando download.")
                              skip_download = True
                          else:
                              logger.info(f"Arquivo local {filename} completo mas desatualizado. Baixando completo.")
                              file_mode = 'wb'
                              initial_size = 0
                     else:
                          # Tamanho igual, sem data remota -> Assume OK, pula.
                          logger.info(f"Arquivo local {filename} completo (sem data remota). Pulando download.")
                          skip_download = True
                else: # local_size > remote_size
                     logger.warning(f"Arquivo local {filename} maior que o remoto ({local_size} > {remote_size}). Baixando completo.")
                     file_mode = 'wb'
                     initial_size = 0
            else:
                 # Arquivo não existe localmente -> Baixar completo
                 logger.info(f"Arquivo {filename} não encontrado localmente. Baixando completo.")
                 file_mode = 'wb'
                 initial_size = 0

            # Pular se decidido acima
            if skip_download:
                progress_bar.total = remote_size # Define o total para a barra parecer 100%
                progress_bar.update(remote_size)
                # Garante atualização no cache se necessário (ex: cache desabilitado antes)
                if config.cache.enabled and not download_cache.is_file_cached(filename, remote_size, remote_last_modified):
                     download_cache.update_file_cache(filename, remote_size, remote_last_modified)
                return destination_path, None

            # 3. Executar Download (Completo ou Retomada)
            progress_bar.reset()
            progress_bar.total = remote_size
            progress_bar.update(initial_size) # Começa do tamanho inicial (0 ou local_size)
            progress_bar.set_description(f"{filename[:20]} ({'retomando' if attempt_resume else 'baixando'}...)", refresh=True)

            async with session.get(url, headers=resume_header, timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)) as response:

                 # Verificar resposta do servidor à tentativa de retomada
                 if attempt_resume:
                     if response.status == 206: # Partial Content - OK!
                         logger.info(f"Servidor aceitou retomar download para {filename}.")
                         # file_mode já é 'ab', initial_size já foi considerado na barra
                     elif response.status == 200: # OK - Servidor ignorou Range, enviou tudo
                         logger.warning(f"Servidor ignorou Range para {filename}. Baixando arquivo completo novamente.")
                         file_mode = 'wb'
                         initial_size = 0
                         progress_bar.reset() # Reseta barra
                         progress_bar.total = remote_size # Define total novamente
                         progress_bar.update(0) # Começa do 0
                         progress_bar.set_description(f"{filename[:20]} (baixando...)", refresh=True)
                         # Continua para ler e escrever o conteúdo completo
                     else: # 416 ou outro erro ao tentar retomar
                         logger.error(f"Falha ao tentar retomar {filename} (Status: {response.status}). Baixando arquivo completo como fallback.")
                         # Forçar download completo sem Range
                         file_mode = 'wb'
                         initial_size = 0
                         progress_bar.reset()
                         progress_bar.total = remote_size
                         progress_bar.update(0)
                         progress_bar.set_description(f"{filename[:20]} (baixando...)", refresh=True)
                         # Precisamos refazer a requisição GET sem o header Range
                         # Fechar a resposta atual e refazer
                         response.release() # Libera conexão
                         async with session.get(url, timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)) as response_fallback:
                             response_fallback.raise_for_status()
                             # Agora processa response_fallback
                             return await _process_download_response(response_fallback, destination_path, file_mode, progress_bar, remote_size, initial_size, filename, remote_last_modified)

                 # Se não estava tentando retomar, ou se fallback para 200 foi necessário
                 response.raise_for_status() # Levanta erro para 4xx/5xx aqui

                 # Processa a resposta (seja parcial ou completa)
                 return await _process_download_response(response, destination_path, file_mode, progress_bar, remote_size, initial_size, filename, remote_last_modified)


        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            logger.error(f"Erro ao processar {filename} ({url}): {e}")
            progress_bar.set_description(f"{filename[:20]} (ERRO)", refresh=True)
            # Remove do cache em caso de erro
            if config.cache.enabled:
                download_cache.remove_file_from_cache(filename)
            # Considerar remover arquivo parcial?
            # if os.path.exists(destination_path):
            #    try: os.remove(destination_path) except OSError:
            return url, e
        except Exception as e:
            logger.error(f"Erro inesperado ao processar {filename} ({url}): {e}")
            progress_bar.set_description(f"{filename[:20]} (ERRO Inesp.)", refresh=True)
            if config.cache.enabled:
                download_cache.remove_file_from_cache(filename)
            return url, e
        finally:
            if not progress_bar.disable and not skip_download: # Não fecha se pulou
                 progress_bar.close()

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
    Downloads multiple files asynchronously with progress bars.
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
        logger.info(f"Diretório criado: {destination_folder}")

    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = []
    downloaded_files = []
    failed_downloads = []

    progress_bars = {}

    # Usar uma única sessão para todos os downloads
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(urls):
            filename = url.split('/')[-1] # Usando método simples por enquanto
            destination_path = os.path.join(destination_folder, filename)

            # Criar uma barra de progresso tqdm para cada arquivo
            # unit='B', unit_scale=True -> mostra em KB, MB, etc.
            # leave=False -> remove a barra ao concluir (ou True para manter)
            # desc -> descrição curta do arquivo
            pbar = tqdm(total=None, unit='B', unit_scale=True, desc=f"{filename[:30]:<30}", leave=True, position=i)
            progress_bars[url] = pbar

            tasks.append(download_file(session, url, destination_path, semaphore, pbar))

        results = await asyncio.gather(*tasks)
        for result in results:
            file_or_url, error = result
            if error is None:
                downloaded_files.append(file_or_url)
            else:
                # A barra de erro já foi atualizada na função download_file
                failed_downloads.append((file_or_url, error))

    # Limpar barras de progresso que podem não ter sido fechadas por erros inesperados
    for pbar in progress_bars.values():
        if not pbar.disable:
            pbar.close()

    if failed_downloads:
        logger.warning(f"{len(failed_downloads)} downloads falharam.")
        # Os detalhes já foram loggados ou mostrados na barra de progresso

    logger.info(f"Total de downloads concluídos: {len(downloaded_files)}")
    return downloaded_files, failed_downloads

# Exemplo de uso (agora usando .env)
async def main_example():
    # Obter configurações do .env
    base_url = os.getenv('URL_ORIGIN')
    download_folder = os.getenv('PATH_ZIP')

    if not base_url:
        logger.error("Variável de ambiente URL_ORIGIN não definida no arquivo .env")
        return
    if not download_folder:
        logger.error("Variável de ambiente PATH_ZIP não definida no arquivo .env")
        return

    # Buscar as URLs dos arquivos .zip
    all_zip_urls = get_latest_month_zip_urls(base_url)
    # print(all_zip_urls) # Debug: Mostrar todas as URLs encontradas

    if not all_zip_urls:
        logger.warning("Nenhuma URL .zip encontrada na origem. Verifique a URL_ORIGIN ou a estrutura da página.")
        return

    # Filtrar as URLs pelos tipos desejados
    tipos_desejados = ("Empresas", "Estabelecimentos", "Simples", "Socios")
    zip_urls_to_download, ignored_count = _filter_urls_by_type(all_zip_urls, tipos_desejados)

    if not zip_urls_to_download:
        logger.warning(f"Nenhuma URL relevante para download encontrada após filtrar por tipos: {tipos_desejados}")
        return


    # Limitar a quantidade para teste inicial (opcional) - Aplicar após filtrar
    # zip_urls_to_download = zip_urls_to_download[:2]
    logger.info(f"Iniciando download de {len(zip_urls_to_download)} arquivos relevantes para {download_folder}...")

    # Definir o número máximo de downloads concorrentes
    max_concurrent_downloads = 5 # Ajuste conforme necessário

    downloaded, failed = await download_multiple_files(zip_urls_to_download, download_folder, max_concurrent=max_concurrent_downloads)

    print("\n--- Resumo Final ---")
    print(f"Arquivos baixados com sucesso: {len(downloaded)}")

    if failed:
        logger.warning(f"Total de downloads falhados: {len(failed)}")
        for file_or_url, error in failed:
            logger.error(f"Erro ao baixar {file_or_url}: {error}")

if __name__ == "__main__":
    # Para rodar este exemplo diretamente: python src/async_downloader.py
    # Nota: Em produção, chame download_multiple_files a partir do seu fluxo principal.
    try:
        asyncio.run(main_example())
    except KeyboardInterrupt:
        logger.info("Download interrompido pelo usuário.") 