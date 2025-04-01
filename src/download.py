import datetime
import os
import sys
import time
import logging
import pycurl
import requests
from typing import Tuple, List, Dict
from concurrent.futures import ThreadPoolExecutor
from config import config, IGNORED_FILES
from .utils.colors import Colors
from .utils import check_internet_connection, DownloadCache

logger = logging.getLogger(__name__)

# Configurações de retry
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

# Dicionário para controlar a posição de cada download na tela
download_positions: Dict[str, int] = {}
current_position = 0

# Inicializa o cache de downloads
download_cache = DownloadCache(config.cache.cache_path)

def move_cursor(position: int) -> None:
    """Move o cursor para uma posição específica."""
    sys.stdout.write(f"\033[{position};0H")
    sys.stdout.flush()

def clear_line() -> None:
    """Limpa a linha atual."""
    sys.stdout.write("\033[K")
    sys.stdout.flush()

def setup_progress_display(files: List[Tuple[str, str, str, str]]) -> None:
    """Configura a exibição do progresso para múltiplos downloads."""
    global current_position
    
    # Limpa o terminal
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()
    
    # Imprime cabeçalho
    print(f"{Colors.BLUE}Iniciando downloads em paralelo...{Colors.END}")
    print(f"{Colors.BLUE}{'=' * 80}{Colors.END}")
    
    # Inicializa posições para cada arquivo
    for i, file_info in enumerate(files):
        file_name = file_info[0]
        download_positions[file_name] = i + 3  # +3 para deixar espaço para o cabeçalho
        print(f"{Colors.CYAN}Aguardando início do download: {file_name}{Colors.END}")
    
    current_position = len(files) + 3
    print(f"{Colors.BLUE}{'=' * 80}{Colors.END}")
    
    # Move o cursor para a posição após a última linha
    move_cursor(current_position)

def progress(file_name: str, download_t: float, download_d: float, upload_t: float, upload_d: float) -> None:
    """Exibe o progresso do download."""
    if download_t > 0 and file_name in download_positions:
        progress = (download_d / download_t) * 100
        downloaded_mb = download_d / 1024 / 1024
        total_mb = download_t / 1024 / 1024
        
        # Salva a posição atual do cursor
        current = current_position
        
        # Move para a linha do arquivo
        move_cursor(download_positions[file_name])
        clear_line()
        
        # Mostra o progresso
        if progress < 100:
            color = Colors.CYAN
        else:
            color = Colors.GREEN
        sys.stdout.write(f"{color}Baixando {file_name}: {progress:.1f}% ({downloaded_mb:.1f}MB/{total_mb:.1f}MB){Colors.END}")
        
        # Retorna o cursor para a posição original
        move_cursor(current)
        sys.stdout.flush()

def get_info_file(path: str, filename: str) -> Tuple[int, int]:
    """Obtém informações do arquivo local."""
    if os.path.exists(path + filename):
        return os.stat(path + filename).st_size, os.stat(path + filename).st_mtime
    return 0, 0

def download_file(file_info: Tuple[str, str, str, str], retry_count: int = 0) -> bool:
    """Realiza o download de um arquivo específico com retry."""
    file_download, file_url, path_zip, file_name = file_info
    
    try:
        # Se for uma tentativa de retry, verifica a conexão com a internet novamente
        if retry_count > 0:
            move_cursor(download_positions[file_download])
            clear_line()
            sys.stdout.write(f"{Colors.YELLOW}Verificando conexão antes de tentar novamente: {file_download} (tentativa {retry_count}/{MAX_RETRIES}){Colors.END}")
            move_cursor(current_position)
            
            internet_ok, _ = check_internet_connection(timeout=3, max_retries=1)
            if not internet_ok:
                move_cursor(download_positions[file_download])
                clear_line()
                sys.stdout.write(f"{Colors.RED}Conexão com a internet instável. Aguardando {RETRY_DELAY} segundos antes de tentar novamente...{Colors.END}")
                move_cursor(current_position)
                time.sleep(RETRY_DELAY)
        
        # Verifica o arquivo remoto
        response = requests.head(file_url, timeout=30)
        if response.status_code != 200:
            move_cursor(download_positions[file_download])
            clear_line()
            sys.stdout.write(f"{Colors.RED}Erro ao tentar baixar {file_download} - Status Code: {response.status_code}{Colors.END}")
            move_cursor(current_position)
            return False

        # Obtém informações do arquivo remoto
        remote_size = int(response.headers.get('Content-Length', 0))
        file_remote_last_modified: list = response.headers['Last-Modified'].split()
        file_remote_last_modified_time: str = str(file_remote_last_modified[4]).split(':')
        
        timestamp_last_modified: int = int(datetime.datetime(
            int(file_remote_last_modified[3]),
            int(time.strptime(file_remote_last_modified[2], '%b').tm_mon),
            int(file_remote_last_modified[1]),
            int(file_remote_last_modified_time[0]),
            int(file_remote_last_modified_time[1]),
            int(file_remote_last_modified_time[2])
        ).timestamp())
        
        # Verifica se o arquivo já está em cache
        if config.cache.enabled and download_cache.is_file_cached(file_download, remote_size, timestamp_last_modified):
            move_cursor(download_positions[file_download])
            clear_line()
            sys.stdout.write(f"{Colors.GREEN}Arquivo {file_download} já está em cache e atualizado.{Colors.END}")
            move_cursor(current_position)
            
            # Verifica se o arquivo existe no sistema de arquivos
            file_local = os.path.join(path_zip, file_download)
            if os.path.exists(file_local) and os.path.getsize(file_local) == remote_size:
                # Arquivo já existe e está atualizado
                return True
            else:
                # Arquivo está em cache mas não existe no disco ou está corrompido
                # Vamos continuar o download
                sys.stdout.write(f"{Colors.YELLOW}Arquivo em cache mas não encontrado no disco. Baixando novamente...{Colors.END}")
        
        # Configura o cURL
        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, file_url)
        curl.setopt(pycurl.FOLLOWLOCATION, 1)
        curl.setopt(pycurl.MAXREDIRS, 5)
        curl.setopt(pycurl.NOPROGRESS, False)
        curl.setopt(pycurl.XFERINFOFUNCTION, lambda *args: progress(file_download, *args))
        curl.setopt(pycurl.CONNECTTIMEOUT, 30)
        curl.setopt(pycurl.LOW_SPEED_TIME, 300)
        curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)

        # Verifica arquivo local
        file_local_size, file_local_last_modified = get_info_file(path_zip, file_download)
        file_local = os.path.join(path_zip, file_download)
        
        # Abre arquivo para download
        if file_local_size == 0:
            f = open(file_local, "wb")
        elif file_local_size > 0:
            if file_local_last_modified >= timestamp_last_modified:
                if file_local_size != remote_size:
                    f = open(file_local, "ab")
                    curl.setopt(pycurl.RESUME_FROM, file_local_size)
                else:
                    move_cursor(download_positions[file_download])
                    clear_line()
                    sys.stdout.write(f"{Colors.GREEN}Arquivo {file_download} já está atualizado.{Colors.END}")
                    move_cursor(current_position)
                    
                    # Atualiza o cache
                    if config.cache.enabled:
                        download_cache.update_file_cache(file_download, remote_size, timestamp_last_modified)
                    
                    return True
            else:
                f = open(file_local, "wb")
        
        curl.setopt(pycurl.WRITEDATA, f)

        try:
            curl.perform()
            move_cursor(download_positions[file_download])
            clear_line()
            sys.stdout.write(f"{Colors.GREEN}Download concluído com sucesso: {file_download}{Colors.END}")
            move_cursor(current_position)
            
            # Atualiza o cache após download bem-sucedido
            if config.cache.enabled:
                download_cache.update_file_cache(file_download, remote_size, timestamp_last_modified)
            
            return True
        except pycurl.error as e:
            # Captura erros específicos do pycurl
            error_code, error_msg = e.args
            move_cursor(download_positions[file_download])
            clear_line()
            
            # Determina se o erro está relacionado à conexão
            connection_related = any(keyword in str(error_msg).lower() for keyword in ['timeout', 'connection', 'network', 'reset', 'refused'])
            
            if connection_related:
                sys.stdout.write(f"{Colors.RED}Erro de conexão durante o download do arquivo {file_download}: {error_msg}{Colors.END}")
            else:
                sys.stdout.write(f"{Colors.RED}Erro durante o download do arquivo {file_download}: {error_msg}{Colors.END}")
            
            # Remove do cache em caso de erro
            if config.cache.enabled:
                download_cache.remove_file_from_cache(file_download)
                
            if retry_count < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
                return download_file(file_info, retry_count + 1)
            move_cursor(current_position)
            return False
        except Exception as e:
            move_cursor(download_positions[file_download])
            clear_line()
            sys.stdout.write(f"{Colors.RED}Erro inesperado durante o download do arquivo {file_download}: {str(e)}{Colors.END}")
            
            # Remove do cache em caso de erro
            if config.cache.enabled:
                download_cache.remove_file_from_cache(file_download)
                
            if retry_count < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
                return download_file(file_info, retry_count + 1)
            move_cursor(current_position)
            return False
        finally:
            curl.close()
            f.close()
            os.utime(file_local, (timestamp_last_modified, timestamp_last_modified))
            sys.stdout.flush()
    except requests.exceptions.RequestException as e:
        move_cursor(download_positions[file_download])
        clear_line()
        
        # Determina se é um erro relacionado à conexão
        connection_related = any(keyword in str(e).lower() for keyword in ['timeout', 'connection', 'network', 'reset', 'refused'])
        
        if connection_related:
            sys.stdout.write(f"{Colors.RED}Erro de conexão na requisição para {file_download}: {str(e)}{Colors.END}")
        else:
            sys.stdout.write(f"{Colors.RED}Erro na requisição HTTP para {file_download}: {str(e)}{Colors.END}")
            
        if retry_count < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return download_file(file_info, retry_count + 1)
        move_cursor(current_position)
        return False
    except Exception as e:
        move_cursor(download_positions[file_download])
        clear_line()
        sys.stdout.write(f"{Colors.RED}Erro inesperado ao processar {file_download}: {str(e)}{Colors.END}")
        if retry_count < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return download_file(file_info, retry_count + 1)
        move_cursor(current_position)
        return False

def check_download(link, file: str, url: str, path_zip: str) -> bool:
    """Verifica se o arquivo pode ser baixado."""
    file_download: str = link.get('href')
    
    if str(file_download).endswith('.zip') and file in str(file_download):
        file_url: str = url + file_download

        if not file_download.startswith('http'):
            return True
        else:
            logger.error(f'{Colors.RED}URL inválida para download: {file_download}{Colors.END}')
            return False
    else:
        return False

def download_files_parallel(soup, file: str, url: str, path_zip: str) -> bool:
    """Realiza o download paralelo dos arquivos com retry."""
    global download_positions, current_position
    
    logger.info(f"Verificando conexão com a internet antes de iniciar o download...")
    internet_ok, message = check_internet_connection()
    if not internet_ok:
        logger.error(f"{Colors.RED}Falha na conexão com a internet: {message}{Colors.END}")
        logger.error(f"{Colors.RED}Não é possível continuar com o download. Verifique sua conexão e tente novamente.{Colors.END}")
        return False
    
    logger.info(f"{Colors.GREEN}Conexão com a internet verificada com sucesso: {message}{Colors.END}")
    
    # Reseta as variáveis globais
    download_positions = {}
    current_position = 0
    
    # Inicializa o cache se estiver habilitado
    if config.cache.enabled:
        logger.info(f"{Colors.BLUE}Cache de downloads habilitado. Utilizando arquivo: {config.cache.cache_path}{Colors.END}")
        cached_files = download_cache.get_cached_files()
        logger.info(f"{Colors.BLUE}Encontrados {len(cached_files)} arquivos em cache.{Colors.END}")
    else:
        logger.info(f"{Colors.YELLOW}Cache de downloads desabilitado.{Colors.END}")
    
    # Lista para armazenar informações dos arquivos a serem baixados
    files_to_download: List[Tuple[str, str, str, str]] = []
    
    # Lista para armazenar arquivos ignorados
    ignored_files = []
    
    # Coleta informações dos arquivos
    for link in [x for x in soup.find_all('a') if str(x.get('href')).endswith('.zip')]:
        file_name = link.get('href')
        
        # Verifica se o arquivo está na lista de ignorados
        if file_name in IGNORED_FILES:
            ignored_files.append(file_name)
            continue
            
        # Verifica se o arquivo corresponde ao tipo atual sendo processado
        if check_download(link, file, url, path_zip):
            file_download: str = file_name
            file_url: str = url + file_download
            files_to_download.append((file_download, file_url, path_zip, file))
    
    # Log de arquivos ignorados
    if ignored_files:
        ignored_count = len(ignored_files)
        logger.info(f"{Colors.YELLOW}Ignorando {ignored_count} arquivos conforme configuração:{Colors.END}")
        for ignored_file in ignored_files:
            logger.info(f"{Colors.YELLOW}   - {ignored_file}{Colors.END}")
    
    if not files_to_download:
        return True
    
    # Configura a exibição do progresso
    setup_progress_display(files_to_download)
    
    # Realiza downloads em paralelo
    with ThreadPoolExecutor(max_workers=config.dask.n_workers) as executor:
        results = list(executor.map(download_file, files_to_download))
    
    # Move o cursor para depois da área de progresso
    sys.stdout.write("\n" * 2)
    
    # Verifica todos os resultados
    if all(results):
        logger.info(f"{Colors.GREEN}Todos os arquivos foram baixados com sucesso!{Colors.END}")
        return True
    else:
        failed_count = results.count(False)
        logger.error(f"{Colors.RED}Falha ao baixar {failed_count} arquivos.{Colors.END}")
        return False 