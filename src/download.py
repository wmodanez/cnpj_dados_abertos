import datetime
import os
import sys
import time
import logging
import pycurl
import requests
from typing import Tuple, List, Dict
from concurrent.futures import ThreadPoolExecutor
from config import config
from .utils.colors import Colors

logger = logging.getLogger(__name__)

# Configurações de retry
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

# Dicionário para controlar a posição de cada download na tela
download_positions: Dict[str, int] = {}
current_position = 0

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
        # Verifica o arquivo remoto
        response = requests.head(file_url, timeout=30)
        if response.status_code != 200:
            move_cursor(download_positions[file_download])
            clear_line()
            sys.stdout.write(f"{Colors.RED}Erro ao tentar baixar {file_download} - Status Code: {response.status_code}{Colors.END}")
            move_cursor(current_position)
            return False

        # Obtém informações do arquivo remoto
        file_remote_last_modified: list = response.headers['Last-Modified'].split()
        file_remote_last_modified_time: str = str(file_remote_last_modified[4]).split(':')
        
        timestamp_last_modified: int = datetime.datetime(
            int(file_remote_last_modified[3]),
            int(time.strptime(file_remote_last_modified[2], '%b').tm_mon),
            int(file_remote_last_modified[1]),
            int(file_remote_last_modified_time[0]),
            int(file_remote_last_modified_time[1]),
            int(file_remote_last_modified_time[2])
        ).timestamp()
        
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
        file_local = path_zip + file_download
        
        # Abre arquivo para download
        if file_local_size == 0:
            f = open(file_local, "wb")
        elif file_local_size > 0:
            if file_local_last_modified >= timestamp_last_modified:
                if file_local_size != int(response.headers['Content-Length']):
                    f = open(file_local, "ab")
                    curl.setopt(pycurl.RESUME_FROM, file_local_size)
                else:
                    move_cursor(download_positions[file_download])
                    clear_line()
                    sys.stdout.write(f"{Colors.GREEN}Arquivo {file_download} já está atualizado.{Colors.END}")
                    move_cursor(current_position)
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
            return True
        except Exception as e:
            move_cursor(download_positions[file_download])
            clear_line()
            sys.stdout.write(f"{Colors.RED}Erro durante o download do arquivo {file_download}: {str(e)}{Colors.END}")
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
    if str(link.get('href')).endswith('.zip') and file in str(link.get('href')):
        file_download: str = link.get('href')
        file_url: str = url + file_download

        if not file_download.startswith('http'):
            return True
        else:
            logger.error(f'{Colors.RED}URL inválida para download: {file_download}{Colors.END}')
            return False
    else:
        return True

def download_files_parallel(soup, file: str, url: str, path_zip: str) -> bool:
    """Realiza o download paralelo dos arquivos com retry."""
    global download_positions, current_position
    
    # Reseta as variáveis globais
    download_positions = {}
    current_position = 0
    
    # Lista para armazenar informações dos arquivos a serem baixados
    files_to_download: List[Tuple[str, str, str, str]] = []
    
    # Coleta informações dos arquivos
    for link in [x for x in soup.find_all('a') if str(x.get('href')).endswith('.zip')]:
        if check_download(link, file, url, path_zip):
            file_download: str = link.get('href')
            file_url: str = url + file_download
            files_to_download.append((file_download, file_url, path_zip, file))
    
    if not files_to_download:
        return True
    
    # Configura a exibição do progresso
    setup_progress_display(files_to_download)
    
    # Realiza downloads em paralelo
    with ThreadPoolExecutor(max_workers=config.dask.n_workers) as executor:
        results = list(executor.map(download_file, files_to_download))
    
    # Move o cursor para depois da área de progresso
    move_cursor(current_position + 1)
    
    # Verifica se todos os downloads foram bem sucedidos
    failed_downloads = [f for f, r in zip(files_to_download, results) if not r]
    if failed_downloads:
        print(f"{Colors.RED}Falha ao baixar {len(failed_downloads)} arquivos após {MAX_RETRIES} tentativas{Colors.END}")
        for file_info in failed_downloads:
            print(f"{Colors.RED}Arquivo com falha: {file_info[0]}{Colors.END}")
        return False
    
    return True 