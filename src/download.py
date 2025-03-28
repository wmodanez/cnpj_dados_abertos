import datetime
import os
import sys
import time
import logging
import pycurl
import requests
from typing import Tuple, List
from concurrent.futures import ThreadPoolExecutor
from config import config

logger = logging.getLogger(__name__)

# Configurações de retry
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

def progress(download_t: float, download_d: float, upload_t: float, upload_d: float) -> None:
    """Exibe o progresso do download."""
    sys.stdout.write('Downloading: {}/{} kiB ({}%)\r'.format(
        str(int(download_d / config.file.KB)), 
        str(int(download_t / config.file.KB)),
        str(int(download_d / download_t * 100) if download_t > 0 else 0)
    ))
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
            logger.error(f'Erro ao tentar baixar {file_download} - Status Code: {response.status_code}')
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
        curl.setopt(pycurl.XFERINFOFUNCTION, progress)
        curl.setopt(pycurl.CONNECTTIMEOUT, 30)
        curl.setopt(pycurl.LOW_SPEED_TIME, 300)
        curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)

        # Verifica arquivo local
        file_local_size, file_local_last_modified = get_info_file(path_zip, file_download)
        file_local = path_zip + file_download

        logger.info(f'Iniciando download do arquivo: {file_download}')
        
        # Abre arquivo para download
        if file_local_size == 0:
            f = open(file_local, "wb")
        elif file_local_size > 0:
            if file_local_last_modified >= timestamp_last_modified:
                if file_local_size != int(response.headers['Content-Length']):
                    f = open(file_local, "ab")
                    curl.setopt(pycurl.RESUME_FROM, file_local_size)
                else:
                    logger.info(f'Arquivo {file_download} já está atualizado.')
                    return True
            else:
                f = open(file_local, "wb")
        
        curl.setopt(pycurl.WRITEDATA, f)

        try:
            curl.perform()
            logger.info(f'Download concluído com sucesso: {file_download}')
            return True
        except Exception as e:
            logger.error(f'Erro durante o download do arquivo {file_download}: {str(e)}')
            if retry_count < MAX_RETRIES:
                logger.info(f'Tentativa {retry_count + 1} de {MAX_RETRIES} para {file_download}')
                time.sleep(RETRY_DELAY)  # Aguarda antes de tentar novamente
                return download_file(file_info, retry_count + 1)
            return False
        finally:
            curl.close()
            f.close()
            os.utime(file_local, (timestamp_last_modified, timestamp_last_modified))
            sys.stdout.flush()
    except requests.exceptions.RequestException as e:
        logger.error(f'Erro na requisição HTTP para {file_download}: {str(e)}')
        if retry_count < MAX_RETRIES:
            logger.info(f'Tentativa {retry_count + 1} de {MAX_RETRIES} para {file_download}')
            time.sleep(RETRY_DELAY)
            return download_file(file_info, retry_count + 1)
        return False
    except Exception as e:
        logger.error(f'Erro inesperado ao processar {file_download}: {str(e)}')
        if retry_count < MAX_RETRIES:
            logger.info(f'Tentativa {retry_count + 1} de {MAX_RETRIES} para {file_download}')
            time.sleep(RETRY_DELAY)
            return download_file(file_info, retry_count + 1)
        return False

def check_download(link, file: str, url: str, path_zip: str) -> bool:
    """Verifica se o arquivo pode ser baixado."""
    if str(link.get('href')).endswith('.zip') and file in str(link.get('href')):
        file_download: str = link.get('href')
        file_url: str = url + file_download

        if not file_download.startswith('http'):
            return True
        else:
            logger.error(f'URL inválida para download: {file_download}')
            return False
    else:
        return True

def download_files_parallel(soup, file: str, url: str, path_zip: str) -> bool:
    """Realiza o download paralelo dos arquivos com retry."""
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
    
    # Realiza downloads em paralelo
    with ThreadPoolExecutor(max_workers=config.dask.n_workers) as executor:
        results = list(executor.map(download_file, files_to_download))
    
    # Verifica se todos os downloads foram bem sucedidos
    failed_downloads = [f for f, r in zip(files_to_download, results) if not r]
    if failed_downloads:
        logger.error(f'Falha ao baixar {len(failed_downloads)} arquivos após {MAX_RETRIES} tentativas')
        for file_info in failed_downloads:
            logger.error(f'Arquivo com falha: {file_info[0]}')
        return False
    
    return True 