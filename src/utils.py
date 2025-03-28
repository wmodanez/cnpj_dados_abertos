import os
import glob
import zipfile
import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor
from config import config

logger = logging.getLogger(__name__)

def check_basic_folders(folder: str) -> None:
    """Verifica e cria pastas básicas se não existirem."""
    if not os.path.exists(folder):
        os.makedirs(folder)
        logger.info(f'Diretório criado: {folder}')

def file_extractor(folder_ori: str, folder_dest: str, filename: str = '*.*') -> None:
    """Extrai arquivos ZIP usando processamento paralelo."""
    zip_file_list: List[str] = list(glob.glob(os.path.join(folder_ori, filename)))
    
    def extract_file(zip_path: str) -> None:
        try:
            logger.info(f'Descompactando: {zip_path}')
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(folder_dest)
        except Exception as e:
            logger.error(f'Erro ao descompactar {zip_path}: {str(e)}')
    
    # Usa ThreadPoolExecutor para extrair arquivos em paralelo
    with ThreadPoolExecutor(max_workers=config.dask.n_workers) as executor:
        executor.map(extract_file, zip_file_list)

def file_delete(folder: str, filename: str = '*') -> None:
    """Remove arquivos usando processamento paralelo."""
    file_list: List[str] = list(glob.glob(os.path.join(folder, filename)))
    
    def delete_file(file_path: str) -> None:
        try:
            os.remove(file_path)
            logger.info(f'Arquivo removido: {file_path}')
        except Exception as e:
            logger.error(f'Erro ao remover arquivo {file_path}: {str(e)}')
    
    # Usa ThreadPoolExecutor para remover arquivos em paralelo
    with ThreadPoolExecutor(max_workers=config.dask.n_workers) as executor:
        executor.map(delete_file, file_list) 