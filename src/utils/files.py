"""
Funções relacionadas à manipulação de arquivos.
"""
import glob
import logging
import os
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple

# Importações locais do projeto
from ..config import config

logger = logging.getLogger(__name__)


def check_disk_space(path: str, required_mb: int) -> Tuple[bool, float]:
    """
    Verifica se há espaço suficiente em disco.
    
    Args:
        path: Caminho do diretório para verificar
        required_mb: Espaço mínimo necessário em MB
        
    Returns:
        Tupla (bool, float): Indica se há espaço suficiente e quanto espaço está disponível em MB
    """
    try:
        # Garante que o diretório existe
        os.makedirs(path, exist_ok=True)

        # Obtém informações de espaço do disco
        disk_usage = shutil.disk_usage(path)
        available_mb = disk_usage.free / (1024 * 1024)  # Converte bytes para MB

        # Verifica se há espaço suficiente
        if available_mb < required_mb:
            logger.warning(f"Espaço em disco insuficiente: {available_mb:.2f}MB disponível, {required_mb}MB necessário")
            return False, available_mb

        logger.info(f"Espaço em disco verificado: {available_mb:.2f}MB disponível, {required_mb}MB necessário")
        return True, available_mb
    except Exception as e:
        logger.error(f"Erro ao verificar espaço em disco: {str(e)}")
        return False, 0


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
    with ThreadPoolExecutor(max_workers=config.n_workers) as executor:
        executor.map(extract_file, zip_file_list)


def file_delete(folder: str, filename: str = '*') -> None:
    """Remove arquivos usando processamento paralelo."""
    file_list: List[str] = list(glob.glob(os.path.join(folder, filename)))

    def delete_file(file_path: str) -> None:
        try:
            os.remove(file_path)
            logger.debug(f'Arquivo removido: {file_path}')
        except Exception as e:
            logger.error(f'Erro ao remover arquivo {file_path}: {str(e)}')

    # Usa ThreadPoolExecutor para remover arquivos em paralelo
    with ThreadPoolExecutor(max_workers=config.n_workers) as executor:
        executor.map(delete_file, file_list)


def estimate_zip_extracted_size(zip_path: str) -> float:
    """
    Estima o tamanho que um arquivo ZIP ocupará quando extraído.
    
    Args:
        zip_path: Caminho para o arquivo ZIP
        
    Returns:
        float: Tamanho estimado em MB
    """
    try:
        total_size = 0
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for info in zip_ref.infolist():
                # O tamanho do arquivo descompactado está disponível em file_size
                total_size += info.file_size

        # Converte para MB
        total_size_mb = total_size / (1024 * 1024)

        # Adiciona uma margem de segurança (arquivos CSV podem expandir quando carregados)
        estimated_size_mb = total_size_mb * 1.1

        logger.info(f"Tamanho estimado do ZIP extraído {os.path.basename(zip_path)}: {estimated_size_mb:.2f}MB")
        return estimated_size_mb

    except zipfile.BadZipFile as e:
        logger.error(f"Arquivo ZIP corrompido ou inválido {zip_path}: {str(e)}")
        return 0
    except Exception as e:
        logger.error(f"Erro ao estimar tamanho do ZIP {zip_path}: {str(e)}")
        # Em caso de erro, retorna uma estimativa grande para forçar uma verificação cuidadosa
        return 10000  # 10GB como valor seguro
