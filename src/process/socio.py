import logging
import os
import zipfile
import polars as pl
import numpy as np
import gc
import shutil

from ..config import config
from ..utils import file_delete, verify_csv_integrity

logger = logging.getLogger(__name__)

def process_socio(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa os dados de sócios usando Polars."""
    return process_socio_with_polars(path_zip, path_unzip, path_parquet)

def process_socio_with_polars(path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Implementação em Polars para processar os dados de sócios."""
    logger.info('=' * 50)
    logger.info(f'Iniciando processamento de SÓCIOS com Polars')
    logger.info('=' * 50)
    
    # Implementação com Polars vem aqui
    # ...
    
    # Versão simples para exemplo
    try:
        # Encontrar arquivos ZIP de sócios
        zip_files = [f for f in os.listdir(path_zip) 
                     if f.startswith('Socio') and f.endswith('.zip')]
        
        if not zip_files:
            logger.warning('Nenhum arquivo ZIP de Sócios encontrado.')
            return True
            
        # Limpar diretório de destino
        output_dir = os.path.join(path_parquet, 'socios')
        try:
            file_delete(output_dir)
            logger.info(f'Diretório {output_dir} limpo antes do processamento')
        except Exception as e:
            logger.warning(f'Não foi possível limpar o diretório {output_dir}: {str(e)}')
        
        # Garantir que o diretório existe
        os.makedirs(output_dir, exist_ok=True)
        
        # Implementar o processamento com Polars aqui
        # ...
        
        return True
    except Exception as e:
        logger.error(f'Erro no processamento de Sócios com Polars: {str(e)}')
        return False


def process_single_zip_polars(zip_file: str, path_zip: str, path_unzip: str, path_parquet: str) -> bool:
    """Processa um único arquivo ZIP com dados de sócios.
    
    Esta função é chamada pelo download assíncrono para processar arquivos
    imediatamente após o download.
    
    Args:
        zip_file: Nome do arquivo ZIP a ser processado
        path_zip: Caminho para o diretório contendo o arquivo ZIP
        path_unzip: Caminho para o diretório temporário de extração
        path_parquet: Caminho para o diretório onde os dados processados serão salvos
        
    Returns:
        bool: True se o processamento foi bem-sucedido, False caso contrário
    """
    pid = os.getpid()
    logger.info(f"[{pid}] Iniciando processamento de {zip_file} após download")
    extract_dir = os.path.join(path_unzip, os.path.splitext(zip_file)[0])
    
    try:
        # Criar diretório de extração
        os.makedirs(extract_dir, exist_ok=True)
        
        # Extrair o arquivo ZIP
        zip_path = os.path.join(path_zip, zip_file)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Criar o diretório de saída se não existir
        output_dir = os.path.join(path_parquet, 'socios')
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"[{pid}] Arquivo {zip_file} extraído com sucesso")
        
        # Aqui seria implementado o processamento completo dos arquivos
        # Por enquanto, apenas logamos o sucesso da extração
        
        return True
    except Exception as e:
        logger.error(f"[{pid}] Erro ao processar arquivo {zip_file}: {str(e)}")
        return False
    finally:
        # Limpar o diretório de extração
        if os.path.exists(extract_dir):
            try:
                shutil.rmtree(extract_dir)
            except Exception as e:
                logger.warning(f"[{pid}] Não foi possível limpar o diretório {extract_dir}: {str(e)}")
