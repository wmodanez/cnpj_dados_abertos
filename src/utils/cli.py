"""
Utilitários de linha de comando para o projeto.
"""
import os
import argparse
import logging
from .config import config
from .cache import DownloadCache

logger = logging.getLogger(__name__)

def setup_logging():
    """Configura o sistema de logs."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def clear_cache():
    """Limpa o cache de downloads."""
    setup_logging()
    
    cache = DownloadCache(config.cache.cache_path)
    
    if cache.clear_cache():
        logger.info(f"Cache limpo com sucesso: {config.cache.cache_path}")
        return True
    else:
        logger.error(f"Erro ao limpar o cache: {config.cache.cache_path}")
        return False

def cache_info():
    """Exibe informações sobre o cache de downloads."""
    setup_logging()
    
    cache = DownloadCache(config.cache.cache_path)
    cached_files = cache.get_cached_files()
    
    logger.info(f"Arquivo de cache: {config.cache.cache_path}")
    logger.info(f"Total de arquivos em cache: {len(cached_files)}")
    
    if cached_files:
        logger.info("Arquivos em cache:")
        for file in cached_files:
            file_info = cache.get_file_info(file)
            size_mb = file_info.get('size', 0) / (1024 * 1024)
            last_download = file_info.get('last_download', 'Desconhecido')
            logger.info(f"  - {file}: {size_mb:.2f}MB, último download: {last_download}")
    
    return True

def main():
    """Função principal para a interface de linha de comando."""
    parser = argparse.ArgumentParser(description='Utilitários para o processamento de dados da Receita Federal')
    subparsers = parser.add_subparsers(dest='command', help='Comandos disponíveis')
    
    # Comando para limpar o cache
    clear_parser = subparsers.add_parser('clear-cache', help='Limpa o cache de downloads')
    
    # Comando para exibir informações sobre o cache
    info_parser = subparsers.add_parser('cache-info', help='Exibe informações sobre o cache de downloads')
    
    args = parser.parse_args()
    
    if args.command == 'clear-cache':
        return clear_cache()
    elif args.command == 'cache-info':
        return cache_info()
    else:
        parser.print_help()
        return False

if __name__ == '__main__':
    main() 