"""
Módulo de utilitários.
"""
from .folders import check_basic_folders
from .files import file_extractor, file_delete, check_disk_space, estimate_zip_extracted_size 
from .network import check_internet_connection
from .cache import DownloadCache
from .cli import clear_cache, cache_info 