"""
Módulo de utilitários para o projeto.
"""
from .folders import check_basic_folders
from .files import file_delete, check_disk_space, estimate_zip_extracted_size, delete_zip_after_extraction
from .network import check_internet_connection
from .cache import DownloadCache
from .cli import clear_cache, cache_info 
from .parallel import (
    process_csv_files_parallel, 
    process_csv_to_df, 
    verify_csv_integrity,
    process_dataframe_batch
)
from .utils import create_parquet_filename

__all__ = [
    'create_parquet_filename',
    'file_delete',
    'check_disk_space',
    'estimate_zip_extracted_size',
    'delete_zip_after_extraction',
    'process_csv_files_parallel',
    'process_csv_to_df',
    'verify_csv_integrity',
    'process_dataframe_batch'
]