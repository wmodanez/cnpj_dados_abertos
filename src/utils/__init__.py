"""
Módulo de utilitários para o projeto.
"""
from .folders import check_basic_folders
from .files import file_extractor, file_delete, check_disk_space, estimate_zip_extracted_size 
from .network import check_internet_connection
from .cache import DownloadCache
from .cli import clear_cache, cache_info 
from .parallel import (
    process_csv_files_parallel, 
    process_csv_to_df, 
    verify_csv_integrity,
    process_dataframe_batch,
    optimize_dask_read
)
from .utils import create_parquet_filename
from .dask_manager import DaskManager

__all__ = [
    'create_parquet_filename',
    'file_delete',
    'check_disk_space',
    'estimate_zip_extracted_size',
    'process_csv_files_parallel',
    'process_csv_to_df',
    'verify_csv_integrity',
    'process_dataframe_batch',
    'optimize_dask_read',
    'DaskManager'
]