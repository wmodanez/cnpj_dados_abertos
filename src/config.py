from dataclasses import dataclass
from pathlib import Path
import os

@dataclass
class CacheConfig:
    enabled: bool = True
    cache_dir: str = str(Path.home() / '.cnpj_cache')
    max_age_days: int = 30

@dataclass
class DaskConfig:
    n_workers: int = 4  # Número de workers para processamento paralelo

@dataclass
class Config:
    path_download: str = os.path.join(os.getcwd(), 'data', 'download')
    path_unzip: str = os.path.join(os.getcwd(), 'data', 'unzip')
    path_result: str = os.path.join(os.getcwd(), 'data', 'result')
    path_temp: str = os.path.join(os.getcwd(), 'data', 'temp')
    
    cache: CacheConfig = CacheConfig()
    dask: DaskConfig = DaskConfig()
    
config = Config() 