from dataclasses import dataclass, field
from pathlib import Path
import os
from typing import Tuple

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
    
    cache: CacheConfig = field(default_factory=CacheConfig)
    dask: DaskConfig = field(default_factory=DaskConfig)
    
# Lista de arquivos a serem ignorados no download (ex: ['layout.pdf', ...])
IGNORED_FILES: Tuple[str, ...] = ()
    
config = Config() 