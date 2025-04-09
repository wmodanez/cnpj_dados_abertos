import dask
from dask.distributed import Client, LocalCluster
from typing import Optional

class DaskManager:
    _instance: Optional['DaskManager'] = None
    _client: Optional[Client] = None
    
    def __init__(self):
        raise RuntimeError("Use DaskManager.initialize() ou DaskManager.get_instance()")
    
    @classmethod
    def initialize(cls, n_workers: int, memory_limit: str, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls._instance._setup_dask_config()
            cls._instance._create_client(n_workers, memory_limit, **kwargs)
        return cls._instance
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            raise RuntimeError("DaskManager não foi inicializado")
        return cls._instance
    
    def _setup_dask_config(self):
        """Configurações otimizadas centralizadas do Dask"""
        dask.config.set({
            'distributed.worker.memory.target': 0.6,
            'distributed.worker.memory.spill': 0.7,
            'distributed.worker.memory.pause': 0.8,
            'distributed.worker.memory.terminate': 0.95,
            'distributed.comm.timeouts.connect': '30s',
            'distributed.comm.timeouts.tcp': '30s',
            'distributed.nanny.environ.MALLOC_TRIM_THRESHOLD_': '65536',
        })
    
    def _create_client(self, n_workers: int, memory_limit: str, **kwargs):
        """Cria e configura o cliente Dask"""
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=2,
            memory_limit=memory_limit,
            **kwargs
        )
        self._client = Client(cluster)
    
    @property
    def client(self) -> Client:
        return self._client
    
    def shutdown(self):
        """Encerra o cliente e limpa a instância"""
        if self._client:
            self._client.close()
            self._client = None
        DaskManager._instance = None
