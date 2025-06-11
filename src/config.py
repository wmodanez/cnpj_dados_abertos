import os
from dataclasses import dataclass, field
from typing import Dict, List, Type
import polars as pl

# Carregar variáveis de ambiente do arquivo .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Se python-dotenv não estiver instalado, continuar sem carregar
    pass


@dataclass
class FileConfig:
    separator: str = ';'
    encoding: str = 'utf8-lossy'
    KB: int = 1024


@dataclass
class DatabaseConfig:
    threads: int = 4


@dataclass
class CacheConfig:
    """Configurações relacionadas ao cache de downloads."""
    # Nome do arquivo de cache
    cache_file: str = 'download_cache.json'
    # Flag para habilitar ou desabilitar o cache
    enabled: bool = True
    # Diretório base onde serão armazenados os arquivos de cache.
    # Usa PATH_ZIP do .env, com fallback para 'data' na raiz.
    base_cache_dir: str = field(default_factory=lambda: os.getenv('PATH_ZIP', 'data'))
    # Caminho completo do arquivo de cache (calculado após init)
    cache_path: str = field(init=False)
    # Pasta remota atual (AAAA-MM)
    current_remote_folder: str = field(default="")

    def __post_init__(self):
        """Inicializa campos que dependem de outros campos."""
        self._update_cache_path()

    def _update_cache_path(self):
        """Atualiza o caminho do cache baseado na pasta remota atual."""
        if self.current_remote_folder:
            # Usar a pasta remota como diretório base
            self.cache_dir = os.path.join(self.base_cache_dir, self.current_remote_folder)
        else:
            # Usar diretório base se não houver pasta remota
            self.cache_dir = self.base_cache_dir

        # Garante que o diretório existe
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Atualiza o caminho completo do arquivo de cache
        self.cache_path = os.path.join(self.cache_dir, self.cache_file)

    def set_remote_folder(self, remote_folder: str):
        """Define a pasta remota atual e atualiza o caminho do cache."""
        self.current_remote_folder = remote_folder
        self._update_cache_path()


@dataclass
class PipelineConfig:
    """Configurações para otimização do pipeline de processamento."""
    
    # Configurações de streaming
    enable_streaming: bool = True
    streaming_chunk_size: int = 1000000  # 1M linhas por chunk
    max_memory_per_process_mb: int = 2048  # 2GB por processo
    
    # Configurações de cache inteligente
    enable_smart_cache: bool = True
    cache_cleanup_interval_hours: int = 24
    max_cache_size_gb: int = 10
    
    # Configurações adaptativas de concorrência
    adaptive_concurrency: bool = True
    min_download_workers: int = 2
    max_download_workers: int = 8
    min_process_workers: int = 1
    max_process_workers: int = 4
    
    # Configurações de I/O otimizado
    use_async_io: bool = True
    io_buffer_size: int = 8192 * 8  # 64KB buffer
    enable_compression: bool = True
    compression_level: int = 6  # Balanceio entre velocidade e compressão
    
    # Configurações de monitoramento
    enable_resource_monitoring: bool = True
    memory_threshold_percent: int = 85  # Pausar processamento se memória > 85%
    cpu_threshold_percent: int = 90     # Pausar processamento se CPU > 90%
    
    # Configurações de interface
    show_progress_bar: bool = True  # Exibir barras de progresso visuais (padrão: True)
    show_pending_files: bool = True  # Exibir lista de arquivos pendentes/em progresso (padrão: True)
    
    # Configurações de otimização específicas por tipo
    empresa_chunk_size: int = 500000
    estabelecimento_chunk_size: int = 1000000  # Arquivos maiores
    simples_chunk_size: int = 2000000
    socio_chunk_size: int = 750000


@dataclass
class Config:
    # Configurações de paralelismo
    n_workers: int = os.cpu_count() or 4
    memory_limit: str = '4GB'
    
    file: FileConfig = field(default_factory=FileConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)  # Nova configuração

    # Lista de arquivos a serem ignorados no download
    ignored_files: List[str] = field(default_factory=lambda: [
        'cnae', 'cnaes', 'pais', 'paises', 'munic', 'municipios', 'natju', 'naturezas', 
        'moti', 'motivos', 'qual', 'qualificacoes', 'porte', 'portes'
    ])

    # Configurações de colunas e tipos de dados
    empresa_columns: List[str] = field(default_factory=lambda: [
        'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
        'capital_social', 'porte_empresa', 'ente_federativo_responsavel'
    ])

    empresa_dtypes: Dict[str, Type] = field(default_factory=lambda: {
        'cnpj_basico': pl.Int64,
        'razao_social': pl.Utf8,
        'natureza_juridica': pl.Utf8,
        'qualificacao_responsavel': pl.Int64,
        'capital_social': pl.Utf8,  # Pode ser float, mas geralmente vem como string
        'porte_empresa': pl.Utf8,
        'ente_federativo_responsavel': pl.Utf8
    })

    estabelecimento_columns: List[str] = field(default_factory=lambda: [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia',
        'codigo_situacao_cadastral', 'data_situacao_cadastral', 'codigo_motivo_situacao_cadastral',
        'nome_cidade_exterior', 'pais', 'data_inicio_atividades', 'codigo_cnae',
        'cnae_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento',
        'bairro', 'cep', 'uf', 'codigo_municipio', 'ddd1', 'telefone1', 'ddd2',
        'telefone2', 'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial',
        'data_situacao_especial'
    ])

    estabelecimento_dtypes: Dict[str, Type] = field(default_factory=lambda: {
        'cnpj_basico': pl.Int64,
        'cnpj_ordem': pl.Utf8,
        'cnpj_dv': pl.Utf8,
        'matriz_filial': pl.Int64,
        'nome_fantasia': pl.Utf8,
        'codigo_situacao_cadastral': pl.Int64,
        'data_situacao_cadastral': pl.Utf8,
        'codigo_motivo_situacao_cadastral': pl.Int64,
        'nome_cidade_exterior': pl.Utf8,
        'pais': pl.Utf8,
        'data_inicio_atividades': pl.Utf8,
        'codigo_cnae': pl.Int64,
        'cnae_secundaria': pl.Utf8,
        'tipo_logradouro': pl.Utf8,
        'logradouro': pl.Utf8,
        'numero': pl.Utf8,
        'complemento': pl.Utf8,
        'bairro': pl.Utf8,
        'cep': pl.Utf8,
        'uf': pl.Utf8,
        'codigo_municipio': pl.Int64,
        'ddd1': pl.Utf8,
        'telefone1': pl.Utf8,
        'ddd2': pl.Utf8,
        'telefone2': pl.Utf8,
        'ddd_fax': pl.Utf8,
        'fax': pl.Utf8,
        'correio_eletronico': pl.Utf8,
        'situacao_especial': pl.Utf8,
        'data_situacao_especial': pl.Utf8
    })

    simples_columns: List[str] = field(default_factory=lambda: [
        'cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
        'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
    ])

    simples_dtypes: Dict[str, Type] = field(default_factory=lambda: {
        'cnpj_basico': pl.Int64,
        'opcao_simples': pl.Utf8,
        'data_opcao_simples': pl.Utf8,
        'data_exclusao_simples': pl.Utf8,
        'opcao_mei': pl.Utf8,
        'data_opcao_mei': pl.Utf8,
        'data_exclusao_mei': pl.Utf8
    })

    socio_columns: List[str] = field(default_factory=lambda: [
        'cnpj_basico', 'identificador_socio', 'nome_socio', 'cnpj_cpf_socio',
        'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
        'nome_representante', 'qualificacao_representante_legal', 'faixa_etaria'
    ])

    socio_dtypes: Dict[str, Type] = field(default_factory=lambda: {
        'cnpj_basico': pl.Int64,
        'identificador_socio': pl.Int64,
        'nome_socio': pl.Utf8,
        'cnpj_cpf_socio': pl.Utf8,
        'qualificacao_socio': pl.Int64,
        'data_entrada_sociedade': pl.Utf8,
        'pais': pl.Utf8,
        'representante_legal': pl.Utf8,
        'nome_representante': pl.Utf8,
        'qualificacao_representante_legal': pl.Int64,
        'faixa_etaria': pl.Utf8
    })


# Instância global de configuração
config = Config()
