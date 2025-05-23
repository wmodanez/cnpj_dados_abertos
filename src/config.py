import os
from dataclasses import dataclass, field
from typing import Dict, List
import polars as pl


@dataclass
class FileConfig:
    separator: str = ';'
    encoding: str = 'latin1'
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
    # Diretório onde será armazenado o arquivo de cache.
    # Usa PATH_ZIP do .env, com fallback para 'data' na raiz.
    # Cria um subdiretório 'cache' dentro do diretório base.
    cache_dir: str = field(default_factory=lambda: os.path.join(os.getenv('PATH_ZIP', 'data'), 'cache'))
    # Caminho completo do arquivo de cache (calculado após init)
    cache_path: str = field(init=False)

    def __post_init__(self):
        """Inicializa campos que dependem de outros campos."""
        # Garante que cache_dir existe antes de formar o path completo
        # Nota: A classe DownloadCache também cria o diretório ao salvar.
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_path = os.path.join(self.cache_dir, self.cache_file)


@dataclass
class Config:
    # Configurações de paralelismo
    n_workers: int = os.cpu_count() or 4
    memory_limit: str = '4GB'
    
    file: FileConfig = field(default_factory=FileConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)

    # Configurações de colunas e tipos de dados
    empresa_columns: List[str] = field(default_factory=lambda: [
        'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
        'capital_social', 'porte_empresa', 'ente_federativo_responsavel'
    ])

    empresa_dtypes: Dict[str, str] = field(default_factory=lambda: {
        'cnpj_basico': pl.Utf8,
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

    estabelecimento_dtypes: Dict[str, str] = field(default_factory=lambda: {
        'cnpj_basico': pl.Utf8,
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

    simples_dtypes: Dict[str, str] = field(default_factory=lambda: {
        'cnpj_basico': pl.Utf8,
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

    socio_dtypes: Dict[str, str] = field(default_factory=lambda: {
        'cnpj_basico': pl.Utf8,
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
