import os
from dataclasses import dataclass, field
from typing import Dict, List, Any

# Lista de arquivos que devem ser ignorados durante o download
# Esta lista contém arquivos auxiliares que não são necessários para 
# o processamento principal dos dados de CNPJ
# Se necessário incluir ou remover arquivos desta lista, basta editar esta variável
IGNORED_FILES = [
    'cnaes.zip',         # Classificação Nacional de Atividades Econômicas - Tabela de referência
    'motivos.zip',       # Motivos de situação cadastral - Arquivo de apoio
    'municipios.zip',    # Lista de municípios do Brasil - Tabela auxiliar
    'naturezas.zip',     # Naturezas jurídicas - Arquivo de referência
    'paizes.zip',        # Lista de países e nacionalidades - Tabela auxiliar 
    'qualificacoes.zip'  # Qualificações de sócios e representantes - Arquivo de referência
]

@dataclass
class DaskConfig:
    n_workers: int = os.cpu_count() or 4
    threads_per_worker: int = 1
    memory_limit: str = '4GB'
    dashboard_address: str = ':8787'

@dataclass
class FileConfig:
    separator: str = ';'
    encoding: str = 'latin1'
    KB: int = 1024

@dataclass
class DatabaseConfig:
    threads: int = 4

@dataclass
class Config:
    dask: DaskConfig = field(default_factory=DaskConfig)
    file: FileConfig = field(default_factory=FileConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    
    # Configurações de colunas e tipos de dados
    empresa_columns: List[str] = field(default_factory=lambda: [
        'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
        'capital_social', 'porte_empresa', 'ente_federativo_responsavel'
    ])
    
    empresa_dtypes: Dict[str, str] = field(default_factory=lambda: {
        'cnpj_basico': 'string',
        'razao_social': 'string',
        'natureza_juridica': 'int',
        'qualificacao_responsavel': 'int',
        'capital_social': 'float',
        'porte_empresa': 'int',
        'ente_federativo_responsavel': 'string'
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
        'cnpj_basico': 'string',
        'cnpj_ordem': 'string',
        'cnpj_dv': 'string',
        'matriz_filial': 'int',
        'nome_fantasia': 'string',
        'codigo_situacao_cadastral': 'int',
        'data_situacao_cadastral': 'string',
        'codigo_motivo_situacao_cadastral': 'int',
        'nome_cidade_exterior': 'string',
        'pais': 'string',
        'data_inicio_atividades': 'string',
        'codigo_cnae': 'int',
        'cnae_secundaria': 'string',
        'tipo_logradouro': 'string',
        'logradouro': 'string',
        'numero': 'string',
        'complemento': 'string',
        'bairro': 'string',
        'cep': 'string',
        'uf': 'string',
        'codigo_municipio': 'int',
        'ddd1': 'string',
        'telefone1': 'string',
        'ddd2': 'string',
        'telefone2': 'string',
        'ddd_fax': 'string',
        'fax': 'string',
        'correio_eletronico': 'string',
        'situacao_especial': 'string',
        'data_situacao_especial': 'string'
    })
    
    simples_columns: List[str] = field(default_factory=lambda: [
        'cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
        'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
    ])
    
    simples_dtypes: Dict[str, str] = field(default_factory=lambda: {
        'cnpj_basico': 'int',
        'opcao_simples': 'string',
        'data_opcao_simples': 'string',
        'data_exclusao_simples': 'string',
        'opcao_mei': 'string',
        'data_opcao_mei': 'string',
        'data_exclusao_mei': 'string'
    })
    
    socio_columns: List[str] = field(default_factory=lambda: [
        'cnpj_basico', 'identificador_socio', 'nome_socio', 'cnpj_cpf_socio',
        'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
        'nome_representante', 'qualificacao_representante_legal', 'faixa_etaria'
    ])
    
    socio_dtypes: Dict[str, str] = field(default_factory=lambda: {
        'cnpj_basico': 'string',
        'identificador_socio': 'string',
        'nome_socio': 'string',
        'cnpj_cpf_socio': 'string',
        'qualificacao_socio': 'string',
        'data_entrada_sociedade': 'string',
        'pais': 'string',
        'representante_legal': 'string',
        'nome_representante': 'string',
        'qualificacao_representante_legal': 'string',
        'faixa_etaria': 'string'
    })

# Instância global de configuração
config = Config() 