# Sugestões de Refatoração - Processadores de Dados RF

## Contexto
Os arquivos `estabelecimento.py`, `socio.py`, `simples.py` e `empresa.py` compartilham muita lógica similar e têm oportunidades significativas de refatoração para melhorar manutenibilidade e reduzir duplicação de código.

## Análise da Estrutura Entity Existente

### Estado Atual da Pasta `src/Entity`
A pasta `src/Entity` contém arquivos vazios (0 bytes) que representam uma **oportunidade perdida** para implementar um padrão de entidades robusto:

```
src/Entity/
├── Empresa.py (0 bytes) - VAZIO
├── Estabelecimento.py (0 bytes) - VAZIO  
├── Socio.py (0 bytes) - VAZIO
├── Simples.py (0 bytes) - VAZIO
├── Municipio.py (0 bytes) - VAZIO
├── NaturezaJuridica.py (0 bytes) - VAZIO
├── QualificacaoSocio.py (0 bytes) - VAZIO
├── SituacaoCadastral.py (0 bytes) - VAZIO
├── Motivo.py (0 bytes) - VAZIO
├── UF.py (0 bytes) - VAZIO
├── Pais.py (0 bytes) - VAZIO
└── ... (outros arquivos vazios)
```

### Potencial da Estrutura Entity

A estrutura Entity pode ser **fundamental** para resolver os problemas identificados nos processadores, oferecendo:

1. **Validação de Dados Centralizada**: Cada entidade pode ter suas próprias regras de validação
2. **Transformações Tipadas**: Métodos específicos para cada tipo de transformação
3. **Serialização/Deserialização**: Conversão automática entre formatos
4. **Documentação Viva**: Cada entidade documenta sua estrutura de dados
5. **Reutilização**: Entidades podem ser usadas em diferentes contextos

## Problemas Identificados

1. **Duplicação Massiva de Código**
   - **Sistema de fila completo duplicado**: Todos os 4 arquivos têm implementações quase idênticas de:
     - `_processing_lock`, `_active_processes`, `_max_concurrent_processes`, `_process_queue`, `_workers_should_stop`
     - `get_system_resources()`, `can_start_processing()`, `add_to_process_queue()`
     - `process_queue_worker()`, `start_queue_worker()`
   - **Funções de processamento duplicadas**: `process_data_file()` implementada múltiplas vezes em cada arquivo
   - **Sistema de logging de recursos**: Funções como `log_system_resources_*()` são quase idênticas
   - **Tratamento de erros e imports**: Mesmos imports e estruturas de tratamento de erro
   - **Funções de processamento de dados**: `process_data_file_in_chunks()` replicada com pequenas variações

2. **Inconsistências Críticas**
   - **Parâmetros incompatíveis**: `create_private` é passado para todos os processadores, mas só é usado em `empresa.py`
   - **Assinaturas de função diferentes**: 
     - `estabelecimento.py`: `process_data_file(data_path, chunk_size, output_dir, zip_filename_prefix)`
     - `empresa.py`: `process_data_file(data_path)` e `process_data_file(data_file_path)`
     - `socio.py` e `simples.py`: `process_data_file(data_file_path)`
   - **Sistema de fila presente em todos**: Contrário ao que estava documentado, todos os 4 arquivos têm sistema de fila
   - **Diferentes abordagens para chunks**: Cada arquivo tem sua própria lógica de chunking
   - **Implementações ligeiramente diferentes**: Pequenas variações que causam bugs sutis

3. **Funcionalidades Específicas Confirmadas**
   - **`empresa.py`**: 
     - Lógica para extração de CPF da razão social
     - Suporte para criação de subset de empresas privadas (`create_private`)
     - Processamento específico para dados de empresas
   - **`estabelecimento.py`**: 
     - Suporte para subset por UF (`uf_subset`)
     - Processamento otimizado para arquivos grandes (>2GB)
     - Lógica específica para dados de estabelecimentos
   - **`socio.py` e `simples.py`**: 
     - Processamento mais simples, sem subsets específicos
     - Recebem parâmetro `create_private` mas não o utilizam

4. **Problemas de Manutenibilidade**
   - **Código total**: ~5.940 linhas nos 4 arquivos (empresa: 1.402, estabelecimento: 1.427, socio: 1.016, simples: 1.095)
   - **Duplicação estimada**: ~60-70% do código é duplicado ou muito similar
   - **Bugs propagados**: Correções precisam ser aplicadas em 4 lugares diferentes
   - **Testes complexos**: Cada arquivo precisa ser testado separadamente
   - **Documentação fragmentada**: Lógica similar documentada 4 vezes

5. **Problemas de Performance**
   - **Recursos desperdiçados**: Cada arquivo carrega suas próprias estruturas de controle
   - **Inconsistência de otimizações**: Melhorias aplicadas apenas em alguns arquivos
   - **Gerenciamento de memória**: Diferentes estratégias causam uso ineficiente de recursos

## Sugestões de Melhorias

### 1. Implementar Estrutura Entity Robusta

```python
# src/Entity/base.py
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Type
import polars as pl
from datetime import datetime

@dataclass
class BaseEntity(ABC):
    """Classe base para todas as entidades do sistema"""
    
    @classmethod
    @abstractmethod
    def get_column_names(cls) -> List[str]:
        """Retorna nomes das colunas da entidade"""
        pass
    
    @classmethod
    @abstractmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna tipos das colunas da entidade"""
        pass
    
    @classmethod
    @abstractmethod
    def get_transformations(cls) -> List[str]:
        """Retorna lista de transformações aplicáveis"""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Valida os dados da entidade"""
        pass
    
    @classmethod
    def from_dataframe_row(cls, row: Dict[str, Any]) -> 'BaseEntity':
        """Cria instância da entidade a partir de uma linha do DataFrame"""
        pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte entidade para dicionário"""
        pass

# src/Entity/Empresa.py
@dataclass
class Empresa(BaseEntity):
    """Entidade representando uma Empresa da Receita Federal"""
    
    cnpj_basico: str
    razao_social: str
    natureza_juridica: Optional[int] = None
    qualificacao_responsavel: Optional[int] = None
    capital_social: Optional[float] = None
    porte_empresa: Optional[int] = None
    ente_federativo_responsavel: Optional[str] = None
    cpf_extraido: Optional[str] = None  # CPF extraído da razão social
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        return [
            'cnpj_basico', 'razao_social', 'natureza_juridica', 
            'qualificacao_responsavel', 'capital_social', 'porte_empresa', 
            'ente_federativo_responsavel'
        ]
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        return {
            'cnpj_basico': pl.Utf8,
            'razao_social': pl.Utf8,
            'natureza_juridica': pl.Int32,
            'qualificacao_responsavel': pl.Int32,
            'capital_social': pl.Float64,
            'porte_empresa': pl.Int32,
            'ente_federativo_responsavel': pl.Utf8
        }
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        return ['extract_cpf', 'clean_razao_social', 'convert_capital_social']
    
    def validate(self) -> bool:
        """Valida dados da empresa"""
        if not self.cnpj_basico or len(self.cnpj_basico) != 8:
            return False
        if not self.razao_social or len(self.razao_social.strip()) == 0:
            return False
        if self.cpf_extraido and not self._validate_cpf(self.cpf_extraido):
            return False
        return True
    
    def _validate_cpf(self, cpf: str) -> bool:
        """Valida CPF extraído"""
        invalid_cpfs = [
            "00000000000", "11111111111", "22222222222", "33333333333",
            "44444444444", "55555555555", "66666666666", "77777777777",
            "88888888888", "99999999999"
        ]
        return cpf not in invalid_cpfs and len(cpf) == 11 and cpf.isdigit()
    
    def extract_cpf_from_razao_social(self) -> Optional[str]:
        """Extrai CPF da razão social"""
        import re
        cpf_pattern = r'(\d{11})'
        match = re.search(cpf_pattern, self.razao_social)
        if match:
            cpf = match.group(1)
            if self._validate_cpf(cpf):
                return cpf
        return None
    
    def clean_razao_social(self) -> str:
        """Remove CPF da razão social"""
        import re
        cpf_pattern = r'(\d{11})'
        return re.sub(cpf_pattern, '', self.razao_social).strip()

# src/Entity/Estabelecimento.py
@dataclass
class Estabelecimento(BaseEntity):
    """Entidade representando um Estabelecimento da Receita Federal"""
    
    cnpj_basico: str
    cnpj_ordem: str
    cnpj_dv: str
    matriz_filial: Optional[int] = None
    nome_fantasia: Optional[str] = None
    codigo_situacao_cadastral: Optional[int] = None
    data_situacao_cadastral: Optional[datetime] = None
    codigo_motivo_situacao_cadastral: Optional[int] = None
    nome_cidade_exterior: Optional[str] = None
    pais: Optional[str] = None
    data_inicio_atividades: Optional[datetime] = None
    codigo_cnae: Optional[int] = None
    cnae_secundaria: Optional[str] = None
    uf: Optional[str] = None
    codigo_municipio: Optional[int] = None
    cep: Optional[str] = None
    # Campos derivados
    cnpj: Optional[str] = field(init=False)  # CNPJ completo calculado
    
    def __post_init__(self):
        """Calcula campos derivados após inicialização"""
        self.cnpj = self.get_cnpj_completo()
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        return [
            'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia',
            'codigo_situacao_cadastral', 'data_situacao_cadastral', 'codigo_motivo_situacao_cadastral',
            'nome_cidade_exterior', 'pais', 'data_inicio_atividades', 'codigo_cnae',
            'cnae_secundaria', 'uf', 'codigo_municipio', 'cep'
        ]
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        return ['create_cnpj_completo', 'clean_cep', 'convert_dates']
    
    def validate(self) -> bool:
        """Valida dados do estabelecimento"""
        if not all([self.cnpj_basico, self.cnpj_ordem, self.cnpj_dv]):
            return False
        if len(self.cnpj_basico) != 8 or len(self.cnpj_ordem) != 4 or len(self.cnpj_dv) != 2:
            return False
        if self.uf and len(self.uf) != 2:
            return False
        return True
    
    def get_cnpj_completo(self) -> str:
        """Gera CNPJ completo formatado"""
        if all([self.cnpj_basico, self.cnpj_ordem, self.cnpj_dv]):
            return f"{self.cnpj_basico.zfill(8)}{self.cnpj_ordem.zfill(4)}{self.cnpj_dv.zfill(2)}"
        return ""
    
    def clean_cep(self) -> str:
        """Limpa e formata CEP"""
        if self.cep:
            import re
            return re.sub(r'[^\d]', '', self.cep)
        return ""

# src/Entity/Socio.py
@dataclass
class Socio(BaseEntity):
    """Entidade representando um Sócio da Receita Federal"""
    
    cnpj_basico: str
    identificador_socio: Optional[int] = None
    nome_socio: Optional[str] = None
    cnpj_cpf_socio: Optional[str] = None
    qualificacao_socio: Optional[int] = None
    data_entrada_sociedade: Optional[datetime] = None
    pais: Optional[str] = None
    representante_legal: Optional[str] = None
    nome_representante: Optional[str] = None
    qualificacao_representante_legal: Optional[int] = None
    faixa_etaria: Optional[str] = None
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        return [
            'cnpj_basico', 'identificador_socio', 'nome_socio', 'cnpj_cpf_socio',
            'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
            'nome_representante', 'qualificacao_representante_legal', 'faixa_etaria'
        ]
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        return ['convert_dates', 'validate_cpf_cnpj']
    
    def validate(self) -> bool:
        """Valida dados do sócio"""
        if not self.cnpj_basico or len(self.cnpj_basico) != 8:
            return False
        if self.cnpj_cpf_socio and not self._validate_cpf_cnpj(self.cnpj_cpf_socio):
            return False
        return True
    
    def _validate_cpf_cnpj(self, documento: str) -> bool:
        """Valida CPF ou CNPJ do sócio"""
        if len(documento) == 11:  # CPF
            return self._validate_cpf(documento)
        elif len(documento) == 14:  # CNPJ
            return self._validate_cnpj(documento)
        return False

# src/Entity/Simples.py
@dataclass
class Simples(BaseEntity):
    """Entidade representando dados do Simples Nacional"""
    
    cnpj_basico: str
    opcao_simples: Optional[str] = None
    data_opcao_simples: Optional[datetime] = None
    data_exclusao_simples: Optional[datetime] = None
    opcao_mei: Optional[str] = None
    data_opcao_mei: Optional[datetime] = None
    data_exclusao_mei: Optional[datetime] = None
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        return [
            'cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
            'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
        ]
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        return ['convert_dates']
    
    def validate(self) -> bool:
        """Valida dados do Simples Nacional"""
        if not self.cnpj_basico or len(self.cnpj_basico) != 8:
            return False
        return True
```

### 2. Classe Base Abstrata Integrada com Entidades

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Type
import polars as pl
from ..Entity.base import BaseEntity

class BaseProcessor(ABC):
    def __init__(self, path_zip: str, path_unzip: str, path_parquet: str, **kwargs):
        self.path_zip = path_zip
        self.path_unzip = path_unzip
        self.path_parquet = path_parquet
        self.options = kwargs
        self.logger = logging.getLogger(self.__class__.__name__)
        self._validate_options()
        
    @abstractmethod
    def get_entity_class(self) -> Type[BaseEntity]:
        """Retorna a classe de entidade associada ao processador"""
        pass
        
    @abstractmethod
    def get_valid_options(self) -> list:
        """Retorna lista de opções válidas para este processador"""
        pass
        
    def apply_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica transformações usando a entidade associada"""
        entity_class = self.get_entity_class()
        
        # Renomear colunas para corresponder à entidade
        column_mapping = self._get_column_mapping(df, entity_class)
        if column_mapping:
            df = df.select([
                pl.col(old_col).alias(new_col) 
                for old_col, new_col in column_mapping.items()
            ])
        
        # Aplicar transformações específicas da entidade
        transformations = entity_class.get_transformations()
        for transformation in transformations:
            df = self._apply_transformation(df, transformation, entity_class)
        
        # Converter tipos conforme definido na entidade
        df = self._convert_types(df, entity_class)
        
        return df
    
    def _get_column_mapping(self, df: pl.DataFrame, entity_class: Type[BaseEntity]) -> Dict[str, str]:
        """Mapeia colunas do DataFrame para nomes da entidade"""
        entity_columns = entity_class.get_column_names()
        df_columns = df.columns
        
        # Mapear column_1, column_2, etc. para nomes reais
        mapping = {}
        for i, entity_col in enumerate(entity_columns):
            df_col = f"column_{i+1}"
            if df_col in df_columns:
                mapping[df_col] = entity_col
        
        return mapping
    
    def _apply_transformation(self, df: pl.DataFrame, transformation: str, entity_class: Type[BaseEntity]) -> pl.DataFrame:
        """Aplica transformação específica baseada na entidade"""
        # Implementação específica para cada tipo de transformação
        # Pode ser expandida conforme necessário
        return df
    
    def _convert_types(self, df: pl.DataFrame, entity_class: Type[BaseEntity]) -> pl.DataFrame:
        """Converte tipos conforme definido na entidade"""
        type_mapping = entity_class.get_column_types()
        
        conversions = []
        for col_name, col_type in type_mapping.items():
            if col_name in df.columns:
                conversions.append(pl.col(col_name).cast(col_type, strict=False))
        
        if conversions:
            df = df.with_columns(conversions)
        
        return df

class EmpresaProcessor(BaseProcessor):
    def get_entity_class(self) -> Type[BaseEntity]:
        from ..Entity.Empresa import Empresa
        return Empresa
    
    def get_valid_options(self) -> list:
        return ['create_private']
    
    def apply_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        # Aplicar transformações base
        df = super().apply_transformations(df)
        
        # Aplicar transformações específicas de empresa
        if 'razao_social' in df.columns:
            # Extração de CPF
            cpf_pattern = r'(\d{11})'
            df = df.with_columns([
                pl.col("razao_social")
                .str.extract(cpf_pattern, 1)
                .alias("cpf_extraido")
            ])
            
            # Validar CPFs extraídos
            invalid_cpfs = [
                "00000000000", "11111111111", "22222222222", "33333333333",
                "44444444444", "55555555555", "66666666666", "77777777777",
                "88888888888", "99999999999"
            ]
            
            df = df.with_columns([
                pl.when(pl.col("cpf_extraido").is_in(invalid_cpfs))
                .then(None)
                .otherwise(pl.col("cpf_extraido"))
                .alias("cpf_extraido")
            ])
            
            # Remover CPF da razão social
            df = df.with_columns([
                pl.col("razao_social")
                .str.replace_all(cpf_pattern, "")
                .str.strip_chars()
                .alias("razao_social")
            ])
        
        return df
```

### 3. Sistema de Validação Integrado com Schemas

```python
# src/Entity/schemas.py
from pydantic import BaseModel, Field, validator, root_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import re

class EmpresaSchema(BaseModel):
    """Schema de validação para Empresa usando Pydantic"""
    
    cnpj_basico: str = Field(..., min_length=8, max_length=8, regex=r'^\d{8}$')
    razao_social: str = Field(..., min_length=1, max_length=500)
    natureza_juridica: Optional[int] = Field(None, ge=1, le=9999)
    qualificacao_responsavel: Optional[int] = Field(None, ge=1, le=99)
    capital_social: Optional[float] = Field(None, ge=0)
    porte_empresa: Optional[int] = Field(None, ge=1, le=5)
    ente_federativo_responsavel: Optional[str] = Field(None, max_length=100)
    cpf_extraido: Optional[str] = Field(None, regex=r'^\d{11}$')
    
    class Config:
        # Permitir campos extras durante parsing
        extra = "ignore"
        # Validar na atribuição
        validate_assignment = True
        # Usar enum por valor
        use_enum_values = True
    
    @validator('cpf_extraido')
    def validate_cpf(cls, v):
        """Valida CPF extraído"""
        if v is None:
            return v
            
        invalid_cpfs = [
            "00000000000", "11111111111", "22222222222", "33333333333",
            "44444444444", "55555555555", "66666666666", "77777777777",
            "88888888888", "99999999999"
        ]
        
        if v in invalid_cpfs:
            raise ValueError(f'CPF inválido: {v}')
        
        return v
    
    @validator('razao_social')
    def validate_razao_social(cls, v):
        """Valida razão social"""
        if not v or not v.strip():
            raise ValueError('Razão social não pode estar vazia')
        
        # Verificar se não contém apenas números (possível erro de parsing)
        if v.strip().isdigit():
            raise ValueError('Razão social não pode conter apenas números')
        
        return v.strip()
    
    @root_validator
    def validate_empresa_consistency(cls, values):
        """Validações que dependem de múltiplos campos"""
        cnpj_basico = values.get('cnpj_basico')
        razao_social = values.get('razao_social')
        
        # Verificar consistência entre CNPJ e razão social
        if cnpj_basico and razao_social:
            # Empresas com CNPJ iniciado em '00' geralmente são especiais
            if cnpj_basico.startswith('00') and len(razao_social) < 10:
                raise ValueError('Empresas com CNPJ especial devem ter razão social mais detalhada')
        
        return values

class EstabelecimentoSchema(BaseModel):
    """Schema de validação para Estabelecimento"""
    
    cnpj_basico: str = Field(..., regex=r'^\d{8}$')
    cnpj_ordem: str = Field(..., regex=r'^\d{4}$')
    cnpj_dv: str = Field(..., regex=r'^\d{2}$')
    matriz_filial: Optional[int] = Field(None, ge=1, le=2)  # 1=Matriz, 2=Filial
    nome_fantasia: Optional[str] = Field(None, max_length=300)
    codigo_situacao_cadastral: Optional[int] = Field(None, ge=1, le=99)
    data_situacao_cadastral: Optional[datetime] = None
    codigo_motivo_situacao_cadastral: Optional[int] = Field(None, ge=1, le=99)
    nome_cidade_exterior: Optional[str] = Field(None, max_length=100)
    pais: Optional[str] = Field(None, max_length=100)
    data_inicio_atividades: Optional[datetime] = None
    codigo_cnae: Optional[int] = Field(None, ge=1, le=9999999)
    cnae_secundaria: Optional[str] = Field(None, max_length=1000)
    uf: Optional[str] = Field(None, regex=r'^[A-Z]{2}$')
    codigo_municipio: Optional[int] = Field(None, ge=1, le=999999)
    cep: Optional[str] = Field(None, regex=r'^\d{8}$')
    
    class Config:
        extra = "ignore"
        validate_assignment = True
    
    @validator('uf')
    def validate_uf(cls, v):
        """Valida UF brasileira"""
        if v is None:
            return v
            
        ufs_validas = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        
        if v not in ufs_validas:
            raise ValueError(f'UF inválida: {v}')
        
        return v
    
    @validator('cep')
    def validate_cep(cls, v):
        """Valida CEP"""
        if v is None:
            return v
        
        # Remover caracteres não numéricos
        cep_clean = re.sub(r'[^\d]', '', v)
        
        if len(cep_clean) != 8:
            raise ValueError(f'CEP deve ter 8 dígitos: {v}')
        
        return cep_clean
    
    @root_validator
    def validate_cnpj_parts(cls, values):
        """Valida partes do CNPJ"""
        cnpj_basico = values.get('cnpj_basico')
        cnpj_ordem = values.get('cnpj_ordem')
        cnpj_dv = values.get('cnpj_dv')
        
        if all([cnpj_basico, cnpj_ordem, cnpj_dv]):
            # Validar CNPJ completo usando algoritmo
            cnpj_completo = f"{cnpj_basico}{cnpj_ordem}{cnpj_dv}"
            if not cls._validate_cnpj_algorithm(cnpj_completo):
                raise ValueError(f'CNPJ inválido: {cnpj_completo}')
        
        return values
    
    @staticmethod
    def _validate_cnpj_algorithm(cnpj: str) -> bool:
        """Valida CNPJ usando algoritmo oficial"""
        # Implementação do algoritmo de validação de CNPJ
        if len(cnpj) != 14:
            return False
        
        # Verificar se não são todos iguais
        if cnpj == cnpj[0] * 14:
            return False
        
        # Calcular primeiro dígito verificador
        sequence = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum_result = sum(int(cnpj[i]) * sequence[i] for i in range(12))
        remainder = sum_result % 11
        first_digit = 0 if remainder < 2 else 11 - remainder
        
        if int(cnpj[12]) != first_digit:
            return False
        
        # Calcular segundo dígito verificador
        sequence = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum_result = sum(int(cnpj[i]) * sequence[i] for i in range(13))
        remainder = sum_result % 11
        second_digit = 0 if remainder < 2 else 11 - remainder
        
        return int(cnpj[13]) == second_digit

class SocioSchema(BaseModel):
    """Schema de validação para Sócio"""
    
    cnpj_basico: str = Field(..., regex=r'^\d{8}$')
    identificador_socio: Optional[int] = Field(None, ge=1, le=9)
    nome_socio: Optional[str] = Field(None, max_length=300)
    cnpj_cpf_socio: Optional[str] = Field(None, regex=r'^\d{11}$|^\d{14}$')
    qualificacao_socio: Optional[int] = Field(None, ge=1, le=99)
    data_entrada_sociedade: Optional[datetime] = None
    pais: Optional[str] = Field(None, max_length=100)
    representante_legal: Optional[str] = Field(None, max_length=11)
    nome_representante: Optional[str] = Field(None, max_length=300)
    qualificacao_representante_legal: Optional[int] = Field(None, ge=1, le=99)
    faixa_etaria: Optional[str] = Field(None, max_length=2)
    
    class Config:
        extra = "ignore"
        validate_assignment = True
    
    @validator('cnpj_cpf_socio')
    def validate_cnpj_cpf(cls, v):
        """Valida CPF ou CNPJ do sócio"""
        if v is None:
            return v
        
        if len(v) == 11:  # CPF
            return cls._validate_cpf(v)
        elif len(v) == 14:  # CNPJ
            return cls._validate_cnpj(v)
        else:
            raise ValueError(f'Documento deve ter 11 (CPF) ou 14 (CNPJ) dígitos: {v}')
    
    @staticmethod
    def _validate_cpf(cpf: str) -> str:
        """Valida CPF"""
        invalid_cpfs = [
            "00000000000", "11111111111", "22222222222", "33333333333",
            "44444444444", "55555555555", "66666666666", "77777777777",
            "88888888888", "99999999999"
        ]
        
        if cpf in invalid_cpfs:
            raise ValueError(f'CPF inválido: {cpf}')
        
        return cpf
    
    @staticmethod
    def _validate_cnpj(cnpj: str) -> str:
        """Valida CNPJ usando mesmo algoritmo do EstabelecimentoSchema"""
        if not EstabelecimentoSchema._validate_cnpj_algorithm(cnpj):
            raise ValueError(f'CNPJ inválido: {cnpj}')
        return cnpj

class SimplesSchema(BaseModel):
    """Schema de validação para Simples Nacional"""
    
    cnpj_basico: str = Field(..., regex=r'^\d{8}$')
    opcao_simples: Optional[str] = Field(None, regex=r'^[SN]$')
    data_opcao_simples: Optional[datetime] = None
    data_exclusao_simples: Optional[datetime] = None
    opcao_mei: Optional[str] = Field(None, regex=r'^[SN]$')
    data_opcao_mei: Optional[datetime] = None
    data_exclusao_mei: Optional[datetime] = None
    
    class Config:
        extra = "ignore"
        validate_assignment = True
    
    @root_validator
    def validate_dates_consistency(cls, values):
        """Valida consistência entre datas"""
        data_opcao_simples = values.get('data_opcao_simples')
        data_exclusao_simples = values.get('data_exclusao_simples')
        data_opcao_mei = values.get('data_opcao_mei')
        data_exclusao_mei = values.get('data_exclusao_mei')
        
        # Validar Simples Nacional
        if data_opcao_simples and data_exclusao_simples:
            if data_exclusao_simples <= data_opcao_simples:
                raise ValueError('Data de exclusão do Simples deve ser posterior à data de opção')
        
        # Validar MEI
        if data_opcao_mei and data_exclusao_mei:
            if data_exclusao_mei <= data_opcao_mei:
                raise ValueError('Data de exclusão do MEI deve ser posterior à data de opção')
        
        return values

# src/Entity/validation.py
from typing import Dict, Any, List, Type, Union
import polars as pl
from pydantic import BaseModel, ValidationError
import logging

logger = logging.getLogger(__name__)

class EntityValidator:
    """Sistema de validação híbrido usando Pydantic + validações customizadas"""
    
    # Mapeamento de entidades para schemas
    SCHEMA_MAPPING = {
        'empresa': EmpresaSchema,
        'estabelecimento': EstabelecimentoSchema,
        'socio': SocioSchema,
        'simples': SimplesSchema
    }
    
    @classmethod
    def validate_dataframe(cls, df: pl.DataFrame, entity_type: str, 
                          sample_size: int = 1000) -> Dict[str, Any]:
        """
        Valida DataFrame usando schema Pydantic
        
        Args:
            df: DataFrame para validar
            entity_type: Tipo da entidade ('empresa', 'estabelecimento', etc.)
            sample_size: Número de linhas para validar (para performance)
        """
        if entity_type not in cls.SCHEMA_MAPPING:
            raise ValueError(f"Tipo de entidade inválido: {entity_type}")
        
        schema_class = cls.SCHEMA_MAPPING[entity_type]
        
        validation_results = {
            'entity_type': entity_type,
            'total_rows': df.height,
            'validated_rows': 0,
            'valid_rows': 0,
            'invalid_rows': 0,
            'errors': [],
            'warnings': [],
            'error_summary': {},
            'sample_valid_data': [],
            'sample_invalid_data': []
        }
        
        # Validar amostra para performance
        sample_df = df.head(sample_size) if df.height > sample_size else df
        validation_results['validated_rows'] = sample_df.height
        
        logger.info(f"Validando {validation_results['validated_rows']} linhas de {entity_type}")
        
        for i, row in enumerate(sample_df.iter_rows(named=True)):
            try:
                # Tentar criar instância do schema
                validated_data = schema_class(**row)
                validation_results['valid_rows'] += 1
                
                # Guardar amostra de dados válidos
                if len(validation_results['sample_valid_data']) < 5:
                    validation_results['sample_valid_data'].append(validated_data.dict())
                
            except ValidationError as e:
                validation_results['invalid_rows'] += 1
                
                # Processar erros de validação
                error_details = []
                for error in e.errors():
                    field = error['loc'][0] if error['loc'] else 'unknown'
                    message = error['msg']
                    error_type = error['type']
                    
                    error_details.append({
                        'field': field,
                        'message': message,
                        'type': error_type,
                        'value': row.get(field, 'N/A')
                    })
                    
                    # Contar tipos de erro
                    if error_type not in validation_results['error_summary']:
                        validation_results['error_summary'][error_type] = 0
                    validation_results['error_summary'][error_type] += 1
                
                validation_results['errors'].append({
                    'row': i,
                    'errors': error_details
                })
                
                # Guardar amostra de dados inválidos
                if len(validation_results['sample_invalid_data']) < 5:
                    validation_results['sample_invalid_data'].append({
                        'row_data': row,
                        'errors': error_details
                    })
            
            except Exception as e:
                validation_results['invalid_rows'] += 1
                validation_results['errors'].append({
                    'row': i,
                    'errors': [{'field': 'general', 'message': str(e), 'type': 'unexpected_error'}]
                })
        
        # Calcular estatísticas
        validation_results['success_rate'] = (
            validation_results['valid_rows'] / validation_results['validated_rows'] * 100
            if validation_results['validated_rows'] > 0 else 0
        )
        
        # Gerar warnings baseados na taxa de sucesso
        if validation_results['success_rate'] < 50:
            validation_results['warnings'].append(
                f"Taxa de sucesso muito baixa ({validation_results['success_rate']:.1f}%) - "
                "verifique formato dos dados"
            )
        elif validation_results['success_rate'] < 80:
            validation_results['warnings'].append(
                f"Taxa de sucesso moderada ({validation_results['success_rate']:.1f}%) - "
                "alguns dados podem estar inconsistentes"
            )
        
        logger.info(f"Validação concluída: {validation_results['success_rate']:.1f}% de sucesso")
        
        return validation_results
    
    @classmethod
    def clean_dataframe(self, df: pl.DataFrame, entity_type: str, 
                       remove_invalid: bool = True) -> pl.DataFrame:
        """
        Remove ou corrige linhas inválidas do DataFrame
        
        Args:
            df: DataFrame para limpar
            entity_type: Tipo da entidade
            remove_invalid: Se deve remover linhas inválidas (True) ou tentar corrigir (False)
        """
        if entity_type not in cls.SCHEMA_MAPPING:
            raise ValueError(f"Tipo de entidade inválido: {entity_type}")
        
        schema_class = cls.SCHEMA_MAPPING[entity_type]
        
        valid_rows = []
        invalid_count = 0
        
        logger.info(f"Limpando DataFrame de {entity_type} ({df.height} linhas)")
        
        for row in df.iter_rows(named=True):
            try:
                # Tentar validar e corrigir
                if remove_invalid:
                    # Modo estrito: remover inválidos
                    validated_data = schema_class(**row)
                    valid_rows.append(validated_data.dict())
                else:
                    # Modo permissivo: tentar corrigir
                    try:
                        validated_data = schema_class(**row)
                        valid_rows.append(validated_data.dict())
                    except ValidationError:
                        # Tentar corrigir dados básicos
                        corrected_row = cls._attempt_correction(row, schema_class)
                        if corrected_row:
                            valid_rows.append(corrected_row)
                        else:
                            invalid_count += 1
                            
            except Exception:
                invalid_count += 1
        
        if valid_rows:
            cleaned_df = pl.DataFrame(valid_rows)
            logger.info(f"Limpeza concluída: {len(valid_rows)} linhas válidas, {invalid_count} removidas/corrigidas")
            return cleaned_df
        else:
            logger.warning("Nenhuma linha válida encontrada após limpeza")
            return pl.DataFrame()
    
    @staticmethod
    def _attempt_correction(row: Dict[str, Any], schema_class: Type[BaseModel]) -> Dict[str, Any]:
        """Tenta corrigir dados básicos automaticamente"""
        corrected = row.copy()
        
        # Correções básicas comuns
        for field, value in corrected.items():
            if isinstance(value, str):
                # Remover espaços extras
                corrected[field] = value.strip()
                
                # Corrigir campos numéricos
                if field in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'cep']:
                    # Manter apenas dígitos
                    corrected[field] = ''.join(filter(str.isdigit, value))
        
        # Tentar validar dados corrigidos
        try:
            validated = schema_class(**corrected)
            return validated.dict()
        except ValidationError:
            return None
    
    @classmethod
    def get_validation_report(cls, validation_results: Dict[str, Any]) -> str:
        """Gera relatório de validação em formato texto"""
        report = []
        report.append(f"=== RELATÓRIO DE VALIDAÇÃO - {validation_results['entity_type'].upper()} ===")
        report.append(f"Total de linhas: {validation_results['total_rows']}")
        report.append(f"Linhas validadas: {validation_results['validated_rows']}")
        report.append(f"Linhas válidas: {validation_results['valid_rows']}")
        report.append(f"Linhas inválidas: {validation_results['invalid_rows']}")
        report.append(f"Taxa de sucesso: {validation_results['success_rate']:.1f}%")
        
        if validation_results['warnings']:
            report.append("\n⚠️  AVISOS:")
            for warning in validation_results['warnings']:
                report.append(f"  • {warning}")
        
        if validation_results['error_summary']:
            report.append("\n❌ RESUMO DE ERROS:")
            for error_type, count in validation_results['error_summary'].items():
                report.append(f"  • {error_type}: {count} ocorrências")
        
        if validation_results['sample_invalid_data']:
            report.append("\n🔍 AMOSTRAS DE DADOS INVÁLIDOS:")
            for i, sample in enumerate(validation_results['sample_invalid_data'][:3]):
                report.append(f"  Exemplo {i+1}:")
                for error in sample['errors']:
                    report.append(f"    - {error['field']}: {error['message']}")
        
        return "\n".join(report)
```

## Benefícios da Abordagem Híbrida com Schemas

### 🚀 **Vantagens dos Schemas (Pydantic)**
1. **Validação Declarativa**: Regras definidas de forma clara e concisa
2. **Performance**: Validação otimizada em C (via Pydantic)
3. **Ecosystem Maduro**: Integração com FastAPI, SQLAlchemy, etc.
4. **Documentação Automática**: Schemas geram documentação automaticamente
5. **Serialização**: Conversão automática entre formatos (JSON, dict, etc.)
6. **Type Hints**: Suporte completo a tipagem Python

### 🎯 **Vantagens da Validação Customizada**
1. **Regras de Negócio Complexas**: Validações específicas do domínio RF
2. **Performance em Lote**: Validação otimizada para DataFrames grandes
3. **Correção Automática**: Tentativa de corrigir dados malformados
4. **Relatórios Detalhados**: Análise estatística dos erros
5. **Integração com Polars**: Otimizado para processamento de dados

### 📊 **Comparação de Performance**

| Cenário | Validation.py Puro | Schemas (Pydantic) | Híbrido |
|---------|-------------------|-------------------|---------|
| **Validação Simples** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Regras Complexas** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **DataFrames Grandes** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Reutilização** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Manutenibilidade** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

## 🎯 **Recomendação Final**

A abordagem **híbrida com schemas** é superior porque:

1. **Melhor dos dois mundos**: Combina a robustez dos schemas com flexibilidade customizada
2. **Ecosystem**: Schemas podem ser reutilizados em APIs, documentação, testes
3. **Manutenibilidade**: Regras centralizadas e declarativas
4. **Performance**: Validação otimizada + processamento em lote inteligente
5. **Evolução**: Fácil adicionar novas validações ou modificar existentes

Esta abordagem transforma a validação de dados de um **processo manual e propenso a erros** em um **sistema robusto, automatizado e reutilizável**!

## Benefícios da Integração com Entidades

1. **Validação Automática**: Cada linha de dados é validada automaticamente
2. **Transformações Tipadas**: Transformações específicas para cada tipo de entidade
3. **Documentação Viva**: Estrutura de dados documentada nas próprias entidades
4. **Reutilização**: Entidades podem ser usadas em APIs, relatórios, etc.
5. **Testes Simplificados**: Cada entidade pode ser testada independentemente
6. **Evolução Controlada**: Mudanças na estrutura são centralizadas nas entidades

## Benefícios Esperados

1. **Redução Massiva de Código**
   - **Antes**: ~5.940 linhas nos 4 arquivos
   - **Depois**: ~1.500 linhas nos processadores específicos + ~2.000 linhas de código compartilhado
   - **Redução**: ~75% de código duplicado eliminado
   - **Manutenção**: Correções aplicadas em um único lugar

2. **Manutenibilidade Drasticamente Melhorada**
   - Mudanças no sistema de fila afetam todos os processadores automaticamente
   - Novos processadores podem ser adicionados facilmente
   - Testes centralizados para funcionalidades comuns
   - Documentação unificada

3. **Consistência Total**
   - Tratamento de erros padronizado em todos os processadores
   - Logging uniforme com informações específicas por tipo
   - Comportamento previsível e documentado
   - Validação de parâmetros automática

4. **Performance Otimizada**
   - Sistema de fila único e otimizado
   - Gerenciamento de recursos centralizado
   - Otimizações aplicadas globalmente
   - Melhor utilização de memória e CPU

5. **Robustez Aumentada**
   - Validação automática de parâmetros
   - Tratamento de erros consistente
   - Logs detalhados e padronizados
   - Facilidade para debugging

## Próximos Passos

### Fase 1: Implementação das Entidades (Estimativa: 3-4 dias)
- [ ] **Dia 1**: Implementar `BaseEntity` e estrutura base
- [ ] **Dia 2**: Implementar entidades `Empresa` e `Estabelecimento`
- [ ] **Dia 3**: Implementar entidades `Socio` e `Simples`
- [ ] **Dia 4**: Implementar sistema de validação e testes

### Fase 2: Preparação da Refatoração (Estimativa: 2-3 dias)
- [ ] Criar estrutura base de classes (`BaseProcessor`, `ProcessingQueueManager`)
- [ ] Implementar `ProcessorFactory` com validação
- [ ] Integrar processadores com entidades
- [ ] Criar módulos de utilidades unificados
- [ ] Implementar sistema de logging unificado

### Fase 3: Migração Gradual (Estimativa: 1 semana)
- [ ] **Dia 1-2**: Migrar `socio.py` (mais simples, sem funcionalidades específicas)
- [ ] **Dia 3-4**: Migrar `simples.py` (similar ao socio)
- [ ] **Dia 5-6**: Migrar `estabelecimento.py` (com funcionalidade uf_subset)
- [ ] **Dia 7**: Migrar `empresa.py` (mais complexo, com create_private)

### Fase 4: Otimização e Testes (Estimativa: 2-3 dias)
- [ ] Implementar testes automatizados para todos os processadores e entidades
- [ ] Otimizar performance do sistema unificado
- [ ] Validar compatibilidade com código existente
- [ ] Benchmark de performance antes/depois

### Fase 5: Documentação e Finalização (Estimativa: 1 dia)
- [ ] Documentar classes e métodos
- [ ] Criar exemplos de uso das entidades
- [ ] Atualizar README com nova arquitetura
- [ ] Documentar processo de migração

## Impacto Detalhado na Base de Código

### Estrutura Atual
```
src/process/
├── empresa.py (1.402 linhas) - 70% código duplicado
├── estabelecimento.py (1.427 linhas) - 70% código duplicado  
├── socio.py (1.016 linhas) - 75% código duplicado
└── simples.py (1.095 linhas) - 75% código duplicado
Total: 5.940 linhas (~4.200 linhas duplicadas)

src/Entity/ - TODOS VAZIOS (0 bytes)
```

### Estrutura Proposta
```
src/Entity/
├── base.py (200 linhas) - Classe base para entidades
├── Empresa.py (150 linhas) - Entidade Empresa com validações
├── Estabelecimento.py (180 linhas) - Entidade Estabelecimento
├── Socio.py (120 linhas) - Entidade Socio
├── Simples.py (100 linhas) - Entidade Simples
├── validation.py (150 linhas) - Sistema de validação
└── __init__.py (50 linhas) - Exports e utilitários

src/process/
├── base/
│   ├── processor.py (500 linhas) - Classe base integrada com entidades
│   ├── queue_manager.py (300 linhas) - Sistema de fila
│   └── factory.py (100 linhas) - Factory pattern
├── utils/
│   ├── processing.py (400 linhas) - Utilidades comuns
│   └── logging_resources.py (200 linhas) - Logging unificado
├── empresa.py (200 linhas) - Só lógica específica + integração com entidade
├── estabelecimento.py (250 linhas) - Só lógica específica + integração com entidade
├── socio.py (150 linhas) - Só lógica específica + integração com entidade
└── simples.py (150 linhas) - Só lógica específica + integração com entidade

Total: 3.400 linhas (~43% redução + estrutura robusta de entidades)
```

### Benefícios Quantificados com Entidades
- **Redução de código**: 2.540 linhas eliminadas (43%)
- **Duplicação eliminada**: ~4.200 linhas de código duplicado
- **Estrutura de entidades**: +950 linhas de código estruturado e reutilizável
- **Validação automática**: Dados validados em tempo real
- **Documentação viva**: Estrutura autodocumentada
- **Reutilização**: Entidades usáveis em outros contextos (APIs, relatórios, etc.)
- **Manutenção**: 1 lugar para definir estrutura vs 4 lugares
- **Testes**: Entidades testáveis independentemente

## Conclusão

A integração da estrutura Entity com a refatoração dos processadores oferece uma **oportunidade única** de criar um sistema robusto, bem estruturado e altamente reutilizável. 

A pasta `src/Entity` vazia representa um **potencial inexplorado** que, quando implementado, pode:

1. **Eliminar duplicação**: Reduzir 43% do código total
2. **Centralizar validação**: Dados sempre validados e consistentes  
3. **Facilitar manutenção**: Mudanças estruturais em um só lugar
4. **Aumentar reutilização**: Entidades usáveis em todo o sistema
5. **Melhorar documentação**: Estrutura autodocumentada e tipada
6. **Simplificar testes**: Cada componente testável independentemente

Esta abordagem transforma uma refatoração simples em uma **modernização completa** da arquitetura do sistema, criando uma base sólida para futuras expansões e melhorias. 