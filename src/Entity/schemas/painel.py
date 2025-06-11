"""
Schema Pydantic para validação da entidade Painel.

Este schema define as regras de validação para dados do painel,
que combina informações de estabelecimentos, empresas e Simples Nacional.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class PainelSchema(BaseModel):
    """
    Schema de validação para a entidade Painel.
    
    Representa dados combinados de:
    - Estabelecimento (base)
    - Empresa (inner join)
    - Simples Nacional (left join)
    """
    
    # Dados do Estabelecimento (base)
    cnpj_basico: int = Field(..., ge=10000000, le=99999999, description="CNPJ básico (8 dígitos)")
    matriz_filial: Optional[int] = Field(None, ge=1, le=2, description="1=Matriz, 2=Filial")
    codigo_situacao: Optional[int] = Field(None, ge=1, le=8, description="Código da situação cadastral")
    data_situacao_cadastral: Optional[datetime] = Field(None, description="Data da situação cadastral")
    codigo_motivo: Optional[int] = Field(None, description="Código do motivo da situação")
    data_inicio_atividades: Optional[datetime] = Field(None, description="Data de início das atividades")
    codigo_cnae: Optional[int] = Field(None, description="Código CNAE principal")
    uf: Optional[str] = Field(None, min_length=2, max_length=2, description="Unidade Federativa")
    codigo_municipio: Optional[int] = Field(None, description="Código do município")
    tipo_situacao_cadastral: Optional[int] = Field(None, ge=1, le=3, description="Tipo de situação cadastral")
    
    # Dados da Empresa (inner join)
    natureza_juridica: Optional[int] = Field(None, ge=1, le=9999, description="Código da natureza jurídica")
    porte_empresa: Optional[int] = Field(None, ge=1, le=5, description="Porte da empresa")
    
    # Dados do Simples Nacional (left join)
    opcao_simples: Optional[str] = Field(None, pattern="^[SN]$", description="Opção pelo Simples Nacional")
    data_opcao_simples: Optional[datetime] = Field(None, description="Data da opção pelo Simples")
    data_exclusao_simples: Optional[datetime] = Field(None, description="Data de exclusão do Simples")
    opcao_mei: Optional[str] = Field(None, pattern="^[SN]$", description="Opção pelo MEI")
    data_opcao_mei: Optional[datetime] = Field(None, description="Data da opção pelo MEI")
    data_exclusao_mei: Optional[datetime] = Field(None, description="Data de exclusão do MEI")
    
    @field_validator('uf')
    @classmethod
    def validate_uf(cls, v):
        """Valida UF brasileira."""
        if v is None:
            return v
        
        ufs_validas = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        
        if v.upper() not in ufs_validas:
            raise ValueError(f'UF inválida: {v}. Deve ser uma das: {", ".join(ufs_validas)}')
        
        return v.upper()
    
    @field_validator('data_situacao_cadastral', 'data_inicio_atividades', 
                     'data_opcao_simples', 'data_exclusao_simples',
                     'data_opcao_mei', 'data_exclusao_mei')
    @classmethod
    def validate_dates(cls, v):
        """Valida datas."""
        if v is None:
            return v
        
        # Verificar se não é muito antiga (Simples Nacional criado em 2006)
        if v.year < 1900:
            raise ValueError('Data muito antiga (anterior a 1900)')
        
        # Verificar se não é no futuro
        if v > datetime.now():
            raise ValueError('Data não pode ser no futuro')
        
        return v
    
    class Config:
        """Configuração do schema."""
        from_attributes = True
        arbitrary_types_allowed = True 