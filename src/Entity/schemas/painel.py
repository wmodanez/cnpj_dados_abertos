"""
Schema Pydantic para validação da entidade Painel.

Este schema define as regras de validação para dados do painel,
que combina informações de estabelecimentos, empresas e Simples Nacional.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional


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
    data_situacao_cadastral: Optional[str] = Field(None, description="Data da situação cadastral")
    codigo_motivo: Optional[int] = Field(None, description="Código do motivo da situação")
    data_inicio_atividades: Optional[str] = Field(None, description="Data de início das atividades")
    codigo_cnae: Optional[int] = Field(None, description="Código CNAE principal")
    uf: Optional[str] = Field(None, min_length=2, max_length=2, description="Unidade Federativa")
    codigo_municipio: Optional[int] = Field(None, description="Código do município")
    tipo_situacao_cadastral: Optional[int] = Field(None, ge=1, le=3, description="Tipo de situação cadastral")
    
    # Dados da Empresa (inner join)
    natureza_juridica: Optional[int] = Field(None, ge=1, le=9999, description="Código da natureza jurídica")
    porte_empresa: Optional[int] = Field(None, ge=1, le=5, description="Porte da empresa")
    
    # Dados do Simples Nacional (left join)
    opcao_simples: Optional[str] = Field(None, pattern="^[SN]$", description="Opção pelo Simples Nacional")
    data_opcao_simples: Optional[str] = Field(None, description="Data da opção pelo Simples")
    data_exclusao_simples: Optional[str] = Field(None, description="Data de exclusão do Simples")
    opcao_mei: Optional[str] = Field(None, pattern="^[SN]$", description="Opção pelo MEI")
    data_opcao_mei: Optional[str] = Field(None, description="Data da opção pelo MEI")
    data_exclusao_mei: Optional[str] = Field(None, description="Data de exclusão do MEI")
    
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
    
    @field_validator('opcao_simples', 'opcao_mei')
    @classmethod
    def validate_opcoes(cls, v):
        """Valida opções S/N."""
        if v is not None and v not in ['S', 'N']:
            raise ValueError('Deve ser S ou N')
        return v
    
    class Config:
        """Configuração do schema."""
        from_attributes = True
        arbitrary_types_allowed = True
        schema_extra = {
            "example": {
                "cnpj_basico": 12345678,
                "matriz_filial": 1,
                "codigo_situacao": 2,
                "data_situacao_cadastral": "20200115",
                "codigo_motivo": 0,
                "data_inicio_atividades": "20150610",
                "codigo_cnae": 6201500,
                "codigo_municipio": 7107,
                "tipo_situacao_cadastral": 1,
                "natureza_juridica": 2062,
                "porte_empresa": 2,
                "opcao_simples": "S",
                "data_opcao_simples": "20070101",
                "data_exclusao_simples": None,
                "opcao_mei": "N",
                "data_opcao_mei": None,
                "data_exclusao_mei": None
            }
        } 