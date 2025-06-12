"""
Schema de validação Pydantic para entidade Estabelecimento.
"""

from pydantic import BaseModel, Field
from typing import Optional


class EstabelecimentoSchema(BaseModel):
    """Schema Pydantic para Estabelecimento."""
    
    cnpj_basico: int = Field(..., description="CNPJ básico")
    cnpj_ordem: Optional[str] = Field(None, description="Ordem do CNPJ")
    cnpj_dv: Optional[str] = Field(None, description="Dígitos verificadores do CNPJ")
    matriz_filial: Optional[int] = Field(None, description="1=Matriz, 2=Filial")
    nome_fantasia: Optional[str] = Field(None, description="Nome fantasia")
    codigo_situacao: Optional[int] = Field(None, description="Código da situação cadastral")
    data_situacao_cadastral: Optional[str] = Field(None, description="Data da situação cadastral")
    codigo_motivo: Optional[int] = Field(None, description="Código do motivo da situação")
    nome_cidade_exterior: Optional[str] = Field(None, description="Nome da cidade no exterior")
    data_inicio_atividades: Optional[str] = Field(None, description="Data de início das atividades")
    codigo_cnae: Optional[int] = Field(None, description="Código CNAE principal")
    cnae_secundaria: Optional[str] = Field(None, description="CNAEs secundários")
    uf: Optional[str] = Field(None, description="Unidade Federativa")
    codigo_municipio: Optional[int] = Field(None, description="Código do município")
    cep: Optional[str] = Field(None, description="CEP")
    cnpj_completo: Optional[str] = Field(None, description="CNPJ completo")
    
    class Config:
        """Configuração do modelo."""
        schema_extra = {
            "example": {
                "cnpj_basico": 12345678,
                "cnpj_ordem": "0001",
                "cnpj_dv": "12",
                "matriz_filial": 1,
                "nome_fantasia": "EMPRESA EXEMPLO LTDA",
                "codigo_situacao": 2,
                "data_situacao_cadastral": "20200101",
                "codigo_motivo": 0,
                "nome_cidade_exterior": None,
                "data_inicio_atividades": "20150610",
                "codigo_cnae": 6201500,
                "cnae_secundaria": "6202300,6311900",
                "uf": "SP",
                "codigo_municipio": 7107,
                "cep": "01234567",
                "cnpj_completo": "12345678000112"
            }
        } 