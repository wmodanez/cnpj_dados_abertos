"""
Schema de validação Pydantic para entidade Simples Nacional.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class SimplesSchema(BaseModel):
    """Schema Pydantic para Simples Nacional."""
    
    cnpj_basico: int = Field(..., description="CNPJ básico da empresa")
    opcao_simples: Optional[str] = Field(None, description="Opção pelo Simples Nacional (S/N)")
    data_opcao_simples: Optional[str] = Field(None, description="Data da opção pelo Simples")
    data_exclusao_simples: Optional[str] = Field(None, description="Data de exclusão do Simples")
    opcao_mei: Optional[str] = Field(None, description="Opção pelo MEI (S/N)")
    data_opcao_mei: Optional[str] = Field(None, description="Data da opção pelo MEI")
    data_exclusao_mei: Optional[str] = Field(None, description="Data de exclusão do MEI")
    
    @field_validator('opcao_simples', 'opcao_mei')
    @classmethod
    def validate_opcao(cls, v):
        """Valida opções S/N."""
        if v is not None and v not in ['S', 'N']:
            raise ValueError('Deve ser S ou N')
        return v
    
    class Config:
        """Configuração do modelo."""
        schema_extra = {
            "example": {
                "cnpj_basico": 12345678,
                "opcao_simples": "S",
                "data_opcao_simples": "20070101",
                "data_exclusao_simples": None,
                "opcao_mei": "N",
                "data_opcao_mei": None,
                "data_exclusao_mei": None
            }
        } 