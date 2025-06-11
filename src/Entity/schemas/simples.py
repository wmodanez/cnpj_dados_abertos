"""
Schema de validação Pydantic para entidade Simples Nacional.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SimplesSchema(BaseModel):
    """Schema de validação para Simples Nacional"""
    
    cnpj_basico: str = Field(..., pattern=r'^\d{8}$', description="CNPJ básico")
    opcao_simples: Optional[str] = Field(None, pattern=r'^[SN]$', 
                                        description="Opção pelo Simples (S/N)")
    data_opcao_simples: Optional[datetime] = Field(None, 
                                                  description="Data da opção pelo Simples")
    data_exclusao_simples: Optional[datetime] = Field(None, 
                                                     description="Data de exclusão do Simples")
    opcao_mei: Optional[str] = Field(None, pattern=r'^[SN]$', 
                                    description="Opção pelo MEI (S/N)")
    data_opcao_mei: Optional[datetime] = Field(None, description="Data da opção pelo MEI")
    data_exclusao_mei: Optional[datetime] = Field(None, description="Data de exclusão do MEI")
    
    class Config:
        extra = "ignore"
        validate_assignment = True
        json_schema_extra = {
            "example": {
                "cnpj_basico": "12345678",
                "opcao_simples": "S",
                "data_opcao_simples": "2020-01-01T00:00:00",
                "data_exclusao_simples": None,
                "opcao_mei": "N",
                "data_opcao_mei": None,
                "data_exclusao_mei": None
            }
        }
    
    @field_validator('data_opcao_simples', 'data_exclusao_simples', 'data_opcao_mei', 'data_exclusao_mei')
    @classmethod
    def validate_dates(cls, v):
        if v is None:
            return v
        
        # Verificar se a data não é anterior a 2006 (criação do Simples Nacional)
        if isinstance(v, datetime) and v.year < 2006:
            raise ValueError('Data anterior à criação do Simples Nacional (2006)')
        
        # Verificar se a data não é no futuro
        if isinstance(v, datetime) and v > datetime.now():
            raise ValueError('Data no futuro')
        
        return v
    
    @model_validator(mode='after')
    def validate_simples_consistency(self):
        """Validar consistência entre datas e opções do Simples Nacional."""
        
        # Validar consistência Simples Nacional
        if self.data_opcao_simples and self.data_exclusao_simples:
            if self.data_exclusao_simples <= self.data_opcao_simples:
                raise ValueError('Data de exclusão do Simples deve ser posterior à data de opção')
        
        # Validar consistência MEI
        if self.data_opcao_mei and self.data_exclusao_mei:
            if self.data_exclusao_mei <= self.data_opcao_mei:
                raise ValueError('Data de exclusão do MEI deve ser posterior à data de opção')
        
        return self 