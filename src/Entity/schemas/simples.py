"""
Schema de validação Pydantic para entidade Simples Nacional.
"""

from pydantic import BaseModel, Field, validator, root_validator
from typing import Optional
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
    
    @validator('data_opcao_simples', 'data_exclusao_simples', 'data_opcao_mei', 'data_exclusao_mei')
    def validate_dates(cls, v):
        """Valida datas"""
        if v is None:
            return v
        
        # Verificar se a data não é muito antiga (Simples Nacional criado em 2006)
        if v.year < 2006:
            raise ValueError(f'Data anterior à criação do Simples Nacional: {v}')
        
        # Verificar se a data não é no futuro
        if v > datetime.now():
            raise ValueError(f'Data no futuro: {v}')
        
        return v
    
    @root_validator(skip_on_failure=True)
    def validate_dates_consistency(cls, values):
        """Valida consistência entre datas"""
        data_opcao_simples = values.get('data_opcao_simples')
        data_exclusao_simples = values.get('data_exclusao_simples')
        data_opcao_mei = values.get('data_opcao_mei')
        data_exclusao_mei = values.get('data_exclusao_mei')
        opcao_simples = values.get('opcao_simples')
        opcao_mei = values.get('opcao_mei')
        
        # Validar Simples Nacional
        if data_opcao_simples and data_exclusao_simples:
            if data_exclusao_simples <= data_opcao_simples:
                raise ValueError('Data de exclusão do Simples deve ser posterior à data de opção')
        
        # Validar MEI
        if data_opcao_mei and data_exclusao_mei:
            if data_exclusao_mei <= data_opcao_mei:
                raise ValueError('Data de exclusão do MEI deve ser posterior à data de opção')
        
        # Validar consistência entre opção e datas
        if opcao_simples == 'S' and not data_opcao_simples:
            logger.warning("Empresa optante do Simples sem data de opção")
        
        if opcao_mei == 'S' and not data_opcao_mei:
            logger.warning("Empresa optante do MEI sem data de opção")
        
        # MEI e Simples são mutuamente exclusivos em alguns casos
        if (opcao_simples == 'S' and opcao_mei == 'S' and 
            data_opcao_simples and data_opcao_mei):
            # Verificar se as datas fazem sentido
            if abs((data_opcao_simples - data_opcao_mei).days) < 30:
                logger.warning("Opção simultânea por Simples e MEI em datas próximas")
        
        return values 