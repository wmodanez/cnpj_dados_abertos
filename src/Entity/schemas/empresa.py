"""
Schema de validação Pydantic para entidade Empresa.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class EmpresaSchema(BaseModel):
    """Schema de validação para Empresa usando Pydantic"""
    
    cnpj_basico: int = Field(..., ge=10000000, le=99999999, 
                             description="CNPJ básico (8 dígitos)")
    razao_social: str = Field(..., min_length=1, max_length=500, 
                             description="Razão social da empresa")
    natureza_juridica: Optional[int] = Field(None, ge=1, le=9999, 
                                           description="Código da natureza jurídica")
    qualificacao_responsavel: Optional[int] = Field(None, ge=1, le=99, 
                                                   description="Qualificação do responsável")
    capital_social: Optional[float] = Field(None, ge=0, 
                                           description="Capital social da empresa")
    porte_empresa: Optional[int] = Field(None, ge=1, le=5, 
                                        description="Porte da empresa (1-5)")
    ente_federativo_responsavel: Optional[str] = Field(None, max_length=100, 
                                                      description="Ente federativo responsável")
    # cpf_extraido será adicionado durante o processamento como campo calculado
    
    @field_validator('cnpj_basico')
    @classmethod
    def validate_cnpj_basico(cls, v: int) -> int:
        """Valida CNPJ básico."""
        if not isinstance(v, int):
            try:
                v = int(v)
            except (ValueError, TypeError):
                raise ValueError('CNPJ básico deve ser um número inteiro')
        
        if not (10000000 <= v <= 99999999):
            raise ValueError('CNPJ básico deve ter exatamente 8 dígitos')
        
        return v
    
    @field_validator('razao_social')
    @classmethod
    def validate_razao_social(cls, v):
        if not v or not v.strip():
            raise ValueError('Razão social é obrigatória')
        
        razao_limpa = v.strip().upper()
        if len(razao_limpa) < 3:
            raise ValueError('Razão social deve ter pelo menos 3 caracteres')
        
        return razao_limpa
    
    @field_validator('capital_social')
    @classmethod
    def validate_capital_social(cls, v):
        if v is not None and v < 0:
            raise ValueError('Capital social não pode ser negativo')
        return v
    
    @model_validator(mode='after')
    def validate_consistency(self):
        """Validar consistência entre campos."""
        # Verificar se porte da empresa é consistente com capital social
        if self.capital_social is not None and self.porte_empresa is not None:
            if self.capital_social > 1000000 and self.porte_empresa == 1:  # Microempresa
                raise ValueError('Capital social inconsistente com porte da empresa')
        
        return self 

    class Config:
        # Permitir campos extras durante parsing
        extra = "ignore"
        # Validar na atribuição
        validate_assignment = True
        # Usar enum por valor
        use_enum_values = True
        # Schema para documentação
        json_schema_extra = {
            "example": {
                "cnpj_basico": 12345678,
                "razao_social": "EMPRESA EXEMPLO LTDA",
                "natureza_juridica": 206,
                "qualificacao_responsavel": 10,
                "capital_social": 100000.00,
                "porte_empresa": 2,
                "ente_federativo_responsavel": "",
                "cpf_extraido": None
            }
        } 