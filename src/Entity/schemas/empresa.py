"""
Schema de validação Pydantic para entidade Empresa.
"""

from pydantic import BaseModel, Field, validator, root_validator
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class EmpresaSchema(BaseModel):
    """Schema de validação para Empresa usando Pydantic"""
    
    cnpj_basico: str = Field(..., min_length=8, max_length=8, pattern=r'^\d{8}$', 
                             description="CNPJ básico com 8 dígitos")
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
    cpf_extraido: Optional[str] = Field(None, pattern=r'^\d{11}$', 
                                       description="CPF extraído da razão social")
    
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
                "cnpj_basico": "12345678",
                "razao_social": "EMPRESA EXEMPLO LTDA",
                "natureza_juridica": 206,
                "qualificacao_responsavel": 10,
                "capital_social": 100000.00,
                "porte_empresa": 2,
                "ente_federativo_responsavel": "",
                "cpf_extraido": None
            }
        }
    
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
        
        # Verificar caracteres especiais suspeitos
        if any(char in v for char in ['<', '>', '&', '"', "'"]):
            logger.warning(f"Razão social contém caracteres especiais: {v}")
        
        return v.strip()
    
    @validator('capital_social')
    def validate_capital_social(cls, v):
        """Valida capital social"""
        if v is not None and v < 0:
            raise ValueError('Capital social não pode ser negativo')
        
        # Verificar valores muito altos (possível erro)
        if v is not None and v > 1e12:  # 1 trilhão
            logger.warning(f"Capital social muito alto: {v}")
        
        return v
    
    @root_validator(skip_on_failure=True)
    def validate_empresa_consistency(cls, values):
        """Validações que dependem de múltiplos campos"""
        cnpj_basico = values.get('cnpj_basico')
        razao_social = values.get('razao_social')
        capital_social = values.get('capital_social')
        porte_empresa = values.get('porte_empresa')
        
        # Verificar consistência entre CNPJ e razão social
        if cnpj_basico and razao_social:
            # Empresas com CNPJ iniciado em '00' geralmente são especiais
            if cnpj_basico.startswith('00') and len(razao_social) < 10:
                raise ValueError('Empresas com CNPJ especial devem ter razão social mais detalhada')
        
        # Verificar consistência entre capital social e porte
        if capital_social is not None and porte_empresa is not None:
            # Regras básicas de porte vs capital (simplificadas)
            if porte_empresa == 1 and capital_social > 360000:  # Microempresa
                logger.warning("Capital social alto para microempresa")
            elif porte_empresa == 2 and capital_social > 4800000:  # Pequena empresa
                logger.warning("Capital social alto para pequena empresa")
        
        return values 