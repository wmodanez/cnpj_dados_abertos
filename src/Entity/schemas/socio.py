"""
Schema de validação Pydantic para entidade Socio.
"""

from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SocioSchema(BaseModel):
    """Schema de validação para Sócio"""
    
    cnpj_basico: str = Field(..., pattern=r'^\d{8}$', description="CNPJ básico da empresa")
    identificador_socio: Optional[int] = Field(None, ge=1, le=9, 
                                              description="Tipo de sócio (1-9)")
    nome_socio: Optional[str] = Field(None, max_length=300, description="Nome do sócio")
    cnpj_cpf_socio: Optional[str] = Field(None, pattern=r'^\d{11}$|^\d{14}$', 
                                         description="CPF ou CNPJ do sócio")
    qualificacao_socio: Optional[int] = Field(None, ge=1, le=99, 
                                             description="Qualificação do sócio")
    data_entrada_sociedade: Optional[datetime] = Field(None, 
                                                      description="Data de entrada na sociedade")
    pais: Optional[str] = Field(None, max_length=100, description="País do sócio")
    representante_legal: Optional[str] = Field(None, max_length=11, 
                                              description="CPF do representante legal")
    nome_representante: Optional[str] = Field(None, max_length=300, 
                                             description="Nome do representante")
    qualificacao_representante_legal: Optional[int] = Field(None, ge=1, le=99, 
                                                           description="Qualificação do representante")
    faixa_etaria: Optional[str] = Field(None, max_length=2, description="Faixa etária")
    
    class Config:
        extra = "ignore"
        validate_assignment = True
        json_schema_extra = {
            "example": {
                "cnpj_basico": "12345678",
                "identificador_socio": 2,
                "nome_socio": "JOÃO DA SILVA",
                "cnpj_cpf_socio": "12345678901",
                "qualificacao_socio": 10,
                "data_entrada_sociedade": "2020-01-01T00:00:00"
            }
        }
    
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
    
    @validator('nome_socio', 'nome_representante')
    def validate_names(cls, v):
        """Valida nomes"""
        if v is None:
            return v
        
        v = v.strip()
        
        # Verificar se não é apenas números
        if v.isdigit():
            raise ValueError('Nome não pode ser apenas números')
        
        # Verificar tamanho mínimo
        if len(v) < 2:
            raise ValueError('Nome muito curto')
        
        return v if v else None
    
    @validator('representante_legal')
    def validate_representante_legal(cls, v):
        """Valida CPF do representante legal"""
        if v is None:
            return v
        
        # Deve ser um CPF válido
        if len(v) != 11 or not v.isdigit():
            raise ValueError('Representante legal deve ser um CPF válido')
        
        return cls._validate_cpf(v)
    
    @validator('data_entrada_sociedade')
    def validate_data_entrada(cls, v):
        """Valida data de entrada na sociedade"""
        if v is None:
            return v
        
        # Verificar se a data não é muito antiga
        if v.year < 1900:
            raise ValueError(f'Data de entrada muito antiga: {v}')
        
        # Verificar se a data não é no futuro
        if v > datetime.now():
            raise ValueError(f'Data de entrada no futuro: {v}')
        
        return v
    
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
        """Valida CNPJ usando algoritmo oficial"""
        # Importar validação de CNPJ do estabelecimento
        from .estabelecimento import EstabelecimentoSchema
        
        if not EstabelecimentoSchema._validate_cnpj_algorithm(cnpj):
            raise ValueError(f'CNPJ inválido: {cnpj}')
        return cnpj 