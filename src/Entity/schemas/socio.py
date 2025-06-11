"""
Schema de validação Pydantic para entidade Socio.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any
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
    
    @field_validator('cnpj_cpf_socio')
    @classmethod
    def validate_cnpj_cpf(cls, v):
        if not v:
            return v
        
        # Remover caracteres não numéricos
        doc_limpo = ''.join(char for char in str(v) if char.isdigit())
        
        if len(doc_limpo) == 11:
            # É um CPF
            if not cls._validate_cpf(doc_limpo):
                raise ValueError(f'CPF inválido: {doc_limpo}')
        elif len(doc_limpo) == 14:
            # É um CNPJ
            if not cls._validate_cnpj(doc_limpo):
                raise ValueError(f'CNPJ inválido: {doc_limpo}')
        else:
            raise ValueError(f'Documento deve ter 11 (CPF) ou 14 (CNPJ) dígitos: {doc_limpo}')
        
        return doc_limpo
    
    @field_validator('nome_socio', 'nome_representante')
    @classmethod
    def validate_names(cls, v):
        if not v:
            return v
        
        nome_limpo = v.strip().upper()
        
        # Verificar se não contém apenas números
        if nome_limpo.isdigit():
            raise ValueError('Nome não pode conter apenas números')
        
        # Verificar tamanho mínimo
        if len(nome_limpo) < 3:
            raise ValueError('Nome deve ter pelo menos 3 caracteres')
        
        return nome_limpo
    
    @field_validator('representante_legal')
    @classmethod
    def validate_representante(cls, v):
        if not v:
            return v
        
        # Remover caracteres não numéricos
        doc_limpo = ''.join(char for char in str(v) if char.isdigit())
        
        # CPF do representante legal deve ter 11 dígitos
        if len(doc_limpo) != 11:
            raise ValueError('CPF do representante deve ter 11 dígitos')
        
        if not cls._validate_cpf(doc_limpo):
            raise ValueError(f'CPF do representante inválido: {doc_limpo}')
        
        return doc_limpo
    
    @field_validator('data_entrada_sociedade')
    @classmethod
    def validate_data_entrada(cls, v):
        if v is None:
            return v
        
        # Verificar se a data não é muito antiga
        if isinstance(v, datetime) and v.year < 1900:
            raise ValueError('Data de entrada muito antiga')
        
        # Verificar se a data não é no futuro
        if isinstance(v, datetime) and v > datetime.now():
            raise ValueError('Data de entrada no futuro')
        
        return v
    
    @staticmethod
    def _validate_cpf(cpf: str) -> bool:
        """Valida CPF"""
        invalid_cpfs = [
            "00000000000", "11111111111", "22222222222", "33333333333",
            "44444444444", "55555555555", "66666666666", "77777777777",
            "88888888888", "99999999999"
        ]
        
        if cpf in invalid_cpfs:
            return False
        
        return True
    
    @staticmethod
    def _validate_cnpj(cnpj: str) -> bool:
        """Valida CNPJ usando algoritmo oficial"""
        # Importar validação de CNPJ do estabelecimento
        from .estabelecimento import EstabelecimentoSchema
        
        if not EstabelecimentoSchema._validate_cnpj_algorithm(cnpj):
            return False
        return True 