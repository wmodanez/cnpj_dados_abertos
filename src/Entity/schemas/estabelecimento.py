"""
Schema de validação Pydantic para entidade Estabelecimento.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Dict, Any
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)


class EstabelecimentoSchema(BaseModel):
    """Schema de validação para Estabelecimento"""
    
    cnpj_basico: str = Field(..., pattern=r'^\d{8}$', description="CNPJ básico")
    cnpj_ordem: str = Field(..., pattern=r'^\d{4}$', description="Ordem do estabelecimento")
    cnpj_dv: str = Field(..., pattern=r'^\d{2}$', description="Dígito verificador")
    matriz_filial: Optional[int] = Field(None, ge=1, le=2, description="1=Matriz, 2=Filial")
    nome_fantasia: Optional[str] = Field(None, max_length=300, description="Nome fantasia")
    codigo_situacao_cadastral: Optional[int] = Field(None, ge=1, le=99, 
                                                    description="Código da situação cadastral")
    data_situacao_cadastral: Optional[datetime] = Field(None, description="Data da situação cadastral")
    codigo_motivo_situacao_cadastral: Optional[int] = Field(None, ge=1, le=99, 
                                                           description="Motivo da situação")
    nome_cidade_exterior: Optional[str] = Field(None, max_length=100, 
                                               description="Cidade no exterior")
    pais: Optional[str] = Field(None, max_length=100, description="País")
    data_inicio_atividades: Optional[datetime] = Field(None, description="Data de início das atividades")
    codigo_cnae: Optional[int] = Field(None, ge=1, le=9999999, description="Código CNAE principal")
    cnae_secundaria: Optional[str] = Field(None, max_length=1000, description="CNAEs secundários")
    uf: Optional[str] = Field(None, pattern=r'^[A-Z]{2}$', description="Unidade Federativa")
    codigo_municipio: Optional[int] = Field(None, ge=1, le=999999, description="Código do município")
    cep: Optional[str] = Field(None, pattern=r'^\d{8}$', description="CEP")
    
    class Config:
        extra = "ignore"
        validate_assignment = True
        json_schema_extra = {
            "example": {
                "cnpj_basico": "12345678",
                "cnpj_ordem": "0001",
                "cnpj_dv": "95",
                "matriz_filial": 1,
                "nome_fantasia": "LOJA EXEMPLO",
                "codigo_situacao_cadastral": 2,
                "data_situacao_cadastral": "2020-01-01T00:00:00",
                "uf": "SP",
                "codigo_municipio": 7107,
                "cep": "01310100"
            }
        }
    
    @field_validator('uf')
    @classmethod
    def validate_uf(cls, v):
        if not v:
            return v
        
        ufs_validas = {
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        }
        
        if v.upper() not in ufs_validas:
            raise ValueError(f'UF inválida: {v}')
        
        return v.upper()
    
    @field_validator('cep')
    @classmethod
    def validate_cep(cls, v):
        if not v:
            return v
        
        # Remover caracteres não numéricos
        cep_limpo = ''.join(char for char in str(v) if char.isdigit())
        
        if len(cep_limpo) != 8:
            raise ValueError('CEP deve ter 8 dígitos')
        
        return cep_limpo
    
    @field_validator('nome_fantasia')
    @classmethod
    def validate_nome_fantasia(cls, v):
        if not v:
            return None
        
        nome_limpo = v.strip().upper()
        
        # Se for apenas números, considerar inválido
        if nome_limpo.isdigit():
            return None
        
        return nome_limpo if len(nome_limpo) >= 2 else None
    
    @field_validator('data_situacao_cadastral', 'data_inicio_atividades')
    @classmethod
    def validate_dates(cls, v):
        if v is None:
            return v
        
        if isinstance(v, str):
            try:
                # Tentar converter string para datetime
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError(f'Data inválida: {v}')
        
        return v
    
    @model_validator(mode='after')
    def validate_establishment_consistency(self):
        """Validações que dependem de múltiplos campos."""
        # Verificar consistência entre datas
        if (self.data_situacao_cadastral and self.data_inicio_atividades and
            self.data_situacao_cadastral < self.data_inicio_atividades):
            raise ValueError('Data de situação cadastral não pode ser anterior ao início das atividades')
        
        # Validar partes do CNPJ se disponíveis
        if all([self.cnpj_basico, self.cnpj_ordem, self.cnpj_dv]):
            cnpj_completo = f"{self.cnpj_basico}{self.cnpj_ordem}{self.cnpj_dv}"
            if not self._validate_cnpj_algorithm(cnpj_completo):
                raise ValueError(f'CNPJ inválido: {cnpj_completo}')
        
        return self
    
    @staticmethod
    def _validate_cnpj_algorithm(cnpj: str) -> bool:
        """Valida CNPJ usando algoritmo oficial"""
        # Implementação do algoritmo de validação de CNPJ
        if len(cnpj) != 14:
            return False
        
        # Verificar se não são todos iguais
        if cnpj == cnpj[0] * 14:
            return False
        
        # Calcular primeiro dígito verificador
        sequence = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum_result = sum(int(cnpj[i]) * sequence[i] for i in range(12))
        remainder = sum_result % 11
        first_digit = 0 if remainder < 2 else 11 - remainder
        
        if int(cnpj[12]) != first_digit:
            return False
        
        # Calcular segundo dígito verificador
        sequence = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum_result = sum(int(cnpj[i]) * sequence[i] for i in range(13))
        remainder = sum_result % 11
        second_digit = 0 if remainder < 2 else 11 - remainder
        
        return int(cnpj[13]) == second_digit 