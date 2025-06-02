"""
Schema de validação Pydantic para entidade Estabelecimento.
"""

from pydantic import BaseModel, Field, validator, root_validator
from typing import Optional
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
    
    @validator('uf')
    def validate_uf(cls, v):
        """Valida UF brasileira"""
        if v is None:
            return v
            
        ufs_validas = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        
        if v not in ufs_validas:
            raise ValueError(f'UF inválida: {v}')
        
        return v
    
    @validator('cep')
    def validate_cep(cls, v):
        """Valida CEP"""
        if v is None:
            return v
        
        # Remover caracteres não numéricos
        cep_clean = re.sub(r'[^\d]', '', v)
        
        if len(cep_clean) != 8:
            raise ValueError(f'CEP deve ter 8 dígitos: {v}')
        
        # Verificar CEPs obviamente inválidos
        if cep_clean == '00000000' or cep_clean == '99999999':
            raise ValueError(f'CEP inválido: {cep_clean}')
        
        return cep_clean
    
    @validator('nome_fantasia')
    def validate_nome_fantasia(cls, v):
        """Valida nome fantasia"""
        if v is None:
            return v
        
        # Remover espaços extras
        v = v.strip()
        
        # Verificar se não é apenas números
        if v.isdigit():
            raise ValueError('Nome fantasia não pode ser apenas números')
        
        return v if v else None
    
    @validator('data_situacao_cadastral', 'data_inicio_atividades')
    def validate_dates(cls, v):
        """Valida datas"""
        if v is None:
            return v
        
        # Verificar se a data não é muito antiga (antes de 1900)
        if v.year < 1900:
            raise ValueError(f'Data muito antiga: {v}')
        
        # Verificar se a data não é no futuro
        if v > datetime.now():
            raise ValueError(f'Data no futuro: {v}')
        
        return v
    
    @root_validator(skip_on_failure=True)
    def validate_cnpj_parts(cls, values):
        """Valida partes do CNPJ"""
        cnpj_basico = values.get('cnpj_basico')
        cnpj_ordem = values.get('cnpj_ordem')
        cnpj_dv = values.get('cnpj_dv')
        
        if all([cnpj_basico, cnpj_ordem, cnpj_dv]):
            # Validar CNPJ completo usando algoritmo
            cnpj_completo = f"{cnpj_basico}{cnpj_ordem}{cnpj_dv}"
            if not cls._validate_cnpj_algorithm(cnpj_completo):
                raise ValueError(f'CNPJ inválido: {cnpj_completo}')
        
        # Validar consistência de datas
        data_situacao = values.get('data_situacao_cadastral')
        data_inicio = values.get('data_inicio_atividades')
        
        if data_situacao and data_inicio:
            if data_situacao < data_inicio:
                logger.warning("Data de situação cadastral anterior ao início das atividades")
        
        return values
    
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