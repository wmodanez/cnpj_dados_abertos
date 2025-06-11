"""
Schema de validação Pydantic para entidade Painel.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Dict, Any
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)


class PainelSchema(BaseModel):
    """Schema de validação para Painel (dados combinados de Estabelecimento, Simples Nacional, Empresa e Município)"""
    
    # Dados do Estabelecimento (obrigatórios)
    cnpj_basico: int = Field(..., ge=10000000, le=99999999, description="CNPJ básico com 8 dígitos")
    
    # Dados do Estabelecimento (opcionais)
    matriz_filial: Optional[int] = Field(None, ge=1, le=2, description="1=Matriz, 2=Filial")
    codigo_situacao: Optional[int] = Field(None, ge=1, le=99, description="Código da situação cadastral")
    data_situacao_cadastral: Optional[datetime] = Field(None, description="Data da situação cadastral")
    codigo_motivo: Optional[int] = Field(None, ge=1, le=99, description="Código do motivo")
    data_inicio_atividades: Optional[datetime] = Field(None, description="Data de início das atividades")
    codigo_cnae: Optional[int] = Field(None, ge=1, le=9999999, description="Código CNAE principal")
    uf: Optional[str] = Field(None, pattern=r'^[A-Z]{2}$', description="Unidade Federativa")
    codigo_municipio: Optional[int] = Field(None, ge=1, le=999999, description="Código do município")
    tipo_situacao_cadastral: Optional[int] = Field(None, ge=1, le=3, description="Tipo situação: 1=Ativa, 2=Baixa Voluntária, 3=Outras Baixas")
    
    # Dados da Empresa (right join)
    natureza_juridica: Optional[int] = Field(None, ge=1, le=9999, description="Código da natureza jurídica")
    porte_empresa: Optional[int] = Field(None, ge=1, le=5, description="Porte da empresa (1-5)")
    
    # Dados do Simples Nacional (opcionais)
    opcao_simples: Optional[str] = Field(None, pattern=r'^[SN]$', description="Opção pelo Simples Nacional (S/N)")
    data_opcao_simples: Optional[datetime] = Field(None, description="Data da opção pelo Simples")
    data_exclusao_simples: Optional[datetime] = Field(None, description="Data de exclusão do Simples")
    opcao_mei: Optional[str] = Field(None, pattern=r'^[SN]$', description="Opção pelo MEI (S/N)")
    data_opcao_mei: Optional[datetime] = Field(None, description="Data da opção pelo MEI")
    data_exclusao_mei: Optional[datetime] = Field(None, description="Data de exclusão do MEI")
    
    # Campos calculados (opcionais - serão gerados automaticamente)
    situacao_simples: Optional[str] = Field(None, description="Situação atual no Simples Nacional")
    situacao_mei: Optional[str] = Field(None, description="Situação atual no MEI")
    
    class Config:
        extra = "ignore"
        validate_assignment = True
        json_schema_extra = {
            "example": {
                "cnpj_basico": 12345678,
                "matriz_filial": 1,
                "codigo_situacao": 2,
                "data_situacao_cadastral": "2020-01-01T00:00:00",
                "uf": "SP",
                "codigo_municipio": 7107,
                "tipo_situacao_cadastral": 1,
                "natureza_juridica": 206,
                "porte_empresa": 2,
                "opcao_simples": "S",
                "data_opcao_simples": "2020-01-01T00:00:00",
                "opcao_mei": "N"
            }
        }
    
    @field_validator('uf')
    @classmethod
    def validate_uf(cls, v):
        if not v:
            return v
        
        # Excluir estabelecimentos no exterior
        if v.upper() == 'EX':
            raise ValueError('Estabelecimentos no exterior (EX) são excluídos')
        
        ufs_validas = {
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        }
        
        if v.upper() not in ufs_validas:
            raise ValueError(f'UF inválida: {v}')
        
        return v.upper()
    
    @field_validator('natureza_juridica')
    @classmethod
    def validate_natureza_juridica(cls, v):
        if v is not None and not (1 <= v <= 9999):
            raise ValueError('Natureza jurídica deve estar entre 1 e 9999')
        return v
    
    @field_validator('porte_empresa')
    @classmethod
    def validate_porte_empresa(cls, v):
        if v is not None and not (1 <= v <= 5):
            raise ValueError('Porte da empresa deve estar entre 1 e 5')
        return v
    
    @field_validator('data_situacao_cadastral', 'data_inicio_atividades', 
                    'data_opcao_simples', 'data_exclusao_simples', 
                    'data_opcao_mei', 'data_exclusao_mei')
    @classmethod
    def validate_dates(cls, v):
        if v is None:
            return v
        
        if isinstance(v, str):
            try:
                # Tentar converter string para datetime
                v = datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError(f'Data inválida: {v}')
        
        # Verificar se a data não é muito antiga para dados do Simples Nacional
        current_date = datetime.now()
        
        # Para datas do Simples Nacional, não pode ser anterior a 2006
        field_name = cls.__name__ if hasattr(cls, '__name__') else 'data'
        if ('simples' in str(v) or 'mei' in str(v)) and v.year < 2006:
            raise ValueError('Data anterior à criação do Simples Nacional (2006)')
        
        # Verificar se a data não é no futuro
        if v > current_date:
            raise ValueError('Data no futuro')
        
        return v
    
    @model_validator(mode='after')
    def validate_painel_consistency(self):
        """Validar consistência entre todos os campos do painel."""
        
        # Validar consistência entre datas do estabelecimento
        if (self.data_situacao_cadastral and self.data_inicio_atividades and
            self.data_situacao_cadastral < self.data_inicio_atividades):
            raise ValueError('Data de situação cadastral não pode ser anterior ao início das atividades')
        
        # Validar consistência entre datas do Simples Nacional
        if self.data_opcao_simples and self.data_exclusao_simples:
            if self.data_exclusao_simples <= self.data_opcao_simples:
                raise ValueError('Data de exclusão do Simples deve ser posterior à data de opção')
        
        # Validar consistência entre datas do MEI
        if self.data_opcao_mei and self.data_exclusao_mei:
            if self.data_exclusao_mei <= self.data_opcao_mei:
                raise ValueError('Data de exclusão do MEI deve ser posterior à data de opção')
        
        # Validar que se tem opção pelo Simples/MEI, deve ter data de opção
        if self.opcao_simples == 'S' and not self.data_opcao_simples:
            raise ValueError('Se optante do Simples Nacional, deve ter data de opção')
        
        if self.opcao_mei == 'S' and not self.data_opcao_mei:
            raise ValueError('Se optante do MEI, deve ter data de opção')
        
        # Validar que não pode ser optante de ambos Simples e MEI simultaneamente ativos
        is_simples_ativo = (self.opcao_simples == 'S' and 
                           self.data_opcao_simples and 
                           not self.data_exclusao_simples)
        
        is_mei_ativo = (self.opcao_mei == 'S' and 
                       self.data_opcao_mei and 
                       not self.data_exclusao_mei)
        
        if is_simples_ativo and is_mei_ativo:
            # Permitir apenas se as datas forem diferentes (transição)
            if (self.data_opcao_simples and self.data_opcao_mei and
                abs((self.data_opcao_simples - self.data_opcao_mei).days) < 30):
                logger.warning('Empresa com Simples e MEI ativos simultaneamente - possível transição')
        
        # Validar tipo_situacao_cadastral baseado em codigo_situacao e codigo_motivo
        if self.tipo_situacao_cadastral is not None:
            if self.codigo_situacao == 2 and self.tipo_situacao_cadastral != 1:
                raise ValueError('Estabelecimento ativo deve ter tipo_situacao_cadastral = 1')
            elif (self.codigo_situacao == 8 and self.codigo_motivo == 1 and 
                  self.tipo_situacao_cadastral != 2):
                raise ValueError('Baixa com motivo 1 deve ter tipo_situacao_cadastral = 2')
            elif (self.codigo_situacao == 8 and self.codigo_motivo != 1 and 
                  self.tipo_situacao_cadastral != 3):
                raise ValueError('Outras baixas devem ter tipo_situacao_cadastral = 3')
        
        return self 