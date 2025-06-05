"""
Entidade Estabelecimento para dados da Receita Federal.

Esta classe representa um estabelecimento e implementa todas as validações
e transformações específicas para dados de estabelecimentos.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Type
import polars as pl
import re
from datetime import datetime
import logging
from .base import BaseEntity

logger = logging.getLogger(__name__)


@dataclass
class Estabelecimento(BaseEntity):
    """
    Entidade representando um Estabelecimento da Receita Federal.
    
    Attributes:
        cnpj_basico: CNPJ básico (8 dígitos)
        cnpj_ordem: Ordem do estabelecimento (4 dígitos)
        cnpj_dv: Dígito verificador (2 dígitos)
        matriz_filial: 1=Matriz, 2=Filial
        nome_fantasia: Nome fantasia do estabelecimento
        codigo_situacao_cadastral: Código da situação cadastral
        data_situacao_cadastral: Data da situação cadastral
        codigo_motivo_situacao_cadastral: Motivo da situação
        nome_cidade_exterior: Cidade no exterior
        pais: País
        data_inicio_atividades: Data de início das atividades
        codigo_cnae: Código CNAE principal
        cnae_secundaria: CNAEs secundários
        uf: Unidade Federativa
        codigo_municipio: Código do município
        cep: CEP
        cnpj_completo: CNPJ completo calculado
    """
    
    cnpj_basico: str
    cnpj_ordem: str
    cnpj_dv: str
    matriz_filial: Optional[int] = None
    nome_fantasia: Optional[str] = None
    codigo_situacao_cadastral: Optional[int] = None
    data_situacao_cadastral: Optional[datetime] = None
    codigo_motivo_situacao_cadastral: Optional[int] = None
    nome_cidade_exterior: Optional[str] = None
    pais: Optional[str] = None
    data_inicio_atividades: Optional[datetime] = None
    codigo_cnae: Optional[int] = None
    cnae_secundaria: Optional[str] = None
    uf: Optional[str] = None
    codigo_municipio: Optional[int] = None
    cep: Optional[str] = None
    cnpj_completo: Optional[str] = field(init=False, default=None)
    
    def __post_init__(self):
        """Executa processamento após inicialização."""
        # Calcular CNPJ completo
        self.cnpj_completo = self._calculate_cnpj_completo()
        
        # Chamar método da classe pai
        super().__post_init__()
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Retorna nomes das colunas da entidade."""
        return [
            'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia',
            'codigo_situacao_cadastral', 'data_situacao_cadastral', 'codigo_motivo_situacao_cadastral',
            'nome_cidade_exterior', 'pais', 'data_inicio_atividades', 'codigo_cnae',
            'cnae_secundaria', 'uf', 'codigo_municipio', 'cep', 'cnpj_completo'
        ]
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna tipos das colunas da entidade."""
        return {
            'cnpj_basico': pl.Utf8,
            'cnpj_ordem': pl.Utf8,
            'cnpj_dv': pl.Utf8,
            'matriz_filial': pl.Int32,
            'nome_fantasia': pl.Utf8,
            'codigo_situacao_cadastral': pl.Int32,
            'data_situacao_cadastral': pl.Datetime,
            'codigo_motivo_situacao_cadastral': pl.Int32,
            'nome_cidade_exterior': pl.Utf8,
            'pais': pl.Utf8,
            'data_inicio_atividades': pl.Datetime,
            'codigo_cnae': pl.Int32,
            'cnae_secundaria': pl.Utf8,
            'uf': pl.Utf8,
            'codigo_municipio': pl.Int32,
            'cep': pl.Utf8,
            'cnpj_completo': pl.Utf8
        }
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        """Retorna lista de transformações aplicáveis."""
        return [
            'create_cnpj_completo', 
            'clean_cep', 
            'convert_dates',
            'normalize_strings',
            'validate_cnpj_parts',
            'clean_nome_fantasia'
        ]
    
    def validate(self) -> bool:
        """
        Valida dados do estabelecimento.
        
        Returns:
            bool: True se válido, False caso contrário
        """
        self._validation_errors.clear()
        
        # Validar partes do CNPJ
        if not self._validate_cnpj_parts():
            return False
        
        # Validar CNPJ completo usando algoritmo
        if not self._validate_cnpj_algorithm():
            return False
        
        # Validar UF
        if self.uf and not self._validate_uf():
            return False
        
        # Validar CEP
        if self.cep and not self._validate_cep():
            return False
        
        # Validar matriz/filial
        if self.matriz_filial is not None and not (1 <= self.matriz_filial <= 2):
            self._validation_errors.append("Matriz/filial deve ser 1 (Matriz) ou 2 (Filial)")
            return False
        
        # Validar datas
        if not self._validate_dates():
            return False
        
        return True
    
    def _validate_cnpj_parts(self) -> bool:
        """Valida partes do CNPJ."""
        if not all([self.cnpj_basico, self.cnpj_ordem, self.cnpj_dv]):
            self._validation_errors.append("CNPJ básico, ordem e DV são obrigatórios")
            return False
        
        if len(self.cnpj_basico) != 8 or not self.cnpj_basico.isdigit():
            self._validation_errors.append("CNPJ básico deve ter 8 dígitos")
            return False
        
        if len(self.cnpj_ordem) != 4 or not self.cnpj_ordem.isdigit():
            self._validation_errors.append("CNPJ ordem deve ter 4 dígitos")
            return False
        
        if len(self.cnpj_dv) != 2 or not self.cnpj_dv.isdigit():
            self._validation_errors.append("CNPJ DV deve ter 2 dígitos")
            return False
        
        return True
    
    def _validate_cnpj_algorithm(self) -> bool:
        """Valida CNPJ usando algoritmo oficial."""
        if not self.cnpj_completo:
            return True  # Se não tem CNPJ completo, não valida
        
        cnpj = self.cnpj_completo
        
        if len(cnpj) != 14:
            self._validation_errors.append("CNPJ completo deve ter 14 dígitos")
            return False
        
        # Verificar se não são todos iguais
        if cnpj == cnpj[0] * 14:
            self._validation_errors.append("CNPJ inválido (todos os dígitos iguais)")
            return False
        
        # Calcular primeiro dígito verificador
        sequence = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum_result = sum(int(cnpj[i]) * sequence[i] for i in range(12))
        remainder = sum_result % 11
        first_digit = 0 if remainder < 2 else 11 - remainder
        
        if int(cnpj[12]) != first_digit:
            self._validation_errors.append("CNPJ inválido (primeiro dígito verificador)")
            return False
        
        # Calcular segundo dígito verificador
        sequence = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum_result = sum(int(cnpj[i]) * sequence[i] for i in range(13))
        remainder = sum_result % 11
        second_digit = 0 if remainder < 2 else 11 - remainder
        
        if int(cnpj[13]) != second_digit:
            self._validation_errors.append("CNPJ inválido (segundo dígito verificador)")
            return False
        
        return True
    
    def _validate_uf(self) -> bool:
        """Valida UF brasileira."""
        ufs_validas = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        
        if self.uf not in ufs_validas:
            self._validation_errors.append(f"UF inválida: {self.uf}")
            return False
        
        return True
    
    def _validate_cep(self) -> bool:
        """Valida CEP."""
        if len(self.cep) != 8 or not self.cep.isdigit():
            self._validation_errors.append("CEP deve ter 8 dígitos")
            return False
        
        # CEPs obviamente inválidos
        if self.cep in ['00000000', '99999999']:
            self._validation_errors.append(f"CEP inválido: {self.cep}")
            return False
        
        return True
    
    def _validate_dates(self) -> bool:
        """Valida datas."""
        current_date = datetime.now()
        
        # Validar data de situação cadastral
        if self.data_situacao_cadastral:
            if self.data_situacao_cadastral.year < 1900:
                self._validation_errors.append("Data de situação cadastral muito antiga")
                return False
            
            if self.data_situacao_cadastral > current_date:
                self._validation_errors.append("Data de situação cadastral no futuro")
                return False
        
        # Validar data de início das atividades
        if self.data_inicio_atividades:
            if self.data_inicio_atividades.year < 1900:
                self._validation_errors.append("Data de início das atividades muito antiga")
                return False
            
            if self.data_inicio_atividades > current_date:
                self._validation_errors.append("Data de início das atividades no futuro")
                return False
        
        return True
    
    def _calculate_cnpj_completo(self) -> Optional[str]:
        """Calcula CNPJ completo."""
        if all([self.cnpj_basico, self.cnpj_ordem, self.cnpj_dv]):
            return f"{self.cnpj_basico.zfill(8)}{self.cnpj_ordem.zfill(4)}{self.cnpj_dv.zfill(2)}"
        return None
    
    def get_cnpj_formatado(self) -> str:
        """Retorna CNPJ formatado (XX.XXX.XXX/XXXX-XX)."""
        if not self.cnpj_completo:
            return ""
        
        cnpj = self.cnpj_completo
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
    
    def is_matriz(self) -> bool:
        """Verifica se é matriz."""
        return self.matriz_filial == 1
    
    def is_filial(self) -> bool:
        """Verifica se é filial."""
        return self.matriz_filial == 2
    
    def is_ativo(self) -> bool:
        """Verifica se está ativo (situação cadastral 2)."""
        return self.codigo_situacao_cadastral == 2
    
    def get_cep_formatado(self) -> str:
        """Retorna CEP formatado (XXXXX-XXX)."""
        if not self.cep or len(self.cep) != 8:
            return ""
        
        return f"{self.cep[:5]}-{self.cep[5:]}"
    
    # Métodos de transformação
    
    def _transform_create_cnpj_completo(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: criar CNPJ completo."""
        if all(key in data for key in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
            cnpj_basico = str(data['cnpj_basico']).zfill(8)
            cnpj_ordem = str(data['cnpj_ordem']).zfill(4)
            cnpj_dv = str(data['cnpj_dv']).zfill(2)
            data['cnpj_completo'] = f"{cnpj_basico}{cnpj_ordem}{cnpj_dv}"
        
        return data
    
    def _transform_clean_cep(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: limpar CEP."""
        if 'cep' in data and data['cep']:
            cep = re.sub(r'[^\d]', '', str(data['cep']))
            data['cep'] = cep.zfill(8)[:8]
        
        return data
    
    def _transform_convert_dates(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: converter datas."""
        date_fields = ['data_situacao_cadastral', 'data_inicio_atividades']
        
        for field in date_fields:
            if field in data and data[field]:
                if isinstance(data[field], str):
                    try:
                        # Tentar converter string para datetime
                        data[field] = datetime.fromisoformat(data[field].replace('Z', '+00:00'))
                    except ValueError:
                        data[field] = None
        
        return data
    
    def _transform_normalize_strings(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: normalizar strings."""
        string_fields = ['nome_fantasia', 'nome_cidade_exterior', 'pais', 'uf']
        
        for field in string_fields:
            if field in data and data[field]:
                data[field] = str(data[field]).strip().upper()
        
        return data
    
    def _transform_validate_cnpj_parts(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: validar e corrigir partes do CNPJ."""
        cnpj_fields = [
            ('cnpj_basico', 8),
            ('cnpj_ordem', 4),
            ('cnpj_dv', 2)
        ]
        
        for field, length in cnpj_fields:
            if field in data and data[field]:
                value = re.sub(r'[^\d]', '', str(data[field]))
                data[field] = value.zfill(length)[:length]
        
        return data
    
    def _transform_clean_nome_fantasia(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: limpar nome fantasia."""
        if 'nome_fantasia' in data and data['nome_fantasia']:
            nome = str(data['nome_fantasia']).strip()
            
            # Se é apenas números, remover
            if nome.isdigit():
                data['nome_fantasia'] = None
            else:
                data['nome_fantasia'] = nome.upper()
        
        return data
