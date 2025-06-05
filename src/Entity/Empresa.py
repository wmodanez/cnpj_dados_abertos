"""
Entidade Empresa para dados da Receita Federal.

Esta classe representa uma empresa e implementa todas as validações
e transformações específicas para dados de empresas.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Type
import polars as pl
import re
import logging
from .base import BaseEntity

logger = logging.getLogger(__name__)


@dataclass
class Empresa(BaseEntity):
    """
    Entidade representando uma Empresa da Receita Federal.
    
    Attributes:
        cnpj_basico: CNPJ básico da empresa (8 dígitos)
        razao_social: Razão social da empresa
        natureza_juridica: Código da natureza jurídica
        qualificacao_responsavel: Qualificação do responsável
        capital_social: Capital social da empresa
        porte_empresa: Porte da empresa (1-5)
        ente_federativo_responsavel: Ente federativo responsável
        cpf_extraido: CPF extraído da razão social (calculado)
    """
    
    cnpj_basico: str
    razao_social: str
    natureza_juridica: Optional[int] = None
    qualificacao_responsavel: Optional[int] = None
    capital_social: Optional[float] = None
    porte_empresa: Optional[int] = None
    ente_federativo_responsavel: Optional[str] = None
    cpf_extraido: Optional[str] = None
    
    def __post_init__(self):
        """Executa processamento após inicialização."""
        # Extrair CPF da razão social se não fornecido
        if not self.cpf_extraido and self.razao_social:
            self.cpf_extraido = self._extract_cpf_from_razao_social()
        
        # Chamar método da classe pai
        super().__post_init__()
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Retorna nomes das colunas da entidade."""
        return [
            'cnpj_basico', 'razao_social', 'natureza_juridica', 
            'qualificacao_responsavel', 'capital_social', 'porte_empresa', 
            'ente_federativo_responsavel', 'cpf_extraido'
        ]
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna tipos das colunas da entidade."""
        return {
            'cnpj_basico': pl.Utf8,
            'razao_social': pl.Utf8,
            'natureza_juridica': pl.Int32,
            'qualificacao_responsavel': pl.Int32,
            'capital_social': pl.Float64,
            'porte_empresa': pl.Int32,
            'ente_federativo_responsavel': pl.Utf8
        }
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        """Retorna lista de transformações aplicáveis."""
        return [
            'extract_cpf', 
            'clean_razao_social', 
            'convert_capital_social',
            'normalize_strings',
            'validate_cnpj_basico'
        ]
    
    def validate(self) -> bool:
        """
        Valida dados da empresa.
        
        Returns:
            bool: True se válida, False caso contrário
        """
        self._validation_errors.clear()
        
        # Validar CNPJ básico
        if not self._validate_cnpj_basico():
            return False
        
        # Validar razão social
        if not self._validate_razao_social():
            return False
        
        # Validar CPF extraído (se presente)
        if self.cpf_extraido and not self._validate_cpf(self.cpf_extraido):
            self._validation_errors.append(f"CPF extraído inválido: {self.cpf_extraido}")
            return False
        
        # Validar capital social
        if self.capital_social is not None and self.capital_social < 0:
            self._validation_errors.append("Capital social não pode ser negativo")
            return False
        
        # Validar porte da empresa
        if self.porte_empresa is not None and not (1 <= self.porte_empresa <= 5):
            self._validation_errors.append("Porte da empresa deve estar entre 1 e 5")
            return False
        
        return True
    
    def _validate_cnpj_basico(self) -> bool:
        """Valida CNPJ básico."""
        if not self.cnpj_basico:
            self._validation_errors.append("CNPJ básico é obrigatório")
            return False
        
        if len(self.cnpj_basico) != 8:
            self._validation_errors.append("CNPJ básico deve ter 8 dígitos")
            return False
        
        if not self.cnpj_basico.isdigit():
            self._validation_errors.append("CNPJ básico deve conter apenas números")
            return False
        
        return True
    
    def _validate_razao_social(self) -> bool:
        """Valida razão social."""
        if not self.razao_social:
            self._validation_errors.append("Razão social é obrigatória")
            return False
        
        if len(self.razao_social.strip()) == 0:
            self._validation_errors.append("Razão social não pode estar vazia")
            return False
        
        if self.razao_social.strip().isdigit():
            self._validation_errors.append("Razão social não pode conter apenas números")
            return False
        
        return True
    
    def _validate_cpf(self, cpf: str) -> bool:
        """Valida CPF extraído."""
        if len(cpf) != 11 or not cpf.isdigit():
            return False
        
        invalid_cpfs = [
            "00000000000", "11111111111", "22222222222", "33333333333",
            "44444444444", "55555555555", "66666666666", "77777777777",
            "88888888888", "99999999999"
        ]
        
        return cpf not in invalid_cpfs
    
    def _extract_cpf_from_razao_social(self) -> Optional[str]:
        """Extrai CPF da razão social."""
        if not self.razao_social:
            return None
        
        cpf_pattern = r'(\d{11})'
        match = re.search(cpf_pattern, self.razao_social)
        
        if match:
            cpf = match.group(1)
            if self._validate_cpf(cpf):
                return cpf
        
        return None
    
    def get_razao_social_limpa(self) -> str:
        """Retorna razão social sem CPF."""
        if not self.razao_social:
            return ""
        
        cpf_pattern = r'(\d{11})'
        return re.sub(cpf_pattern, '', self.razao_social).strip()
    
    def is_microempresa(self) -> bool:
        """Verifica se é microempresa."""
        return self.porte_empresa == 1
    
    def is_pequena_empresa(self) -> bool:
        """Verifica se é pequena empresa."""
        return self.porte_empresa == 2
    
    def is_empresa_privada(self) -> bool:
        """Verifica se é empresa privada (tem CPF extraído)."""
        return self.cpf_extraido is not None
    
    # Métodos de transformação
    
    def _transform_extract_cpf(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: extrair CPF da razão social."""
        if 'razao_social' in data and data['razao_social']:
            cpf_pattern = r'(\d{11})'
            match = re.search(cpf_pattern, data['razao_social'])
            
            if match:
                cpf = match.group(1)
                if self._validate_cpf(cpf):
                    data['cpf_extraido'] = cpf
        
        return data
    
    def _transform_clean_razao_social(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: limpar razão social removendo CPF."""
        if 'razao_social' in data and data['razao_social']:
            cpf_pattern = r'(\d{11})'
            data['razao_social'] = re.sub(cpf_pattern, '', data['razao_social']).strip()
        
        return data
    
    def _transform_convert_capital_social(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: converter capital social para float."""
        if 'capital_social' in data and data['capital_social'] is not None:
            try:
                if isinstance(data['capital_social'], str):
                    # Remover caracteres não numéricos exceto ponto e vírgula
                    cleaned = re.sub(r'[^\d.,]', '', data['capital_social'])
                    # Converter vírgula para ponto
                    cleaned = cleaned.replace(',', '.')
                    data['capital_social'] = float(cleaned)
            except (ValueError, TypeError):
                data['capital_social'] = None
        
        return data
    
    def _transform_normalize_strings(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: normalizar strings."""
        string_fields = ['razao_social', 'ente_federativo_responsavel']
        
        for field in string_fields:
            if field in data and data[field]:
                data[field] = str(data[field]).strip().upper()
        
        return data
    
    def _transform_validate_cnpj_basico(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: validar e corrigir CNPJ básico."""
        if 'cnpj_basico' in data and data['cnpj_basico']:
            cnpj = re.sub(r'[^\d]', '', str(data['cnpj_basico']))
            data['cnpj_basico'] = cnpj.zfill(8)[:8]
        
        return data
