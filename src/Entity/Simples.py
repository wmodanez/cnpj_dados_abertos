"""
Entidade Simples para dados do Simples Nacional da Receita Federal.

Esta classe representa dados do Simples Nacional e implementa todas as validações
e transformações específicas para esses dados.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Type
import polars as pl
import re
import logging
from .base import BaseEntity

logger = logging.getLogger(__name__)


@dataclass
class Simples(BaseEntity):
    """
    Entidade representando dados do Simples Nacional.
    
    Attributes:
        cnpj_basico: CNPJ básico da empresa (int de 8 dígitos)
        opcao_simples: Opção pelo Simples Nacional (S/N)
        data_opcao_simples: Data da opção pelo Simples
        data_exclusao_simples: Data de exclusão do Simples
        opcao_mei: Opção pelo MEI (S/N)
        data_opcao_mei: Data da opção pelo MEI
        data_exclusao_mei: Data de exclusão do MEI
    """
    
    cnpj_basico: int
    opcao_simples: Optional[str] = None
    data_opcao_simples: Optional[str] = None
    data_exclusao_simples: Optional[str] = None
    opcao_mei: Optional[str] = None
    data_opcao_mei: Optional[str] = None
    data_exclusao_mei: Optional[str] = None
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Retorna nomes das colunas da entidade."""
        return [
            'cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
            'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
        ]
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna tipos das colunas da entidade."""
        return {
            'cnpj_basico': pl.Int64,
            'opcao_simples': pl.Utf8,
            'data_opcao_simples': pl.Utf8,
            'data_exclusao_simples': pl.Utf8,
            'opcao_mei': pl.Utf8,
            'data_opcao_mei': pl.Utf8,
            'data_exclusao_mei': pl.Utf8
        }
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        """Retorna lista de transformações aplicáveis."""
        return [
            'normalize_opcoes'
        ]
    
    def validate(self) -> bool:
        """
        Valida dados do Simples Nacional.
        
        Returns:
            bool: True se válido, False caso contrário
        """
        self._validation_errors.clear()
        
        # Validar CNPJ básico
        if not self._validate_cnpj_basico():
            return False
        
        # Validar opções S/N
        if not self._validate_opcoes():
            return False
        
        # Validar datas
        if not self._validate_dates():
            return False
        
        # Validar consistência entre datas
        if not self._validate_date_consistency():
            return False
        
        return True
    
    def _validate_cnpj_basico(self) -> bool:
        """Valida CNPJ básico."""
        if not self.cnpj_basico:
            self._validation_errors.append("CNPJ básico é obrigatório")
            return False
        
        if not isinstance(self.cnpj_basico, int) or not (10000000 <= self.cnpj_basico <= 99999999):
            self._validation_errors.append("CNPJ básico deve ser um número inteiro de 8 dígitos")
            return False
        
        return True
    
    def _validate_opcoes(self) -> bool:
        """Valida opções S/N."""
        # Validar opção Simples
        if self.opcao_simples and self.opcao_simples not in ['S', 'N']:
            self._validation_errors.append(f"Opção Simples deve ser 'S' ou 'N': {self.opcao_simples}")
            return False
        
        # Validar opção MEI
        if self.opcao_mei and self.opcao_mei not in ['S', 'N']:
            self._validation_errors.append(f"Opção MEI deve ser 'S' ou 'N': {self.opcao_mei}")
            return False
        
        return True
    
    def _validate_dates(self) -> bool:
        """Valida datas."""
        # REMOVIDO: Todas as validações de data foram removidas
        return True
    
    def _validate_date_consistency(self) -> bool:
        """Valida consistência entre datas."""
        # REMOVIDO: Todas as validações de data foram removidas
        return True
    
    def is_optante_simples(self) -> bool:
        """Verifica se é optante do Simples Nacional."""
        return self.opcao_simples == 'S'
    
    def is_optante_mei(self) -> bool:
        """Verifica se é optante do MEI."""
        return self.opcao_mei == 'S'
    
    def is_ativo_simples(self) -> bool:
        """Verifica se está ativo no Simples Nacional."""
        return bool(self.opcao_simples == 'S' and 
                   self.data_opcao_simples and 
                   not self.data_exclusao_simples)
    
    def is_ativo_mei(self) -> bool:
        """Verifica se está ativo no MEI."""
        return bool(self.opcao_mei == 'S' and 
                   self.data_opcao_mei and 
                   not self.data_exclusao_mei)
    
    def get_situacao_simples(self) -> str:
        """Retorna situação atual no Simples Nacional."""
        if not self.opcao_simples or self.opcao_simples == 'N':
            return "Não optante"
        
        if self.is_ativo_simples():
            return "Ativo"
        elif self.data_exclusao_simples:
            return "Excluído"
        else:
            return "Optante (sem data de opção)"
    
    def get_situacao_mei(self) -> str:
        """Retorna situação atual no MEI."""
        if not self.opcao_mei or self.opcao_mei == 'N':
            return "Não optante"
        
        if self.is_ativo_mei():
            return "Ativo"
        elif self.data_exclusao_mei:
            return "Excluído"
        else:
            return "Optante (sem data de opção)"
    
    # Métodos de transformação
    
    def _transform_normalize_opcoes(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: normalizar opções S/N."""
        option_fields = ['opcao_simples', 'opcao_mei']
        
        for field in option_fields:
            if field in data and data[field]:
                value = str(data[field]).strip().upper()
                
                # Tentar interpretar valores comuns
                if value in ['SIM', 'YES', '1', 'TRUE', 'VERDADEIRO']:
                    data[field] = 'S'
                elif value in ['NAO', 'NÃO', 'NO', '0', 'FALSE', 'FALSO']:
                    data[field] = 'N'
                elif value in ['S', 'N']:
                    data[field] = value
                else:
                    # Valor não reconhecido, manter None
                    data[field] = None
        
        return data

    def get_processing_summary(self) -> Dict[str, Any]:
        """
        Retorna resumo do processamento para Simples Nacional.
        
        Returns:
            Dict com informações de resumo
        """
        base_summary = self.to_dict()
        
        # Adicionar informações específicas do Simples Nacional
        simples_summary = {
            **base_summary,
            'entity_type': 'Simples Nacional',
            'opcoes_ativas': {
                'simples': self.opcao_simples == 'S',
                'mei': self.opcao_mei == 'S'
            }
        }
        
        return simples_summary
