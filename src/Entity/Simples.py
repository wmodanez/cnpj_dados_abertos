"""
Entidade Simples para dados do Simples Nacional da Receita Federal.

Esta classe representa dados do Simples Nacional e implementa todas as validações
e transformações específicas para esses dados.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Type
import polars as pl
import re
from datetime import datetime
import logging
from .base import BaseEntity

logger = logging.getLogger(__name__)


@dataclass
class Simples(BaseEntity):
    """
    Entidade representando dados do Simples Nacional.
    
    Attributes:
        cnpj_basico: CNPJ básico da empresa
        opcao_simples: Opção pelo Simples Nacional (S/N)
        data_opcao_simples: Data da opção pelo Simples
        data_exclusao_simples: Data de exclusão do Simples
        opcao_mei: Opção pelo MEI (S/N)
        data_opcao_mei: Data da opção pelo MEI
        data_exclusao_mei: Data de exclusão do MEI
    """
    
    cnpj_basico: str
    opcao_simples: Optional[str] = None
    data_opcao_simples: Optional[datetime] = None
    data_exclusao_simples: Optional[datetime] = None
    opcao_mei: Optional[str] = None
    data_opcao_mei: Optional[datetime] = None
    data_exclusao_mei: Optional[datetime] = None
    
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
            'cnpj_basico': pl.Utf8,
            'opcao_simples': pl.Utf8,
            'data_opcao_simples': pl.Date,
            'data_exclusao_simples': pl.Date,
            'opcao_mei': pl.Utf8,
            'data_opcao_mei': pl.Date,
            'data_exclusao_mei': pl.Date
        }
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        """Retorna lista de transformações aplicáveis."""
        return [
            'convert_dates',
            'normalize_opcoes',
            'validate_cnpj_basico',
            'validate_date_consistency'
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
        
        if len(self.cnpj_basico) != 8 or not self.cnpj_basico.isdigit():
            self._validation_errors.append("CNPJ básico deve ter 8 dígitos")
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
        current_date = datetime.now()
        
        # Datas para validar
        dates_to_validate = [
            ('data_opcao_simples', self.data_opcao_simples),
            ('data_exclusao_simples', self.data_exclusao_simples),
            ('data_opcao_mei', self.data_opcao_mei),
            ('data_exclusao_mei', self.data_exclusao_mei)
        ]
        
        for field_name, date_value in dates_to_validate:
            if date_value:
                # Verificar se a data não é muito antiga (Simples Nacional criado em 2006)
                if date_value.year < 2006:
                    self._validation_errors.append(f"{field_name} anterior à criação do Simples Nacional (2006)")
                    return False
                
                # Verificar se a data não é no futuro
                if date_value > current_date:
                    self._validation_errors.append(f"{field_name} no futuro")
                    return False
        
        return True
    
    def _validate_date_consistency(self) -> bool:
        """Valida consistência entre datas."""
        # Validar Simples Nacional
        if self.data_opcao_simples and self.data_exclusao_simples:
            if self.data_exclusao_simples <= self.data_opcao_simples:
                self._validation_errors.append("Data de exclusão do Simples deve ser posterior à data de opção")
                return False
        
        # Validar MEI
        if self.data_opcao_mei and self.data_exclusao_mei:
            if self.data_exclusao_mei <= self.data_opcao_mei:
                self._validation_errors.append("Data de exclusão do MEI deve ser posterior à data de opção")
                return False
        
        # Validar consistência entre opção e datas
        if self.opcao_simples == 'S' and not self.data_opcao_simples:
            logger.warning(f"CNPJ {self.cnpj_basico}: Empresa optante do Simples sem data de opção")
        
        if self.opcao_mei == 'S' and not self.data_opcao_mei:
            logger.warning(f"CNPJ {self.cnpj_basico}: Empresa optante do MEI sem data de opção")
        
        # MEI e Simples podem ser mutuamente exclusivos em alguns casos
        if (self.opcao_simples == 'S' and self.opcao_mei == 'S' and 
            self.data_opcao_simples and self.data_opcao_mei):
            # Verificar se as datas fazem sentido
            diff_days = abs((self.data_opcao_simples - self.data_opcao_mei).days)
            if diff_days < 30:
                logger.warning(f"CNPJ {self.cnpj_basico}: Opção simultânea por Simples e MEI em datas próximas")
        
        return True
    
    def is_optante_simples(self) -> bool:
        """Verifica se é optante do Simples Nacional."""
        return self.opcao_simples == 'S'
    
    def is_optante_mei(self) -> bool:
        """Verifica se é optante do MEI."""
        return self.opcao_mei == 'S'
    
    def is_ativo_simples(self) -> bool:
        """Verifica se está ativo no Simples Nacional."""
        return (self.opcao_simples == 'S' and 
                self.data_opcao_simples and 
                not self.data_exclusao_simples)
    
    def is_ativo_mei(self) -> bool:
        """Verifica se está ativo no MEI."""
        return (self.opcao_mei == 'S' and 
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
    
    def get_tempo_simples(self) -> Optional[int]:
        """Retorna tempo em dias no Simples Nacional."""
        if not self.data_opcao_simples:
            return None
        
        end_date = self.data_exclusao_simples or datetime.now()
        return (end_date - self.data_opcao_simples).days
    
    def get_tempo_mei(self) -> Optional[int]:
        """Retorna tempo em dias no MEI."""
        if not self.data_opcao_mei:
            return None
        
        end_date = self.data_exclusao_mei or datetime.now()
        return (end_date - self.data_opcao_mei).days
    
    # Métodos de transformação
    
    def _transform_convert_dates(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: converter datas."""
        date_fields = [
            'data_opcao_simples', 'data_exclusao_simples', 
            'data_opcao_mei', 'data_exclusao_mei'
        ]
        
        for field in date_fields:
            if field in data and data[field]:
                if isinstance(data[field], str):
                    try:
                        # Tentar converter string para datetime
                        data[field] = datetime.fromisoformat(data[field].replace('Z', '+00:00'))
                    except ValueError:
                        data[field] = None
        
        return data
    
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
    
    def _transform_validate_cnpj_basico(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: validar e corrigir CNPJ básico."""
        if 'cnpj_basico' in data and data['cnpj_basico']:
            cnpj = re.sub(r'[^\d]', '', str(data['cnpj_basico']))
            data['cnpj_basico'] = cnpj.zfill(8)[:8]
        
        return data
    
    def _transform_validate_date_consistency(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: validar consistência de datas."""
        # Verificar se data de exclusão é posterior à data de opção (Simples)
        if (data.get('data_opcao_simples') and data.get('data_exclusao_simples')):
            if data['data_exclusao_simples'] <= data['data_opcao_simples']:
                # Remover data de exclusão inválida
                data['data_exclusao_simples'] = None
                logger.warning("Data de exclusão do Simples inválida removida")
        
        # Verificar se data de exclusão é posterior à data de opção (MEI)
        if (data.get('data_opcao_mei') and data.get('data_exclusao_mei')):
            if data['data_exclusao_mei'] <= data['data_opcao_mei']:
                # Remover data de exclusão inválida
                data['data_exclusao_mei'] = None
                logger.warning("Data de exclusão do MEI inválida removida")
        
        return data
