"""
Entidade Estabelecimento para dados da Receita Federal.

Esta classe representa um estabelecimento e implementa todas as validações
e transformações específicas para dados de estabelecimentos.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Type
import polars as pl
import re
import logging
from .base import BaseEntity

logger = logging.getLogger(__name__)


@dataclass
class Estabelecimento(BaseEntity):
    """
    Entidade representando um Estabelecimento da Receita Federal.
    
    Attributes:
        cnpj_basico: CNPJ básico (int de 8 dígitos)
        cnpj_ordem: Campo temporário para construir CNPJ completo
        cnpj_dv: Campo temporário para construir CNPJ completo
        matriz_filial: 1=Matriz, 2=Filial
        nome_fantasia: Nome fantasia do estabelecimento
        codigo_situacao: Código da situação cadastral
        data_situacao_cadastral: Data da situação cadastral
        codigo_motivo: Motivo da situação
        nome_cidade_exterior: Cidade no exterior
        data_inicio_atividades: Data de início das atividades
        codigo_cnae: Código CNAE principal
        cnae_secundaria: CNAEs secundários
        uf: Unidade Federativa
        codigo_municipio: Código do município
        cep: CEP
        cnpj_completo: CNPJ completo calculado
    """
    
    cnpj_basico: int  # Volta para int para performance de JOINs
    cnpj_ordem: Optional[str] = field(init=False, default=None)  # Campo temporário para construir CNPJ completo
    cnpj_dv: Optional[str] = field(init=False, default=None)     # Campo temporário para construir CNPJ completo
    matriz_filial: Optional[int] = None
    nome_fantasia: Optional[str] = None
    codigo_situacao: Optional[int] = None
    data_situacao_cadastral: Optional[str] = None  # ALTERADO: De datetime para string
    codigo_motivo: Optional[int] = None
    nome_cidade_exterior: Optional[str] = None
    data_inicio_atividades: Optional[str] = None   # ALTERADO: De datetime para string
    codigo_cnae: Optional[int] = None
    cnae_secundaria: Optional[str] = None
    uf: Optional[str] = None
    codigo_municipio: Optional[int] = None
    cep: Optional[str] = None
    cnpj_completo: Optional[str] = field(init=False, default=None)
    
    def __post_init__(self):
        """Executa processamento após inicialização."""
        # CNPJ completo agora é criado no processador a partir das partes originais
        # Aqui apenas validamos se já foi criado
        
        # Chamar método da classe pai
        super().__post_init__()
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Retorna nomes das colunas da entidade (incluindo campos temporários para CNPJ)."""
        return [
            'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia', 'codigo_situacao',
            'data_situacao_cadastral', 'codigo_motivo', 'nome_cidade_exterior', 'data_inicio_atividades',
            'codigo_cnae', 'cnae_secundaria', 'uf', 'codigo_municipio', 'cep', 'cnpj_completo'
        ]
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna tipos das colunas da entidade."""
        return {
            'cnpj_basico': pl.Int64,     # Volta para int para performance de JOINs
            'cnpj_ordem': pl.Utf8,  # Campo temporário
            'cnpj_dv': pl.Utf8,     # Campo temporário
            'matriz_filial': pl.Int32,
            'nome_fantasia': pl.Utf8,
            'codigo_situacao': pl.Int32,
            'data_situacao_cadastral': pl.Utf8,  # ALTERADO: Manter como string
            'codigo_motivo': pl.Int32,
            'nome_cidade_exterior': pl.Utf8,
            'data_inicio_atividades': pl.Utf8,   # ALTERADO: Manter como string
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
            'normalize_strings',
            'validate_cnpj_parts',
            'create_cnpj_completo'
        ]
    
    def validate(self) -> bool:
        """
        Valida dados do estabelecimento.
        
        Returns:
            bool: True se válido, False caso contrário
        """
        self._validation_errors.clear()
        
        # Validar CNPJ básico
        if not self._validate_cnpj_parts():
            return False
        
        # Validar CNPJ completo usando algoritmo (se disponível)
        if self.cnpj_completo and not self._validate_cnpj_algorithm():
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
        
        return True
    
    def _validate_cnpj_parts(self) -> bool:
        """Valida partes do CNPJ."""
        if not self.cnpj_basico:
            self._validation_errors.append("CNPJ básico é obrigatório")
            return False
        
        # Validar se é inteiro válido
        if not isinstance(self.cnpj_basico, int) or self.cnpj_basico < 0:
            self._validation_errors.append("CNPJ básico deve ser um número inteiro positivo")
            return False
        
        # Rejeitar apenas CNPJ básico zero (inválido)
        if self.cnpj_basico == 0:
            self._validation_errors.append("CNPJ básico não pode ser zero")
            return False
        
        # Verificar se não ultrapassa 8 dígitos (99999999)
        if self.cnpj_basico > 99999999:
            self._validation_errors.append("CNPJ básico não pode ter mais de 8 dígitos")
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
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO', 'EX'
        ]
        
        if self.uf not in ufs_validas:
            self._validation_errors.append(f"UF inválida: {self.uf}")
            return False
        
        return True
    
    def _validate_cep(self) -> bool:
        """Valida CEP."""
        if not self.cep:
            return True  # CEP é opcional
        
        if len(self.cep) != 8 or not self.cep.isdigit():
            self._validation_errors.append("CEP deve ter 8 dígitos")
            return False
        
        return True
    
    def _validate_dates(self) -> bool:
        """Valida datas."""
        # REMOVIDO: Todas as validações de data foram removidas
        return True
    
    def _calculate_cnpj_completo(self) -> Optional[str]:
        """Calcula CNPJ completo."""
        # CNPJ completo agora é criado no processador a partir das partes originais do CSV
        # que são removidas após o processamento
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
        return self.codigo_situacao == 2
    
    def get_cep_formatado(self) -> str:
        """Retorna CEP formatado (XXXXX-XXX)."""
        if not self.cep or len(self.cep) != 8:
            return ""
        
        return f"{self.cep[:5]}-{self.cep[5:]}"
    
    # Métodos de transformação
    
    def _transform_create_cnpj_completo(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: criar CNPJ completo."""
        # Esta transformação é feita no processador agora
        # Aqui apenas retornamos os dados sem modificação
        return data
    
    def _transform_clean_cep(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: limpar CEP."""
        if 'cep' in data and data['cep']:
            cep = re.sub(r'[^\d]', '', str(data['cep']))
            data['cep'] = cep.zfill(8)[:8]
        
        return data
    
    def _transform_normalize_strings(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: normalizar strings."""
        string_fields = ['nome_fantasia', 'nome_cidade_exterior']
        
        for field in string_fields:
            if field in data and data[field]:
                data[field] = str(data[field]).strip().upper()
        
        return data
    
    def _transform_validate_cnpj_parts(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: validar e corrigir partes do CNPJ."""
        if 'cnpj_basico' in data and data['cnpj_basico']:
            try:
                # Se for string, converter para int
                if isinstance(data['cnpj_basico'], str):
                    cnpj = re.sub(r'[^\d]', '', data['cnpj_basico'])
                    data['cnpj_basico'] = int(cnpj) if cnpj else None
                elif isinstance(data['cnpj_basico'], (int, float)):
                    data['cnpj_basico'] = int(data['cnpj_basico'])
                
                # Verificar se tem 8 dígitos
                if data['cnpj_basico'] and not (10000000 <= data['cnpj_basico'] <= 99999999):
                    data['cnpj_basico'] = None
                    
            except (ValueError, TypeError):
                data['cnpj_basico'] = None
        
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
