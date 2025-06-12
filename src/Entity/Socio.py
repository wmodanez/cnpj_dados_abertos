"""
Entidade Socio para dados da Receita Federal.

Esta classe representa um sócio e implementa todas as validações
e transformações específicas para dados de sócios.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Type
import polars as pl
import re
import logging
from .base import BaseEntity

logger = logging.getLogger(__name__)


@dataclass
class Socio(BaseEntity):
    """
    Entidade representando um Sócio da Receita Federal.
    
    Attributes:
        cnpj_basico: CNPJ básico da empresa (int de 8 dígitos)
        identificador_socio: Tipo de sócio (1-9)
        nome_socio: Nome do sócio
        cnpj_cpf_socio: CPF ou CNPJ do sócio
        qualificacao_socio: Qualificação do sócio
        data_entrada_sociedade: Data de entrada na sociedade
        pais: País do sócio
        representante_legal: CPF do representante legal
        nome_representante: Nome do representante
        qualificacao_representante_legal: Qualificação do representante
        faixa_etaria: Faixa etária
    """
    
    cnpj_basico: int
    identificador_socio: Optional[int] = None
    nome_socio: Optional[str] = None
    cnpj_cpf_socio: Optional[str] = None
    qualificacao_socio: Optional[int] = None
    data_entrada_sociedade: Optional[str] = None
    pais: Optional[str] = None
    representante_legal: Optional[str] = None
    nome_representante: Optional[str] = None
    qualificacao_representante_legal: Optional[int] = None
    faixa_etaria: Optional[str] = None
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Retorna nomes das colunas da entidade."""
        return [
            'cnpj_basico', 'identificador_socio', 'nome_socio', 'cnpj_cpf_socio',
            'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
            'nome_representante', 'qualificacao_representante_legal', 'faixa_etaria'
        ]
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna tipos das colunas da entidade."""
        return {
            'cnpj_basico': pl.Int64,
            'identificador_socio': pl.Int64,
            'nome_socio': pl.Utf8,
            'cnpj_cpf_socio': pl.Utf8,
            'qualificacao_socio': pl.Int64,
            'data_entrada_sociedade': pl.Utf8,
            'pais': pl.Utf8,
            'representante_legal': pl.Utf8,
            'nome_representante': pl.Utf8,
            'qualificacao_representante_legal': pl.Int64,
            'faixa_etaria': pl.Utf8
        }
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        """Retorna lista de transformações aplicáveis."""
        return [
            'validate_cpf_cnpj',
            'normalize_names',
            'clean_representante_legal'
        ]
    
    def validate(self) -> bool:
        """
        Valida dados do sócio.
        
        Returns:
            bool: True se válido, False caso contrário
        """
        self._validation_errors.clear()
        
        # Validar CNPJ básico
        if not self._validate_cnpj_basico():
            return False
        
        # Validar CPF/CNPJ do sócio
        if self.cnpj_cpf_socio and not self._validate_cpf_cnpj(self.cnpj_cpf_socio):
            return False
        
        # Validar representante legal (CPF)
        if self.representante_legal and not self._validate_cpf(self.representante_legal):
            self._validation_errors.append(f"CPF do representante legal inválido: {self.representante_legal}")
            return False
        
        # Validar identificador do sócio
        if self.identificador_socio is not None and not (1 <= self.identificador_socio <= 9):
            self._validation_errors.append("Identificador do sócio deve estar entre 1 e 9")
            return False
        
        # Validar nomes
        if not self._validate_names():
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
    
    def _validate_cpf_cnpj(self, documento: str) -> bool:
        """Valida CPF ou CNPJ do sócio."""
        if len(documento) == 11:  # CPF
            return self._validate_cpf(documento)
        elif len(documento) == 14:  # CNPJ
            return self._validate_cnpj(documento)
        else:
            self._validation_errors.append(f"Documento deve ter 11 (CPF) ou 14 (CNPJ) dígitos: {documento}")
            return False
    
    def _validate_cpf(self, cpf: str) -> bool:
        """Valida CPF."""
        if len(cpf) != 11 or not cpf.isdigit():
            return False
        
        invalid_cpfs = [
            "00000000000", "11111111111", "22222222222", "33333333333",
            "44444444444", "55555555555", "66666666666", "77777777777",
            "88888888888", "99999999999"
        ]
        
        if cpf in invalid_cpfs:
            return False
        
        # Algoritmo de validação de CPF
        # Calcular primeiro dígito verificador
        sum_result = sum(int(cpf[i]) * (10 - i) for i in range(9))
        remainder = sum_result % 11
        first_digit = 0 if remainder < 2 else 11 - remainder
        
        if int(cpf[9]) != first_digit:
            return False
        
        # Calcular segundo dígito verificador
        sum_result = sum(int(cpf[i]) * (11 - i) for i in range(10))
        remainder = sum_result % 11
        second_digit = 0 if remainder < 2 else 11 - remainder
        
        return int(cpf[10]) == second_digit
    
    def _validate_cnpj(self, cnpj: str) -> bool:
        """Valida CNPJ usando algoritmo oficial."""
        if len(cnpj) != 14 or not cnpj.isdigit():
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
    
    def _validate_names(self) -> bool:
        """Valida nomes."""
        # Nome do sócio não pode ser apenas números
        if self.nome_socio and self.nome_socio.strip().isdigit():
            self._validation_errors.append("Nome do sócio não pode ser apenas números")
            return False
        
        # Nome do representante não pode ser apenas números
        if self.nome_representante and self.nome_representante.strip().isdigit():
            self._validation_errors.append("Nome do representante não pode ser apenas números")
            return False
        
        return True
    
    def is_pessoa_fisica(self) -> bool:
        """Verifica se o sócio é pessoa física."""
        return self.identificador_socio in [1, 2] if self.identificador_socio else False
    
    def is_pessoa_juridica(self) -> bool:
        """Verifica se o sócio é pessoa jurídica."""
        return self.identificador_socio == 3 if self.identificador_socio else False
    
    def has_representante_legal(self) -> bool:
        """Verifica se tem representante legal."""
        return bool(self.representante_legal and self.nome_representante)
    
    def get_documento_formatado(self) -> str:
        """Retorna documento formatado (CPF ou CNPJ)."""
        if not self.cnpj_cpf_socio:
            return ""
        
        if len(self.cnpj_cpf_socio) == 11:  # CPF
            cpf = self.cnpj_cpf_socio
            return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}"
        elif len(self.cnpj_cpf_socio) == 14:  # CNPJ
            cnpj = self.cnpj_cpf_socio
            return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
        
        return self.cnpj_cpf_socio
    
    def get_representante_formatado(self) -> str:
        """Retorna CPF do representante formatado."""
        if not self.representante_legal or len(self.representante_legal) != 11:
            return ""
        
        cpf = self.representante_legal
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}"
    
    # Métodos de transformação
    
    def _transform_validate_cpf_cnpj(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: validar e corrigir CPF/CNPJ."""
        if 'cnpj_cpf_socio' in data and data['cnpj_cpf_socio']:
            documento = re.sub(r'[^\d]', '', str(data['cnpj_cpf_socio']))
            
            # Determinar se é CPF (11) ou CNPJ (14)
            if len(documento) <= 11:
                data['cnpj_cpf_socio'] = documento.zfill(11)
            else:
                data['cnpj_cpf_socio'] = documento.zfill(14)[:14]
        
        return data
    
    def _transform_normalize_names(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: normalizar nomes."""
        name_fields = ['nome_socio', 'nome_representante', 'pais']
        
        for field in name_fields:
            if field in data and data[field]:
                nome = str(data[field]).strip().upper()
                
                # Se é apenas números, remover
                if nome.isdigit():
                    data[field] = None
                else:
                    data[field] = nome
        
        return data
    
    def _transform_clean_representante_legal(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: limpar CPF do representante legal."""
        if 'representante_legal' in data and data['representante_legal']:
            cpf = re.sub(r'[^\d]', '', str(data['representante_legal']))
            data['representante_legal'] = cpf.zfill(11)[:11]
        
        return data

    def _validate_dates(self) -> bool:
        """Valida datas."""
        # REMOVIDO: Todas as validações de data foram removidas
        return True
