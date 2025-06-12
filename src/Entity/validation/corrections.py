"""
Sistema de correções automáticas para dados de entidades.

Este módulo contém as lógicas específicas para corrigir automaticamente
dados malformados de cada tipo de entidade.
"""

from typing import Dict, Any, List, Tuple, Optional
from pydantic import ValidationError
import re
import logging

logger = logging.getLogger(__name__)


class EntityCorrections:
    """Sistema de correções automáticas para entidades"""
    
    def __init__(self, entity_type: str):
        """
        Inicializa o sistema de correções.
        
        Args:
            entity_type: Tipo da entidade ('empresa', 'estabelecimento', etc.)
        """
        self.entity_type = entity_type.lower()
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{self.entity_type}")
    
    def correct_data(self, row: Dict[str, Any], 
                    error: ValidationError) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        """
        Corrige dados automaticamente baseado no tipo de entidade.
        
        Args:
            row: Dados originais da linha
            error: Erro de validação
            
        Returns:
            Tuple[Dict, List]: Dados corrigidos e lista de correções aplicadas
        """
        corrected = row.copy()
        corrections = []
        
        # Aplicar correções baseadas no tipo de entidade
        if self.entity_type == 'empresa':
            corrected, entity_corrections = self._correct_empresa_data(corrected)
            corrections.extend(entity_corrections)
            
        elif self.entity_type == 'estabelecimento':
            corrected, entity_corrections = self._correct_estabelecimento_data(corrected)
            corrections.extend(entity_corrections)
            
        elif self.entity_type == 'socio':
            corrected, entity_corrections = self._correct_socio_data(corrected)
            corrections.extend(entity_corrections)
            
        elif self.entity_type == 'simples':
            corrected, entity_corrections = self._correct_simples_data(corrected)
            corrections.extend(entity_corrections)
        
        # Aplicar correções gerais
        corrected, general_corrections = self._apply_general_corrections(corrected)
        corrections.extend(general_corrections)
        
        return corrected, corrections
    
    def _correct_empresa_data(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Correções específicas para dados de empresa."""
        corrections = []
        
        # Corrigir CNPJ básico
        if 'cnpj_basico' in data and data['cnpj_basico']:
            original = data['cnpj_basico']
            corrected = re.sub(r'[^\d]', '', str(original))
            if len(corrected) < 8:
                corrected = corrected.zfill(8)
            elif len(corrected) > 8:
                corrected = corrected[:8]
            
            if corrected != str(original):
                data['cnpj_basico'] = corrected
                corrections.append(f"CNPJ básico corrigido: {original} -> {corrected}")
        
        # Corrigir razão social
        if 'razao_social' in data and data['razao_social']:
            original = data['razao_social']
            corrected = str(original).strip().upper()
            
            if corrected != original:
                data['razao_social'] = corrected
                corrections.append("Razão social normalizada")
        
        # Corrigir capital social
        if 'capital_social' in data and data['capital_social'] is not None:
            try:
                original = data['capital_social']
                if isinstance(original, str):
                    # Remover caracteres não numéricos exceto ponto e vírgula
                    cleaned = re.sub(r'[^\d.,]', '', original)
                    # Converter vírgula para ponto
                    cleaned = cleaned.replace(',', '.')
                    corrected = float(cleaned)
                    
                    data['capital_social'] = corrected
                    corrections.append(f"Capital social convertido: {original} -> {corrected}")
            except (ValueError, TypeError):
                data['capital_social'] = None
                corrections.append("Capital social inválido removido")
        
        return data, corrections
    
    def _correct_estabelecimento_data(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Correções específicas para dados de estabelecimento."""
        corrections = []
        
        # Corrigir partes do CNPJ
        for field, expected_length in [('cnpj_basico', 8), ('cnpj_ordem', 4), ('cnpj_dv', 2)]:
            if field in data and data[field]:
                original = data[field]
                corrected = re.sub(r'[^\d]', '', str(original))
                corrected = corrected.zfill(expected_length)
                
                if len(corrected) > expected_length:
                    corrected = corrected[:expected_length]
                
                if corrected != str(original):
                    data[field] = corrected
                    corrections.append(f"{field} corrigido: {original} -> {corrected}")
        
        # Corrigir CEP
        if 'cep' in data and data['cep']:
            original = data['cep']
            corrected = re.sub(r'[^\d]', '', str(original))
            corrected = corrected.zfill(8)
            
            if len(corrected) > 8:
                corrected = corrected[:8]
            
            if corrected != str(original):
                data['cep'] = corrected
                corrections.append(f"CEP corrigido: {original} -> {corrected}")
        
        # Corrigir UF
        if 'uf' in data and data['uf']:
            original = data['uf']
            corrected = str(original).strip().upper()
            
            if len(corrected) > 2:
                corrected = corrected[:2]
            
            if corrected != original:
                data['uf'] = corrected
                corrections.append(f"UF corrigida: {original} -> {corrected}")
        
        # Corrigir nome fantasia
        if 'nome_fantasia' in data and data['nome_fantasia']:
            original = data['nome_fantasia']
            corrected = str(original).strip().upper()
            
            if corrected != original:
                data['nome_fantasia'] = corrected
                corrections.append("Nome fantasia normalizado")
        
        return data, corrections
    
    def _correct_socio_data(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Correções específicas para dados de sócio."""
        corrections = []
        
        # Corrigir CNPJ básico
        if 'cnpj_basico' in data and data['cnpj_basico']:
            original = data['cnpj_basico']
            corrected = re.sub(r'[^\d]', '', str(original))
            corrected = corrected.zfill(8)
            
            if len(corrected) > 8:
                corrected = corrected[:8]
            
            if corrected != str(original):
                data['cnpj_basico'] = corrected
                corrections.append(f"CNPJ básico corrigido: {original} -> {corrected}")
        
        # Corrigir CPF/CNPJ do sócio
        if 'cnpj_cpf_socio' in data and data['cnpj_cpf_socio']:
            original = data['cnpj_cpf_socio']
            corrected = re.sub(r'[^\d]', '', str(original))
            
            # Determinar se é CPF (11) ou CNPJ (14)
            if len(corrected) <= 11:
                corrected = corrected.zfill(11)
            else:
                corrected = corrected.zfill(14)
                if len(corrected) > 14:
                    corrected = corrected[:14]
            
            if corrected != str(original):
                data['cnpj_cpf_socio'] = corrected
                corrections.append(f"CPF/CNPJ do sócio corrigido: {original} -> {corrected}")
        
        # Corrigir nomes
        for field in ['nome_socio', 'nome_representante']:
            if field in data and data[field]:
                original = data[field]
                corrected = str(original).strip().upper()
                
                if corrected != original:
                    data[field] = corrected
                    corrections.append(f"{field} normalizado")
        
        # Corrigir representante legal (CPF)
        if 'representante_legal' in data and data['representante_legal']:
            original = data['representante_legal']
            corrected = re.sub(r'[^\d]', '', str(original))
            corrected = corrected.zfill(11)
            
            if len(corrected) > 11:
                corrected = corrected[:11]
            
            if corrected != str(original):
                data['representante_legal'] = corrected
                corrections.append(f"CPF representante legal corrigido: {original} -> {corrected}")
        
        return data, corrections
    
    def _correct_simples_data(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Correções específicas para dados do Simples Nacional."""
        corrections = []
        
        # Corrigir CNPJ básico
        if 'cnpj_basico' in data and data['cnpj_basico']:
            original = data['cnpj_basico']
            corrected = re.sub(r'[^\d]', '', str(original))
            corrected = corrected.zfill(8)
            
            if len(corrected) > 8:
                corrected = corrected[:8]
            
            if corrected != str(original):
                data['cnpj_basico'] = corrected
                corrections.append(f"CNPJ básico corrigido: {original} -> {corrected}")
        
        # Corrigir opções S/N
        for field in ['opcao_simples', 'opcao_mei']:
            if field in data and data[field]:
                original = data[field]
                corrected = str(original).strip().upper()
                
                if corrected not in ['S', 'N']:
                    # Tentar interpretar valores comuns
                    if corrected in ['SIM', 'YES', '1', 'TRUE', 'VERDADEIRO']:
                        corrected = 'S'
                    elif corrected in ['NAO', 'NÃO', 'NO', '0', 'FALSE', 'FALSO']:
                        corrected = 'N'
                    else:
                        corrected = None
                
                if corrected != original:
                    data[field] = corrected
                    corrections.append(f"{field} corrigido: {original} -> {corrected}")
        
        return data, corrections
    
    def _apply_general_corrections(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Aplica correções gerais para qualquer tipo de entidade."""
        messages = []
        
        # Corrigir CNPJ básico
        if 'cnpj_basico' in data and data['cnpj_basico'] is not None:
            try:
                cnpj = int(data['cnpj_basico'])
                if not (10000000 <= cnpj <= 99999999):
                    data['cnpj_basico'] = None
                    messages.append("CNPJ básico inválido - removido")
            except (ValueError, TypeError):
                data['cnpj_basico'] = None
                messages.append("CNPJ básico inválido - removido")
        
        # Corrigir strings vazias
        for key, value in data.items():
            if isinstance(value, str) and not value.strip():
                data[key] = None
                messages.append(f"Campo {key} vazio - removido")
        
        return data, messages


class ValidationUtils:
    """Utilitários para validação de dados específicos"""
    
    @staticmethod
    def validate_cnpj(cnpj: str) -> bool:
        """
        Valida CNPJ usando algoritmo oficial.
        
        Args:
            cnpj: CNPJ para validar
            
        Returns:
            bool: True se válido
        """
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
    
    @staticmethod
    def validate_cpf(cpf: str) -> bool:
        """
        Valida CPF usando algoritmo oficial.
        
        Args:
            cpf: CPF para validar
            
        Returns:
            bool: True se válido
        """
        if len(cpf) != 11:
            return False
        
        # CPFs inválidos conhecidos
        invalid_cpfs = [
            "00000000000", "11111111111", "22222222222", "33333333333",
            "44444444444", "55555555555", "66666666666", "77777777777",
            "88888888888", "99999999999"
        ]
        
        if cpf in invalid_cpfs:
            return False
        
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
    
    @staticmethod
    def validate_uf(uf: str) -> bool:
        """
        Valida UF brasileira.
        
        Args:
            uf: UF para validar
            
        Returns:
            bool: True se válida
        """
        ufs_validas = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        
        return uf.upper() in ufs_validas
    
    @staticmethod
    def validate_cep(cep: str) -> bool:
        """
        Valida CEP brasileiro.
        
        Args:
            cep: CEP para validar
            
        Returns:
            bool: True se válido
        """
        if len(cep) != 8:
            return False
        
        if not cep.isdigit():
            return False
        
        # CEPs obviamente inválidos
        if cep in ['00000000', '99999999']:
            return False
        
        return True 