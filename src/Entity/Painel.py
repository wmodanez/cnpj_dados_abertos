"""
Entidade Painel para dados combinados da Receita Federal.

Esta classe representa um painel com dados combinados obtidos através de relacionamentos específicos:
- LEFT JOIN entre Estabelecimento e Simples Nacional (por cnpj_basico)
- INNER JOIN com Empresa (por cnpj_basico) 
- LEFT JOIN com Município (por codigo_municipio)
- LEFT JOIN com dados auxiliares (CNAE, Natureza Jurídica, etc.)

Implementa validações e transformações específicas para dados consolidados de exportação.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Type
import polars as pl
import re
import logging
from .base import BaseEntity

try:
    from .schemas.painel import PainelSchema
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class Painel(BaseEntity):
    """
    Entidade representando um Painel com dados combinados da Receita Federal.
    
    Combina informações através dos seguintes relacionamentos:
    - LEFT JOIN: Estabelecimento + Simples Nacional (por cnpj_basico)
    - INNER JOIN: + Empresa (por cnpj_basico)
    - LEFT JOIN: + Município (por codigo_municipio)
    - LEFT JOIN: + dados auxiliares (CNAE, Natureza Jurídica, Qualificação Sócio, etc.)
    
    Attributes:
        # Dados do Estabelecimento (base)
        cnpj_basico: CNPJ básico (bigint de 8 dígitos)
        matriz_filial: 1=Matriz, 2=Filial
        codigo_situacao: Código da situação cadastral
        data_situacao_cadastral: Data da situação cadastral
        codigo_motivo: Motivo da situação
        data_inicio_atividades: Data de início das atividades
        codigo_cnae: Código CNAE principal
        codigo_municipio: Código do município (Receita Federal)
        tipo_situacao_cadastral: Tipo de situação cadastral (1=Ativa, 2=Baixa Voluntária, 3=Outras Baixas)
        
        # Dados do Município (left join por codigo_municipio)
        codigo_ibge: Código IBGE do município (7 dígitos)
        nome_municipio: Nome do município
        uf: Unidade Federativa (estado)
        sigla_uf: Sigla da UF (ex: SP, GO, MG)
        
        # Dados da Empresa (inner join por cnpj_basico)
        natureza_juridica: Código da natureza jurídica
        porte_empresa: Porte da empresa (1-5)
        
        # Dados do Simples Nacional (left join - podem ser null se não optante)
        opcao_simples: Opção pelo Simples Nacional (S/N)
        data_opcao_simples: Data da opção pelo Simples
        data_exclusao_simples: Data de exclusão do Simples
        opcao_mei: Opção pelo MEI (S/N)
        data_opcao_mei: Data da opção pelo MEI
        data_exclusao_mei: Data de exclusão do MEI
    """
    
    # Dados do Estabelecimento (obrigatórios)
    cnpj_basico: int
    
    # Dados do Estabelecimento (opcionais)
    matriz_filial: Optional[int] = None
    codigo_situacao: Optional[int] = None
    data_situacao_cadastral: Optional[str] = None
    codigo_motivo: Optional[int] = None
    data_inicio_atividades: Optional[str] = None
    codigo_cnae: Optional[int] = None
    codigo_municipio: Optional[int] = None
    tipo_situacao_cadastral: Optional[int] = None
    
    # Dados do Município (left join por codigo_municipio)
    codigo_ibge: Optional[int] = None
    nome_municipio: Optional[str] = None
    uf: Optional[str] = None
    sigla_uf: Optional[str] = None
    
    # Dados da Empresa (inner join)
    natureza_juridica: Optional[int] = None
    porte_empresa: Optional[int] = None
    
    # Dados do Simples Nacional (left join - podem não existir)
    opcao_simples: Optional[str] = None
    data_opcao_simples: Optional[str] = None
    data_exclusao_simples: Optional[str] = None
    opcao_mei: Optional[str] = None
    data_opcao_mei: Optional[str] = None
    data_exclusao_mei: Optional[str] = None
    
    def __post_init__(self):
        """Executa processamento após inicialização."""
        super().__post_init__()
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Retorna nomes das colunas da entidade."""
        return [
            # Dados do Estabelecimento
            'cnpj_basico', 'matriz_filial', 'codigo_situacao', 'data_situacao_cadastral', 
            'codigo_motivo', 'data_inicio_atividades', 'codigo_cnae', 'codigo_municipio',
            'tipo_situacao_cadastral',
            
            # Dados do Município
            'codigo_ibge', 'nome_municipio', 'uf', 'sigla_uf',
            
            # Dados da Empresa
            'natureza_juridica', 'porte_empresa',
            
            # Dados do Simples Nacional
            'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
            'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
        ]
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna tipos das colunas da entidade."""
        return {
            # Dados do Estabelecimento
            'cnpj_basico': pl.Int64,
            'matriz_filial': pl.Int32,
            'codigo_situacao': pl.Int32,
            'data_situacao_cadastral': pl.Utf8,
            'codigo_motivo': pl.Int32,
            'data_inicio_atividades': pl.Utf8,
            'codigo_cnae': pl.Int32,
            'codigo_municipio': pl.Int32,
            'tipo_situacao_cadastral': pl.Int32,
            
            # Dados do Município
            'codigo_ibge': pl.Int32,
            'nome_municipio': pl.Utf8,
            'uf': pl.Utf8,
            'sigla_uf': pl.Utf8,
            
            # Dados da Empresa
            'natureza_juridica': pl.Int32,
            'porte_empresa': pl.Int32,
            
            # Dados do Simples Nacional
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
            'normalize_cnpj_basico',
            'calculate_simples_status',
            'normalize_empresa_fields'
        ]
    
    def validate(self) -> bool:
        """
        Valida dados do painel.
        
        Returns:
            bool: True se válido, False caso contrário
        """
        self._validation_errors.clear()
        
        # Validar CNPJ básico (obrigatório)
        if not self._validate_cnpj_basico():
            return False
        
        # Validar dados do Simples Nacional
        if not self._validate_simples_data():
            return False
        
        # Validar dados da Empresa
        if not self._validate_empresa_data():
            return False
        
        # Validar consistência entre dados
        if not self._validate_data_consistency():
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
    
    def _validate_simples_data(self) -> bool:
        """Valida dados do Simples Nacional."""
        # Validar opções S/N
        if self.opcao_simples and self.opcao_simples not in ['S', 'N']:
            self._validation_errors.append(f"Opção Simples deve ser 'S' ou 'N': {self.opcao_simples}")
            return False
        
        if self.opcao_mei and self.opcao_mei not in ['S', 'N']:
            self._validation_errors.append(f"Opção MEI deve ser 'S' ou 'N': {self.opcao_mei}")
            return False
        
        # REMOVIDO: Todas as validações de data foram removidas
        return True
    
    def _validate_empresa_data(self) -> bool:
        """Valida dados da Empresa."""
        # Validar natureza jurídica
        if self.natureza_juridica is not None and not (1 <= self.natureza_juridica <= 9999):
            self._validation_errors.append("Natureza jurídica deve estar entre 1 e 9999")
            return False
        
        # Validar porte da empresa
        if self.porte_empresa is not None and not (1 <= self.porte_empresa <= 5):
            self._validation_errors.append("Porte da empresa deve estar entre 1 e 5")
            return False
        
        return True
    
    def _validate_data_consistency(self) -> bool:
        """Valida consistência entre dados de estabelecimento e Simples."""
        # REMOVIDO: Todas as validações de data foram removidas
        return True
    
    def _calculate_derived_fields(self):
        """Calcula campos derivados (removido - não há mais campos calculados)."""
        pass
    
    def _get_situacao_simples(self) -> str:
        """Calcula situação atual no Simples Nacional."""
        if not self.opcao_simples or self.opcao_simples == 'N':
            return "Não optante"
        
        if self.opcao_simples == 'S':
            if not self.data_exclusao_simples:
                return "Ativo"
            else:
                return "Excluído"
        
        return "Indefinido"
    
    def _get_situacao_mei(self) -> str:
        """Calcula situação atual no MEI."""
        if not self.opcao_mei or self.opcao_mei == 'N':
            return "Não optante"
        
        if self.opcao_mei == 'S':
            if not self.data_exclusao_mei:
                return "Ativo"
            else:
                return "Excluído"
        
        return "Indefinido"
    
    # Métodos de conveniência
    
    def is_matriz(self) -> bool:
        """Verifica se é matriz."""
        return self.matriz_filial == 1 if self.matriz_filial is not None else False
    
    def is_filial(self) -> bool:
        """Verifica se é filial."""
        return self.matriz_filial == 2 if self.matriz_filial is not None else False
    
    def is_ativo_estabelecimento(self) -> bool:
        """Verifica se estabelecimento está ativo."""
        return self.codigo_situacao == 2 if self.codigo_situacao is not None else False
    
    def is_optante_simples(self) -> bool:
        """Verifica se é optante do Simples Nacional."""
        return self.opcao_simples == 'S' if self.opcao_simples is not None else False
    
    def is_optante_mei(self) -> bool:
        """Verifica se é optante do MEI."""
        return self.opcao_mei == 'S' if self.opcao_mei is not None else False
    
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
    
    def is_microempresa(self) -> bool:
        """Verifica se é microempresa."""
        return self.porte_empresa == 1 if self.porte_empresa is not None else False
    
    def is_pequena_empresa(self) -> bool:
        """Verifica se é pequena empresa."""
        return self.porte_empresa == 2 if self.porte_empresa is not None else False
    
    def get_situacao_cadastral_descricao(self) -> str:
        """Retorna descrição da situação cadastral."""
        if self.codigo_situacao == 1:
            return "Nula"
        elif self.codigo_situacao == 2:
            return "Ativa"
        elif self.codigo_situacao == 3:
            return "Suspensa"
        elif self.codigo_situacao == 4:
            return "Inapta"
        elif self.codigo_situacao == 8:
            return "Baixada"
        else:
            return "Outros"
    
    def get_tipo_situacao_descricao(self) -> str:
        """Retorna descrição do tipo de situação cadastral."""
        if self.tipo_situacao_cadastral == 1:
            return "Ativa"
        elif self.tipo_situacao_cadastral == 2:
            return "Baixa Voluntária"
        elif self.tipo_situacao_cadastral == 3:
            return "Outras Baixas"
        else:
            return "Indefinido"
    
    def get_resumo_status(self) -> Dict[str, Any]:
        """Retorna resumo do status do estabelecimento."""
        return {
            'cnpj_basico': self.cnpj_basico,
            'tipo_estabelecimento': 'Matriz' if self.is_matriz() else 'Filial' if self.is_filial() else 'Indefinido',
            'estabelecimento_ativo': self.is_ativo_estabelecimento(),
            'situacao_simples': self._get_situacao_simples(),
            'situacao_mei': self._get_situacao_mei(),
            'natureza_juridica': self.natureza_juridica,
            'porte_empresa': self.porte_empresa,
            'is_microempresa': self.is_microempresa(),
            'is_pequena_empresa': self.is_pequena_empresa()
        }
    
    # Métodos de transformação
    
    def _transform_normalize_cnpj_basico(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: normalizar CNPJ básico."""
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
    
    def _transform_calculate_simples_status(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: calcular status do Simples e MEI."""
        # Esta transformação será feita no __post_init__
        return data
    
    def _transform_normalize_empresa_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transformação: normalizar campos da empresa."""
        # Validar natureza jurídica
        if 'natureza_juridica' in data and data['natureza_juridica'] is not None:
            try:
                nj = int(data['natureza_juridica'])
                if 1 <= nj <= 9999:
                    data['natureza_juridica'] = nj
                else:
                    data['natureza_juridica'] = None
            except (ValueError, TypeError):
                data['natureza_juridica'] = None
        
        # Validar porte da empresa
        if 'porte_empresa' in data and data['porte_empresa'] is not None:
            try:
                pe = int(data['porte_empresa'])
                if 1 <= pe <= 5:
                    data['porte_empresa'] = pe
                else:
                    data['porte_empresa'] = None
            except (ValueError, TypeError):
                data['porte_empresa'] = None
        
        return data


if PYDANTIC_AVAILABLE:
    # Schema Pydantic já definido em schemas/painel.py
    pass
