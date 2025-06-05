#!/usr/bin/env python3
"""
Entidade para representar Natureza Jurídica
Dados da Receita Federal convertidos de CSV para Parquet
"""

from typing import Dict, List, Optional, Any
from .base import BaseEntity

class NaturezaJuridica(BaseEntity):
    """
    Entidade para representar a Natureza Jurídica de uma empresa.
    
    Atributos:
        codigo (int): Código da natureza jurídica (0-8885)
        descricao (str): Descrição da natureza jurídica
    """
    
    def __init__(self, codigo: int, descricao: str = ""):
        """
        Inicializa uma NaturezaJuridica.
        
        Args:
            codigo: Código da natureza jurídica
            descricao: Descrição da natureza jurídica
        """
        self.codigo = codigo
        self.descricao = descricao.strip() if descricao else ""
        
        super().__init__()
    
    def get_column_names(self) -> List[str]:
        """Retorna os nomes das colunas da entidade."""
        return ['codigo', 'descricao']
    
    def get_column_types(self) -> Dict[str, type]:
        """Retorna os tipos das colunas da entidade."""
        return {
            'codigo': int,
            'descricao': str
        }
    
    def get_transformations(self) -> Dict[str, Any]:
        """Retorna as transformações aplicáveis à entidade."""
        return {
            'codigo': lambda x: int(x) if x is not None else 0,
            'descricao': lambda x: str(x).strip() if x is not None else ""
        }
    
    def validate(self) -> bool:
        """
        Valida os dados da natureza jurídica.
        
        Returns:
            True se válida, False caso contrário
        """
        self.validation_errors = []
        
        # Validar código
        if not isinstance(self.codigo, int):
            self.validation_errors.append("Código deve ser um número inteiro")
        elif self.codigo < 0:
            self.validation_errors.append("Código não pode ser negativo")
        elif self.codigo > 9999:
            self.validation_errors.append("Código deve ser menor que 10000")
        
        # Validar descrição
        if not isinstance(self.descricao, str):
            self.validation_errors.append("Descrição deve ser uma string")
        elif len(self.descricao.strip()) == 0 and self.codigo != 0:
            self.validation_errors.append("Descrição não pode estar vazia para códigos diferentes de 0")
        
        return len(self.validation_errors) == 0
    
    def is_empresa_privada(self) -> bool:
        """Verifica se é uma empresa privada (códigos 2000-2999)."""
        return 2000 <= self.codigo < 3000
    
    def is_entidade_publica(self) -> bool:
        """Verifica se é uma entidade pública (códigos 1000-1999)."""
        return 1000 <= self.codigo < 2000
    
    def is_associacao_fundacao(self) -> bool:
        """Verifica se é associação ou fundação (códigos 3000-3999)."""
        return 3000 <= self.codigo < 4000
    
    def is_outros(self) -> bool:
        """Verifica se é outros tipos (códigos 4000+)."""
        return self.codigo >= 4000
    
    def is_nao_informada(self) -> bool:
        """Verifica se a natureza jurídica não foi informada."""
        return self.codigo == 0
    
    def is_sociedade_limitada(self) -> bool:
        """Verifica se é sociedade limitada."""
        return self.codigo in [2062, 2305]  # Ltda e EIRELI
    
    def is_sociedade_anonima(self) -> bool:
        """Verifica se é sociedade anônima."""
        return self.codigo in [2054, 2046]  # SA Fechada e Aberta
    
    def is_empresario_individual(self) -> bool:
        """Verifica se é empresário individual."""
        return self.codigo == 2135
    
    def is_mei(self) -> bool:
        """Verifica se é MEI (Microempreendedor Individual)."""
        return self.codigo == 2135  # MEI usa mesmo código do EI
    
    def get_categoria(self) -> str:
        """
        Retorna a categoria da natureza jurídica.
        
        Returns:
            Categoria da natureza jurídica
        """
        if self.is_nao_informada():
            return "Não Informada"
        elif self.is_entidade_publica():
            return "Entidade Pública"
        elif self.is_empresa_privada():
            return "Empresa Privada"
        elif self.is_associacao_fundacao():
            return "Associação/Fundação"
        elif self.is_outros():
            return "Outros"
        else:
            return "Indefinida"
    
    def get_tipo_societario(self) -> str:
        """
        Retorna o tipo societário simplificado.
        
        Returns:
            Tipo societário
        """
        if self.is_sociedade_limitada():
            if self.codigo == 2305:
                return "EIRELI"
            else:
                return "LTDA"
        elif self.is_sociedade_anonima():
            return "S/A"
        elif self.is_empresario_individual():
            return "EI/MEI"
        elif self.codigo == 1244:
            return "Município"
        else:
            return "Outros"
    
    def get_descricao_formatada(self) -> str:
        """
        Retorna a descrição formatada (capitalizada).
        
        Returns:
            Descrição formatada
        """
        if not self.descricao:
            return ""
        
        # Palavras que devem permanecer minúsculas
        minusculas = ['e', 'de', 'da', 'do', 'das', 'dos', 'em', 'na', 'no', 'nas', 'nos', 'a', 'o', 'as', 'os']
        
        palavras = self.descricao.lower().split()
        resultado = []
        
        for i, palavra in enumerate(palavras):
            if i == 0 or palavra not in minusculas:
                resultado.append(palavra.capitalize())
            else:
                resultado.append(palavra)
        
        return ' '.join(resultado)
    
    def is_porte_empresarial(self) -> bool:
        """Verifica se é relacionada a porte empresarial (ME, EPP, etc)."""
        descricao_lower = self.descricao.lower()
        return any(termo in descricao_lower for termo in [
            'microempresa', 'pequeno porte', 'eireli', 'mei', 'individual'
        ])
    
    def __str__(self) -> str:
        """Representação string da natureza jurídica."""
        status = "✅" if self.is_valid() else "❌"
        categoria = self.get_categoria()
        tipo = self.get_tipo_societario()
        
        descricao_truncada = self.descricao[:30] + "..." if len(self.descricao) > 30 else self.descricao
        
        return f"{status} Natureza {self.codigo:4d}: {descricao_truncada} [{categoria}] [{tipo}]"
    
    def __repr__(self) -> str:
        """Representação técnica da natureza jurídica."""
        return f"NaturezaJuridica(codigo={self.codigo}, descricao='{self.descricao[:20]}...')"
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte a entidade para dicionário."""
        return {
            'codigo': self.codigo,
            'descricao': self.descricao,
            'categoria': self.get_categoria(),
            'tipo_societario': self.get_tipo_societario(),
            'is_empresa_privada': self.is_empresa_privada(),
            'is_entidade_publica': self.is_entidade_publica(),
            'validation_status': self.is_valid()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NaturezaJuridica':
        """
        Cria uma NaturezaJuridica a partir de um dicionário.
        
        Args:
            data: Dicionário com os dados
            
        Returns:
            Instância de NaturezaJuridica
        """
        return cls(
            codigo=data.get('codigo', 0),
            descricao=data.get('descricao', '')
        )
