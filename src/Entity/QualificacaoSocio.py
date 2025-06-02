#!/usr/bin/env python3
"""
Entidade para representar Qualificação do Sócio
Dados da Receita Federal convertidos de CSV para Parquet
"""

from typing import Dict, List, Optional, Any
from .base import BaseEntity

class QualificacaoSocio(BaseEntity):
    """
    Entidade para representar a Qualificação do Sócio.
    
    Atributos:
        codigo (int): Código da qualificação do sócio (0-79)
        descricao (str): Descrição da qualificação do sócio
    """
    
    def __init__(self, codigo: int, descricao: str = ""):
        """
        Inicializa uma QualificacaoSocio.
        
        Args:
            codigo: Código da qualificação do sócio
            descricao: Descrição da qualificação do sócio
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
        Valida os dados da qualificação do sócio.
        
        Returns:
            True se válida, False caso contrário
        """
        self.validation_errors = []
        
        # Validar código
        if not isinstance(self.codigo, int):
            self.validation_errors.append("Código deve ser um número inteiro")
        elif self.codigo < 0:
            self.validation_errors.append("Código não pode ser negativo")
        elif self.codigo > 99:
            self.validation_errors.append("Código deve ser menor que 100")
        
        # Validar descrição
        if not isinstance(self.descricao, str):
            self.validation_errors.append("Descrição deve ser uma string")
        elif len(self.descricao.strip()) == 0 and self.codigo != 0:
            self.validation_errors.append("Descrição não pode estar vazia para códigos diferentes de 0")
        
        return len(self.validation_errors) == 0
    
    def is_nao_informada(self) -> bool:
        """Verifica se a qualificação não foi informada."""
        return self.codigo == 0
    
    def is_socio(self) -> bool:
        """Verifica se é um tipo de sócio."""
        descricao_lower = self.descricao.lower()
        return 'sócio' in descricao_lower or 'socio' in descricao_lower
    
    def is_administrador(self) -> bool:
        """Verifica se é um cargo administrativo."""
        descricao_lower = self.descricao.lower()
        return any(termo in descricao_lower for termo in [
            'administrador', 'diretor', 'presidente', 'gerente', 'superintendente'
        ])
    
    def is_representante_legal(self) -> bool:
        """Verifica se é um representante legal."""
        descricao_lower = self.descricao.lower()
        return any(termo in descricao_lower for termo in [
            'representante', 'procurador', 'curador', 'tutor', 'inventariante'
        ])
    
    def is_familiar(self) -> bool:
        """Verifica se é relação familiar."""
        descricao_lower = self.descricao.lower()
        return any(termo in descricao_lower for termo in [
            'mãe', 'pai', 'filho', 'filha', 'cônjuge', 'companheiro'
        ])
    
    def is_conselheiro(self) -> bool:
        """Verifica se é membro de conselho."""
        descricao_lower = self.descricao.lower()
        return 'conselheiro' in descricao_lower
    
    def is_relacionado_exterior(self) -> bool:
        """Verifica se está relacionado ao exterior."""
        descricao_lower = self.descricao.lower()
        return 'exterior' in descricao_lower
    
    def is_pessoa_fisica(self) -> Optional[bool]:
        """
        Verifica se é pessoa física baseado na descrição.
        
        Returns:
            True se pessoa física, False se jurídica, None se indefinido
        """
        descricao_lower = self.descricao.lower()
        if 'pessoa física' in descricao_lower:
            return True
        elif 'pessoa jurídica' in descricao_lower:
            return False
        else:
            return None
    
    def is_pessoa_juridica(self) -> Optional[bool]:
        """
        Verifica se é pessoa jurídica baseado na descrição.
        
        Returns:
            True se pessoa jurídica, False se física, None se indefinido
        """
        resultado = self.is_pessoa_fisica()
        return None if resultado is None else not resultado
    
    def get_categoria(self) -> str:
        """
        Retorna a categoria da qualificação.
        
        Returns:
            Categoria da qualificação
        """
        if self.is_nao_informada():
            return "Não Informada"
        elif self.is_socio():
            return "Sócio"
        elif self.is_administrador():
            return "Administração"
        elif self.is_conselheiro():
            return "Conselho"
        elif self.is_representante_legal():
            return "Representação Legal"
        elif self.is_familiar():
            return "Familiar"
        elif self.is_relacionado_exterior():
            return "Exterior"
        else:
            return "Outros"
    
    def get_tipo_pessoa(self) -> str:
        """
        Retorna o tipo de pessoa (física ou jurídica).
        
        Returns:
            Tipo de pessoa
        """
        if self.is_pessoa_fisica():
            return "Pessoa Física"
        elif self.is_pessoa_juridica():
            return "Pessoa Jurídica"
        else:
            return "Indefinido"
    
    def get_nivel_hierarquico(self) -> str:
        """
        Retorna o nível hierárquico da qualificação.
        
        Returns:
            Nível hierárquico
        """
        descricao_lower = self.descricao.lower()
        
        if 'presidente' in descricao_lower:
            return "Alto"
        elif any(termo in descricao_lower for termo in ['diretor', 'superintendente']):
            return "Alto"
        elif any(termo in descricao_lower for termo in ['administrador', 'gerente']):
            return "Médio"
        elif 'conselheiro' in descricao_lower:
            return "Médio"
        elif self.is_socio():
            return "Proprietário"
        else:
            return "Operacional"
    
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
    
    def has_poder_decisao(self) -> bool:
        """Verifica se a qualificação implica em poder de decisão."""
        return self.is_administrador() or self.is_conselheiro() or (
            self.is_socio() and 'administrador' in self.descricao.lower()
        )
    
    def is_cargo_executivo(self) -> bool:
        """Verifica se é um cargo executivo."""
        descricao_lower = self.descricao.lower()
        return any(termo in descricao_lower for termo in [
            'presidente', 'diretor', 'superintendente', 'gerente'
        ])
    
    def get_abreviacao(self) -> str:
        """
        Retorna uma abreviação da qualificação.
        
        Returns:
            Abreviação da qualificação
        """
        descricao_lower = self.descricao.lower()
        
        if 'presidente' in descricao_lower:
            return "PRES"
        elif 'diretor' in descricao_lower:
            return "DIR"
        elif 'administrador' in descricao_lower:
            return "ADM"
        elif 'conselheiro' in descricao_lower:
            return "CONS"
        elif 'sócio' in descricao_lower or 'socio' in descricao_lower:
            return "SOC"
        elif 'procurador' in descricao_lower:
            return "PROC"
        else:
            return "OUT"
    
    def __str__(self) -> str:
        """Representação string da qualificação do sócio."""
        status = "✅" if self.is_valid() else "❌"
        categoria = self.get_categoria()
        tipo_pessoa = self.get_tipo_pessoa()
        
        descricao_truncada = self.descricao[:25] + "..." if len(self.descricao) > 25 else self.descricao
        
        return f"{status} Qualif. {self.codigo:2d}: {descricao_truncada} [{categoria}] [{tipo_pessoa}]"
    
    def __repr__(self) -> str:
        """Representação técnica da qualificação do sócio."""
        return f"QualificacaoSocio(codigo={self.codigo}, descricao='{self.descricao[:15]}...')"
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte a entidade para dicionário."""
        return {
            'codigo': self.codigo,
            'descricao': self.descricao,
            'categoria': self.get_categoria(),
            'tipo_pessoa': self.get_tipo_pessoa(),
            'nivel_hierarquico': self.get_nivel_hierarquico(),
            'abreviacao': self.get_abreviacao(),
            'is_socio': self.is_socio(),
            'is_administrador': self.is_administrador(),
            'has_poder_decisao': self.has_poder_decisao(),
            'validation_status': self.is_valid()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QualificacaoSocio':
        """
        Cria uma QualificacaoSocio a partir de um dicionário.
        
        Args:
            data: Dicionário com os dados
            
        Returns:
            Instância de QualificacaoSocio
        """
        return cls(
            codigo=data.get('codigo', 0),
            descricao=data.get('descricao', '')
        )
