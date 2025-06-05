"""
Entidade Motivo para representar motivos de situação cadastral.

Esta entidade gerencia dados de motivos de situação cadastral conforme disponibilizados
pela Receita Federal, incluindo validações específicas e métodos utilitários.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Type

try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

from .base import BaseEntity


@dataclass
class Motivo(BaseEntity):
    """
    Entidade para representar motivos de situação cadastral.
    
    Attributes:
        codigo: Código do motivo
        descricao: Descrição do motivo
    """
    
    # Campos obrigatórios
    codigo: int
    descricao: str
    
    # Campos calculados automaticamente
    _validation_errors_motivo: List[str] = field(default_factory=list, init=False)
    _is_valid: Optional[bool] = field(default=None, init=False)
    
    def __post_init__(self):
        """Executado após a inicialização para realizar validações e transformações."""
        super().__post_init__()
        self._validate_motivo()
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Retorna lista com os nomes das colunas da entidade."""
        return ['codigo', 'descricao']
    
    @classmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """Retorna dicionário com os tipos das colunas da entidade."""
        return {
            'codigo': int,
            'descricao': str
        }
    
    @classmethod
    def get_transformations(cls) -> List[str]:
        """Retorna lista com as transformações aplicáveis à entidade."""
        return [
            'normalize_description',
            'categorize_motivo',
            'format_description'
        ]
    
    def validate(self) -> bool:
        """Valida os dados da entidade."""
        self._validation_errors.clear()
        self._validation_errors_motivo = []
        
        # Validar código
        if not isinstance(self.codigo, int) or self.codigo < 0:
            error_msg = "Código do motivo deve ser um número inteiro não negativo"
            self._validation_errors.append(error_msg)
            self._validation_errors_motivo.append(error_msg)
        
        # Validar descrição
        if not self.descricao or not self.descricao.strip():
            error_msg = "Descrição do motivo é obrigatória"
            self._validation_errors.append(error_msg)
            self._validation_errors_motivo.append(error_msg)
        elif len(self.descricao.strip()) < 3:
            error_msg = "Descrição do motivo deve ter pelo menos 3 caracteres"
            self._validation_errors.append(error_msg)
            self._validation_errors_motivo.append(error_msg)
        
        self._is_valid = len(self._validation_errors) == 0
        return self._is_valid
    
    def _validate_motivo(self) -> None:
        """Valida os dados do motivo (método para compatibilidade)."""
        self.validate()
    
    def is_valid(self) -> bool:
        """Retorna True se o motivo é válido."""
        return self._is_valid if self._is_valid is not None else False
    
    def get_validation_errors(self) -> List[str]:
        """Retorna lista de erros de validação."""
        return self._validation_errors_motivo.copy()
    
    def is_sem_motivo(self) -> bool:
        """
        Verifica se é o motivo "SEM MOTIVO" (código 0).
        
        Returns:
            bool: True se for sem motivo
        """
        return self.codigo == 0
    
    def is_encerramento(self) -> bool:
        """
        Verifica se o motivo está relacionado a encerramento de atividades.
        
        Returns:
            bool: True se for motivo de encerramento
        """
        termos_encerramento = [
            'ENCERRAMENTO', 'EXTINÇÃO', 'LIQUIDAÇÃO', 'FALÊNCIA', 
            'DISSOLUÇÃO', 'BAIXA', 'CANCELAMENTO'
        ]
        descricao_upper = self.descricao.upper()
        return any(termo in descricao_upper for termo in termos_encerramento)
    
    def is_transformacao(self) -> bool:
        """
        Verifica se o motivo está relacionado a transformação empresarial.
        
        Returns:
            bool: True se for motivo de transformação
        """
        termos_transformacao = [
            'INCORPORAÇÃO', 'FUSÃO', 'CISÃO', 'TRANSFORMAÇÃO',
            'SUCESSÃO', 'CONVERSÃO'
        ]
        descricao_upper = self.descricao.upper()
        return any(termo in descricao_upper for termo in termos_transformacao)
    
    def is_suspensao(self) -> bool:
        """
        Verifica se o motivo está relacionado a suspensão de atividades.
        
        Returns:
            bool: True se for motivo de suspensão
        """
        termos_suspensao = [
            'SUSPENSÃO', 'SUSPENSA', 'PARALISAÇÃO', 'PARALIZADA',
            'INATIVA', 'INATIVIDADE'
        ]
        descricao_upper = self.descricao.upper()
        return any(termo in descricao_upper for termo in termos_suspensao)
    
    def get_categoria(self) -> str:
        """
        Retorna a categoria do motivo baseado na descrição.
        
        Returns:
            str: Categoria do motivo
        """
        if self.is_sem_motivo():
            return "Sem Motivo"
        elif self.is_encerramento():
            return "Encerramento"
        elif self.is_transformacao():
            return "Transformação"
        elif self.is_suspensao():
            return "Suspensão"
        else:
            return "Outros"
    
    def get_descricao_formatada(self) -> str:
        """
        Retorna a descrição formatada (primeira letra maiúscula, resto minúsculo).
        
        Returns:
            str: Descrição formatada
        """
        if not self.descricao:
            return ""
        
        return self.descricao.title()
    
    def get_descricao_resumida(self, max_length: int = 50) -> str:
        """
        Retorna uma versão resumida da descrição.
        
        Args:
            max_length: Tamanho máximo da descrição
            
        Returns:
            str: Descrição resumida
        """
        if not self.descricao:
            return ""
        
        if len(self.descricao) <= max_length:
            return self.descricao
        
        return self.descricao[:max_length-3] + "..."
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Motivo':
        """
        Cria uma instância de Motivo a partir de um dicionário.
        
        Args:
            data: Dicionário com os dados do motivo
            
        Returns:
            Motivo: Nova instância da entidade
        """
        return cls(
            codigo=data.get('codigo'),
            descricao=data.get('descricao', '')
        )
    
    @classmethod
    def from_polars_row(cls, row: Dict[str, Any]) -> 'Motivo':
        """
        Cria uma instância de Motivo a partir de uma linha do Polars.
        
        Args:
            row: Dicionário representando uma linha do DataFrame
            
        Returns:
            Motivo: Nova instância da entidade
        """
        return cls.from_dict(row)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converte a entidade para um dicionário.
        
        Returns:
            Dict: Dicionário com todos os campos da entidade
        """
        return {
            'codigo': self.codigo,
            'descricao': self.descricao,
            'categoria': self.get_categoria(),
            'descricao_formatada': self.get_descricao_formatada(),
            'is_valid': self.is_valid(),
            'validation_errors': self.get_validation_errors()
        }
    
    def __str__(self) -> str:
        """Representação em string da entidade."""
        status = "✅" if self.is_valid() else "❌"
        categoria = self.get_categoria()
        descricao_resumida = self.get_descricao_resumida(30)
        return f"{status} Motivo {self.codigo}: {descricao_resumida} [{categoria}]"
    
    def __repr__(self) -> str:
        """Representação técnica da entidade."""
        return f"Motivo(codigo={self.codigo}, descricao='{self.descricao[:30]}...', valid={self.is_valid()})"


if PYDANTIC_AVAILABLE:
    class MotivoSchema(BaseModel):
        """Schema Pydantic para validação da entidade Motivo."""
        
        codigo: int = Field(..., ge=0, description="Código do motivo")
        descricao: str = Field(..., min_length=3, max_length=200, description="Descrição do motivo")
        
        class Config:
            json_schema_extra = {
                "example": {
                    "codigo": 1,
                    "descricao": "EXTINÇÃO POR ENCERRAMENTO LIQUIDAÇÃO VOLUNTÁRIA"
                }
            }
        
        @validator('descricao')
        def validate_descricao(cls, v):
            if not v or not v.strip():
                raise ValueError('Descrição não pode estar vazia')
            return v.strip().upper()
