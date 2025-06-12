"""
Classe base para todas as entidades do sistema.

Este módulo define a interface comum que todas as entidades devem implementar,
incluindo métodos para validação, transformação e serialização de dados.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Type, Union
import polars as pl
from datetime import datetime
import logging
import json

logger = logging.getLogger(__name__)


@dataclass
class BaseEntity(ABC):
    """
    Classe base abstrata para todas as entidades do sistema.
    
    Define a interface comum que todas as entidades devem implementar,
    incluindo métodos para validação, transformação e serialização.
    """
    
    # Metadados da entidade (preenchidos automaticamente)
    _created_at: datetime = field(default_factory=datetime.now, init=False)
    _validated: bool = field(default=False, init=False)
    _validation_errors: List[str] = field(default_factory=list, init=False)
    
    def __post_init__(self):
        """Executado após a inicialização da entidade."""
        self._setup_logging()
        self._auto_validate()
    
    def _setup_logging(self):
        """Configura logging específico para a entidade."""
        self._logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
    
    def _auto_validate(self):
        """Executa validação automática após inicialização."""
        try:
            self._validated = self.validate()
            if not self._validated:
                self._logger.warning(f"Entidade {self.__class__.__name__} criada com dados inválidos")
        except Exception as e:
            self._validated = False
            self._validation_errors.append(f"Erro na validação automática: {str(e)}")
            self._logger.error(f"Erro na validação automática de {self.__class__.__name__}: {str(e)}")
    
    # Métodos abstratos que devem ser implementados pelas subclasses
    
    @classmethod
    @abstractmethod
    def get_column_names(cls) -> List[str]:
        """
        Retorna lista com os nomes das colunas da entidade.
        
        Returns:
            List[str]: Lista com nomes das colunas
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_column_types(cls) -> Dict[str, Type]:
        """
        Retorna dicionário com os tipos das colunas da entidade.
        
        Returns:
            Dict[str, Type]: Mapeamento coluna -> tipo
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_transformations(cls) -> List[str]:
        """
        Retorna lista com as transformações aplicáveis à entidade.
        
        Returns:
            List[str]: Lista com nomes das transformações
        """
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Valida os dados da entidade.
        
        Returns:
            bool: True se os dados são válidos, False caso contrário
        """
        pass
    
    # Métodos concretos com implementação padrão
    
    @classmethod
    def get_schema_class(cls):
        """
        Retorna a classe de schema Pydantic associada à entidade.
        
        Returns:
            Type[BaseModel]: Classe do schema Pydantic
        """
        # Importação dinâmica para evitar dependências circulares
        from .schemas import (
            EmpresaSchema, EstabelecimentoSchema, 
            SocioSchema, SimplesSchema
        )
        
        schema_mapping = {
            'Empresa': EmpresaSchema,
            'Estabelecimento': EstabelecimentoSchema,
            'Socio': SocioSchema,
            'Simples': SimplesSchema
        }
        
        class_name = cls.__name__
        if class_name in schema_mapping:
            return schema_mapping[class_name]
        
        raise NotImplementedError(f"Schema não encontrado para entidade {class_name}")
    
    @classmethod
    def from_dataframe_row(cls, row: Dict[str, Any]) -> 'BaseEntity':
        """
        Cria instância da entidade a partir de uma linha do DataFrame.
        
        Args:
            row: Dicionário com dados da linha
            
        Returns:
            BaseEntity: Instância da entidade
            
        Raises:
            ValueError: Se os dados são inválidos
        """
        try:
            # Filtrar apenas campos que existem na entidade
            entity_fields = cls.get_column_names()
            filtered_data = {k: v for k, v in row.items() if k in entity_fields}
            
            # Criar instância
            instance = cls(**filtered_data)
            
            if not instance.is_valid():
                logger.warning(f"Entidade {cls.__name__} criada com dados inválidos: {instance.get_validation_errors()}")
            
            return instance
            
        except Exception as e:
            logger.error(f"Erro ao criar {cls.__name__} a partir de linha: {str(e)}")
            raise ValueError(f"Não foi possível criar {cls.__name__}: {str(e)}")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseEntity':
        """
        Cria instância da entidade a partir de um dicionário.
        
        Args:
            data: Dicionário com dados da entidade
            
        Returns:
            BaseEntity: Instância da entidade
        """
        return cls.from_dataframe_row(data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'BaseEntity':
        """
        Cria instância da entidade a partir de uma string JSON.
        
        Args:
            json_str: String JSON com dados da entidade
            
        Returns:
            BaseEntity: Instância da entidade
        """
        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON inválido: {str(e)}")
    
    def to_dict(self, include_metadata: bool = False) -> Dict[str, Any]:
        """
        Converte entidade para dicionário.
        
        Args:
            include_metadata: Se deve incluir metadados (_created_at, etc.)
            
        Returns:
            Dict[str, Any]: Dicionário com dados da entidade
        """
        data = asdict(self)
        
        if not include_metadata:
            # Remover campos de metadados
            metadata_fields = ['_created_at', '_validated', '_validation_errors']
            for field in metadata_fields:
                data.pop(field, None)
        
        return data
    
    def to_json(self, include_metadata: bool = False, indent: int = None) -> str:
        """
        Converte entidade para string JSON.
        
        Args:
            include_metadata: Se deve incluir metadados
            indent: Indentação para formatação (None = compacto)
            
        Returns:
            str: String JSON
        """
        data = self.to_dict(include_metadata=include_metadata)
        
        return json.dumps(data, indent=indent, ensure_ascii=False)
    
    def is_valid(self) -> bool:
        """
        Verifica se a entidade é válida.
        
        Returns:
            bool: True se válida, False caso contrário
        """
        return self._validated
    
    def get_validation_errors(self) -> List[str]:
        """
        Retorna lista de erros de validação.
        
        Returns:
            List[str]: Lista com erros de validação
        """
        return self._validation_errors.copy()
    
    def revalidate(self) -> bool:
        """
        Executa nova validação da entidade.
        
        Returns:
            bool: True se válida após revalidação
        """
        self._validation_errors.clear()
        self._validated = self.validate()
        return self._validated
    
    def apply_transformations(self, transformations: Optional[List[str]] = None) -> 'BaseEntity':
        """
        Aplica transformações à entidade.
        
        Args:
            transformations: Lista de transformações a aplicar (None = todas)
            
        Returns:
            BaseEntity: Nova instância com transformações aplicadas
        """
        if transformations is None:
            transformations = self.get_transformations()
        
        # Criar cópia dos dados atuais
        current_data = self.to_dict()
        
        # Aplicar cada transformação
        for transformation in transformations:
            try:
                current_data = self._apply_single_transformation(current_data, transformation)
            except Exception as e:
                self._logger.warning(f"Erro ao aplicar transformação '{transformation}': {str(e)}")
        
        # Criar nova instância com dados transformados
        return self.__class__.from_dict(current_data)
    
    def _apply_single_transformation(self, data: Dict[str, Any], transformation: str) -> Dict[str, Any]:
        """
        Aplica uma única transformação aos dados.
        
        Args:
            data: Dados atuais
            transformation: Nome da transformação
            
        Returns:
            Dict[str, Any]: Dados transformados
        """
        # Implementação base - subclasses podem sobrescrever
        method_name = f"_transform_{transformation}"
        
        if hasattr(self, method_name):
            transform_method = getattr(self, method_name)
            return transform_method(data)
        else:
            self._logger.warning(f"Transformação '{transformation}' não implementada em {self.__class__.__name__}")
            return data
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Retorna resumo da entidade.
        
        Returns:
            Dict[str, Any]: Resumo com informações principais
        """
        return {
            'entity_type': self.__class__.__name__,
            'created_at': self._created_at.isoformat(),
            'is_valid': self._validated,
            'validation_errors_count': len(self._validation_errors),
            'field_count': len(self.get_column_names()),
            'available_transformations': self.get_transformations()
        }
    
    def __str__(self) -> str:
        """Representação string da entidade."""
        return f"{self.__class__.__name__}(valid={self._validated}, fields={len(self.get_column_names())})"
    
    def __repr__(self) -> str:
        """Representação detalhada da entidade."""
        return f"{self.__class__.__name__}({self.to_dict()})"
    
    def __eq__(self, other) -> bool:
        """Comparação de igualdade entre entidades."""
        if not isinstance(other, self.__class__):
            return False
        
        # Comparar apenas dados, não metadados
        self_data = self.to_dict(include_metadata=False)
        other_data = other.to_dict(include_metadata=False)
        
        return self_data == other_data
    
    def __hash__(self) -> int:
        """Hash da entidade baseado nos dados."""
        # Usar apenas campos imutáveis para hash
        data = self.to_dict(include_metadata=False)
        
        # Converter para tupla ordenada para garantir hash consistente
        items = tuple(sorted(data.items()))
        return hash(items)


class EntityFactory:
    """
    Factory para criação de entidades baseado no tipo.
    """
    
    _entity_registry: Dict[str, Type[BaseEntity]] = {}
    
    @classmethod
    def register_entity(cls, entity_type: str, entity_class: Type[BaseEntity]):
        """
        Registra uma classe de entidade.
        
        Args:
            entity_type: Nome do tipo da entidade
            entity_class: Classe da entidade
        """
        cls._entity_registry[entity_type.lower()] = entity_class
        logger.info(f"Entidade '{entity_type}' registrada: {entity_class.__name__}")
    
    @classmethod
    def create_entity(cls, entity_type: str, data: Dict[str, Any]) -> BaseEntity:
        """
        Cria entidade baseado no tipo.
        
        Args:
            entity_type: Tipo da entidade ('empresa', 'estabelecimento', etc.)
            data: Dados para criar a entidade
            
        Returns:
            BaseEntity: Instância da entidade
            
        Raises:
            ValueError: Se o tipo não está registrado
        """
        entity_type_lower = entity_type.lower()
        
        if entity_type_lower not in cls._entity_registry:
            available_types = list(cls._entity_registry.keys())
            raise ValueError(f"Tipo de entidade '{entity_type}' não registrado. Disponíveis: {available_types}")
        
        entity_class = cls._entity_registry[entity_type_lower]
        return entity_class.from_dict(data)
    
    @classmethod
    def get_registered_types(cls) -> List[str]:
        """
        Retorna lista de tipos de entidade registrados.
        
        Returns:
            List[str]: Lista com tipos registrados
        """
        return list(cls._entity_registry.keys())
    
    @classmethod
    def get_entity_class(cls, entity_type: str) -> Type[BaseEntity]:
        """
        Retorna classe da entidade baseado no tipo.
        
        Args:
            entity_type: Tipo da entidade
            
        Returns:
            Type[BaseEntity]: Classe da entidade
        """
        entity_type_lower = entity_type.lower()
        
        if entity_type_lower not in cls._entity_registry:
            raise ValueError(f"Tipo de entidade '{entity_type}' não registrado")
        
        return cls._entity_registry[entity_type_lower]


# Utilitários para trabalhar com entidades

def validate_entity_data(entity_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valida dados usando schema da entidade.
    
    Args:
        entity_type: Tipo da entidade
        data: Dados para validar
        
    Returns:
        Dict[str, Any]: Resultado da validação
    """
    try:
        entity_class = EntityFactory.get_entity_class(entity_type)
        schema_class = entity_class.get_schema_class()
        
        # Validar usando schema Pydantic
        validated_data = schema_class(**data)
        
        return {
            'valid': True,
            'data': validated_data.dict(),
            'errors': []
        }
        
    except Exception as e:
        return {
            'valid': False,
            'data': None,
            'errors': [str(e)]
        }


def create_entity_from_row(entity_type: str, row: Dict[str, Any]) -> BaseEntity:
    """
    Cria entidade a partir de linha de dados.
    
    Args:
        entity_type: Tipo da entidade
        row: Dados da linha
        
    Returns:
        BaseEntity: Instância da entidade
    """
    return EntityFactory.create_entity(entity_type, row)


def batch_create_entities(entity_type: str, rows: List[Dict[str, Any]]) -> List[BaseEntity]:
    """
    Cria múltiplas entidades em lote.
    
    Args:
        entity_type: Tipo da entidade
        rows: Lista com dados das linhas
        
    Returns:
        List[BaseEntity]: Lista com entidades criadas
    """
    entities = []
    
    for i, row in enumerate(rows):
        try:
            entity = EntityFactory.create_entity(entity_type, row)
            entities.append(entity)
        except Exception as e:
            logger.warning(f"Erro ao criar entidade {entity_type} na linha {i}: {str(e)}")
    
    return entities 