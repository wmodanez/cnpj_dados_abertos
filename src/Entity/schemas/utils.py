"""
Utilitários para schemas de validação.
"""

from typing import Dict, Any
from pydantic import BaseModel
from .empresa import EmpresaSchema
from .estabelecimento import EstabelecimentoSchema
from .socio import SocioSchema
from .simples import SimplesSchema


def get_schema_for_entity(entity_type: str) -> BaseModel:
    """
    Retorna o schema apropriado para um tipo de entidade.
    
    Args:
        entity_type: Tipo da entidade ('empresa', 'estabelecimento', etc.)
        
    Returns:
        BaseModel: Classe do schema
        
    Raises:
        ValueError: Se o tipo não é reconhecido
    """
    schema_mapping = {
        'empresa': EmpresaSchema,
        'estabelecimento': EstabelecimentoSchema,
        'socio': SocioSchema,
        'simples': SimplesSchema
    }
    
    entity_type_lower = entity_type.lower()
    if entity_type_lower not in schema_mapping:
        available_types = list(schema_mapping.keys())
        raise ValueError(f"Tipo de entidade '{entity_type}' não reconhecido. Disponíveis: {available_types}")
    
    return schema_mapping[entity_type_lower]


def validate_data_with_schema(entity_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valida dados usando o schema apropriado.
    
    Args:
        entity_type: Tipo da entidade
        data: Dados para validar
        
    Returns:
        Dict[str, Any]: Resultado da validação
    """
    try:
        schema_class = get_schema_for_entity(entity_type)
        validated_data = schema_class(**data)
        
        return {
            'valid': True,
            'data': validated_data.dict(),
            'errors': [],
            'warnings': []
        }
        
    except Exception as e:
        return {
            'valid': False,
            'data': None,
            'errors': [str(e)],
            'warnings': []
        } 