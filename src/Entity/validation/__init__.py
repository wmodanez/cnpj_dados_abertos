"""
Módulo de Validação para Entidades

Este módulo contém todas as classes e funções relacionadas à validação
de dados das entidades da Receita Federal.
"""

from .validator import EntityValidator
from .batch import validate_dataframe_batch, create_validation_summary

__all__ = [
    'EntityValidator',
    'validate_dataframe_batch', 
    'create_validation_summary'
] 