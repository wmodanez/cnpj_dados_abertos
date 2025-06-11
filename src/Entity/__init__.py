"""
Módulo de Entidades para o Sistema de Dados da Receita Federal

Este módulo contém as definições de entidades que representam os dados
da Receita Federal, incluindo validações, transformações e schemas.
"""

from .base import BaseEntity, EntityFactory

# Imports dos schemas das pastas específicas
from .schemas import (
    EmpresaSchema,
    EstabelecimentoSchema,
    SocioSchema,
    SimplesSchema,
    PainelSchema
)

# Imports do sistema de validação da pasta específica
from .validation import EntityValidator

# Imports das entidades implementadas
from .Empresa import Empresa
from .Estabelecimento import Estabelecimento
from .Socio import Socio
from .Simples import Simples
from .Painel import Painel

# Imports das novas entidades de dados auxiliares
from .Municipio import Municipio
from .Motivo import Motivo
from .Cnae import Cnae
from .NaturezaJuridica import NaturezaJuridica
from .QualificacaoSocio import QualificacaoSocio

# Registrar entidades no factory
EntityFactory.register_entity('empresa', Empresa)
EntityFactory.register_entity('estabelecimento', Estabelecimento)
EntityFactory.register_entity('socio', Socio)
EntityFactory.register_entity('simples', Simples)
EntityFactory.register_entity('painel', Painel)

# Registrar entidades auxiliares no factory
EntityFactory.register_entity('municipio', Municipio)
EntityFactory.register_entity('motivo', Motivo)
EntityFactory.register_entity('cnae', Cnae)
EntityFactory.register_entity('natureza_juridica', NaturezaJuridica)
EntityFactory.register_entity('qualificacao_socio', QualificacaoSocio)

__all__ = [
    # Classes base
    'BaseEntity',
    'EntityFactory',
    'EntityValidator',
    
    # Schemas de validação
    'EmpresaSchema',
    'EstabelecimentoSchema', 
    'SocioSchema',
    'SimplesSchema',
    'PainelSchema',
    
    # Entidades principais
    'Empresa',
    'Estabelecimento',
    'Socio',
    'Simples',
    'Painel',
    
    # Entidades auxiliares
    'Municipio',
    'Motivo',
    'Cnae',
    'NaturezaJuridica',
    'QualificacaoSocio'
]

__version__ = '1.0.0'
__author__ = 'Sistema RF'
__description__ = 'Entidades para dados da Receita Federal' 