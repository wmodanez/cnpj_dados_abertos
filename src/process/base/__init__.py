"""
Módulo base para processadores unificados.

Este módulo contém as classes base e utilitários compartilhados
por todos os processadores de dados da Receita Federal.
"""

from .processor import BaseProcessor
from .queue_manager import ProcessingQueueManager  
from .factory import ProcessorFactory
from .resource_monitor import ResourceMonitor

__all__ = [
    'BaseProcessor',
    'ProcessingQueueManager', 
    'ProcessorFactory',
    'ResourceMonitor'
] 