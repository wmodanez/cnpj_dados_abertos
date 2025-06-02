"""
Factory para criação de processadores unificados.

Este módulo centraliza a criação e registro de diferentes tipos de processadores,
permitindo extensibilidade e configuração dinâmica.
"""

import logging
from typing import Dict, Type, Any, Optional, List
from .processor import BaseProcessor

logger = logging.getLogger(__name__)


class ProcessorFactory:
    """
    Factory para criação e registro de processadores.
    
    Permite:
    - Registro dinâmico de novos tipos de processadores
    - Criação com validação de parâmetros
    - Listagem de processadores disponíveis
    - Configuração centralizada
    """
    
    _processors: Dict[str, Type[BaseProcessor]] = {}
    _instances: Dict[str, BaseProcessor] = {}
    
    @classmethod
    def register(cls, name: str, processor_class: Type[BaseProcessor]) -> None:
        """
        Registra um novo tipo de processador.
        
        Args:
            name: Nome do processador (ex: 'empresa', 'socio')
            processor_class: Classe do processador
        """
        if not issubclass(processor_class, BaseProcessor):
            raise ValueError(f"Classe {processor_class.__name__} deve herdar de BaseProcessor")
        
        cls._processors[name.lower()] = processor_class
        logger.info(f"Processador '{name}' registrado: {processor_class.__name__}")
    
    @classmethod
    def create(
        cls, 
        processor_type: str, 
        path_zip: str, 
        path_unzip: str, 
        path_parquet: str, 
        **kwargs
    ) -> BaseProcessor:
        """
        Cria uma instância de processador.
        
        Args:
            processor_type: Tipo do processador (ex: 'empresa', 'socio')
            path_zip: Diretório com arquivos ZIP
            path_unzip: Diretório para extração
            path_parquet: Diretório de saída
            **kwargs: Opções específicas do processador
            
        Returns:
            Instância do processador
            
        Raises:
            ValueError: Se tipo não registrado ou parâmetros inválidos
        """
        processor_type = processor_type.lower()
        
        if processor_type not in cls._processors:
            available = ', '.join(cls._processors.keys())
            raise ValueError(f"Processador '{processor_type}' não registrado. Disponíveis: {available}")
        
        try:
            processor_class = cls._processors[processor_type]
            
            # Validar parâmetros obrigatórios
            cls._validate_paths(path_zip, path_unzip, path_parquet)
            
            # Criar instância
            instance = processor_class(path_zip, path_unzip, path_parquet, **kwargs)
            
            # Armazenar instância para reutilização (opcional)
            instance_key = f"{processor_type}_{hash((path_zip, path_unzip, path_parquet, str(sorted(kwargs.items()))))}"
            cls._instances[instance_key] = instance
            
            logger.info(f"Processador '{processor_type}' criado com sucesso")
            return instance
            
        except Exception as e:
            logger.error(f"Erro ao criar processador '{processor_type}': {str(e)}")
            raise
    
    @classmethod
    def create_multiple(
        cls,
        processor_types: List[str],
        path_zip: str,
        path_unzip: str, 
        path_parquet: str,
        **common_kwargs
    ) -> Dict[str, BaseProcessor]:
        """
        Cria múltiplos processadores com configurações comuns.
        
        Args:
            processor_types: Lista de tipos de processadores
            path_zip, path_unzip, path_parquet: Caminhos comuns
            **common_kwargs: Configurações comuns para todos
            
        Returns:
            Dict mapeando tipo -> instância do processador
        """
        processors = {}
        
        for processor_type in processor_types:
            try:
                processor = cls.create(
                    processor_type, path_zip, path_unzip, path_parquet, **common_kwargs
                )
                processors[processor_type] = processor
                
            except Exception as e:
                logger.error(f"Falha ao criar processador '{processor_type}': {str(e)}")
                # Continua criando os outros processadores
        
        logger.info(f"Criados {len(processors)} processadores: {list(processors.keys())}")
        return processors
    
    @classmethod
    def get_registered_processors(cls) -> List[str]:
        """
        Retorna lista de processadores registrados.
        
        Returns:
            Lista com nomes dos processadores disponíveis
        """
        return list(cls._processors.keys())
    
    @classmethod
    def get_processor_info(cls, processor_type: str) -> Dict[str, Any]:
        """
        Retorna informações detalhadas sobre um processador.
        
        Args:
            processor_type: Tipo do processador
            
        Returns:
            Dict com informações do processador
        """
        processor_type = processor_type.lower()
        
        if processor_type not in cls._processors:
            raise ValueError(f"Processador '{processor_type}' não registrado")
        
        processor_class = cls._processors[processor_type]
        
        # Criar instância temporária para obter informações
        try:
            temp_instance = processor_class("/tmp", "/tmp", "/tmp")
            
            info = {
                'name': processor_type,
                'class_name': processor_class.__name__,
                'processor_name': temp_instance.get_processor_name(),
                'entity_class': temp_instance.get_entity_class().__name__,
                'valid_options': temp_instance.get_valid_options(),
                'description': processor_class.__doc__ or "Sem descrição",
            }
            
            return info
            
        except Exception as e:
            logger.warning(f"Não foi possível obter informações detalhadas de '{processor_type}': {str(e)}")
            return {
                'name': processor_type,
                'class_name': processor_class.__name__,
                'description': processor_class.__doc__ or "Sem descrição",
                'error': str(e)
            }
    
    @classmethod
    def list_all_processors(cls) -> Dict[str, Dict[str, Any]]:
        """
        Lista todos os processadores registrados com suas informações.
        
        Returns:
            Dict mapeando nome -> informações do processador
        """
        result = {}
        
        for processor_type in cls._processors:
            try:
                result[processor_type] = cls.get_processor_info(processor_type)
            except Exception as e:
                result[processor_type] = {
                    'name': processor_type,
                    'error': str(e)
                }
        
        return result
    
    @classmethod
    def validate_configuration(
        cls, 
        processor_type: str, 
        **kwargs
    ) -> tuple[bool, List[str]]:
        """
        Valida configuração para um processador sem criar instância.
        
        Args:
            processor_type: Tipo do processador
            **kwargs: Configurações a validar
            
        Returns:
            Tuple (is_valid, error_messages)
        """
        processor_type = processor_type.lower()
        errors = []
        
        # Verificar se processador está registrado
        if processor_type not in cls._processors:
            errors.append(f"Processador '{processor_type}' não registrado")
            return False, errors
        
        try:
            # Obter informações do processador
            info = cls.get_processor_info(processor_type)
            valid_options = info.get('valid_options', [])
            
            # Validar opções fornecidas
            for option in kwargs:
                if option not in valid_options and option not in ['max_workers']:
                    errors.append(f"Opção '{option}' não é válida para processador '{processor_type}'")
            
            # Validações específicas por tipo
            if processor_type == 'empresa':
                if 'create_private' in kwargs and not isinstance(kwargs['create_private'], bool):
                    errors.append("Opção 'create_private' deve ser boolean")
            
            elif processor_type == 'estabelecimento':
                if 'uf_subset' in kwargs:
                    uf = kwargs['uf_subset']
                    if not isinstance(uf, str) or len(uf) != 2:
                        errors.append("Opção 'uf_subset' deve ser string de 2 caracteres (ex: 'SP')")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            errors.append(f"Erro na validação: {str(e)}")
            return False, errors
    
    @classmethod
    def clear_cache(cls) -> None:
        """Remove todas as instâncias em cache."""
        cls._instances.clear()
        logger.info("Cache de instâncias de processadores limpo")
    
    @classmethod
    def get_cached_instance(cls, processor_type: str, **kwargs) -> Optional[BaseProcessor]:
        """
        Retorna instância em cache se existir.
        
        Args:
            processor_type: Tipo do processador
            **kwargs: Configurações usadas na criação
            
        Returns:
            Instância em cache ou None
        """
        # Gerar chave de cache
        cache_key = f"{processor_type.lower()}_{hash(str(sorted(kwargs.items())))}"
        return cls._instances.get(cache_key)
    
    @classmethod
    def unregister(cls, processor_type: str) -> bool:
        """
        Remove registro de um processador.
        
        Args:
            processor_type: Tipo do processador a remover
            
        Returns:
            bool: True se removido, False se não existia
        """
        processor_type = processor_type.lower()
        
        if processor_type in cls._processors:
            del cls._processors[processor_type]
            
            # Remover instâncias em cache relacionadas
            keys_to_remove = [k for k in cls._instances.keys() if k.startswith(f"{processor_type}_")]
            for key in keys_to_remove:
                del cls._instances[key]
            
            logger.info(f"Processador '{processor_type}' removido do registro")
            return True
        
        return False
    
    @classmethod
    def _validate_paths(cls, path_zip: str, path_unzip: str, path_parquet: str) -> None:
        """
        Valida caminhos fornecidos.
        
        Args:
            path_zip, path_unzip, path_parquet: Caminhos a validar
            
        Raises:
            ValueError: Se algum caminho é inválido
        """
        import os
        
        # Validar path_zip (deve existir)
        if not os.path.exists(path_zip):
            raise ValueError(f"Diretório ZIP não existe: {path_zip}")
        
        if not os.path.isdir(path_zip):
            raise ValueError(f"Path ZIP não é um diretório: {path_zip}")
        
        # Validar path_unzip e path_parquet (podem ser criados)
        for name, path in [("UNZIP", path_unzip), ("PARQUET", path_parquet)]:
            if not path:
                raise ValueError(f"Path {name} não pode ser vazio")
            
            # Tentar criar se não existir
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                raise ValueError(f"Não foi possível criar diretório {name} '{path}': {str(e)}")


# Função de conveniência para auto-registro
def auto_register_processors():
    """
    Registra automaticamente processadores padrão.
    
    Esta função deve ser chamada durante a inicialização para
    registrar os processadores básicos do sistema.
    """
    try:
        # Import aqui para evitar imports circulares
        from ..processors.empresa_processor import EmpresaProcessor
        from ..processors.estabelecimento_processor import EstabelecimentoProcessor
        from ..processors.socio_processor import SocioProcessor
        from ..processors.simples_processor import SimplesProcessor
        
        # Registrar processadores
        ProcessorFactory.register('empresa', EmpresaProcessor)
        ProcessorFactory.register('estabelecimento', EstabelecimentoProcessor)
        ProcessorFactory.register('socio', SocioProcessor)
        ProcessorFactory.register('simples', SimplesProcessor)
        
        logger.info("Processadores padrão registrados automaticamente")
        
    except ImportError as e:
        logger.warning(f"Alguns processadores não puderam ser registrados automaticamente: {str(e)}")
        logger.info("Processadores podem ser registrados manualmente quando necessário")


# Aliases para conveniência
create_processor = ProcessorFactory.create
list_processors = ProcessorFactory.get_registered_processors
processor_info = ProcessorFactory.get_processor_info 