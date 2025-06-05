"""
Sistema de Circuit Breaker Global para Interrupção de Aplicação.

Este módulo implementa um mecanismo de detecção de falhas críticas
que pode interromper toda a aplicação para evitar desperdício de recursos.
"""

import logging
import threading
import time
from typing import List, Set, Callable, Optional
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta
import os
import psutil

logger = logging.getLogger(__name__)


class FailureType(Enum):
    """Tipos de falhas que podem ser monitoradas."""
    CONNECTIVITY = "connectivity"
    DISK_SPACE = "disk_space"
    MEMORY = "memory"
    PERMISSIONS = "permissions"
    DATA_CORRUPTION = "data_corruption"
    SYSTEM_RESOURCE = "system_resource"
    PROCESSING_FAILURE = "processing_failure"
    DOWNLOAD_FAILURE = "download_failure"


class CriticalityLevel(Enum):
    """Níveis de criticidade das falhas."""
    WARNING = "warning"      # Log apenas
    MODERATE = "moderate"    # Pausa temporária
    CRITICAL = "critical"    # Interrupção imediata
    FATAL = "fatal"         # Termina aplicação


@dataclass
class FailureEvent:
    """Representa um evento de falha."""
    failure_type: FailureType
    criticality: CriticalityLevel
    message: str
    timestamp: datetime
    component: str
    additional_info: dict = None


class GlobalCircuitBreaker:
    """
    Circuit Breaker global para monitoramento e interrupção da aplicação.
    
    Características:
    - Monitora falhas críticas de diferentes componentes
    - Decide quando interromper toda a aplicação
    - Coordena a parada de processos paralelos
    - Mantém histórico de falhas para análise
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Implementa Singleton para garantir instância única."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Inicializa o circuit breaker se ainda não foi inicializado."""
        if hasattr(self, '_initialized'):
            return
            
        self._initialized = True
        self._is_application_stopped = False
        self._stop_callbacks: List[Callable] = []
        self._failure_history: List[FailureEvent] = []
        self._component_failures: dict = {}
        self._lock = threading.RLock()
        
        # Contadores e limites
        self._failure_thresholds = {
            FailureType.CONNECTIVITY: 3,
            FailureType.DISK_SPACE: 1,     # Imediato
            FailureType.MEMORY: 2,
            FailureType.PERMISSIONS: 1,    # Imediato
            FailureType.DATA_CORRUPTION: 5,
            FailureType.SYSTEM_RESOURCE: 2,
            FailureType.PROCESSING_FAILURE: 10,
            FailureType.DOWNLOAD_FAILURE: 8,
        }
        
        # Janela de tempo para contar falhas (minutos)
        self._time_window = 10
        
        logger.info("🚨 Circuit Breaker Global inicializado")
        logger.info(f"📊 Limites configurados: {dict(self._failure_thresholds)}")
        logger.info(f"⏱️  Janela de tempo: {self._time_window} minutos")
    
    def register_stop_callback(self, callback: Callable) -> None:
        """
        Registra callback para ser chamado quando aplicação deve parar.
        
        Args:
            callback: Função a ser chamada para parar componente
        """
        with self._lock:
            self._stop_callbacks.append(callback)
            logger.debug(f"Callback de parada registrado: {callback.__name__}")
    
    def report_failure(
        self, 
        failure_type: FailureType, 
        message: str, 
        component: str,
        criticality: CriticalityLevel = CriticalityLevel.WARNING,
        additional_info: dict = None
    ) -> bool:
        """
        Reporta uma falha ao circuit breaker.
        
        Args:
            failure_type: Tipo da falha
            message: Descrição da falha
            component: Componente que reportou a falha
            criticality: Nível de criticidade
            additional_info: Informações adicionais
            
        Returns:
            bool: True se aplicação deve continuar, False se deve parar
        """
        with self._lock:
            if self._is_application_stopped:
                return False
            
            # Criar evento de falha
            failure_event = FailureEvent(
                failure_type=failure_type,
                criticality=criticality,
                message=message,
                timestamp=datetime.now(),
                component=component,
                additional_info=additional_info or {}
            )
            
            # Adicionar ao histórico
            self._failure_history.append(failure_event)
            
            # Contar falhas por componente
            component_key = f"{component}_{failure_type.value}"
            self._component_failures[component_key] = self._component_failures.get(component_key, 0) + 1
            
            # Log do evento
            self._log_failure_event(failure_event)
            
            # Verificar se deve interromper aplicação
            should_stop = self._should_stop_application(failure_event)
            
            if should_stop:
                self._trigger_application_stop(failure_event)
                return False
            
            return True
    
    def _should_stop_application(self, latest_failure: FailureEvent) -> bool:
        """
        Determina se a aplicação deve ser interrompida.
        
        Args:
            latest_failure: Último evento de falha
            
        Returns:
            bool: True se deve parar, False caso contrário
        """
        # Falhas de criticidade FATAL sempre param
        if latest_failure.criticality == CriticalityLevel.FATAL:
            return True
        
        # Falhas CRITICAL param imediatamente
        if latest_failure.criticality == CriticalityLevel.CRITICAL:
            return True
        
        # Verificar se excedeu limite de falhas do mesmo tipo
        failure_type = latest_failure.failure_type
        threshold = self._failure_thresholds.get(failure_type, 5)
        
        # Contar falhas do mesmo tipo na janela de tempo
        cutoff_time = datetime.now() - timedelta(minutes=self._time_window)
        recent_failures = [
            f for f in self._failure_history 
            if f.failure_type == failure_type and f.timestamp >= cutoff_time
        ]
        
        if len(recent_failures) >= threshold:
            logger.warning(f"🚨 Limite de falhas excedido: {len(recent_failures)}/{threshold} para {failure_type.value}")
            return True
        
        # Verificar falhas críticas de sistema
        if failure_type in [FailureType.DISK_SPACE, FailureType.MEMORY, FailureType.PERMISSIONS]:
            if len(recent_failures) >= 1:  # Qualquer falha crítica de sistema
                return True
        
        # Verificar padrões de falhas múltiplas
        if self._detect_cascading_failures():
            return True
        
        return False
    
    def _detect_cascading_failures(self) -> bool:
        """
        Detecta falhas em cascata que indicam problema sistêmico.
        
        Returns:
            bool: True se detectou falhas em cascata
        """
        cutoff_time = datetime.now() - timedelta(minutes=5)  # Janela menor para cascata
        recent_failures = [f for f in self._failure_history if f.timestamp >= cutoff_time]
        
        if len(recent_failures) < 3:
            return False
        
        # Verificar se há falhas de tipos diferentes em componentes diferentes
        unique_types = len(set(f.failure_type for f in recent_failures))
        unique_components = len(set(f.component for f in recent_failures))
        
        # Falhas em cascata: múltiplos tipos em múltiplos componentes
        if unique_types >= 2 and unique_components >= 2 and len(recent_failures) >= 5:
            logger.warning(f"🚨 Falhas em cascata detectadas: {unique_types} tipos, {unique_components} componentes")
            return True
        
        return False
    
    def _trigger_application_stop(self, trigger_failure: FailureEvent) -> None:
        """
        Aciona a parada da aplicação.
        
        Args:
            trigger_failure: Falha que acionou a parada
        """
        if self._is_application_stopped:
            return
        
        self._is_application_stopped = True
        
        logger.critical("🚨🛑 INTERRUPÇÃO GLOBAL DA APLICAÇÃO ACIONADA!")
        logger.critical(f"🔥 Motivo: {trigger_failure.failure_type.value} - {trigger_failure.message}")
        logger.critical(f"📍 Componente: {trigger_failure.component}")
        logger.critical(f"📊 Criticidade: {trigger_failure.criticality.value}")
        
        # Mostrar estatísticas finais
        self._log_failure_statistics()
        
        # Chamar todos os callbacks de parada
        logger.info(f"📞 Chamando {len(self._stop_callbacks)} callbacks de parada...")
        
        for i, callback in enumerate(self._stop_callbacks):
            try:
                logger.info(f"📞 Executando callback {i+1}: {callback.__name__}")
                callback()
            except Exception as e:
                logger.error(f"❌ Erro ao executar callback {callback.__name__}: {e}")
        
        logger.critical("🛑 Aplicação interrompida devido a falhas críticas")
    
    def _log_failure_event(self, failure: FailureEvent) -> None:
        """Log estruturado do evento de falha."""
        level_map = {
            CriticalityLevel.WARNING: logger.warning,
            CriticalityLevel.MODERATE: logger.warning,
            CriticalityLevel.CRITICAL: logger.error,
            CriticalityLevel.FATAL: logger.critical
        }
        
        log_func = level_map.get(failure.criticality, logger.info)
        
        emoji_map = {
            CriticalityLevel.WARNING: "⚠️",
            CriticalityLevel.MODERATE: "🔶",
            CriticalityLevel.CRITICAL: "🔥",
            CriticalityLevel.FATAL: "💀"
        }
        
        emoji = emoji_map.get(failure.criticality, "📝")
        
        log_func(f"{emoji} FALHA {failure.criticality.value.upper()}: {failure.message}")
        log_func(f"📍 Componente: {failure.component}")
        log_func(f"🏷️  Tipo: {failure.failure_type.value}")
        
        if failure.additional_info:
            log_func(f"📋 Info adicional: {failure.additional_info}")
    
    def _log_failure_statistics(self) -> None:
        """Log das estatísticas de falhas."""
        logger.info("📊 ESTATÍSTICAS DE FALHAS:")
        logger.info(f"📈 Total de falhas: {len(self._failure_history)}")
        
        # Falhas por tipo
        type_counts = {}
        for failure in self._failure_history:
            type_counts[failure.failure_type.value] = type_counts.get(failure.failure_type.value, 0) + 1
        
        logger.info("📊 Falhas por tipo:")
        for failure_type, count in sorted(type_counts.items()):
            logger.info(f"   {failure_type}: {count}")
        
        # Falhas por componente
        component_counts = {}
        for failure in self._failure_history:
            component_counts[failure.component] = component_counts.get(failure.component, 0) + 1
        
        logger.info("📊 Falhas por componente:")
        for component, count in sorted(component_counts.items()):
            logger.info(f"   {component}: {count}")
    
    def is_application_stopped(self) -> bool:
        """
        Verifica se a aplicação foi marcada para parada.
        
        Returns:
            bool: True se aplicação deve parar
        """
        return self._is_application_stopped
    
    def force_stop(self, reason: str = "Parada forçada") -> None:
        """
        Força a parada da aplicação.
        
        Args:
            reason: Motivo da parada forçada
        """
        fake_failure = FailureEvent(
            failure_type=FailureType.SYSTEM_RESOURCE,
            criticality=CriticalityLevel.FATAL,
            message=reason,
            timestamp=datetime.now(),
            component="MANUAL"
        )
        
        self._trigger_application_stop(fake_failure)
    
    def get_failure_summary(self) -> dict:
        """
        Retorna resumo das falhas para relatórios.
        
        Returns:
            dict: Resumo das falhas
        """
        with self._lock:
            return {
                'total_failures': len(self._failure_history),
                'application_stopped': self._is_application_stopped,
                'registered_callbacks': len(self._stop_callbacks),
                'failure_thresholds': dict(self._failure_thresholds),
                'time_window_minutes': self._time_window,
                'recent_failures': [
                    {
                        'type': f.failure_type.value,
                        'component': f.component,
                        'message': f.message,
                        'criticality': f.criticality.value,
                        'timestamp': f.timestamp.isoformat()
                    }
                    for f in self._failure_history[-10:]  # Últimas 10 falhas
                ]
            }
    
    def reset(self) -> None:
        """
        Reseta o circuit breaker (uso em testes).
        """
        with self._lock:
            self._is_application_stopped = False
            self._failure_history.clear()
            self._component_failures.clear()
            logger.info("🔄 Circuit Breaker resetado")


# Instância global (Singleton)
circuit_breaker = GlobalCircuitBreaker()


# Funções de conveniência
def report_critical_failure(
    failure_type: FailureType, 
    message: str, 
    component: str,
    additional_info: dict = None
) -> bool:
    """
    Função de conveniência para reportar falhas críticas.
    
    Returns:
        bool: True se aplicação deve continuar
    """
    return circuit_breaker.report_failure(
        failure_type=failure_type,
        message=message,
        component=component,
        criticality=CriticalityLevel.CRITICAL,
        additional_info=additional_info
    )


def report_fatal_failure(
    failure_type: FailureType, 
    message: str, 
    component: str,
    additional_info: dict = None
) -> bool:
    """
    Função de conveniência para reportar falhas fatais.
    
    Returns:
        bool: True se aplicação deve continuar (sempre False para fatais)
    """
    return circuit_breaker.report_failure(
        failure_type=failure_type,
        message=message,
        component=component,
        criticality=CriticalityLevel.FATAL,
        additional_info=additional_info
    )


def should_continue_processing() -> bool:
    """
    Verifica se o processamento deve continuar.
    
    Returns:
        bool: True se deve continuar processando
    """
    return not circuit_breaker.is_application_stopped()


def register_stop_callback(callback: Callable) -> None:
    """
    Registra callback para parada da aplicação.
    
    Args:
        callback: Função a ser chamada para parar componente
    """
    circuit_breaker.register_stop_callback(callback) 