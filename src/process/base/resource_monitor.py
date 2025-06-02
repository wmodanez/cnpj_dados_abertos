"""
Monitor de recursos do sistema unificado.

Este módulo centraliza todo o monitoramento de recursos (CPU, memória, disco)
que estava duplicado nos 4 processadores originais.
"""

import logging
import os
import psutil
from typing import Dict, NamedTuple, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class SystemResources(NamedTuple):
    """Estrutura para armazenar informações de recursos do sistema."""
    cpu_percent: float
    memory_percent: float
    disk_percent: float
    memory_total_gb: float
    memory_available_gb: float
    cpu_count: int


@dataclass
class ResourceThresholds:
    """Configurações de limites de recursos."""
    max_cpu_percent: float = 80.0
    max_memory_percent: float = 80.0
    max_disk_percent: float = 90.0
    min_memory_gb: float = 2.0


class ResourceMonitor:
    """
    Monitor centralizado de recursos do sistema.
    
    Substitui as funções duplicadas de monitoramento presentes em:
    - get_system_resources()
    - can_start_processing()
    - log_system_resources_*()
    """
    
    def __init__(self, thresholds: ResourceThresholds = None):
        self.thresholds = thresholds or ResourceThresholds()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def get_system_resources(self) -> SystemResources:
        """
        Retorna informações atuais sobre os recursos do sistema.
        
        Returns:
            SystemResources: Informações completas do sistema
        """
        try:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = os.cpu_count() or 4
            
            # Memória
            memory_info = psutil.virtual_memory()
            memory_total_gb = memory_info.total / (1024**3)
            memory_available_gb = memory_info.available / (1024**3)
            memory_percent = memory_info.percent
            
            # Disco
            disk_info = psutil.disk_usage('/')
            disk_percent = disk_info.percent
            
            return SystemResources(
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                disk_percent=disk_percent,
                memory_total_gb=memory_total_gb,
                memory_available_gb=memory_available_gb,
                cpu_count=cpu_count
            )
        except Exception as e:
            self.logger.error(f"Erro ao obter recursos do sistema: {e}")
            # Retornar valores padrão seguros
            return SystemResources(
                cpu_percent=0.0,
                memory_percent=0.0,
                disk_percent=0.0,
                memory_total_gb=8.0,
                memory_available_gb=4.0,
                cpu_count=4
            )
    
    def get_system_resources_dict(self) -> Dict[str, Any]:
        """
        Retorna informações dos recursos do sistema como dict.
        
        Versão de compatibilidade para testes que esperam dict.
        
        Returns:
            Dict com informações do sistema
        """
        resources = self.get_system_resources()
        return {
            'cpu_percent': resources.cpu_percent,
            'memory_percent': resources.memory_percent,
            'disk_percent': resources.disk_percent,
            'memory_total': resources.memory_total_gb,
            'memory_available': resources.memory_available_gb,
            'cpu_count': resources.cpu_count
        }
    
    def can_start_processing(self, active_processes: int, max_processes: int) -> bool:
        """
        Verifica se é possível iniciar um novo processamento.
        
        Args:
            active_processes: Número atual de processos ativos
            max_processes: Máximo de processos permitidos
            
        Returns:
            bool: True se pode iniciar processamento, False caso contrário
        """
        resources = self.get_system_resources()
        
        # Verificar recursos do sistema
        if resources.cpu_percent > self.thresholds.max_cpu_percent:
            self.logger.debug(f"CPU alta: {resources.cpu_percent:.1f}% > {self.thresholds.max_cpu_percent}%")
            return False
        
        if resources.memory_percent > self.thresholds.max_memory_percent:
            self.logger.debug(f"Memória alta: {resources.memory_percent:.1f}% > {self.thresholds.max_memory_percent}%")
            return False
        
        if resources.disk_percent > self.thresholds.max_disk_percent:
            self.logger.debug(f"Disco cheio: {resources.disk_percent:.1f}% > {self.thresholds.max_disk_percent}%")
            return False
        
        if resources.memory_available_gb < self.thresholds.min_memory_gb:
            self.logger.debug(f"Pouca memória disponível: {resources.memory_available_gb:.1f}GB < {self.thresholds.min_memory_gb}GB")
            return False
        
        # Verificar número de processos ativos
        if active_processes >= max_processes:
            self.logger.debug(f"Muitos processos ativos: {active_processes}/{max_processes}")
            return False
        
        return True
    
    def log_system_resources(self, processor_name: str, max_workers: int = None):
        """
        Log detalhado dos recursos do sistema para um processador específico.
        
        Substitui as funções log_system_resources_*() específicas de cada processador.
        
        Args:
            processor_name: Nome do processador (ex: "EMPRESA", "SÓCIO")
            max_workers: Número de workers configurados
        """
        resources = self.get_system_resources()
        
        if max_workers is None:
            max_workers = max(2, resources.cpu_count // 2)
        
        self.logger.info("=" * 50)
        self.logger.info(f"🏭 MÓDULO {processor_name.upper()} - CONFIGURAÇÃO DE RECURSOS")
        self.logger.info("=" * 50)
        self.logger.info(f"💻 CPU: {resources.cpu_count} núcleos disponíveis ({resources.cpu_percent:.1f}% em uso)")
        self.logger.info(f"🧠 RAM: {resources.memory_total_gb:.1f}GB total, {resources.memory_available_gb:.1f}GB disponível ({100-resources.memory_percent:.1f}%)")
        self.logger.info(f"💽 Disco: {100-resources.disk_percent:.1f}% livre")
        self.logger.info(f"⚙️  Workers configurados: {max_workers} ({(max_workers/resources.cpu_count)*100:.1f}% dos núcleos)")
        self.logger.info(f"📊 Estratégia: Usar pelo menos 50% dos núcleos para processamento paralelo")
        self.logger.info(f"🔄 Capacidade estimada: ~{max_workers * 2} arquivos ZIP simultâneos")
        self.logger.info(f"💾 Memória por worker: ~{resources.memory_available_gb/max_workers:.1f}GB")
        
        # Avisos e recomendações
        if resources.memory_percent > 80:
            self.logger.warning(f"⚠️  ATENÇÃO: Uso alto de memória ({resources.memory_percent:.1f}%)")
        if resources.cpu_count < 4:
            self.logger.warning(f"⚠️  ATENÇÃO: Poucos núcleos CPU ({resources.cpu_count}) - considere upgrade")
        if resources.disk_percent > 80:
            self.logger.warning(f"⚠️  ATENÇÃO: Pouco espaço em disco ({100-resources.disk_percent:.1f}% livre)")
        
        # Status da configuração
        if max_workers == resources.cpu_count:
            self.logger.info(f"✅ Configuração otimizada: usando todos os núcleos disponíveis")
        elif max_workers >= resources.cpu_count // 2:
            self.logger.info(f"✅ Configuração balanceada: usando {(max_workers/resources.cpu_count)*100:.0f}% dos núcleos")
        else:
            self.logger.info(f"⚠️  Configuração conservadora: usando apenas {(max_workers/resources.cpu_count)*100:.0f}% dos núcleos")
        
        self.logger.info("=" * 50)
    
    def get_optimal_workers(self) -> int:
        """
        Calcula o número ótimo de workers baseado nos recursos disponíveis.
        
        Returns:
            int: Número recomendado de workers
        """
        resources = self.get_system_resources()
        
        # Fator baseado na memória disponível (mínimo 2GB por worker)
        memory_factor = max(1, int(resources.memory_available_gb // 2))
        
        # Fator baseado no número de núcleos (usar pelo menos 50%)
        cpu_factor = max(2, resources.cpu_count // 2)
        
        # Usar o menor dos dois fatores para não sobrecarregar
        optimal_workers = min(memory_factor, cpu_factor)
        
        self.logger.debug(f"Workers ótimos calculados: {optimal_workers} "
                         f"(mem_factor: {memory_factor}, cpu_factor: {cpu_factor})")
        
        return optimal_workers
    
    def monitor_resources_periodically(self, interval_seconds: int = 30):
        """
        Monitora recursos periodicamente e registra logs de alerta.
        
        Args:
            interval_seconds: Intervalo entre verificações
        """
        import time
        import threading
        
        def monitor_loop():
            while True:
                try:
                    resources = self.get_system_resources()
                    
                    # Log de alerta se recursos estão altos
                    if resources.cpu_percent > self.thresholds.max_cpu_percent:
                        self.logger.warning(f"🔥 CPU alta: {resources.cpu_percent:.1f}%")
                    
                    if resources.memory_percent > self.thresholds.max_memory_percent:
                        self.logger.warning(f"🧠 Memória alta: {resources.memory_percent:.1f}%")
                    
                    if resources.disk_percent > self.thresholds.max_disk_percent:
                        self.logger.warning(f"💽 Disco quase cheio: {resources.disk_percent:.1f}%")
                    
                    time.sleep(interval_seconds)
                    
                except Exception as e:
                    self.logger.error(f"Erro no monitoramento de recursos: {e}")
                    time.sleep(interval_seconds)
        
        # Iniciar thread de monitoramento
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        
        self.logger.info(f"Monitor de recursos iniciado (intervalo: {interval_seconds}s)") 