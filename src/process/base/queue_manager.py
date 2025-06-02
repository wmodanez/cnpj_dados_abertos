"""
Gerenciador de filas de processamento unificado.

Este módulo centraliza todo o sistema de fila de processamento
que estava duplicado nos 4 processadores originais.
"""

import logging
import threading
import time
from queue import PriorityQueue, Empty
from multiprocessing import Lock, Value
from typing import Callable, Any, Optional, Dict, Tuple
from dataclasses import dataclass

# Corrigir imports
try:
    from ...utils.progress_tracker import progress_tracker
    from ...utils.statistics import global_stats
except ImportError:
    # Fallback se o import falhar
    progress_tracker = None
    global_stats = None
    
from .resource_monitor import ResourceMonitor

logger = logging.getLogger(__name__)


@dataclass
class QueueItem:
    """Item da fila de processamento."""
    priority: int
    timestamp: float
    zip_file: str
    extra_data: Dict[str, Any] = None


class ProcessingQueueManager:
    """
    Gerenciador centralizado de filas de processamento.
    
    Substitui o sistema de fila duplicado presente em todos os 4 processadores:
    - _processing_lock, _active_processes, _max_concurrent_processes
    - _process_queue, _workers_should_stop  
    - add_to_process_queue(), process_queue_worker(), start_queue_worker()
    """
    
    def __init__(self, processor_name: str, max_workers: int = None):
        self.processor_name = processor_name
        self.resource_monitor = ResourceMonitor()
        
        # Variáveis de controle (substituem as globais duplicadas)
        self._processing_lock = Lock()
        self._active_processes = Value('i', 0)
        self._process_queue = PriorityQueue()
        self._workers_should_stop = Value('b', False)
        
        # Configurar número máximo de workers
        if max_workers is None:
            max_workers = self.resource_monitor.get_optimal_workers()
        self._max_concurrent_processes = Value('i', max_workers)
        
        # Thread pool para workers
        self._workers = []
        self._worker_count = 0
        
        self.logger = logging.getLogger(f"{__name__}.{processor_name}")
        
        # Log da configuração inicial
        self.resource_monitor.log_system_resources(processor_name, max_workers)
    
    @property
    def active_processes(self) -> int:
        """Número atual de processos ativos."""
        return self._active_processes.value
    
    @property
    def max_processes(self) -> int:
        """Número máximo de processos permitidos."""
        return self._max_concurrent_processes.value
    
    @property
    def queue_size(self) -> int:
        """Tamanho atual da fila."""
        return self._process_queue.qsize()
    
    def can_start_processing(self) -> bool:
        """
        Verifica se é possível iniciar um novo processamento.
        
        Returns:
            bool: True se pode iniciar, False caso contrário
        """
        return self.resource_monitor.can_start_processing(
            self.active_processes, 
            self.max_processes
        )
    
    def add_to_queue(self, zip_file: str, priority: int = 1, **extra_data) -> None:
        """
        Adiciona um arquivo à fila de processamento.
        
        Args:
            zip_file: Nome do arquivo ZIP a processar
            priority: Prioridade (menor número = maior prioridade)  
            **extra_data: Dados extras para o processamento
        """
        item = QueueItem(
            priority=priority,
            timestamp=time.time(),
            zip_file=zip_file,
            extra_data=extra_data or {}
        )
        
        self._process_queue.put((priority, item.timestamp, item))
        self.logger.info(f"Arquivo {zip_file} adicionado à fila de processamento (prioridade: {priority})")
    
    def get_next_item(self, timeout: float = 5.0) -> Optional[QueueItem]:
        """
        Obtém o próximo item da fila.
        
        Args:
            timeout: Timeout em segundos para esperar um item
            
        Returns:
            QueueItem ou None se fila vazia/timeout
        """
        try:
            priority, timestamp, item = self._process_queue.get(timeout=timeout)
            return item
        except Empty:
            return None
    
    def start_worker(
        self, 
        process_function: Callable,
        path_zip: str, 
        path_unzip: str, 
        path_parquet: str,
        **kwargs
    ) -> None:
        """
        Inicia um worker para processar a fila.
        
        Args:
            process_function: Função que processa um único arquivo ZIP
            path_zip: Diretório com arquivos ZIP
            path_unzip: Diretório para extração  
            path_parquet: Diretório de saída
            **kwargs: Argumentos extras para a função de processamento
        """
        self._worker_count += 1
        worker_id = f"{self.processor_name}-Worker-{self._worker_count}"
        
        def worker_loop():
            self.logger.info(f"[{worker_id}] Worker iniciado para processamento de {self.processor_name}")
            
            # Registrar início do worker (se disponível)
            if progress_tracker:
                progress_tracker.start_worker(self.processor_name.lower(), worker_id)
            
            consecutive_empty_checks = 0
            max_empty_checks = 6  # 30 segundos antes de parar
            
            try:
                while not self._workers_should_stop.value:
                    try:
                        # Verificar se pode processar
                        if not self.can_start_processing():
                            resources = self.resource_monitor.get_system_resources()
                            self.logger.debug(
                                f"[{worker_id}] Recursos insuficientes - "
                                f"CPU: {resources.cpu_percent:.1f}%, "
                                f"Memória: {resources.memory_percent:.1f}%, "
                                f"Processos: {self.active_processes}/{self.max_processes}"
                            )
                            time.sleep(10)  # Esperar recursos liberarem
                            continue
                        
                        # Tentar obter próximo item
                        item = self.get_next_item(timeout=5.0)
                        
                        if item is None:
                            consecutive_empty_checks += 1
                            if consecutive_empty_checks >= max_empty_checks:
                                self.logger.info(f"[{worker_id}] Fila vazia por {max_empty_checks * 5}s. Finalizando worker.")
                                break
                            
                            self.logger.debug(f"[{worker_id}] Fila vazia, aguardando... ({consecutive_empty_checks}/{max_empty_checks})")
                            continue
                        
                        # Reset contador se encontrou trabalho
                        consecutive_empty_checks = 0
                        
                        # Processar item
                        success = self._process_item(
                            item, worker_id, process_function,
                            path_zip, path_unzip, path_parquet, **kwargs
                        )
                        
                        if success:
                            self.logger.info(f"[{worker_id}] ✓ {item.zip_file} processado com sucesso")
                        else:
                            self.logger.error(f"[{worker_id}] ✗ Falha ao processar {item.zip_file}")
                            
                    except Exception as e:
                        self.logger.error(f"[{worker_id}] Erro no loop do worker: {str(e)}")
                        if 'item' in locals() and item and progress_tracker:
                            progress_tracker.complete_file(self.processor_name.lower(), item.zip_file, False, worker_id)
                        time.sleep(5)  # Pausa antes de tentar novamente
                        
            except Exception as e:
                self.logger.error(f"[{worker_id}] Erro crítico no worker: {str(e)}")
            finally:
                if progress_tracker:
                    progress_tracker.end_worker(self.processor_name.lower(), worker_id)
                self.logger.info(f"[{worker_id}] Worker finalizado")
        
        # Iniciar thread do worker
        worker_thread = threading.Thread(target=worker_loop, name=worker_id, daemon=True)
        worker_thread.start()
        self._workers.append(worker_thread)
        
        self.logger.info(f"Worker {worker_id} iniciado")
    
    def _process_item(
        self,
        item: QueueItem,
        worker_id: str,
        process_function: Callable,
        path_zip: str,
        path_unzip: str, 
        path_parquet: str,
        **kwargs
    ) -> bool:
        """
        Processa um item da fila.
        
        Args:
            item: Item a ser processado
            worker_id: ID do worker
            process_function: Função de processamento
            path_zip, path_unzip, path_parquet: Diretórios
            **kwargs: Argumentos extras
            
        Returns:
            bool: True se sucesso, False se falha
        """
        import os
        
        # Importação condicional do cache
        try:
            from ...utils.processing_cache import processing_cache
        except ImportError:
            processing_cache = None
        
        zip_path = os.path.join(path_zip, item.zip_file)
        file_size = os.path.getsize(zip_path) if os.path.exists(zip_path) else 0
        file_mtime = int(os.path.getmtime(zip_path)) if os.path.exists(zip_path) else 0
        
        # Verificar cache (se disponível)
        if processing_cache and processing_cache.is_processed(item.zip_file, file_size, file_mtime):
            self.logger.warning(f"[{worker_id}] Arquivo {item.zip_file} já processado. Pulando.")
            return True
        
        # Incrementar contador de processos ativos
        with self._processing_lock:
            self._active_processes.value += 1
            self.logger.debug(f"[{worker_id}] Processos ativos: {self.active_processes}/{self.max_processes}")
        
        try:
            # Registrar início do processamento
            start_time = time.time()
            if progress_tracker:
                progress_tracker.start_file(self.processor_name.lower(), item.zip_file, worker_id)
            
            # Executar função de processamento
            # Combinar kwargs do item com kwargs fornecidos
            all_kwargs = {**kwargs, **(item.extra_data or {})}
            result = process_function(
                item.zip_file, path_zip, path_unzip, path_parquet, **all_kwargs
            )
            
            # Registrar conclusão
            elapsed_time = time.time() - start_time
            if progress_tracker:
                progress_tracker.complete_file(self.processor_name.lower(), item.zip_file, result, worker_id, elapsed_time)
            
            # Registrar estatísticas (se disponível)
            if global_stats:
                global_stats.add_processing_stat(
                    filename=item.zip_file,
                    file_type=self.processor_name.lower(),
                    size_bytes=file_size,
                    start_time=start_time,
                    end_time=time.time(),
                    success=result,
                    error=None if result else "Processamento falhou"
                )
            
            # Marcar no cache se sucesso (se disponível)
            if result and processing_cache:
                processing_cache.mark_completed(item.zip_file, file_size, file_mtime, path_parquet)
            
            return result
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            self.logger.error(f"[{worker_id}] Erro ao processar {item.zip_file}: {str(e)}")
            
            # Registrar erro
            if progress_tracker:
                progress_tracker.complete_file(self.processor_name.lower(), item.zip_file, False, worker_id, elapsed_time)
            if global_stats:
                global_stats.add_processing_stat(
                    filename=item.zip_file,
                    file_type=self.processor_name.lower(),
                    size_bytes=file_size,
                    start_time=start_time,
                    end_time=time.time(),
                    success=False,
                    error=str(e)
                )
            
            return False
            
        finally:
            # Decrementar contador de processos ativos
            with self._processing_lock:
                self._active_processes.value -= 1
                self.logger.debug(f"[{worker_id}] Processo finalizado. Ativos: {self.active_processes}/{self.max_processes}")
    
    def start_multiple_workers(
        self,
        num_workers: int,
        process_function: Callable,
        path_zip: str,
        path_unzip: str,
        path_parquet: str,
        **kwargs
    ) -> None:
        """
        Inicia múltiplos workers.
        
        Args:
            num_workers: Número de workers a iniciar
            process_function: Função de processamento
            path_zip, path_unzip, path_parquet: Diretórios
            **kwargs: Argumentos extras
        """
        if num_workers <= 0:
            num_workers = self.max_processes
        
        self.logger.info(f"Iniciando {num_workers} workers para {self.processor_name}")
        
        for i in range(num_workers):
            self.start_worker(process_function, path_zip, path_unzip, path_parquet, **kwargs)
    
    def stop_all_workers(self, timeout: float = 30.0) -> None:
        """
        Para todos os workers.
        
        Args:
            timeout: Timeout em segundos para aguardar finalização
        """
        self.logger.info(f"Parando todos os workers de {self.processor_name}")
        
        # Sinalizar para workers pararem
        self._workers_should_stop.value = True
        
        # Aguardar finalização dos workers
        start_time = time.time()
        for worker_thread in self._workers:
            remaining_time = timeout - (time.time() - start_time)
            if remaining_time > 0:
                worker_thread.join(timeout=remaining_time)
            
            if worker_thread.is_alive():
                self.logger.warning(f"Worker {worker_thread.name} não finalizou no tempo esperado")
        
        self.logger.info(f"Todos os workers de {self.processor_name} foram finalizados")
    
    def wait_for_completion(self, check_interval: float = 5.0) -> None:
        """
        Aguarda até que toda a fila seja processada.
        
        Args:
            check_interval: Intervalo de verificação em segundos
        """
        self.logger.info(f"Aguardando conclusão do processamento de {self.processor_name}")
        
        while True:
            # Verificar se fila está vazia e nenhum processo ativo
            if self.queue_size == 0 and self.active_processes == 0:
                self.logger.info(f"Processamento de {self.processor_name} concluído")
                break
            
            self.logger.debug(f"Aguardando... Fila: {self.queue_size}, Ativos: {self.active_processes}")
            time.sleep(check_interval)
    
    def get_status(self) -> Dict[str, Any]:
        """
        Retorna status atual do gerenciador.
        
        Returns:
            Dict com informações de status
        """
        resources = self.resource_monitor.get_system_resources()
        
        return {
            'processor_name': self.processor_name,
            'queue_size': self.queue_size,
            'active_processes': self.active_processes,
            'max_processes': self.max_processes,
            'workers_running': len([w for w in self._workers if w.is_alive()]),
            'workers_should_stop': self._workers_should_stop.value,
            'can_process': self.can_start_processing(),
            'system_resources': {
                'cpu_percent': resources.cpu_percent,
                'memory_percent': resources.memory_percent,
                'disk_percent': resources.disk_percent,
                'memory_available_gb': resources.memory_available_gb
            }
        } 