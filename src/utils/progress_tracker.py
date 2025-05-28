"""
Sistema de rastreamento de progresso unificado para o pipeline CNPJ.
Fornece barras de progresso visuais que mostram "X de Y arquivos" para cada módulo.
"""

import logging
import threading
import time
from typing import Dict, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
import os

logger = logging.getLogger(__name__)

@dataclass
class ProgressInfo:
    """Informações de progresso para um módulo específico."""
    module_name: str
    total_files: int
    completed_files: int = 0
    failed_files: int = 0
    current_file: Optional[str] = None
    start_time: Optional[datetime] = None
    workers_active: int = 0
    workers_total: int = 0
    last_update: Optional[datetime] = field(default_factory=datetime.now)
    
    @property
    def progress_percent(self) -> float:
        """Calcula a porcentagem de progresso."""
        if self.total_files == 0:
            return 0.0
        return (self.completed_files / self.total_files) * 100
    
    @property
    def success_rate(self) -> float:
        """Calcula a taxa de sucesso."""
        processed = self.completed_files + self.failed_files
        if processed == 0:
            return 100.0
        return (self.completed_files / processed) * 100
    
    @property
    def elapsed_time(self) -> float:
        """Calcula o tempo decorrido em segundos."""
        if self.start_time is None:
            return 0.0
        return (datetime.now() - self.start_time).total_seconds()
    
    @property
    def estimated_remaining_time(self) -> float:
        """Estima o tempo restante em segundos."""
        if self.completed_files == 0 or self.elapsed_time == 0:
            return 0.0
        
        avg_time_per_file = self.elapsed_time / self.completed_files
        remaining_files = self.total_files - self.completed_files
        return avg_time_per_file * remaining_files


class ProgressTracker:
    """
    Rastreador de progresso centralizado para todos os módulos do pipeline CNPJ.
    
    Fornece barras de progresso visuais e logs estruturados mostrando:
    - X de Y arquivos processados
    - Workers ativos
    - Tempo estimado restante
    - Taxa de sucesso
    """
    
    def __init__(self):
        self._progress_data: Dict[str, ProgressInfo] = {}
        self._lock = threading.Lock()
        self._display_thread: Optional[threading.Thread] = None
        self._should_stop = threading.Event()
        self._update_interval = 5.0  # segundos
        self._last_display_time = 0
        
    def register_module(self, module_name: str, total_files: int, workers_total: int = 1) -> None:
        """
        Registra um novo módulo para rastreamento de progresso.
        
        Args:
            module_name: Nome do módulo (ex: 'empresas', 'estabelecimentos')
            total_files: Número total de arquivos a serem processados
            workers_total: Número total de workers que serão usados
        """
        with self._lock:
            self._progress_data[module_name] = ProgressInfo(
                module_name=module_name,
                total_files=total_files,
                workers_total=workers_total,
                start_time=datetime.now()
            )
            
        logger.info(f"📊 {module_name.upper()}: Registrado para rastreamento - {total_files} arquivos, {workers_total} workers")
    
    def start_worker(self, module_name: str, worker_id: str = None) -> None:
        """
        Marca o início de um worker.
        
        Args:
            module_name: Nome do módulo
            worker_id: ID opcional do worker para logs
        """
        with self._lock:
            if module_name in self._progress_data:
                self._progress_data[module_name].workers_active += 1
                self._progress_data[module_name].last_update = datetime.now()
                
        worker_info = f" (Worker: {worker_id})" if worker_id else ""
        self._log_progress_update(module_name, f"🔧 Worker iniciado{worker_info}")
    
    def stop_worker(self, module_name: str, worker_id: str = None) -> None:
        """
        Marca o fim de um worker.
        
        Args:
            module_name: Nome do módulo
            worker_id: ID opcional do worker para logs
        """
        with self._lock:
            if module_name in self._progress_data:
                self._progress_data[module_name].workers_active = max(0, 
                    self._progress_data[module_name].workers_active - 1)
                self._progress_data[module_name].last_update = datetime.now()
                
        worker_info = f" (Worker: {worker_id})" if worker_id else ""
        self._log_progress_update(module_name, f"🏁 Worker finalizado{worker_info}")
    
    def start_file(self, module_name: str, filename: str, worker_id: str = None) -> None:
        """
        Marca o início do processamento de um arquivo.
        
        Args:
            module_name: Nome do módulo
            filename: Nome do arquivo sendo processado
            worker_id: ID opcional do worker
        """
        with self._lock:
            if module_name in self._progress_data:
                self._progress_data[module_name].current_file = filename
                self._progress_data[module_name].last_update = datetime.now()
                
        worker_info = f" (Worker: {worker_id})" if worker_id else ""
        self._log_progress_update(module_name, f"🔄 Iniciando: {filename}{worker_info}")
    
    def complete_file(self, module_name: str, filename: str, success: bool = True, 
                     worker_id: str = None, processing_time: float = None) -> None:
        """
        Marca a conclusão do processamento de um arquivo.
        
        Args:
            module_name: Nome do módulo
            filename: Nome do arquivo processado
            success: Se o processamento foi bem-sucedido
            worker_id: ID opcional do worker
            processing_time: Tempo de processamento em segundos
        """
        with self._lock:
            if module_name in self._progress_data:
                progress = self._progress_data[module_name]
                if success:
                    progress.completed_files += 1
                else:
                    progress.failed_files += 1
                progress.current_file = None
                progress.last_update = datetime.now()
                
        # Preparar informações para log
        status_emoji = "✅" if success else "❌"
        status_text = "sucesso" if success else "falha"
        worker_info = f" (Worker: {worker_id})" if worker_id else ""
        time_info = f" em {processing_time:.2f}s" if processing_time else ""
        
        self._log_progress_update(module_name, 
            f"{status_emoji} {filename}: {status_text}{time_info}{worker_info}")
    
    def _log_progress_update(self, module_name: str, message: str) -> None:
        """
        Registra uma atualização de progresso com informações contextuais.
        
        Args:
            module_name: Nome do módulo
            message: Mensagem específica do evento
        """
        with self._lock:
            if module_name not in self._progress_data:
                return
                
            progress = self._progress_data[module_name]
            
            # Calcular informações de progresso
            completed = progress.completed_files
            failed = progress.failed_files
            total = progress.total_files
            processed = completed + failed
            percent = progress.progress_percent
            success_rate = progress.success_rate
            workers_active = progress.workers_active
            workers_total = progress.workers_total
            
            # Estimar tempo restante
            remaining_time = progress.estimated_remaining_time
            remaining_str = ""
            if remaining_time > 0 and processed > 0:
                if remaining_time < 60:
                    remaining_str = f" | ETA: {remaining_time:.0f}s"
                elif remaining_time < 3600:
                    remaining_str = f" | ETA: {remaining_time/60:.1f}min"
                else:
                    remaining_str = f" | ETA: {remaining_time/3600:.1f}h"
            
            # Informações de workers
            worker_info = ""
            if workers_total > 1:
                worker_info = f" | Workers: {workers_active}/{workers_total}"
            
            # Informações de taxa de sucesso (só mostrar se houver falhas)
            success_info = ""
            if failed > 0:
                success_info = f" | Taxa sucesso: {success_rate:.1f}%"
            
            # Log principal com barra de progresso
            progress_bar = self._create_progress_bar(percent)
            logger.info(f"📊 {module_name.upper()}: [{processed}/{total}] {progress_bar} {percent:.1f}%{worker_info}{remaining_str}{success_info}")
            
            # Log da mensagem específica do evento
            logger.info(f"   {message}")
    
    def _create_progress_bar(self, percent: float, width: int = 20) -> str:
        """
        Cria uma barra de progresso visual.
        
        Args:
            percent: Porcentagem de progresso (0-100)
            width: Largura da barra em caracteres
            
        Returns:
            String representando a barra de progresso
        """
        filled = int(width * percent / 100)
        bar = "█" * filled + "░" * (width - filled)
        return f"{bar}"
    
    def get_progress_summary(self, module_name: str) -> Optional[Dict]:
        """
        Obtém um resumo do progresso de um módulo.
        
        Args:
            module_name: Nome do módulo
            
        Returns:
            Dicionário com informações de progresso ou None se módulo não existir
        """
        with self._lock:
            if module_name not in self._progress_data:
                return None
                
            progress = self._progress_data[module_name]
            return {
                'module_name': progress.module_name,
                'total_files': progress.total_files,
                'completed_files': progress.completed_files,
                'failed_files': progress.failed_files,
                'progress_percent': progress.progress_percent,
                'success_rate': progress.success_rate,
                'elapsed_time': progress.elapsed_time,
                'estimated_remaining_time': progress.estimated_remaining_time,
                'workers_active': progress.workers_active,
                'workers_total': progress.workers_total,
                'current_file': progress.current_file
            }
    
    def get_all_progress(self) -> Dict[str, Dict]:
        """
        Obtém o progresso de todos os módulos registrados.
        
        Returns:
            Dicionário com progresso de todos os módulos
        """
        with self._lock:
            return {name: self.get_progress_summary(name) 
                   for name in self._progress_data.keys()}
    
    def print_final_summary(self, module_name: str) -> None:
        """
        Imprime um resumo final do processamento de um módulo.
        
        Args:
            module_name: Nome do módulo
        """
        summary = self.get_progress_summary(module_name)
        if not summary:
            return
            
        logger.info("=" * 60)
        logger.info(f"📋 RESUMO FINAL - {module_name.upper()}")
        logger.info("=" * 60)
        logger.info(f"✅ Arquivos processados com sucesso: {summary['completed_files']}")
        logger.info(f"❌ Arquivos com falha: {summary['failed_files']}")
        logger.info(f"📊 Total de arquivos: {summary['total_files']}")
        logger.info(f"📈 Taxa de sucesso: {summary['success_rate']:.1f}%")
        logger.info(f"⏱️  Tempo total de processamento: {summary['elapsed_time']:.1f}s")
        
        if summary['completed_files'] > 0:
            avg_time = summary['elapsed_time'] / summary['completed_files']
            logger.info(f"⚡ Tempo médio por arquivo: {avg_time:.2f}s")
            
        logger.info("=" * 60)
    
    def cleanup(self, module_name: str = None) -> None:
        """
        Limpa os dados de progresso de um módulo específico ou todos.
        
        Args:
            module_name: Nome do módulo para limpar, ou None para limpar todos
        """
        with self._lock:
            if module_name:
                self._progress_data.pop(module_name, None)
                logger.debug(f"Dados de progresso limpos para módulo: {module_name}")
            else:
                self._progress_data.clear()
                logger.debug("Todos os dados de progresso foram limpos")


# Instância global do rastreador de progresso
progress_tracker = ProgressTracker()


def format_time_duration(seconds: float) -> str:
    """
    Formata uma duração em segundos para uma string legível.
    
    Args:
        seconds: Duração em segundos
        
    Returns:
        String formatada (ex: "2h 30m 45s", "5m 30s", "45s")
    """
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours}h {minutes}m {secs}s" 