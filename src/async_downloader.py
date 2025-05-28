import asyncio
import datetime
import gc
import logging
import os
import re
import time
from typing import List, Tuple
from urllib.parse import urljoin

# Bibliotecas de terceiros
import aiofiles
import aiohttp
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
)
import psutil  # Adicionar import do psutil

# Importações locais do projeto
from src.config import config
from src.utils.statistics import global_stats
from .process.empresa import process_single_zip as process_empresa_zip
from .process.estabelecimento import process_single_zip as process_estabelecimento_zip
from .process.simples import process_single_zip as process_simples_zip
from .process.socio import process_single_zip as process_socio_zip
from .utils.time_utils import format_elapsed_time
from .utils.statistics import global_stats
from .utils.cache import DownloadCache  # Corrigir import do cache

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Instanciar console do Rich
console = Console()

# Instanciar o cache
# TODO: Considerar injetar a instância ao invés de criar globalmente, se necessário
download_cache = None  # Será inicializado quando necessário

def get_download_cache():
    """Obtém a instância do cache de downloads."""
    global download_cache
    # Sempre recriar o cache se o caminho mudou ou se não existe
    current_cache_path = config.cache.cache_path
    if download_cache is None or download_cache.cache_path != current_cache_path:
        download_cache = DownloadCache(current_cache_path)
        logger.debug(f"Cache inicializado/atualizado para: {current_cache_path}")
    return download_cache

logger = logging.getLogger(__name__)

# Mapeamento de tipos de arquivo para funções de processamento
PROCESSOR_MAP = {
    'Empresas': process_empresa_zip,
    'Estabelecimentos': process_estabelecimento_zip,
    'Simples': process_simples_zip,
    'Socios': process_socio_zip
}

# ===== OTIMIZAÇÕES DE PIPELINE =====

# Configurações adaptativas baseadas nos recursos do sistema
def get_optimal_concurrency():
    """Calcula a concorrência ótima baseada nos recursos do sistema."""
    cpu_count = os.cpu_count() or 4
    memory_gb = psutil.virtual_memory().total / (1024**3)
    
    # Algoritmo adaptativo para concorrência
    if memory_gb >= 16:
        # Sistema com muita RAM - pode processar mais arquivos simultaneamente
        download_workers = min(8, cpu_count)
        process_workers = min(4, cpu_count // 2)
    elif memory_gb >= 8:
        # Sistema com RAM moderada
        download_workers = min(6, cpu_count)
        process_workers = min(3, cpu_count // 2)
    else:
        # Sistema com pouca RAM - ser mais conservador
        download_workers = min(4, cpu_count)
        process_workers = min(2, cpu_count // 3)
    
    logger.info(f"Configuração adaptativa: {memory_gb:.1f}GB RAM, {cpu_count} CPUs")
    logger.info(f"Workers de download: {download_workers}, Workers de processamento: {process_workers}")
    
    return download_workers, process_workers

# Cache inteligente para resultados intermediários
class ProcessingCache:
    """Cache inteligente para otimizar reprocessamento."""
    
    def __init__(self):
        self.cache_dir = os.path.join(os.getenv('PATH_ZIP', 'data'), '.processing_cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_file = os.path.join(self.cache_dir, 'processing_cache.json')
        self._load_cache()
    
    def _load_cache(self):
        """Carrega cache do disco."""
        try:
            if os.path.exists(self.cache_file):
                import json
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
            else:
                self.cache = {}
        except Exception as e:
            logger.warning(f"Erro ao carregar cache de processamento: {e}")
            self.cache = {}
    
    def _save_cache(self):
        """Salva cache no disco."""
        try:
            import json
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            logger.warning(f"Erro ao salvar cache de processamento: {e}")
    
    def is_processed(self, filename: str, file_size: int, file_mtime: int) -> bool:
        """Verifica se arquivo já foi processado com sucesso."""
        key = f"{filename}_{file_size}_{file_mtime}"
        return key in self.cache and self.cache[key].get('status') == 'completed'
    
    def mark_completed(self, filename: str, file_size: int, file_mtime: int, output_path: str):
        """Marca arquivo como processado com sucesso."""
        key = f"{filename}_{file_size}_{file_mtime}"
        self.cache[key] = {
            'status': 'completed',
            'output_path': output_path,
            'timestamp': time.time()
        }
        self._save_cache()

# Instância global do cache de processamento
processing_cache = ProcessingCache()

# Semáforos adaptativos
download_workers, process_workers = get_optimal_concurrency()
download_semaphore = asyncio.Semaphore(download_workers)
process_semaphore = asyncio.Semaphore(process_workers)

# ===== STREAMING E PIPELINE OTIMIZADO =====

class StreamingProcessor:
    """Processador com streaming otimizado para grandes volumes de dados."""
    
    def __init__(self, max_memory_mb: int = 2048):
        self.max_memory_mb = max_memory_mb
        self.active_streams = {}
        self.processing_queue = asyncio.Queue(maxsize=20)  # Limitar fila para controlar memória
    
    async def process_with_streaming(self, file_path: str, filename: str, path_unzip: str, path_parquet: str):
        """Processa arquivo usando streaming otimizado."""
        try:
            # Verificar cache primeiro
            file_stat = os.stat(file_path)
            if processing_cache.is_processed(filename, file_stat.st_size, int(file_stat.st_mtime)):
                cached_output = processing_cache.cache[f"{filename}_{file_stat.st_size}_{int(file_stat.st_mtime)}"]["output_path"]
                if os.path.exists(cached_output):
                    logger.info(f"Arquivo {filename} já processado (cache). Pulando.")
                    return cached_output, None
            
            # Monitorar uso de memória durante processamento
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024  # MB
            
            # Processamento com monitoramento de recursos
            logger.info(f"Iniciando processamento streaming de {filename}")
            
            # Determinar tipo de arquivo e processador
            processor_fn = None
            file_type = None
            for tipo_prefixo, processor in PROCESSOR_MAP.items():
                if filename.lower().startswith(tipo_prefixo.lower()):
                    processor_fn = processor
                    file_type = tipo_prefixo
                    break
            
            if not processor_fn:
                raise ValueError(f"Tipo de arquivo não reconhecido: {filename}")
            
            # Extrair pasta remota
            remote_folder = os.path.basename(os.path.dirname(file_path))
            if not re.match(r'^\d{4}-\d{2}$', remote_folder):
                match = re.search(r'(20\d{2}-\d{2})', file_path)
                remote_folder = match.group(1) if match else "dados"
            
            # Executar processamento com argumentos otimizados
            kwargs = {
                "zip_file": filename,
                "path_zip": os.path.dirname(file_path),
                "path_unzip": path_unzip,
                "path_parquet": path_parquet,
            }
            
            # Adicionar argumentos específicos
            if "remote_folder" in processor_fn.__code__.co_varnames:
                kwargs["remote_folder"] = remote_folder
            if "create_private" in processor_fn.__code__.co_varnames:
                kwargs["create_private"] = False
            
            # Executar em thread separada para não bloquear event loop
            loop = asyncio.get_event_loop()
            
            # Criar função wrapper para passar argumentos corretamente
            def run_processor():
                return processor_fn(**kwargs)
            
            result = await loop.run_in_executor(None, run_processor)
            
            # Verificar uso de memória após processamento
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_used = final_memory - initial_memory
            
            if memory_used > self.max_memory_mb:
                logger.warning(f"Processamento de {filename} usou {memory_used:.1f}MB (limite: {self.max_memory_mb}MB)")
                # Forçar coleta de lixo
                gc.collect()
            
            if result:
                # Construir caminho do parquet
                if remote_folder:
                    parquet_path = os.path.join(path_parquet, remote_folder, file_type.lower())
                else:
                    parquet_path = os.path.join(path_parquet, file_type.lower())
                
                # Marcar como processado no cache
                processing_cache.mark_completed(filename, file_stat.st_size, int(file_stat.st_mtime), parquet_path)
                
                logger.info(f"Processamento streaming de {filename} concluído. Memória usada: {memory_used:.1f}MB")
                return parquet_path, None
            else:
                raise ValueError(f"Processamento falhou para {filename}")
                
        except Exception as e:
            logger.error(f"Erro no processamento streaming de {filename}: {e}")
            return file_path, e

# Instância global do processador streaming
streaming_processor = StreamingProcessor()

# ===== SISTEMA DE MONITORAMENTO DE RECURSOS =====

class ResourceMonitor:
    """Monitor de recursos do sistema com controle adaptativo."""
    
    def __init__(self):
        self.monitoring = False
        self.max_download_workers = 8
        self.max_processing_workers = 4
        self.current_download_workers = 4
        self.current_processing_workers = 2
        self.adjustments_made = 0
        self.peak_cpu = 0.0
        self.peak_memory = 0.0
        self.avg_cpu = 0.0
        self.avg_memory = 0.0
        self._last_status_log = 0
        self._cpu_samples = []
        self._memory_samples = []
    
    async def start_monitoring(self):
        """Inicia o monitoramento de recursos."""
        self.monitoring = True
        logger.info("Iniciando monitoramento de recursos...")
        
        while self.monitoring:
            try:
                # Obter uso de CPU e memória
                cpu_percent = psutil.cpu_percent(interval=1)
                memory_percent = psutil.virtual_memory().percent
                
                # Atualizar estatísticas
                self._cpu_samples.append(cpu_percent)
                self._memory_samples.append(memory_percent)
                self.peak_cpu = max(self.peak_cpu, cpu_percent)
                self.peak_memory = max(self.peak_memory, memory_percent)
                
                # Calcular médias
                if self._cpu_samples:
                    self.avg_cpu = sum(self._cpu_samples) / len(self._cpu_samples)
                    self.avg_memory = sum(self._memory_samples) / len(self._memory_samples)
                
                # Ajustar concorrência se necessário
                await self._adjust_concurrency_if_needed(cpu_percent, memory_percent)
                
                # Aguardar próxima verificação
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Erro no monitoramento de recursos: {e}")
                await asyncio.sleep(10)
    
    def stop_monitoring(self):
        """Para o monitoramento de recursos."""
        self.monitoring = False
        logger.info("Monitoramento de recursos parado")

    async def _adjust_concurrency_if_needed(self, cpu_percent: float, memory_percent: float):
        """Ajusta a concorrência baseado no uso de recursos com feedback detalhado."""
        current_downloads = self.current_download_workers
        current_processing = self.current_processing_workers
        
        # Thresholds mais conservadores
        high_cpu_threshold = 85.0
        high_memory_threshold = 85.0
        low_cpu_threshold = 60.0
        low_memory_threshold = 60.0
        
        # Verificar se recursos estão sobrecarregados
        resources_overloaded = cpu_percent > high_cpu_threshold or memory_percent > high_memory_threshold
        resources_available = cpu_percent < low_cpu_threshold and memory_percent < low_memory_threshold
        
        if resources_overloaded:
            # Reduzir workers se recursos estão sobrecarregados
            if current_processing > 1:
                new_processing = max(1, current_processing - 1)
                self.current_processing_workers = new_processing
                logger.warning(f"Recursos sobrecarregados (CPU: {cpu_percent:.1f}%, RAM: {memory_percent:.1f}%). "
                             f"Reduzindo workers de processamento: {current_processing} → {new_processing}")
                logger.info(f"Impacto: Processamento mais lento, mas sistema mais estável")
                
            elif current_downloads > 1:
                new_downloads = max(1, current_downloads - 1)
                self.current_download_workers = new_downloads
                logger.warning(f"Recursos sobrecarregados (CPU: {cpu_percent:.1f}%, RAM: {memory_percent:.1f}%). "
                             f"Reduzindo workers de download: {current_downloads} → {new_downloads}")
                logger.info(f"Impacto: Downloads mais lentos, mas menos pressão no sistema")
            else:
                logger.error(f"SISTEMA CRÍTICO: Recursos sobrecarregados (CPU: {cpu_percent:.1f}%, RAM: {memory_percent:.1f}%) "
                           f"mas já estamos no mínimo de workers. Sistema pode ficar instável!")
                
        elif resources_available:
            # Aumentar workers se recursos estão disponíveis
            max_downloads = min(12, self.max_download_workers)
            max_processing = min(6, self.max_processing_workers)
            
            if current_downloads < max_downloads:
                new_downloads = min(max_downloads, current_downloads + 1)
                self.current_download_workers = new_downloads
                logger.info(f"Recursos disponíveis (CPU: {cpu_percent:.1f}%, RAM: {memory_percent:.1f}%). "
                          f"Aumentando workers de download: {current_downloads} → {new_downloads}")
                
            elif current_processing < max_processing:
                new_processing = min(max_processing, current_processing + 1)
                self.current_processing_workers = new_processing
                logger.info(f"Recursos disponíveis (CPU: {cpu_percent:.1f}%, RAM: {memory_percent:.1f}%). "
                          f"Aumentando workers de processamento: {current_processing} → {new_processing}")
        
        # Log de status periódico (a cada 30 segundos)
        if hasattr(self, '_last_status_log'):
            if time.time() - self._last_status_log > 30:
                self._log_resource_status(cpu_percent, memory_percent)
                self._last_status_log = time.time()
        else:
            self._last_status_log = time.time()
    
    def _log_resource_status(self, cpu_percent: float, memory_percent: float):
        """Log detalhado do status dos recursos."""
        status_emoji = "OK" if cpu_percent < 70 and memory_percent < 70 else "WARN" if cpu_percent < 85 and memory_percent < 85 else "CRIT"
        
        logger.info(f"[{status_emoji}] Status do Sistema: CPU {cpu_percent:.1f}% | RAM {memory_percent:.1f}% | "
                   f"Downloads: {self.current_download_workers} | Processamento: {self.current_processing_workers}")
        
        if cpu_percent > 90 or memory_percent > 90:
            logger.warning(f"Sistema sob alta pressão! Considere reduzir a carga de trabalho.")
        elif cpu_percent < 50 and memory_percent < 50:
            logger.info(f"Sistema com recursos abundantes. Pipeline otimizado!")
    
    def get_resource_impact_summary(self) -> dict:
        """Retorna um resumo do impacto das mudanças de recursos."""
        return {
            'adjustments_made': self.adjustments_made,
            'peak_cpu': self.peak_cpu,
            'peak_memory': self.peak_memory,
            'avg_cpu': self.avg_cpu,
            'avg_memory': self.avg_memory,
            'current_downloads': self.current_download_workers,
            'current_processing': self.current_processing_workers,
            'system_stable': self.peak_cpu < 90 and self.peak_memory < 90
        }

# Instância global do monitor de recursos
resource_monitor = ResourceMonitor()

# ===== OTIMIZAÇÕES NO PIPELINE PRINCIPAL =====

def _fetch_and_parse(url: str) -> BeautifulSoup | None:
    """Busca HTML de uma URL e parseia com BeautifulSoup."""
    try:
        logger.debug(f"Fetching: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Verifica erros HTTP
        return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        logger.error(f"Erro ao buscar {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao parsear {url}: {e}")
        return None


def _find_links(soup: BeautifulSoup, base_url: str, ends_with: str | None = None) -> List[str]:
    """Encontra links em um objeto BeautifulSoup que terminam com um sufixo específico."""
    found_urls = []
    if not soup:
        return found_urls

    for link in soup.find_all('a'):
        href = link.get('href')
        if not href or href.startswith('?') or href == '../':
            continue

        # Verifica se o link termina com o sufixo desejado (case-insensitive)
        if ends_with and href.lower().endswith(ends_with.lower()):
            full_url = urljoin(base_url, href)  # Constrói URL completa
            if full_url not in found_urls:
                found_urls.append(full_url)
        # Se não busca por sufixo, pega todos os links válidos (usado para diretórios)
        elif ends_with is None and href.endswith('/'):  # Considera apenas diretórios se ends_with não for especificado
            full_url = urljoin(base_url, href)
            if full_url not in found_urls:
                found_urls.append(full_url)

    return found_urls


def _filter_urls_by_type(urls: List[str], tipos: Tuple[str, ...]) -> Tuple[List[str], int]:
    """Filtra uma lista de URLs, mantendo apenas aquelas cujo nome de arquivo começa com um dos tipos fornecidos."""
    filtered_urls = []
    ignored_count = 0
    for url in urls:
        filename = url.split('/')[-1]
        filename_lower = filename.lower()
        
        # Primeiro verificar se o arquivo está na lista de ignorados
        should_ignore = False
        for ignored in config.ignored_files:
            if filename_lower.startswith(ignored.lower()):
                logger.debug(f"Ignorando arquivo de referência: {filename}")
                should_ignore = True
                ignored_count += 1
                break
        
        if should_ignore:
            continue
            
        # Se não está na lista de ignorados, verificar se é um dos tipos desejados
        match_found = False
        for tipo in tipos:
            tipo_lower = tipo.lower()
            if filename_lower.startswith(tipo_lower):
                match_found = True
                filtered_urls.append(url)
                break
        
        if not match_found:
            logger.debug(f"Ignorando URL (tipo não desejado): {filename}")
            ignored_count += 1
            
    return filtered_urls, ignored_count


def get_latest_month_zip_urls(base_url: str, remote_folder: str | None = None) -> Tuple[List[str], str]:
    """Busca URLs de arquivos .zip na pasta AAAA-MM mais recente ou na pasta especificada.

    1. Busca e parseia a URL base.
    2. Encontra links de diretórios.
    3. Filtra diretórios no formato AAAA-MM e encontra o mais recente ou usa o especificado.
    4. Busca e parseia a URL do diretório escolhido.
    5. Encontra links .zip nesse diretório.
    
    Args:
        base_url: URL base para buscar diretórios
        remote_folder: Pasta específica a ser usada (formato AAAA-MM), se None usa a mais recente

    Returns:
        Tuple[List[str], str]: Lista de URLs e nome da pasta escolhida
    """
    zip_urls = []
    folder_url = None
    year_month_folders = []

    # 1 & 2: Buscar e encontrar links de diretórios na URL base
    logger.info(f"Buscando diretórios em: {base_url}")
    base_soup = _fetch_and_parse(base_url)
    if not base_soup:
        return [], ""

    directory_links = _find_links(base_soup, base_url,
                                  ends_with=None)  # ends_with=None busca diretórios terminados em /

    # 3: Filtrar diretórios AAAA-MM e encontrar o mais recente ou o especificado
    for dir_url in directory_links:
        folder_name = dir_url.strip('/').split('/')[-1]
        match = re.fullmatch(r'(\d{4})-(\d{2})', folder_name)
        if match:
            # Se existe um remote_folder específico e esse é o diretório
            if remote_folder and folder_name == remote_folder:
                folder_url = dir_url
                logger.info(f"Diretório especificado encontrado: {folder_name} ({folder_url})")
                break
            
            year_month_folders.append((folder_name, dir_url))
            logger.debug(f"Encontrado diretório AAAA-MM: {folder_name} -> {dir_url}")

    # Se não temos um folder_url específico (porque remote_folder não foi encontrado ou não foi especificado)
    if not folder_url:
        if remote_folder:
            # Se o usuário solicitou uma pasta específica, mas não foi encontrada
            logger.warning(f"Diretório solicitado '{remote_folder}' não foi encontrado.")
            return [], ""
            
        if not year_month_folders:
            logger.warning(f"Nenhum diretório no formato AAAA-MM encontrado em {base_url}")
            return [], ""

        # Ordena pela chave (nome da pasta AAAA-MM) para encontrar o mais recente
        year_month_folders.sort(key=lambda x: x[0], reverse=True)
        folder_name, folder_url = year_month_folders[0]
        logger.info(f"Diretório mais recente encontrado: {folder_name} ({folder_url})")
    else:
        # Usamos a pasta específica encontrada
        folder_name = folder_url.strip('/').split('/')[-1]

    # 4 & 5: Buscar e encontrar links .zip no diretório escolhido
    logger.info(f"Buscando arquivos .zip em: {folder_url}")
    folder_soup = _fetch_and_parse(folder_url)
    if not folder_soup:
        return [], folder_name  # Erro já logado

    zip_urls = _find_links(folder_soup, folder_url, ends_with='.zip')

    if not zip_urls:
        logger.warning(f"Nenhum arquivo .zip encontrado em {folder_url}")

    logger.info(f"Total de {len(zip_urls)} URLs .zip encontradas na pasta {folder_name}. ")
    return zip_urls, folder_name  # Retorna URLs e nome da pasta


async def _process_download_response(response: aiohttp.ClientResponse, destination_path: str, file_mode: str,
                                     progress: Progress, task_id: int, expected_size: int, initial_size: int,
                                     filename: str, remote_last_modified: int | None):
    """Processa o corpo da resposta HTTP e escreve no arquivo, atualizando a barra Rich."""
    downloaded_size_since_start = 0
    try:
        # Verifica o tamanho real do conteúdo a ser baixado (útil se for 200 OK em vez de 206)
        content_length_header = response.headers.get('Content-Length')
        if content_length_header is not None:
            size_to_download = int(content_length_header)
            # Ajusta o total na barra Rich
            progress.update(task_id, total=size_to_download, completed=0)
            initial_size = 0
        else:
            size_to_download = None

        async with aiofiles.open(destination_path, mode=file_mode) as f:
            # Inicia a tarefa na barra de progresso se ainda não estiver iniciada
            progress.start_task(task_id)
            async for chunk in response.content.iter_chunked(8192):
                await f.write(chunk)
                chunk_len = len(chunk)
                downloaded_size_since_start += chunk_len
                # Avança a barra Rich
                progress.update(task_id, advance=chunk_len)

        # Verificação final de tamanho
        final_local_size = initial_size + downloaded_size_since_start
        current_task = progress.tasks[task_id]

        if current_task.total and final_local_size < current_task.total:
            logger.warning(
                f"Download de {filename} parece incompleto. Esperado: {current_task.total}, Obtido: {final_local_size}")
            # Marca a barra como erro (vermelho)
            progress.update(task_id, description=f"[red]{filename[:30]} (incompleto)[/red]", style="red")
            
            # Registra o erro no cache
            if config.cache.enabled:
                error_msg = f"Download incompleto: Esperado {current_task.total}, Obtido {final_local_size}"
                get_download_cache().register_file_error(filename, error_msg)

        elif current_task.total and final_local_size > current_task.total:
            logger.warning(
                f"Download de {filename} maior que o esperado? Esperado: {current_task.total}, Obtido: {final_local_size}")
            # Marca a barra como aviso (amarelo)
            progress.update(task_id, description=f"[yellow]{filename[:30]} (tamanho>esperado)[/yellow]", style="yellow")
            
            # Registra o aviso no cache
            if config.cache.enabled:
                error_msg = f"Tamanho maior que o esperado: Esperado {current_task.total}, Obtido {final_local_size}"
                get_download_cache().register_file_error(filename, error_msg)

        # Garante que a barra chegue a 100% se o total era conhecido e foi atingido
        # Rich faz isso automaticamente se advance cobrir o total.

        # Marca a barra como concluída (verde)
        progress.update(task_id, description=f"[green]{filename[:30]} (concluído)[/green]", style="green")
        logger.info(
            f"Escrita do arquivo {filename} concluída (modo: {file_mode}). Tamanho final local: {final_local_size}")

        # 4. Atualizar data de modificação local e cache
        if remote_last_modified is not None:
            try:
                os.utime(destination_path, (time.time(), remote_last_modified))
            except Exception as e_utime:
                logger.warning(f"Não foi possível definir data de modificação para {filename}: {e_utime}")

        if config.cache.enabled:
            if final_local_size == expected_size:
                # Atualiza o cache silenciosamente (sem log duplicado)
                get_download_cache().update_file_cache(filename, expected_size, remote_last_modified, log_update=False)
                logger.debug(f"Cache atualizado para {filename}")
            else:
                error_msg = f"Tamanho final ({final_local_size}) difere do remoto esperado ({expected_size})"
                logger.error(f"Arquivo {filename}: {error_msg}. Cache não atualizado.")
                # Registra o erro no cache
                get_download_cache().register_file_error(filename, error_msg)
                # Marca a barra como erro se o cache falhar devido ao tamanho
                progress.update(task_id, description=f"[red]{filename[:30]} (erro cache)[/red]", style="red")

        return destination_path, None

    except Exception as e_proc:
        error_msg = f"Erro durante o processamento da resposta/escrita: {e_proc}"
        logger.error(f"{filename}: {error_msg}")
        # Marca a barra como erro
        progress.update(task_id, description=f"[red]{filename[:30]} (ERRO escrita)[/red]", style="red")
        
        # Registra o erro no cache
        if config.cache.enabled:
            get_download_cache().register_file_error(filename, error_msg)
            
        return filename, e_proc


async def download_file(session: aiohttp.ClientSession | None, url: str, destination_path: str, semaphore: asyncio.Semaphore,
                        progress: Progress, task_id: int, force_download: bool = False) -> Tuple[str, Exception | None, str | None]:
    """
    Baixa um arquivo de uma URL para um caminho de destino com retry robusto e recuperação inteligente.
    
    Args:
        session: Sessão HTTP para fazer o download
        url: URL do arquivo para baixar
        destination_path: Caminho onde salvar o arquivo
        semaphore: Semáforo para controlar concorrência
        progress: Objeto Progress para atualizar progresso
        task_id: ID da tarefa de progresso
        force_download: Se True, força o download mesmo se o arquivo já existir
        
    Returns:
        Tupla com (caminho_do_arquivo, erro, motivo_skip)
    """
    filename = os.path.basename(url)
    max_retries = 5  # Aumentado para 5 tentativas
    initial_retry_delay = 3  # Delay inicial reduzido
    max_retry_delay = 60  # Delay máximo
    
    # Configurações de timeout mais robustas
    connect_timeout = 30
    read_timeout = 300  # 5 minutos para leitura
    total_timeout = 600  # 10 minutos total
    
    for attempt in range(max_retries + 1):
        retry_delay = min(initial_retry_delay * (2 ** attempt), max_retry_delay)
        
        try:
            # Verificar se arquivo já existe e está completo
            if os.path.exists(destination_path) and not force_download:
                local_size = os.path.getsize(destination_path)
                
                # Obter metadados remotos para comparação
                remote_size, remote_last_modified = await get_remote_file_metadata(session, url)
                
                if remote_size and local_size == remote_size:
                    # Validação adicional de integridade
                    if await _validate_file_integrity(destination_path, remote_size):
                        logger.info(f"Arquivo {filename} já existe e está íntegro. Pulando download.")
                        return destination_path, None, "Arquivo já existe"
                    else:
                        logger.warning(f"Arquivo {filename} existe mas pode estar corrompido. Redownload necessário.")
                        force_download = True
            
            # Obter metadados do arquivo remoto com retry
            remote_size, remote_last_modified = await get_remote_file_metadata(session, url)
            if remote_size is None:
                error_msg = f"Não foi possível obter metadados remotos para {url}"
                if attempt < max_retries:
                    logger.warning(f"Tentativa {attempt + 1}/{max_retries + 1} falhou para {filename}: {error_msg}. Tentando novamente em {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Todas as tentativas falharam para {filename}. Último erro: {error_msg}")
                    return filename, Exception(error_msg), None
            
            # Verificar se arquivo parcial existe
            initial_size = 0
            file_mode = "wb"
            
            if os.path.exists(destination_path) and not force_download:
                local_size = os.path.getsize(destination_path)
                
                # Verificar se arquivo está corrompido ou incompleto
                if local_size < remote_size:
                    # Verificar se o servidor suporta range requests
                    if await _server_supports_range_requests(session, url):
                        # Tentar resumir download
                        initial_size = local_size
                        file_mode = "ab"
                        logger.info(f"Resumindo download de {filename} a partir de {initial_size} bytes")
                    else:
                        logger.info(f"Servidor não suporta resumo. Fazendo download completo de {filename}")
                        initial_size = 0
                        file_mode = "wb"
                elif local_size == remote_size:
                    # Arquivo completo - validar integridade
                    if await _validate_file_integrity(destination_path, remote_size):
                        logger.info(f"Arquivo {filename} já está completo e íntegro")
                        return destination_path, None, "Arquivo já completo"
                    else:
                        logger.warning(f"Arquivo {filename} completo mas corrompido. Redownload necessário.")
                        initial_size = 0
                        file_mode = "wb"
                else:
                    # Arquivo maior que o esperado - redownload
                    logger.warning(f"Arquivo {filename} local é maior que o remoto. Fazendo download completo.")
                    initial_size = 0
                    file_mode = "wb"
            
            # Configurar headers para o download
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
            
            if initial_size > 0:
                headers['Range'] = f'bytes={initial_size}-'
            
            # Configurar timeout personalizado
            timeout = aiohttp.ClientTimeout(
                total=total_timeout,
                connect=connect_timeout,
                sock_read=read_timeout
            )
            
            logger.info(f"Iniciando download de {filename} (tentativa {attempt + 1}/{max_retries + 1}, {initial_size} bytes já baixados)")
            
            try:
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    # Verificar status da resposta
                    if response.status == 416:  # Range Not Satisfiable
                        logger.warning(f"Servidor não suporta resumo para {filename}. Fazendo download completo.")
                        initial_size = 0
                        file_mode = "wb"
                        headers.pop('Range', None)
                        
                        async with session.get(url, headers=headers, timeout=timeout) as response:
                            response.raise_for_status()
                            result_path, error = await _process_download_response(
                                response, destination_path, file_mode, progress, task_id, 
                                remote_size, initial_size, filename, remote_last_modified
                            )
                            
                            # Validar arquivo baixado
                            if error is None and await _validate_file_integrity(result_path, remote_size):
                                return result_path, None, None
                            elif error is None:
                                error = Exception("Arquivo baixado falhou na validação de integridade")
                            
                            if attempt < max_retries:
                                logger.warning(f"Download de {filename} falhou na validação. Tentando novamente...")
                                continue
                            else:
                                return filename, error, None
                                
                    elif response.status in [200, 206]:  # OK ou Partial Content
                        result_path, error = await _process_download_response(
                            response, destination_path, file_mode, progress, task_id, 
                            remote_size, initial_size, filename, remote_last_modified
                        )
                        
                        # Validar arquivo baixado
                        if error is None and await _validate_file_integrity(result_path, remote_size):
                            logger.info(f"Download de {filename} concluído e validado com sucesso")
                            return result_path, None, None
                        elif error is None:
                            error = Exception("Arquivo baixado falhou na validação de integridade")
                        
                        if attempt < max_retries:
                            logger.warning(f"Download de {filename} falhou na validação. Tentando novamente em {retry_delay}s...")
                            await asyncio.sleep(retry_delay)
                            continue
                        else:
                            return filename, error, None
                    else:
                        response.raise_for_status()
                        
            except asyncio.TimeoutError as e:
                error_msg = f"Timeout durante download: {e}"
                logger.warning(f"Tentativa {attempt + 1}/{max_retries + 1} - {filename}: {error_msg}")
                if attempt < max_retries:
                    logger.info(f"Tentando novamente em {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Todas as tentativas falharam para {filename}. Último erro: {error_msg}")
                    return filename, Exception(error_msg), None
                    
            except aiohttp.ClientError as e:
                error_msg = f"Erro de cliente HTTP: {e}"
                logger.warning(f"Tentativa {attempt + 1}/{max_retries + 1} - {filename}: {error_msg}")
                if attempt < max_retries:
                    logger.info(f"Tentando novamente em {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Todas as tentativas falharam para {filename}. Último erro: {error_msg}")
                    return filename, Exception(error_msg), None
                    
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"Tentativa {attempt + 1}/{max_retries + 1} falhou para {filename}: {error_msg}")
            
            if attempt < max_retries:
                logger.info(f"Tentando novamente em {retry_delay}s...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Todas as tentativas falharam para {filename}. Último erro: {error_msg}")
                return filename, e, None
    
    # Não deveria chegar aqui
    return filename, Exception("Erro inesperado no download"), None


async def _validate_file_integrity(file_path: str, expected_size: int) -> bool:
    """
    Valida a integridade de um arquivo baixado.
    
    Args:
        file_path: Caminho do arquivo
        expected_size: Tamanho esperado em bytes
        
    Returns:
        bool: True se o arquivo está íntegro
    """
    try:
        if not os.path.exists(file_path):
            return False
            
        actual_size = os.path.getsize(file_path)
        
        # Verificar tamanho
        if actual_size != expected_size:
            logger.warning(f"Tamanho do arquivo {os.path.basename(file_path)} não confere: {actual_size} != {expected_size}")
            return False
        
        # Verificar se o arquivo pode ser lido
        try:
            with open(file_path, 'rb') as f:
                # Ler primeiro e último chunk para verificar se o arquivo não está corrompido
                f.read(1024)  # Primeiro KB
                if actual_size > 1024:
                    f.seek(-1024, 2)  # Último KB
                    f.read(1024)
        except Exception as e:
            logger.warning(f"Arquivo {os.path.basename(file_path)} não pode ser lido: {e}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao validar integridade de {os.path.basename(file_path)}: {e}")
        return False


async def _server_supports_range_requests(session: aiohttp.ClientSession, url: str) -> bool:
    """
    Verifica se o servidor suporta range requests (resumo de download).
    
    Args:
        session: Sessão HTTP
        url: URL para testar
        
    Returns:
        bool: True se suporta range requests
    """
    try:
        async with session.head(url, timeout=30) as response:
            accept_ranges = response.headers.get('Accept-Ranges', '').lower()
            return accept_ranges == 'bytes'
    except Exception:
        # Se não conseguir verificar, assume que não suporta
        return False


async def get_remote_file_metadata(session: aiohttp.ClientSession, url: str) -> Tuple[int | None, int | None]:
    """Obtém tamanho e timestamp de modificação de um arquivo remoto via HEAD request com retry robusto."""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            timeout = aiohttp.ClientTimeout(total=60, connect=30)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': '*/*'
            }
            
            async with session.head(url, timeout=timeout, allow_redirects=True, headers=headers) as response:
                response.raise_for_status()
                
                # Obter tamanho do arquivo
                content_length = response.headers.get('Content-Length')
                if content_length:
                    remote_size = int(content_length)
                else:
                    logger.warning(f"Content-Length não encontrado para {url}")
                    remote_size = None
                
                # Obter timestamp de modificação
                last_modified_str = response.headers.get('Last-Modified')
                timestamp_last_modified = None
                
                if last_modified_str:
                    try:
                        # Tenta formato RFC 1123 (mais comum)
                        dt_obj = datetime.datetime.strptime(last_modified_str, '%a, %d %b %Y %H:%M:%S GMT')
                        timestamp_last_modified = int(dt_obj.timestamp())
                    except ValueError:
                        try:
                            # Tenta formato RFC 850
                            dt_obj = datetime.datetime.strptime(last_modified_str, '%A, %d-%b-%y %H:%M:%S GMT')
                            timestamp_last_modified = int(dt_obj.timestamp())
                        except ValueError:
                            logger.warning(f"Formato inesperado de Last-Modified: {last_modified_str} para {url}")
                
                return remote_size, timestamp_last_modified
                
        except asyncio.TimeoutError:
            error_msg = f"Timeout ao obter metadados de {url} (tentativa {attempt + 1}/{max_retries})"
            logger.warning(error_msg)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
                continue
            else:
                logger.error(f"Falha final ao obter metadados de {url}: timeout")
                return None, None
                
        except aiohttp.ClientError as e:
            error_msg = f"Erro de cliente ao obter metadados de {url}: {e} (tentativa {attempt + 1}/{max_retries})"
            logger.warning(error_msg)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
                continue
            else:
                logger.error(f"Falha final ao obter metadados de {url}: {e}")
                return None, None
                
        except Exception as e:
            error_msg = f"Erro inesperado ao obter metadados de {url}: {e} (tentativa {attempt + 1}/{max_retries})"
            logger.warning(error_msg)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
                continue
            else:
                logger.error(f"Falha final ao obter metadados de {url}: {e}")
                return None, None
    
    return None, None


async def process_downloaded_file(filename: str, file_path: str, path_unzip: str, path_parquet: str) -> Tuple[str, Exception | None]:
    """
    Processa um arquivo baixado usando o sistema de streaming otimizado.
    
    Args:
        filename: Nome do arquivo
        file_path: Caminho do arquivo zip
        path_unzip: Diretório onde extrair o arquivo
        path_parquet: Diretório onde salvar o arquivo parquet
        
    Returns:
        Tuple[str, Exception | None]: (Caminho do arquivo, Erro se houver)
    """
    try:
        # Usar o processador streaming otimizado
        result, error = await streaming_processor.process_with_streaming(
            file_path, filename, path_unzip, path_parquet
        )
        
        if error is None:
            logger.info(f"Arquivo {filename} processado com sucesso via streaming")
            return result, None
        else:
            logger.error(f"Erro no processamento streaming de {filename}: {error}")
            return file_path, error
            
    except Exception as e:
        logger.error(f"Erro crítico no processamento de {filename}: {e}")
        return file_path, e


def format_elapsed_time(seconds: float) -> str:
    """Formata tempo decorrido em formato legível."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"


async def download_multiple_files(
    urls: List[str], 
    path_zip: str, 
    path_unzip: str, 
    path_parquet: str, 
    force_download: bool = False,
    max_concurrent_downloads: int = 4,
    max_concurrent_processing: int = 2
) -> Tuple[List[str], List[Tuple[str, Exception]]]:
    """
    Baixa múltiplos arquivos de forma assíncrona com pipeline otimizado e controle de falhas.
    """
    start_time = time.time()
    
    # Listas para rastrear resultados
    downloaded_files = []
    processed_files = []
    failed_downloads = []
    failed_processing = []
    
    # Controle de falhas críticas
    consecutive_failures = 0
    max_consecutive_failures = 5  # Para após 5 falhas consecutivas
    total_failures = 0
    max_failure_rate = 0.5  # Para se mais de 50% dos arquivos falharem
    
    # Configurar semáforos adaptativos
    download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
    process_semaphore = asyncio.Semaphore(max_concurrent_processing)
    
    # Inicializar monitor de recursos
    resource_monitor = ResourceMonitor()
    resource_monitor.max_download_workers = max_concurrent_downloads * 2
    resource_monitor.max_processing_workers = max_concurrent_processing * 2
    resource_monitor.current_download_workers = max_concurrent_downloads
    resource_monitor.current_processing_workers = max_concurrent_processing
    
    # Fila de processamento
    processing_queue = asyncio.Queue()
    
    # Iniciar monitoramento de recursos
    monitor_task = None
    if config.pipeline.enable_resource_monitoring:
        monitor_task = asyncio.create_task(resource_monitor.start_monitoring())
        logger.info("🔍 Sistema de monitoramento de recursos ativado")
    
    # Inicializar estatísticas globais
    global_stats.reset()
    
    # Ordenar URLs por tamanho (menores primeiro)
    try:
        logger.info("📏 Ordenando arquivos por tamanho para otimizar pipeline...")
        sorted_urls_with_sizes = await get_file_sizes_and_sort(urls)
        sorted_urls = [url for url, _ in sorted_urls_with_sizes]
        
        # Log da ordenação
        logger.info("Arquivos ordenados por tamanho (menor → maior):")
        for i, (url, size) in enumerate(sorted_urls_with_sizes[:10]):
            filename = os.path.basename(url)
            size_mb = size / (1024 * 1024)
            logger.info(f"   {i+1:2d}. {filename}: {size_mb:.1f}MB")
        if len(sorted_urls_with_sizes) > 10:
            logger.info(f"   ... e mais {len(sorted_urls_with_sizes) - 10} arquivos")
        logger.info("✅ Arquivos ordenados por tamanho (menores primeiro)")
    except Exception as e:
        logger.warning(f"Erro ao ordenar arquivos por tamanho: {e}. Usando ordem original.")
        sorted_urls = urls
    
    print(f"\n🚀 Iniciando pipeline otimizado para {len(sorted_urls)} arquivos")
    print(f"📊 Configuração: {max_concurrent_downloads} downloads | {max_concurrent_processing} processamentos")
    print(f"🧠 Streaming: ✅")
    print(f"📈 Monitoramento adaptativo: ✅")
    print(f"📏 Ordenação por tamanho: ✅ (menores primeiro)")
    print(f"🛡️ Controle de falhas: máx {max_consecutive_failures} consecutivas, {max_failure_rate*100:.0f}% taxa máxima")

    # Função que processa os arquivos da fila em paralelo
    async def process_files_from_queue():
        """Consome a fila de processamento e processa os arquivos em tempo real."""
        nonlocal consecutive_failures, total_failures
        
        processor_id = id(asyncio.current_task())
        processor_name = f"Worker-{processor_id % 1000}"  # Nome mais legível
        
        # Log visível no console
        print(f"\n🔧 {processor_name} iniciado e aguardando arquivos...")
        logger.info(f"Iniciando processador {processor_id}")
        
        while True:
            try:
                # Verificar se devemos parar por falhas críticas
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"🛑 {processor_name}: Parando devido a {consecutive_failures} falhas consecutivas")
                    break
                
                failure_rate = total_failures / max(1, len(downloaded_files) + total_failures)
                if failure_rate > max_failure_rate and len(downloaded_files) > 5:
                    logger.error(f"🛑 {processor_name}: Parando devido a alta taxa de falhas ({failure_rate*100:.1f}%)")
                    break
                
                # Aguardar arquivo da fila com timeout
                try:
                    file_path = await asyncio.wait_for(processing_queue.get(), timeout=10.0)
                except asyncio.TimeoutError:
                    # Verificar se ainda há downloads ativos
                    if 'download_tasks' in locals() and not any(not task.done() for task in download_tasks):
                        # Não há downloads ativos, verificar se fila está vazia
                        if processing_queue.empty():
                            logger.debug(f"Processador {processor_id} finalizando - sem downloads ativos e fila vazia")
                            break
                    continue
                
                # Sinal de parada
                if file_path is None:
                    logger.debug(f"Processador {processor_id} recebeu sinal de parada")
                    break
                
                filename = os.path.basename(file_path)
                logger.debug(f"Processador {processor_id} aguardando arquivo...")
                
                try:
                    # Usar o semáforo adaptativo para limitar processamento paralelo
                    logger.debug(f"Processador {processor_id} aguardando semáforo para {filename}")
                    async with process_semaphore:
                        print(f"🔄 {processor_name}: Iniciando processamento otimizado de {filename}")
                        logger.info(f"Processador {processor_id} obteve semáforo para {filename}")
                        
                        # Usar o novo sistema de processamento streaming
                        result, error = await streaming_processor.process_with_streaming(
                            file_path, filename, path_unzip, path_parquet
                        )
                        
                        if error is None:
                            processed_files.append(result)
                            consecutive_failures = 0  # Reset contador de falhas consecutivas
                            print(f"✅ {processor_name}: {filename} processado com sucesso via streaming!")
                            logger.info(f"Processador {processor_id} concluiu processamento streaming de {filename}")
                        else:
                            failed_processing.append((result, error))
                            consecutive_failures += 1
                            total_failures += 1
                            print(f"❌ {processor_name}: Falha no processamento streaming de {filename} - {error}")
                            logger.warning(f"Processador {processor_id}: Falha no processamento streaming de {filename}: {error}")
                            
                            # Log de alerta se muitas falhas
                            if consecutive_failures >= 3:
                                logger.warning(f"⚠️ {consecutive_failures} falhas consecutivas detectadas. Sistema pode estar instável.")
                                
                except Exception as e:
                    consecutive_failures += 1
                    total_failures += 1
                    print(f"💥 {processor_name}: Erro crítico no processamento streaming de {filename} - {e}")
                    logger.error(f"Processador {processor_id}: Erro no processamento streaming de {filename}: {e}")
                    failed_processing.append((filename, e))
                
                logger.debug(f"Processador {processor_id} finalizando tarefa para {filename}")
                processing_queue.task_done()
            except Exception as e:
                print(f"💥 {processor_name}: Erro na fila de processamento - {e}")
                logger.error(f"Processador {processor_id}: Erro no processamento da fila: {e}")
                processing_queue.task_done()

        print(f"🏁 {processor_name} encerrado")
        logger.info(f"Processador {processor_id} encerrado")

    # Função de download otimizada
    async def download_and_queue(url: str):
        """Baixa um arquivo e o adiciona à fila de processamento."""
        nonlocal consecutive_failures, total_failures
        
        filename = os.path.basename(url)
        
        try:
            async with download_semaphore:
                # Verificar se devemos parar por falhas críticas
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"🛑 Parando download de {filename} devido a falhas críticas")
                    return
                
                print(f"⬇️  Iniciando download otimizado: {filename}")
                
                # Criar caminho de destino
                destination_path = os.path.join(path_zip, filename)
                
                # Configurar sessão HTTP robusta
                connector = aiohttp.TCPConnector(
                    limit=100,
                    limit_per_host=10,
                    ttl_dns_cache=300,
                    use_dns_cache=True,
                    keepalive_timeout=60,
                    enable_cleanup_closed=True
                )
                
                timeout = aiohttp.ClientTimeout(
                    total=1200,  # 20 minutos total
                    connect=60,  # 1 minuto para conectar
                    sock_read=600  # 10 minutos para leitura
                )
                
                # Usar sessão HTTP temporária para este download
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': '*/*',
                        'Accept-Encoding': 'gzip, deflate',
                        'Connection': 'keep-alive'
                    }
                ) as session:
                    # Usar Progress simples sem display para evitar conflitos
                    progress = Progress(console=None, disable=True)  # Progress desabilitado
                    task_id = progress.add_task(f"Downloading {filename}", total=100)
                    
                    # Chamar download_file com todos os argumentos necessários
                    file_path, error, skip_reason = await download_file(
                        session, url, destination_path, download_semaphore, progress, task_id, force_download
                    )
                
                if error is None:
                    downloaded_files.append(file_path)
                    consecutive_failures = 0  # Reset contador de falhas consecutivas
                    # Adicionar à fila de processamento imediatamente
                    await processing_queue.put(file_path)
                    print(f"✅ Download concluído e enfileirado: {filename}")
                    logger.info(f"Arquivo {filename} baixado e adicionado à fila de processamento")
                else:
                    failed_downloads.append((filename, error))
                    consecutive_failures += 1
                    total_failures += 1
                    print(f"❌ Falha no download: {filename} - {error}")
                    logger.error(f"Falha no download de {filename}: {error}")
                    
                    # Log de alerta se muitas falhas
                    if consecutive_failures >= 3:
                        logger.warning(f"⚠️ {consecutive_failures} falhas consecutivas de download detectadas.")
                        
        except aiohttp.ClientError as e:
            failed_downloads.append((filename, e))
            consecutive_failures += 1
            total_failures += 1
            print(f"💥 Erro de rede no download: {filename} - {e}")
            logger.error(f"Erro de rede no download de {filename}: {e}")
            
        except asyncio.TimeoutError as e:
            failed_downloads.append((filename, e))
            consecutive_failures += 1
            total_failures += 1
            print(f"⏰ Timeout no download: {filename} - {e}")
            logger.error(f"Timeout no download de {filename}: {e}")
            
        except Exception as e:
            failed_downloads.append((filename, e))
            consecutive_failures += 1
            total_failures += 1
            print(f"💥 Erro crítico no download: {filename} - {e}")
            logger.error(f"Erro crítico no download de {filename}: {e}")

    try:
        # Inicializar processador streaming
        streaming_processor = StreamingProcessor()
        
        # Criar tasks de processamento
        processing_tasks = [
            asyncio.create_task(process_files_from_queue())
            for _ in range(max_concurrent_processing)
        ]
        
        # Criar tasks de download
        download_tasks = [
            asyncio.create_task(download_and_queue(url))
            for url in sorted_urls
        ]
        
        print(f"\n🔄 Executando {len(download_tasks)} downloads e {len(processing_tasks)} processadores...")
        
        # Aguardar todos os downloads terminarem
        await asyncio.gather(*download_tasks, return_exceptions=True)
        print("📥 Todos os downloads finalizados")
        
        # Verificar se devemos continuar processamento
        if consecutive_failures >= max_consecutive_failures:
            logger.error(f"🛑 Pipeline interrompido devido a {consecutive_failures} falhas consecutivas")
            print(f"🛑 Pipeline interrompido por falhas críticas")
        elif total_failures / max(1, len(sorted_urls)) > max_failure_rate:
            logger.error(f"🛑 Pipeline interrompido devido a alta taxa de falhas ({total_failures}/{len(sorted_urls)})")
            print(f"🛑 Pipeline interrompido por alta taxa de falhas")
        else:
            # Aguardar a fila de processamento esvaziar
            await processing_queue.join()
            print("🔄 Todos os processamentos finalizados")
        
        # Sinalizar para os workers de processamento pararem
        for _ in processing_tasks:
            await processing_queue.put(None)
        
        # Aguardar workers de processamento terminarem
        await asyncio.gather(*processing_tasks, return_exceptions=True)
        print("🏁 Todos os workers finalizados")
        
    except Exception as e:
        logger.error(f"Erro no pipeline principal: {e}")
        print(f"💥 Erro no pipeline principal: {e}")
    
    finally:
        # Parar monitoramento de recursos
        if monitor_task:
            resource_monitor.stop_monitoring()
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        
        # Calcular estatísticas finais
        end_time = time.time()
        total_time = end_time - start_time
        
        # Obter estatísticas do monitor de recursos
        resource_stats = resource_monitor.get_resource_impact_summary()
        
        # Relatório final otimizado
        print(f"\n📊 === RELATÓRIO FINAL DO PIPELINE OTIMIZADO ===")
        print(f"⏱️  Tempo total: {format_elapsed_time(total_time)}")
        print(f"📥 Downloads: {len(downloaded_files)} sucessos, {len(failed_downloads)} falhas")
        print(f"🔄 Processamentos: {len(processed_files)} sucessos, {len(failed_processing)} falhas")
        
        # Status de falhas críticas
        if consecutive_failures >= max_consecutive_failures:
            print(f"🛑 Pipeline interrompido por {consecutive_failures} falhas consecutivas")
        elif total_failures / max(1, len(sorted_urls)) > max_failure_rate:
            print(f"🛑 Pipeline interrompido por alta taxa de falhas ({total_failures}/{len(sorted_urls)})")
        else:
            print(f"✅ Pipeline concluído sem falhas críticas")
        
        if isinstance(resource_stats, dict):
            print(f"💻 CPU médio: {resource_stats['avg_cpu']:.1f}% (máx: {resource_stats['peak_cpu']:.1f}%)")
            print(f"🧠 Memória média: {resource_stats['avg_memory']:.1f}% (máx: {resource_stats['peak_memory']:.1f}%)")
        
        # Estatísticas globais
        stats_summary = global_stats.get_summary()
        if stats_summary['total_files'] > 0:
            print(f"📈 Throughput: {stats_summary['total_files'] / total_time:.2f} arquivos/segundo")
            print(f"💾 Dados processados: {stats_summary['total_size_mb']:.1f} MB")
        
        print("=" * 50)
    
    return processed_files, failed_downloads + failed_processing


def get_remote_folders(base_url: str) -> List[str]:
    """Busca todas as pastas remotas disponíveis no formato AAAA-MM.
    
    Args:
        base_url: URL base para buscar diretórios
        
    Returns:
        List[str]: Lista de nomes de pastas no formato AAAA-MM
    """
    year_month_folders = []
    
    # Buscar e encontrar links de diretórios na URL base
    logger.info(f"Buscando diretórios em: {base_url}")
    base_soup = _fetch_and_parse(base_url)
    if not base_soup:
        return []
    
    directory_links = _find_links(base_soup, base_url, ends_with=None)
    
    # Filtrar diretórios AAAA-MM
    for dir_url in directory_links:
        folder_name = dir_url.strip('/').split('/')[-1]
        if re.fullmatch(r'(\d{4})-(\d{2})', folder_name):
            year_month_folders.append(folder_name)
            logger.debug(f"Encontrado diretório AAAA-MM: {folder_name}")
    
    # Ordenar pastas por data (mais recente primeiro)
    year_month_folders.sort(reverse=True)
    
    logger.info(f"Total de {len(year_month_folders)} pastas remotas encontradas")
    return year_month_folders


async def get_latest_remote_folder(base_url: str) -> str:
    """Busca a pasta remota mais recente no formato AAAA-MM.
    
    Args:
        base_url: URL base para buscar diretórios
        
    Returns:
        str: Nome da pasta mais recente no formato AAAA-MM
    """
    folders = get_remote_folders(base_url)
    if not folders:
        logger.error("Nenhuma pasta remota encontrada")
        return ""
    
    latest_folder = folders[0]
    return latest_folder


async def get_file_sizes_and_sort(urls: List[str]) -> List[Tuple[str, int]]:
    """
    Obtém os tamanhos dos arquivos remotos e retorna uma lista ordenada por tamanho (menor primeiro).
    
    Args:
        urls: Lista de URLs para verificar
        
    Returns:
        Lista de tuplas (url, tamanho) ordenada por tamanho crescente
    """
    logger.info(f"Obtendo tamanhos de {len(urls)} arquivos para ordenação...")
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(get_remote_file_metadata(session, url))
            tasks.append((url, task))
        
        url_sizes = []
        for url, task in tasks:
            try:
                size, _ = await task
                if size is not None:
                    url_sizes.append((url, size))
                    filename = os.path.basename(url)
                    size_mb = size / (1024 * 1024)
                    logger.debug(f"Arquivo {filename}: {size_mb:.1f}MB")
                else:
                    logger.warning(f"Não foi possível obter tamanho de {os.path.basename(url)}")
                    # Adiciona com tamanho 0 para não perder o arquivo
                    url_sizes.append((url, 0))
            except Exception as e:
                logger.error(f"Erro ao obter tamanho de {os.path.basename(url)}: {e}")
                # Adiciona com tamanho 0 para não perder o arquivo
                url_sizes.append((url, 0))
    
    # Ordenar por tamanho (menor primeiro)
    url_sizes.sort(key=lambda x: x[1])
    
    # Log da ordenação
    logger.info("Arquivos ordenados por tamanho (menor → maior):")
    for i, (url, size) in enumerate(url_sizes[:10]):  # Mostrar apenas os 10 primeiros
        filename = os.path.basename(url)
        size_mb = size / (1024 * 1024)
        logger.info(f"  {i+1:2d}. {filename}: {size_mb:.1f}MB")
    
    if len(url_sizes) > 10:
        logger.info(f"  ... e mais {len(url_sizes) - 10} arquivos")
    
    return url_sizes
