import asyncio
import datetime
import logging
import os
import re
import time
import zipfile
from typing import List, Tuple, Callable, Dict, Any
from urllib.parse import urljoin

# Bibliotecas de terceiros
import aiofiles
import aiohttp
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rich.logging import RichHandler
from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
)

# Importações locais do projeto
from .config import config
from .utils import DownloadCache
from .process.empresa import process_single_zip as process_empresa_zip
from .process.estabelecimento import process_single_zip as process_estabelecimento_zip
from .process.simples import process_single_zip as process_simples_zip
from .process.socio import process_single_zip as process_socio_zip
from .utils.time_utils import format_elapsed_time
from .utils.statistics import global_stats

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
    if download_cache is None:
        download_cache = DownloadCache(config.cache.cache_path)
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
    import psutil
    
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
            import psutil
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
                import gc
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
    """Monitor de recursos do sistema para otimização dinâmica."""
    
    def __init__(self):
        self.monitoring = False
        self.stats = {
            'cpu_percent': [],
            'memory_percent': [],
            'disk_io': [],
            'network_io': []
        }
        self.thresholds = {
            'memory': config.pipeline.memory_threshold_percent,
            'cpu': config.pipeline.cpu_threshold_percent
        }
    
    async def start_monitoring(self):
        """Inicia o monitoramento de recursos."""
        if not config.pipeline.enable_resource_monitoring:
            return
            
        self.monitoring = True
        logger.info("🔍 Iniciando monitoramento de recursos do sistema")
        
        while self.monitoring:
            try:
                import psutil
                
                # Coletar métricas
                cpu_percent = psutil.cpu_percent(interval=0.1)
                memory = psutil.virtual_memory()
                disk_io = psutil.disk_io_counters()
                net_io = psutil.net_io_counters()
                
                # Armazenar estatísticas
                self.stats['cpu_percent'].append(cpu_percent)
                self.stats['memory_percent'].append(memory.percent)
                
                if disk_io:
                    self.stats['disk_io'].append({
                        'read_bytes': disk_io.read_bytes,
                        'write_bytes': disk_io.write_bytes
                    })
                
                if net_io:
                    self.stats['network_io'].append({
                        'bytes_sent': net_io.bytes_sent,
                        'bytes_recv': net_io.bytes_recv
                    })
                
                # Verificar se precisa ajustar concorrência
                await self._adjust_concurrency_if_needed(cpu_percent, memory.percent)
                
                # Manter apenas últimas 100 medições
                for key in self.stats:
                    if len(self.stats[key]) > 100:
                        self.stats[key] = self.stats[key][-100:]
                
                await asyncio.sleep(2)  # Monitorar a cada 2 segundos
                
            except Exception as e:
                logger.warning(f"Erro no monitoramento de recursos: {e}")
                await asyncio.sleep(5)
    
    async def _adjust_concurrency_if_needed(self, cpu_percent: float, memory_percent: float):
        """Ajusta a concorrência dinamicamente baseado no uso de recursos."""
        global download_semaphore, process_semaphore
        
        # Se recursos estão sobrecarregados, reduzir concorrência
        if memory_percent > self.thresholds['memory'] or cpu_percent > self.thresholds['cpu']:
            current_download = download_semaphore._value
            current_process = process_semaphore._value
            
            if current_download > config.pipeline.min_download_workers:
                # Reduzir workers de download
                new_download = max(config.pipeline.min_download_workers, current_download - 1)
                download_semaphore = asyncio.Semaphore(new_download)
                logger.warning(f"🔻 Recursos sobrecarregados. Reduzindo workers de download: {current_download} → {new_download}")
            
            if current_process > config.pipeline.min_process_workers:
                # Reduzir workers de processamento
                new_process = max(config.pipeline.min_process_workers, current_process - 1)
                process_semaphore = asyncio.Semaphore(new_process)
                logger.warning(f"🔻 Recursos sobrecarregados. Reduzindo workers de processamento: {current_process} → {new_process}")
        
        # Se recursos estão ociosos, aumentar concorrência gradualmente
        elif memory_percent < self.thresholds['memory'] * 0.7 and cpu_percent < self.thresholds['cpu'] * 0.7:
            current_download = download_semaphore._value
            current_process = process_semaphore._value
            
            if current_download < config.pipeline.max_download_workers:
                new_download = min(config.pipeline.max_download_workers, current_download + 1)
                download_semaphore = asyncio.Semaphore(new_download)
                logger.info(f"🔺 Recursos disponíveis. Aumentando workers de download: {current_download} → {new_download}")
            
            if current_process < config.pipeline.max_process_workers:
                new_process = min(config.pipeline.max_process_workers, current_process + 1)
                process_semaphore = asyncio.Semaphore(new_process)
                logger.info(f"🔺 Recursos disponíveis. Aumentando workers de processamento: {current_process} → {new_process}")
    
    def stop_monitoring(self):
        """Para o monitoramento de recursos."""
        self.monitoring = False
        logger.info("🛑 Monitoramento de recursos parado")
    
    def get_stats_summary(self):
        """Retorna um resumo das estatísticas coletadas."""
        if not self.stats['cpu_percent']:
            return "Nenhuma estatística coletada"
        
        cpu_avg = sum(self.stats['cpu_percent']) / len(self.stats['cpu_percent'])
        cpu_max = max(self.stats['cpu_percent'])
        memory_avg = sum(self.stats['memory_percent']) / len(self.stats['memory_percent'])
        memory_max = max(self.stats['memory_percent'])
        
        return {
            'cpu_avg': cpu_avg,
            'cpu_max': cpu_max,
            'memory_avg': memory_avg,
            'memory_max': memory_max,
            'samples': len(self.stats['cpu_percent'])
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
                        progress: Progress, task_id: int) -> Tuple[str, Exception | None, str | None]:
    """
    Downloads a single file asynchronously, updating a Rich progress bar.
    Attempts to resume download if local file is partial and server supports Range.
    Otherwise, downloads the entire file. Checks cache and metadata.
    Applies styles (colors) to the progress bar based on status.
    Returns:
        Tuple[str, Exception | None, str | None]: (Path/URL, Error or None, Skip Reason or None)
    """
    filename = os.path.basename(destination_path)
    skip_reason: str | None = None
    created_session = False
    max_retries = 3
    retry_count = 0
    last_error = None
    
    # Iniciar coleta de estatísticas
    download_start_time = time.time()
    file_size = 0

    while retry_count < max_retries:
        try:
            async with semaphore:
                file_mode = 'wb'
                resume_header = {}
                initial_size = 0
                skip_download = False
                force_download = False

                # Verificar se o download forçado foi solicitado via variável de ambiente
                if os.getenv('FORCE_DOWNLOAD', '').lower() == 'true':
                    force_download = True
                    logger.info(f"Download forçado ativado para {filename} via variável de ambiente")

                # Criar sessão se não for fornecida
                if session is None:
                    session = aiohttp.ClientSession()
                    created_session = True

                # Verificar se o arquivo tem erros registrados no cache
                if config.cache.enabled and get_download_cache().has_file_error(filename):
                    logger.warning(f"Arquivo {filename} tem erros registrados no cache. Forçando download completo.")
                    force_download = True
                    progress.update(task_id, description=f"[orange]{filename[:30]} (forçando download)[/orange]",
                                   style="yellow")
                    file_mode = 'wb'
                    initial_size = 0

                # 1. Obter metadados remotos
                progress.update(task_id, description=f"[cyan]{filename[:30]} (verificando...)[/cyan]")
                remote_size, remote_last_modified = await get_remote_file_metadata(session, url)

                if remote_size is None:
                    raise ValueError(f"Não foi possível obter metadados remotos para {url}")

                # Armazenar tamanho do arquivo para estatísticas
                file_size = remote_size

                # Atualiza o total da barra assim que conhecido
                progress.update(task_id, total=remote_size)

                # 2. Verificar cache e arquivo local
                file_exists = os.path.exists(destination_path)
                local_size = os.path.getsize(destination_path) if file_exists else 0
                attempt_resume = False

                if file_exists and not force_download:
                    # Checa cache primeiro (se habilitado)
                    if config.cache.enabled and get_download_cache().is_file_cached(filename, remote_size, remote_last_modified):
                        if local_size == remote_size:
                            progress.update(task_id, description=f"[green]{filename[:30]} (cache)[/green]",
                                            completed=remote_size, style="green")
                            skip_download = True
                            skip_reason = "cache"
                        else:
                            logger.warning(f"Arquivo {filename} em cache, mas tamanho local difere. Baixando completo.")
                            progress.update(task_id, description=f"[yellow]{filename[:30]} (cache inválido)[/yellow]",
                                            style="yellow")
                            file_mode = 'wb'
                            initial_size = 0
                    elif local_size == remote_size:
                        # Arquivo tem o mesmo tamanho que o remoto
                        if remote_last_modified is not None:
                            local_last_modified = int(os.path.getmtime(destination_path))
                            if local_last_modified >= remote_last_modified:
                                progress.update(task_id, description=f"[green]{filename[:30]} (atualizado)[/green]",
                                                completed=remote_size, style="green")
                                skip_download = True
                                skip_reason = "up-to-date"
                                logger.info(f"Arquivo {filename} já está atualizado (mesmo tamanho e data)")
                            else:
                                logger.info(f"Arquivo {filename} desatualizado. Baixando novamente.")
                                file_mode = 'wb'
                                initial_size = 0
                        else:
                            # Sem data de modificação remota, mas tamanho igual - assumir que está OK
                            progress.update(task_id, description=f"[green]{filename[:30]} (completo)[/green]",
                                            completed=remote_size, style="green")
                            skip_download = True
                            skip_reason = "same-size"
                            logger.info(f"Arquivo {filename} tem o mesmo tamanho do remoto, assumindo que está completo")
                    elif local_size < remote_size:
                        if remote_last_modified is not None:
                            local_last_modified = int(os.path.getmtime(destination_path))
                            if local_last_modified >= remote_last_modified:
                                logger.warning(
                                    f"Arquivo local parcial {filename} é mais recente que o remoto? Baixando completo.")
                                file_mode = 'wb'
                                initial_size = 0
                            else:
                                logger.info(f"Arquivo local parcial {filename} encontrado. Tentando retomar download.")
                                progress.update(task_id, description=f"[cyan]{filename[:30]} (retomando?)[/cyan]",
                                                style="cyan")
                                attempt_resume = True
                                file_mode = 'ab'
                                initial_size = local_size
                                resume_header = {'Range': f'bytes={local_size}-'}
                        else:
                            logger.info(f"Arquivo local parcial {filename} encontrado (sem data remota). Tentando retomar.")
                            progress.update(task_id, description=f"[cyan]{filename[:30]} (retomando?)[/cyan]",
                                            style="cyan")
                            attempt_resume = True
                            file_mode = 'ab'
                            initial_size = local_size
                            resume_header = {'Range': f'bytes={local_size}-'}
                    elif local_size > remote_size:
                        logger.warning(
                            f"Arquivo local {filename} maior que remoto? Local: {local_size}, Remoto: {remote_size}")
                        file_mode = 'wb'
                        initial_size = 0

                if not skip_download:
                    # 3. Baixar arquivo
                    if attempt_resume:
                        try:
                            async with session.get(url, headers=resume_header) as response:
                                if response.status == 206:  # Partial Content
                                    logger.info(f"Retomando download de {filename} a partir do byte {initial_size}")
                                    result, error = await _process_download_response(
                                        response, destination_path, file_mode, progress, task_id,
                                        remote_size, initial_size, filename, remote_last_modified)
                                    if error is None:
                                        # Registrar estatística de download bem-sucedido
                                        download_end_time = time.time()
                                        global_stats.add_download_stat(
                                            filename=filename,
                                            url=url,
                                            size_bytes=file_size,
                                            start_time=download_start_time,
                                            end_time=download_end_time,
                                            success=True
                                        )
                                        return result, None, skip_reason
                                else:
                                    logger.warning(f"Servidor não suporta retomada para {filename}. Baixando completo.")
                                    file_mode = 'wb'
                                    initial_size = 0
                        except Exception as e_resume:
                            logger.warning(f"Erro ao tentar retomar {filename}: {e_resume}. Baixando completo.")
                            file_mode = 'wb'
                            initial_size = 0

                    # Download completo (seja porque resume falhou ou não era necessário)
                    if file_mode == 'wb':
                        progress.update(task_id, description=f"[cyan]{filename[:30]} (baixando...)[/cyan]")
                        async with session.get(url) as response:
                            if response.status == 200:
                                result, error = await _process_download_response(
                                    response, destination_path, file_mode, progress, task_id,
                                    remote_size, initial_size, filename, remote_last_modified)
                                if error is None:
                                    # Registrar estatística de download bem-sucedido
                                    download_end_time = time.time()
                                    global_stats.add_download_stat(
                                        filename=filename,
                                        url=url,
                                        size_bytes=file_size,
                                        start_time=download_start_time,
                                        end_time=download_end_time,
                                        success=True
                                    )
                                    return result, None, skip_reason
                            else:
                                raise aiohttp.ClientError(
                                    f"Status inesperado ao baixar {filename}: {response.status}")
                else:
                    # Arquivo foi pulado (cache ou up-to-date)
                    download_end_time = time.time()
                    global_stats.add_download_stat(
                        filename=filename,
                        url=url,
                        size_bytes=file_size,
                        start_time=download_start_time,
                        end_time=download_end_time,
                        success=True,
                        skip_reason=skip_reason
                    )
                    return destination_path, None, skip_reason

        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionResetError) as e:
            last_error = e
            retry_count += 1
            if retry_count < max_retries:
                wait_time = 2 ** retry_count  # Backoff exponencial: 2, 4, 8 segundos
                logger.warning(f"Tentativa {retry_count} de {max_retries} para {filename} falhou. "
                             f"Aguardando {wait_time} segundos antes de tentar novamente. Erro: {e}")
                progress.update(task_id, description=f"[yellow]{filename[:30]} (tentativa {retry_count})[/yellow]")
                await asyncio.sleep(wait_time)
            continue
        except Exception as e:
            last_error = e
            break
        finally:
            if created_session:
                await session.close()

    # Se chegou aqui, todas as tentativas falharam
    error_msg = f"Todas as tentativas falharam para {filename}. Último erro: {last_error}"
    logger.error(error_msg)
    progress.update(task_id, description=f"[red]{filename[:30]} (ERRO final)[/red]", style="red")
    
    # Registrar estatística de download falhado
    download_end_time = time.time()
    global_stats.add_download_stat(
        filename=filename,
        url=url,
        size_bytes=file_size,
        start_time=download_start_time,
        end_time=download_end_time,
        success=False,
        error=str(last_error)
    )
    
    if config.cache.enabled:
        get_download_cache().register_file_error(filename, str(error_msg))
    
    return filename, last_error, None


async def get_remote_file_metadata(session: aiohttp.ClientSession, url: str) -> Tuple[int | None, int | None]:
    """Obtém tamanho e timestamp de modificação de um arquivo remoto via HEAD request."""
    try:
        async with session.head(url, timeout=30, allow_redirects=True) as response:
            response.raise_for_status()
            remote_size = int(response.headers.get('Content-Length', 0))
            last_modified_str = response.headers.get('Last-Modified')
            if last_modified_str:
                # Parseia o timestamp - Exemplo: 'Wed, 21 Oct 2015 07:28:00 GMT'
                # Nota: O formato pode variar, ajuste se necessário.
                try:
                    # Tenta formato RFC 1123 (mais comum)
                    dt_obj = datetime.datetime.strptime(last_modified_str, '%a, %d %b %Y %H:%M:%S GMT')
                except ValueError:
                    # Tentar outros formatos se necessário ou logar um erro
                    logger.warning(f"Formato inesperado de Last-Modified: {last_modified_str} para {url}")
                    return remote_size, None
                timestamp_last_modified = int(dt_obj.timestamp())
                return remote_size, timestamp_last_modified
            else:
                logger.warning(f"Cabeçalho Last-Modified não encontrado para {url}")
                return remote_size, None
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
        logger.error(f"Erro ao obter metadados de {url}: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Erro inesperado ao obter metadados de {url}: {e}")
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
    """
    Formata o tempo decorrido em horas, minutos e segundos.
    
    Args:
        seconds: Tempo em segundos
        
    Returns:
        str: Tempo formatado (ex: "2h 15min 30s" ou "15min 30s" ou "30s")
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:  # Mostra minutos se tiver horas
        parts.append(f"{minutes}min")
    parts.append(f"{secs}s")
    
    return " ".join(parts)


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
    Baixa múltiplos arquivos de forma assíncrona com pipeline otimizado.
    
    Args:
        urls: Lista de URLs para baixar
        path_zip: Diretório onde salvar os arquivos zip
        path_unzip: Diretório onde extrair os arquivos
        path_parquet: Diretório onde salvar os arquivos parquet
        force_download: Se True, força o download mesmo se o arquivo já existir
        max_concurrent_downloads: Número máximo de downloads simultâneos
        max_concurrent_processing: Número máximo de processamentos simultâneos
        
    Returns:
        Tuple[List[str], List[Tuple[str, Exception]]]: (arquivos processados, falhas)
    """
    global download_semaphore, process_semaphore
    
    # Configurar semáforos adaptativos baseados na configuração
    if config.pipeline.adaptive_concurrency:
        max_concurrent_downloads = min(max_concurrent_downloads, config.pipeline.max_download_workers)
        max_concurrent_processing = min(max_concurrent_processing, config.pipeline.max_process_workers)
    
    download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
    process_semaphore = asyncio.Semaphore(max_concurrent_processing)
    
    # Inicializar listas de resultados
    downloaded_files = []
    processed_files = []
    failed_downloads = []
    failed_processing = []
    
    # Fila para comunicação entre download e processamento
    processing_queue = asyncio.Queue()
    
    # Iniciar monitoramento de recursos
    monitor_task = None
    if config.pipeline.enable_resource_monitoring:
        monitor_task = asyncio.create_task(resource_monitor.start_monitoring())
        logger.info("🔍 Sistema de monitoramento de recursos ativado")
    
    # Inicializar estatísticas globais
    global_stats.reset()
    start_time = time.time()
    
    # ORDENAR ARQUIVOS POR TAMANHO (MENOR PRIMEIRO)
    logger.info("📏 Ordenando arquivos por tamanho para otimizar pipeline...")
    try:
        sorted_urls_with_sizes = await get_file_sizes_and_sort(urls)
        sorted_urls = [url for url, size in sorted_urls_with_sizes]
        logger.info("✅ Arquivos ordenados por tamanho (menores primeiro)")
    except Exception as e:
        logger.warning(f"Erro ao ordenar arquivos por tamanho: {e}. Usando ordem original.")
        sorted_urls = urls
    
    print(f"\n🚀 Iniciando pipeline otimizado para {len(sorted_urls)} arquivos")
    print(f"📊 Configuração: {max_concurrent_downloads} downloads | {max_concurrent_processing} processamentos")
    print(f"🧠 Streaming: {'✅' if config.pipeline.enable_streaming else '❌'}")
    print(f"📈 Monitoramento adaptativo: {'✅' if config.pipeline.adaptive_concurrency else '❌'}")
    print(f"📏 Ordenação por tamanho: ✅ (menores primeiro)")
    
    # Função que processa os arquivos da fila em paralelo
    async def process_files_from_queue():
        """Consome a fila de processamento e processa os arquivos em tempo real."""
        processor_id = id(asyncio.current_task())
        processor_name = f"Worker-{processor_id % 1000}"  # Nome mais legível
        
        # Log visível no console
        print(f"\n🔧 {processor_name} iniciado e aguardando arquivos...")
        logger.info(f"Iniciando processador {processor_id}")
        
        while True:
            try:
                # Aguardar um arquivo da fila (com timeout para evitar travamento)
                try:
                    file_path = await asyncio.wait_for(processing_queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    # Verificar se ainda há downloads ativos
                    if 'download_tasks' in locals() and not any(not task.done() for task in download_tasks if hasattr(task, 'done')):
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
                            print(f"✅ {processor_name}: {filename} processado com sucesso via streaming!")
                            logger.info(f"Processador {processor_id} concluiu processamento streaming de {filename}")
                        else:
                            failed_processing.append((result, error))
                            print(f"❌ {processor_name}: Falha no processamento streaming de {filename} - {error}")
                            logger.warning(f"Processador {processor_id}: Falha no processamento streaming de {filename}: {error}")
                except Exception as e:
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
        try:
            async with download_semaphore:
                filename = os.path.basename(url)
                print(f"⬇️  Iniciando download otimizado: {filename}")
                
                # Criar caminho de destino
                destination_path = os.path.join(path_zip, filename)
                
                # Criar sessão HTTP temporária para este download
                async with aiohttp.ClientSession() as session:
                    # Usar Progress simples sem display para evitar conflitos
                    progress = Progress(console=None, disable=True)  # Progress desabilitado
                    task_id = progress.add_task(f"Downloading {filename}", total=100)
                    
                    # Chamar download_file com todos os argumentos necessários
                    file_path, error, skip_reason = await download_file(
                        session, url, destination_path, download_semaphore, progress, task_id
                    )
                
                if error is None:
                    downloaded_files.append(file_path)
                    # Adicionar à fila de processamento imediatamente
                    await processing_queue.put(file_path)
                    print(f"✅ Download concluído e enfileirado: {filename}")
                    logger.info(f"Arquivo {filename} baixado e adicionado à fila de processamento")
                else:
                    failed_downloads.append((filename, error))
                    print(f"❌ Falha no download: {filename} - {error}")
                    logger.error(f"Falha no download de {filename}: {error}")
        except Exception as e:
            filename = os.path.basename(url)
            failed_downloads.append((filename, e))
            print(f"💥 Erro crítico no download: {filename} - {e}")
            logger.error(f"Erro crítico no download de {filename}: {e}")

    try:
        # Iniciar workers de processamento
        processing_tasks = [
            asyncio.create_task(process_files_from_queue()) 
            for _ in range(max_concurrent_processing)
        ]
        
        # Iniciar downloads
        download_tasks = [
            asyncio.create_task(download_and_queue(url)) 
            for url in sorted_urls
        ]
        
        print(f"\n🔄 Executando {len(download_tasks)} downloads e {len(processing_tasks)} processadores...")
        
        # Aguardar todos os downloads terminarem
        await asyncio.gather(*download_tasks, return_exceptions=True)
        print("📥 Todos os downloads finalizados")
        
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
        resource_stats = resource_monitor.get_stats_summary()
        
        # Relatório final otimizado
        print(f"\n📊 === RELATÓRIO FINAL DO PIPELINE OTIMIZADO ===")
        print(f"⏱️  Tempo total: {format_elapsed_time(total_time)}")
        print(f"📥 Downloads: {len(downloaded_files)} sucessos, {len(failed_downloads)} falhas")
        print(f"🔄 Processamentos: {len(processed_files)} sucessos, {len(failed_processing)} falhas")
        
        if isinstance(resource_stats, dict):
            print(f"💻 CPU médio: {resource_stats['cpu_avg']:.1f}% (máx: {resource_stats['cpu_max']:.1f}%)")
            print(f"🧠 Memória média: {resource_stats['memory_avg']:.1f}% (máx: {resource_stats['memory_max']:.1f}%)")
        
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
