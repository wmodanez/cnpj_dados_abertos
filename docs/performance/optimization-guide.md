# 🚀 Guia de Otimização - Sistema de Processadores RF

## 📋 Visão Geral

Este guia oferece estratégias práticas para otimizar performance do sistema refatorado, cobrindo desde configurações básicas até técnicas avançadas de tunning específicas para processamento de dados da RF.

**Áreas de Otimização:**
- ✅ **Configuração de Sistema**: OS, hardware, Python
- ✅ **Processadores**: Tunning específico por tipo
- ✅ **Infraestrutura**: ResourceMonitor, QueueManager
- ✅ **Memória e E/O**: Otimizações de disco e RAM
- ✅ **Paralelismo**: Workers, threads, processamento concorrente

## 🔧 Configuração de Sistema

### Hardware Recomendado

#### Configuração Mínima
```yaml
CPU: 4 núcleos (2.0GHz+)
RAM: 8GB DDR4
Disco: SSD 500GB
OS: Windows 10+ / Linux Ubuntu 20+
Python: 3.9+
```

#### Configuração Otimizada
```yaml
CPU: 8+ núcleos (3.0GHz+)
RAM: 32GB DDR4 3200MHz
Disco: NVME SSD 1TB+
OS: Windows 11 / Linux Ubuntu 22+
Python: 3.10+ (performance melhorada)
```

#### Configuração High-Performance
```yaml
CPU: 16+ núcleos (3.5GHz+)
RAM: 64GB DDR4 3600MHz
Disco: NVME SSD RAID 0
OS: Otimizado para E/O
Python: 3.11+ com PGO enabled
```

### Configuração do Python

#### Otimizações de Runtime

```python
# Configurar garbage collector para melhor performance
import gc

# Reduzir frequência do GC para dados grandes
gc.set_threshold(1000, 15, 15)

# Para processamento intensivo, desabilitar temporariamente
def optimize_gc_for_processing():
    gc.disable()
    try:
        # Seu processamento aqui
        yield
    finally:
        gc.enable()
        gc.collect()
```

#### Configuração de Polars

```python
import polars as pl

# Configurar Polars para máxima performance
pl.Config.set_tbl_rows(1000)  # Mais linhas em outputs
pl.Config.set_tbl_cols(20)    # Mais colunas em outputs
pl.Config.set_tbl_width_chars(120)  # Largura maior

# Configurar paralelismo
import os
os.environ["POLARS_MAX_THREADS"] = str(os.cpu_count())
```

## ⚙️ Otimização dos Processadores

### ResourceMonitor

#### Configuração Padrão vs Otimizada

```python
from src.process.base.resource_monitor import ResourceMonitor, ResourceThresholds

# Configuração padrão (conservadora)
monitor_default = ResourceMonitor()

# Configuração otimizada (mais agressiva)
thresholds_optimized = ResourceThresholds(
    max_cpu_percent=85.0,     # Usar mais CPU (padrão: 80%)
    max_memory_percent=90.0,  # Usar mais RAM (padrão: 80%)
    max_disk_percent=95.0,    # Usar mais disco (padrão: 90%)
    min_memory_gb=1.0         # Menos RAM livre (padrão: 2GB)
)

monitor_optimized = ResourceMonitor(thresholds_optimized)
```

#### Configuração para SSDs

```python
# Para SSDs NVME (sem limitação de E/O)
thresholds_ssd = ResourceThresholds(
    max_cpu_percent=95.0,     # CPU quase máxima
    max_memory_percent=95.0,  # RAM quase máxima
    max_disk_percent=98.0,    # Disco quase máximo (SSD aguenta)
    min_memory_gb=0.5         # Mínimo absoluto
)
```

### ProcessingQueueManager

#### Calculando Workers Ótimos

```python
from src.process.base.queue_manager import ProcessingQueueManager
from src.process.base.resource_monitor import ResourceMonitor

def calculate_optimal_workers():
    monitor = ResourceMonitor()
    resources = monitor.get_system_resources_dict()
    
    # Fórmula otimizada baseada em recursos
    cpu_factor = resources['cpu_count']
    memory_factor = int(resources['memory_available'] // 2)  # 2GB por worker
    
    # Para SSDs, usar mais workers (I/O não é limitante)
    optimal = min(cpu_factor, memory_factor)
    
    # Ajuste por tipo de processamento
    if is_cpu_intensive():
        optimal = max(1, optimal // 2)  # CPU intensivo: menos workers
    elif is_memory_intensive():
        optimal = max(1, optimal // 3)  # Memória intensiva: ainda menos
    else:
        optimal = optimal  # I/O intensivo: usar todos
    
    return optimal
```

#### Configuração Avançada de Fila

```python
# Fila com prioridades otimizada
queue_manager = ProcessingQueueManager(
    processor_name="EMPRESA",
    max_workers=calculate_optimal_workers()
)

# Estratégia de priorização por tamanho
def add_files_with_smart_priority(files):
    for file_path in files:
        file_size = os.path.getsize(file_path)
        
        # Arquivos pequenos: alta prioridade (processam rápido)
        if file_size < 50 * 1024 * 1024:  # < 50MB
            priority = 10
        # Arquivos médios: prioridade normal
        elif file_size < 200 * 1024 * 1024:  # < 200MB
            priority = 5
        # Arquivos grandes: baixa prioridade (deixar por último)
        else:
            priority = 1
        
        queue_manager.add_to_queue(file_path, priority=priority)
```

## 📊 Otimização por Tipo de Processador

### SocioProcessor (I/O Intensive)

```python
# Configuração otimizada para I/O
class OptimizedSocioProcessor(SocioProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Configurações específicas para I/O
        self.chunk_size = 50000  # Chunks maiores para I/O
        self.buffer_size = 1024 * 1024  # Buffer de 1MB
        
    def optimize_for_ssd(self):
        # Para SSDs: aumentar paralelismo de leitura
        self.chunk_size = 100000
        self.concurrent_reads = 4
```

### SimplesProcessor (CPU Intensive)

```python
# Otimização para processamento CPU-intensivo
class OptimizedSimples Processor(SimplesProcessor):
    def optimize_for_cpu(self):
        # Usar menos workers mas chunks maiores
        optimal_workers = max(1, os.cpu_count() // 2)
        self.chunk_size = 20000  # Chunks menores para CPU
        
        # Otimizar conversões S/N com vectorização
        import numpy as np
        
        def vectorized_sn_conversion(series):
            # Usar numpy para conversão mais rápida
            return np.where(series == "S", 1, 0)
```

### EmpresaProcessor (Memory Intensive)

```python
# Otimização para processamento que usa muita memória
class OptimizedEmpresaProcessor(EmpresaProcessor):
    def optimize_for_memory(self):
        # Chunks menores para controlar memória
        self.chunk_size = 10000
        
        # Processar em batches com limpeza
        def process_with_memory_management(self, df):
            batch_size = 5000
            results = []
            
            for i in range(0, len(df), batch_size):
                batch = df[i:i + batch_size]
                processed = self.apply_transformations(batch)
                results.append(processed)
                
                # Limpar memória a cada batch
                del batch
                gc.collect()
            
            return pl.concat(results)
```

## 🚀 Otimizações de Performance

### Lazy Loading e Streaming

```python
# Lazy loading para arquivos grandes
import polars as pl

def optimized_lazy_processing(file_path):
    # Usar lazy frame para não carregar tudo na memória
    lazy_df = pl.scan_csv(file_path)
    
    # Aplicar transformações lazy
    processed = (
        lazy_df
        .filter(pl.col("cnpj_basico").is_not_null())
        .with_columns([
            pl.col("razao_social").str.upper().alias("razao_social_upper")
        ])
        .select([
            "cnpj_basico",
            "razao_social_upper",
            # ... outros campos
        ])
    )
    
    # Coletar apenas quando necessário
    return processed.collect()
```

### Cache Inteligente

```python
from functools import lru_cache
import pickle
import hashlib

class SmartCache:
    def __init__(self, cache_dir="/tmp/processor_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
    
    def get_cache_key(self, file_path, processor_type):
        # Hash baseado no arquivo e processador
        with open(file_path, 'rb') as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
        return f"{processor_type}_{file_hash}.pkl"
    
    @lru_cache(maxsize=100)
    def get_cached_result(self, cache_key):
        cache_file = self.cache_dir / cache_key
        if cache_file.exists():
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        return None
    
    def save_result(self, cache_key, result):
        cache_file = self.cache_dir / cache_key
        with open(cache_file, 'wb') as f:
            pickle.dump(result, f)

# Usar cache em processadores
def process_with_cache(self, zip_file):
    cache = SmartCache()
    cache_key = cache.get_cache_key(zip_file, self.get_processor_name())
    
    # Tentar cache primeiro
    result = cache.get_cached_result(cache_key)
    if result:
        return result
    
    # Processar se não em cache
    result = self.process_single_zip_impl(zip_file)
    
    # Salvar no cache
    cache.save_result(cache_key, result)
    return result
```

### Paralelismo Avançado

```python
import asyncio
import concurrent.futures

class AdvancedParallelProcessor:
    def __init__(self, max_workers=None):
        self.max_workers = max_workers or os.cpu_count()
        
    async def process_multiple_files_async(self, files, processor_type):
        # Usar ProcessPoolExecutor para CPU-bound tasks
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            # Criar tasks para cada arquivo
            tasks = []
            for file_path in files:
                task = asyncio.create_task(
                    self.process_single_file_async(
                        executor, file_path, processor_type
                    )
                )
                tasks.append(task)
            
            # Aguardar todos completarem
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results
    
    async def process_single_file_async(self, executor, file_path, processor_type):
        loop = asyncio.get_event_loop()
        
        # Executar processamento em thread separada
        result = await loop.run_in_executor(
            executor,
            self.process_file_sync,
            file_path,
            processor_type
        )
        return result
```

## 💾 Otimizações de Memória

### Monitoramento de Memória

```python
import psutil
import tracemalloc

class MemoryOptimizer:
    def __init__(self):
        self.memory_threshold = 0.85  # 85% da RAM
    
    def monitor_memory_usage(self):
        """Monitor contínuo de uso de memória"""
        memory = psutil.virtual_memory()
        if memory.percent > self.memory_threshold * 100:
            self.trigger_memory_cleanup()
    
    def trigger_memory_cleanup(self):
        """Limpeza agressiva de memória"""
        gc.collect()
        
        # Forçar limpeza de caches do Polars
        pl._internal.clear_cache()
        
        # Limpar caches Python
        import functools
        for obj in gc.get_objects():
            if hasattr(obj, 'cache_clear'):
                obj.cache_clear()
    
    def profile_memory_usage(self, func):
        """Decorator para profile de memória"""
        def wrapper(*args, **kwargs):
            tracemalloc.start()
            
            # Memória antes
            before = tracemalloc.get_traced_memory()[0]
            
            result = func(*args, **kwargs)
            
            # Memória depois
            after = tracemalloc.get_traced_memory()[0]
            tracemalloc.stop()
            
            print(f"Memória usada: {(after - before) / 1024 / 1024:.2f}MB")
            return result
        
        return wrapper
```

### Estratégias de Chunk Size

```python
class AdaptiveChunkSizer:
    def __init__(self):
        self.base_chunk_size = 10000
        self.memory_monitor = MemoryOptimizer()
    
    def calculate_optimal_chunk_size(self, file_size, available_memory):
        """Calcula chunk size baseado em recursos disponíveis"""
        
        # Estimar linhas no arquivo (assumindo ~200 bytes por linha)
        estimated_lines = file_size // 200
        
        # Calcular chunk baseado na memória disponível
        memory_mb = available_memory / 1024 / 1024
        
        # Usar 10% da memória disponível por chunk
        memory_based_chunk = int((memory_mb * 0.1) * 1000)
        
        # Limitar entre mín e máx
        optimal_chunk = max(
            1000,  # Mínimo
            min(memory_based_chunk, 100000)  # Máximo
        )
        
        return optimal_chunk
    
    def adaptive_process_file(self, file_path, processor):
        """Processamento adaptativo baseado em recursos"""
        file_size = os.path.getsize(file_path)
        memory = psutil.virtual_memory()
        
        chunk_size = self.calculate_optimal_chunk_size(
            file_size, 
            memory.available
        )
        
        return processor.process_file_chunked(file_path, chunk_size)
```

## 🗄️ Otimizações de E/O

### Configuração de Disco

```python
class DiskOptimizer:
    @staticmethod
    def optimize_for_ssd():
        """Configurações específicas para SSDs"""
        return {
            'buffer_size': 2 * 1024 * 1024,  # 2MB buffer
            'concurrent_reads': 8,            # Mais leituras simultâneas
            'prefetch_size': 10,              # Prefetch mais arquivos
            'compression_level': 1            # Compressão mínima (SSD é rápido)
        }
    
    @staticmethod
    def optimize_for_hdd():
        """Configurações específicas para HDDs"""
        return {
            'buffer_size': 512 * 1024,       # 512KB buffer (menor)
            'concurrent_reads': 2,            # Menos concorrência
            'prefetch_size': 3,               # Prefetch menor
            'compression_level': 6            # Mais compressão (I/O é lento)
        }
    
    def detect_disk_type(self, path):
        """Detecta tipo de disco automaticamente"""
        # Heurística: medir latência de acesso aleatório
        import time
        
        test_file = os.path.join(path, "disk_test.tmp")
        
        # Criar arquivo de teste
        with open(test_file, 'wb') as f:
            f.write(b'0' * 1024 * 1024)  # 1MB
        
        try:
            # Teste de acesso aleatório
            start = time.time()
            with open(test_file, 'rb') as f:
                for _ in range(100):
                    f.seek(random.randint(0, 1024 * 1024))
                    f.read(1024)
            latency = time.time() - start
            
            # SSD: < 0.1s, HDD: > 0.5s
            return "SSD" if latency < 0.2 else "HDD"
        
        finally:
            os.remove(test_file)
```

### Async File Operations

```python
import aiofiles
import asyncio

class AsyncFileProcessor:
    async def read_zip_async(self, zip_path):
        """Leitura assíncrona de arquivo ZIP"""
        async with aiofiles.open(zip_path, 'rb') as f:
            content = await f.read()
        
        # Processar ZIP em thread separada
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            extracted_data = await loop.run_in_executor(
                executor,
                self.extract_zip_content,
                content
            )
        
        return extracted_data
    
    async def write_parquet_async(self, df, output_path):
        """Escrita assíncrona de parquet"""
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            await loop.run_in_executor(
                executor,
                df.write_parquet,
                output_path
            )
```

## 📈 Monitoramento de Performance

### Profiling Integrado

```python
import cProfile
import pstats
from contextlib import contextmanager

class PerformanceProfiler:
    def __init__(self):
        self.profiles = {}
    
    @contextmanager
    def profile_function(self, name):
        """Context manager para profiling"""
        profiler = cProfile.Profile()
        profiler.enable()
        
        try:
            yield
        finally:
            profiler.disable()
            
            # Salvar stats
            stats = pstats.Stats(profiler)
            self.profiles[name] = stats
            
            # Log top 10 funções mais lentas
            stats.sort_stats('cumulative')
            stats.print_stats(10)
    
    def generate_performance_report(self):
        """Gera relatório de performance"""
        report = {}
        for name, stats in self.profiles.items():
            stats.sort_stats('cumulative')
            
            # Extrair métricas principais
            total_time = stats.total_tt
            call_count = stats.total_calls
            
            report[name] = {
                'total_time': total_time,
                'call_count': call_count,
                'avg_time_per_call': total_time / call_count if call_count > 0 else 0
            }
        
        return report

# Usar profiler nos processadores
profiler = PerformanceProfiler()

with profiler.profile_function("empresa_processing"):
    processor = EmpresaProcessor(...)
    result = processor.process_single_zip("arquivo.zip")
```

### Métricas em Tempo Real

```python
import time
from collections import defaultdict

class RealTimeMetrics:
    def __init__(self):
        self.metrics = defaultdict(list)
        self.start_times = {}
    
    def start_timer(self, operation):
        """Inicia timer para operação"""
        self.start_times[operation] = time.time()
    
    def end_timer(self, operation):
        """Finaliza timer e registra métrica"""
        if operation in self.start_times:
            elapsed = time.time() - self.start_times[operation]
            self.metrics[operation].append(elapsed)
            del self.start_times[operation]
    
    def get_avg_time(self, operation):
        """Tempo médio de uma operação"""
        times = self.metrics[operation]
        return sum(times) / len(times) if times else 0
    
    def get_throughput(self, operation, items_processed):
        """Calcula throughput (items/segundo)"""
        avg_time = self.get_avg_time(operation)
        return items_processed / avg_time if avg_time > 0 else 0
    
    def print_dashboard(self):
        """Dashboard em tempo real"""
        print("\n" + "="*50)
        print("📊 PERFORMANCE DASHBOARD")
        print("="*50)
        
        for operation, times in self.metrics.items():
            if times:
                avg_time = sum(times) / len(times)
                print(f"{operation:20}: {avg_time:.3f}s avg ({len(times)} samples)")
```

## 🎯 Configurações de Produção

### Configuração High-Performance

```python
# settings_production.py
import os

class ProductionSettings:
    # Configurações de sistema
    POLARS_MAX_THREADS = os.cpu_count()
    PYTHON_GC_THRESHOLD = (1000, 15, 15)
    
    # Configurações de processamento
    MAX_WORKERS = min(16, os.cpu_count())
    CHUNK_SIZE = 50000
    MEMORY_THRESHOLD = 0.90
    
    # Configurações de cache
    ENABLE_CACHE = True
    CACHE_SIZE_MB = 1000
    CACHE_DIR = "/fast_disk/cache"
    
    # Configurações de monitoramento
    ENABLE_PROFILING = False  # Desabilitar em produção
    LOG_LEVEL = "INFO"
    METRICS_ENABLED = True
    
    # Configurações de E/O
    BUFFER_SIZE = 2 * 1024 * 1024  # 2MB
    CONCURRENT_READS = 8
    COMPRESSION_LEVEL = 1  # Mínimo para SSD

def apply_production_settings():
    """Aplica configurações de produção"""
    import polars as pl
    import gc
    
    # Configurar Polars
    os.environ["POLARS_MAX_THREADS"] = str(ProductionSettings.POLARS_MAX_THREADS)
    
    # Configurar GC
    gc.set_threshold(*ProductionSettings.PYTHON_GC_THRESHOLD)
    
    # Configurar ResourceMonitor
    from src.process.base.resource_monitor import ResourceThresholds
    
    return ResourceThresholds(
        max_cpu_percent=95.0,
        max_memory_percent=ProductionSettings.MEMORY_THRESHOLD * 100,
        max_disk_percent=98.0,
        min_memory_gb=0.5
    )
```

### Script de Inicialização Otimizada

```python
#!/usr/bin/env python3
"""
Script de inicialização otimizada para produção
"""

def initialize_optimized_environment():
    """Configura ambiente otimizado"""
    
    # 1. Aplicar configurações de produção
    apply_production_settings()
    
    # 2. Pré-aquecer caches
    warm_up_caches()
    
    # 3. Registrar todos os processadores
    register_all_processors()
    
    # 4. Configurar monitoramento
    setup_monitoring()
    
    print("✅ Ambiente otimizado inicializado com sucesso!")

def warm_up_caches():
    """Pré-aquece caches do sistema"""
    from src.process.base.factory import ProcessorFactory
    
    # Criar uma instância de cada processador para cache warming
    dummy_dirs = ["/tmp"] * 3
    
    for processor_type in ["socio", "simples", "empresa", "estabelecimento"]:
        try:
            ProcessorFactory.create(processor_type, *dummy_dirs)
            print(f"✅ Cache warm-up: {processor_type}")
        except:
            pass  # Falhas de warm-up não são críticas

if __name__ == "__main__":
    initialize_optimized_environment()
```

---

**💡 Este guia de otimização maximiza a performance do sistema refatorado, oferecendo configurações específicas para diferentes cenários de uso e recursos de hardware disponíveis.** 