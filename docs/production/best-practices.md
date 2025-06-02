# 🎯 Melhores Práticas - Sistema de Processadores RF

## 📋 Visão Geral

Este documento estabelece as melhores práticas para desenvolvimento, manutenção e operação do sistema refatorado, garantindo qualidade, performance e sustentabilidade a longo prazo.

**Áreas Cobertas:**
- ✅ **Desenvolvimento**: Padrões de código e arquitetura
- ✅ **Testes**: Estratégias de validação e qualidade
- ✅ **Performance**: Otimização e monitoramento
- ✅ **Segurança**: Proteção e conformidade
- ✅ **Operação**: Deploy, monitoramento e manutenção

## 💻 Práticas de Desenvolvimento

### Estrutura de Código

#### Organização de Arquivos

```python
# ✅ BOM: Estrutura clara e hierárquica
src/
├── process/
│   ├── base/           # Infraestrutura central
│   └── processors/     # Processadores específicos
├── Entity/             # Sistema de entidades
└── utils/              # Utilitários compartilhados

# ❌ RUIM: Estrutura plana e confusa
src/
├── all_processors.py
├── utils_and_entities.py
└── everything_else.py
```

#### Nomenclatura Consistente

```python
# ✅ BOM: Nomes descritivos e consistentes
class SocioProcessor(BaseProcessor):
    def process_single_zip(self, zip_file: str) -> bool:
        return self._process_zip_internal(zip_file)
    
    def _process_zip_internal(self, zip_file: str) -> bool:
        # Implementação privada
        pass

# ❌ RUIM: Nomes vagos e inconsistentes
class SP(BaseProcessor):
    def proc(self, f):
        return self.do_stuff(f)
```

#### Type Hints Obrigatórios

```python
# ✅ BOM: Type hints completos
from typing import List, Dict, Optional, Union
import polars as pl

def process_dataframe(
    df: pl.DataFrame, 
    options: Dict[str, Union[str, bool]]
) -> Optional[pl.DataFrame]:
    """Processa DataFrame com opções específicas."""
    pass

# ❌ RUIM: Sem type hints
def process_dataframe(df, options):
    pass
```

### Padrões de Design

#### Factory Pattern (Já Implementado)

```python
# ✅ BOM: Usar ProcessorFactory
from src.process.base.factory import ProcessorFactory

# Registrar processador
ProcessorFactory.register("custom", CustomProcessor)

# Criar processador
processor = ProcessorFactory.create("custom", zip_dir, unzip_dir, parquet_dir)

# ❌ RUIM: Instanciação direta
processor = CustomProcessor(zip_dir, unzip_dir, parquet_dir)
```

#### Strategy Pattern para Transformações

```python
# ✅ BOM: Strategy pattern para diferentes transformações
class TransformationStrategy(ABC):
    @abstractmethod
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        pass

class SimplesTransformation(TransformationStrategy):
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        # Conversões S/N específicas do Simples
        return df.with_columns([
            pl.when(pl.col("opcao_simples") == "S").then(1).otherwise(0)
        ])

# ❌ RUIM: Lógica hardcoded no processador
def transform_data(self, df):
    if self.processor_type == "simples":
        # código duplicado...
    elif self.processor_type == "empresa":
        # mais código duplicado...
```

#### Dependency Injection

```python
# ✅ BOM: Injeção de dependências
class SocioProcessor(BaseProcessor):
    def __init__(
        self, 
        zip_dir: str, 
        unzip_dir: str, 
        parquet_dir: str,
        resource_monitor: Optional[ResourceMonitor] = None,
        queue_manager: Optional[ProcessingQueueManager] = None
    ):
        super().__init__(zip_dir, unzip_dir, parquet_dir)
        self.resource_monitor = resource_monitor or ResourceMonitor()
        self.queue_manager = queue_manager

# ❌ RUIM: Dependências hardcoded
class SocioProcessor(BaseProcessor):
    def __init__(self, zip_dir, unzip_dir, parquet_dir):
        super().__init__(zip_dir, unzip_dir, parquet_dir)
        self.resource_monitor = ResourceMonitor()  # Acoplamento forte
```

## 🧪 Práticas de Testes

### Estrutura de Testes

```python
# tests/
├── unit/                    # Testes unitários
│   ├── test_entities/
│   ├── test_processors/
│   └── test_infrastructure/
├── integration/             # Testes de integração
│   ├── test_processor_integration/
│   └── test_end_to_end/
├── performance/             # Testes de performance
│   └── test_benchmarks.py
└── fixtures/                # Dados de teste
    ├── sample_socio.zip
    └── sample_empresa.zip
```

### Testes Unitários

```python
# ✅ BOM: Testes focados e isolados
import pytest
from unittest.mock import Mock, patch
from src.process.processors.socio_processor import SocioProcessor

class TestSocioProcessor:
    @pytest.fixture
    def mock_resource_monitor(self):
        monitor = Mock()
        monitor.get_system_resources_dict.return_value = {
            'cpu_count': 4,
            'memory_available': 8.0,
            'cpu_percent': 50.0
        }
        return monitor
    
    def test_process_single_zip_success(self, mock_resource_monitor):
        """Testa processamento bem-sucedido de ZIP"""
        processor = SocioProcessor("/zip", "/unzip", "/parquet")
        processor.resource_monitor = mock_resource_monitor
        
        with patch('polars.read_csv') as mock_read:
            mock_read.return_value = Mock()
            result = processor.process_single_zip("test.zip")
            
        assert result is True
        mock_read.assert_called_once()

# ❌ RUIM: Testes que dependem de arquivos reais
def test_socio_processor():
    processor = SocioProcessor("/real/path", "/real/unzip", "/real/output")
    result = processor.process_single_zip("/real/file.zip")  # Dependência externa
    assert result
```

### Testes de Integração

```python
# ✅ BOM: Testes de integração controlados
import tempfile
import pytest
from pathlib import Path

class TestProcessorIntegration:
    @pytest.fixture
    def temp_environment(self):
        """Cria ambiente temporário para testes"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            zip_dir = temp_path / "zip"
            unzip_dir = temp_path / "unzip"
            parquet_dir = temp_path / "parquet"
            
            zip_dir.mkdir()
            unzip_dir.mkdir()
            parquet_dir.mkdir()
            
            yield {
                'zip_dir': str(zip_dir),
                'unzip_dir': str(unzip_dir),
                'parquet_dir': str(parquet_dir)
            }
    
    def test_full_processing_pipeline(self, temp_environment):
        """Testa pipeline completo de processamento"""
        # Criar dados de teste
        self.create_test_data(temp_environment['zip_dir'])
        
        # Executar processamento
        from src.process.base.factory import ProcessorFactory
        processor = ProcessorFactory.create("socio", **temp_environment)
        
        result = processor.process_single_zip("test_socio.zip")
        
        # Verificar resultados
        assert result is True
        output_files = list(Path(temp_environment['parquet_dir']).glob("*.parquet"))
        assert len(output_files) > 0
```

### Testes de Performance

```python
# ✅ BOM: Benchmarks automatizados
import pytest
import time
from src.process.processors.socio_processor import SocioProcessor

class TestPerformance:
    @pytest.mark.performance
    def test_socio_processor_benchmark(self, benchmark_data):
        """Benchmark do SocioProcessor"""
        processor = SocioProcessor("/zip", "/unzip", "/parquet")
        
        start_time = time.time()
        result = processor.process_single_zip("benchmark_500_lines.zip")
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        # Assertions de performance
        assert result is True
        assert processing_time < 0.01  # Menos de 10ms
        
        # Log métricas
        throughput = 500 / processing_time
        print(f"Throughput: {throughput:.1f} linhas/segundo")

# ❌ RUIM: Testes de performance não determinísticos
def test_performance():
    # Sem controle de ambiente
    start = time.time()
    some_function()
    end = time.time()
    assert end - start < 1  # Pode falhar em máquinas lentas
```

## 🚀 Práticas de Performance

### Uso Eficiente de Recursos

```python
# ✅ BOM: Monitoramento e controle de recursos
from src.process.base.resource_monitor import ResourceMonitor

class OptimizedProcessor:
    def __init__(self):
        self.monitor = ResourceMonitor()
        
    def process_large_file(self, file_path: str):
        # Verificar recursos antes de processar
        if not self.monitor.can_start_processing(0, 1):
            self.wait_for_resources()
        
        # Processar em chunks baseado na memória disponível
        chunk_size = self.calculate_optimal_chunk_size()
        
        for chunk in self.read_file_chunks(file_path, chunk_size):
            self.process_chunk(chunk)

# ❌ RUIM: Uso descontrolado de recursos
def process_large_file(file_path):
    # Carregar arquivo inteiro na memória
    df = pl.read_csv(file_path)  # Pode causar OOM
    return process_dataframe(df)
```

### Lazy Loading e Streaming

```python
# ✅ BOM: Lazy loading com Polars
def process_large_dataset(file_path: str) -> pl.DataFrame:
    """Processa dataset grande usando lazy evaluation"""
    return (
        pl.scan_csv(file_path)
        .filter(pl.col("cnpj_basico").is_not_null())
        .with_columns([
            pl.col("razao_social").str.upper().alias("razao_social_clean")
        ])
        .select([
            "cnpj_basico",
            "razao_social_clean",
            "situacao_cadastral"
        ])
        .collect()  # Executar apenas quando necessário
    )

# ❌ RUIM: Carregamento eagerly
def process_large_dataset(file_path: str) -> pl.DataFrame:
    df = pl.read_csv(file_path)  # Carrega tudo na memória
    df = df.filter(pl.col("cnpj_basico").is_not_null())
    df = df.with_columns([pl.col("razao_social").str.upper()])
    return df.select(["cnpj_basico", "razao_social", "situacao_cadastral"])
```

### Cache Inteligente

```python
# ✅ BOM: Cache com TTL e invalidação
from functools import lru_cache
from datetime import datetime, timedelta
import hashlib

class SmartCache:
    def __init__(self, ttl_minutes: int = 60):
        self.ttl = timedelta(minutes=ttl_minutes)
        self.cache = {}
        self.timestamps = {}
    
    def get_cache_key(self, file_path: str, processor_type: str) -> str:
        """Gera chave de cache baseada no arquivo e processador"""
        file_stat = os.stat(file_path)
        content = f"{file_path}_{processor_type}_{file_stat.st_mtime}_{file_stat.st_size}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def get(self, key: str):
        """Obtém item do cache se ainda válido"""
        if key not in self.cache:
            return None
            
        if datetime.now() - self.timestamps[key] > self.ttl:
            del self.cache[key]
            del self.timestamps[key]
            return None
            
        return self.cache[key]
    
    def set(self, key: str, value):
        """Armazena item no cache"""
        self.cache[key] = value
        self.timestamps[key] = datetime.now()

# ❌ RUIM: Cache sem controle
cache = {}  # Cache global sem invalidação

def process_file(file_path):
    if file_path in cache:
        return cache[file_path]  # Pode retornar dados obsoletos
    
    result = expensive_processing(file_path)
    cache[file_path] = result  # Cache cresce indefinidamente
    return result
```

## 🔒 Práticas de Segurança

### Validação de Entrada

```python
# ✅ BOM: Validação rigorosa de entrada
from pathlib import Path
from typing import List

class SecureFileProcessor:
    ALLOWED_EXTENSIONS = {'.zip', '.csv'}
    MAX_FILE_SIZE_MB = 2048
    ALLOWED_BASE_PATHS = [
        Path("/secure/data/input"),
        Path("/opt/rf_data/input")
    ]
    
    def validate_file_path(self, file_path: str) -> bool:
        """Valida se arquivo é seguro para processamento"""
        path = Path(file_path).resolve()
        
        # Verificar se está em diretório permitido
        is_in_allowed_path = any(
            self._is_subpath(path, allowed_path) 
            for allowed_path in self.ALLOWED_BASE_PATHS
        )
        
        if not is_in_allowed_path:
            raise SecurityError(f"Arquivo fora de diretório permitido: {path}")
        
        # Verificar extensão
        if path.suffix.lower() not in self.ALLOWED_EXTENSIONS:
            raise SecurityError(f"Extensão não permitida: {path.suffix}")
        
        # Verificar tamanho
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            if size_mb > self.MAX_FILE_SIZE_MB:
                raise SecurityError(f"Arquivo muito grande: {size_mb:.1f}MB")
        
        return True
    
    @staticmethod
    def _is_subpath(path: Path, parent: Path) -> bool:
        """Verifica se path é subdiretório de parent"""
        try:
            path.relative_to(parent)
            return True
        except ValueError:
            return False

# ❌ RUIM: Sem validação de segurança
def process_file(file_path):
    # Aceita qualquer arquivo de qualquer lugar
    return expensive_processing(file_path)
```

### Logs Seguros

```python
# ✅ BOM: Logs sem informações sensíveis
import logging
import re

class SecureLogger:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def log_processing_start(self, file_path: str, user_id: str):
        """Log de início de processamento (sanitizado)"""
        safe_file_path = self._sanitize_path(file_path)
        safe_user_id = self._sanitize_user_id(user_id)
        
        self.logger.info(
            f"Iniciando processamento - File: {safe_file_path}, User: {safe_user_id}"
        )
    
    def _sanitize_path(self, path: str) -> str:
        """Remove informações sensíveis do path"""
        return Path(path).name  # Apenas nome do arquivo
    
    def _sanitize_user_id(self, user_id: str) -> str:
        """Ofusca ID do usuário"""
        if len(user_id) > 4:
            return user_id[:2] + "***" + user_id[-2:]
        return "***"

# ❌ RUIM: Logs com informações sensíveis
def process_file(file_path, user_credentials):
    logging.info(f"Processing {file_path} for user {user_credentials}")  # Expõe credenciais
```

### Sanitização de Dados

```python
# ✅ BOM: Sanitização de dados de entrada
import re
from typing import Union

class DataSanitizer:
    # Patterns para limpeza
    CPF_PATTERN = re.compile(r'[^\d]')
    CNPJ_PATTERN = re.compile(r'[^\d]')
    SQL_INJECTION_PATTERN = re.compile(r"[';\"\\]")
    
    @staticmethod
    def sanitize_cpf(cpf: str) -> str:
        """Sanitiza CPF removendo caracteres não numéricos"""
        if not cpf:
            return ""
        
        clean_cpf = DataSanitizer.CPF_PATTERN.sub('', str(cpf))
        
        # Validar comprimento
        if len(clean_cpf) != 11:
            return ""
        
        return clean_cpf
    
    @staticmethod
    def sanitize_text_field(text: str, max_length: int = 255) -> str:
        """Sanitiza campo de texto removendo caracteres perigosos"""
        if not text:
            return ""
        
        # Remover caracteres de SQL injection
        clean_text = DataSanitizer.SQL_INJECTION_PATTERN.sub('', str(text))
        
        # Limitar comprimento
        clean_text = clean_text[:max_length]
        
        # Remover espaços extras
        clean_text = ' '.join(clean_text.split())
        
        return clean_text.upper()

# ❌ RUIM: Dados não sanitizados
def process_company_data(data):
    company_name = data['name']  # Pode conter SQL injection
    cpf = data['cpf']  # Pode ter formatação inconsistente
    return save_to_database(company_name, cpf)
```

## 📊 Práticas de Monitoramento

### Métricas Estruturadas

```python
# ✅ BOM: Métricas estruturadas
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any

@dataclass
class ProcessingMetrics:
    processor_type: str
    file_name: str
    start_time: datetime
    end_time: datetime
    lines_processed: int
    success: bool
    error_message: str = ""
    
    @property
    def processing_time_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()
    
    @property
    def throughput_lines_per_second(self) -> float:
        if self.processing_time_seconds > 0:
            return self.lines_processed / self.processing_time_seconds
        return 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'processor_type': self.processor_type,
            'file_name': self.file_name,
            'processing_time_seconds': self.processing_time_seconds,
            'lines_processed': self.lines_processed,
            'throughput_lps': self.throughput_lines_per_second,
            'success': self.success,
            'timestamp': self.start_time.isoformat()
        }

class MetricsCollector:
    def __init__(self):
        self.metrics = []
    
    def record_processing(self, metrics: ProcessingMetrics):
        """Registra métricas de processamento"""
        self.metrics.append(metrics)
        
        # Enviar para sistema de monitoramento
        self._send_to_monitoring_system(metrics.to_dict())

# ❌ RUIM: Logs não estruturados
def process_file(file_path):
    start = time.time()
    # ... processamento
    end = time.time()
    
    print(f"Processed {file_path} in {end-start}s")  # Log não estruturado
```

### Health Checks Detalhados

```python
# ✅ BOM: Health checks abrangentes
from enum import Enum
from dataclasses import dataclass

class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

@dataclass
class HealthCheck:
    name: str
    status: HealthStatus
    message: str
    details: Dict[str, Any]

class SystemHealthMonitor:
    def __init__(self):
        self.resource_monitor = ResourceMonitor()
        self.factory = ProcessorFactory()
    
    def check_overall_health(self) -> List[HealthCheck]:
        """Executa todos os health checks"""
        checks = [
            self.check_system_resources(),
            self.check_processors(),
            self.check_disk_space(),
            self.check_memory_usage(),
            self.check_dependencies()
        ]
        return checks
    
    def check_system_resources(self) -> HealthCheck:
        """Verifica recursos do sistema"""
        try:
            resources = self.resource_monitor.get_system_resources_dict()
            
            if resources['cpu_percent'] > 90:
                return HealthCheck(
                    "system_resources",
                    HealthStatus.DEGRADED,
                    f"Alto uso de CPU: {resources['cpu_percent']:.1f}%",
                    resources
                )
            
            return HealthCheck(
                "system_resources",
                HealthStatus.HEALTHY,
                "Recursos dentro do normal",
                resources
            )
            
        except Exception as e:
            return HealthCheck(
                "system_resources",
                HealthStatus.UNHEALTHY,
                f"Erro ao verificar recursos: {e}",
                {}
            )

# ❌ RUIM: Health check simplificado demais
def health_check():
    return {"status": "ok"}  # Muito básico
```

## 🔄 Práticas de Manutenção

### Versionamento e Changelog

```yaml
# ✅ BOM: Versionamento semântico
# CHANGELOG.md
## [2.1.0] - 2025-06-02

### Added
- Novo EstabelecimentoProcessor com validação de CNPJ completo
- Cache inteligente com TTL configurável
- Health checks detalhados

### Changed
- ResourceMonitor agora suporta thresholds customizados
- Performance melhorada em 300% para arquivos grandes

### Fixed
- Correção de memory leak no processamento em lote
- Validação de CPF agora funciona com todos os formatos

### Deprecated
- Método process_data_file será removido na versão 3.0

### Security
- Validação de paths para prevenir directory traversal
- Sanitização de logs para remover dados sensíveis
```

### Migração de Dados

```python
# ✅ BOM: Sistema de migração estruturado
from abc import ABC, abstractmethod

class DataMigration(ABC):
    version_from: str
    version_to: str
    
    @abstractmethod
    def migrate(self, data_path: str) -> bool:
        """Executa migração dos dados"""
        pass
    
    @abstractmethod
    def rollback(self, data_path: str) -> bool:
        """Desfaz migração se necessário"""
        pass

class Migration_v1_to_v2(DataMigration):
    version_from = "1.0"
    version_to = "2.0"
    
    def migrate(self, data_path: str) -> bool:
        """Migra formato de dados da v1 para v2"""
        try:
            # Backup dos dados originais
            self._create_backup(data_path)
            
            # Aplicar transformações
            self._convert_file_format(data_path)
            self._update_schema(data_path)
            
            # Validar resultado
            if self._validate_migration(data_path):
                self._cleanup_backup(data_path)
                return True
            else:
                self.rollback(data_path)
                return False
                
        except Exception as e:
            logging.error(f"Erro na migração: {e}")
            self.rollback(data_path)
            return False

# ❌ RUIM: Migração manual sem estrutura
def migrate_data():
    # Script manual sem rollback ou validação
    for file in files:
        process_file(file)  # Sem backup, sem validação
```

### Documentação Viva

```python
# ✅ BOM: Documentação integrada ao código
class EmpresaProcessor(BaseProcessor):
    """
    Processador especializado para dados de empresas.
    
    Funcionalidades:
    - Extração automática de CPF da razão social
    - Validação de CNPJ básico (8 dígitos)
    - Opção create_private para subset de empresas privadas
    - Classificação automática de porte empresarial
    
    Performance:
    - ~250 linhas/segundo em hardware médio
    - Uso de memória: ~25MB por 10.000 registros
    - Suporte a arquivos de até 2GB
    
    Exemplo:
        >>> from src.process.base.factory import ProcessorFactory
        >>> processor = ProcessorFactory.create("empresa", "/zip", "/unzip", "/out")
        >>> success = processor.process_single_zip("empresa.zip", create_private=True)
        >>> print(f"Processamento {'bem-sucedido' if success else 'falhou'}")
        
    Veja também:
        - BaseProcessor: Classe base com funcionalidades comuns
        - Empresa: Entidade para validação e transformação de dados
    """
    
    def process_single_zip(self, zip_file: str, create_private: bool = False) -> bool:
        """
        Processa arquivo ZIP de empresas.
        
        Args:
            zip_file: Caminho para arquivo ZIP contendo dados CSV
            create_private: Se True, cria subset apenas com empresas privadas
            
        Returns:
            True se processamento foi bem-sucedido, False caso contrário
            
        Raises:
            FileNotFoundError: Se arquivo ZIP não existir
            SecurityError: Se arquivo não passar na validação de segurança
            
        Note:
            Arquivos de saída são salvos em formato Parquet no diretório configurado.
            Empresas com natureza jurídica pública são filtradas quando create_private=True.
        """
        # Implementação...

# ❌ RUIM: Código sem documentação
class EmpresaProcessor(BaseProcessor):
    def process_single_zip(self, zip_file, create_private=False):
        # Sem documentação, parâmetros não explicados
        pass
```

## 📋 Checklist de Qualidade

### Code Review Checklist

- [ ] ✅ **Type hints**: Todos os métodos têm type hints
- [ ] ✅ **Documentação**: Docstrings em classes e métodos públicos
- [ ] ✅ **Testes**: Cobertura de testes > 90%
- [ ] ✅ **Performance**: Sem operações bloqueantes desnecessárias
- [ ] ✅ **Segurança**: Validação de entrada implementada
- [ ] ✅ **Logs**: Logs estruturados sem dados sensíveis
- [ ] ✅ **Recursos**: Uso controlado de memória e CPU
- [ ] ✅ **Padrões**: Seguindo padrões de design estabelecidos

### Deploy Checklist

- [ ] ✅ **Testes passando**: 100% dos testes unitários e integração
- [ ] ✅ **Performance**: Benchmarks dentro dos limites esperados
- [ ] ✅ **Configuração**: Configurações de produção validadas
- [ ] ✅ **Monitoramento**: Health checks funcionando
- [ ] ✅ **Backup**: Estratégia de backup testada
- [ ] ✅ **Rollback**: Plano de rollback documentado e testado
- [ ] ✅ **Documentação**: Changelog atualizado
- [ ] ✅ **Alertas**: Alertas configurados para métricas críticas

### Manutenção Checklist

- [ ] ✅ **Dependências**: Dependências atualizadas e seguras
- [ ] ✅ **Logs**: Logs rotacionando corretamente
- [ ] ✅ **Métricas**: Métricas sendo coletadas
- [ ] ✅ **Performance**: Performance dentro dos SLAs
- [ ] ✅ **Capacidade**: Recursos suficientes para demanda
- [ ] ✅ **Backups**: Backups funcionando e testados
- [ ] ✅ **Segurança**: Patches de segurança aplicados
- [ ] ✅ **Documentação**: Documentação atualizada

---

**💡 Estas melhores práticas garantem que o sistema refatorado mantenha alta qualidade, performance e segurança ao longo do tempo, facilitando manutenção e evolução futuras.** 