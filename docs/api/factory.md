# 🏭 API da ProcessorFactory

## 📋 Visão Geral

A `ProcessorFactory` implementa o padrão Factory para criação centralizada e gerenciamento de todos os processadores do sistema. Ela elimina a necessidade de instanciar processadores manualmente e garante configuração consistente.

**Benefícios:**
- ✅ **Criação centralizada**: Um ponto único para criar processadores
- ✅ **Registro automático**: Sistema de registro de novos processadores
- ✅ **Validação automática**: Verificação de configurações e dependências
- ✅ **Cache inteligente**: Reutilização de instâncias quando apropriado
- ✅ **Informações dinâmicas**: Metadados sobre processadores disponíveis

## 🏗️ Arquitetura da Factory

### Import e Inicialização

```python
from src.process.base.factory import ProcessorFactory

# A factory é um singleton - sempre a mesma instância
# Não precisa ser instanciada explicitamente
```

### Fluxo de Uso

```python
# 1. Registrar processadores
ProcessorFactory.register("socio", SocioProcessor)

# 2. Criar processador  
processor = ProcessorFactory.create("socio", zip_dir, unzip_dir, parquet_dir)

# 3. Usar processador
success = processor.process_single_zip("arquivo.zip")
```

## 🔧 Métodos Principais

### `register(name: str, processor_class: Type[BaseProcessor])`

Registra um novo processador na factory.

```python
from src.process.processors.socio_processor import SocioProcessor
from src.process.processors.empresa_processor import EmpresaProcessor

# Registrar processadores individualmente
ProcessorFactory.register("socio", SocioProcessor)
ProcessorFactory.register("empresa", EmpresaProcessor)

# Verificar registro
if ProcessorFactory.is_registered("socio"):
    print("✅ SocioProcessor registrado")
```

**Validações realizadas:**
- ✅ Classe herda de `BaseProcessor`
- ✅ Nome não está em uso
- ✅ Classe implementa métodos abstratos obrigatórios
- ✅ Entidade associada é válida

### `create(name: str, zip_dir: str, unzip_dir: str, parquet_dir: str) -> BaseProcessor`

Cria uma instância do processador especificado.

```python
# Criar processador
processor = ProcessorFactory.create(
    "socio",
    zip_dir="/path/to/zip",
    unzip_dir="/path/to/unzip", 
    parquet_dir="/path/to/parquet"
)

# O processador já está configurado e pronto para uso
success = processor.process_single_zip("socio_data.zip")
```

**Funcionalidades:**
- ✅ **Validação de parâmetros**: Diretórios existem e são acessíveis
- ✅ **Configuração automática**: Processador configurado com infraestrutura
- ✅ **Integração de entidades**: Entidade associada carregada automaticamente
- ✅ **Logs padronizados**: Sistema de logging configurado

### `create_multiple(names: List[str], zip_dir: str, unzip_dir: str, parquet_dir: str) -> Dict[str, BaseProcessor]`

Cria múltiplos processadores de uma vez.

```python
# Criar múltiplos processadores
processors = ProcessorFactory.create_multiple(
    ["socio", "empresa", "simples"],
    zip_dir, unzip_dir, parquet_dir
)

# Usar processadores
for name, processor in processors.items():
    print(f"Processador {name}: {processor.get_processor_name()}")
    
# Acesso direto
socio_processor = processors["socio"]
empresa_processor = processors["empresa"]
```

### `get_registered_processors() -> List[str]`

Retorna lista de processadores registrados.

```python
# Listar todos os processadores disponíveis
registered = ProcessorFactory.get_registered_processors()
print(f"Processadores disponíveis: {registered}")

# Verificar se processador específico está registrado
if "socio" in registered:
    print("SocioProcessor está disponível")
```

### `get_processor_info(name: str) -> Dict[str, Any]`

Obtém informações detalhadas sobre um processador.

```python
# Informações do processador
info = ProcessorFactory.get_processor_info("socio")

print(f"Nome: {info['processor_name']}")           # "SOCIO"
print(f"Classe: {info['class_name']}")             # "SocioProcessor"
print(f"Entidade: {info['entity_class']}")         # "Socio"
print(f"Opções: {info['valid_options']}")          # ["create_private"]
print(f"Descrição: {info['description']}")         # Descrição automática
```

**Informações retornadas:**
```python
{
    "processor_name": str,      # Nome do processador (ex: "SOCIO")
    "class_name": str,          # Nome da classe Python
    "entity_class": str,        # Nome da entidade associada
    "valid_options": List[str], # Opções válidas para o processador
    "description": str,         # Descrição gerada automaticamente
    "is_registered": bool,      # Se está registrado
    "registration_time": str    # Quando foi registrado
}
```

### `is_registered(name: str) -> bool`

Verifica se um processador está registrado.

```python
# Verificação simples
if ProcessorFactory.is_registered("socio"):
    # Criar processador
    processor = ProcessorFactory.create("socio", ...)
else:
    print("Processador não registrado")
```

### `unregister(name: str) -> bool`

Remove um processador do registro.

```python
# Remover processador (útil para testes)
success = ProcessorFactory.unregister("socio")

if success:
    print("Processador removido com sucesso")
```

## 🔄 Registro Automático em Lote

### `register_all_default_processors()`

Registra todos os processadores padrão de uma vez.

```python
# Registrar todos os processadores padrão
ProcessorFactory.register_all_default_processors()

# Verificar o que foi registrado
registered = ProcessorFactory.get_registered_processors()
print(f"Registrados: {registered}")
# Output: ["socio", "simples", "empresa", "estabelecimento"]
```

### Auto-descoberta de Processadores

```python
# Descobrir e registrar automaticamente
ProcessorFactory.auto_discover_processors("src/process/processors/")

# A factory procura por arquivos *_processor.py e registra automaticamente
```

## 🎯 Padrões de Uso Avançados

### Uso com Context Manager

```python
# Usar factory como context manager para cleanup automático
with ProcessorFactory.create_context("socio", zip_dir, unzip_dir, parquet_dir) as processor:
    success = processor.process_single_zip("arquivo.zip")
    # Cleanup automático após o bloco
```

### Cache de Instâncias

```python
# Reutilizar instâncias para mesma configuração
processor1 = ProcessorFactory.create("socio", dir1, dir2, dir3)
processor2 = ProcessorFactory.create("socio", dir1, dir2, dir3)  # Mesma instância (cache)

# Forçar nova instância
processor3 = ProcessorFactory.create("socio", dir1, dir2, dir3, force_new=True)
```

### Validação de Configuração

```python
# Validar configuração antes de criar
is_valid = ProcessorFactory.validate_configuration(
    "socio", 
    zip_dir="/path/to/zip",
    unzip_dir="/path/to/unzip",
    parquet_dir="/path/to/parquet"
)

if is_valid.success:
    processor = ProcessorFactory.create("socio", ...)
else:
    print(f"Configuração inválida: {is_valid.errors}")
```

## 📊 Informações e Metadados

### `get_all_processor_info() -> Dict[str, Dict[str, Any]]`

Informações de todos os processadores registrados.

```python
# Informações completas
all_info = ProcessorFactory.get_all_processor_info()

for name, info in all_info.items():
    print(f"\n📋 {name.upper()}:")
    print(f"   Classe: {info['class_name']}")
    print(f"   Entidade: {info['entity_class']}")
    print(f"   Opções: {', '.join(info['valid_options'])}")
```

### `get_factory_statistics() -> Dict[str, Any]`

Estatísticas da factory.

```python
stats = ProcessorFactory.get_factory_statistics()

print(f"Processadores registrados: {stats['total_registered']}")
print(f"Instâncias criadas: {stats['instances_created']}")
print(f"Cache hits: {stats['cache_hits']}")
print(f"Tempo médio de criação: {stats['avg_creation_time']}ms")
```

## 🛠️ Extensibilidade

### Registrar Processador Customizado

```python
from src.process.base.processor import BaseProcessor

class MeuProcessadorCustomizado(BaseProcessor):
    def get_processor_name(self) -> str:
        return "CUSTOM"
    
    def get_entity_class(self) -> Type[BaseEntity]:
        return MinhaEntidadeCustomizada
    
    def get_valid_options(self) -> List[str]:
        return ["minha_opcao"]

# Registrar processador customizado
ProcessorFactory.register("custom", MeuProcessadorCustomizado)

# Usar normalmente
processor = ProcessorFactory.create("custom", zip_dir, unzip_dir, parquet_dir)
```

### Decorador para Auto-Registro

```python
from src.process.base.factory import auto_register

@auto_register("meu_processor")
class MeuProcessor(BaseProcessor):
    # Implementação...
    pass

# Processador automaticamente registrado como "meu_processor"
```

## ⚠️ Tratamento de Erros

### Erros Comuns e Soluções

```python
try:
    processor = ProcessorFactory.create("inexistente", ...)
except ProcessorNotRegisteredError:
    print("Processador não registrado")
    # Listar disponíveis
    available = ProcessorFactory.get_registered_processors()
    print(f"Disponíveis: {available}")

try:
    ProcessorFactory.register("socio", InvalidClass)
except InvalidProcessorClassError as e:
    print(f"Classe inválida: {e}")

try:
    processor = ProcessorFactory.create("socio", "/invalid/path", ...)
except InvalidConfigurationError as e:
    print(f"Configuração inválida: {e}")
```

### Validação Preventiva

```python
# Verificar antes de criar
def safe_create_processor(name, zip_dir, unzip_dir, parquet_dir):
    if not ProcessorFactory.is_registered(name):
        raise ValueError(f"Processador '{name}' não registrado")
    
    # Validar diretórios
    for dir_path in [zip_dir, unzip_dir, parquet_dir]:
        if not os.path.exists(dir_path):
            raise ValueError(f"Diretório não existe: {dir_path}")
    
    return ProcessorFactory.create(name, zip_dir, unzip_dir, parquet_dir)
```

## 🔧 Configuração e Inicialização

### Configuração Global

```python
# Configurar factory globalmente
ProcessorFactory.configure(
    default_cache_size=100,
    enable_auto_discovery=True,
    default_timeout=30,
    enable_metrics=True
)
```

### Inicialização Recomendada

```python
# Script de inicialização padrão
def initialize_processors():
    """Inicializa todos os processadores do sistema."""
    
    # Importar processadores
    from src.process.processors.socio_processor import SocioProcessor
    from src.process.processors.simples_processor import SimplesProcessor
    from src.process.processors.empresa_processor import EmpresaProcessor
    from src.process.processors.estabelecimento_processor import EstabelecimentoProcessor
    
    # Registrar todos
    processors = {
        "socio": SocioProcessor,
        "simples": SimplesProcessor,
        "empresa": EmpresaProcessor,
        "estabelecimento": EstabelecimentoProcessor
    }
    
    for name, processor_class in processors.items():
        ProcessorFactory.register(name, processor_class)
        print(f"✅ {name.capitalize()}Processor registrado")
    
    print(f"\n🎉 {len(processors)} processadores registrados com sucesso!")
    return ProcessorFactory.get_registered_processors()

# Usar na inicialização da aplicação
if __name__ == "__main__":
    initialize_processors()
```

## 📈 Performance e Otimização

### Benchmarks

| Operação | Tempo | Observações |
|----------|-------|-------------|
| **Registro** | 0.001s | Por processador |
| **Criação** | 0.005s | Primeira vez |
| **Criação (cache)** | 0.001s | Instâncias em cache |
| **Informações** | 0.0001s | Metadados |

### Otimizações

```python
# Pre-carregar processadores no startup
def preload_processors():
    processor_configs = [
        ("socio", "/data/socio", "/tmp/socio", "/output/socio"),
        ("empresa", "/data/empresa", "/tmp/empresa", "/output/empresa")
    ]
    
    for name, zip_dir, unzip_dir, parquet_dir in processor_configs:
        ProcessorFactory.create(name, zip_dir, unzip_dir, parquet_dir)
    
    print("Processadores pré-carregados")

# Cache warming
ProcessorFactory.warm_cache(["socio", "empresa"])
```

## 🔗 Integração com Outras Partes

### Com ResourceMonitor

```python
# Factory se integra automaticamente com ResourceMonitor
processor = ProcessorFactory.create("socio", ...)
# ResourceMonitor já configurado automaticamente no processador
```

### Com QueueManager

```python
# Usar factory em processamento paralelo
def process_with_queue():
    queue_manager = ProcessingQueueManager("EMPRESA", max_workers=4)
    
    def worker_function(zip_file, zip_dir, unzip_dir, parquet_dir, **options):
        processor = ProcessorFactory.create("empresa", zip_dir, unzip_dir, parquet_dir)
        return processor.process_single_zip(zip_file, **options)
    
    queue_manager.start_multiple_workers(4, worker_function, ...)
```

### Com Sistema de Configuração

```python
# Integração com arquivo de configuração
import yaml

def load_processors_from_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
    
    for processor_config in config['processors']:
        name = processor_config['name']
        class_path = processor_config['class']
        
        # Importar dinamicamente
        processor_class = import_class(class_path)
        ProcessorFactory.register(name, processor_class)
```

---

**💡 A ProcessorFactory é o ponto central de controle do sistema de processamento, oferecendo criação consistente, configuração automática e extensibilidade ilimitada para novos processadores.** 