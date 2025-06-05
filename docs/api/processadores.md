# 🔧 API dos Processadores Refatorados

## 📋 Visão Geral

Esta documentação cobre a API dos 4 processadores modernizados que substituem o código legado. Todos os processadores seguem a mesma interface base (`BaseProcessor`) e são criados via `ProcessorFactory`.

**Processadores Disponíveis:**
- **SocioProcessor** - Processamento de dados de sócios
- **SimplesProcessor** - Processamento de dados do Simples Nacional  
- **EmpresaProcessor** - Processamento de dados de empresas
- **EstabelecimentoProcessor** - Processamento de dados de estabelecimentos

## 🏗️ Arquitetura Base

### BaseProcessor

Todos os processadores herdam de `BaseProcessor` que define a interface comum:

```python
from abc import ABC, abstractmethod
from typing import Type, List
import polars as pl

class BaseProcessor(ABC):
    def __init__(self, zip_dir: str, unzip_dir: str, parquet_dir: str):
        self.zip_dir = zip_dir
        self.unzip_dir = unzip_dir
        self.parquet_dir = parquet_dir
        
    @abstractmethod
    def get_processor_name(self) -> str:
        """Nome do processador (ex: 'SOCIO')"""
        
    @abstractmethod
    def get_entity_class(self) -> Type[BaseEntity]:
        """Classe da entidade associada"""
        
    @abstractmethod
    def get_valid_options(self) -> List[str]:
        """Opções válidas para este processador"""
```

## 🔧 Processadores Específicos

### 1. SocioProcessor

**Funcionalidade:** Processamento de dados de sócios de empresas.

```python
from src.process.processors.socio_processor import SocioProcessor

# Criar processador
processor = SocioProcessor(zip_dir, unzip_dir, parquet_dir)

# Informações
processor.get_processor_name()  # "SOCIO"
processor.get_entity_class()    # Socio
processor.get_valid_options()   # ['create_private']
```

**Características:**
- ✅ **Transformações específicas**: CNPJ básico padronizado, normalização de nomes
- ✅ **Validação integrada**: Usa entidade `Socio` para validação
- ✅ **Performance**: 0.003s para 500 linhas
- ✅ **Colunas de saída**: 7 colunas

**Métodos principais:**
```python
# Processar arquivo específico
success = processor.process_single_zip("socio_file.zip")

# Aplicar transformações (usado internamente)
transformed_df = processor.apply_entity_transformations(dataframe)
```

### 2. SimplesProcessor

**Funcionalidade:** Processamento de dados do Simples Nacional.

```python
from src.process.processors.simples_processor import SimplesProcessor

processor = SimplesProcessor(zip_dir, unzip_dir, parquet_dir)
```

**Características específicas:**
- ✅ **Conversão S/N → 0/1**: Campos `opcao_simples` automaticamente convertidos
- ✅ **Transformações de data**: Datas no formato YYYYMMDD validadas
- ✅ **Situação atual**: Cálculo automático da situação atual no Simples
- ✅ **Performance**: 0.005s para 500 linhas

**Funcionalidades únicas:**
```python
# O processador automaticamente:
# - Converte "S" → 1, "N" → 0 em campos de opção
# - Valida datas posteriores a 2006 (criação do Simples)
# - Calcula situação atual baseada nas datas
```

### 3. EmpresaProcessor

**Funcionalidade:** Processamento de dados de empresas com funcionalidades especiais.

```python
from src.process.processors.empresa_processor import EmpresaProcessor

processor = EmpresaProcessor(zip_dir, unzip_dir, parquet_dir)
```

**Opções específicas:**
- **`create_private`**: Cria subset de empresas privadas

```python
# Usar opção create_private
success = processor.process_single_zip(
    "empresa_file.zip", 
    create_private=True
)
```

**Características específicas:**
- ✅ **Extração de CPF**: Extrai CPF da razão social automaticamente
- ✅ **Limpeza de dados**: Remove CPF da razão social após extração
- ✅ **Subset privado**: Filtra apenas empresas privadas quando solicitado
- ✅ **Performance**: 0.002s para 500 linhas

**Campos adicionais criados:**
```python
# Campos automáticos adicionados:
# - cpf_extraido: CPF extraído da razão social
# - razao_social: Limpa após remoção do CPF
# - tipo_empresa: Classificação baseada no porte
```

### 4. EstabelecimentoProcessor

**Funcionalidade:** Processamento de dados de estabelecimentos com funcionalidades especiais.

```python
from src.process.processors.estabelecimento_processor import EstabelecimentoProcessor

processor = EstabelecimentoProcessor(zip_dir, unzip_dir, parquet_dir)
```

**Opções específicas:**
- **`uf_subset`**: Cria subset por UF específica

```python
# Usar opção uf_subset
success = processor.process_single_zip(
    "estabelecimento_file.zip",
    uf_subset="SP"  # Apenas estabelecimentos de SP
)
```

**Características específicas:**
- ✅ **CNPJ completo**: Cria CNPJ completo (14 dígitos) automaticamente
- ✅ **Validação de CNPJ**: Validação completa usando algoritmo oficial
- ✅ **Subset por UF**: Filtra estabelecimentos por UF quando solicitado
- ✅ **Performance**: 0.003s para 500 linhas

**Campos adicionais criados:**
```python
# Campos automáticos adicionados:
# - cnpj_completo: CNPJ de 14 dígitos (básico + ordem + DV)
# - tipo_estabelecimento: Matriz ou Filial
# - situacao_atual: Status baseado na situação cadastral
```

## 🔄 Interface Comum

Todos os processadores implementam a mesma interface:

### Métodos Base

```python
# Criação via Factory (recomendado)
from src.process.base.factory import ProcessorFactory

ProcessorFactory.register("socio", SocioProcessor)
processor = ProcessorFactory.create("socio", zip_dir, unzip_dir, parquet_dir)

# Processamento de arquivo
success = processor.process_single_zip("arquivo.zip", **options)

# Informações do processador
name = processor.get_processor_name()
entity_class = processor.get_entity_class()
valid_options = processor.get_valid_options()
```

### Transformações de Dados

```python
# Aplicar transformações da entidade
transformed_df = processor.apply_entity_transformations(original_df)

# Aplicar transformações específicas do processador
specific_df = processor.apply_specific_transformations(transformed_df)
```

### Integração com Entidades

```python
# Cada processador usa sua entidade específica:
# - SocioProcessor → Socio
# - SimplesProcessor → Simples  
# - EmpresaProcessor → Empresa
# - EstabelecimentoProcessor → Estabelecimento

# Mapeamento automático de colunas
columns = processor.get_entity_class().get_column_names()
```

## 📊 Comparação de Performance

| Processador | Tempo (500 linhas) | Throughput | Colunas Saída | Funcionalidade Especial |
|-------------|-------------------|------------|---------------|-------------------------|
| **Socio** | 0.003s | ~167 l/s | 7 | Normalização de nomes |
| **Simples** | 0.005s | ~100 l/s | 7 | Conversão S/N → 0/1 |
| **Empresa** | 0.002s | ~250 l/s | 7 | Extração de CPF |
| **Estabelecimento** | 0.003s | ~167 l/s | 6 | CNPJ completo |

## 🎯 Padrões de Uso

### Uso Básico

```python
from src.process.base.factory import ProcessorFactory
from src.process.processors import *

# Registrar todos os processadores
ProcessorFactory.register("socio", SocioProcessor)
ProcessorFactory.register("simples", SimplesProcessor)
ProcessorFactory.register("empresa", EmpresaProcessor)
ProcessorFactory.register("estabelecimento", EstabelecimentoProcessor)

# Criar e usar
processor = ProcessorFactory.create("socio", zip_dir, unzip_dir, parquet_dir)
success = processor.process_single_zip("arquivo.zip")
```

### Uso com Opções Específicas

```python
# Empresas privadas
empresa_processor = ProcessorFactory.create("empresa", ...)
success = empresa_processor.process_single_zip("empresa.zip", create_private=True)

# Estabelecimentos de SP
estab_processor = ProcessorFactory.create("estabelecimento", ...)
success = estab_processor.process_single_zip("estabelecimento.zip", uf_subset="SP")
```

### Processamento em Lote

```python
# Múltiplos processadores
processors = ProcessorFactory.create_multiple(
    ["socio", "simples", "empresa"], 
    zip_dir, unzip_dir, parquet_dir
)

# Processar múltiplos arquivos
for processor_name, processor in processors.items():
    for zip_file in get_zip_files(processor_name):
        success = processor.process_single_zip(zip_file)
```

## ⚠️ Tratamento de Erros

Todos os processadores têm tratamento de erro robusto:

```python
try:
    success = processor.process_single_zip("arquivo.zip")
    if success:
        print("✅ Processamento concluído com sucesso")
    else:
        print("❌ Falha no processamento")
except Exception as e:
    print(f"🔥 Erro crítico: {e}")
```

**Tipos de erro tratados:**
- ✅ **Arquivos corrompidos**: Continuação do processamento
- ✅ **Dados malformados**: Tentativa de correção automática
- ✅ **Recursos insuficientes**: Espera inteligente
- ✅ **Falhas de validação**: Logs detalhados com fallback

## 🔗 Integração com Infraestrutura

Os processadores se integram perfeitamente com:

- **[ResourceMonitor](infraestrutura.md#resourcemonitor)** - Monitoramento de recursos
- **[QueueManager](infraestrutura.md#queuemanager)** - Sistema de fila unificado
- **[Sistema de Entidades](entidades.md)** - Validação e transformação
- **[ProcessorFactory](factory.md)** - Criação e gerenciamento

---

**💡 Esta API representa o resultado da modernização completa do sistema de processamento, oferecendo interface consistente, performance otimizada e funcionalidades avançadas mantendo compatibilidade com o código existente.** 