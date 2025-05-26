# Sugestões de Refatoração - Processadores de Dados RF

## Contexto
Os arquivos `estabelecimento.py`, `socio.py`, `simples.py` e `empresa.py` compartilham muita lógica similar e têm oportunidades significativas de refatoração para melhorar manutenibilidade e reduzir duplicação de código.

## Problemas Identificados

1. **Duplicação de Código**
   - Funções de processamento quase idênticas nos quatro arquivos
   - Sistema de logging repetido
   - Funções de extração de ZIP duplicadas
   - Tratamento de erros inconsistente
   - Funções de processamento de dados (`process_data_file`) replicadas

2. **Inconsistências**
   - Sistema de fila presente em `socio.py`, `simples.py` e `empresa.py`, mas não em `estabelecimento.py`
   - Diferentes abordagens para otimização de memória
   - Variação no tratamento de erros entre arquivos
   - Implementações ligeiramente diferentes de `create_parquet` em cada arquivo

3. **Funcionalidades Específicas**
   - `empresa.py`: Possui lógica para extração de CPF da razão social
   - `empresa.py`: Suporte para criação de subset de empresas privadas
   - `estabelecimento.py`: Suporte para subset por UF
   - Cada arquivo tem suas próprias transformações específicas

4. **Inconsistência de Parâmetros**
   - Diferentes processadores aceitam diferentes parâmetros
   - Erro ao passar parâmetros específicos entre processadores
   - Falta de padronização nas interfaces dos métodos
   - Dificuldade em manter compatibilidade entre processadores

## Sugestões de Melhorias

### 1. Criar Classe Base Abstrata

```python
from abc import ABC, abstractmethod

class BaseProcessor(ABC):
    def __init__(self, path_zip: str, path_unzip: str, path_parquet: str, **kwargs):
        self.path_zip = path_zip
        self.path_unzip = path_unzip
        self.path_parquet = path_parquet
        self.options = kwargs  # Armazena opções específicas de cada processador
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def apply_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica transformações específicas do processador"""
        pass
        
    def process_file(self) -> bool:
        """Implementação comum do processamento"""
        pass
        
    @abstractmethod
    def create_subsets(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        """Cria subsets específicos para cada tipo de processador"""
        pass

    def process_single_zip(self, zip_file: str) -> bool:
        """
        Método base para processamento de arquivo ZIP.
        Usa self.options para parâmetros específicos de cada processador.
        """
        pass
```

### 2. Módulo de Utilidades Comum

```python
# utils/processing.py
class ProcessingUtils:
    @staticmethod
    def extract_file_parallel(zip_path: str, extract_dir: str) -> bool:
        """Extração paralela de arquivos ZIP"""
        pass
        
    @staticmethod
    def create_parquet(df: pl.DataFrame, config: ParquetConfig) -> bool:
        """Criação padronizada de arquivos Parquet"""
        pass
        
    @staticmethod
    def process_data_file(data_path: str, columns: List[str]) -> pl.DataFrame:
        """Processamento padronizado de arquivos de dados"""
        pass
```

### 3. Sistema de Fila Unificado

```python
# queue_manager.py
class ProcessingQueueManager:
    def __init__(self):
        self._processing_lock = Lock()
        self._active_processes = Value('i', 0)
        self._process_queue = PriorityQueue()
        
    def add_to_queue(self, item: ProcessingItem):
        pass
        
    def process_queue(self):
        pass
```

### 4. Processadores Específicos

```python
class EmpresaProcessor(BaseProcessor):
    def __init__(self, path_zip: str, path_unzip: str, path_parquet: str, create_private: bool = False):
        super().__init__(path_zip, path_unzip, path_parquet, create_private=create_private)

    def apply_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        # Transformações específicas para empresas
        # Inclui extração de CPF da razão social
        pass
        
    def create_subsets(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        # Criar subset de empresas privadas se necessário
        pass

class EstabelecimentoProcessor(BaseProcessor):
    def __init__(self, path_zip: str, path_unzip: str, path_parquet: str, uf_subset: str | None = None):
        super().__init__(path_zip, path_unzip, path_parquet, uf_subset=uf_subset)

    def apply_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        # Transformações específicas para estabelecimentos
        pass
        
    def create_subsets(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        # Criar subsets por UF se necessário
        pass

class SocioProcessor(BaseProcessor):
    def apply_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        # Transformações específicas para sócios
        pass
        
    def create_subsets(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        return {}  # Sem subsets

class SimplesProcessor(BaseProcessor):
    def apply_transformations(self, df: pl.DataFrame) -> pl.DataFrame:
        # Transformações específicas para Simples Nacional
        pass
        
    def create_subsets(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        return {}  # Sem subsets
```

## Benefícios Esperados

1. **Redução de Código**
   - Eliminação de código duplicado entre os quatro arquivos
   - Centralização de lógica comum
   - Melhor organização do código
   - Redução estimada de 70% no código duplicado

2. **Manutenibilidade**
   - Mudanças podem ser feitas em um único lugar
   - Mais fácil adicionar novos processadores
   - Testes mais simples de implementar
   - Funcionalidades específicas bem isoladas

3. **Consistência**
   - Tratamento de erros padronizado
   - Logging uniforme
   - Comportamento previsível
   - Sistema de fila unificado

4. **Performance**
   - Otimizações podem ser aplicadas globalmente
   - Melhor gerenciamento de recursos
   - Facilidade para implementar melhorias
   - Reutilização de código otimizado

## Próximos Passos

1. **Fase 1: Preparação**
   - [ ] Criar estrutura base de classes
   - [ ] Implementar módulo de utilidades
   - [ ] Definir interfaces comuns
   - [ ] Criar sistema de fila unificado

2. **Fase 2: Migração**
   - [ ] Migrar empresa.py (primeiro por ter mais funcionalidades)
   - [ ] Migrar estabelecimento.py
   - [ ] Migrar socio.py
   - [ ] Migrar simples.py

3. **Fase 3: Otimização**
   - [ ] Implementar sistema de fila unificado
   - [ ] Adicionar testes automatizados
   - [ ] Otimizar uso de memória
   - [ ] Melhorar tratamento de erros

4. **Fase 4: Documentação**
   - [ ] Documentar classes e métodos
   - [ ] Criar exemplos de uso
   - [ ] Atualizar README
   - [ ] Documentar funcionalidades específicas

## Notas Adicionais

- Manter compatibilidade com código existente durante migração
- Implementar gradualmente para minimizar riscos
- Adicionar testes antes de grandes mudanças
- Considerar feedback dos usuários do sistema
- Priorizar funcionalidades mais críticas na migração

## Impacto na Base de Código

- **Antes**: ~2000 linhas por arquivo (total ~8000 linhas)
- **Depois**: ~400 linhas por arquivo + 1000 linhas de código compartilhado (total ~2600 linhas)
- **Redução**: ~70% de código duplicado

## Riscos e Mitigações

1. **Risco**: Quebra de funcionalidade existente
   - **Mitigação**: Testes extensivos antes/depois
   - **Mitigação**: Migração gradual arquivo por arquivo

2. **Risco**: Complexidade aumentada
   - **Mitigação**: Documentação clara e exemplos
   - **Mitigação**: Código bem organizado e comentado

3. **Risco**: Tempo de migração
   - **Mitigação**: Implementação gradual
   - **Mitigação**: Priorização das funcionalidades mais críticas

4. **Risco**: Perda de otimizações específicas
   - **Mitigação**: Manter otimizações como métodos específicos
   - **Mitigação**: Documentar decisões de performance

5. **Risco**: Incompatibilidade de interfaces entre processadores
   - **Mitigação**: Criar interface base com suporte a parâmetros opcionais
   - **Mitigação**: Documentar claramente os parâmetros específicos de cada processador
   - **Mitigação**: Implementar validação de parâmetros no construtor
   - **Mitigação**: Usar padrão de design Factory para criar processadores com configurações específicas 