# Processador de Dados CNPJ 🏢

Este projeto automatiza o download, processamento e armazenamento dos dados públicos de CNPJ disponibilizados pela Receita Federal. Ele foi desenvolvido para ser eficiente, resiliente e fácil de usar.

## Navegação

<details>
  <summary>🚀 Como Usar</summary>
  
  - [Como Usar](#-como-usar)
  - [Pré-requisitos](#pré-requisitos)
  - [Instalação](#instalação)
  - [Execução](#execução)
  - [Gerenciamento de Cache](#gerenciamento-de-cache)
  - [O que o Script Faz](#-o-que-o-script-faz)
</details>

<details>
  <summary>📋 Fluxo do Processo</summary>
  
  - [Fluxo do Processo](#-fluxo-do-processo)
  - [Etapas do Fluxo Atual](#etapas-do-fluxo-atual)
  - [Ferramentas Utilizadas Atualmente](#ferramentas-utilizadas-atualmente)
</details>

<details>
  <summary>✨ Características</summary>
  
  - [Características](#-características)
</details>

<details>
  <summary>📋 Sugestões de Otimização</summary>
  
  - [Sugestões de Otimização](#-sugestões-de-otimização)
  - [1. Paralelização e Desempenho](#1-paralelização-e-desempenho)
  - [2. Migração Completa para Dask](#2-migração-completa-para-dask)
  - [3. Otimizações do Dask](#3-otimizações-do-dask)
  - [4. Resiliência e Monitoramento](#4-resiliência-e-monitoramento)
  - [5. Arquitetura Geral](#5-arquitetura-geral)
</details>

<details>
  <summary>📊 Comparação e Implementação</summary>
  
  - [Comparação de Tecnologias](#-comparação-de-tecnologias)
  - [Plano de Implementação Progressiva](#-plano-de-implementação-progressiva)
  - [Diagrama de Gantt do Plano de Implementação](#diagrama-de-gantt-do-plano-de-implementação)
  - [Tabela de Implementação das Branches](#tabela-de-implementação-das-branches)
</details>

<details>
  <summary>📝 Monitoramento e Configuração</summary>
  
  - [Logs e Monitoramento](#-logs-e-monitoramento)
  - [Configurações](#️-configurações)
</details>

<details>
  <summary>⚡ Otimizações de Processamento</summary>
  
  - [Otimizações de Processamento](#otimizações-de-processamento)
  - [Processamento sequencial de arquivos ZIP](#processamento-sequencial-de-arquivos-zip)
  - [Sistema de Cache para Downloads](#sistema-de-cache-para-downloads)
  - [Paralelização do Processamento de CSV](#paralelização-do-processamento-de-csv)
  - [Tratamento Específico de Exceções](#tratamento-específico-de-exceções)
  - [Verificações de Segurança](#verificações-de-segurança)
  - [Limpeza de arquivos temporários](#limpeza-de-arquivos-temporários)
  - [Melhorias na Conversão de Tipos](#melhorias-na-conversão-de-tipos)
</details>

<details>
  <summary>🤝 Contribuição e Licença</summary>
  
  - [Contribuindo](#-contribuindo)
  - [Licença](#-licença)
  - [Notas](#️-notas)
</details>

## 🚀 Como Usar

### Pré-requisitos

- Python 3.8 ou superior
- Espaço em disco suficiente para os arquivos
- Conexão com internet estável

### Instalação

1. **Clone o repositório**
```bash
git clone https://github.com/seu-usuario/cnpj.git
cd cnpj
```

2. **Crie um ambiente virtual**
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

3. **Instale as dependências**
```bash
pip install -r requirements.txt
```

4. **Configure o ambiente**
   - Copie o arquivo `.env.local.example` para `.env.local`
   - Ajuste as configurações conforme necessário:
```env
# URL base dos dados da Receita Federal
URL_ORIGIN=https://dados.rfb.gov.br/CNPJ/

# Diretórios para download e processamento
PATH_ZIP=./download/      # Arquivos ZIP baixados
PATH_UNZIP=./unzip/      # Arquivos extraídos
PATH_PARQUET=./parquet/  # Arquivos Parquet processados

# Configurações do banco de dados
FILE_DB_PARQUET=cnpj.duckdb
PATH_REMOTE_PARQUET=//servidor/compartilhado/
```

### Execução

```bash
python main.py
```

### Gerenciamento de Cache

```bash
# Exibir informações sobre arquivos em cache
python -m src.cache_manager cache-info

# Limpar o cache de downloads
python -m src.cache_manager clear-cache
```

### Benchmarks

O projeto inclui scripts de benchmark para comparar o desempenho de diferentes bibliotecas (Pandas, Dask, Polars) no processamento dos dados:

```bash
# Benchmark para dados de Empresas
python benchmark/benchmark_empresa.py --completo --path_zip dados-abertos-zip

# Benchmark para dados do Simples Nacional
python benchmark/benchmark_simples.py --completo --path_zip dados-abertos-zip

# Benchmark para dados de Estabelecimentos (Exemplo)
python benchmark/benchmark_estabelecimento.py
```

**Observação:** Os benchmarks utilizam um sistema de **pontuação ponderada** para determinar o método mais adequado, considerando diferentes métricas de desempenho com pesos específicos (ex: Tempo Total peso 5, Espaço em Disco peso 4, etc.). Os resultados detalhados e a pontuação são exibidos no relatório final.

## 📊 O que o Script Faz

1. **Download dos Dados**
   - Identifica os arquivos mais recentes
   - Baixa em paralelo com retry automático
   - Verifica integridade dos arquivos
   - Mantém cache para evitar downloads desnecessários

2. **Processamento**
   - Verifica espaço em disco e conexão com a internet
   - Extrai arquivos ZIP sequencialmente
   - Processa dados CSV em paralelo com Dask
   - Gera arquivos Parquet otimizados

3. **Armazenamento**
   - Cria banco de dados DuckDB
   - Organiza dados em tabelas
   - Copia para local remoto

## 📋 Fluxo do Processo

O atual pipeline de processamento de dados de CNPJs segue um fluxo estruturado, mas com oportunidades de otimização:

```mermaid
---
config:
   theme: neutral
   layout: elk
   elk:
      direction: DOWN
      nodeSpacing: 50
      rankSpacing: 70
      mergeEdges: true
      nodePlacementStrategy: SIMPLE
      algorithm: layered
      layered:
        alignedLayout: true
   flowchart:
      useMaxWidth: true
      curve: basis
      defaultRenderer: elk
      htmlLabels: true
      animate: true

---
flowchart TD
    %% Etapa 1: Coleta de Dados
    A[Início] --> B[Configuração das variáveis]
    B --> C[Inicialização do Dask Cluster]
    C --> D[Busca URL mais recente]
    
    %% Verificação inicial de novos dados
    D --> N{Novos dados disponíveis?}
    N -->|Não| Q[Fim]
    
    %% Etapa 2: Download e Descompactação
    N -->|Sim| E{Loop por tipos de dados}
    E --> F[Download dos arquivos ZIP com PyCurl]
    F --> G[Descompactação com zipfile]
    
    %% Etapa 3: Processamento com Dask
    G --> H[Leitura dos CSV com Dask DataFrame]
    H --> I[Transformação e limpeza]
    
    %% Etapa 4: Segmentação de dados
    I --> J[Processamento específico do tipo de dado]
    
    %% Etapa 5: Armazenamento em Parquet
    J --> K[Conversão para Parquet]
    
    %% Próximo tipo de dado ou finalização
    K --> L{Mais tipos?}
    L -->|Sim| E
    
    %% Consolidação dos arquivos Parquet em DuckDB
    L -->|Não| R[Leitura e verificação dos arquivos Parquet com Python/Pandas]
    R --> S{Existem arquivos Parquet suficientes?}
    S -->|Sim| O[Consolidação dos Parquets em um banco DuckDB]
    S -->|Não| P
    
    %% Finalização
    O --> P[Encerramento do cliente Dask]
    P --> Q
    
    %% Estilos
    classDef ferramenta fill:#f9f,stroke:#333,stroke-width:2px
    classDef process fill:#bbf,stroke:#333,stroke-width:1px
    classDef decision fill:#fbb,stroke:#333,stroke-width:1px
    
    class C,F,G,H,I,J,K,O,R ferramenta
    class B,D,P process
    class E,L,N,S decision
    
    %% Animação das setas
    linkStyle default stroke:#00AA00,stroke-width:2px,color:green,animation:flowing 1.5s linear infinite,stroke-dasharray:5,2,3,2
```

### Etapas do Fluxo Atual

1. **Configuração e Inicialização**
   - Carregamento de variáveis de ambiente com `dotenv`
   - Inicialização do cluster `Dask` para processamento distribuído
   - Configuração de logging para acompanhamento do processo

2. **Obtenção e Extração dos Dados**
   - Uso de `requests` e `BeautifulSoup` para identificar URLs mais recentes
   - Download sequencial de arquivos ZIP usando `PyCurl`
   - Extração dos arquivos com o módulo `zipfile` do Python

3. **Processamento dos Dados**
   - Leitura dos CSVs extraídos utilizando `Dask DataFrame`
   - Processamento separado para cada tipo de dado (Empresas, Estabelecimentos, Simples, Sócios)
   - Transformações e limpezas específicas para cada conjunto

4. **Armazenamento Intermediário**
   - Conversão para formato `Parquet` usando `Dask.to_parquet()`
   - Organização em diretórios por mês/ano e tipo de dado

5. **Consolidação em Banco Analítico**
   - Verificação dos arquivos Parquet gerados para cada tipo de dado
   - Junção de todos os arquivos Parquet em um único banco DuckDB
   - Criação de tabelas, views e otimizações para análise

6. **Finalização**
   - Encerramento do cliente Dask
   - Geração de logs de conclusão

### Ferramentas Utilizadas Atualmente

- **Processamento distribuído:** Dask
- **Download:** PyCurl, requests
- **Parsing HTML:** BeautifulSoup
- **Armazenamento:** Parquet (via Dask)
- **Banco de dados analítico:** DuckDB

## ✨ Características

- **Download Paralelo**: Baixa múltiplos arquivos simultaneamente
- **Sistema de Cache**: Evita baixar novamente arquivos recentemente processados
- **Verificação de Espaço em Disco**: Garante espaço suficiente antes do processamento
- **Verificação de Conexão**: Verifica conectividade com a internet antes dos downloads
- **Tratamento Específico de Exceções**: Melhor robustez e recuperação de falhas
- **Paralelização do Processamento**: Processamento eficiente de arquivos CSV usando Dask e ThreadPoolExecutor
- **Resiliência**: Sistema de retry automático em caso de falhas
- **Processamento Eficiente**: Utiliza Dask para processamento paralelo
- **Armazenamento Otimizado**: Dados em formato Parquet e DuckDB
- **Logging Detalhado**: Rastreamento completo das operações
- **Configurável**: Fácil adaptação às necessidades específicas
- **Conversão Robusta de Tipos**: Tratamento avançado para campos numéricos, datas e valores monetários

## 📝 Logs e Monitoramento

- Logs são gerados em `logs/cnpj_process_YYYYMMDD_HHMMSS.log`
- Dashboard Dask disponível em `http://localhost:8787`
- Progresso de downloads exibido em tempo real
- Logs detalhados de erros com tratamento específico por tipo de exceção

## ⚙️ Configurações

O arquivo `config.py` permite ajustar:

- **Processamento**
  - Número de workers Dask (`config.dask.n_workers`)
  - Threads por worker
  - Limite de memória

- **Cache**
  - Habilitar/desabilitar cache (`config.cache.enabled`)
  - Diretório do cache (`config.cache.cache_dir`)
  - Tempo de expiração do cache (`config.cache.max_age_days`)

- **Arquivos**
  - Encoding
  - Separador
  - Tipos de dados

- **Banco de Dados**
  - Número de threads
  - Configurações de compressão

## 📋 Sugestões de Otimização

O fluxo de processamento pode ser aprimorado conforme o diagrama e sugestões a seguir:

```mermaid
---
config:
   theme: neutral
   layout: elk
   elk:
      direction: DOWN
      nodeSpacing: 70
      rankSpacing: 100
      mergeEdges: false
      spacing: 70
      layeringStrategy: NETWORK_SIMPLEX
      nodePlacementStrategy: BRANDES_KOEPF
      algorithm: layered
      layered:
        alignedLayout: true
        nodePlacement: SIMPLE
   flowchart:
      useMaxWidth: true
      curve: basis
      defaultRenderer: elk
      htmlLabels: true
      animate: true

---
flowchart TD
    %% Etapa 1: Inicialização e Verificação
    A[Início] --> B[Configuração do ambiente]
    B --> C[Inicialização do Dask Cluster]
    C --> D[Verificação de novas versões de dados CNPJ]
    D --> D1{Novos dados CNPJ disponíveis?}
    D1 -->|Não| P1[Encerramento do Dask]
    P1 --> Z[Fim]
    
    %% Etapa 2: Preparação com Cache
    D1 -->|Sim| E[Consulta ao cache de metadados]
    E --> E1{É necessário download completo?}
    E1 -->|Sim| F1[Download paralelo com asyncio]
    E1 -->|Não| F2[Download seletivo de arquivos]
    F1 --> G[Descompactação paralela]
    F2 --> G
    G --> H[Atualização do cache]
    
    %% Etapa 3: Loop de Processamento com Dask otimizado
    H --> T[Loop por tipos de dados]
    T --> I[Processamento e transformação com Dask]
    
    %% Conexão direta para validação
    I --> J[Validação de dados avançada]
    J --> J1{Dados OK?}
    J1 -->|Não| J2[Correção com transformações Dask]
    J2 --> J
    J1 -->|Sim| K[Armazenamento em Parquet otimizado]
    K --> T1{Mais tipos de dados?}
    T1 -->|Sim| T
    
    %% Etapa 4: Consolidação e Finalização
    T1 -->|Não| L[Verificação dos arquivos Parquet]
    L --> L1{Parquets completos?}
    L1 -->|Não| P1
    L1 -->|Sim| M[Criação de views otimizadas no DuckDB]
    M --> P1
    
    %% Estilos mais claros
    classDef processo fill:#d1e7dd,stroke:#0d6832,stroke-width:1px
    classDef novo fill:#a3cfbb,stroke:#0d6832,stroke-width:2px
    classDef decisao fill:#fff3cd,stroke:#856404,stroke-width:1px
    classDef inicio fill:#cfe2ff,stroke:#084298,stroke-width:1px
    classDef fim fill:#f8d7da,stroke:#842029,stroke-width:1px
    classDef loop fill:#e0cffc,stroke:#6f42c1,stroke-width:1px
    classDef dask fill:#d5e5f9,stroke:#084298,stroke-width:1px
    
    class A,B inicio
    class D1,J1,T1,L1 decisao
    class F1,F2,G,J2,M novo
    class E,H processo
    class C,I,J,K,L,P1 dask
    class T loop
    class Z fim
    
    %% Animação das setas
    linkStyle default stroke:#00AA00,stroke-width:2px,color:green,animation:flowing 2s ease infinite,stroke-dasharray:5,2,3,2
```

### 1. Paralelização e Desempenho

#### Downloads Assíncronos
- Implementar downloads paralelos com `asyncio` e `aiohttp`
- Redução de 60-80% no tempo de download total
- Funciona em conjunto com o cache de metadados

```bash
# Criar branch para implementação de downloads assíncronos
git checkout -b feature/async-downloads master
```

#### Descompactação em Paralelo
- Usar `concurrent.futures` para extrair múltiplos arquivos simultaneamente
- Redução significativa no tempo de extração

```bash
# Criar branch para implementação de descompactação paralela
git checkout -b feature/parallel-extraction master
```

#### Cache de Metadados

- Implementar cache de metadados (SQLite ou arquivo JSON)
- Evitar reprocessamento desnecessário, processando apenas o que mudou

```bash
# Criar branch para implementação do cache de metadados
git checkout -b feature/metadata-cache master
```

### 2. Migração Completa para Dask

#### Substituição de Pandas por Dask
- Identificar todas as partes do código que usam Pandas diretamente
- Converter operações Pandas para suas equivalentes em Dask
- Garantir que toda a pipeline de dados aproveite o processamento paralelo

```bash
# Criar branch para migração completa para Dask
git checkout -b feature/pandas-to-dask master
```

#### Refatoração de Código para Processamento Lazy
- Implementar padrões de processamento lazy/tardio
- Evitar materialização desnecessária de DataFrames
- Otimizar cadeia de transformações

```bash
# Criar branch para refatoração para processamento lazy
git checkout -b feature/lazy-processing master
```

### 3. Otimizações do Dask

#### Otimização do Dask
- Melhorar a configuração e utilização do Dask
- Implementar particionamento otimizado
- Utilizar funcionalidades avançadas como Dask Bag para processamento inicial

```bash
# Criar branch para otimização do Dask
git checkout -b feature/dask-optimization master
```

#### Formato de Armazenamento Otimizado

- Otimização avançada do Parquet com compressão e estatísticas
- Melhoria de esquemas e particionamento de dados
- Implementação de caching de resultados intermediários

```bash
# Criar branch para implementação de armazenamento otimizado
git checkout -b feature/optimized-storage master
```

#### Validação de Dados Integrada

- Implementar validação integrada ao fluxo de processamento
- Esquemas de validação para cada tipo de dados
- Correção automática de problemas comuns

```bash
# Criar branch para implementação de validação de dados integrada
git checkout -b feature/integrated-validation master
```

### 4. Resiliência e Monitoramento

#### Checkpoints de Recuperação

- Implementar sistema de checkpoints para recuperação de falhas
- Capacidade de retomar de falhas sem reprocessamento completo

```bash
# Criar branch para implementação de checkpoints de recuperação
git checkout -b feature/recovery-checkpoints master
```

#### Sistema de Monitoramento

- Melhorar a integração com dashboard Dask
- Adicionar métricas e monitoramento avançado
- Integração com sistemas de observabilidade

```bash
# Criar branch para implementação do sistema de monitoramento
git checkout -b feature/monitoring-system master
```

#### Tratamento Avançado de Erros

- Melhorar o sistema de tratamento de erros
- Logging detalhado com categorização de problemas
- Estratégias de recuperação por tipo de erro

```bash
# Criar branch para implementação de tratamento avançado de erros
git checkout -b feature/advanced-error-handling master
```

### 5. Arquitetura Geral

#### Pipeline Modular

- Arquitetura em etapas independentes
- Facilidade de manutenção e possibilidade de executar apenas partes específicas

```bash
# Criar branch para implementação de pipeline modular
git checkout -b feature/modular-pipeline master
```

#### Integração Avançada com DuckDB

- Melhorar a integração entre Dask e DuckDB
- Otimização de querys e carregamento
- Criação de visualizações analíticas

```bash
# Criar branch para implementação de integração com DuckDB
git checkout -b feature/duckdb-integration master
```

## 📊 Comparação de Tecnologias Atuais e Otimizadas

| Aspecto | Atual | Sugestão de Otimização | Benefício |
|---------|-------|----------|-----------|
| Processamento Distribuído | Dask básico com Pandas em algumas partes | Dask completo com particionamento adequado | Maior velocidade de processamento e uso eficiente de recursos |
| Formato de Armazenamento | Parquet básico via Dask | Parquet otimizado com estatísticas e compressão | Melhor compressão e desempenho de leitura |
| Download de Arquivos | PyCurl sequencial | asyncio/aiohttp paralelo | Redução de 60-80% no tempo de download |
| Descompactação | zipfile sequencial | concurrent.futures paralelo | Redução significativa no tempo de extração |
| Validação de Dados | Mínima | Sistema integrado de validação | Maior qualidade dos dados e robustez |
| Recuperação de Falhas | Inexistente | Sistema de checkpoints para retomada | Continuidade em caso de interrupções |
| Monitoramento | Logs básicos | Dashboard Dask aprimorado + métricas | Melhor observabilidade |

## 📅 Plano de Implementação Progressiva

Para implementar estas melhorias de forma gradual e segura:

### Fase 1: Otimizações Imediatas (1-2 semanas)

- Implementar downloads paralelos com asyncio
- Adicionar descompactação em paralelo
- Implementar cache básico de metadados

### Fase 2: Migração Completa para Dask (2-3 semanas)

- Identificar e substituir operações Pandas por Dask
- Refatorar código para processamento lazy
- Implementar padrões de processamento distribuído em toda a pipeline

### Fase 3: Otimização do Dask (2-3 semanas)

- Configurar particionamento otimizado do Dask
- Melhorar utilização de recursos
- Implementar validação de dados integrada

### Fase 4: Otimização de Fluxo (2-3 semanas)

- Implementar sistema de correção de dados
- Adicionar sistema de checkpoints
- Otimizar armazenamento Parquet

### Fase 5: Refinamentos Finais (1-2 semanas)

- Implementar integração avançada com DuckDB
- Configurar monitoramento e métricas
- Testes de desempenho e ajustes finais


### Tabela de Implementação das Branches

| Fase | Nome da Branch              | Descrição                               | Data Início | Data Previsão | Data Conclusão | Status | Dependências |
| :--- | :-------------------------- | :-------------------------------------- | :---------- | :------------ | :------------- | :----- | :--------- |
| 1    | feature/async-downloads     | Implementação de downloads assíncronos  | 10/04/2025  | 15/04/2025    | 08/04/2025     | ✅     | -            |
| 1    | feature/parallel-extraction | Descompactação em paralelo de arquivos  | 09/04/2025  | 14/04/2025    | 08/04/2025     | ✅     | -            |
| 1    | feature/metadata-cache      | Sistema de cache de metadados           | 15/04/2025  | 24/04/2025    | 08/04/2025     | ✅     | -            |
| 2    | feature/pandas-to-dask      | Migração de operações Pandas para Dask  | 25/04/2025  | 05/05/2025    | -              | ⏳     | -            |
| 2    | feature/lazy-processing     | Refatoração para processamento lazy     | 06/05/2025  | 15/05/2025    | -              | ⏳     | feature/pandas-to-dask |
| 3    | feature/dask-optimization   | Otimização do Dask e particionamento    | 16/05/2025  | 25/05/2025    | -              | ⏳     | feature/lazy-processing |
| 3    | feature/optimized-storage   | Otimização do formato de armazenamento  | 26/05/2025  | 02/06/2025    | -              | ⏳     | feature/dask-optimization |
| 3    | feature/integrated-validation | Validação integrada de dados  | 03/06/2025  | 10/06/2025    | -              | ⏳     | feature/dask-optimization |
| 4    | feature/recovery-checkpoints| Sistema de checkpoints para recuperação | 11/06/2025  | 18/06/2025    | -              | ⏳     | feature/integrated-validation |
| 4    | feature/advanced-error-handling| Tratamento avançado de erros         | 11/06/2025  | 18/06/2025    | -              | ⏳     | feature/integrated-validation |
| 4    | feature/monitoring-system   | Implementação de sistema de monitoramento| 19/06/2025  | 26/06/2025    | -              | ⏳     | feature/recovery-checkpoints |
| 5    | feature/modular-pipeline    | Implementação de pipeline modular       | 27/06/2025  | 04/07/2025    | -              | ⏳     | feature/monitoring-system |
| 5    | feature/duckdb-integration  | Integração avançada com DuckDB          | 27/06/2025  | 04/07/2025    | -              | ⏳     | feature/monitoring-system |

### Diagrama de Gantt do Plano de Implementação

O diagrama abaixo ilustra a programação temporal das tarefas, suas interdependências e o caminho crítico do projeto de otimização:

```mermaid
gantt
    title Cronograma de Implementação das Otimizações do Fluxo CNPJ
    dateFormat  YYYY-MM-DD
    axisFormat %d/%m
    excludes weekends 2025-04-17 2025-04-18 2025-04-21 2025-05-01 2025-05-02 2025-06-19 2025-06-20
    
    %% Feriados brasileiros e pontos facultativos
    section Dias Não Úteis
    Ponto Facultativo (antes da Paixão)       :crit, active, holiday0a, 2025-04-17, 1d
    Sexta-feira da Paixão                    :crit, active, holiday0, 2025-04-18, 1d
    Tiradentes                               :crit, active, holiday1, 2025-04-21, 1d
    Dia do Trabalho                          :crit, active, holiday2, 2025-05-01, 1d
    Ponto Facultativo (após Dia do Trabalho) :crit, active, holiday2b, 2025-05-02, 1d
    Corpus Christi                           :crit, active, holiday3, 2025-06-19, 1d
    Ponto Facultativo (após Corpus Christi)  :crit, active, holiday3b, 2025-06-20, 1d
    
    section Fase 1: Otimizações Imediatas
    Análise inicial e planejamento detalhado    :a1, 2025-04-14, 3d
    Implementar downloads paralelos com asyncio  :a2, after a1, 4d
    Adicionar descompactação em paralelo        :a3, after a1, 4d
    Implementar cache básico de metadados       :a4, after a2 a3, 5d
    Testes de performance da Fase 1             :a5, after a4, 2d
    
    section Fase 2: Migração Completa para Dask
    Identificar e substituir operações Pandas    :b1, after a5, 7d
    Refatorar para processamento lazy            :b2, after b1, 6d
    Testes de compatibilidade                    :b3, after b2, 3d
    
    section Fase 3: Otimização do Dask
    Configurar particionamento otimizado        :c1, after b3, 5d
    Melhorar utilização de recursos             :c2, after c1, 3d
    Implementar validação de dados integrada    :c3, after c2, 5d
    Testes de otimização do Dask                :c4, after c3, 3d
    
    section Fase 4: Otimização de Fluxo
    Implementar sistema de correção de dados    :d1, after c4, 5d
    Adicionar sistema de checkpoints            :d2, after d1, 4d
    Otimizar armazenamento Parquet              :d3, after d1, 5d
    Testes de carga do fluxo completo           :d4, after d2 d3, 3d
    
    section Fase 5: Refinamentos Finais
    Implementar integração avançada com DuckDB  :e1, after d4, 4d
    Configurar monitoramento e métricas         :e2, after d4, 4d
    Testes finais de desempenho                 :e3, after e1 e2, 3d
    Documentação e treinamento                  :e4, after e3, 2d
```

O diagrama acima representa:

- **Duração das tarefas**: Cada barra representa uma tarefa com sua duração estimada
- **Dependências**: As tarefas conectadas mostram quais precisam ser concluídas antes de outras começarem
- **Agrupamento**: As tarefas estão organizadas nas cinco fases do plano de implementação
- **Caminho crítico**: A sequência de tarefas que determina a duração total do projeto

Este cronograma prevê aproximadamente 12-14 semanas para a implementação completa, considerando as dependências entre tarefas e tempos realistas para desenvolvimento e testes.

## Otimizações de Processamento

Este projeto foi otimizado para lidar com grandes volumes de dados de maneira eficiente. 
As seguintes otimizações foram implementadas:

### Processamento sequencial de arquivos ZIP

Em vez de descompactar todos os arquivos de uma vez (o que poderia consumir muito espaço em disco), 
o processamento agora é feito sequencialmente:

1. Cada arquivo ZIP é descompactado individualmente
2. Os arquivos CSV resultantes são processados em paralelo
3. Os arquivos temporários são excluídos imediatamente
4. Só então o próximo arquivo ZIP é processado

Essa abordagem tem as seguintes vantagens:
- Reduz significativamente o uso de espaço em disco
- Previne vazamentos de memória durante o processamento
- Mantém o diretório de trabalho limpo
- Permite processamento de conjuntos de dados maiores sem esgotar o armazenamento

### Sistema de Cache para Downloads

- Evita baixar novamente arquivos já processados recentemente
- Configurável via parâmetros de tempo de expiração
- Fornece comandos para gerenciar o cache (visualizar informações e limpar)

### Paralelização do Processamento de CSV

- Os arquivos CSV dentro de cada ZIP são processados em paralelo
- Utiliza ThreadPoolExecutor e Dask para processamento eficiente
- Número de workers configurável via `config.dask.n_workers`

### Tratamento Específico de Exceções

- Implementado tratamento específico para diferentes tipos de exceções
- Mensagens de erro detalhadas para facilitar a depuração
- Melhor robustez e recuperação de falhas

### Verificações de Segurança

- Verificação de espaço em disco antes de iniciar o processamento
- Verificação de espaço antes de descompactar cada arquivo ZIP
- Verificação de conexão com a internet antes de iniciar downloads
- Estimativa do tamanho de arquivos após descompactação

### Limpeza de arquivos temporários

Todos os arquivos temporários descompactados são excluídos após o processamento, mesmo em caso de erro,
garantindo que não fiquem arquivos residuais no sistema.

### Melhorias na Conversão de Tipos

- **Tratamento robusto para valores numéricos**: Conversão segura para Int64 com suporte para valores nulos
- **Conversão avançada de datas**: Tratamento melhorado para valores inválidos (zeros, valores vazios, etc.)
- **Processamento de valores monetários**: Conversão adequada de valores com vírgulas como separador decimal
- **Validação de tipos após conversão**: Verificação da integridade dos dados pós-conversão
- **Logs detalhados**: Rastreamento do processo de conversão para facilitar depuração

## 🤝 Contribuindo

Contribuições são bem-vindas! Por favor:

1. Faça um fork do projeto
2. Crie uma branch para sua feature
3. Faça commit das mudanças
4. Push para a branch
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## ⚠️ Notas

- O processamento pode levar algumas horas dependendo do hardware
- Requisitos mínimos de espaço em disco:
  - Empresas: 5GB
  - Estabelecimentos: 8GB
  - Simples Nacional: 3GB
- Em caso de falhas, o sistema tentará novamente automaticamente
- Verificação de espaço em disco é realizada antes da descompactação

## 🛠️ Processamento e Regras de Negócio

Durante o processamento, várias transformações e regras de negócio são aplicadas, especialmente aos dados de Empresas:

1.  **Conversão de Tipos**: Colunas numéricas e de data são convertidas para os tipos apropriados.
2.  **Renomeação**: Algumas colunas são renomeadas para maior clareza (ex: `razao_social_nome_empresarial` para `razao_social`).
3.  **Extração de CPF**: 
    - O CPF (Pessoa Física) é extraído da coluna `razao_social`.
    - O script busca por padrões formatados (`xxx.xxx.xxx-xx`) ou por sequências de 11 dígitos.
    - O CPF extraído (apenas os 11 dígitos) é armazenado em uma nova coluna chamada `CPF`.
    - Esta coluna não é obrigatória, pois nem todas as razões sociais conterão um CPF.
4.  **Limpeza da Razão Social**: Após a extração do CPF, o mesmo é **removido** da coluna `razao_social` original para manter apenas o nome/razão social. Espaços extras são removidos.

Essas transformações são implementadas nas funções `apply_empresa_transformations_pandas`, `apply_empresa_transformations_polars`, e `apply_empresa_transformations_dask` dentro de `src/process/empresa.py`.

## ✨ Características

- **Automatizado**: Busca, baixa e processa os dados automaticamente.
- **Resiliente**: Possui retries em caso de falha no download e verificações de integridade.

## 🤝 Contribuindo

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou pull requests.

## 📜 Licença

Este projeto está licenciado sob a [MIT License](LICENSE).

## 📝 Notas

- Os dados da Receita Federal são atualizados periodicamente. Execute o script regularmente para manter seus dados atualizados.
- O processamento pode exigir uma quantidade significativa de recursos (CPU, memória, disco) dependendo do volume de dados.

---
*Desenvolvido com ❤️ e Python!* 

