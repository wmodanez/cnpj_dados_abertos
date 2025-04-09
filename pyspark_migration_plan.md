# Plano de Migração para PySpark

## Visão Geral
Este documento descreve o plano de trabalho para migrar a infraestrutura de processamento atual baseada em Dask/Pandas para PySpark. A migração visa melhorar o desempenho, escalabilidade e otimização do processamento de dados de CNPJ.

## Objetivos
- Substituir o processamento baseado em Dask/Pandas por PySpark
- Melhorar o desempenho e a escalabilidade
- Manter a compatibilidade com a infraestrutura existente
- Documentar adequadamente todas as mudanças

## Plano de Trabalho

### Tabela de Etapas

| Fase | Etapa | Descrição | Concluído |
|------|-------|-----------|-----------|
| **Fase 0: Preparação e Configuração** | 0.1 | Instalação e configuração do ambiente Spark | [ ] |
| | 0.2 | Adicionar dependência PySpark ao projeto | [ ] |
| | 0.3 | Estudo da API DataFrame do PySpark | [ ] |
| | 0.4 | Configuração inicial do SparkSession | [ ] |
| **Fase 1: Refatoração dos Módulos de Processamento** | 1.1 | Refatoração do módulo empresa.py | [ ] |
| | 1.2 | Refatoração do módulo estabelecimento.py | [ ] |
| | 1.3 | Refatoração do módulo simples.py | [ ] |
| | 1.4 | Refatoração do módulo socio.py | [ ] |
| **Fase 2: Integração e Orquestração** | 2.1 | Modificação do script principal | [ ] |
| | 2.2 | Integração com recursos existentes | [ ] |
| | 2.3 | Ajuste do fluxo de processamento | [ ] |
| **Fase 3: Testes e Validação** | 3.1 | Testes unitários | [ ] |
| | 3.2 | Testes funcionais | [ ] |
| | 3.3 | Validação de saídas de dados | [ ] |
| | 3.4 | Benchmark de desempenho | [ ] |
| **Fase 4: Finalização e Documentação** | 4.1 | Atualização da documentação | [ ] |
| | 4.2 | Limpeza de código legado | [ ] |
| | 4.3 | Revisão final e ajustes | [ ] |

## Detalhamento das Fases

### Fase 0: Preparação e Configuração (1-2 dias)
- **Instalação e configuração do ambiente Spark**: Configurar o ambiente Spark localmente e garantir compatibilidade com a infraestrutura existente.
- **Adicionar dependência PySpark**: Atualizar o gerenciador de dependências do projeto para incluir PySpark.
- **Estudo da API DataFrame do PySpark**: Revisar a documentação e exemplos para entender como melhor adaptar o código existente.
- **Configuração inicial do SparkSession**: Criar um módulo para inicializar e configurar a sessão Spark de forma centralizada.

### Fase 1: Refatoração dos Módulos de Processamento (5-10 dias)

#### Refatoração de empresa.py
- Substituir a leitura de CSV com Dask por `spark.read.csv`
- Adaptar transformações para usar funções PySpark
- Modificar a escrita de arquivos Parquet
- Ajustar o filtro para empresas privadas

#### Refatoração de estabelecimento.py
- Converter a leitura de CSV para PySpark
- Adaptar transformações específicas (tipo de situação cadastral, CNPJ completo)
- Ajustar a lógica de filtro para estabelecimentos em Goiás
- Modificar a escrita de arquivos Parquet

#### Refatoração de simples.py
- Converter a leitura de CSV para PySpark
- Adaptar transformações de datas e opções (S/N para 1/0)
- Modificar a escrita de arquivos Parquet

#### Refatoração de socio.py
- Converter a leitura de CSV para PySpark
- Adaptar transformações específicas (conversões de tipos)
- Modificar a escrita de arquivos Parquet

### Fase 2: Integração e Orquestração (1-2 dias)
- Modificar o script principal para utilizar a nova lógica PySpark
- Garantir compatibilidade com recursos existentes
- Ajustar o fluxo de processamento conforme necessário

### Fase 3: Testes e Validação (2-4 dias)
- Implementar testes unitários para os módulos refatorados
- Realizar testes funcionais para garantir a correção do processamento
- Validar as saídas de dados comparando com os resultados anteriores
- Realizar benchmarks de desempenho comparando com a implementação Dask

### Fase 4: Finalização e Documentação (1-2 dias)
- Atualizar a documentação do projeto
- Limpar código legado relacionado ao Dask
- Realizar uma revisão final e ajustes necessários

## Considerações Importantes

### Desempenho e Memória
- Configurar adequadamente os parâmetros de memória do Spark
- Utilizar particionamento eficiente para grandes conjuntos de dados
- Considerar técnicas de cache e persistência para operações frequentes

### Definição de Schema
- Definir explicitamente os schemas ao ler CSVs para melhorar o desempenho
- Manter consistência com os tipos de dados utilizados anteriormente

### Integração com Outras Funcionalidades
- Garantir compatibilidade com o download assíncrono existente
- Adaptar qualquer análise exploratória para funcionar com DataFrames PySpark 