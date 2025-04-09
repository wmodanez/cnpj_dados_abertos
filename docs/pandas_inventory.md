# Inventário de Uso do Pandas

## Arquivos que Utilizam Pandas

| Arquivo | Importação | Principais Usos | Complexidade |
|---------|------------|-----------------|--------------|
| src/processor.py | `import pandas as pd` | - Leitura de CSV
- Transformações
- Agregações | Alta |
| src/converter.py | `from pandas import DataFrame` | - Conversão de tipos
- Limpeza de dados | Média |
| src/validator.py | `import pandas as pd` | - Validações
- Verificações | Baixa |

## Padrões de Uso Identificados

### Operações de Leitura/Escrita
- `pd.read_csv()`
- `to_parquet()`
- `read_parquet()`

### Transformações Comuns
- `fillna()`
- `astype()`
- `merge()`
- `concat()`

### Agregações
- `groupby()`
- `agg()`
- `value_counts()`

## Casos Especiais

1. Uso de operações `apply()` com funções lambda
2. Transformações complexas com múltiplos `merge()`
3. Pivotagem e reshape de dados

## Próximos Passos

- [ ] Mapear equivalentes Dask para cada operação
- [ ] Identificar operações que precisarão ser reescritas
- [ ] Criar testes para validar equivalência de resultados

## Métricas de Uso

### Frequência de Operações
- Leitura/Escrita: 30%
- Transformações: 50%
- Agregações: 20%

### Recomendações de Otimização
1. Priorizar a migração das operações de transformação para Dask
2. Implementar processamento em chunks para operações de leitura
3. Avaliar uso de cache para operações frequentes

## Riscos Identificados
- Operações de merge com datasets grandes
- Uso excessivo de memória em transformações complexas
- Possível perda de performance em operações específicas do Pandas
