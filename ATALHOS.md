# Guia de Atalhos - Sistema CNPJ

## Referência Rápida de Argumentos

### ⚡ Argumentos Principais
| Atalho | Argumento Completo | Descrição |
|--------|-------------------|-----------|
| `-t` | `--tipos` | Tipos de dados (empresas, estabelecimentos, simples, socios) |
| `-s` | `--step` | Etapa (download, process, database, all) |
| `-q` | `--quiet` | Modo silencioso |
| `-v` | `--verbose-ui` | Interface visual completa |
| `-l` | `--log-level` | Nível de logging (DEBUG, INFO, WARNING, ERROR) |

### 📁 Downloads e Pastas
| Atalho | Argumento Completo | Descrição |
|--------|-------------------|-----------|
| `-r` | `--remote-folder` | Pasta remota específica (AAAA-MM) |
| `-a` | `--all-folders` | Baixar todas as pastas disponíveis |
| `-f` | `--from-folder` | Pasta inicial para download/processamento |
| `-F` | `--force-download` | Forçar download mesmo que arquivo exista |
| `-o` | `--output-subfolder` | Subpasta de saída para parquets |
| `-z` | `--source-zip-folder` | Pasta de origem dos ZIPs |
| `-p` | `--process-all-folders` | Processar todas as pastas locais |

### 🎯 Processamento Específico
| Atalho | Argumento Completo | Descrição |
|--------|-------------------|-----------|
| `-E` | `--criar-empresa-privada` | Criar subset empresas privadas |
| `-U` | `--criar-subset-uf` | Criar subset por UF (ex: -U SP) |

### 🧹 Limpeza e Otimização
| Atalho | Argumento Completo | Descrição |
|--------|-------------------|-----------|
| `-d` | `--delete-zips-after-extract` | Deletar ZIPs após extração |
| `-c` | `--cleanup-after-db` | Deletar parquets após DB |
| `-C` | `--cleanup-all-after-db` | Deletar parquets E ZIPs após DB |

### 🖥️ Interface Visual
| Atalho | Argumento Completo | Descrição |
|--------|-------------------|-----------|
| `-P` | `--show-progress` | Forçar exibição de barras de progresso |
| `-H` | `--hide-progress` | Ocultar barras de progresso |
| `-S` | `--show-pending` | Forçar exibição de lista de pendentes |
| `-W` | `--hide-pending` | Ocultar lista de arquivos pendentes |

## 📝 Exemplos Práticos com Atalhos

### Comandos Básicos
```bash
# Download apenas empresas em modo silencioso
python main.py -t empresas -q

# Processar apenas estabelecimentos da pasta 2024-01
python main.py -s process -t estabelecimentos -z dados-zip/2024-01

# Download de todas as pastas desde 2023-01
python main.py -a -f 2023-01
```

### Comandos com Otimização de Espaço
```bash
# Download + processamento deletando ZIPs após extração
python main.py -t empresas -d

# Processamento completo com limpeza máxima
python main.py -t estabelecimentos -C

# Download conservador de espaço
python main.py -a -f 2023-01 -d -c
```

### Comandos com Subsets Específicos
```bash
# Empresas privadas apenas
python main.py -t empresas -E -o apenas_privadas

# Estabelecimentos de São Paulo
python main.py -t estabelecimentos -U SP -o estabelecimentos_sp

# Múltiplos tipos com subset
python main.py -t empresas estabelecimentos -E -U RJ -o empresas_estab_rj
```

### Comandos com Interface Personalizada
```bash
# Modo verboso com todas as barras visuais
python main.py -v -P -S

# Modo ultra-silencioso (sem nenhuma interface visual)
python main.py -q -H -W

# Apenas barras de progresso, sem lista de pendentes
python main.py -P -W
```

### Comandos Avançados Combinados
```bash
# Download sequencial com economia máxima de espaço
python main.py -a -f 2022-01 -q -d -C

# Processamento específico com logging detalhado
python main.py -s process -t empresas socios -l DEBUG -v -z dados/2024-01 -o empresas_socios_2024

# Download forçado com interface mínima
python main.py -r 2024-03 -F -q -H -o redownload_2024_03

# Processamento de todas as pastas locais com limpeza
python main.py -s process -p -f 2023-01 -d -c -o processamento_completo
```

## 💡 Dicas de Uso

### Combinações Úteis
- **Download rápido:** `-a -f 2023-01 -q` (todas as pastas desde 2023-01 em modo silencioso)
- **Processamento econômico:** `-s process -d -c` (processar deletando ZIPs e parquets)
- **Debug completo:** `-l DEBUG -v -P -S` (logging detalhado com interface completa)
- **Produção limpa:** `-q -H -W` (interface mínima para logs limpos)

### Sequências Comuns
```bash
# 1. Download inicial
python main.py -a -q -o dados_completos

# 2. Processamento específico  
python main.py -s process -t empresas estabelecimentos -z dados_completos -o processados

# 3. Criação de banco com limpeza
python main.py -s database -o processados -c
```

### Atalhos por Categoria
**Essenciais:** `-t`, `-s`, `-q`, `-v`  
**Downloads:** `-r`, `-a`, `-f`, `-F`  
**Processamento:** `-E`, `-U`, `-p`, `-d`  
**Otimização:** `-c`, `-C`, `-o`, `-z`  
**Interface:** `-P`, `-H`, `-S`, `-W`  

---

**💡 Lembre-se:** Todos os atalhos podem ser combinados livremente para criar comandos personalizados conforme sua necessidade! 