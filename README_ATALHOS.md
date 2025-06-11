# 🚀 Atalhos do Sistema CNPJ - Resumo Executivo

## ⭐ Top 10 Atalhos Mais Úteis

```bash
-t  # --tipos (empresas, estabelecimentos, simples, socios)
-s  # --step (download, process, database, all)
-q  # --quiet (modo silencioso)
-a  # --all-folders (baixar todas as pastas)
-f  # --from-folder (pasta inicial AAAA-MM)
-o  # --output-subfolder (subpasta de saída)
-d  # --delete-zips-after-extract (economizar espaço)
-c  # --cleanup-after-db (limpar após DB)
-F  # --force-download (forçar download)
-v  # --verbose-ui (interface completa)
```

## 🔥 Comandos Mais Usados

### Download Básico
```bash
python main.py -t empresas -q                    # Empresas silencioso
python main.py -a -f 2023-01                     # Todas desde 2023-01
python main.py -r 2024-03 -F                     # Específica forçada
```

### Processamento
```bash
python main.py -s process -t empresas -o resultado    # Processar empresas
python main.py -s process -p -f 2023-01              # Todas as pastas locais
```

### Economia de Espaço
```bash
python main.py -t empresas -d -c                 # Download + limpeza
python main.py -a -f 2022-01 -d -C              # Máxima economia
```

### Interface
```bash
python main.py -q                                # Ultra silencioso
python main.py -v -P -S                         # Interface completa
```

## 🎯 Combinações Power User

```bash
# Download sequencial otimizado
python main.py -a -f 2023-01 -q -d -o dados_2023_completos

# Processamento específico com limpeza
python main.py -s process -t empresas estabelecimentos -d -c -o processados

# Debug completo
python main.py -l DEBUG -v -P -S -t empresas

# Produção limpa
python main.py -a -f 2022-01 -q -d -C -o producao_completa
```

---

**💡 Dica:** Use `python main.py --help` para ver todos os atalhos disponíveis! 