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

## 🆕 Novos Argumentos do Painel (v3.2.1)

```bash
--processar-painel           # Ativar processamento do painel consolidado
--painel-uf SP              # Filtrar por UF específica
--painel-situacao 2         # Filtrar por situação (2=Ativa)
--painel-incluir-inativos   # Incluir estabelecimentos inativos
```

## 🔥 Comandos Mais Usados

### Download Básico
```bash
python main.py -t empresas -q                    # Empresas silencioso
python main.py -a -f 2023-01                     # Todas desde 2023-01
python main.py -r 2024-03 -F                     # Específica forçada
```

### Processamento com Painel (🆕 NOVO)
```bash
python main.py --processar-painel                # Painel COMPLETO (todos os dados)
python main.py --processar-painel --painel-incluir-inativos  # Painel COMPLETO + inativos
python main.py --processar-painel --painel-uf SP # Painel apenas SP
python main.py -t empresas estabelecimentos simples --processar-painel --painel-uf GO  # Painel GO
```

### Processamento Tradicional
```bash
python main.py -s process -t empresas -o resultado    # Processar empresas
python main.py -s process -p -f 2023-01              # Todas as pastas locais
```

### Economia de Espaço
```bash
python main.py -t empresas -d -c                 # Download + limpeza
python main.py -a -f 2022-01 -d -C              # Máxima economia
python main.py --processar-painel --painel-uf SP -C  # Painel + economia máxima
```

### Interface
```bash
python main.py -q                                # Ultra silencioso
python main.py -v -B -S                         # Interface completa
python main.py --processar-painel --painel-uf GO -q  # Painel silencioso
```

## 🎯 Combinações Power User

```bash
# Download sequencial otimizado
python main.py -a -f 2023-01 -q -d -o dados_2023_completos

# Processamento específico com limpeza
python main.py -s process -t empresas estabelecimentos -d -c -o processados

# Painel São Paulo com máxima economia (🆕 NOVO)
python main.py --processar-painel --painel-uf SP --painel-situacao 2 -C -q

# Painel histórico otimizado (🆕 NOVO)
python main.py -a -f 2023-01 --processar-painel --painel-uf MG -q

# Painel COMPLETO (todos os dados) com máxima economia (🆕 NOVO)
python main.py --processar-painel -C -q

# Painel COMPLETO histórico (todos os dados de múltiplas pastas) (🆕 NOVO)
python main.py -a -f 2023-01 --processar-painel -q

# Debug completo
python main.py -l DEBUG -v -B -S -t empresas

# Produção limpa
python main.py -a -f 2022-01 -q -d -C -o producao_completa

# Painel para estabelecimentos suspensos incluindo inativos (🆕 NOVO)
python main.py --processar-painel --painel-situacao 3 --painel-incluir-inativos -q
```

---

**💡 Dica:** Use `python main.py --help` para ver todos os atalhos disponíveis! 