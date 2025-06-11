# Guia de Versionamento Automático via Git Tags

## Como Funciona

O sistema detecta automaticamente a versão baseado nas **tags do git**, seguindo esta prioridade:

1. **Git Tags** (prioridade alta) - Obtém da tag mais recente
2. **Fallback** (prioridade baixa) - Usa a versão definida em `src/__version__.py`

## Fluxo de Trabalho Recomendado

### 1. Desenvolvimento Normal
Durante o desenvolvimento, o sistema usa automaticamente a tag mais recente:

```bash
# Ver versão atual
python -c "from src.__version__ import get_version; print(get_version())"

# Ver no help do sistema
python main.py --help
```

### 2. Criar Nova Versão (Apenas com Git)

```bash
# Fazer commits das suas alterações
git add .
git commit -m "Implementar nova funcionalidade X"

# Criar nova tag (seguindo semver: MAJOR.MINOR.PATCH)
git tag v3.3.0

# Push com as tags
git push origin main --tags
```

**✅ Pronto!** O sistema detectará automaticamente a nova versão `3.3.0`.

### 3. Tipos de Versão (Semantic Versioning)

- **MAJOR** (3.x.x): Mudanças que quebram compatibilidade
- **MINOR** (x.3.x): Novas funcionalidades mantendo compatibilidade
- **PATCH** (x.x.3): Correções de bugs

Exemplos:
```bash
git tag v3.2.1    # Correção de bug
git tag v3.3.0    # Nova funcionalidade 
git tag v4.0.0    # Mudança que quebra compatibilidade
```

### 4. Comando Avançado com Anotação

```bash
# Tag anotada com mensagem (recomendado para releases)
git tag -a v3.3.0 -m "Release 3.3.0: Adicionar funcionalidade Y"

# Push da tag específica
git push origin v3.3.0
```

### 5. Verificar e Listar Tags

```bash
# Listar todas as tags
git tag --list

# Listar tags ordenadas por versão
git tag --list | sort -V

# Ver detalhes de uma tag
git show v3.2.0

# Ver tag mais recente
git describe --tags --abbrev=0
```

## Vantagens do Sistema via Git Tags

✅ **Automático**: Não precisa editar arquivos manualmente  
✅ **Consistente**: Versão sempre sincronizada com o git  
✅ **Histórico**: Mantém histórico completo de versões  
✅ **Distribuído**: Funciona em qualquer clone do repositório  
✅ **CI/CD Friendly**: Fácil integração com pipelines  

## Scripts Auxiliares

### Script de Release Automático
```bash
#!/bin/bash
# release.sh - Criar nova release automaticamente

if [ -z "$1" ]; then
    echo "Uso: ./release.sh <versão>"
    echo "Exemplo: ./release.sh 3.3.0"
    exit 1
fi

VERSION=$1

# Validar formato da versão
if [[ ! $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Erro: Versão deve estar no formato X.Y.Z"
    exit 1
fi

echo "🚀 Criando release v$VERSION..."

# Criar tag
git tag -a "v$VERSION" -m "Release $VERSION"

# Push da tag
git push origin "v$VERSION"

echo "✅ Release v$VERSION criada com sucesso!"
echo "🔗 A versão será detectada automaticamente pelo sistema"
```

### Verificar Versão Atual
```bash
# ver-versao.sh
#!/bin/bash
echo "📋 Informações de Versão:"
echo "========================"
echo "Git Tag mais recente: $(git describe --tags --abbrev=0 2>/dev/null || echo 'Nenhuma')"
echo "Versão do Sistema: $(python -c "from src.__version__ import get_version; print(get_version())")"
echo "Versão Fallback: $(python -c "from src.__version__ import __version_fallback__; print(__version_fallback__)")"
```

## Resolução de Problemas

### Problema: Sistema não detecta nova tag
```bash
# Verificar se a tag existe
git tag --list | grep v3.2.0

# Verificar se o git está funcionando
git describe --tags --abbrev=0

# Recarregar o módulo Python (se necessário)
python -c "import importlib; import src.__version__; importlib.reload(src.__version__); from src.__version__ import get_version; print(get_version())"
```

### Problema: Versão errada sendo detectada
```bash
# Ver qual tag está sendo usada
git describe --tags --abbrev=0

# Ver todas as tags em ordem
git tag --list | sort -V

# Deletar tag incorreta (CUIDADO!)
git tag -d v3.2.0        # Local
git push origin :v3.2.0  # Remoto
```

## Integração com CI/CD

O sistema funciona perfeitamente com pipelines de CI/CD:

```yaml
# GitHub Actions exemplo
name: Build and Release
on:
  push:
    tags:
      - 'v*'

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Get version
        run: |
          VERSION=$(python -c "from src.__version__ import get_version; print(get_version())")
          echo "Building version: $VERSION"
```

## Resumo dos Comandos

```bash
# Fluxo completo para nova versão
git add .
git commit -m "Suas alterações"
git tag v3.3.0
git push origin main --tags

# Verificar resultado
python main.py --help  # Deve mostrar v3.3.0
```

**🎯 Com este sistema, você nunca mais precisa atualizar manualmente o número da versão no código!** 