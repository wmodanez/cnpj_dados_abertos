#!/usr/bin/env python3
"""
Script para atualizar a versão do Sistema CNPJ

Uso:
    python scripts/update_version.py 3.2.0
    python scripts/update_version.py --auto  # Obtém do git tags
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

def get_git_latest_tag():
    """Obtém a tag mais recente do git."""
    try:
        result = subprocess.run(
            ['git', 'describe', '--tags', '--abbrev=0'],
            capture_output=True,
            text=True,
            check=True
        )
        tag = result.stdout.strip()
        # Remover 'v' se presente
        if tag.startswith('v'):
            tag = tag[1:]
        return tag
    except subprocess.CalledProcessError:
        print("Erro: Não foi possível obter a tag do git")
        return None

def update_version_file(version):
    """Atualiza o arquivo src/__version__.py com a nova versão."""
    version_file = Path("src/__version__.py")
    
    if not version_file.exists():
        print(f"Erro: {version_file} não encontrado")
        return False
    
    # Ler conteúdo atual
    content = version_file.read_text(encoding='utf-8')
    
    # Substituir a versão fallback
    pattern = r'__version_fallback__ = "[^"]*"'
    replacement = f'__version_fallback__ = "{version}"'
    
    new_content = re.sub(pattern, replacement, content)
    
    if new_content == content:
        print("Aviso: Nenhuma alteração foi feita no arquivo de versão")
        return False
    
    # Escrever novo conteúdo
    version_file.write_text(new_content, encoding='utf-8')
    print(f"✅ Versão atualizada para {version} em {version_file}")
    return True

def validate_version(version):
    """Valida se a versão está no formato correto (X.Y.Z)."""
    pattern = r'^\d+\.\d+\.\d+(-[a-zA-Z0-9]+)*$'
    return re.match(pattern, version) is not None

def main():
    parser = argparse.ArgumentParser(description="Atualizar versão do Sistema CNPJ")
    parser.add_argument('version', nargs='?', help='Nova versão (formato X.Y.Z)')
    parser.add_argument('--auto', action='store_true', help='Usar a tag mais recente do git')
    
    args = parser.parse_args()
    
    if args.auto:
        version = get_git_latest_tag()
        if not version:
            sys.exit(1)
    elif args.version:
        version = args.version
    else:
        print("Erro: Especifique uma versão ou use --auto")
        parser.print_help()
        sys.exit(1)
    
    if not validate_version(version):
        print(f"Erro: Versão '{version}' não está no formato válido (X.Y.Z)")
        sys.exit(1)
    
    print(f"Atualizando para versão: {version}")
    
    if update_version_file(version):
        print("\n✅ Versão atualizada com sucesso!")
        print("\nPróximos passos:")
        print("1. Commit das alterações: git add src/__version__.py && git commit -m 'Atualizar versão para v{}'".format(version))
        print("2. Criar tag: git tag v{}".format(version))
        print("3. Push com tags: git push origin main --tags")
    else:
        print("\n❌ Falha ao atualizar versão")
        sys.exit(1)

if __name__ == "__main__":
    main() 