#!/usr/bin/env python3
"""
Script para criar releases automaticamente via Git Tags

Uso:
    python scripts/release.py 3.3.0
    python scripts/release.py 3.3.0 --message "Nova funcionalidade X"
    python scripts/release.py --patch  # Incrementa automaticamente patch (3.2.0 -> 3.2.1)
    python scripts/release.py --minor  # Incrementa automaticamente minor (3.2.0 -> 3.3.0)
    python scripts/release.py --major  # Incrementa automaticamente major (3.2.0 -> 4.0.0)
"""

import argparse
import re
import subprocess
import sys
from datetime import datetime

def run_command(cmd, check=True):
    """Executa um comando e retorna o resultado."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar comando: {cmd}")
        print(f"Erro: {e.stderr}")
        return None

def get_current_version():
    """Obtém a versão atual via git tags."""
    try:
        version = run_command('git describe --tags --abbrev=0')
        if version and version.startswith('v'):
            return version[1:]  # Remove 'v' prefix
        return version
    except:
        return None

def parse_version(version_str):
    """Parse uma string de versão e retorna (major, minor, patch)."""
    if not version_str:
        return (0, 0, 0)
    
    match = re.match(r'^(\d+)\.(\d+)\.(\d+)', version_str)
    if match:
        return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    return (0, 0, 0)

def increment_version(current_version, increment_type):
    """Incrementa a versão baseado no tipo."""
    major, minor, patch = parse_version(current_version)
    
    if increment_type == 'major':
        return f"{major + 1}.0.0"
    elif increment_type == 'minor':
        return f"{major}.{minor + 1}.0"
    elif increment_type == 'patch':
        return f"{major}.{minor}.{patch + 1}"
    
    return current_version

def validate_version(version):
    """Valida se a versão está no formato correto."""
    pattern = r'^\d+\.\d+\.\d+(-[a-zA-Z0-9]+)*$'
    return re.match(pattern, version) is not None

def create_release(version, message=None, push=True):
    """Cria uma release com git tag."""
    tag_name = f"v{version}"
    
    # Verificar se a tag já existe
    existing = run_command(f'git tag -l {tag_name}', check=False)
    if existing:
        print(f"❌ Tag {tag_name} já existe!")
        return False
    
    # Mensagem padrão se não fornecida
    if not message:
        timestamp = datetime.now().strftime("%Y-%m-%d")
        message = f"Release {version} - {timestamp}"
    
    print(f"🚀 Criando release {tag_name}...")
    print(f"📝 Mensagem: {message}")
    
    # Criar tag anotada
    cmd = f'git tag -a {tag_name} -m "{message}"'
    result = run_command(cmd, check=False)
    
    if result is None:
        print(f"❌ Erro ao criar tag {tag_name}")
        return False
    
    print(f"✅ Tag {tag_name} criada localmente")
    
    if push:
        print(f"📤 Fazendo push da tag...")
        push_result = run_command(f'git push origin {tag_name}', check=False)
        
        if push_result is None:
            print(f"⚠️ Tag criada localmente, mas falha no push")
            print(f"Execute manualmente: git push origin {tag_name}")
        else:
            print(f"✅ Tag {tag_name} enviada para o repositório remoto")
    
    return True

def show_version_info():
    """Mostra informações sobre as versões."""
    print("📋 Informações de Versão:")
    print("=" * 40)
    
    current_git = get_current_version()
    if current_git:
        print(f"🏷️  Tag mais recente: v{current_git}")
    else:
        print("🏷️  Nenhuma tag encontrada")
    
    try:
        from src.__version__ import get_version, __version_fallback__
        system_version = get_version()
        print(f"🖥️  Versão do sistema: {system_version}")
        print(f"🔄 Versão fallback: {__version_fallback__}")
    except ImportError:
        print("⚠️  Não foi possível importar informações de versão do sistema")
    
    # Listar últimas 5 tags
    tags = run_command('git tag --sort=-version:refname | head -5', check=False)
    if tags:
        print(f"\n🏷️  Últimas 5 tags:")
        for tag in tags.split('\n'):
            if tag.strip():
                print(f"   • {tag}")

def main():
    parser = argparse.ArgumentParser(
        description="Criar releases via Git Tags",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python scripts/release.py 3.3.0                    # Criar versão específica
  python scripts/release.py --patch                   # Incrementar patch
  python scripts/release.py --minor                   # Incrementar minor  
  python scripts/release.py --major                   # Incrementar major
  python scripts/release.py 3.3.0 -m "Nova feature"  # Com mensagem customizada
  python scripts/release.py --info                    # Ver informações de versão
        """
    )
    
    parser.add_argument('version', nargs='?', help='Versão no formato X.Y.Z')
    parser.add_argument('--patch', action='store_true', help='Incrementar versão patch')
    parser.add_argument('--minor', action='store_true', help='Incrementar versão minor')
    parser.add_argument('--major', action='store_true', help='Incrementar versão major')
    parser.add_argument('-m', '--message', help='Mensagem personalizada para a tag')
    parser.add_argument('--no-push', action='store_true', help='Não fazer push automático')
    parser.add_argument('--info', action='store_true', help='Mostrar informações de versão')
    
    args = parser.parse_args()
    
    if args.info:
        show_version_info()
        return
    
    # Verificar se estamos em um repositório git
    if not run_command('git rev-parse --is-inside-work-tree', check=False):
        print("❌ Este script deve ser executado dentro de um repositório git")
        sys.exit(1)
    
    # Determinar a versão
    version = None
    current_version = get_current_version()
    
    if args.patch or args.minor or args.major:
        if not current_version:
            print("❌ Não foi possível obter a versão atual para incrementar")
            sys.exit(1)
        
        increment_type = 'patch' if args.patch else ('minor' if args.minor else 'major')
        version = increment_version(current_version, increment_type)
        print(f"📈 Incrementando versão {increment_type}: {current_version} → {version}")
        
    elif args.version:
        version = args.version
    else:
        print("❌ Especifique uma versão ou use --patch/--minor/--major")
        parser.print_help()
        sys.exit(1)
    
    # Validar versão
    if not validate_version(version):
        print(f"❌ Versão '{version}' não está no formato válido (X.Y.Z)")
        sys.exit(1)
    
    # Criar release
    push = not args.no_push
    success = create_release(version, args.message, push)
    
    if success:
        print(f"\n🎉 Release v{version} criada com sucesso!")
        print(f"\n📝 Para verificar:")
        print(f"   python main.py --help")
        print(f"   python -c \"from src.__version__ import get_version; print('Versão:', get_version())\"")
        
        if not push:
            print(f"\n💡 Para fazer push da tag:")
            print(f"   git push origin v{version}")
    else:
        print(f"\n❌ Falha ao criar release")
        sys.exit(1)

if __name__ == "__main__":
    main() 