"""
Arquivo centralizado de versionamento do Sistema CNPJ
"""
import subprocess
import os

# Versão fallback (atualize sempre que fizer um release)
__version_fallback__ = "3.1.4"
__title__ = "Sistema de Processamento de Dados CNPJ"

def get_git_version():
    """
    Tenta obter a versão atual do git tags.
    Retorna None se não conseguir.
    """
    try:
        # Verificar se estamos em um repositório git
        if not os.path.exists('.git') and not os.environ.get('GIT_DIR'):
            return None
            
        # Tentar obter a tag mais recente
        result = subprocess.run(
            ['git', 'describe', '--tags', '--abbrev=0'], 
            capture_output=True, 
            text=True, 
            timeout=5
        )
        
        if result.returncode == 0:
            version = result.stdout.strip()
            # Remover 'v' se estiver no início (ex: v3.1.4 -> 3.1.4)
            if version.startswith('v'):
                version = version[1:]
            return version
            
    except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError):
        pass
    
    return None

def get_version():
    """
    Retorna a versão atual do sistema.
    Prioridade: Git tags > Versão fallback
    """
    git_version = get_git_version()
    return git_version if git_version else __version_fallback__

def get_full_description():
    """Retorna a descrição completa do sistema."""
    version = get_version()
    return f"{__title__} v{version}"

# Informações adicionais
__author__ = "Equipe CNPJ"
__email__ = ""
__url__ = ""
__license__ = "" 