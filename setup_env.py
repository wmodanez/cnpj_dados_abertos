#!/usr/bin/env python
"""
Script para verificar e configurar as variáveis de ambiente necessárias para 
o processamento paralelo dos arquivos de CNPJ.

Ele verifica o arquivo .env e adiciona as variáveis PATH_UNZIP e PATH_PARQUET
se elas não estiverem presentes.
"""

import os
import sys

def verificar_env():
    """Verifica se o arquivo .env existe e contém as variáveis necessárias."""
    env_file = '.env'
    
    # Verificar se o arquivo existe
    if not os.path.exists(env_file):
        print(f"Arquivo {env_file} não encontrado. Criando...")
        with open(env_file, 'w') as f:
            f.write("# Arquivo de configuração de variáveis de ambiente\n")
    
    # Ler o conteúdo do arquivo
    with open(env_file, 'r') as f:
        content = f.read()
    
    # Verificar variáveis necessárias
    variaveis_faltando = []
    if 'PATH_UNZIP=' not in content:
        variaveis_faltando.append('PATH_UNZIP')
    if 'PATH_PARQUET=' not in content:
        variaveis_faltando.append('PATH_PARQUET')
    
    # Se houver variáveis faltando, adicionar
    if variaveis_faltando:
        print(f"Variáveis faltando no arquivo {env_file}: {', '.join(variaveis_faltando)}")
        with open(env_file, 'a') as f:
            f.write("\n# Diretórios para processamento paralelo dos arquivos\n")
            
            # Adicionar PATH_UNZIP se não existir
            if 'PATH_UNZIP' in variaveis_faltando:
                unzip_path = input("Digite o caminho para o diretório de descompactação (PATH_UNZIP): ")
                if not unzip_path:
                    unzip_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados-unzip")
                    print(f"Usando caminho padrão: {unzip_path}")
                
                # Garantir que o diretório existe
                os.makedirs(unzip_path, exist_ok=True)
                f.write(f"PATH_UNZIP={unzip_path}\n")
            
            # Adicionar PATH_PARQUET se não existir
            if 'PATH_PARQUET' in variaveis_faltando:
                parquet_path = input("Digite o caminho para o diretório de arquivos parquet (PATH_PARQUET): ")
                if not parquet_path:
                    parquet_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "parquet")
                    print(f"Usando caminho padrão: {parquet_path}")
                
                # Garantir que o diretório existe
                os.makedirs(parquet_path, exist_ok=True)
                f.write(f"PATH_PARQUET={parquet_path}\n")
        
        print(f"Variáveis de ambiente adicionadas ao arquivo {env_file}.")
        print("Execute o comando 'source .env' ou reinicie o terminal para aplicar as alterações.")
        
        # Configura as variáveis no ambiente atual (para testes)
        if 'PATH_UNZIP' in variaveis_faltando:
            os.environ['PATH_UNZIP'] = unzip_path
        if 'PATH_PARQUET' in variaveis_faltando:
            os.environ['PATH_PARQUET'] = parquet_path
            
        return True
    else:
        print(f"Todas as variáveis de ambiente necessárias estão configuradas no arquivo {env_file}.")
        return False

if __name__ == "__main__":
    verificar_env()
    print("\nPara verificar se as variáveis foram configuradas corretamente, execute:")
    print("python -c \"import os; print('PATH_UNZIP:', os.getenv('PATH_UNZIP')); print('PATH_PARQUET:', os.getenv('PATH_PARQUET'))\"") 