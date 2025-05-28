#!/usr/bin/env python3
"""
Script de teste para demonstrar as diferentes configurações de interface visual.
"""

import asyncio
import os
import sys
from pathlib import Path

# Adicionar o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent))

from src.async_downloader import download_multiple_files
from src.config import config

async def test_interface_configurations():
    """Testa diferentes configurações de interface visual."""
    
    # URLs de teste (pequenos arquivos para demonstração)
    test_urls = [
        "https://dadosabertos.rfb.gov.br/CNPJ/2024-12/Empresas0.zip",
        "https://dadosabertos.rfb.gov.br/CNPJ/2024-12/Empresas1.zip",
        "https://dadosabertos.rfb.gov.br/CNPJ/2024-12/Empresas2.zip"
    ]
    
    # Diretórios de teste
    test_dir = "test_interface"
    path_zip = os.path.join(test_dir, "zip")
    path_unzip = os.path.join(test_dir, "unzip")
    path_parquet = os.path.join(test_dir, "parquet")
    
    # Criar diretórios
    for path in [path_zip, path_unzip, path_parquet]:
        os.makedirs(path, exist_ok=True)
    
    print("🧪 TESTE DAS CONFIGURAÇÕES DE INTERFACE VISUAL")
    print("=" * 60)
    
    # Teste 1: Apenas barra de progresso
    print("\n📊 TESTE 1: Apenas barra de progresso")
    print("-" * 40)
    config.pipeline.show_progress_bar = True
    config.pipeline.show_pending_files = False
    
    print(f"✅ show_progress_bar = {config.pipeline.show_progress_bar}")
    print(f"❌ show_pending_files = {config.pipeline.show_pending_files}")
    print("Resultado esperado: Apenas barras de progresso, sem lista de arquivos")
    
    try:
        processed, failed = await download_multiple_files(
            test_urls[:1],  # Apenas 1 arquivo para teste rápido
            path_zip,
            path_unzip,
            path_parquet,
            force_download=True,
            max_concurrent_downloads=1,
            show_progress_bar=config.pipeline.show_progress_bar,
            show_pending_files=config.pipeline.show_pending_files
        )
        print(f"✅ Teste 1 concluído: {len(processed)} processados, {len(failed)} falhas")
    except Exception as e:
        print(f"❌ Erro no Teste 1: {e}")
    
    await asyncio.sleep(2)  # Pausa entre testes
    
    # Teste 2: Apenas lista de arquivos pendentes
    print("\n📋 TESTE 2: Apenas lista de arquivos pendentes")
    print("-" * 40)
    config.pipeline.show_progress_bar = False
    config.pipeline.show_pending_files = True
    
    print(f"❌ show_progress_bar = {config.pipeline.show_progress_bar}")
    print(f"✅ show_pending_files = {config.pipeline.show_pending_files}")
    print("Resultado esperado: Apenas lista de arquivos, sem barras de progresso")
    
    try:
        processed, failed = await download_multiple_files(
            test_urls[1:2],  # Segundo arquivo
            path_zip,
            path_unzip,
            path_parquet,
            force_download=True,
            max_concurrent_downloads=1,
            show_progress_bar=config.pipeline.show_progress_bar,
            show_pending_files=config.pipeline.show_pending_files
        )
        print(f"✅ Teste 2 concluído: {len(processed)} processados, {len(failed)} falhas")
    except Exception as e:
        print(f"❌ Erro no Teste 2: {e}")
    
    await asyncio.sleep(2)  # Pausa entre testes
    
    # Teste 3: Ambos ativados (interface completa)
    print("\n🎯 TESTE 3: Interface completa (ambos ativados)")
    print("-" * 40)
    config.pipeline.show_progress_bar = True
    config.pipeline.show_pending_files = True
    
    print(f"✅ show_progress_bar = {config.pipeline.show_progress_bar}")
    print(f"✅ show_pending_files = {config.pipeline.show_pending_files}")
    print("Resultado esperado: Barras de progresso + lista de arquivos em layout dividido")
    
    try:
        processed, failed = await download_multiple_files(
            test_urls[2:3],  # Terceiro arquivo
            path_zip,
            path_unzip,
            path_parquet,
            force_download=True,
            max_concurrent_downloads=1,
            show_progress_bar=config.pipeline.show_progress_bar,
            show_pending_files=config.pipeline.show_pending_files
        )
        print(f"✅ Teste 3 concluído: {len(processed)} processados, {len(failed)} falhas")
    except Exception as e:
        print(f"❌ Erro no Teste 3: {e}")
    
    await asyncio.sleep(2)  # Pausa entre testes
    
    # Teste 4: Ambos desativados (modo silencioso)
    print("\n🔇 TESTE 4: Modo silencioso (ambos desativados)")
    print("-" * 40)
    config.pipeline.show_progress_bar = False
    config.pipeline.show_pending_files = False
    
    print(f"❌ show_progress_bar = {config.pipeline.show_progress_bar}")
    print(f"❌ show_pending_files = {config.pipeline.show_pending_files}")
    print("Resultado esperado: Apenas logs no console, sem interface visual")
    
    try:
        processed, failed = await download_multiple_files(
            test_urls[:1],  # Primeiro arquivo novamente
            path_zip,
            path_unzip,
            path_parquet,
            force_download=True,
            max_concurrent_downloads=1,
            show_progress_bar=config.pipeline.show_progress_bar,
            show_pending_files=config.pipeline.show_pending_files
        )
        print(f"✅ Teste 4 concluído: {len(processed)} processados, {len(failed)} falhas")
    except Exception as e:
        print(f"❌ Erro no Teste 4: {e}")
    
    print("\n🎉 TODOS OS TESTES CONCLUÍDOS!")
    print("=" * 60)
    print("📝 RESUMO DAS CONFIGURAÇÕES:")
    print("• show_progress_bar = True, show_pending_files = False → Apenas barras de progresso")
    print("• show_progress_bar = False, show_pending_files = True → Apenas lista de arquivos")
    print("• show_progress_bar = True, show_pending_files = True → Interface completa")
    print("• show_progress_bar = False, show_pending_files = False → Modo silencioso")
    
    # Restaurar configuração padrão
    config.pipeline.show_progress_bar = True
    config.pipeline.show_pending_files = True
    print(f"\n🔄 Configuração restaurada para padrão: progress_bar={config.pipeline.show_progress_bar}, pending_files={config.pipeline.show_pending_files}")

if __name__ == "__main__":
    asyncio.run(test_interface_configurations()) 