#!/usr/bin/env python3
"""
Script de demonstração para mostrar como habilitar a barra de progresso 
com lista de arquivos pendentes.
"""

from src.config import Config

def test_progress_config():
    """Testa a configuração da barra de progresso."""
    
    # Carregar configuração padrão
    config = Config()
    
    print("=== CONFIGURAÇÃO DA BARRA DE PROGRESSO ===")
    print(f"📊 Barra de progresso habilitada: {config.pipeline.show_progress_bar}")
    print()
    
    # Mostrar como habilitar
    print("Para HABILITAR a barra de progresso com lista de arquivos pendentes:")
    print("1. Edite o arquivo src/config.py")
    print("2. Altere a linha: show_progress_bar: bool = False")
    print("3. Para: show_progress_bar: bool = True")
    print()
    
    # Mostrar funcionalidades
    print("🎯 FUNCIONALIDADES DA LISTA DE ARQUIVOS PENDENTES:")
    print("   📊 Status em tempo real: Total, Concluídos, Em progresso, Falhas, Pendentes")
    print("   📋 Lista dos próximos 10 arquivos na fila")
    print("   🔄 Atualização automática a cada 0.5 segundos")
    print("   🎨 Interface visual colorida e organizada")
    print("   📏 Nomes de arquivos truncados se muito longos")
    print("   🎉 Mensagem especial quando todos os downloads são iniciados")
    print()
    
    print("💡 EXEMPLO DE USO:")
    print("   python main.py --download --tipos empresa estabelecimento")
    print("   (Com show_progress_bar=True, você verá a barra + lista de pendentes)")
    print()
    
    print("⚠️  NOTA: Por padrão, a barra está DESABILITADA para não poluir os logs")
    print("   em execuções automatizadas ou em servidores.")

if __name__ == "__main__":
    test_progress_config() 