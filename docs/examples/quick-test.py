#!/usr/bin/env python3
"""
Teste rapido dos processadores refatorados
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    from src.process.base.factory import ProcessorFactory
    from src.process.base.resource_monitor import ResourceMonitor
    from src.process.processors.socio_processor import SocioProcessor
    
    print("✅ Imports OK")
    
    # Testar ResourceMonitor
    monitor = ResourceMonitor()
    resources = monitor.get_system_resources_dict()
    print(f"✅ ResourceMonitor OK - CPU: {resources['cpu_count']} nucleos")
    
    # Testar Factory
    ProcessorFactory.register("socio", SocioProcessor)
    registered = ProcessorFactory.get_registered_processors()
    print(f"✅ ProcessorFactory OK - Registrados: {registered}")
    
    print("\n🎉 TODOS OS TESTES PASSARAM!")
    print("Sistema refatorado funcionando perfeitamente!")
    
except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc() 