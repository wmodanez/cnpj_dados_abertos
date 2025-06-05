#!/usr/bin/env python3
"""
Exemplo Basico: Processamento com Novos Processadores Refatorados

Este exemplo demonstra o uso basico dos processadores modernizados para 
processar dados da Receita Federal com a nova arquitetura.

Caracteristicas demonstradas:
- Criacao de processadores via ProcessorFactory
- Processamento simples de arquivos ZIP
- Uso da infraestrutura unificada
- Monitoramento basico de recursos
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Imports dos novos processadores refatorados
from src.process.base.factory import ProcessorFactory
from src.process.base.resource_monitor import ResourceMonitor
from src.process.processors.socio_processor import SocioProcessor
from src.process.processors.simples_processor import SimplesProcessor
from src.process.processors.empresa_processor import EmpresaProcessor
from src.process.processors.estabelecimento_processor import EstabelecimentoProcessor

print("EXEMPLO BASICO: Processadores Refatorados\n")
print("="*60)

def setup_example_environment():
    """
    Configura ambiente de exemplo com diretórios temporários.
    
    Returns:
        tuple: (zip_dir, unzip_dir, parquet_dir)
    """
    # Criar diretórios temporários
    temp_base = tempfile.mkdtemp(prefix="exemplo_processadores_")
    temp_path = Path(temp_base)
    
    zip_dir = temp_path / "zip"
    unzip_dir = temp_path / "unzip"
    parquet_dir = temp_path / "parquet"
    
    zip_dir.mkdir()
    unzip_dir.mkdir()
    parquet_dir.mkdir()
    
    print(f"📁 Diretórios criados:")
    print(f"   ZIP: {zip_dir}")
    print(f"   Unzip: {unzip_dir}")
    print(f"   Parquet: {parquet_dir}")
    
    return str(zip_dir), str(unzip_dir), str(parquet_dir)

def create_sample_data(zip_dir: str):
    """
    Cria arquivos de exemplo para demonstração.
    
    Args:
        zip_dir: Diretório onde criar os arquivos
    """
    import zipfile
    
    print(f"\n📝 Criando dados de exemplo...")
    
    # Dados de exemplo para sócios
    socio_data = [
        "12345678;12345678901;SOCIO EXEMPLO LTDA;1;98765432100;REPRESENTANTE EXEMPLO;2",
        "87654321;11111111111;OUTRO SOCIO;2;99999999999;OUTRO REP;3",
        "11111111;22222222222;TERCEIRO SOCIO;1;88888888888;TERCEIRO REP;1"
    ]
    
    # Criar arquivo ZIP de sócios
    socio_zip_path = Path(zip_dir) / "socio_exemplo.zip"
    with zipfile.ZipFile(socio_zip_path, 'w') as zf:
        zf.writestr("socio_data.csv", "\n".join(socio_data))
    
    print(f"   ✅ Criado: {socio_zip_path} ({len(socio_data)} registros)")
    
    # Dados de exemplo para simples
    simples_data = [
        "12345678;S;20200101;0;N;0;0",
        "87654321;N;0;0;S;20210101;0",
        "11111111;S;20190101;20220101;N;0;0"
    ]
    
    # Criar arquivo ZIP do simples
    simples_zip_path = Path(zip_dir) / "simples_exemplo.zip"
    with zipfile.ZipFile(simples_zip_path, 'w') as zf:
        zf.writestr("simples_data.csv", "\n".join(simples_data))
    
    print(f"   ✅ Criado: {simples_zip_path} ({len(simples_data)} registros)")
    
    return ["socio_exemplo.zip", "simples_exemplo.zip"]

def demonstrate_resource_monitoring():
    """Demonstra o uso do ResourceMonitor."""
    print(f"\n🔍 DEMONSTRAÇÃO: ResourceMonitor")
    print("-" * 40)
    
    # Criar monitor de recursos
    monitor = ResourceMonitor()
    
    # Obter recursos do sistema
    resources = monitor.get_system_resources_dict()
    
    print(f"💻 CPU: {resources['cpu_count']} núcleos ({resources['cpu_percent']:.1f}% em uso)")
    print(f"🧠 RAM: {resources['memory_total']:.1f}GB total, {resources['memory_available']:.1f}GB disponível")
    print(f"💽 Disco: {resources['disk_percent']:.1f}% usado")
    
    # Calcular workers ótimos
    optimal_workers = monitor.get_optimal_workers()
    print(f"⚙️  Workers recomendados: {optimal_workers}")
    
    # Verificar se pode processar
    can_process = monitor.can_start_processing(0, optimal_workers)
    status = "✅ SIM" if can_process else "❌ NÃO"
    print(f"🚦 Pode iniciar processamento: {status}")

def register_all_processors():
    """Registra todos os processadores no factory."""
    print(f"\n📦 REGISTRO: Processadores no Factory")
    print("-" * 40)
    
    # Registrar todos os processadores
    ProcessorFactory.register("socio", SocioProcessor)
    ProcessorFactory.register("simples", SimplesProcessor) 
    ProcessorFactory.register("empresa", EmpresaProcessor)
    ProcessorFactory.register("estabelecimento", EstabelecimentoProcessor)
    
    # Verificar registro
    registered = ProcessorFactory.get_registered_processors()
    print(f"✅ Processadores registrados: {len(registered)}")
    for name in registered:
        info = ProcessorFactory.get_processor_info(name)
        print(f"   • {name}: {info['class_name']} → {info['entity_class']}")

def demonstrate_basic_processing(zip_dir: str, unzip_dir: str, parquet_dir: str, zip_files: list):
    """
    Demonstra processamento básico com os novos processadores.
    
    Args:
        zip_dir: Diretório com arquivos ZIP
        unzip_dir: Diretório para extração
        parquet_dir: Diretório de saída
        zip_files: Lista de arquivos para processar
    """
    print(f"\n⚙️  PROCESSAMENTO: Arquivos de Exemplo")
    print("-" * 40)
    
    # Mapear arquivos para processadores
    file_processor_map = {
        "socio_exemplo.zip": "socio",
        "simples_exemplo.zip": "simples"
    }
    
    results = {}
    
    for zip_file in zip_files:
        if zip_file in file_processor_map:
            processor_name = file_processor_map[zip_file]
            
            print(f"\n🔧 Processando {zip_file} com {processor_name.upper()}Processor:")
            
            try:
                # Criar processador via factory
                processor = ProcessorFactory.create(
                    processor_name,
                    zip_dir,
                    unzip_dir, 
                    parquet_dir
                )
                
                # Informações do processador
                info = ProcessorFactory.get_processor_info(processor_name)
                print(f"   📋 Processador: {info['processor_name']}")
                print(f"   📄 Entidade: {info['entity_class']}")
                print(f"   ⚙️  Opções válidas: {info['valid_options']}")
                
                # Processar arquivo
                import time
                start_time = time.time()
                
                success = processor.process_single_zip(zip_file)
                
                end_time = time.time()
                elapsed = end_time - start_time
                
                # Resultado
                if success:
                    print(f"   ✅ Sucesso em {elapsed:.3f}s")
                    results[zip_file] = {"success": True, "time": elapsed}
                else:
                    print(f"   ❌ Falha em {elapsed:.3f}s")
                    results[zip_file] = {"success": False, "time": elapsed}
                    
            except Exception as e:
                print(f"   🔥 Erro: {str(e)}")
                results[zip_file] = {"success": False, "error": str(e)}
    
    return results

def demonstrate_output_verification(parquet_dir: str):
    """
    Verifica os arquivos de saída gerados.
    
    Args:
        parquet_dir: Diretório de saída
    """
    print(f"\n📊 VERIFICAÇÃO: Arquivos de Saída")
    print("-" * 40)
    
    parquet_path = Path(parquet_dir)
    parquet_files = list(parquet_path.glob("*.parquet"))
    
    if parquet_files:
        print(f"✅ Arquivos Parquet criados: {len(parquet_files)}")
        
        for file in parquet_files:
            size_kb = file.stat().st_size / 1024
            print(f"   📄 {file.name}: {size_kb:.1f}KB")
            
            # Tentar ler com polars se disponível
            try:
                import polars as pl
                df = pl.read_parquet(file)
                print(f"      📊 Linhas: {df.height}, Colunas: {len(df.columns)}")
                print(f"      🏷️  Colunas: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
            except ImportError:
                print(f"      📊 (Polars não disponível para verificação)")
            except Exception as e:
                print(f"      ⚠️  Erro ao ler: {e}")
    else:
        print(f"❌ Nenhum arquivo Parquet encontrado")

def show_performance_summary(results: dict):
    """
    Mostra resumo de performance.
    
    Args:
        results: Dicionário com resultados do processamento
    """
    print(f"\n📈 RESUMO DE PERFORMANCE")
    print("-" * 40)
    
    total_files = len(results)
    successful_files = sum(1 for r in results.values() if r.get("success", False))
    total_time = sum(r.get("time", 0) for r in results.values())
    
    print(f"📁 Arquivos processados: {total_files}")
    print(f"✅ Sucessos: {successful_files}")
    print(f"❌ Falhas: {total_files - successful_files}")
    print(f"⏱️  Tempo total: {total_time:.3f}s")
    
    if successful_files > 0:
        avg_time = total_time / successful_files
        print(f"⚡ Tempo médio por arquivo: {avg_time:.3f}s")
    
    # Detalhes por arquivo
    print(f"\n📋 Detalhes por arquivo:")
    for file, result in results.items():
        status = "✅" if result.get("success", False) else "❌"
        time_str = f"{result.get('time', 0):.3f}s" if 'time' in result else "N/A"
        print(f"   {status} {file}: {time_str}")

def cleanup_environment(zip_dir: str):
    """
    Limpa ambiente de exemplo.
    
    Args:
        zip_dir: Diretório base para remover
    """
    try:
        # Remover diretório temporário
        temp_base = Path(zip_dir).parent
        shutil.rmtree(temp_base)
        print(f"\n🧹 Ambiente limpo: {temp_base}")
    except Exception as e:
        print(f"\n⚠️  Erro ao limpar ambiente: {e}")

def main():
    """Função principal do exemplo."""
    try:
        # 1. Configurar ambiente
        zip_dir, unzip_dir, parquet_dir = setup_example_environment()
        
        # 2. Criar dados de exemplo
        zip_files = create_sample_data(zip_dir)
        
        # 3. Demonstrar monitoramento de recursos
        demonstrate_resource_monitoring()
        
        # 4. Registrar processadores
        register_all_processors()
        
        # 5. Processar arquivos
        results = demonstrate_basic_processing(zip_dir, unzip_dir, parquet_dir, zip_files)
        
        # 6. Verificar saída
        demonstrate_output_verification(parquet_dir)
        
        # 7. Mostrar resumo
        show_performance_summary(results)
        
        print(f"\n🎉 EXEMPLO CONCLUÍDO COM SUCESSO!")
        print("="*60)
        print("💡 Este exemplo demonstrou:")
        print("   • Uso da ProcessorFactory")
        print("   • ResourceMonitor para otimização")
        print("   • Processamento com infraestrutura unificada")
        print("   • Integração com sistema de entidades")
        print("   • Performance e monitoramento")
        
        # 8. Limpar ambiente
        cleanup_environment(zip_dir)
        
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Exemplo interrompido pelo usuário")
    except Exception as e:
        print(f"\n\n🔥 Erro no exemplo: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 