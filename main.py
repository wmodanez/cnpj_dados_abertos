"""
Exemplos de uso:

1. Execução padrão (Baixa todos os tipos na pasta mais recente e salva em subpasta com nome da data baixada):
   python main.py

2. Baixa e processa apenas Empresas e Sócios (salva em subpasta com nome da data baixada):
   python main.py --tipos empresas socios

3. Baixa e processa todos os tipos (salva em subpasta com nome da data baixada):
   python main.py

4. Pular o download e processar todos os tipos da pasta ZIP '../dados-abertos-zip/2023-05', salvando Parquet na subpasta 'meu_processamento_manual' (dentro de PATH_PARQUET):
   python main.py --step process --source-zip-folder ../dados-abertos-zip/2023-05 --output-subfolder meu_processamento_manual

5. Pular o download, processar apenas Simples e Sócios da pasta ZIP 'D:/MeusDownloads/CNPJ_ZIPs/2023-01', salvando Parquet na subpasta 'simples_socios' (dentro de PATH_PARQUET):
   python main.py --step process --source-zip-folder "D:/MeusDownloads/CNPJ_ZIPs/2023-01" --output-subfolder simples_socios --tipos simples socios

6. Baixa e processa apenas Empresas (salva em subpasta com nome da data baixada):
   python main.py --tipos empresas

7. Baixa e processa apenas Empresas e salvando na subpasta 'apenas_empresas' (dentro de PATH_PARQUET):
   python main.py --tipos empresas --output-subfolder apenas_empresas

8. Como o exemplo 7, mas também cria o subconjunto 'empresa_privada' no diretório de saída:
   python main.py --tipos empresas --output-subfolder apenas_empresas --criar-empresa-privada

9. Pular download E processamento, criando apenas o arquivo DuckDB a partir dos Parquets existentes na subpasta 'processamento_anterior' (dentro de PATH_PARQUET):
    python main.py --step database --output-subfolder processamento_anterior

10. Pular download, processar, e depois criar o DuckDB, usando a pasta de origem 'meus_zips/2023-05' e salvando na subpasta 'resultado':
    python main.py --step process --source-zip-folder meus_zips/2023-05 --output-subfolder resultado

11. Processar apenas estabelecimentos, criando também um subset para São Paulo (SP) na saída 'parquet/process_sp/estabelecimentos_sp':
    python main.py --tipos estabelecimentos --output-subfolder process_sp --criar-subset-uf SP

12. Baixar e processar dados de uma pasta remota específica (2023-05) em vez da pasta mais recente:
    python main.py --remote-folder 2023-05

13. Baixar arquivos de TODOS os diretórios remotos disponíveis (salvando em subpastas separadas):
    python main.py --all-folders --step download

14. Processar dados de uma pasta baixada anteriormente (aponta diretamente para a subpasta com arquivos):
    python main.py --step process --source-zip-folder pasta_zips/2023-05 --output-subfolder processados_2023_05

15. Baixar arquivos forçando download mesmo que já existam localmente ou no cache:
    python main.py --remote-folder 2023-05 --force-download

16. Processar todas as pastas no formato AAAA-MM encontradas dentro de PATH_ZIP (útil após download com --all-folders):
    python main.py --step process --process-all-folders --output-subfolder processados_completos

17. Baixar arquivos de todas as pastas remotas a partir de 2023-01 até a mais atual:
    python main.py --all-folders --from-folder 2023-01 --step download

18. Baixar e processar arquivos de todas as pastas remotas desde a mais antiga até a mais atual:
    python main.py --all-folders

19. Baixar e processar arquivos a partir de uma pasta específica (2023-06) até a mais atual:
    python main.py --all-folders --from-folder 2023-06

20. Processar todas as pastas locais no formato AAAA-MM a partir de 2023-03:
    python main.py --step process --process-all-folders --output-subfolder processados_desde_2023_03

21. Baixar sequencialmente da pasta mais antiga até a mais atual, processando cada uma:
    python main.py --all-folders --from-folder 2022-01

22. Processar dados deletando os ZIPs após extração para economizar espaço:
    python main.py --tipos empresas --delete-zips-after-extract

23. Baixar e processar dados de 2023-01 até atual, deletando ZIPs após processamento:
    python main.py --all-folders --from-folder 2023-01 --delete-zips-after-extract

24. Processar todas as pastas locais deletando ZIPs para economizar espaço:
    python main.py --step process --process-all-folders --output-subfolder economizando_espaco --delete-zips-after-extract

25. Processamento conservador de espaço - apenas estabelecimentos com deleção de ZIPs:
    python main.py --tipos estabelecimentos --delete-zips-after-extract --output-subfolder estabelecimentos_sem_zips

EXEMPLOS COM CONTROLE DE INTERFACE VISUAL:

26. Download em modo silencioso (sem barras de progresso nem lista de pendentes):
    python main.py --quiet

27. Download com interface completa (barras de progresso + lista de pendentes):
    python main.py --verbose-ui

28. Download ocultando apenas as barras de progresso:
    python main.py --hide-progress

29. Download mostrando apenas as barras de progresso (oculta lista de pendentes):
    python main.py --show-progress --hide-pending

30. Processamento em modo verboso com todas as informações visuais:
    python main.py --step process --source-zip-folder ../dados/2023-05 --output-subfolder teste --verbose-ui

31. Download de todas as pastas em modo silencioso para logs limpos:
    python main.py --all-folders --quiet

32. Processamento mostrando lista de arquivos pendentes mas sem barras de progresso:
    python main.py --tipos empresas --show-pending --hide-progress

33. Download forçado com interface mínima (apenas lista de pendentes):
    python main.py --force-download --hide-progress --show-pending

34. Processamento de múltiplas pastas em modo silencioso:
    python main.py --step process --process-all-folders --output-subfolder batch_silent --quiet

35. Download de pasta específica com barras de progresso ativadas:
    python main.py --remote-folder 2024-01 --show-progress

EXEMPLOS COM LIMPEZA DE ARQUIVOS:

36. Processar dados e criar banco DuckDB, removendo arquivos parquet após criação:
    python main.py --step all --tipos empresas --cleanup-after-db

37. Processar dados e criar banco DuckDB, removendo arquivos parquet E ZIP após criação:
    python main.py --step all --tipos empresas --cleanup-all-after-db

38. Criar banco DuckDB a partir de parquets existentes e remover os parquets:
    python main.py --step database --output-subfolder processados_2023_05 --cleanup-after-db

39. Download, processamento e banco completo com limpeza total (economiza máximo espaço):
    python main.py --all-folders --from-folder 2023-01 --cleanup-all-after-db

40. Processamento conservador com deleção de ZIPs durante extração e limpeza final:
    python main.py --tipos estabelecimentos --delete-zips-after-extract --cleanup-after-db

EXEMPLOS COM PROCESSAMENTO DO PAINEL CONSOLIDADO (NOVO):

41. Processamento completo com painel consolidado (TODOS OS DADOS - SEM FILTROS):
    python main.py --processar-painel

42. Painel completo incluindo estabelecimentos inativos (TODOS OS DADOS):
    python main.py --processar-painel --painel-incluir-inativos

43. Painel filtrado apenas para São Paulo com estabelecimentos ativos:
    python main.py --processar-painel --painel-uf SP --painel-situacao 2

44. Painel de Goiás incluindo estabelecimentos inativos:
    python main.py --processar-painel --painel-uf GO --painel-incluir-inativos

45. Processamento de tipos específicos + painel para Minas Gerais:
    python main.py --tipos empresas estabelecimentos simples --processar-painel --painel-uf MG

46. Painel de pasta remota específica com filtro de situação:
    python main.py --remote-folder 2024-01 --processar-painel --painel-situacao 2

47. Painel completo com todos os filtros (SP, ativos, sem inativos):
    python main.py --processar-painel --painel-uf SP --painel-situacao 2

48. Painel para estabelecimentos suspensos de todas as UFs:
    python main.py --processar-painel --painel-situacao 3 --painel-incluir-inativos

49. Pipeline completo: download + processamento + painel + banco (economia máxima):
    python main.py --processar-painel --painel-uf SP --cleanup-all-after-db

50. Pipeline completo com painel de TODOS OS DADOS + economia máxima:
    python main.py --processar-painel --cleanup-all-after-db

51. Painel em modo silencioso para processamento em lote:
    python main.py --processar-painel --painel-uf GO --quiet

52. Painel COMPLETO em modo silencioso (automação):
    python main.py --processar-painel --quiet

53. Painel com dados de múltiplas pastas remotas:
    python main.py --all-folders --from-folder 2023-01 --processar-painel --painel-uf SP

54. Painel de TODOS OS DADOS com múltiplas pastas remotas:
    python main.py --all-folders --from-folder 2023-01 --processar-painel

EXEMPLOS COM STEP 'PAINEL' (PROCESSAMENTO EXCLUSIVO DO PAINEL):

55. Processar apenas o painel com dados da pasta mais recente:
    python main.py --step painel

56. Processar apenas o painel de uma pasta específica:
    python main.py --step painel --source-zip-folder dados-abertos-zip/2024-01

57. Processar apenas o painel filtrado por UF:
    python main.py --step painel --painel-uf GO

58. Processar apenas o painel com filtros combinados:
    python main.py --step painel --painel-uf SP --painel-situacao 2

59. Processar apenas o painel incluindo inativos:
    python main.py --step painel --painel-incluir-inativos

60. Processar apenas o painel de pasta específica com filtros:
    python main.py --step painel --source-zip-folder dados-abertos-zip/2023-12 --painel-uf MG --painel-situacao 2

61. Processar apenas o painel em modo silencioso:
    python main.py --step painel --painel-uf GO --quiet

62. Processar apenas o painel salvando em subpasta específica:
    python main.py --step painel --output-subfolder painel_personalizado

NOTA: O download sempre salvará os arquivos em uma subpasta com o nome da pasta remota.
      Exemplo: se --remote-folder=2023-05, os arquivos serão salvos em PATH_ZIP/2023-05/.
      Ao usar --source-zip-folder, aponte diretamente para o diretório que contém os arquivos ZIP.
      
NOVO COMPORTAMENTO:
      - --from-folder especifica a pasta inicial para download/processamento sequencial
      - Sem --from-folder + --all-folders: baixa/processa da mais antiga até a mais atual
      - --process-all-folders agora suporta --from-folder para processamento sequencial local
      - --delete-zips-after-extract deleta arquivos ZIP após extração bem-sucedida (economiza espaço)
      - --cleanup-after-db deleta arquivos parquet após criação do banco DuckDB (economiza espaço)
      - --cleanup-all-after-db deleta arquivos parquet E ZIP após criação do banco (máxima economia)
      - A deleção só ocorre após verificação de que as operações foram realizadas com sucesso

CONTROLE DE INTERFACE VISUAL:
      - --quiet (-q): Modo silencioso - desativa barras de progresso e lista de pendentes
      - --verbose-ui (-v): Modo verboso - ativa barras de progresso e lista de pendentes
      - --show-progress (-B): Força exibição de barras de progresso
      - --hide-progress (-H): Força ocultação de barras de progresso
      - --show-pending (-S): Força exibição da lista de arquivos pendentes
      - --hide-pending (-W): Força ocultação da lista de arquivos pendentes
      - Argumentos específicos têm prioridade sobre modos gerais (quiet/verbose-ui)
      - Modo silencioso tem prioridade máxima sobre todos os outros argumentos

PROCESSAMENTO DO PAINEL CONSOLIDADO:
      - --processar-painel: Ativa o processamento do painel (combina estabelecimentos + simples + empresas)
      - --painel-uf UF: Filtra painel por UF específica (ex: SP, GO, MG) 
      - --painel-situacao CODIGO: Filtra por situação cadastral (1=Nula, 2=Ativa, 3=Suspensa, 4=Inapta, 8=Baixada)
      - --painel-incluir-inativos: Inclui estabelecimentos inativos no painel (padrão é só ativos)
      - O painel é processado após o processamento individual das entidades
      - Requer que os parquets de empresas, estabelecimentos e simples já existam
      - Gera arquivo painel_dados_TIMESTAMP_filtros.parquet com estatísticas automáticas

STEP 'PAINEL' (NOVO):
      - --step painel: Processa APENAS o painel usando dados já processados
      - Funciona com --source-zip-folder para especificar pasta de dados
      - Ou detecta automaticamente a pasta mais recente se não especificada
      - Todos os filtros do painel (--painel-uf, --painel-situacao, --painel-incluir-inativos) funcionam
      - Mais rápido que --step all quando só precisa do painel

# 40. Painel de Goiás incluindo estabelecimentos inativos:
python main.py --processar-painel --painel-uf GO --painel-incluir-inativos

# 41. Processamento de tipos específicos + painel para Minas Gerais:
python main.py -t empresas estabelecimentos simples --processar-painel --painel-uf MG

# 42. Painel de pasta remota específica com filtro de situação:
python main.py -r 2024-01 --processar-painel --painel-situacao 2

# 43. Painel completo com todos os filtros (SP, ativos, sem inativos):
python main.py --processar-painel --painel-uf SP --painel-situacao 2

# 44. Painel para estabelecimentos suspensos de todas as UFs:
python main.py --processar-painel --painel-situacao 3 --painel-incluir-inativos

# 45. Pipeline completo: download + processamento + painel + banco (economia máxima):
python main.py --processar-painel --painel-uf SP -C

# 46. Painel em modo silencioso para processamento em lote:
python main.py --processar-painel --painel-uf GO -q

# 47. Painel com dados de múltiplas pastas remotas:
python main.py -a -f 2023-01 --processar-painel --painel-uf SP
"""
import argparse
import asyncio
import datetime
import logging
import os
from multiprocessing import freeze_support
import psutil
import re
import signal
import sys
import time
import socket
import requests
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from rich.logging import RichHandler

# CARREGAR VARIÁVEIS DE AMBIENTE ANTES DAS IMPORTAÇÕES QUE DEPENDEM DELAS
load_dotenv()

# Importar versão centralizada
from src.__version__ import get_full_description

from src.async_downloader import (
    download_multiple_files, 
    get_latest_month_zip_urls, 
    get_remote_folders, 
    get_latest_remote_folder,
    _filter_urls_by_type,
    download_only_files,
    get_network_test_results
)
from src.config import config
from src.database import create_duckdb_file
from src.process.base.factory import ProcessorFactory
from src.process.processors.empresa_processor import EmpresaProcessor
from src.process.processors.estabelecimento_processor import EstabelecimentoProcessor
from src.process.processors.simples_processor import SimplesProcessor
from src.process.processors.socio_processor import SocioProcessor
from src.process.processors.painel_processor import PainelProcessor
from src.utils import check_basic_folders
from src.utils.time_utils import format_elapsed_time
from src.utils.statistics import global_stats

# Configurar logger global
logger = logging.getLogger(__name__)

# Imports do circuit breaker
from src.utils.global_circuit_breaker import (
    circuit_breaker,
    FailureType,
    CriticalityLevel,
    should_continue_processing,
    report_critical_failure,
    report_fatal_failure,
    register_stop_callback
)

from typing import List, Tuple

def check_internet_connection() -> bool:
    """
    Verifica se há conexão com a internet.
    
    Returns:
        bool: True se houver conexão, False caso contrário
    """
    try:
        # Tenta fazer uma requisição para um servidor confiável
        requests.get("http://www.google.com", timeout=5)
        return True
    except requests.RequestException:
        try:
            # Tenta resolver um domínio conhecido
            socket.create_connection(("8.8.8.8", 53), timeout=5)
            return True
        except OSError:
            return False

def check_disk_space() -> bool:
    """
    Verifica se há espaço suficiente em disco.
    
    Returns:
        bool: True se há espaço suficiente, False caso contrário
    """
    try:
        disk_usage = psutil.disk_usage("/")
        
        # Verificar se há pelo menos 5GB livres
        min_free_gb = 5
        free_gb = disk_usage.free / (1024**3)
        
        if free_gb < min_free_gb:
            logger.error(f"Espaço em disco insuficiente. Disponível: {free_gb:.2f}GB, Mínimo: {min_free_gb}GB")
            return False
        
        logger.info(f"Espaço em disco verificado: {free_gb:.2f}GB disponíveis")
        return True
        
    except Exception as e:
        logger.warning(f"Erro ao verificar espaço em disco: {e}")
        return True  # Assumir que está OK se não conseguir verificar

def setup_logging(log_level_str: str):
    """Configura o sistema de logging com base no nível fornecido."""
    if not os.path.exists('logs'):
        os.makedirs('logs')

    log_filename = f'logs/cnpj_process_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Comando de execução
    cmd_line = ' '.join(sys.argv)
    # Escreve o comando como primeira linha do log
    with open(log_filename, 'w', encoding='utf-8') as f:
        f.write(f"# Linha de comando: {cmd_line}\n")

    # Converte a string do argumento para o nível de log correspondente
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)

    # Configuração do logger raiz para capturar tudo
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        print("[setup_logging] Handlers de log anteriores removidos.")

    # Handler para arquivo (sem cores)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(file_handler)

    # Handler para console (com RichHandler)
    console_handler = RichHandler(rich_tracebacks=True)
    root_logger.addHandler(console_handler)

    logger_instance = logging.getLogger(__name__)
    logger_instance.info(f"Nível de log configurado para: {logging.getLevelName(log_level)}")
    logger_instance.info(f"Linha de comando: {cmd_line}")
    return logger_instance


def print_header(text: str):
    """Imprime um cabeçalho formatado."""
    print(f"\n{'=' * 50}")
    print(f"{text}")
    print(f"{'=' * 50}\n")
    # Também logar
    logger.info("=" * 50)
    logger.info(text)
    logger.info("=" * 50)


def print_section(text: str):
    """Imprime uma seção formatada."""
    print(f"\n▶ {text}")
    # Também logar
    logger.info(f"▶ {text}")


def print_success(text: str):
    """Imprime uma mensagem de sucesso formatada."""
    print(f"✓ {text}")
    # Também logar
    logger.info(f"✓ {text}")


def print_warning(text: str):
    """Imprime uma mensagem de aviso formatada."""
    print(f"⚠ {text}")
    # Também logar
    logger.warning(f"⚠ {text}")


def print_error(text: str):
    """Imprime uma mensagem de erro formatada."""
    print(f"✗ {text}")
    # Também logar
    logger.error(f"✗ {text}")


def initialize_processors():
    try:
        # Registrar todos os processadores na factory
        ProcessorFactory.register("empresa", EmpresaProcessor)
        ProcessorFactory.register("estabelecimento", EstabelecimentoProcessor)
        ProcessorFactory.register("simples", SimplesProcessor)
        ProcessorFactory.register("socio", SocioProcessor)
        ProcessorFactory.register("painel", PainelProcessor)
        
        registered = ProcessorFactory.get_registered_processors()
        logger.info(f"✅ Processadores registrados: {', '.join(registered)}")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar processadores: {e}")
        return False

def check_parquet_completeness(output_parquet_path: str, tipos_processados: List[str]) -> tuple[bool, List[str]]:
    """
    Verifica se todos os parquets necessários foram criados com sucesso.
    
    Args:
        output_parquet_path: Caminho onde os parquets devem estar
        tipos_processados: Lista de tipos que deveriam ter sido processados
        
    Returns:
        tuple: (sucesso_completo, tipos_faltando)
    """
    try:
        logger.info("🔍 Verificando integridade dos arquivos parquet gerados...")
        
        # Mapeamento de tipos para nomes de diretórios
        tipo_to_folder = {
            'empresas': 'empresa',
            'estabelecimentos': 'estabelecimento', 
            'simples': 'simples',
            'socios': 'socio'
        }
        
        tipos_faltando = []
        tipos_verificados = []
        
        for tipo in tipos_processados:
            folder_name = tipo_to_folder.get(tipo, tipo)
            parquet_path = os.path.join(output_parquet_path, folder_name)
            
            # Verificar se o diretório existe
            if not os.path.exists(parquet_path):
                logger.error(f"❌ Diretório não encontrado para {tipo}: {parquet_path}")
                tipos_faltando.append(tipo)
                continue
            
            # Verificar se há arquivos parquet no diretório
            try:
                parquet_files = [f for f in os.listdir(parquet_path) if f.endswith('.parquet')]
                if not parquet_files:
                    logger.error(f"❌ Nenhum arquivo parquet encontrado para {tipo} em: {parquet_path}")
                    tipos_faltando.append(tipo)
                    continue
                
                # Verificar se pelo menos um arquivo parquet é válido
                valid_files = 0
                total_size = 0
                
                for parquet_file in parquet_files:
                    file_path = os.path.join(parquet_path, parquet_file)
                    try:
                        # Verificar tamanho do arquivo (arquivos muito pequenos são suspeitos)
                        file_size = os.path.getsize(file_path)
                        if file_size < 1024:  # Menor que 1KB é suspeito
                            logger.warning(f"⚠️ Arquivo parquet muito pequeno: {parquet_file} ({file_size} bytes)")
                            continue
                        
                        # Verificar se o arquivo parquet é válido
                        import pyarrow.parquet as pq
                        pq.read_metadata(file_path)
                        valid_files += 1
                        total_size += file_size
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Arquivo parquet corrompido ou inválido: {parquet_file} - {e}")
                        continue
                
                if valid_files == 0:
                    logger.error(f"❌ Nenhum arquivo parquet válido encontrado para {tipo}")
                    tipos_faltando.append(tipo)
                    continue
                
                # Log de sucesso
                size_mb = total_size / (1024 * 1024)
                logger.info(f"✅ {tipo}: {valid_files} arquivos válidos, {size_mb:.1f} MB")
                tipos_verificados.append(tipo)
                
            except Exception as e:
                logger.error(f"❌ Erro ao verificar diretório {tipo}: {e}")
                tipos_faltando.append(tipo)
        
        # Resultado final
        sucesso_completo = len(tipos_faltando) == 0
        
        if sucesso_completo:
            logger.info(f"✅ Verificação completa: Todos os {len(tipos_verificados)} tipos processados com sucesso")
        else:
            logger.error(f"❌ Verificação falhou: {len(tipos_faltando)} tipo(s) com problemas: {', '.join(tipos_faltando)}")
        
        return sucesso_completo, tipos_faltando
        
    except Exception as e:
        logger.error(f"❌ Erro durante verificação de integridade: {e}")
        return False, tipos_processados

def main():
    """Função principal de execução."""
    return asyncio.run(async_main())

async def async_main():
    """Função principal assíncrona de execução."""
    global overall_success
    overall_success = True
    
    start_time = time.time()  # Definir start_time no início
    
    # Inicializar variáveis de tempo para evitar erros
    download_time = 0.0
    process_time = 0.0
    db_time = 0.0
    latest_folder = ""
    
    # Parser de argumentos
    parser = argparse.ArgumentParser(
        description=get_full_description()
    )
    
    parser.add_argument('--tipos', '-t', nargs='+', choices=['empresas', 'estabelecimentos', 'simples', 'socios'],
                         default=[], help='Tipos de dados a serem processados. Se não especificado, processa todos (relevante para steps \'process\' e \'all\').')
    parser.add_argument('--step', '-s', choices=['download', 'process', 'database', 'painel', 'all'], default='all',
                         help='Etapa a ser executada. Padrão: all')
    parser.add_argument('--quiet', '-q', action='store_true',
                         help='Modo silencioso - reduz drasticamente as saídas no console')
    parser.add_argument('--verbose-ui', '-v', action='store_true',
                         help='Interface visual mais completa - só funciona com UI interativo')
    parser.add_argument('--log-level', '-l', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                         help='Nível de logging. Padrão: INFO')
    parser.add_argument('--remote-folder', '-r', type=str, 
                         help='Usar uma pasta específica do servidor (formato AAAA-MM). Exemplo: 2024-05')
    parser.add_argument('--all-folders', '-a', action='store_true',
                         help='Baixar de todas as pastas disponíveis do servidor. Sobrescreve --remote-folder')
    parser.add_argument('--from-folder', '-f', type=str,
                         help='Iniciar download/processamento a partir de uma pasta específica (formato AAAA-MM)')
    parser.add_argument('--force-download', '-F', action='store_true',
                         help='Forçar download mesmo que arquivo já exista')
    parser.add_argument('--criar-empresa-privada', '-E', action='store_true',
                         help='Criar subconjunto de empresas privadas (apenas para empresas)')
    parser.add_argument('--criar-subset-uf', '-U', type=str, metavar='UF',
                         help='Criar subconjunto por UF (apenas para estabelecimentos). Ex: --criar-subset-uf SP')
    parser.add_argument('--output-subfolder', '-o', type=str,
                         help='Nome da subpasta onde salvar os arquivos parquet')
    parser.add_argument('--source-zip-folder', '-z', type=str,
                         help='Pasta de origem dos arquivos ZIP (para step \'process\')')
    parser.add_argument('--process-all-folders', '-p', action='store_true',
                         help='Processar todas as pastas de data (formato AAAA-MM) em PATH_ZIP')
    parser.add_argument('--delete-zips-after-extract', '-d', action='store_true',
                         help='Deletar arquivos ZIP após extração bem-sucedida (economiza espaço)')
    parser.add_argument('--cleanup-after-db', '-c', action='store_true',
                         help='Deletar arquivos parquet após criação do banco DuckDB (economiza espaço)')
    parser.add_argument('--cleanup-all-after-db', '-C', action='store_true',
                         help='Deletar arquivos parquet E ZIP após criação do banco (máxima economia)')
    parser.add_argument('--show-progress', '-B', action='store_true',
                         help='Forçar exibição da barra de progresso (sobrescreve config)')
    parser.add_argument('--hide-progress', '-H', action='store_true',
                         help='Forçar ocultação da barra de progresso (sobrescreve config)')
    parser.add_argument('--show-pending', '-S', action='store_true',
                         help='Forçar exibição da lista de arquivos pendentes (sobrescreve config)')
    parser.add_argument('--hide-pending', '-W', action='store_true',
                         help='Forçar ocultação da lista de arquivos pendentes (sobrescreve config)')
    parser.add_argument('--processar-painel', '-P', action='store_true',
                         help='Processar dados do painel (combina estabelecimentos + simples + empresas)')
    parser.add_argument('--painel-uf', type=str, metavar='UF',
                         help='Filtrar painel por UF específica (ex: SP, GO, MG)')
    parser.add_argument('--painel-situacao', type=int, metavar='CODIGO',
                         help='Filtrar painel por situação cadastral (1=Nula, 2=Ativa, 3=Suspensa, 4=Inapta, 8=Baixada)')
    parser.add_argument('--painel-incluir-inativos', action='store_true',
                         help='Incluir estabelecimentos inativos no painel')

    args = parser.parse_args()
    
    # Configurar logging
    logger = setup_logging(args.log_level)
    
    # Configurar manipulador de sinal de emergência
    def emergency_stop_main():
        """Manipulador de emergência para sinais críticos."""
        print("\n🛑 SINAL DE EMERGÊNCIA RECEBIDO!")
        print("⚠️ Interrompendo execução...")
        logger.critical("🛑 Execução interrompida por sinal de emergência")
        global overall_success
        overall_success = False
        sys.exit(1)

    # Registrar manipulador de sinal
    signal.signal(signal.SIGINT, lambda s, f: emergency_stop_main())
    signal.signal(signal.SIGTERM, lambda s, f: emergency_stop_main())
    
    # Carregar variáveis de ambiente
    load_dotenv()
    print_header("Carregando variáveis de ambiente...")
    PATH_ZIP = os.getenv('PATH_ZIP', './dados-zip')
    PATH_UNZIP = os.getenv('PATH_UNZIP', './dados-unzip')
    PATH_PARQUET = os.getenv('PATH_PARQUET', './dados-parquet')
    FILE_DB_PARQUET = os.getenv('FILE_DB_PARQUET', 'cnpj.duckdb')
    PATH_REMOTE_PARQUET = os.getenv('PATH_REMOTE_PARQUET', 'destino/')
    
    if PATH_ZIP and PATH_UNZIP and PATH_PARQUET:
        print_success("Variáveis de ambiente carregadas com sucesso")
        logger.info(f"PATH_ZIP = {PATH_ZIP}")
        logger.info(f"PATH_UNZIP = {PATH_UNZIP}")
        logger.info(f"PATH_PARQUET = {PATH_PARQUET}")
        logger.info(f"FILE_DB_PARQUET = {FILE_DB_PARQUET}")
        logger.info(f"PATH_REMOTE_PARQUET = {PATH_REMOTE_PARQUET}")
    else:
        print_error("Erro ao carregar variáveis de ambiente. Verifique o arquivo .env")
        logger.error("Variáveis de ambiente PATH_ZIP, PATH_UNZIP ou PATH_PARQUET não definidas")
        return False, ""
        
    if not initialize_processors():
        print_error("Falha ao inicializar nova arquitetura. Verifique os logs.")
        return False, ""
    print_success("Arquitetura refatorada inicializada com sucesso")
    
    # Verificar conectividade de rede antes de qualquer operação
    if not check_internet_connection():
        logger.warning("⚠️ Sem conectividade de rede. Algumas funcionalidades podem estar limitadas.")
        report_critical_failure(
            FailureType.CONNECTIVITY,
            "Sem conexão com a internet",
            "MAIN_CONNECTIVITY"
        )
    
    # Verificar espaço em disco
    if not check_disk_space():
        logger.warning("⚠️ Espaço em disco limitado. Monitorando recursos durante execução.")
    
    # Inicializar sistema de estatísticas
    global_stats.start_session()
    
    # Processamento exclusivo do painel
    if args.step == 'painel':
        print_header("Processamento Exclusivo do Painel Consolidado")
        
        # Verificar se foi especificada uma pasta de origem ou usar padrão
        if args.source_zip_folder:
            # Usar pasta específica fornecida pelo usuário
            source_zip_path = args.source_zip_folder
            if not os.path.isabs(source_zip_path):
                source_zip_path = os.path.join(PATH_ZIP, source_zip_path)
            
            # Extrair nome da pasta para usar como output
            folder_name = os.path.basename(source_zip_path.rstrip('/\\'))
            output_subfolder = args.output_subfolder if args.output_subfolder else folder_name
        else:
            # Tentar determinar a pasta mais recente ou usar --remote-folder
            if args.remote_folder:
                latest_folder = args.remote_folder
                logger.info(f"Usando pasta remota especificada para painel: {latest_folder}")
            else:
                # Obter pasta mais recente
                try:
                    from src.async_downloader import get_latest_remote_folder
                    base_url = os.getenv('BASE_URL', 'https://dados.rfb.gov.br/CNPJ/')
                    latest_folder = await get_latest_remote_folder(base_url)
                    if not latest_folder:
                        logger.error("Não foi possível determinar a pasta remota. Use --source-zip-folder ou --remote-folder")
                        return False, ""
                    logger.info(f"Pasta mais recente detectada para painel: {latest_folder}")
                except Exception as e:
                    logger.error(f"Erro ao obter pasta remota: {e}")
                    logger.error("Use --source-zip-folder para especificar os dados a processar")
                    return False, ""

            source_zip_path = os.path.join(PATH_ZIP, latest_folder)
            output_subfolder = args.output_subfolder if args.output_subfolder else latest_folder
        
        # Definir pasta de saída
        output_parquet_path = os.path.join(PATH_PARQUET, output_subfolder)
        
        logger.info(f"Processando painel com dados de: {source_zip_path}")
        logger.info(f"Salvando painel em: {output_parquet_path}")
        
        # Verificar se as pastas de dados existem
        if not os.path.exists(source_zip_path):
            logger.error(f"Pasta de dados não encontrada: {source_zip_path}")
            logger.error("Execute primeiro o download e processamento dos dados ou use --source-zip-folder")
            return False, ""

        # Processar painel
        painel_start_time = time.time()
        
        painel_success = process_painel_complete(
            source_zip_path=source_zip_path,
            unzip_path=PATH_UNZIP,
            output_parquet_path=output_parquet_path,
            uf_filter=args.painel_uf,
            situacao_filter=args.painel_situacao,
            output_filename=None  # Será gerado automaticamente
        )
        
        painel_time = time.time() - painel_start_time
        
        if painel_success:
            print_success(f"Processamento exclusivo do painel concluído em {format_elapsed_time(painel_time)}")
            
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: SUCESSO")
            logger.info("=" * 50)
            
            # Finalizar estatísticas
            global_stats.end_session()
            global_stats.print_detailed_report()
            
            return True, output_subfolder
        else:
            print_error("Falha no processamento exclusivo do painel")
            
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            
            return False, ""
    
    # Se chegou até aqui após processamento bem-sucedido, usar pipeline otimizado
    if args.step == 'all':
        remote_folder_param = args.remote_folder if args.remote_folder else None
        from_folder_param = args.from_folder if args.from_folder else None
        
        # Determinar pasta remota a usar
        if remote_folder_param:
            latest_folder = remote_folder_param
            logger.info(f"Usando pasta remota especificada: {latest_folder}")
        else:
            # Obter pasta mais recente
            from src.async_downloader import get_latest_remote_folder
            base_url = os.getenv('BASE_URL', 'https://dados.rfb.gov.br/CNPJ/')
            latest_folder = await get_latest_remote_folder(base_url)
            if not latest_folder:
                logger.error("Não foi possível determinar a pasta remota mais recente")
                return False, ""
            logger.info(f"Pasta remota mais recente: {latest_folder}")

        # Definir caminhos
        source_zip_path = os.path.join(PATH_ZIP, latest_folder)
        output_subfolder = args.output_subfolder if args.output_subfolder else latest_folder
        output_parquet_path = os.path.join(PATH_PARQUET, output_subfolder)
        os.makedirs(source_zip_path, exist_ok=True)
        os.makedirs(output_parquet_path, exist_ok=True)
        
        logger.info(f"Processando arquivos de: {source_zip_path}")
        logger.info(f"Salvando Parquets em: {output_parquet_path}")
        
        # Obter URLs dos arquivos da pasta remota
        from src.async_downloader import get_latest_month_zip_urls, _filter_urls_by_type
        
        base_url = os.getenv('BASE_URL')
        if not base_url:
            logger.error("BASE_URL não definida no arquivo .env")
            return False, ""
            
        zip_urls, _ = get_latest_month_zip_urls(base_url, latest_folder)
            
        # Filtrar URLs por tipos desejados
        tipos_desejados = args.tipos if args.tipos else ['empresas', 'estabelecimentos', 'simples', 'socios']
        if tipos_desejados:
            zip_urls, ignored = _filter_urls_by_type(zip_urls, tuple(tipos_desejados))
            logger.info(f"Filtrados {ignored} URLs não desejadas para processamento. Restaram {len(zip_urls)} URLs.")
        
        # Lista de tipos a processar (todos ou filtrados)
        tipos_a_processar = args.tipos if args.tipos else ['empresas', 'estabelecimentos', 'simples', 'socios']
        
        # Preparar opções de processamento
        processing_options = {}
        if hasattr(args, 'criar_empresa_privada') and args.criar_empresa_privada:
            processing_options['create_private'] = True
        if hasattr(args, 'criar_subset_uf') and args.criar_subset_uf:
            processing_options['uf_subset'] = args.criar_subset_uf
        
        # 🆕 Executar pipeline otimizado unificado (download + processamento em paralelo)
        print_section("Pipeline Otimizado: Download e Processamento Paralelo")
        pipeline_start_time = time.time()
        
        logger.info("🚀 Iniciando pipeline otimizado: download e processamento em paralelo")
        logger.info(f"📋 Arquivos a processar: {len(zip_urls)}")
        logger.info(f"🎯 Tipos de dados: {', '.join(tipos_a_processar)}")
        
        process_results = await optimized_download_and_process_pipeline(
            urls=zip_urls,
            source_zip_path=source_zip_path,
            unzip_path=PATH_UNZIP,
            output_parquet_path=output_parquet_path,
            tipos_a_processar=tipos_a_processar,
            delete_zips_after_extract=args.delete_zips_after_extract,
            force_download=args.force_download,
            **processing_options
        )
        
        pipeline_time = time.time() - pipeline_start_time
        logger.info("=" * 50)
        logger.info(f"Tempo do pipeline otimizado: {format_elapsed_time(pipeline_time)}")
        
        # Simular tempos separados para compatibilidade com logs finais
        download_time = pipeline_time * 0.3  # Aproximadamente 30% do tempo em downloads
        process_time = pipeline_time * 0.7   # Aproximadamente 70% do tempo em processamento
        
        # Verificar se houve problemas no pipeline
        if not process_results.get('all_ok', False):
            print_warning("Alguns erros ocorreram durante o pipeline. O banco de dados NÃO será criado.")
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return False, ""
        else:
            print_success("Pipeline de download e processamento concluído com sucesso.")
        
        # Verificação adicional: confirmar que todos os parquets foram criados corretamente
        print_section("Verificando integridade dos dados processados")
        parquets_ok, tipos_faltando = check_parquet_completeness(output_parquet_path, tipos_a_processar)
        
        if not parquets_ok:
            print_error(f"Arquivos parquet incompletos ou corrompidos detectados para: {', '.join(tipos_faltando)}")
            print_error("O banco de dados DuckDB NÃO será criado devido a dados incompletos.")
            logger.error("Verificação de integridade dos parquets falhou")
            logger.error(f"Tipos com problemas: {', '.join(tipos_faltando)}")
            
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA - DADOS INCOMPLETOS")
            logger.info("=" * 50)
            return False, ""
        else:
            print_success("Verificação de integridade dos parquets concluída com sucesso.")
        
        # 2.5. Processamento do Painel (se solicitado)
        if args.processar_painel:
            print_section("Etapa 2.5: Processamento do Painel Consolidado")
            painel_start_time = time.time()
            
            painel_success = process_painel_complete(
                source_zip_path=source_zip_path,
                unzip_path=PATH_UNZIP,
                output_parquet_path=output_parquet_path,
                uf_filter=args.painel_uf,
                situacao_filter=args.painel_situacao,
                output_filename=None  # Será gerado automaticamente
            )
            
            painel_time = time.time() - painel_start_time
            logger.info("=" * 50)
            logger.info(f"Tempo de processamento do painel: {format_elapsed_time(painel_time)}")
            
            if painel_success:
                print_success("Processamento do painel concluído com sucesso.")
            else:
                print_warning("Falha no processamento do painel.")
                print_warning("⚠️ O painel consolidado não foi gerado, mas o banco de dados DuckDB será criado normalmente.")
                logger.warning("Processamento do painel falhou, mas continuando com criação do banco")
                logger.warning("O banco DuckDB será criado apenas com os dados das entidades individuais")
        
        # 3. Criação do banco de dados (verificações essenciais já passaram)
        print_section("Etapa 3: Criação do banco de dados DuckDB")
        logger.info("🎯 Verificações essenciais passaram - prosseguindo com criação do banco")
        db_start_time = time.time()
        
        try:
            logger.info(f"Criando arquivo DuckDB em: {output_parquet_path}")
            db_success = create_duckdb_file(output_parquet_path, FILE_DB_PARQUET, PATH_REMOTE_PARQUET)
            db_time = time.time() - db_start_time
            
            if db_success:
                logger.info("=" * 50)
                logger.info(f"Tempo de processamento do banco: {format_elapsed_time(db_time)}")
                db_file = os.path.join(output_parquet_path, FILE_DB_PARQUET)
                print_success(f"Banco de dados DuckDB criado com sucesso em: {db_file}")
                
                # Realizar limpeza se solicitada
                if args.cleanup_after_db or args.cleanup_all_after_db:
                    cleanup_zip = args.cleanup_all_after_db
                    
                    cleanup_success = cleanup_after_database(
                        parquet_folder=output_parquet_path,
                        zip_folder=source_zip_path if cleanup_zip else "",
                        cleanup_parquet=True,  # Sempre limpar parquet se foi solicitado
                        cleanup_zip=cleanup_zip
                    )
                    
                    if not cleanup_success:
                        print_warning("Alguns arquivos podem não ter sido removidos durante a limpeza")
                
            else:
                logger.info("=" * 50)
                logger.info(f"Tempo de processamento do banco (falhou): {format_elapsed_time(db_time)}")
                print_error("Falha ao criar banco de dados. Verifique os logs para mais detalhes.")
                logger.error("Criação do banco de dados falhou")
                total_time = time.time() - start_time
                logger.info("=" * 50)
                logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
                logger.info("STATUS FINAL: FALHA")
                logger.info("=" * 50)
                return False, ""
        except Exception as e:
            db_time = time.time() - db_start_time
            logger.exception(f"Erro ao criar banco de dados: {e}")
            logger.info("=" * 50)
            logger.info(f"Tempo de processamento do banco (erro): {format_elapsed_time(db_time)}")
            print_error(f"Falha ao criar banco de dados: {str(e)}")
            total_time = time.time() - start_time
            logger.info("=" * 50)
            logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
            logger.info("STATUS FINAL: FALHA")
            logger.info("=" * 50)
            return False, ""

    total_time = time.time() - start_time
    
    # Finalizar coleta de estatísticas
    global_stats.end_session()

    # Resumo final
    print_header("Processamento concluído")
    logger.info("=" * 50)
    logger.info("RESUMO FINAL DE EXECUÇÃO:")
    logger.info("=" * 50)
    
    if args.step == 'all':
        logger.info(f"Download: {format_elapsed_time(download_time)}")
        logger.info(f"Processamento: {format_elapsed_time(process_time)}")
        logger.info(f"Criação do banco: {format_elapsed_time(db_time)}")
    
    logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {format_elapsed_time(total_time)}")
    logger.info("=" * 50)
    logger.info("Execução concluída.")
    
    # Exibir relatório detalhado de estatísticas
    global_stats.print_detailed_report()
    
    # Salvar estatísticas em arquivo
    try:
        stats_filename = f"estatisticas_cnpj_{time.strftime('%Y%m%d_%H%M%S')}.json"
        stats_path = os.path.join("logs", stats_filename)
        os.makedirs("logs", exist_ok=True)
        global_stats.save_to_json(stats_path)
        print(f"\n📄 Estatísticas detalhadas salvas em: {stats_path}")
        logger.info(f"📄 Estatísticas detalhadas salvas em: {stats_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar estatísticas: {e}")
    
    return overall_success, latest_folder

def process_painel_complete(source_zip_path: str, unzip_path: str, output_parquet_path: str, 
                          uf_filter: str | None = None, situacao_filter: int | None = None, 
                          output_filename: str | None = None) -> bool:
    """
    Processa dados do painel combinando estabelecimentos, simples e empresas.
    
    Args:
        source_zip_path: Caminho dos arquivos ZIP
        unzip_path: Caminho para extração
        output_parquet_path: Caminho de saída
        uf_filter: Filtro por UF (opcional)
        situacao_filter: Filtro por situação cadastral (opcional)
        output_filename: Nome do arquivo de saída (opcional)
        
    Returns:
        bool: True se processamento foi bem-sucedido
    """
    try:
        logger.info("=" * 60)
        logger.info("🏢 INICIANDO PROCESSAMENTO DO PAINEL CONSOLIDADO")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        # Definir caminhos dos parquets das entidades individuais
        estabelecimento_path = os.path.join(output_parquet_path, 'estabelecimento')
        simples_path = os.path.join(output_parquet_path, 'simples') 
        empresa_path = os.path.join(output_parquet_path, 'empresa')
        
        # Verificar se os parquets das entidades individuais existem
        missing_paths = []
        if not os.path.exists(estabelecimento_path) or not os.listdir(estabelecimento_path):
            missing_paths.append('estabelecimento')
        if not os.path.exists(simples_path) or not os.listdir(simples_path):
            missing_paths.append('simples')
        if not os.path.exists(empresa_path) or not os.listdir(empresa_path):
            missing_paths.append('empresa')
        
        if missing_paths:
            logger.error(f"Parquets não encontrados para: {', '.join(missing_paths)}")
            logger.error("Execute primeiro o processamento das entidades individuais")
            return False
        
        # Configurar opções do processador
        painel_options = {
            'path_zip': source_zip_path,
            'path_unzip': unzip_path,
            'path_parquet': output_parquet_path,
            'estabelecimento_path': estabelecimento_path,
            'simples_path': simples_path,
            'empresa_path': empresa_path,
            'skip_download': True,
            'skip_unzip': True,
            'skip_individual_processing': True,
        }
        
        # Adicionar filtros se especificados
        if uf_filter:
            painel_options['uf_filter'] = uf_filter.upper()
            logger.info(f"Filtro por UF aplicado: {uf_filter.upper()}")
        
        if situacao_filter is not None:
            painel_options['situacao_filter'] = situacao_filter
            situacao_map = {1: 'Nula', 2: 'Ativa', 3: 'Suspensa', 4: 'Inapta', 8: 'Baixada'}
            situacao_nome = situacao_map.get(situacao_filter, f'Código {situacao_filter}')
            logger.info(f"Filtro por situação aplicado: {situacao_nome}")
        
        # Criar processador do painel
        processor = PainelProcessor(**painel_options)
        
        # Processar dados do painel
        if not output_filename:
            output_filename = "painel_dados.parquet"
        
        logger.info(f"Arquivo de saída: {output_filename}")
        
        success = processor.process_painel_data(output_filename)
        
        elapsed_time = time.time() - start_time
        
        if success:
            output_path = os.path.join(output_parquet_path, output_filename)
            logger.info("=" * 60)
            logger.info(f"✅ PAINEL PROCESSADO COM SUCESSO em {format_elapsed_time(elapsed_time)}")
            logger.info(f"📄 Arquivo salvo em: {output_path}")
            logger.info("=" * 60)
            return True
        else:
            logger.error("=" * 60)
            logger.error(f"❌ FALHA NO PROCESSAMENTO DO PAINEL após {format_elapsed_time(elapsed_time)}")
            logger.error("=" * 60)
            return False
            
    except Exception as e:
        logger.error(f"Erro no processamento do painel: {e}")
        return False

async def optimized_download_and_process_pipeline(
    urls: List[str], 
    source_zip_path: str, 
    unzip_path: str, 
    output_parquet_path: str,
    tipos_a_processar: List[str],
    delete_zips_after_extract: bool = False,
    force_download: bool = False,
    **processing_options
) -> dict:
    """
    Pipeline otimizado que baixa e processa arquivos em paralelo.
    """
    from src.async_downloader import _filter_urls_by_type
    
    # ✅ CORREÇÃO 1: Filtrar URLs antes de processar para evitar baixar arquivos auxiliares
    logger.info("🔍 Filtrando URLs por tipos desejados...")
    filtered_urls, ignored_count = _filter_urls_by_type(urls, tuple(tipos_a_processar))
    
    if ignored_count > 0:
        logger.info(f"📊 Filtrados {ignored_count} arquivos auxiliares (Cnaes, Motivos, etc.)")
        logger.info(f"🎯 URLs válidos para processamento: {len(filtered_urls)}")
    
    # Usar URLs filtrados em vez dos URLs originais
    urls = filtered_urls
    
    # Controlar concorrência
    max_concurrent_downloads = 3  # Baseado no teste de rede
    # ✅ CORREÇÃO 2: Aumentar limite de processamento paralelo
    max_concurrent_processing = 4  # Permitir mais processamentos simultâneos
    
    download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
    process_semaphore = asyncio.Semaphore(max_concurrent_processing)  # Aumentado de 3 para 4
    
    # Listas para rastrear resultados
    successful_downloads = []
    failed_downloads = []
    processed_files = {}
    
    # Criar processadores
    processors = {}
    processing_results = {}
    
    logger.info(f"📋 Criando processadores para: {', '.join(tipos_a_processar)}")
    
    for tipo in tipos_a_processar:
        processor_key = {
            'empresas': 'empresa',
            'estabelecimentos': 'estabelecimento', 
            'simples': 'simples',
            'socios': 'socio',
            'painel': 'painel'
        }.get(tipo.lower())
        
        if processor_key:
            try:
                processor = ProcessorFactory.create(
                    processor_key,
                    source_zip_path,
                    unzip_path, 
                    output_parquet_path,
                    delete_zips_after_extract=delete_zips_after_extract,
                    **processing_options
                )
                processors[processor_key] = processor
                processing_results[tipo] = {'success': False, 'time': 0, 'files_processed': 0}
                logger.info(f"✅ Processador '{processor_key}' criado com sucesso")
            except Exception as e:
                logger.error(f"❌ Erro ao criar processador '{processor_key}': {e}")
                processing_results[tipo] = {'success': False, 'time': 0, 'error': str(e)}
    
    logger.info(f"📊 Total de processadores criados: {len(processors)}")
    
    # Obter configurações de rede
    try:
        network_results = await get_network_test_results()
        max_concurrent_downloads = min(6, network_results.get("recommendations", {}).get("max_concurrent_downloads", 3))
        connection_quality = network_results.get("quality", {}).get("connection_quality", "unknown")
    except Exception as e:
        logger.warning(f"Erro ao obter configurações de rede: {e}")
        max_concurrent_downloads = 3
        connection_quality = "unknown"
    
    logger.info(f"🌐 Rede: {connection_quality}")
    logger.info(f"🔧 Downloads simultâneos: {max_concurrent_downloads}")
    
    # Configurar semáforos
    download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
    process_semaphore = asyncio.Semaphore(max_concurrent_processing)  # Aumentado de 3 para 4
    
    # Listas para rastreamento
    successful_downloads = []
    failed_downloads = []
    processed_files = {}
    
    # Função para processar arquivo imediatamente após download/verificação
    async def process_file_immediately(file_path: str, filename: str) -> bool:
        """Processa um arquivo assim que ele está disponível."""
        async with process_semaphore:  # Controlar processamentos simultâneos
            # Determinar tipo do processador baseado no nome do arquivo
            processor_key = None
            tipo_original = None
            
            if filename.startswith('Empr'):
                processor_key = 'empresa'
                tipo_original = 'empresas'
            elif filename.startswith('Estabel'):
                processor_key = 'estabelecimento'
                tipo_original = 'estabelecimentos'
            elif filename.startswith('Simples'):
                processor_key = 'simples'
                tipo_original = 'simples'
            elif filename.startswith('Socio'):
                processor_key = 'socio'
                tipo_original = 'socios'
            elif filename.startswith('Painel'):
                processor_key = 'painel'
                tipo_original = 'painel'
            
            if not processor_key or processor_key not in processors:
                logger.warning(f"⚠️ Processador não encontrado para {filename} (tipo: {processor_key})")
                return False
            
            try:
                start_time = time.time()
                logger.info(f"🔄 Iniciando processamento de {filename}")
                
                processor = processors[processor_key]
                
                # ✅ CORREÇÃO 3: Executar processamento em thread separada para não bloquear event loop
                import concurrent.futures
                loop = asyncio.get_event_loop()
                
                # Executar processamento em executor para liberação do event loop
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    success = await loop.run_in_executor(
                        executor,
                        lambda: processor.process_single_zip(
                            filename, 
                            source_zip_path, 
                            unzip_path, 
                            output_parquet_path, 
                            **processing_options
                        )
                    )
                
                elapsed_time = time.time() - start_time
                
                if success:
                    logger.info(f"✅ {filename} processado com sucesso em {elapsed_time:.1f}s")
                    if processor_key not in processed_files:
                        processed_files[processor_key] = []
                    processed_files[processor_key].append(filename)
                    
                    # Atualizar resultados
                    if tipo_original in processing_results:
                        processing_results[tipo_original]['files_processed'] += 1
                    return True
                else:
                    logger.error(f"❌ Falha ao processar {filename}")
                    return False
                    
            except Exception as e:
                logger.error(f"❌ Erro no processamento de {filename}: {e}")
                return False
    
    # Função para verificar/baixar e processar imediatamente
    async def download_and_process_immediately(url: str, session: aiohttp.ClientSession):
        """Baixa/verifica um arquivo e o processa imediatamente."""
        filename = os.path.basename(url)
        destination_path = os.path.join(source_zip_path, filename)
        
        try:
            async with download_semaphore:
                # Verificar se arquivo já existe E está íntegro
                if os.path.exists(destination_path) and not force_download:
                    # Verificar integridade do arquivo
                    if await validate_zip_integrity(destination_path):
                        logger.info(f"✅ Arquivo {filename} já existe e está íntegro. Processando imediatamente...")
                        successful_downloads.append(destination_path)
                        
                        # Processar imediatamente
                        await process_file_immediately(destination_path, filename)
                        return
                    else:
                        logger.error(f"❌ Arquivo {filename} baixado mas falhou na validação de integridade")
                        failed_downloads.append((filename, "Falha na validação de integridade"))
                        # Remover arquivo corrompido
                        try:
                            os.remove(destination_path)
                        except Exception:
                            pass
                else:
                    logger.warning(f"⚠️ Arquivo {filename} existe mas está corrompido. Fazendo novo download...")
                    # Remover arquivo corrompido
                    try:
                        os.remove(destination_path)
                    except Exception as e:
                        logger.warning(f"Erro ao remover arquivo corrompido {filename}: {e}")
                
                # Fazer download
                logger.info(f"📥 Baixando {filename}...")
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            with open(destination_path, 'wb') as f:
                                async for chunk in response.content.iter_chunked(8192):
                                    f.write(chunk)
                            
                            # Validar arquivo após download
                            if await validate_zip_integrity(destination_path):
                                logger.info(f"✅ Download de {filename} concluído e validado")
                                successful_downloads.append(destination_path)
                                
                                # Processar imediatamente após download
                                await process_file_immediately(destination_path, filename)
                            else:
                                logger.error(f"❌ Arquivo {filename} baixado mas falhou na validação de integridade")
                                failed_downloads.append((filename, "Falha na validação de integridade"))
                                # Remover arquivo corrompido
                                try:
                                    os.remove(destination_path)
                                except Exception:
                                    pass
                        else:
                            error_msg = f"HTTP {response.status}"
                            logger.error(f"❌ Erro no download de {filename}: {error_msg}")
                            failed_downloads.append((filename, error_msg))
                except Exception as download_error:
                    logger.error(f"❌ Erro no download de {filename}: {download_error}")
                    failed_downloads.append((filename, str(download_error)))
            
        except Exception as e:
            logger.error(f"❌ Erro inesperado com {filename}: {e}")
            failed_downloads.append((filename, str(e)))
    
    # Função para validar integridade de arquivo ZIP
    async def validate_zip_integrity(file_path: str) -> bool:
        """Valida se um arquivo ZIP está íntegro."""
        import zipfile
        
        try:
            if not os.path.exists(file_path):
                return False
            
            # Verificar tamanho mínimo (arquivos muito pequenos são suspeitos)
            file_size = os.path.getsize(file_path)
            if file_size < 1024:  # Menor que 1KB é suspeito
                logger.warning(f"Arquivo {os.path.basename(file_path)} muito pequeno: {file_size} bytes")
                return False
            
            # Verificar se é um ZIP válido
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    # Tentar listar o conteúdo (detecta corrupção)
                    file_list = zip_ref.namelist()
                    if not file_list:
                        logger.warning(f"Arquivo ZIP {os.path.basename(file_path)} está vazio")
                        return False
                    
                    # Verificar se pelo menos um arquivo pode ser lido
                    first_file = file_list[0]
                    try:
                        with zip_ref.open(first_file) as f:
                            # Ler primeiro chunk para verificar se não está corrompido
                            f.read(1024)
                    except Exception as e:
                        logger.warning(f"Erro ao ler conteúdo do ZIP {os.path.basename(file_path)}: {e}")
                        return False
                
                return True
                
            except zipfile.BadZipFile:
                logger.warning(f"Arquivo {os.path.basename(file_path)} não é um ZIP válido")
                return False
            except Exception as e:
                logger.warning(f"Erro ao validar ZIP {os.path.basename(file_path)}: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Erro geral na validação de {os.path.basename(file_path)}: {e}")
            return False
    
    # Executar downloads e processamentos em paralelo
    start_time = time.time()
    
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=3600, connect=30),
        connector=aiohttp.TCPConnector(limit=100, limit_per_host=20)
    ) as session:
        
        # Criar tasks para todos os URLs
        tasks = [download_and_process_immediately(url, session) for url in urls]
        
        logger.info(f"🚀 Iniciando pipeline com {len(tasks)} arquivos...")
        logger.info("📊 Cada arquivo será processado assim que for verificado/baixado")
        logger.info(f"⚙️ Configuração: máx {max_concurrent_downloads} downloads + máx {max_concurrent_processing} processamentos simultâneos")
        
        # Executar todas as tasks em paralelo
        await asyncio.gather(*tasks, return_exceptions=True)
    
    total_time = time.time() - start_time
    
    # Calcular estatísticas finais
    for tipo in tipos_a_processar:
        processor_key = {
            'empresas': 'empresa',
            'estabelecimentos': 'estabelecimento', 
            'simples': 'simples',
            'socios': 'socio',
            'painel': 'painel'
        }.get(tipo.lower())
        
        if processor_key in processed_files:
            files_count = len(processed_files[processor_key])
            processing_results[tipo]['success'] = files_count > 0
            processing_results[tipo]['files_processed'] = files_count
        
        processing_results[tipo]['time'] = total_time
    
    # Log do resumo
    logger.info("=" * 60)
    logger.info("📊 RESUMO DO PIPELINE OTIMIZADO:")
    logger.info("=" * 60)
    logger.info(f"⏱️  Tempo total: {format_elapsed_time(total_time)}")
    logger.info(f"📥 Downloads bem-sucedidos: {len(successful_downloads)}")
    logger.info(f"❌ Downloads com falha: {len(failed_downloads)}")
    
    for tipo, result in processing_results.items():
        if isinstance(result, dict) and 'success' in result:
            status = "✅ SUCESSO" if result['success'] else "❌ FALHA"
            files_processed = result.get('files_processed', 0)
            logger.info(f"{tipo.upper()}: {status} - {files_processed} arquivos processados")
    
    logger.info("=" * 60)
    
    # Determinar sucesso geral
    all_success = all(result.get('success', False) for result in processing_results.values() if isinstance(result, dict) and 'success' in result)
    processing_results['all_ok'] = all_success
    processing_results['total_time'] = total_time
    processing_results['downloads_successful'] = len(successful_downloads)
    processing_results['downloads_failed'] = len(failed_downloads)
    
    return processing_results

def cleanup_after_database(parquet_folder: str, zip_folder: str = "", cleanup_parquet: bool = False, cleanup_zip: bool = False) -> bool:
    """
    Realiza limpeza de arquivos após criação bem-sucedida do banco DuckDB.
    
    Args:
        parquet_folder: Pasta contendo os arquivos parquet
        zip_folder: Pasta contendo os arquivos ZIP (opcional)
        cleanup_parquet: Se deve deletar os arquivos parquet
        cleanup_zip: Se deve deletar os arquivos ZIP
        
    Returns:
        bool: True se limpeza foi bem-sucedida, False caso contrário
    """
    success = True
    
    if not cleanup_parquet and not cleanup_zip:
        logger.debug("Nenhuma limpeza solicitada")
        return True
    
    print_section("Realizando limpeza de arquivos")
    
    try:
        # Contadores para estatísticas
        parquet_files_deleted = 0
        parquet_size_freed = 0
        zip_files_deleted = 0
        zip_size_freed = 0
        
        # Limpar arquivos parquet se solicitado
        if cleanup_parquet and os.path.exists(parquet_folder):
            logger.info(f"Iniciando limpeza de arquivos parquet em: {parquet_folder}")
            
            for root, dirs, files in os.walk(parquet_folder):
                for file in files:
                    if file.endswith('.parquet'):
                        file_path = os.path.join(root, file)
                        try:
                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            parquet_files_deleted += 1
                            parquet_size_freed += file_size
                            logger.debug(f"Arquivo parquet deletado: {file}")
                        except Exception as e:
                            logger.error(f"Erro ao deletar arquivo parquet {file}: {e}")
                            success = False
        
        # Limpar arquivos ZIP se solicitado
        if cleanup_zip and zip_folder and os.path.exists(zip_folder):
            logger.info(f"Iniciando limpeza de arquivos ZIP em: {zip_folder}")
            
            for root, dirs, files in os.walk(zip_folder):
                for file in files:
                    if file.endswith('.zip'):
                        file_path = os.path.join(root, file)
                        try:
                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            zip_files_deleted += 1
                            zip_size_freed += file_size
                            logger.debug(f"Arquivo ZIP deletado: {file}")
                        except Exception as e:
                            logger.error(f"Erro ao deletar arquivo ZIP {file}: {e}")
                            success = False
        
        # Exibir estatísticas da limpeza
        total_size_freed = parquet_size_freed + zip_size_freed
        total_files_deleted = parquet_files_deleted + zip_files_deleted
        
        if total_files_deleted > 0:
            size_freed_mb = total_size_freed / (1024 * 1024)
            size_freed_gb = size_freed_mb / 1024
            
            if size_freed_gb >= 1:
                print_success(f"Limpeza concluída: {total_files_deleted} arquivos removidos, {size_freed_gb:.2f} GB liberados")
            else:
                print_success(f"Limpeza concluída: {total_files_deleted} arquivos removidos, {size_freed_mb:.2f} MB liberados")
        else:
            print_warning("Nenhum arquivo foi removido durante a limpeza")
        
        return success
        
    except Exception as e:
        logger.error(f"Erro durante limpeza de arquivos: {e}")
        print_error(f"Erro durante limpeza: {e}")
        return False

if __name__ == '__main__':
    main()
