# benchmark_empresa.py
"""
Benchmark para comparar processamento de dados de Empresas usando Pandas, Dask e Polars.

INSTALAÇÃO DAS DEPENDÊNCIAS:
===========================
Dependências necessárias:
```
pip install matplotlib pandas numpy psutil py-cpuinfo tqdm memory_profiler
```

Dependências opcionais:
```
pip install gputil setuptools  # Para informações de GPU
```

Se você encontrar erro 'No module named distutils', instale setuptools:
```
pip install setuptools
```

EXEMPLOS DE USO:
===============
# Para benchmark completo (ambos os métodos + gráficos) usando todos os ZIPs do diretório:
python benchmark_empresa.py --completo --path_zip dados-abertos-zip --parquet_destino parquet/2025-03/empresas

# Benchmark com arquivo específico:
python benchmark_empresa.py --completo --path_zip dados-abertos-zip --arquivo_zip Empresa.zip --parquet_destino parquet/2025-03/empresas

# Benchmark com padrão de arquivos (usando glob):
python benchmark_empresa.py --completo --path_zip dados-abertos-zip --arquivo_zip "Empre*.*" --parquet_destino parquet/2025-03/empresas

# Usar automaticamente o número ótimo de workers (recomendado)
python benchmark_empresa.py --completo --path_zip dados-abertos-zip

# Especificar manualmente o número de workers
python benchmark_empresa.py --completo --path_zip dados-abertos-zip --workers 4

# Processar apenas com Pandas em paralelo
python benchmark_empresa.py --pandas --path_zip dados-abertos-zip
"""
import argparse
import gc
import json
import logging
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import psutil
import platform
import shutil
import sys
import time
import traceback
import zipfile
import fnmatch  # Adicionado para suportar padrões de busca
import concurrent.futures  # Para processamento paralelo
import threading  # Para monitoramento de recursos em background
from datetime import datetime
from typing import Dict, List, Any, Tuple
import glob

import dask.dataframe as dd
import dask
from dask import delayed
from dask.distributed import as_completed, Client, LocalCluster

# Importações internas do projeto
from src.config import config
from src.process.empresa import process_empresa, process_single_zip, process_single_zip_polars
from src.utils.dask_manager import DaskManager
from src.utils.logging import setup_logging, print_header, print_section, print_success, print_warning, print_error, Colors

# Torna o GPUtil opcional para evitar erros se não estiver disponível
try:
    import GPUtil
    GPUTIL_AVAILABLE = True
except ImportError:
    print("Aviso: GPUtil não está disponível. Informações de GPU não serão coletadas.")
    GPUTIL_AVAILABLE = False

from tqdm import tqdm

# Ajustar o path para importar os módulos do projeto
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar as funções do arquivo empresa.py
from src.process.empresa import (
    process_single_zip_pandas,   # Versão Pandas
    process_single_zip,          # Versão Dask
    process_single_zip_polars    # Versão Polars
)

# Configurando logging
logger = setup_logging()

# Definir nível de log mais alto para módulos específicos para reduzir verbosidade
logging.getLogger("src.process.empresa").setLevel(logging.WARNING)

class InfoSistema:
    """Coleta informações sobre o sistema onde o script está sendo executado."""
    
    @staticmethod
    def coletar_informacoes():
        """Coleta e retorna informações do sistema."""
        info = {}
        
        # Informações do sistema operacional
        info['sistema'] = platform.system()
        info['versao_sistema'] = platform.version()
        info['arquitetura'] = platform.architecture()[0]
        
        # Informações do processador
        try:
            # Usar platform em vez de psutil para obter informações do processador
            info['processador'] = platform.processor()
            if not info['processador']:
                info['processador'] = platform.machine()
        except Exception:
            info['processador'] = 'Desconhecido'
            
        info['cores_fisicos'] = psutil.cpu_count(logical=False)
        info['cores_logicos'] = psutil.cpu_count(logical=True)
        
        try:
            cpu_freq = psutil.cpu_freq()
            info['frequencia_mhz'] = f"{cpu_freq.max:.0f}" if cpu_freq and cpu_freq.max else 'Desconhecido'
        except Exception:
            info['frequencia_mhz'] = 'Desconhecido'
        
        # Informações de memória
        mem = psutil.virtual_memory()
        info['memoria_total'] = f"{mem.total / (1024**3):.2f} GB"
        info['memoria_disponivel'] = f"{mem.available / (1024**3):.2f} GB"
        
        # Informações de disco
        disk = psutil.disk_usage('/')
        info['disco_total'] = f"{disk.total / (1024**3):.2f} GB"
        info['disco_livre'] = f"{disk.free / (1024**3):.2f} GB"
        
        # Informações da GPU (se disponível)
        if GPUTIL_AVAILABLE:
            gpus = GPUtil.getGPUs()
            if gpus:
                info['gpu'] = gpus[0].name
                info['memoria_gpu'] = f"{gpus[0].memoryTotal} MB"
            else:
                info['gpu'] = "Nenhuma GPU detectada"
                info['memoria_gpu'] = "N/A"
        else:
            info['gpu'] = "Não foi possível obter informações da GPU"
            info['memoria_gpu'] = "N/A"
            
        return info

    @staticmethod
    def imprimir_informacoes(info):
        """Imprime as informações do sistema de forma resumida."""
        print("\n" + "="*40)
        print(" "*10 + "SISTEMA")
        print("="*40)
        
        print(f"Sistema: {info['sistema']} {info['versao_sistema']}")
        print(f"CPU: {info['processador']} ({info['cores_fisicos']} cores, {info['cores_logicos']} threads)")
        print(f"Memória: {info['memoria_total']} (Livre: {info['memoria_disponivel']})")
        print(f"Disco: {info['disco_livre']} livre de {info['disco_total']}")
        print("="*40 + "\n")

# Adicione um argumento para controlar verbosidade do log
def add_log_level_argument(parser):
    """Adiciona argumento para controlar o nível de log."""
    parser.add_argument('--verbose', action='store_true',
                      help='Ativar logs detalhados (DEBUG)')
    parser.add_argument('--quiet', action='store_true',
                      help='Reduzir logs ao mínimo (WARNING)')

def configure_logging(args):
    """Configura o nível de logging com base nos argumentos."""
    if hasattr(args, 'verbose') and args.verbose:
        logger.setLevel(logging.DEBUG)
        logging.getLogger("src.process.empresa").setLevel(logging.INFO)
    elif hasattr(args, 'quiet') and args.quiet:
        logger.setLevel(logging.WARNING)
        logging.getLogger("src.process.empresa").setLevel(logging.ERROR)
    else:
        # Nível padrão é INFO para o benchmark, mas WARNING para módulos específicos
        logger.setLevel(logging.INFO)
        logging.getLogger("src.process.empresa").setLevel(logging.WARNING)

class BenchmarkEmpresa:
    def __init__(self, path_zip, path_base, arquivo_zip_especifico=None, path_parquet_destino=None, executar_limpeza=True):
        """
        Inicializa o benchmark.
        
        Args:
            path_zip: Caminho para o diretório com os arquivos ZIP
            path_base: Caminho base para criar diretórios temporários
            arquivo_zip_especifico: Nome de um arquivo ZIP específico ou padrão (ex: "Empre*.*") para teste (opcional)
            path_parquet_destino: Caminho de destino para os arquivos parquet gerados
            executar_limpeza: Se True, limpa os diretórios antes de cada teste
        """
        self.path_zip = path_zip
        self.path_base = path_base
        self.executar_limpeza = executar_limpeza
        
        # Definir caminhos para parquet
        if path_parquet_destino:
            self.path_parquet_pandas = os.path.join(path_base, path_parquet_destino, "pandas")
            self.path_parquet_dask = os.path.join(path_base, path_parquet_destino, "dask")
            self.path_parquet_polars = os.path.join(path_base, path_parquet_destino, "polars")
        else:
            self.path_parquet_pandas = os.path.join(path_base, "parquet_pandas")
            self.path_parquet_dask = os.path.join(path_base, "parquet_dask")
            self.path_parquet_polars = os.path.join(path_base, "parquet_polars")
        
        # Criar diretórios para os testes
        self.path_unzip_pandas = os.path.join(path_base, "unzip_pandas")
        self.path_unzip_dask = os.path.join(path_base, "unzip_dask")
        self.path_unzip_polars = os.path.join(path_base, "unzip_polars")
        
        # Diretório temporário para calcular tamanho de arquivos extraídos
        self.path_unzip_temp = os.path.join(path_base, "unzip_temp")
        
        # Criar diretórios se não existirem
        for path in [self.path_unzip_pandas, self.path_unzip_dask, self.path_unzip_polars,
                     self.path_parquet_pandas, self.path_parquet_dask, self.path_parquet_polars,
                     self.path_unzip_temp]:
            os.makedirs(path, exist_ok=True)
        
        # Listar todos os arquivos no diretório
        todos_arquivos = os.listdir(path_zip)
        
        # Identificar arquivos ZIP de Empresas
        if arquivo_zip_especifico:
            # 1. Verificar se é um caminho direto para um arquivo existente
            caminho_arquivo_especifico = os.path.join(path_zip, arquivo_zip_especifico)
            if os.path.exists(caminho_arquivo_especifico) and os.path.isfile(caminho_arquivo_especifico):
                self.zip_files = [arquivo_zip_especifico]
                print(f"Usando arquivo específico: {arquivo_zip_especifico}")
            
            # 2. Se não for um arquivo específico, verificar se começa com "Empre"
            elif arquivo_zip_especifico.startswith("Empre"):
                print(f"Detectado prefixo 'Empre': buscando arquivos que começam com 'Empre' e terminam com .zip")
                self.zip_files = [f for f in todos_arquivos if f.startswith('Empre') and f.endswith('.zip')]
                
                if not self.zip_files:
                    raise ValueError(f"Nenhum arquivo começando com 'Empre' e terminando com .zip encontrado em '{path_zip}'")
                
                print(f"Encontrados {len(self.zip_files)} arquivos começando com 'Empre'")
            
            # 3. Se não for nenhum dos anteriores, tentar interpretar como padrão genérico
            else:
                padrao_limpo = arquivo_zip_especifico.strip('\'"') # Remover aspas
                # Tentativa de remover caracteres de escape comuns
                padrao_limpo = padrao_limpo.replace('`*', '*').replace('\\*', '*').replace('`?', '?').replace('\\?', '?') 
                
                print(f"Tentando encontrar arquivos que correspondem ao padrão: '{padrao_limpo}'")
                self.zip_files = [f for f in todos_arquivos if fnmatch.fnmatch(f, padrao_limpo) and f.endswith('.zip')]
                
                if not self.zip_files:
                    print(f"Aviso: Nenhum arquivo .zip correspondente ao padrão '{padrao_limpo}' encontrado.")
                    print("Usando todos os arquivos Empresa*.zip como fallback.")
                    self.zip_files = [f for f in todos_arquivos if f.startswith('Empresa') and f.endswith('.zip')]
                    
                    if not self.zip_files:
                        raise ValueError(f"Nenhum arquivo ZIP de Empresas encontrado em '{path_zip}' como fallback.")
                else:
                    print(f"Encontrados {len(self.zip_files)} arquivos correspondentes ao padrão '{padrao_limpo}'")
        else:
            # Comportamento padrão: todos os arquivos começando com 'Empresa' e terminando com '.zip'
            self.zip_files = [f for f in todos_arquivos if f.startswith('Empresa') and f.endswith('.zip')]
            print(f"Usando comportamento padrão: {len(self.zip_files)} arquivos que começam com 'Empresa'")
        
        if not self.zip_files:
            raise ValueError(f"Nenhum arquivo ZIP encontrado em '{path_zip}' com os critérios especificados.")
            
        logger.info(f"Encontrados {len(self.zip_files)} arquivos ZIP para benchmark: {', '.join(self.zip_files)}")
        
        # Inicializar resultados
        self.resultados = {
            'pandas': {
                'tempo_total': 0, 
                'tempo_extracao': 0,
                'tempo_processamento': 0,
                'memoria_pico': 0, 
                'memoria_media': 0,
                'cpu_medio': 0, 
                'cpu_pico': 0,
                'espaco_disco': 0,
                'num_arquivos': 0,
                'arquivos_parquet': [],
                'compressao_taxa': 0
            },
            'dask': {
                'tempo_total': 0, 
                'tempo_extracao': 0,
                'tempo_processamento': 0,
                'memoria_pico': 0, 
                'memoria_media': 0,
                'cpu_medio': 0, 
                'cpu_pico': 0,
                'espaco_disco': 0,
                'num_arquivos': 0,
                'arquivos_parquet': [],
                'compressao_taxa': 0
            },
            'polars': {
                'tempo_total': 0,
                'tempo_extracao': 0,
                'tempo_processamento': 0,
                'memoria_pico': 0,
                'memoria_media': 0,
                'cpu_pico': 0,
                'cpu_medio': 0,
                'espaco_disco': 0,
                'num_arquivos': 0,
                'arquivos_parquet': [],
                'compressao_taxa': 0
            }
        }
        
        # Informações do arquivo original
        self.info_arquivo = self._coletar_info_arquivo()
    
    def _coletar_info_arquivo(self):
        """Coleta informações sobre o arquivo ZIP original."""
        info = {}
        
        for zip_file in self.zip_files:
            zip_path = os.path.join(self.path_zip, zip_file)
            if os.path.exists(zip_path):
                # Informações básicas do arquivo ZIP
                info[zip_file] = {
                    'tamanho_zip_mb': os.path.getsize(zip_path) / (1024 * 1024),
                    'data_modificacao': datetime.fromtimestamp(os.path.getmtime(zip_path)).strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Verificar conteúdo do ZIP
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        arquivos = zip_ref.namelist()
                        csv_files = [f for f in arquivos if f.endswith('.CSV')]
                        info[zip_file]['num_arquivos'] = len(arquivos)
                        info[zip_file]['num_csv'] = len(csv_files)
                        
                        # Extrair arquivos para calcular tamanho real
                        temp_dir = os.path.join(self.path_unzip_temp, os.path.splitext(zip_file)[0])
                        if os.path.exists(temp_dir):
                            shutil.rmtree(temp_dir)
                        os.makedirs(temp_dir, exist_ok=True)
                        
                        print(f"Extraindo {zip_file} para calcular tamanho real dos arquivos...")
                        zip_ref.extractall(temp_dir)
                        
                        # Calcular tamanho total dos arquivos extraídos
                        tamanho_real_mb = self.calcular_tamanho_diretorio(temp_dir)
                        info[zip_file]['tamanho_real_mb'] = tamanho_real_mb
                        
                        # Limpar diretório temporário após medição
                        shutil.rmtree(temp_dir)
                        
                except Exception as e:
                    logger.error(f"Erro ao analisar ZIP {zip_file}: {str(e)}")
                    info[zip_file]['tamanho_real_mb'] = info[zip_file]['tamanho_zip_mb']  # Fallback para tamanho do ZIP
                    
        return info

    def limpar_diretorios(self, preservar_parquet=True, limpar_docs_logs=False):
        """Limpa os diretórios temporários.
        
        Args:
            preservar_parquet: Se True, não limpa os diretórios de parquet.
            limpar_docs_logs: Se True, limpa também os diretórios docs e logs.
        """
        if not self.executar_limpeza:
            return
            
        # Sempre limpar os diretórios de extração temporária
        diretorios_extracao = [self.path_unzip_pandas, self.path_unzip_dask, self.path_unzip_polars]
        for path in diretorios_extracao:
            if os.path.exists(path):
                print(f"Limpando diretório: {path}")
                for item in os.listdir(path):
                    item_path = os.path.join(path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
        
        # Limpar diretórios de parquet apenas se não estiver preservando
        if not preservar_parquet:
            diretorios_parquet = [self.path_parquet_pandas, self.path_parquet_dask, self.path_parquet_polars]
            for path in diretorios_parquet:
                if os.path.exists(path):
                    print(f"Limpando diretório: {path}")
                    for item in os.listdir(path):
                        item_path = os.path.join(path, item)
                        if os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                        else:
                            os.remove(item_path)
        
        # Limpar diretórios docs e logs se solicitado
        if limpar_docs_logs:
            docs_dir = os.path.join(self.path_base, "docs")
            logs_dir = os.path.join(self.path_base, "logs")
            
            for path in [docs_dir, logs_dir]:
                if os.path.exists(path):
                    print(f"Limpando diretório: {path}")
                    for item in os.listdir(path):
                        item_path = os.path.join(path, item)
                        if os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                        else:
                            os.remove(item_path)
            
        logger.debug("Diretórios temporários limpos" + (" (preservando parquet)" if preservar_parquet else ""))
    
    def calcular_tamanho_diretorio(self, path):
        """Calcula o tamanho total de um diretório em MB."""
        if not os.path.exists(path):
            return 0
            
        total_size = 0
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if os.path.exists(fp):  # Verificar se o arquivo existe
                    total_size += os.path.getsize(fp)
        return total_size / (1024 * 1024)  # Converter para MB
    
    def contar_arquivos(self, path):
        """Conta o número de arquivos em um diretório e subdiretorios."""
        if not os.path.exists(path):
            return 0
            
        count = 0
        for dirpath, _, filenames in os.walk(path):
            count += len(filenames)
        return count
    
    def listar_arquivos_parquet(self, path):
        """Lista os arquivos parquet em um diretório."""
        if not os.path.exists(path):
            return []
            
        parquet_files = []
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                if f.endswith('.parquet'):
                    parquet_files.append(os.path.join(dirpath, f))
        return parquet_files
    
    def calcular_taxa_compressao(self, tamanho_original, tamanho_comprimido):
        """Calcula a taxa de compressão."""
        if tamanho_original == 0:
            return 0
        return (1 - (tamanho_comprimido / tamanho_original)) * 100
    
    def formatar_tempo(self, segundos):
        """Formata o tempo em horas, minutos e segundos quando superior a 60s."""
        if segundos < 60:
            return f"{segundos:.2f} segundos"
        
        # Converter para horas, minutos e segundos
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        segundos_restantes = segundos % 60
        
        # Montar a string formatada
        partes = []
        if horas > 0:
            partes.append(f"{horas}h")
        if minutos > 0 or horas > 0:  # Se tiver horas, mostra minutos mesmo com zero
            partes.append(f"{minutos}min")
        partes.append(f"{segundos_restantes:.2f}s")
        
        return " ".join(partes)

    def processar_arquivo_zip(self, zip_file):
        """
        Processa um único arquivo ZIP com Pandas.
        Esta função é projetada para ser chamada em paralelo 
        através de ProcessPoolExecutor.
        
        Args:
            zip_file: Nome do arquivo ZIP a ser processado
            
        Returns:
            dict: Dicionário com informações do resultado
        """
        try:
            local_inicio = time.time()
            print(f"[Worker] Iniciando processamento de {zip_file}")
            
            # Capturar métricas locais
            cpu_local = psutil.cpu_percent(interval=0.1)
            memoria_local = psutil.virtual_memory().percent
            
            # Chamar a função de processamento
            resultado = process_single_zip_pandas(
                zip_file=zip_file, 
                path_zip=self.path_zip, 
                path_unzip=self.path_unzip_pandas, 
                path_parquet=self.path_parquet_pandas
            )
            
            # Calcular tempo de processamento
            tempo_processamento = time.time() - local_inicio
            
            # Incluir informações de CPU e memória juntamente com o resultado
            info_resultado = {
                'arquivo': zip_file,
                'sucesso': resultado,
                'tempo': tempo_processamento,
                'cpu': cpu_local,
                'memoria': memoria_local
            }
            
            if resultado:
                print(f"[Worker] {zip_file} processado com sucesso em {tempo_processamento:.2f}s")
                logger.info(f"{zip_file}: processado em {tempo_processamento:.2f}s")
            else:
                print(f"[Worker] {zip_file} falhou após {tempo_processamento:.2f}s")
                logger.warning(f"{zip_file}: falha no processamento")
            
            return info_resultado
            
        except Exception as e:
            print(f"[Worker] Erro ao processar {zip_file}: {str(e)}")
            logger.error(f"Erro ao processar {zip_file}: {str(e)}")
            logger.debug(traceback.format_exc())
            
            # Retornar informação de falha
            return {
                'arquivo': zip_file,
                'sucesso': False,
                'tempo': 0,
                'erro': str(e),
                'cpu': 0,
                'memoria': 0
            }
    
    def executar_benchmark_pandas(self, max_workers=None):
        """
        Executa o benchmark usando Pandas com processamento paralelo.
        
        Args:
            max_workers: Número máximo de workers para processamento paralelo. 
                         Se None, será usado o número de cores disponíveis.
        """
        logger.info("\nBENCHMARK COM PANDAS (PROCESSAMENTO PARALELO)")
        
        # Medir tempo de início do benchmark completo (incluindo preparação)
        tempo_benchmark_inicio = time.time()
        
        # Limpar diretórios (não preservar parquet antes de executar o benchmark)
        self.limpar_diretorios(preservar_parquet=False)
        
        # Se max_workers não foi especificado, usar o número de cores do sistema
        if max_workers is None:
            max_workers = os.cpu_count()
            # Limitar a número razoável para evitar sobrecarga
            max_workers = min(max_workers, len(self.zip_files), 8)
        
        logger.info(f"Usando {max_workers} workers para processamento paralelo")
        print(f"\n[Pandas] Iniciando processamento paralelo com {max_workers} workers")
        
        # Lista para armazenar métricas de CPU e memória durante o processamento
        cpu_medidas = []
        memoria_medidas = []
        
        # Medir tempo de processamento efetivo (apenas a parte paralela)
        tempo_inicio_processamento = time.time()
        
        # Iniciar medição periódica de CPU e memória durante todo o processo
        def monitor_recursos():
            while monitorar_recursos[0]:
                cpu_medidas.append(psutil.cpu_percent(interval=0.5))
                memoria_medidas.append(psutil.virtual_memory().percent)
                time.sleep(0.5)
                
        # Flag para controlar o monitoramento
        monitorar_recursos = [True]
        
        # Iniciar monitoramento em uma thread separada
        monitor_thread = threading.Thread(target=monitor_recursos)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        resultados_paralelos = []
        sucessos = 0
        tempo_processamento_total = 0
        
        log_file = None
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                log_file = handler.baseFilename
                break
        if log_file is None:
            logger.warning("Não foi possível encontrar o FileHandler principal para passar aos workers.")
            # Considerar definir um caminho padrão ou falhar
        
        try:
            # Criar uma referência ao método de processamento
            process_func = self.processar_arquivo_zip
            
            # Executar o processamento em paralelo
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                # Submeter todas as tarefas, passando o caminho do log
                future_to_zip = {}
                for zip_file in self.zip_files:
                    # Passar log_file como argumento para a função do worker
                    future = executor.submit(process_func, zip_file)
                    future_to_zip[future] = zip_file
                
                # Processar resultados conforme são concluídos
                for future in concurrent.futures.as_completed(future_to_zip):
                    zip_file = future_to_zip[future]
                    try:
                        # info_resultado agora vem da função do worker que retorna um dict
                        # (não precisamos do resultado direto aqui, apenas do status sucesso/falha implícito)
                        # A função processar_arquivo_zip retorna True/False
                        sucesso_worker = future.result() 
                        # Precisamos reavaliar como contamos sucessos e tempo
                        # Se a função worker retorna True/False, precisamos pegar o tempo de outra forma
                        # Ou modificar a função worker para retornar um dict como antes
                        
                        # *** REVERTENDO: Vamos manter a função worker retornando o dict ***
                        # *** E adicionar a configuração do log dentro dela ***
                        info_resultado = future.result() # Espera dict: {'sucesso': bool, 'tempo': float, ...}
                        resultados_paralelos.append(info_resultado)
                        
                        if info_resultado.get('sucesso', False): # Usar .get para segurança
                            sucessos += 1
                            tempo_processamento_total += info_resultado.get('tempo', 0)
                        else:
                            logger.warning(f"Worker para {zip_file} reportou falha ou não retornou sucesso.")
                            
                    except Exception as e:
                        print(f"[Pandas] {zip_file} gerou exceção ao obter resultado: {str(e)}")
                        logger.error(f"Exceção ao obter resultado de {zip_file}: {str(e)}")
                        logger.debug(traceback.format_exc())
        
        finally:
            # Calcular tempo de processamento paralelo
            tempo_processamento_paralelo = time.time() - tempo_inicio_processamento
            
            # Parar o monitoramento
            monitorar_recursos[0] = False
            monitor_thread.join(timeout=1.0)
        
        # Calcular o tempo total do benchmark (incluindo preparação e finalização)
        tempo_benchmark_total = time.time() - tempo_benchmark_inicio
        
        # Coletar resultados
        self.resultados['pandas']['tempo_total'] = tempo_benchmark_total
        self.resultados['pandas']['tempo_extracao'] = 0  # Não temos essa informação separada agora
        self.resultados['pandas']['tempo_processamento'] = tempo_processamento_paralelo
        self.resultados['pandas']['tempo_worker_total'] = tempo_processamento_total
        self.resultados['pandas']['memoria_pico'] = max(memoria_medidas) if memoria_medidas else 0
        self.resultados['pandas']['memoria_media'] = sum(memoria_medidas) / len(memoria_medidas) if memoria_medidas else 0
        self.resultados['pandas']['cpu_medio'] = sum(cpu_medidas) / len(cpu_medidas) if cpu_medidas else 0
        self.resultados['pandas']['cpu_pico'] = max(cpu_medidas) if cpu_medidas else 0
        self.resultados['pandas']['espaco_disco'] = self.calcular_tamanho_diretorio(self.path_parquet_pandas)
        self.resultados['pandas']['num_arquivos'] = self.contar_arquivos(self.path_parquet_pandas)
        self.resultados['pandas']['arquivos_parquet'] = self.listar_arquivos_parquet(self.path_parquet_pandas)
        
        # Mostrar caminhos onde os arquivos estão salvos
        parquet_dir = os.path.join(self.path_parquet_pandas, 'empresas')
        print(f"\n[Pandas] Arquivos parquet salvos em: {os.path.abspath(parquet_dir)}")
        
        if self.resultados['pandas']['num_arquivos'] > 0:
            print(f"[Pandas] {self.resultados['pandas']['num_arquivos']} arquivos parquet gerados")
            exemplo_arquivo = self.resultados['pandas']['arquivos_parquet'][0] if self.resultados['pandas']['arquivos_parquet'] else "Nenhum arquivo encontrado"
            print(f"[Pandas] Exemplo de arquivo: {exemplo_arquivo}")
        else:
            print("[Pandas] ATENÇÃO: Nenhum arquivo parquet foi gerado!")
        
        # Calcular taxa de compressão usando o tamanho real dos arquivos extraídos
        tamanho_original = sum([info['tamanho_real_mb'] for info in self.info_arquivo.values()])
        if tamanho_original > 0 and self.resultados['pandas']['espaco_disco'] > 0:
            self.resultados['pandas']['compressao_taxa'] = self.calcular_taxa_compressao(
                tamanho_original, self.resultados['pandas']['espaco_disco'])
        else:
            self.resultados['pandas']['compressao_taxa'] = 0
            logger.warning("Não foi possível calcular taxa de compressão")
        
        # Resumo do processamento paralelo
        print(f"\n[Pandas] Benchmark paralelo concluído em {tempo_benchmark_total:.2f} segundos")
        print(f"[Pandas] {sucessos} de {len(self.zip_files)} arquivos processados com sucesso")
        if sucessos > 0:
            velocidade = sucessos / tempo_benchmark_total
            print(f"[Pandas] Velocidade média: {velocidade:.2f} arquivos/segundo")
        
        logger.info(f"Benchmark com Pandas concluído em {tempo_benchmark_total:.2f} segundos")
        logger.info(f"Arquivos processados: {sucessos}/{len(self.zip_files)}")
        
        # Forçar limpeza de memória
        gc.collect()
        
        return self.resultados['pandas']

    def executar_benchmark_dask(self):
        """
        Executa o benchmark usando Dask.
        
        Returns:
            Dict com os resultados do benchmark
        """
        # Garantir que o diretório de destino existe
        os.makedirs(self.path_parquet_dask, exist_ok=True)
        
        logger.info('\nBENCHMARK COM DASK')
        
        # Listas para armazenar medidas de desempenho
        cpu_medidas = []
        memoria_medidas = []
        
        # Medir o tempo total de execução
        tempo_inicio_total = time.time()
        
        # Armazenar tempos parciais
        tempo_extracao = 0
        tempo_processamento = 0
        
        # Inicializar o cliente Dask
        try:
            print(f"\n[Dask] Inicializando cliente Dask... ", end="", flush=True)
            dask_manager = DaskManager.initialize(
                n_workers=config.dask.n_workers,
                memory_limit=config.dask.memory_limit,
                dashboard_address=config.dask.dashboard_address
            )
            client = dask_manager.client
            print(f"✓ Cliente Dask inicializado com {config.dask.n_workers} workers")
            logger.info(f"Cliente Dask inicializado")
        except Exception as e:
            print(f"✗ Erro ao inicializar cliente Dask: {str(e)}")
            logger.error(f"Erro ao inicializar cliente Dask: {str(e)}")
            logger.debug(traceback.format_exc())
            self.resultados['dask']['tempo_total'] = time.time() - tempo_inicio_total
            return self.resultados['dask']
        
        try:
            sucessos = 0
            total_arquivos = len(self.zip_files)
            
            # Executar o processamento com o método Dask para cada arquivo
            for i, zip_file in enumerate(self.zip_files):
                logger.info(f"Processando {zip_file} com Dask ({i+1}/{total_arquivos})...")
                print(f"\n[Dask] Processando arquivo: {zip_file} ({i+1}/{total_arquivos})")
                
                # Abordagem simplificada: processar cada arquivo diretamente
                for tentativa in range(3):  # Até 3 tentativas por arquivo
                    if tentativa > 0:
                        print(f"  → Tentativa {tentativa+1} para {zip_file}")
                        logger.info(f"Tentativa {tentativa+1} para {zip_file}")
                    
                    try:
                        # Medir CPU e memória durante o processamento
                        cpu_medidas.append(psutil.cpu_percent(interval=0.1))
                        memoria_medidas.append(psutil.virtual_memory().percent)
                        
                        # Processar diretamente com Dask, sem client.submit
                        tempo_inicio_processamento = time.time()
                        
                        # Status de processamento
                        print(f"Status: Extraindo e processando... ", end="", flush=True)
                        
                        # Chamar a função diretamente
                        resultado = process_single_zip(
                            zip_file=zip_file,
                            path_zip=self.path_zip, 
                            path_unzip=self.path_unzip_dask, 
                            path_parquet=self.path_parquet_dask
                        )
                        
                        # Verificar se o resultado é um objeto Delayed
                        if hasattr(resultado, 'compute') and callable(getattr(resultado, 'compute')):
                            print("Computando resultado... ", end="", flush=True)
                            logger.debug(f"Computando objeto Delayed para {zip_file}...")
                            try:
                                # Computar o objeto Delayed
                                resultado = resultado.compute()
                            except Exception as e:
                                print(f"✗ Erro: {str(e)}")
                                logger.error(f"Erro ao computar resultado: {str(e)}")
                                logger.debug(traceback.format_exc())
                                resultado = False
                        
                        # Calcular tempo de processamento
                        tempo_processamento_arquivo = time.time() - tempo_inicio_processamento
                        tempo_processamento += tempo_processamento_arquivo
                        
                        logger.debug(f"Arquivo processado em {tempo_processamento_arquivo:.2f}s, resultado: {resultado}")
                        
                        if resultado is True:
                            print(f"✓ Concluído em {tempo_processamento_arquivo:.2f}s")
                            logger.info(f"{zip_file}: processado em {tempo_processamento_arquivo:.2f}s")
                            sucessos += 1
                            break  # Saia do loop de tentativas
                        else:
                            print(f"✗ Falha após {tempo_processamento_arquivo:.2f}s")
                            logger.warning(f"{zip_file}: falha no processamento")
                                
                    except Exception as e:
                        print(f"✗ Erro: {str(e)}")
                        logger.error(f"Erro: {str(e)}")
                        logger.debug(traceback.format_exc())
                    
                    # Esperar brevemente antes de tentar novamente
                    if tentativa < 2:  # Não mostrar após a última tentativa
                        print("  → Aguardando para nova tentativa...")
                        time.sleep(2)
                
                # Forçar uma limpeza de memória entre arquivos
                gc.collect()
            
            # Resultado final
            print(f"\n[Dask] Concluído: {sucessos} de {total_arquivos} arquivos processados com sucesso")
            logger.info(f"Concluído: {sucessos} de {total_arquivos} arquivos processados")
        
        except Exception as e:
            print(f"✗ Erro no processamento: {str(e)}")
            logger.error(f"Erro no processamento: {str(e)}")
            logger.debug(traceback.format_exc())
        finally:
            # Calcular o tempo total de execução
            tempo_total = time.time() - tempo_inicio_total
            
            # Atualizar tempos parciais nos resultados
            self.resultados['dask']['tempo_total'] = tempo_total
            self.resultados['dask']['tempo_extracao'] = tempo_extracao
            self.resultados['dask']['tempo_processamento'] = tempo_processamento
            
            # Encerrar o cliente Dask
            try:
                print(f"[Dask] Encerrando cliente Dask... ", end="", flush=True)
                dask_manager.shutdown()
                print("✓ Cliente encerrado")
                logger.debug("Cliente Dask encerrado")
            except Exception as e:
                print(f"✗ Erro ao encerrar Dask: {str(e)}")
                logger.error(f"Erro ao encerrar Dask: {str(e)}")
        
        # Coletar resultados
        self.resultados['dask']['memoria_pico'] = max(memoria_medidas) if memoria_medidas else 0
        self.resultados['dask']['memoria_media'] = sum(memoria_medidas) / len(memoria_medidas) if memoria_medidas else 0
        self.resultados['dask']['cpu_medio'] = sum(cpu_medidas) / len(cpu_medidas) if cpu_medidas else 0
        self.resultados['dask']['cpu_pico'] = max(cpu_medidas) if cpu_medidas else 0
        self.resultados['dask']['espaco_disco'] = self.calcular_tamanho_diretorio(self.path_parquet_dask)
        self.resultados['dask']['num_arquivos'] = self.contar_arquivos(self.path_parquet_dask)
        self.resultados['dask']['arquivos_parquet'] = self.listar_arquivos_parquet(self.path_parquet_dask)
        
        # Calcular taxa de compressão usando o tamanho real dos arquivos extraídos
        tamanho_original = sum([info['tamanho_real_mb'] for info in self.info_arquivo.values()])
        if tamanho_original > 0 and self.resultados['dask']['espaco_disco'] > 0:
            self.resultados['dask']['compressao_taxa'] = self.calcular_taxa_compressao(
                tamanho_original, self.resultados['dask']['espaco_disco'])
        else:
            self.resultados['dask']['compressao_taxa'] = 0
            logger.warning("Não foi possível calcular taxa de compressão")
        
        print(f"\n[Dask] Benchmark concluído em {tempo_total:.2f} segundos")
        logger.info(f"Benchmark com Dask concluído em {tempo_total:.2f} segundos")
        
        # Forçar limpeza de memória
        gc.collect()
        
        return self.resultados['dask']

    def executar_benchmark_polars(self):
        """Executa o benchmark usando Polars."""
        logger.info("\nBENCHMARK COM POLARS")
        
        # Limpar diretórios (não preservar parquet antes de executar o benchmark)
        self.limpar_diretorios(preservar_parquet=False)
        
        # Medir uso de CPU e memória
        cpu_medidas = []
        memoria_medidas = []
        
        # Medir tempo total de execução
        tempo_inicio_total = time.time()
        
        # Armazenar tempos parciais
        tempo_extracao = 0
        tempo_processamento = 0
        
        # Executar o processamento para cada arquivo ZIP
        for zip_file in self.zip_files:
            logger.info(f"Processando {zip_file} com Polars...")
            
            # Mostrar progresso
            print(f"\n[Polars] Processando arquivo: {zip_file}")
            print("Status: Extraindo e processando CSV... ", end="", flush=True)
            
            try:
                # Medir CPU e memória durante o processamento
                cpu_medidas.append(psutil.cpu_percent(interval=0.1))
                memoria_medidas.append(psutil.virtual_memory().percent)
                
                # Medir o tempo de processamento
                tempo_arquivo_inicio = time.time()
                
                # Usar a função process_single_zip_polars
                resultado = process_single_zip_polars(
                    zip_file=zip_file, 
                    path_zip=self.path_zip, 
                    path_unzip=self.path_unzip_polars, 
                    path_parquet=self.path_parquet_polars
                )
                
                # Atualizar tempo de processamento
                tempo_arquivo_total = time.time() - tempo_arquivo_inicio
                
                # Verificar resultado
                if resultado:
                    print("✓ Concluído!")
                    logger.info(f"Arquivo {zip_file} processado com sucesso usando Polars em {tempo_arquivo_total:.2f} segundos")
                else:
                    print("✗ Falha!")
                    logger.warning(f"Falha ao processar {zip_file} com Polars após {tempo_arquivo_total:.2f} segundos")
                    
            except Exception as e:
                print("✗ Erro!")
                logger.error(f"Erro ao processar {zip_file} com Polars: {str(e)}")
                logger.debug(traceback.format_exc())
        
        # Calcular tempo total
        tempo_total = time.time() - tempo_inicio_total
        
        # Calcular uso de CPU e memória
        cpu_medio = np.mean(cpu_medidas) if cpu_medidas else 0
        cpu_pico = np.max(cpu_medidas) if cpu_medidas else 0
        memoria_media = np.mean(memoria_medidas) if memoria_medidas else 0
        memoria_pico = np.max(memoria_medidas) if memoria_medidas else 0
        
        # Verificar existência dos arquivos parquet
        parquet_dir = os.path.join(self.path_parquet_polars, 'empresas')
        if os.path.exists(parquet_dir):
            parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith('.parquet')]
            num_arquivos = len(parquet_files)
        else:
            parquet_files = []
            num_arquivos = 0
        
        # Calcular espaço em disco dos arquivos parquet
        espaco_disco = sum(os.path.getsize(os.path.join(parquet_dir, f)) for f in parquet_files) / (1024 * 1024) if parquet_files else 0
        
        # Calcular taxa de compressão usando o tamanho real dos arquivos extraídos
        if self.info_arquivo and espaco_disco > 0:
            tamanho_original = sum(info['tamanho_real_mb'] for info in self.info_arquivo.values())
            compressao_taxa = 100 * (1 - (espaco_disco / tamanho_original)) if tamanho_original > 0 else 0
        else:
            compressao_taxa = 0
        
        # Guardar resultados
        self.resultados['polars'] = {
            'tempo_total': tempo_total,
            'tempo_extracao': tempo_extracao,
            'tempo_processamento': tempo_processamento,
            'memoria_pico': memoria_pico,
            'memoria_media': memoria_media,
            'cpu_pico': cpu_pico,
            'cpu_medio': cpu_medio,
            'espaco_disco': espaco_disco,
            'num_arquivos': num_arquivos,
            'arquivos_parquet': parquet_files,
            'compressao_taxa': compressao_taxa
        }
        
        logger.info(f"Benchmark com Polars concluído em {tempo_total:.2f} segundos")
        
        # Forçar limpeza de memória
        gc.collect()
        
        return self.resultados['polars']

    def comparar_resultados(self):
        """Compara os resultados dos benchmarks e retorna o melhor método."""
        pandas_results = self.resultados['pandas']
        dask_results = self.resultados['dask']
        polars_results = self.resultados['polars']
        
        # Verificar se os métodos geraram resultados válidos (arquivos parquet)
        pandas_valido = pandas_results['num_arquivos'] > 0
        dask_valido = dask_results['num_arquivos'] > 0
        polars_valido = polars_results['num_arquivos'] > 0
        
        # Contar quantos métodos geraram resultados válidos
        metodos_validos = [('pandas', pandas_valido), ('dask', dask_valido), ('polars', polars_valido)]
        validos = [m for m, v in metodos_validos if v]
        
        # Se apenas um método gerou resultados válidos, ele é automaticamente o melhor
        if len(validos) == 1:
            melhor = validos[0]
            return {
                'comparacao': {'processamento_sucesso': {'melhor': melhor, 'diferenca_percentual': 100}},
                'contagem': {m: 1 if m == melhor else 0 for m, _ in metodos_validos},
                'melhor_metodo': melhor
            }
        elif len(validos) == 0:
            return {
                'comparacao': {'sem_dados': {'melhor': 'indeterminado', 'diferenca_percentual': 0}},
                'contagem': {m: 0 for m, _ in metodos_validos},
                'melhor_metodo': 'indeterminado'
            }
        
        comparacao = {}
        
        # Função auxiliar para evitar divisão por zero
        def calcular_diferenca_percentual(valor1, valor2):
            if valor1 == 0 and valor2 == 0:
                return 0
            if valor1 == 0:
                return 100  # Diferença de 100% se valor1 for zero
            if valor2 == 0:
                return 100  # Diferença de 100% se valor2 for zero
            maximo = max(valor1, valor2)
            return abs(valor1 - valor2) / maximo * 100
        
        # Lista de métricas para comparação
        metricas_para_comparar = [
            'tempo_total', 
            'memoria_pico', 
            'cpu_medio', 
            'espaco_disco',
            'compressao_taxa'
        ]
        
        # Compara cada métrica apenas se existir nos resultados
        for metrica in metricas_para_comparar:
            # Verificar se a métrica existe em todos os resultados válidos
            valores = {}
            if pandas_valido and metrica in pandas_results:
                valores['pandas'] = pandas_results[metrica]
            if dask_valido and metrica in dask_results:
                valores['dask'] = dask_results[metrica]
            if polars_valido and metrica in polars_results:
                valores['polars'] = polars_results[metrica]
            
            if not valores:
                continue
            
            # Determinar qual é melhor (menor valor é melhor, exceto para taxa de compressão)
            if metrica == 'compressao_taxa':
                # Para taxa de compressão, maior é melhor
                melhor = max(valores.items(), key=lambda x: x[1])[0]
            else:
                # Para outras métricas, menor é melhor
                melhor = min(valores.items(), key=lambda x: x[1])[0]
                
                # Nunca considerar um valor zero como melhor quando outro tem resultado válido
                if valores[melhor] == 0:
                    valores_sem_zero = {k: v for k, v in valores.items() if v > 0}
                    if valores_sem_zero:
                        melhor = min(valores_sem_zero.items(), key=lambda x: x[1])[0]
            
            # Calcular diferença percentual em relação ao segundo melhor
            valores_ordenados = sorted(valores.items(), key=lambda x: x[1] if metrica != 'compressao_taxa' else -x[1])
            if len(valores_ordenados) > 1:
                melhor_valor = valores_ordenados[0][1]
                segundo_valor = valores_ordenados[1][1]
                
                if melhor_valor == 0 and segundo_valor == 0:
                    diferenca = 0
                elif melhor_valor == 0:
                    diferenca = 100
                elif segundo_valor == 0:
                    diferenca = 100
                else:
                    maximo = max(melhor_valor, segundo_valor)
                    diferenca = abs(melhor_valor - segundo_valor) / maximo * 100
            else:
                diferenca = 100  # Se só há um método, a diferença é 100%
            
            # Adicionar à comparação
            comparacao[metrica] = {
                'melhor': melhor,
                'diferenca_percentual': diferenca
            }
        
        # Se não houver critérios para comparação, retorne um resultado padrão
        if not comparacao:
            return {
                'comparacao': {'sem_dados': {'melhor': 'indeterminado', 'diferenca_percentual': 0}},
                'contagem': {'pandas': 0, 'dask': 0, 'polars': 0},
                'melhor_metodo': 'indeterminado'
            }
        
        # Contar qual método ganhou em mais critérios
        contagem = {'pandas': 0, 'dask': 0, 'polars': 0}
        for criterio, resultado in comparacao.items():
            contagem[resultado['melhor']] += 1
        
        # Determinar o melhor método
        melhor_metodo = max(contagem.items(), key=lambda x: x[1])[0]
        
        return {
            'comparacao': comparacao,
            'contagem': contagem,
            'melhor_metodo': melhor_metodo
        }
    
    def gerar_graficos(self):
        """Gera gráficos comparativos dos resultados."""
        try:
            # Criar diretório para gráficos se não existir
            docs_dir = os.path.join(self.path_base, 'docs')
            os.makedirs(docs_dir, exist_ok=True)
            
            # Gráfico de comparação principal
            plt.figure(figsize=(15, 10))
            
            # Dados para os gráficos
            metricas = ['tempo_total', 'memoria_pico', 'cpu_medio', 'espaco_disco']
            valores_pandas = [self.resultados['pandas'][m] for m in metricas]
            valores_dask = [self.resultados['dask'][m] for m in metricas]
            valores_polars = [self.resultados['polars'][m] for m in metricas]
            
            # Criar subplots
            for i, metrica in enumerate(metricas):
                plt.subplot(2, 2, i+1)
                barras = plt.bar(['Pandas', 'Dask', 'Polars'], [self.resultados['pandas'][metrica], self.resultados['dask'][metrica], self.resultados['polars'][metrica]], 
                                color=['#1f77b4', '#ff7f0e', '#2ca02c'])  # Cores azul, laranja e verde
                plt.title(f'{metrica.replace("_", " ").title()}')
                plt.ylabel('Valor')
                
                # Adicionar valores nas barras
                for barra in barras:
                    altura = barra.get_height()
                    plt.text(barra.get_x() + barra.get_width()/2., altura,
                            f'{altura:.2f}',
                            ha='center', va='bottom')
            
            plt.tight_layout()
            grafico_path = os.path.join(docs_dir, 'benchmark_comparacao.png')
            plt.savefig(grafico_path)
            logger.info(f"Gráfico comparativo salvo: {grafico_path}")
            
            # Verificar se o arquivo foi realmente criado
            graficos_criados = {
                'comparacao': os.path.exists(grafico_path)
            }
            
            # Gráfico de tempo detalhado para Pandas
            if self.resultados['pandas']['tempo_extracao'] > 0:
                try:
                    plt.figure(figsize=(10, 6))
                    tempos = ['tempo_extracao', 'tempo_processamento']
                    valores = [self.resultados['pandas'][t] for t in tempos]
                    plt.pie(valores, labels=[t.replace('tempo_', '').title() for t in tempos], 
                            autopct='%1.1f%%', colors=['#2ca02c'])  # Verde
                    plt.title('Distribuição do Tempo (Pandas)')
                    grafico_tempo_path = os.path.join(docs_dir, 'benchmark_tempo_pandas.png')
                    plt.savefig(grafico_tempo_path)
                    graficos_criados['tempo_pandas'] = os.path.exists(grafico_tempo_path)
                    logger.info(f"Gráfico tempo Pandas salvo")
                except Exception as e:
                    logger.error(f"Erro ao criar gráfico de tempo Pandas: {str(e)}")
                    graficos_criados['tempo_pandas'] = False
            
            # Gráfico de tempo detalhado para Dask
            if self.resultados['dask']['tempo_extracao'] > 0:
                try:
                    plt.figure(figsize=(10, 6))
                    tempos = ['tempo_extracao', 'tempo_processamento']
                    valores = [self.resultados['dask'][t] for t in tempos]
                    plt.pie(valores, labels=[t.replace('tempo_', '').title() for t in tempos], 
                            autopct='%1.1f%%', colors=['#ff7f0e'])  # Laranja
                    plt.title('Distribuição do Tempo (Dask)')
                    grafico_tempo_path = os.path.join(docs_dir, 'benchmark_tempo_dask.png')
                    plt.savefig(grafico_tempo_path)
                    graficos_criados['tempo_dask'] = os.path.exists(grafico_tempo_path)
                    logger.info(f"Gráfico tempo Dask salvo")
                except Exception as e:
                    logger.error(f"Erro ao criar gráfico de tempo Dask: {str(e)}")
                    graficos_criados['tempo_dask'] = False
            
            # Gráfico de tempo detalhado para Polars
            if self.resultados['polars']['tempo_extracao'] > 0:
                try:
                    plt.figure(figsize=(10, 6))
                    tempos = ['tempo_extracao', 'tempo_processamento']
                    valores = [self.resultados['polars'][t] for t in tempos]
                    plt.pie(valores, labels=[t.replace('tempo_', '').title() for t in tempos], 
                            autopct='%1.1f%%', colors=['#2ca02c'])  # Verde
                    plt.title('Distribuição do Tempo (Polars)')
                    grafico_tempo_path = os.path.join(docs_dir, 'benchmark_tempo_polars.png')
                    plt.savefig(grafico_tempo_path)
                    graficos_criados['tempo_polars'] = os.path.exists(grafico_tempo_path)
                    logger.info(f"Gráfico tempo Polars salvo")
                except Exception as e:
                    logger.error(f"Erro ao criar gráfico de tempo Polars: {str(e)}")
                    graficos_criados['tempo_polars'] = False
            
            return graficos_criados
        except Exception as e:
            logger.error(f"Erro na geração de gráficos: {str(e)}")
            logger.debug(traceback.format_exc())
            return {}

    def imprimir_relatorio(self):
        """Imprime um relatório resumido com os resultados do benchmark."""
        comparacao = self.comparar_resultados()
        
        print("\n" + "="*60)
        print(" "*20 + "RELATÓRIO DE BENCHMARK")
        print("="*60)
        
        # Informações dos arquivos originais
        print("\nARQUIVOS ORIGINAIS:")
        for zip_file, info in self.info_arquivo.items():
            print(f"  - {zip_file}: {info['tamanho_zip_mb']:.1f} MB (ZIP), {info['tamanho_real_mb']:.1f} MB (extraído), {info['num_csv']} CSV")
        
        # Resultados do Pandas
        print("\nRESULTADOS PANDAS:")
        print(f"  - Tempo Total: {self.formatar_tempo(self.resultados['pandas']['tempo_total'])}")
        print(f"  - Memória: {self.resultados['pandas']['memoria_pico']:.1f}% (pico)")
        print(f"  - CPU: {self.resultados['pandas']['cpu_medio']:.1f}% (média)")
        print(f"  - Espaço: {self.resultados['pandas']['espaco_disco']:.1f} MB")
        print(f"  - Compressão: {self.resultados['pandas']['compressao_taxa']:.1f}%")
        
        # Resultados do Dask
        print("\nRESULTADOS DASK:")
        print(f"  - Tempo Total: {self.formatar_tempo(self.resultados['dask']['tempo_total'])}")
        print(f"  - Memória: {self.resultados['dask']['memoria_pico']:.1f}% (pico)")
        print(f"  - CPU: {self.resultados['dask']['cpu_medio']:.1f}% (média)")
        print(f"  - Espaço: {self.resultados['dask']['espaco_disco']:.1f} MB")
        print(f"  - Compressão: {self.resultados['dask']['compressao_taxa']:.1f}%")
        
        # Resultados do Polars
        print("\nRESULTADOS POLARS:")
        print(f"  - Tempo Total: {self.formatar_tempo(self.resultados['polars']['tempo_total'])}")
        print(f"  - Memória: {self.resultados['polars']['memoria_pico']:.1f}% (pico)")
        print(f"  - CPU: {self.resultados['polars']['cpu_medio']:.1f}% (média)")
        print(f"  - Espaço: {self.resultados['polars']['espaco_disco']:.1f} MB")
        print(f"  - Compressão: {self.resultados['polars']['compressao_taxa']:.1f}%")
        
        # Comparação resumida
        print("\nCOMPARAÇÃO:")
        melhor_metodo = comparacao['melhor_metodo'].upper()
        vitorias = comparacao['contagem'][comparacao['melhor_metodo']]
        total = len(comparacao['comparacao'])
        print(f"  - {melhor_metodo} é mais adequado ({vitorias}/{total} critérios)")
        
        print("="*60)

    def gerar_relatorio_markdown(self, timestamp):
        """Gera um relatório Markdown com os resultados do benchmark."""
        docs_dir = os.path.join(self.path_base, 'docs')
        os.makedirs(docs_dir, exist_ok=True)
        
        # Caminho para o arquivo de relatório
        md_path = os.path.join(docs_dir, f"relatorio_completo_{timestamp}.md")
        
        relatorio = f"# Relatório de Benchmark - {timestamp}\n\n"
        relatorio += "## Resultados\n\n"
        
        # Adicionar informações do sistema
        info_sistema = InfoSistema.coletar_informacoes()
        relatorio += f"- **Sistema:** {info_sistema['sistema']} {info_sistema['versao_sistema']} ({info_sistema['arquitetura']})\n"
        relatorio += f"- **Processador:** {info_sistema['processador']}\n"
        relatorio += f"- **Cores:** {info_sistema['cores_fisicos']} físicos, {info_sistema['cores_logicos']} lógicos\n"
        relatorio += f"- **Frequência:** {info_sistema['frequencia_mhz']} MHz\n"
        relatorio += f"- **Memória:** {info_sistema['memoria_total']} (Disponível: {info_sistema['memoria_disponivel']})\n"
        relatorio += f"- **Disco:** {info_sistema['disco_total']} (Livre: {info_sistema['disco_livre']})\n"
        relatorio += f"- **GPU:** {info_sistema['gpu']}\n"
        relatorio += f"- **Memória GPU:** {info_sistema['memoria_gpu']}\n\n"
        
        # Adicionar informações sobre os arquivos originais
        relatorio += "## Arquivos Processados\n\n"
        relatorio += "| Arquivo | Tamanho ZIP (MB) | Tamanho Extraído (MB) | Número de Arquivos | Arquivos CSV |\n"
        relatorio += "|---------|-----------------|-------------------------|-------------------|-------------|\n"
        
        for zip_file, info in self.info_arquivo.items():
            relatorio += f"| {zip_file} | {info['tamanho_zip_mb']:.2f} | {info['tamanho_real_mb']:.2f} | {info['num_arquivos']} | {info['num_csv']} |\n"
        
        relatorio += "\n"
        
        # Adicionar resultados do Pandas
        relatorio += "### Resultados Pandas\n\n"
        relatorio += f"- **Tempo Total:** {self.formatar_tempo(self.resultados['pandas']['tempo_total'])}\n"
        relatorio += f"- **Tempo de Extração:** {self.formatar_tempo(self.resultados['pandas']['tempo_extracao'])}\n"
        relatorio += f"- **Tempo de Processamento:** {self.formatar_tempo(self.resultados['pandas']['tempo_processamento'])}\n"
        relatorio += f"- **Memória (pico):** {self.resultados['pandas']['memoria_pico']:.2f}%\n"
        relatorio += f"- **Memória (média):** {self.resultados['pandas']['memoria_media']:.2f}%\n"
        relatorio += f"- **CPU (pico):** {self.resultados['pandas']['cpu_pico']:.2f}%\n"
        relatorio += f"- **CPU (média):** {self.resultados['pandas']['cpu_medio']:.2f}%\n"
        relatorio += f"- **Espaço em Disco:** {self.resultados['pandas']['espaco_disco']:.2f} MB\n"
        relatorio += f"- **Taxa de Compressão:** {self.resultados['pandas']['compressao_taxa']:.2f}%\n\n"
        
        # Adicionar resultados do Dask
        relatorio += "### Resultados Dask\n\n"
        relatorio += f"- **Tempo Total:** {self.formatar_tempo(self.resultados['dask']['tempo_total'])}\n"
        relatorio += f"- **Tempo de Extração:** {self.formatar_tempo(self.resultados['dask']['tempo_extracao'])}\n"
        relatorio += f"- **Tempo de Processamento:** {self.formatar_tempo(self.resultados['dask']['tempo_processamento'])}\n"
        relatorio += f"- **Memória (pico):** {self.resultados['dask']['memoria_pico']:.2f}%\n"
        relatorio += f"- **Memória (média):** {self.resultados['dask']['memoria_media']:.2f}%\n"
        relatorio += f"- **CPU (pico):** {self.resultados['dask']['cpu_pico']:.2f}%\n"
        relatorio += f"- **CPU (média):** {self.resultados['dask']['cpu_medio']:.2f}%\n"
        relatorio += f"- **Espaço em Disco:** {self.resultados['dask']['espaco_disco']:.2f} MB\n"
        relatorio += f"- **Taxa de Compressão:** {self.resultados['dask']['compressao_taxa']:.2f}%\n\n"
        
        # Adicionar resultados do Polars
        relatorio += "### Resultados Polars\n\n"
        relatorio += f"- **Tempo Total:** {self.formatar_tempo(self.resultados['polars']['tempo_total'])}\n"
        relatorio += f"- **Tempo de Extração:** {self.formatar_tempo(self.resultados['polars']['tempo_extracao'])}\n"
        relatorio += f"- **Tempo de Processamento:** {self.formatar_tempo(self.resultados['polars']['tempo_processamento'])}\n"
        relatorio += f"- **Memória (pico):** {self.resultados['polars']['memoria_pico']:.2f}%\n"
        relatorio += f"- **Memória (média):** {self.resultados['polars']['memoria_media']:.2f}%\n"
        relatorio += f"- **CPU (pico):** {self.resultados['polars']['cpu_pico']:.2f}%\n"
        relatorio += f"- **CPU (média):** {self.resultados['polars']['cpu_medio']:.2f}%\n"
        relatorio += f"- **Espaço em Disco:** {self.resultados['polars']['espaco_disco']:.2f} MB\n"
        relatorio += f"- **Taxa de Compressão:** {self.resultados['polars']['compressao_taxa']:.2f}%\n\n"
        
        # Comparação dos resultados
        comparacao = self.comparar_resultados()
        relatorio += "### Comparação\n\n"
        
        relatorio += "| Métrica | Melhor Método | Diferença |\n"
        relatorio += "|---------|---------------|----------|\n"
        
        for criterio, resultado in comparacao['comparacao'].items():
            melhor = resultado['melhor'].upper()
            diferenca = f"{resultado['diferenca_percentual']:.2f}%"
            metrica_formatada = criterio.replace('_', ' ').title()
            relatorio += f"| {metrica_formatada} | **{melhor}** | {diferenca} |\n"
        
        relatorio += "\n"
        relatorio += f"**Conclusão:** {comparacao['melhor_metodo'].upper()} é o método mais adequado, "
        relatorio += f"vencendo em {comparacao['contagem'][comparacao['melhor_metodo']]} de {len(comparacao['comparacao'])} critérios.\n\n"
        
        # Verificar se os gráficos existem antes de incluí-los no relatório
        grafico_comparacao_path = os.path.join(docs_dir, 'benchmark_comparacao.png')
        if os.path.exists(grafico_comparacao_path):
            relatorio += "## Gráficos\n\n"
            relatorio += "### Gráfico de Comparação\n\n"
            relatorio += "![Gráfico de Comparação](benchmark_comparacao.png)\n\n"
            
            # Adicionar gráficos de tempo se existirem
            grafico_tempo_pandas = os.path.join(docs_dir, 'benchmark_tempo_pandas.png')
            if os.path.exists(grafico_tempo_pandas):
                relatorio += "### Distribuição de Tempo - Pandas\n\n"
                relatorio += "![Tempo Pandas](benchmark_tempo_pandas.png)\n\n"
                
            grafico_tempo_dask = os.path.join(docs_dir, 'benchmark_tempo_dask.png')
            if os.path.exists(grafico_tempo_dask):
                relatorio += "### Distribuição de Tempo - Dask\n\n"
                relatorio += "![Tempo Dask](benchmark_tempo_dask.png)\n\n"
            
            grafico_tempo_polars = os.path.join(docs_dir, 'benchmark_tempo_polars.png')
            if os.path.exists(grafico_tempo_polars):
                relatorio += "### Distribuição de Tempo - Polars\n\n"
                relatorio += "![Tempo Polars](benchmark_tempo_polars.png)\n\n"
        else:
            relatorio += "## Gráficos\n\n"
            relatorio += "*Não foi possível gerar gráficos para este relatório.*\n\n"
        
        # Salvar o relatório
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(relatorio)
        
        logger.info(f"Relatório completo gerado: {md_path}")
        return md_path

def main():
    """Função principal."""
    # Adicionar timestamp no início da execução
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Configuração do parser de argumentos
    parser = argparse.ArgumentParser(description='Benchmark para comparar processamento com Pandas, Dask e Polars')
    parser.add_argument('--path_zip', type=str, default='dados-abertos-zip', 
                        help='Caminho para o diretório com os arquivos ZIP')
    parser.add_argument('--arquivo_zip', type=str, 
                        help='Arquivo ZIP específico para análise ou padrão (ex: "Empre*.*")')
    parser.add_argument('--path_base', type=str, default='benchmark_temp', 
                        help='Caminho base para criar diretórios temporários')
    parser.add_argument('--parquet_destino', type=str, default='parquet', # Mantendo padrão simples
                        help='Caminho relativo dentro de path_base para os arquivos parquet gerados')
    parser.add_argument('--limpar', action='store_true', 
                        help='Limpar diretórios temporários')
    parser.add_argument('--pandas', action='store_true', 
                        help='Executar apenas o benchmark com Pandas')
    parser.add_argument('--dask', action='store_true', 
                        help='Executar apenas o benchmark com Dask')
    parser.add_argument('--polars', action='store_true',  # Adicionando argumento para Polars
                        help='Executar apenas o benchmark com Polars')
    parser.add_argument('--graficos', action='store_true', 
                        help='Gerar gráficos comparativos')
    parser.add_argument('--completo', action='store_true',
                        help='Executar todos os benchmarks e gerar gráficos')
    parser.add_argument('--workers', type=int, default=None,
                        help='Número de workers para processamento paralelo (padrão: número de cores)')
    
    # Adicionar argumentos para controle de log
    add_log_level_argument(parser)
    
    args = parser.parse_args()
    
    # Criar diretórios necessários
    os.makedirs(args.path_base, exist_ok=True)
    logs_dir = os.path.join(args.path_base, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    docs_dir = os.path.join(args.path_base, "docs")
    os.makedirs(docs_dir, exist_ok=True)
    
    # Configurar logging
    log_file = os.path.join(logs_dir, f"benchmark_{timestamp}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    configure_logging(args)
    
    logger.info(f"Iniciando benchmark. Log: {log_file}")
    
    # Verificações básicas
    if not os.path.exists(args.path_zip):
        print(f"Erro: Diretório de arquivos ZIP não encontrado: {args.path_zip}")
        logger.error(f"Diretório de arquivos ZIP não encontrado: {args.path_zip}")
        return
    
    # Informações do sistema
    try:
        info_sistema = InfoSistema.coletar_informacoes()
        InfoSistema.imprimir_informacoes(info_sistema)
    except Exception as e:
        logger.error(f"Erro ao coletar informações do sistema: {str(e)}")
    
    # Flags para controlar quais métodos foram executados
    pandas_executado = False
    dask_executado = False
    polars_executado = False  # Nova flag para Polars
    
    try:
        # Inicializar o benchmark
        benchmark = BenchmarkEmpresa(
            path_zip=args.path_zip, 
            path_base=args.path_base, 
            arquivo_zip_especifico=args.arquivo_zip,
            path_parquet_destino=args.parquet_destino,
            executar_limpeza=args.limpar
        )
        
        # Limpar diretórios docs e logs no início, se solicitado
        if args.limpar:
            print("\nLimpando diretórios antes de iniciar o benchmark...")
            benchmark.limpar_diretorios(preservar_parquet=False, limpar_docs_logs=True)
        
        # Executar os benchmarks
        try:
            # Verificar que combinação de flags usar
            executar_todos = args.completo
            executar_especificos = args.pandas or args.dask or args.polars
            
            # Se nenhuma flag específica foi fornecida e não é completo, executar pandas por padrão
            if not executar_especificos and not executar_todos:
                args.pandas = True
            
            # Executa Pandas se solicitado ou se completo e não específico para outros
            if args.pandas or (executar_todos and not (args.dask or args.polars)):
                benchmark.executar_benchmark_pandas(max_workers=args.workers)
                pandas_executado = True
                
            # Executa Dask se solicitado ou se completo e não específico para outros
            if args.dask or (executar_todos and not (args.pandas or args.polars)):
                benchmark.executar_benchmark_dask()
                dask_executado = True
                
            # Executa Polars se solicitado ou se completo e não específico para outros  
            if args.polars or (executar_todos and not (args.pandas or args.dask)):
                benchmark.executar_benchmark_polars()
                polars_executado = True
            
            # Se --completo foi especificado explicitamente, executar todos os métodos não executados ainda
            if args.completo:
                if not pandas_executado:
                    benchmark.executar_benchmark_pandas()
                    pandas_executado = True
                
                if not dask_executado:
                    benchmark.executar_benchmark_dask()
                    dask_executado = True
                
                if not polars_executado:
                    benchmark.executar_benchmark_polars()
                    polars_executado = True
            
            # Gerar gráficos comparativos somente se mais de um método foi executado
            graficos_criados = {}
            metodos_executados = [m for m, flag in [('pandas', pandas_executado), 
                                                   ('dask', dask_executado),
                                                   ('polars', polars_executado)] if flag]
            
            if (args.graficos or args.completo) and len(metodos_executados) >= 2:
                try:
                    graficos_criados = benchmark.gerar_graficos()
                    if not graficos_criados.get('comparacao', False):
                        logger.warning("Não foi possível gerar o gráfico de comparação")
                except Exception as e:
                    logger.error(f"Erro ao gerar gráficos: {str(e)}")
                    logger.debug(traceback.format_exc())
            elif (args.graficos or args.completo) and len(metodos_executados) < 2:
                logger.warning("Gráficos comparativos requerem pelo menos dois métodos")
            
            # Imprimir relatório final
            if len(metodos_executados) >= 2:
                benchmark.imprimir_relatorio()
                
                # Gerar relatório Markdown
                md_path = benchmark.gerar_relatorio_markdown(timestamp)
                logger.info(f"Relatório: {md_path}")
            else:
                # Relatório simplificado para método único
                for metodo in metodos_executados:
                    print("\n" + "="*40)
                    print(" "*15 + metodo.upper())
                    print("="*40)
                    print(f"Tempo: {benchmark.formatar_tempo(benchmark.resultados[metodo]['tempo_total'])}")
                    print(f"Memória: {benchmark.resultados[metodo]['memoria_pico']:.1f}%")
                    print(f"Espaço: {benchmark.resultados[metodo]['espaco_disco']:.1f} MB")
            
            # Limpar diretórios temporários se solicitado
            if args.limpar:
                print("\nLimpando diretórios temporários após benchmark...")
                # Limpar diretórios unzip após o processamento, mas preservar parquet
                benchmark.limpar_diretorios(preservar_parquet=True, limpar_docs_logs=False)
                
        except Exception as e:
            logger.error(f"Erro durante o benchmark: {str(e)}")
            logger.debug(traceback.format_exc())
            
    except Exception as e:
        logger.error(f"Erro ao inicializar benchmark: {str(e)}")
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    main()
