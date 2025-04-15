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
import polars as pl

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

# Definir o diretório do script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Definir o path base padrão relativo ao diretório do script
DEFAULT_PATH_BASE = os.path.join(SCRIPT_DIR, 'benchmark_temp')

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

# Função auxiliar para extração paralela
def extract_zip(zip_path, extract_to):
    """Extrai um único arquivo ZIP.
    Retorna True se sucesso, False caso contrário.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        logger.debug(f"Extraído: {zip_path} para {extract_to}")
        return True
    except Exception as e:
        logger.error(f"Falha ao extrair {zip_path}: {str(e)}")
        return False

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
        
        # --- Caminhos de Extração --- 
        # Manter pasta separada para Pandas devido à sua lógica atual
        self.path_unzip_pandas = os.path.join(path_base, "unzip_pandas")
        # Pasta geral para Dask e Polars compartilharem
        self.path_unzip_geral = os.path.join(path_base, "unzip_files") 
        
        # Flag para controlar se a extração geral já foi feita
        self.extrair_individualmente = True 
        
        # Criar diretórios se não existirem
        # Adicionar path_unzip_geral, remover dask/polars específicos
        for path in [self.path_unzip_pandas, self.path_unzip_geral, 
                     self.path_parquet_pandas, self.path_parquet_dask, self.path_parquet_polars]:
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
        """Coleta informações sobre o arquivo ZIP original (otimizado)."""
        info = {}
        
        for zip_file in self.zip_files:
            zip_path = os.path.join(self.path_zip, zip_file)
            if os.path.exists(zip_path):
                # Informações básicas do arquivo ZIP
                info[zip_file] = {
                    'tamanho_zip_mb': os.path.getsize(zip_path) / (1024 * 1024),
                    'data_modificacao': datetime.fromtimestamp(os.path.getmtime(zip_path)).strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Verificar conteúdo e calcular tamanho real a partir dos metadados do ZIP
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        arquivos = zip_ref.namelist()
                        csv_files = [f for f in arquivos if f.lower().endswith('.csv')] # Usar lower() para case-insensitive
                        info[zip_file]['num_arquivos'] = len(arquivos)
                        info[zip_file]['num_csv'] = len(csv_files)
                        
                        # Calcular tamanho total não comprimido somando file_size de cada membro
                        tamanho_total_interno_bytes = sum(member.file_size for member in zip_ref.infolist())
                        tamanho_real_mb = tamanho_total_interno_bytes / (1024 * 1024)
                        info[zip_file]['tamanho_real_mb'] = tamanho_real_mb
                        logger.debug(f"Tamanho real calculado para {zip_file} a partir dos metadados: {tamanho_real_mb:.2f} MB")
                        
                except zipfile.BadZipFile:
                    logger.error(f"Erro: Arquivo ZIP corrompido ou inválido: {zip_path}")
                    info[zip_file]['num_arquivos'] = 0
                    info[zip_file]['num_csv'] = 0
                    info[zip_file]['tamanho_real_mb'] = 0 # Definir como 0 se não puder ler
                except Exception as e:
                    logger.error(f"Erro ao analisar metadados do ZIP {zip_file}: {str(e)}")
                    # Manter valores padrão ou zerar?
                    info[zip_file].setdefault('num_arquivos', 0)
                    info[zip_file].setdefault('num_csv', 0)
                    info[zip_file].setdefault('tamanho_real_mb', 0) # Usar 0 se falhar
                    
        return info

    def limpar_diretorios(self, preservar_parquet=True, limpar_docs_logs=False):
        """Limpa os diretórios temporários."""
        if not self.executar_limpeza:
            return
            
        # Atualizar para limpar path_unzip_pandas e path_unzip_geral
        diretorios_extracao = [self.path_unzip_pandas, self.path_unzip_geral]
        for path in diretorios_extracao:
            if os.path.exists(path):
                print(f"Limpando diretório: {path}")
                # Usar shutil.rmtree para remover diretório e conteúdo
                try:
                    shutil.rmtree(path)
                    os.makedirs(path, exist_ok=True) # Recriar pasta vazia
                except Exception as e:
                     print_warning(f"Falha ao limpar completamente {path}: {e}")
                     logger.warning(f"Falha ao limpar completamente {path}: {e}")
        
        # Limpar diretórios de parquet (sem alteração na lógica)
        if not preservar_parquet:
            diretorios_parquet = [self.path_parquet_pandas, self.path_parquet_dask, self.path_parquet_polars]
            for path in diretorios_parquet:
                if os.path.exists(path):
                    print(f"Limpando diretório: {path}")
                    try:
                        shutil.rmtree(path)
                        os.makedirs(path, exist_ok=True) # Recriar pasta vazia
                    except Exception as e:
                        print_warning(f"Falha ao limpar completamente {path}: {e}")
                        logger.warning(f"Falha ao limpar completamente {path}: {e}")
        
        # Limpar diretórios docs e logs (sem alteração na lógica)
        if limpar_docs_logs:
            docs_dir = os.path.join(self.path_base, "docs")
            logs_dir = os.path.join(self.path_base, "logs")
            for path in [docs_dir, logs_dir]:
                if os.path.exists(path):
                    print(f"Limpando diretório: {path}")
                    try:
                        shutil.rmtree(path)
                        os.makedirs(path, exist_ok=True) # Recriar pasta vazia
                    except Exception as e:
                         print_warning(f"Falha ao limpar completamente {path}: {e}")
                         logger.warning(f"Falha ao limpar completamente {path}: {e}")
            
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
        Executa o benchmark usando Pandas com processamento paralelo POR ZIP.
        (Mantém sua lógica separada e pasta unzip_pandas por enquanto).
        """
        logger.info("\nBENCHMARK COM PANDAS (PROCESSAMENTO PARALELO POR ZIP)")
        print_header("BENCHMARK PANDAS") # Adicionado header
        
        tempo_benchmark_inicio = time.time()
        
        # Limpar APENAS o diretório do Pandas (unzip e parquet)
        print(f"Limpando diretórios Pandas: {self.path_unzip_pandas}, {self.path_parquet_pandas}")
        if os.path.exists(self.path_unzip_pandas):
             shutil.rmtree(self.path_unzip_pandas)
        os.makedirs(self.path_unzip_pandas, exist_ok=True)
        if os.path.exists(self.path_parquet_pandas):
             shutil.rmtree(self.path_parquet_pandas)
        os.makedirs(self.path_parquet_pandas, exist_ok=True)
        
        # ... (resto da lógica do Pandas permanece igual, usando self.path_unzip_pandas) ...
        if max_workers is None:
            max_workers = os.cpu_count()
            max_workers = min(max_workers, len(self.zip_files), 8)
        logger.info(f"Usando {max_workers} workers para processamento paralelo Pandas")
        print(f"\n[Pandas] Iniciando processamento paralelo com {max_workers} workers")
        cpu_medidas = []
        memoria_medidas = []
        tempo_inicio_processamento = time.time()
        monitorar_recursos = [True]
        def monitor_recursos_pandas(): # Renomeado para evitar conflito
            while monitorar_recursos[0]:
                cpu_medidas.append(psutil.cpu_percent(interval=0.5))
                memoria_medidas.append(psutil.virtual_memory().percent)
                time.sleep(0.5)
        monitor_thread = threading.Thread(target=monitor_recursos_pandas)
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
            logger.warning("Não foi possível encontrar o FileHandler principal para passar aos workers Pandas.")
        try:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                future_to_zip = {}
                for zip_file in self.zip_files:
                    # A função process_single_zip_pandas precisa ser importada e usada aqui
                    # Ela já usa path_unzip_pandas e path_parquet_pandas internamente (PRECISA VERIFICAR)
                    # Precisamos garantir que process_single_zip_pandas receba o log_file
                    future = executor.submit(process_single_zip_pandas, 
                                               zip_file, 
                                               self.path_zip, 
                                               self.path_unzip_pandas, # Passa o caminho correto
                                               self.path_parquet_pandas, # Passa o caminho correto
                                               log_file) # Passa o log file
                    future_to_zip[future] = zip_file
                
                for future in concurrent.futures.as_completed(future_to_zip):
                    zip_file = future_to_zip[future]
                    try:
                        info_resultado = future.result() # Espera dict: {'sucesso': bool, 'tempo': float}
                        resultados_paralelos.append(info_resultado)
                        if info_resultado.get('sucesso', False):
                            sucessos += 1
                            tempo_processamento_total += info_resultado.get('tempo', 0)
                        else:
                            logger.warning(f"Worker Pandas para {zip_file} reportou falha ou não retornou sucesso.")
                    except Exception as e:
                        print(f"[Pandas] {zip_file} gerou exceção ao obter resultado: {str(e)}")
                        logger.error(f"Exceção ao obter resultado Pandas de {zip_file}: {str(e)}")
                        logger.debug(traceback.format_exc())
        finally:
            tempo_processamento_paralelo = time.time() - tempo_inicio_processamento
            monitorar_recursos[0] = False
            monitor_thread.join(timeout=1.0)
        
        tempo_benchmark_total = time.time() - tempo_benchmark_inicio
        self.resultados['pandas']['tempo_total'] = tempo_benchmark_total
        self.resultados['pandas']['tempo_extracao'] = 0 # Não medido separadamente aqui
        self.resultados['pandas']['tempo_processamento'] = tempo_processamento_paralelo
        self.resultados['pandas']['tempo_worker_total'] = tempo_processamento_total
        self.resultados['pandas']['memoria_pico'] = max(memoria_medidas) if memoria_medidas else 0
        self.resultados['pandas']['memoria_media'] = sum(memoria_medidas) / len(memoria_medidas) if memoria_medidas else 0
        self.resultados['pandas']['cpu_medio'] = sum(cpu_medidas) / len(cpu_medidas) if cpu_medidas else 0
        self.resultados['pandas']['cpu_pico'] = max(cpu_medidas) if cpu_medidas else 0
        self.resultados['pandas']['espaco_disco'] = self.calcular_tamanho_diretorio(self.path_parquet_pandas)
        self.resultados['pandas']['num_arquivos'] = self.contar_arquivos(self.path_parquet_pandas)
        self.resultados['pandas']['arquivos_parquet'] = self.listar_arquivos_parquet(self.path_parquet_pandas)
        parquet_dir = os.path.join(self.path_parquet_pandas, 'empresas') # Ajustar se o subdir for diferente
        print(f"\n[Pandas] Arquivos parquet salvos em: {os.path.abspath(parquet_dir)}")
        if self.resultados['pandas']['num_arquivos'] > 0:
            print(f"[Pandas] {self.resultados['pandas']['num_arquivos']} arquivos parquet gerados")
            # exemplo_arquivo = self.resultados['pandas']['arquivos_parquet'][0] if self.resultados['pandas']['arquivos_parquet'] else "Nenhum arquivo encontrado"
            # print(f"[Pandas] Exemplo de arquivo: {exemplo_arquivo}")
        else:
            print_warning("[Pandas] Nenhum arquivo parquet foi gerado!")
        tamanho_original = sum([info.get('tamanho_real_mb', 0) for info in self.info_arquivo.values()])
        if tamanho_original > 0 and self.resultados['pandas']['espaco_disco'] > 0:
            self.resultados['pandas']['compressao_taxa'] = self.calcular_taxa_compressao(
                tamanho_original, self.resultados['pandas']['espaco_disco'])
        else:
            self.resultados['pandas']['compressao_taxa'] = 0
            logger.warning("Não foi possível calcular taxa de compressão para Pandas")
        print(f"\n[Pandas] Benchmark paralelo concluído em {self.formatar_tempo(tempo_benchmark_total)}")
        print(f"[Pandas] {sucessos} de {len(self.zip_files)} arquivos processados com sucesso")
        if sucessos > 0:
            velocidade = sucessos / tempo_benchmark_total
            print(f"[Pandas] Velocidade média: {velocidade:.2f} arquivos/segundo")
        logger.info(f"Benchmark com Pandas concluído em {tempo_benchmark_total:.2f} segundos")
        logger.info(f"Arquivos processados (Pandas): {sucessos}/{len(self.zip_files)}")
        gc.collect()
        return self.resultados['pandas']

    def executar_benchmark_dask(self, max_workers=None):
        """
        Executa o benchmark usando Dask, utilizando a pasta geral 'unzip_files'.
        Extrai os arquivos apenas se necessário (extração individual).
        """
        logger.info('\nBENCHMARK COM DASK (Pasta Geral)')
        print_section("BENCHMARK DASK (Pasta Geral)")

        tempo_benchmark_inicio = time.time()
        tempo_extracao = 0
        precisou_extrair = False # Flag para controlar limpeza

        # Limpar diretório de parquet do Dask
        print(f"Limpando diretório parquet Dask: {self.path_parquet_dask}")
        if os.path.exists(self.path_parquet_dask):
             shutil.rmtree(self.path_parquet_dask)
        os.makedirs(self.path_parquet_dask, exist_ok=True)

        # --- 1. Extração Condicional --- 
        if self.extrair_individualmente:
            print("\n[Dask] Extração individual necessária...")
            logger.info("Executando extração individual para Dask.")
            # Chamar o método de extração para a pasta geral
            extracao_ok, tempo_ext = self._extrair_arquivos_zip_paralelo(max_workers)
            tempo_extracao = tempo_ext # Registrar tempo de extração
            precisou_extrair = True
            if not extracao_ok:
                print_error("[Dask] Falha na extração individual. Abortando benchmark Dask.")
                logger.error("Falha na extração individual para Dask.")
                self.resultados['dask']['tempo_total'] = time.time() - tempo_benchmark_inicio
                return self.resultados['dask']
        else:
            print("\n[Dask] Usando arquivos pré-extraídos em: ", self.path_unzip_geral)
            logger.info(f"Dask reutilizando pasta de extração geral: {self.path_unzip_geral}")

        # --- 2. Processamento Dask Consolidado --- 
        print("\n[Dask] Etapa 2: Processamento Dask dos arquivos CSV extraídos...")
        tempo_inicio_processamento = time.time()
        cpu_medidas = []
        memoria_medidas = []
        monitorar_recursos = [True]
        def monitor_recursos_dask():
            while monitorar_recursos[0]:
                cpu_medidas.append(psutil.cpu_percent(interval=0.5))
                memoria_medidas.append(psutil.virtual_memory().percent)
                time.sleep(0.5)
        monitor_thread = threading.Thread(target=monitor_recursos_dask)
        monitor_thread.daemon = True
        monitor_thread.start()
        client = None
        dask_manager = None
        try:
            print(f"[Dask] Inicializando cliente Dask... ", end="", flush=True)
            dask_manager = DaskManager.initialize(
                n_workers=config.dask.n_workers,
                memory_limit=config.dask.memory_limit,
                dashboard_address=config.dask.dashboard_address
            )
            client = dask_manager.client
            print(f"✓ Cliente Dask inicializado com {config.dask.n_workers} workers")
            logger.info(f"Cliente Dask inicializado")

            # Encontrar todos os arquivos que contenham 'CSV' no nome (case-insensitive)
            print(f"[Dask] Procurando por arquivos contendo 'CSV' em {self.path_unzip_geral}...")
            csv_files = []
            try:
                for root, dirs, files in os.walk(self.path_unzip_geral):
                    for filename in files:
                        if "csv" in filename.lower():
                            csv_files.append(os.path.join(root, filename))
            except Exception as e_walk:
                print_error(f"[Dask] Erro ao buscar arquivos CSV em {self.path_unzip_geral}: {e_walk}")
                logger.error(f"Erro ao buscar arquivos CSV em {self.path_unzip_geral}: {e_walk}")
                raise RuntimeError(f"Erro ao buscar arquivos CSV: {e_walk}") from e_walk
                
            logger.info(f"Encontrados {len(csv_files)} arquivos contendo 'CSV' em {self.path_unzip_geral}")

            if not csv_files:
                print_warning(f"[Dask] Nenhum arquivo contendo 'CSV' encontrado para processar em {self.path_unzip_geral}.")
                logger.warning(f"Nenhum arquivo contendo 'CSV' encontrado em {self.path_unzip_geral}.")
                raise RuntimeError(f"Nenhum arquivo contendo 'CSV' encontrado para processar em {self.path_unzip_geral}.")

            print(f"[Dask] Lendo {len(csv_files)} arquivos com Dask...")
            # Obter as chaves do schema ANTES
            try:
                schema_keys = config.empresa_columns # Usar a lista de colunas correta
            except AttributeError as e:
                print_error(f"[Dask] Erro ao acessar config.empresa_columns: {e}") # Atualizar mensagem de erro
                logger.error(f"Erro ao acessar config.empresa_columns: {e}")
                raise RuntimeError("Falha ao obter colunas de empresa da configuração.") from e
                
            # Ler os arquivos encontrados
            ddf = dd.read_csv(csv_files, sep=';', encoding='latin1', dtype=str, header=None)
            ddf.columns = schema_keys # Usar a variável local
            logger.info("Colunas renomeadas conforme schema.")

            print("[Dask] Aplicando transformações...")
            # Reaplicando: Importar a função de transformação Dask com o nome corrigido
            from src.process.empresa import apply_empresa_transformations_dask
            # Reaplicando: Chamar a função correta
            ddf = apply_empresa_transformations_dask(ddf)

            print("[Dask] Salvando arquivos Parquet...")
            # Ajustar o caminho de saída para ser um diretório 'empresas'
            parquet_output_path = os.path.join(self.path_parquet_dask, 'empresas')
            if os.path.exists(parquet_output_path):
                 if os.path.isdir(parquet_output_path):
                     shutil.rmtree(parquet_output_path)
                 else:
                     os.remove(parquet_output_path)
                 logger.info(f"Diretório/arquivo parquet antigo removido: {parquet_output_path}")
            if isinstance(ddf, bool):
                print_error(f"[Dask] Erro: Resultado da transformação é um valor booleano ({ddf}), não um DataFrame.")
                logger.error(f"Resultado da transformação Dask é booleano ({ddf}), não um DataFrame.")
                raise RuntimeError("Transformação Dask retornou booleano em vez de DataFrame.")
            # Remover schema='infer' para deixar o PyArrow lidar com a escrita
            ddf.to_parquet(parquet_output_path, engine='pyarrow', compression='snappy', write_index=False)
            print(f"[Dask] Arquivos Parquet salvos em: {os.path.abspath(parquet_output_path)}")
            logger.info(f"Resultado Dask salvo em Parquet: {parquet_output_path}")
            tempo_processamento = time.time() - tempo_inicio_processamento
            print(f"[Dask] Processamento Dask concluído em {self.formatar_tempo(tempo_processamento)}")

        except Exception as e:
            print_error(f"[Dask] Erro durante o processamento Dask: {str(e)}")
            logger.error(f"Erro durante o processamento Dask: {str(e)}")
            logger.debug(traceback.format_exc())
            tempo_processamento = time.time() - tempo_inicio_processamento
        finally:
            monitorar_recursos[0] = False
            monitor_thread.join(timeout=1.0)
            if dask_manager:
                try:
                    print(f"[Dask] Encerrando cliente Dask... ", end="", flush=True)
                    dask_manager.shutdown()
                    print("✓ Cliente encerrado")
                    logger.debug("Cliente Dask encerrado")
                except Exception as e_shutdown:
                    print(f"✗ Erro ao encerrar Dask: {str(e_shutdown)}")
                    logger.error(f"Erro ao encerrar Dask: {str(e_shutdown)}")
            
            # --- 3. Limpeza Condicional da Pasta Geral --- 
            if precisou_extrair:
                print("\n[Dask] Limpando pasta de extração geral (pois foi extraída individualmente)...")
                try:
                    if os.path.exists(self.path_unzip_geral):
                        shutil.rmtree(self.path_unzip_geral)
                        print(f"[Dask] Diretório {self.path_unzip_geral} removido.")
                        logger.info(f"Diretório de extração geral limpo pelo Dask: {self.path_unzip_geral}")
                except Exception as e_clean:
                    print_warning(f"[Dask] Falha ao limpar diretório de extração geral: {str(e_clean)}")
                    logger.warning(f"Falha ao limpar {self.path_unzip_geral} pelo Dask: {str(e_clean)}")
            else:
                 print("\n[Dask] Não limpando pasta de extração geral (reutilizada).")
                 logger.info(f"Dask não limpou a pasta geral {self.path_unzip_geral} (reutilizada).")

        tempo_benchmark_total = time.time() - tempo_benchmark_inicio
        self.resultados['dask']['tempo_total'] = tempo_benchmark_total
        self.resultados['dask']['tempo_extracao'] = tempo_extracao
        self.resultados['dask']['tempo_processamento'] = tempo_processamento
        self.resultados['dask']['memoria_pico'] = max(memoria_medidas) if memoria_medidas else 0
        self.resultados['dask']['memoria_media'] = sum(memoria_medidas) / len(memoria_medidas) if memoria_medidas else 0
        self.resultados['dask']['cpu_medio'] = sum(cpu_medidas) / len(cpu_medidas) if cpu_medidas else 0
        self.resultados['dask']['cpu_pico'] = max(cpu_medidas) if cpu_medidas else 0
        self.resultados['dask']['espaco_disco'] = self.calcular_tamanho_diretorio(self.path_parquet_dask)
        self.resultados['dask']['num_arquivos'] = self.contar_arquivos(self.path_parquet_dask)
        self.resultados['dask']['arquivos_parquet'] = self.listar_arquivos_parquet(self.path_parquet_dask)
        tamanho_original = sum([info.get('tamanho_real_mb', 0) for info in self.info_arquivo.values()])
        if tamanho_original > 0 and self.resultados['dask']['espaco_disco'] > 0:
            self.resultados['dask']['compressao_taxa'] = self.calcular_taxa_compressao(
                tamanho_original, self.resultados['dask']['espaco_disco'])
        else:
            self.resultados['dask']['compressao_taxa'] = 0
            if tamanho_original == 0:
                 logger.warning("Tamanho original dos arquivos é zero, não foi possível calcular taxa de compressão Dask.")
        print(f"\n[Dask] Benchmark (Pasta Geral) concluído em {self.formatar_tempo(tempo_benchmark_total)}")
        logger.info(f"Benchmark Dask (Pasta Geral) concluído em {tempo_benchmark_total:.2f} segundos")
        gc.collect()
        return self.resultados['dask']

    def executar_benchmark_polars(self, max_workers=None): # Adicionado max_workers para consistência, usado na extração E LEITURA
        """ 
        Executa o benchmark usando Polars, lendo todos os CSVs da pasta geral 'unzip_files'.
        Extrai os arquivos apenas se necessário (extração individual).
        """
        logger.info("\nBENCHMARK COM POLARS (Pasta Geral)")
        print_section("BENCHMARK POLARS (Pasta Geral)")
        
        tempo_benchmark_inicio = time.time()
        tempo_extracao = 0
        precisou_extrair = False
        tempo_processamento = 0 # Inicializar tempo de processamento

        # Limpar diretório de parquet do Polars
        print(f"Limpando diretório parquet Polars: {self.path_parquet_polars}")
        if os.path.exists(self.path_parquet_polars):
             shutil.rmtree(self.path_parquet_polars)
        os.makedirs(self.path_parquet_polars, exist_ok=True)

        # --- 1. Extração Condicional --- 
        if self.extrair_individualmente:
            print("\n[Polars] Extração individual necessária...")
            logger.info("Executando extração individual para Polars.")
            extracao_ok, tempo_ext = self._extrair_arquivos_zip_paralelo(max_workers)
            tempo_extracao = tempo_ext
            precisou_extrair = True
            if not extracao_ok:
                print_error("[Polars] Falha na extração individual. Abortando benchmark Polars.")
                logger.error("Falha na extração individual para Polars.")
                self.resultados['polars']['tempo_total'] = time.time() - tempo_benchmark_inicio
                return self.resultados['polars']
        else:
            print("\n[Polars] Usando arquivos pré-extraídos em: ", self.path_unzip_geral)
            logger.info(f"Polars reutilizando pasta de extração geral: {self.path_unzip_geral}")

        # --- 2. Processamento Polars Consolidado --- 
        print("\n[Polars] Etapa 2: Processamento Polars dos arquivos CSV extraídos...")
        tempo_inicio_processamento = time.time()
        cpu_medidas = []
        memoria_medidas = []
        monitorar_recursos = [True]
        def monitor_recursos_polars():
            while monitorar_recursos[0]:
                cpu_medidas.append(psutil.cpu_percent(interval=0.5))
                memoria_medidas.append(psutil.virtual_memory().percent)
                time.sleep(0.5)
        monitor_thread = threading.Thread(target=monitor_recursos_polars)
        monitor_thread.daemon = True
        monitor_thread.start()

        try:
            # --- ALTERAR LÓGICA DE BUSCA DE ARQUIVOS --- 
            print(f"[Polars] Procurando por arquivos contendo 'CSV' em {self.path_unzip_geral} (case-insensitive)...")
            csv_files = []
            try:
                for root, dirs, files in os.walk(self.path_unzip_geral):
                    for filename in files:
                        if "csv" in filename.lower(): # Usar a mesma lógica do Dask
                            csv_files.append(os.path.join(root, filename))
            except Exception as e_walk:
                print_error(f"[Polars] Erro ao buscar arquivos CSV em {self.path_unzip_geral}: {e_walk}")
                logger.error(f"Erro ao buscar arquivos CSV em {self.path_unzip_geral}: {e_walk}")
                raise RuntimeError(f"Erro ao buscar arquivos CSV: {e_walk}") from e_walk

            # --- FIM DA ALTERAÇÃO --- 
            logger.info(f"Encontrados {len(csv_files)} arquivos contendo 'CSV' em {self.path_unzip_geral}")

            if not csv_files:
                print_warning(f"[Polars] Nenhum arquivo contendo 'CSV' encontrado para processar em {self.path_unzip_geral}.")
                logger.warning(f"Nenhum arquivo contendo 'CSV' encontrado em {self.path_unzip_geral}.")
                raise RuntimeError(f"Nenhum arquivo contendo 'CSV' encontrado para processar em {self.path_unzip_geral}.")

            print(f"[Polars] Lendo {len(csv_files)} arquivos CSV com Polars em paralelo...")
            # Importar funções de transformação e criação de parquet específicas do Polars
            from src.process.empresa import apply_empresa_transformations_polars, create_parquet_polars
            
            # Obter as chaves do schema ANTES
            try:
                schema_keys = config.empresa_columns # Usar a lista de colunas correta
            except AttributeError as e:
                print_error(f"[Polars] Erro ao acessar config.empresa_columns: {e}") # Atualizar mensagem de erro
                logger.error(f"Erro ao acessar config.empresa_columns: {e}")
                raise RuntimeError("Falha ao obter colunas de empresa da configuração.") from e

            # --- PARALELIZAR LEITURA CSV --- 
            all_dfs_polars = [] # Lista para guardar os DataFrames

            # Função auxiliar para ler um CSV (precisa ter acesso a schema_keys)
            def _read_single_csv_polars(csv_path):
                try:
                    df_single = pl.read_csv(
                        source=csv_path, 
                        separator=';', 
                        encoding='latin1', 
                        has_header=False,
                        new_columns=schema_keys, # Usa schema_keys do escopo externo
                        infer_schema_length=0, 
                        ignore_errors=True 
                    )
                    if df_single is not None and df_single.height > 0:
                        # logger.debug(f"Lido {csv_path} com {df_single.height} linhas.") # Log pode ficar verboso em paralelo
                        return df_single
                    else:
                        # logger.warning(f"Arquivo {csv_path} vazio ou não pôde ser lido.")
                        return None
                except Exception as e_read_single:
                    logger.error(f"Erro ao ler arquivo CSV {csv_path} com Polars: {e_read_single}")
                    return None # Retorna None em caso de erro

            # Determinar número de workers para leitura
            num_read_workers = max_workers
            if num_read_workers is None:
                num_read_workers = os.cpu_count() # Padrão: número de CPUs
                num_read_workers = min(num_read_workers, len(csv_files), 8) # Limitar um pouco
            logger.info(f"Usando {num_read_workers} workers para leitura paralela Polars.")
            
            # Usar ThreadPoolExecutor para ler em paralelo
            futures = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_read_workers) as executor:
                for csv_path in csv_files:
                    futures.append(executor.submit(_read_single_csv_polars, csv_path))
            
            # Coletar resultados e filtrar None
            for future in concurrent.futures.as_completed(futures):
                result_df = future.result()
                if result_df is not None:
                    all_dfs_polars.append(result_df)

            # --- FIM DA PARALELIZAÇÃO --- 
            
            if not all_dfs_polars:
                logger.error("Nenhum DataFrame Polars foi lido com sucesso dos arquivos CSV.")
                raise RuntimeError("Falha ao ler qualquer arquivo CSV com Polars.")

            # Concatenar os DataFrames
            logger.info(f"Concatenando {len(all_dfs_polars)} DataFrames Polars lidos em paralelo...")
            df_polars = pl.concat(all_dfs_polars)

            logger.info(f"Leitura e concatenação Polars concluída. DataFrame com {df_polars.height} linhas.")

            print("[Polars] Aplicando transformações...")
            df_polars = apply_empresa_transformations_polars(df_polars)
            logger.info("Transformações Polars aplicadas.")

            print("[Polars] Salvando arquivos Parquet...")
            parquet_output_dir = os.path.join(self.path_parquet_polars, 'empresas') # Definir diretório de saída
            
            # Chamar a função para criar os parquets (já limpa o diretório)
            success_parquet = create_parquet_polars(df_polars, 'empresas', self.path_parquet_polars)

            if success_parquet:
                 print(f"[Polars] Arquivos Parquet salvos em: {os.path.abspath(parquet_output_dir)}")
                 logger.info(f"Resultado Polars salvo em Parquet: {parquet_output_dir}")
            else:
                 print_error("[Polars] Falha ao salvar arquivos Parquet.")
                 logger.error("Falha ao criar arquivos Parquet com Polars.")
                 # Considerar levantar um erro ou apenas registrar

            tempo_processamento = time.time() - tempo_inicio_processamento
            print(f"[Polars] Processamento Polars concluído em {self.formatar_tempo(tempo_processamento)}")

        except Exception as e:
            print_error(f"[Polars] Erro durante o processamento Polars: {str(e)}")
            logger.error(f"Erro durante o processamento Polars: {str(e)}")
            logger.debug(traceback.format_exc())
            tempo_processamento = time.time() - tempo_inicio_processamento
        finally:
            monitorar_recursos[0] = False
            monitor_thread.join(timeout=1.0)

            # --- 3. Limpeza Condicional da Pasta Geral --- 
            if precisou_extrair:
                print("\n[Polars] Limpando pasta de extração geral (pois foi extraída individualmente)...")
                try:
                    if os.path.exists(self.path_unzip_geral):
                        shutil.rmtree(self.path_unzip_geral)
                        print(f"[Polars] Diretório {self.path_unzip_geral} removido.")
                        logger.info(f"Diretório de extração geral limpo pelo Polars: {self.path_unzip_geral}")
                except Exception as e_clean:
                    print_warning(f"[Polars] Falha ao limpar diretório de extração geral: {str(e_clean)}")
                    logger.warning(f"Falha ao limpar {self.path_unzip_geral} pelo Polars: {str(e_clean)}")
            else:
                 print("\n[Polars] Não limpando pasta de extração geral (reutilizada).")
                 logger.info(f"Polars não limpou a pasta geral {self.path_unzip_geral} (reutilizada).")

        tempo_benchmark_total = time.time() - tempo_benchmark_inicio
        self.resultados['polars']['tempo_total'] = tempo_benchmark_total
        self.resultados['polars']['tempo_extracao'] = tempo_extracao
        self.resultados['polars']['tempo_processamento'] = tempo_processamento
        self.resultados['polars']['memoria_pico'] = max(memoria_medidas) if memoria_medidas else 0
        self.resultados['polars']['memoria_media'] = sum(memoria_medidas) / len(memoria_medidas) if memoria_medidas else 0
        self.resultados['polars']['cpu_medio'] = sum(cpu_medidas) / len(cpu_medidas) if cpu_medidas else 0
        self.resultados['polars']['cpu_pico'] = max(cpu_medidas) if cpu_medidas else 0
        self.resultados['polars']['espaco_disco'] = self.calcular_tamanho_diretorio(self.path_parquet_polars)
        self.resultados['polars']['num_arquivos'] = self.contar_arquivos(self.path_parquet_polars)
        self.resultados['polars']['arquivos_parquet'] = self.listar_arquivos_parquet(self.path_parquet_polars)
        tamanho_original = sum([info.get('tamanho_real_mb', 0) for info in self.info_arquivo.values()])
        if tamanho_original > 0 and self.resultados['polars']['espaco_disco'] > 0:
            self.resultados['polars']['compressao_taxa'] = self.calcular_taxa_compressao(
                tamanho_original, self.resultados['polars']['espaco_disco'])
        else:
            self.resultados['polars']['compressao_taxa'] = 0
            if tamanho_original == 0:
                 logger.warning("Tamanho original dos arquivos é zero, não foi possível calcular taxa de compressão Polars.")
        print(f"\n[Polars] Benchmark (Pasta Geral) concluído em {self.formatar_tempo(tempo_benchmark_total)}")
        logger.info(f"Benchmark Polars (Pasta Geral) concluído em {tempo_benchmark_total:.2f} segundos")
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
            grafico_path = os.path.join(docs_dir, 'benchmark_empresa_comparacao.png')
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
                    grafico_tempo_path = os.path.join(docs_dir, 'benchmark_empresa_tempo_pandas.png')
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
                    grafico_tempo_path = os.path.join(docs_dir, 'benchmark_empresa_tempo_dask.png')
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
                    grafico_tempo_path = os.path.join(docs_dir, 'benchmark_empresa_tempo_polars.png')
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
        md_path = os.path.join(docs_dir, f"relatorio_empresa_completo_{timestamp}.md")
        
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
        grafico_comparacao_path = os.path.join(docs_dir, 'benchmark_empresa_comparacao.png')
        if os.path.exists(grafico_comparacao_path):
            relatorio += "## Gráficos\n\n"
            relatorio += "### Gráfico de Comparação\n\n"
            relatorio += "![Gráfico de Comparação](benchmark_empresa_comparacao.png)\n\n"
            
            # Adicionar gráficos de tempo se existirem
            grafico_tempo_pandas = os.path.join(docs_dir, 'benchmark_empresa_tempo_pandas.png')
            if os.path.exists(grafico_tempo_pandas):
                relatorio += "### Distribuição de Tempo - Pandas\n\n"
                relatorio += "![Tempo Pandas](benchmark_empresa_tempo_pandas.png)\n\n"
                
            grafico_tempo_dask = os.path.join(docs_dir, 'benchmark_empresa_tempo_dask.png')
            if os.path.exists(grafico_tempo_dask):
                relatorio += "### Distribuição de Tempo - Dask\n\n"
                relatorio += "![Tempo Dask](benchmark_empresa_tempo_dask.png)\n\n"
            
            grafico_tempo_polars = os.path.join(docs_dir, 'benchmark_empresa_tempo_polars.png')
            if os.path.exists(grafico_tempo_polars):
                relatorio += "### Distribuição de Tempo - Polars\n\n"
                relatorio += "![Tempo Polars](benchmark_empresa_tempo_polars.png)\n\n"
        else:
            relatorio += "## Gráficos\n\n"
            relatorio += "*Não foi possível gerar gráficos para este relatório.*\n\n"
        
        # Salvar o relatório
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(relatorio)
        
        logger.info(f"Relatório completo gerado: {md_path}")
        return md_path

    # --- NOVO MÉTODO PRIVADO PARA EXTRAÇÃO GERAL --- 
    def _extrair_arquivos_zip_paralelo(self, max_workers=None):
        """ 
        Extrai todos os arquivos ZIP listados em self.zip_files 
        para a pasta self.path_unzip_geral usando ProcessPoolExecutor.
        Limpa a pasta de destino antes de extrair.
        Retorna True se pelo menos um arquivo foi extraído com sucesso, False caso contrário.
        Retorna também o tempo gasto na extração.
        """
        target_path = self.path_unzip_geral
        print(f"\nIniciando extração paralela para: {target_path}")
        logger.info(f"Iniciando extração paralela para: {target_path}")
        
        # 1. Limpar diretório de destino
        if os.path.exists(target_path):
            print(f"Limpando diretório de destino existente: {target_path}")
            try:
                shutil.rmtree(target_path)
            except Exception as e_clean:
                print_error(f"Falha ao limpar diretório de destino {target_path}: {e_clean}")
                logger.error(f"Falha ao limpar diretório de destino {target_path}: {e_clean}")
                return False, 0 # Falha na limpeza é crítico
        try:
            os.makedirs(target_path, exist_ok=True)
            logger.debug(f"Diretório de destino recriado: {target_path}")
        except Exception as e_mkdir:
             print_error(f"Falha ao recriar diretório de destino {target_path}: {e_mkdir}")
             logger.error(f"Falha ao recriar diretório de destino {target_path}: {e_mkdir}")
             return False, 0

        # 2. Extrair em paralelo
        tempo_inicio_extracao = time.time()
        if max_workers is None:
            max_workers = os.cpu_count()
            max_workers = min(max_workers, len(self.zip_files), 8)
        logger.info(f"Usando {max_workers} workers para extração paralela em {target_path}")

        futures = []
        sucessos_extracao = 0
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            for zip_file in self.zip_files:
                zip_path = os.path.join(self.path_zip, zip_file)
                future = executor.submit(extract_zip, zip_path, target_path)
                futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                if future.result():
                    sucessos_extracao += 1

        tempo_extracao = time.time() - tempo_inicio_extracao
        print(f"Extração paralela para {target_path} concluída em {self.formatar_tempo(tempo_extracao)}")
        print(f"{sucessos_extracao}/{len(self.zip_files)} arquivos ZIP extraídos com sucesso.")
        logger.info(f"Extração paralela para {target_path} concluída em {tempo_extracao:.2f}s. Sucessos: {sucessos_extracao}/{len(self.zip_files)}")

        return sucessos_extracao > 0, tempo_extracao

def main():
    """Função principal."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(description='Benchmark para comparar processamento com Pandas, Dask e Polars')
    parser.add_argument('--path_zip', type=str, default='dados-abertos-zip', 
                        help='Caminho para o diretório com os arquivos ZIP')
    parser.add_argument('--arquivo_zip', type=str, 
                        help='Arquivo ZIP específico para análise ou padrão (ex: "Empre*.*")')
    # Usar o DEFAULT_PATH_BASE calculado
    parser.add_argument('--path_base', type=str, default=DEFAULT_PATH_BASE, 
                        help='Caminho base para criar diretórios temporários (padrão: benchmark_temp dentro do dir do script)')
    parser.add_argument('--parquet_destino', type=str, default='parquet', # Mantendo padrão simples
                        help='Caminho relativo dentro de path_base para os arquivos parquet gerados')
    parser.add_argument('--limpar', action='store_true', 
                        help='Limpar diretórios temporários (incluindo logs/docs antigos) no início')
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
    add_log_level_argument(parser)
    args = parser.parse_args()
    
    # --- Limpeza Inicial (se --limpar) --- 
    if args.limpar:
        print("\nLimpando diretórios logs e docs antigos antes de iniciar...")
        # Criar instância temporária apenas para limpeza inicial de logs/docs
        temp_cleaner = BenchmarkEmpresa(args.path_zip, args.path_base, executar_limpeza=False) # False para não limpar dados ainda
        temp_cleaner.limpar_diretorios(preservar_parquet=True, limpar_docs_logs=True)
        del temp_cleaner
        print("Limpeza inicial de logs/docs concluída.")
        
    # --- Configurar Logging --- 
    os.makedirs(args.path_base, exist_ok=True)
    logs_dir = os.path.join(args.path_base, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    docs_dir = os.path.join(args.path_base, "docs")
    os.makedirs(docs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, f"benchmark_{timestamp}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [%(process)d] - %(message)s'))
    logger = logging.getLogger() 
    logger.addHandler(file_handler)
    configure_logging(args)
    logger.info(f"Iniciando benchmark. Log principal: {log_file}")
    
    # --- Verificações e Informações do Sistema --- 
    if not os.path.exists(args.path_zip):
        print_error(f"Erro: Diretório de arquivos ZIP não encontrado: {args.path_zip}")
        logger.error(f"Diretório de arquivos ZIP não encontrado: {args.path_zip}")
        return
    try:
        info_sistema = InfoSistema.coletar_informacoes()
        InfoSistema.imprimir_informacoes(info_sistema)
        logger.info(f"Informações do sistema coletadas: {info_sistema}")
    except Exception as e:
        logger.error(f"Erro ao coletar informações do sistema: {str(e)}")

    # --- Execução do Benchmark --- 
    pandas_executado = False
    dask_executado = False
    polars_executado = False
    
    try:
        # Inicializar a instância principal do benchmark
        # Passar executar_limpeza=False aqui, pois a limpeza será controlada pelos métodos
        benchmark = BenchmarkEmpresa(
            path_zip=args.path_zip, 
            path_base=args.path_base, 
            arquivo_zip_especifico=args.arquivo_zip,
            path_parquet_destino=args.parquet_destino,
            executar_limpeza=False # Limpeza manual dentro dos métodos ou no início
        )
        
        # Determinar quais benchmarks rodar
        run_pandas = args.pandas or args.completo
        run_dask = args.dask or args.completo
        run_polars = args.polars or args.completo
        
        if not (run_pandas or run_dask or run_polars):
             print_warning("Nenhum método de benchmark selecionado explicitamente ou via --completo. Executando Pandas por padrão.")
             run_pandas = True
             
        # --- Lógica de Extração Centralizada (para Dask e Polars) --- 
        num_dask_polars = sum([run_dask, run_polars])
        if num_dask_polars > 1:
            print_section("Extração Centralizada (para Dask/Polars)")
            logger.info("Executando extração centralizada para Dask e Polars.")
            extracao_central_ok, tempo_ext_central = benchmark._extrair_arquivos_zip_paralelo(args.workers)
            if extracao_central_ok:
                benchmark.extrair_individualmente = False # Sinaliza para Dask/Polars não extraírem
                logger.info(f"Extração centralizada concluída em {tempo_ext_central:.2f}s.")
            else:
                print_error("Falha na extração centralizada. Dask e Polars podem falhar.")
                logger.error("Falha na extração centralizada. Dask e Polars podem falhar.")
                # Não setar extrair_individualmente = False, permite que tentem individualmente
        
        # Executar os benchmarks selecionados
        try:
            if run_pandas:
                 benchmark.executar_benchmark_pandas(max_workers=args.workers)
                 pandas_executado = True
            
            if run_dask:
                benchmark.executar_benchmark_dask(max_workers=args.workers)
                dask_executado = True
            
            if run_polars: 
                 benchmark.executar_benchmark_polars(max_workers=args.workers)
                 polars_executado = True
            
            # --- Limpeza Final da Pasta Geral (se extração centralizada foi usada) --- 
            if not benchmark.extrair_individualmente: # Se a extração foi central
                 print("\nLimpando pasta de extração geral (pós benchmarks)...")
                 try:
                     if os.path.exists(benchmark.path_unzip_geral):
                         shutil.rmtree(benchmark.path_unzip_geral)
                         print(f"Diretório {benchmark.path_unzip_geral} removido.")
                         logger.info(f"Diretório de extração geral limpo no final: {benchmark.path_unzip_geral}")
                 except Exception as e_clean_final:
                     print_warning(f"Falha ao limpar diretório de extração geral no final: {str(e_clean_final)}")
                     logger.warning(f"Falha ao limpar {benchmark.path_unzip_geral} no final: {str(e_clean_final)}")
            
            # Gerar gráficos e relatórios
            metodos_executados = [m for m, flag in [('pandas', pandas_executado), ('dask', dask_executado), ('polars', polars_executado)] if flag]
            if (args.graficos or args.completo) and len(metodos_executados) >= 2:
                # ... (lógica de gráficos igual)
                print_section("Gerando Gráficos Comparativos")
                try:
                    graficos_criados = benchmark.gerar_graficos()
                    if not graficos_criados.get('comparacao', False):
                        print_warning("Não foi possível gerar o gráfico de comparação principal.")
                    else:
                        print_success("Gráficos gerados com sucesso.")
                except Exception as e_graph:
                    print_error(f"Erro ao gerar gráficos: {e_graph}")
                    logger.exception("Erro na geração de gráficos")
            elif (args.graficos or args.completo):
                 print_warning("Gráficos comparativos requerem a execução de pelo menos dois métodos.")
            if len(metodos_executados) >= 2:
                # ... (lógica de relatório igual)
                print_section("Relatório Final Comparativo")
                benchmark.imprimir_relatorio()
                md_path = benchmark.gerar_relatorio_markdown(timestamp)
                print_success(f"Relatório Markdown gerado: {md_path}")
            elif len(metodos_executados) == 1:
                 # ... (lógica de relatório igual)
                 print_section(f"Relatório Final - {metodos_executados[0].upper()}")
                 metodo = metodos_executados[0]
                 print(f"Tempo Total: {benchmark.formatar_tempo(benchmark.resultados[metodo]['tempo_total'])}")
                 print(f"Memória Pico: {benchmark.resultados[metodo]['memoria_pico']:.1f}%" if benchmark.resultados[metodo]['memoria_pico'] else "Memória Pico: N/A")
                 print(f"Espaço em Disco: {benchmark.resultados[metodo]['espaco_disco']:.1f} MB")
            else:
                 print_warning("Nenhum método de benchmark foi executado com sucesso.")
        except Exception as e_bench:
            print_error(f"Erro durante a execução do benchmark: {e_bench}")
            logger.exception("Erro durante a execução do benchmark")
    except ValueError as ve:
        print_error(f"Erro de inicialização: {ve}")
        logger.error(f"Erro de inicialização: {ve}")
    except Exception as e_init:
        print_error(f"Erro fatal ao inicializar o benchmark: {e_init}")
        logger.exception("Erro fatal na inicialização do benchmark")

    logger.info("Benchmark finalizado.")

if __name__ == "__main__":
    main()
