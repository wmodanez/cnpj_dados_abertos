# benchmark_completo.py
"""
Benchmark para comparar processamento de dados do Simples Nacional usando Pandas e Dask.

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
python benchmark_simples.py --completo --path_zip dados-abertos-zip --parquet_destino parquet/2025-03/simples

# Benchmark com arquivo específico:
python benchmark_simples.py --completo --path_zip dados-abertos-zip --arquivo_zip Simples.zip --parquet_destino parquet/2025-03/simples
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
from datetime import datetime
from typing import Dict, List, Any

import dask.dataframe as dd
import dask
from dask import delayed
from dask.distributed import as_completed, Client, LocalCluster

# Importações internas do projeto
from src.config import config
from src.process.simples import process_simples, process_single_zip
from src.utils.dask_manager import DaskManager

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

# Importar as funções do arquivo simples.py
from src.process.simples import (
    process_single_zip_pandas,   # Versão Pandas
)

# Configurando logging
logging.basicConfig(
    level=logging.INFO,  # Nível padrão é INFO
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Apenas console logging inicialmente
    ]
)
logger = logging.getLogger("benchmark")

# Definir nível de log mais alto para módulos específicos para reduzir verbosidade
logging.getLogger("src.process.simples").setLevel(logging.INFO)

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
        """Imprime as informações do sistema de forma organizada."""
        print("\n" + "="*60)
        print(" "*20 + "INFORMAÇÕES DO SISTEMA")
        print("="*60)
        
        print(f"Sistema: {info['sistema']} {info['versao_sistema']} ({info['arquitetura']})")
        print(f"Processador: {info['processador']}")
        print(f"Cores: {info['cores_fisicos']} físicos, {info['cores_logicos']} lógicos")
        print(f"Frequência: {info['frequencia_mhz']} MHz")
        print(f"Memória: {info['memoria_total']} (Disponível: {info['memoria_disponivel']})")
        print(f"Disco: {info['disco_total']} (Livre: {info['disco_livre']})")
        print(f"GPU: {info['gpu']}")
        print(f"Memória GPU: {info['memoria_gpu']}")
        print("="*60 + "\n")

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
        logging.getLogger("src.process.simples").setLevel(logging.DEBUG)
        logger.debug("Modo verboso ativado: logs detalhados serão exibidos")
    elif hasattr(args, 'quiet') and args.quiet:
        logger.setLevel(logging.WARNING)
        logging.getLogger("src.process.simples").setLevel(logging.WARNING)
        logger.warning("Modo silencioso ativado: apenas avisos e erros serão exibidos")
    else:
        # Nível padrão é INFO, mas reduzir detalhes específicos
        logger.setLevel(logging.INFO)
        # Ajustar nível de log para módulos específicos para reduzir verbosidade
        logging.getLogger("src.process.simples").setLevel(logging.INFO)

class BenchmarkSimples:
    def __init__(self, path_zip, path_base, arquivo_zip_especifico=None, path_parquet_destino=None, executar_limpeza=True):
        """
        Inicializa o benchmark.
        
        Args:
            path_zip: Caminho para o diretório com os arquivos ZIP
            path_base: Caminho base para criar diretórios temporários
            arquivo_zip_especifico: Nome de um arquivo ZIP específico para teste (opcional)
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
        else:
            self.path_parquet_pandas = os.path.join(path_base, "parquet_pandas")
            self.path_parquet_dask = os.path.join(path_base, "parquet_dask")
        
        # Criar diretórios para os testes
        self.path_unzip_pandas = os.path.join(path_base, "unzip_pandas")
        self.path_unzip_dask = os.path.join(path_base, "unzip_dask")
        
        # Criar diretórios se não existirem
        for path in [self.path_unzip_pandas, self.path_unzip_dask, 
                     self.path_parquet_pandas, self.path_parquet_dask]:
            os.makedirs(path, exist_ok=True)
        
        # Identificar arquivos ZIP do Simples Nacional
        if arquivo_zip_especifico:
            if os.path.exists(os.path.join(path_zip, arquivo_zip_especifico)):
                self.zip_files = [arquivo_zip_especifico]
            else:
                raise ValueError(f"Arquivo ZIP específico '{arquivo_zip_especifico}' não encontrado em '{path_zip}'")
        else:
            self.zip_files = [f for f in os.listdir(path_zip) 
                             if f.startswith('Simples') and f.endswith('.zip')]
        
        if not self.zip_files:
            raise ValueError("Nenhum arquivo ZIP do Simples Nacional encontrado.")
            
        logger.info(f"Encontrados {len(self.zip_files)} arquivos ZIP para benchmark")
        
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
                info[zip_file] = {
                    'tamanho_mb': os.path.getsize(zip_path) / (1024 * 1024),
                    'data_modificacao': datetime.fromtimestamp(os.path.getmtime(zip_path)).strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Verificar conteúdo do ZIP
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        arquivos = zip_ref.namelist()
                        csv_files = [f for f in arquivos if f.endswith('.CSV')]
                        info[zip_file]['num_arquivos'] = len(arquivos)
                        info[zip_file]['num_csv'] = len(csv_files)
                except Exception as e:
                    logger.error(f"Erro ao analisar ZIP {zip_file}: {str(e)}")
                    
        return info
    
    def limpar_diretorios(self, preservar_parquet=True):
        """Limpa os diretórios temporários.
        
        Args:
            preservar_parquet: Se True, não limpa os diretórios de parquet.
        """
        if not self.executar_limpeza:
            return
            
        # Sempre limpar os diretórios de extração temporária
        diretorios_extracao = [self.path_unzip_pandas, self.path_unzip_dask]
        for path in diretorios_extracao:
            if os.path.exists(path):
                for item in os.listdir(path):
                    item_path = os.path.join(path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
        
        # Limpar diretórios de parquet apenas se não estiver preservando
        if not preservar_parquet:
            diretorios_parquet = [self.path_parquet_pandas, self.path_parquet_dask]
            for path in diretorios_parquet:
                if os.path.exists(path):
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
    
    def executar_benchmark_pandas(self):
        """Executa o benchmark usando Pandas."""
        logger.info("\n" + "="*60)
        logger.info(" "*20 + "BENCHMARK COM PANDAS")
        logger.info("="*60)
        
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
            logger.info(f"Processando {zip_file} com Pandas...")
            
            try:
                # Medir CPU e memória durante o processamento
                cpu_medidas.append(psutil.cpu_percent(interval=0.1))
                memoria_medidas.append(psutil.virtual_memory().percent)
                
                # Medir o tempo de processamento
                tempo_arquivo_inicio = time.time()
                
                # Usar diretamente a função process_single_zip_pandas do simples.py
                resultado = process_single_zip_pandas(
                    zip_file=zip_file, 
                    path_zip=self.path_zip, 
                    path_unzip=self.path_unzip_pandas, 
                    path_parquet=self.path_parquet_pandas
                )
                
                # Atualizar tempo de processamento
                tempo_processamento_arquivo = time.time() - tempo_arquivo_inicio
                tempo_processamento += tempo_processamento_arquivo
                
                if resultado:
                    logger.info(f"Arquivo {zip_file} processado com sucesso em {tempo_processamento_arquivo:.2f} segundos")
                else:
                    logger.warning(f"Falha no processamento do arquivo {zip_file}")
                
            except Exception as e:
                logger.error(f"Erro no processamento com Pandas: {str(e)}")
                logger.error(traceback.format_exc())
        
        # Calcular o tempo total de execução
        tempo_total = time.time() - tempo_inicio_total
        
        # Coletar resultados
        self.resultados['pandas']['tempo_total'] = tempo_total
        self.resultados['pandas']['tempo_extracao'] = tempo_extracao
        self.resultados['pandas']['tempo_processamento'] = tempo_processamento
        self.resultados['pandas']['memoria_pico'] = max(memoria_medidas) if memoria_medidas else 0
        self.resultados['pandas']['memoria_media'] = sum(memoria_medidas) / len(memoria_medidas) if memoria_medidas else 0
        self.resultados['pandas']['cpu_medio'] = sum(cpu_medidas) / len(cpu_medidas) if cpu_medidas else 0
        self.resultados['pandas']['cpu_pico'] = max(cpu_medidas) if cpu_medidas else 0
        self.resultados['pandas']['espaco_disco'] = self.calcular_tamanho_diretorio(self.path_parquet_pandas)
        self.resultados['pandas']['num_arquivos'] = self.contar_arquivos(self.path_parquet_pandas)
        self.resultados['pandas']['arquivos_parquet'] = self.listar_arquivos_parquet(self.path_parquet_pandas)
        
        # Calcular taxa de compressão
        tamanho_original = sum([info['tamanho_mb'] for info in self.info_arquivo.values()])
        if tamanho_original > 0 and self.resultados['pandas']['espaco_disco'] > 0:
            self.resultados['pandas']['compressao_taxa'] = self.calcular_taxa_compressao(
                tamanho_original, self.resultados['pandas']['espaco_disco'])
        else:
            self.resultados['pandas']['compressao_taxa'] = 0
            logger.warning("Não foi possível calcular taxa de compressão (tamanho original ou disco = 0)")
        
        logger.info(f"Benchmark com Pandas concluído em {tempo_total:.2f} segundos")
        
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
        
        logger.info('\n' + '=' * 60)
        logger.info(' ' * 20 + 'BENCHMARK COM DASK')
        logger.info('=' * 60)
        
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
            dask_manager = DaskManager.initialize(
                n_workers=config.dask.n_workers,
                memory_limit=config.dask.memory_limit,
                dashboard_address=config.dask.dashboard_address
            )
            client = dask_manager.client
            logger.info(f"Cliente Dask inicializado: {client}")
        except Exception as e:
            logger.error(f"Erro ao inicializar cliente Dask: {str(e)}")
            logger.error(traceback.format_exc())
            self.resultados['dask']['tempo_total'] = time.time() - tempo_inicio_total
            return self.resultados['dask']
        
        try:
            sucessos = 0
            total_arquivos = len(self.zip_files)
            
            # Executar o processamento com o método Dask para cada arquivo
            for i, zip_file in enumerate(self.zip_files):
                logger.info(f"Processando {zip_file} com Dask ({i+1}/{total_arquivos})...")
                
                # Abordagem simplificada: processar cada arquivo diretamente
                for tentativa in range(3):  # Até 3 tentativas por arquivo
                    if tentativa > 0:
                        logger.info(f"Tentativa {tentativa+1} para o arquivo {zip_file}")
                    
                    try:
                        # Medir CPU e memória durante o processamento
                        cpu_medidas.append(psutil.cpu_percent(interval=0.1))
                        memoria_medidas.append(psutil.virtual_memory().percent)
                        
                        # Processar diretamente com Dask, sem client.submit
                        tempo_inicio_processamento = time.time()
                        logger.info(f"Processando arquivo {zip_file} com Dask diretamente...")
                        
                        # Chamar a função diretamente
                        resultado = process_single_zip(
                            zip_file=zip_file,
                            path_zip=self.path_zip, 
                            path_unzip=self.path_unzip_dask, 
                            path_parquet=self.path_parquet_dask
                        )
                        
                        # Verificar se o resultado é um objeto Delayed
                        if hasattr(resultado, 'compute') and callable(getattr(resultado, 'compute')):
                            logger.info(f"Detectado objeto Delayed, computando resultado para {zip_file}...")
                            try:
                                # Computar o objeto Delayed
                                resultado = resultado.compute()
                                logger.info(f"Computação finalizada para {zip_file}")
                            except Exception as e:
                                logger.error(f"Erro ao computar objeto Delayed para {zip_file}: {str(e)}")
                                logger.error(traceback.format_exc())
                                resultado = False
                        
                        # Calcular tempo de processamento
                        tempo_processamento_arquivo = time.time() - tempo_inicio_processamento
                        tempo_processamento += tempo_processamento_arquivo
                        
                        logger.info(f"Arquivo {zip_file} processado em {tempo_processamento_arquivo:.2f} segundos, resultado: {resultado}")
                        
                        if resultado is True:  # Verificar explicitamente se o resultado é True
                            logger.info(f"Arquivo {zip_file} processado com sucesso!")
                            sucessos += 1
                            break  # Saia do loop de tentativas
                        else:
                            logger.warning(f"Arquivo {zip_file} processado mas retornou {resultado}")
                                
                    except Exception as e:
                        logger.error(f"Erro ao processar {zip_file}: {str(e)}")
                        logger.error(traceback.format_exc())
                    
                    # Esperar brevemente antes de tentar novamente
                    time.sleep(2)
                
                # Forçar uma limpeza de memória entre arquivos
                gc.collect()
            
            # Resultado final
            logger.info(f"Concluído processamento Dask: {sucessos} de {total_arquivos} arquivos processados com sucesso")
        
        except Exception as e:
            logger.error(f"Erro no processamento com Dask: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            # Calcular o tempo total de execução
            tempo_total = time.time() - tempo_inicio_total
            
            # Atualizar tempos parciais nos resultados
            self.resultados['dask']['tempo_total'] = tempo_total
            self.resultados['dask']['tempo_extracao'] = tempo_extracao
            self.resultados['dask']['tempo_processamento'] = tempo_processamento
            
            # Encerrar o cliente Dask
            try:
                dask_manager.shutdown()
                logger.info("Cliente Dask encerrado")
            except Exception as e:
                logger.error(f"Erro ao encerrar cliente Dask: {str(e)}")
        
        # Coletar resultados
        self.resultados['dask']['memoria_pico'] = max(memoria_medidas) if memoria_medidas else 0
        self.resultados['dask']['memoria_media'] = sum(memoria_medidas) / len(memoria_medidas) if memoria_medidas else 0
        self.resultados['dask']['cpu_medio'] = sum(cpu_medidas) / len(cpu_medidas) if cpu_medidas else 0
        self.resultados['dask']['cpu_pico'] = max(cpu_medidas) if cpu_medidas else 0
        self.resultados['dask']['espaco_disco'] = self.calcular_tamanho_diretorio(self.path_parquet_dask)
        self.resultados['dask']['num_arquivos'] = self.contar_arquivos(self.path_parquet_dask)
        self.resultados['dask']['arquivos_parquet'] = self.listar_arquivos_parquet(self.path_parquet_dask)
        
        # Calcular taxa de compressão
        tamanho_original = sum([info['tamanho_mb'] for info in self.info_arquivo.values()])
        if tamanho_original > 0 and self.resultados['dask']['espaco_disco'] > 0:
            self.resultados['dask']['compressao_taxa'] = self.calcular_taxa_compressao(
                tamanho_original, self.resultados['dask']['espaco_disco'])
        else:
            self.resultados['dask']['compressao_taxa'] = 0
            logger.warning("Não foi possível calcular taxa de compressão (tamanho original ou disco = 0)")
        
        logger.info(f"Benchmark com Dask concluído em {tempo_total:.2f} segundos")
        
        # Forçar limpeza de memória
        gc.collect()
        
        return self.resultados['dask']
    
    def comparar_resultados(self):
        """Compara os resultados dos benchmarks e retorna o melhor método."""
        pandas_results = self.resultados['pandas']
        dask_results = self.resultados['dask']
        
        # Verificar se ambos os métodos geraram resultados válidos (arquivos parquet)
        pandas_valido = pandas_results['num_arquivos'] > 0
        dask_valido = dask_results['num_arquivos'] > 0
        
        # Se apenas um método gerou resultados válidos, ele é automaticamente o melhor
        if pandas_valido and not dask_valido:
            return {
                'comparacao': {'processamento_sucesso': {'melhor': 'pandas', 'diferenca_percentual': 100}},
                'contagem': {'pandas': 1, 'dask': 0},
                'melhor_metodo': 'pandas'
            }
        elif dask_valido and not pandas_valido:
            return {
                'comparacao': {'processamento_sucesso': {'melhor': 'dask', 'diferenca_percentual': 100}},
                'contagem': {'pandas': 0, 'dask': 1},
                'melhor_metodo': 'dask'
            }
        elif not pandas_valido and not dask_valido:
            return {
                'comparacao': {'sem_dados': {'melhor': 'indeterminado', 'diferenca_percentual': 0}},
                'contagem': {'pandas': 0, 'dask': 0},
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
            if metrica in pandas_results and metrica in dask_results:
                # Se ambos são zero, não há diferença significativa
                if pandas_results[metrica] == 0 and dask_results[metrica] == 0:
                    continue
                
                # Determinar qual é melhor (menor valor é melhor, exceto para taxa de compressão)
                if metrica == 'compressao_taxa':
                    # Para taxa de compressão, maior é melhor
                    melhor = 'pandas' if pandas_results[metrica] > dask_results[metrica] else 'dask'
                else:
                    # Para outras métricas, menor é melhor
                    melhor = 'pandas' if pandas_results[metrica] < dask_results[metrica] else 'dask'
                    
                    # Nunca considerar um valor zero como melhor quando o outro tem resultado válido
                    if melhor == 'dask' and dask_results[metrica] == 0 and pandas_results[metrica] > 0:
                        melhor = 'pandas'
                    elif melhor == 'pandas' and pandas_results[metrica] == 0 and dask_results[metrica] > 0:
                        melhor = 'dask'
                
                # Calcular diferença percentual
                diferenca = calcular_diferenca_percentual(pandas_results[metrica], dask_results[metrica])
                
                # Adicionar à comparação
                comparacao[metrica] = {
                    'melhor': melhor,
                    'diferenca_percentual': diferenca
                }
        
        # Se não houver critérios para comparação, retorne um resultado padrão
        if not comparacao:
            return {
                'comparacao': {'sem_dados': {'melhor': 'indeterminado', 'diferenca_percentual': 0}},
                'contagem': {'pandas': 0, 'dask': 0},
                'melhor_metodo': 'indeterminado'
            }
        
        # Contar qual método ganhou em mais critérios
        contagem = {'pandas': 0, 'dask': 0}
        for criterio, resultado in comparacao.items():
            contagem[resultado['melhor']] += 1
        
        # Determinar o melhor método
        melhor_metodo = 'pandas' if contagem['pandas'] >= contagem['dask'] else 'dask'
        
        return {
            'comparacao': comparacao,
            'contagem': contagem,
            'melhor_metodo': melhor_metodo
        }
    
    def gerar_graficos(self):
        """Gera gráficos comparativos dos resultados."""
        plt.figure(figsize=(15, 10))
        
        # Dados para os gráficos
        metricas = ['tempo_total', 'memoria_pico', 'cpu_medio', 'espaco_disco']
        valores_pandas = [self.resultados['pandas'][m] for m in metricas]
        valores_dask = [self.resultados['dask'][m] for m in metricas]
        
        # Criar subplots
        for i, metrica in enumerate(metricas):
            plt.subplot(2, 2, i+1)
            barras = plt.bar(['Pandas', 'Dask'], [self.resultados['pandas'][metrica], self.resultados['dask'][metrica]], 
                             color=['#1f77b4', '#ff7f0e'])  # Cores azul e laranja
            plt.title(f'{metrica.replace("_", " ").title()}')
            plt.ylabel('Valor')
            
            # Adicionar valores nas barras
            for barra in barras:
                altura = barra.get_height()
                plt.text(barra.get_x() + barra.get_width()/2., altura,
                        f'{altura:.2f}',
                        ha='center', va='bottom')
        
        plt.tight_layout()
        docs_dir = os.path.join(self.path_base, 'docs')
        grafico_path = os.path.join(docs_dir, 'benchmark_comparacao.png')
        plt.savefig(grafico_path)
        logger.info(f"Gráfico comparativo salvo como '{grafico_path}'")
        
        # Gráfico de tempo detalhado para Pandas
        if self.resultados['pandas']['tempo_extracao'] > 0:
            plt.figure(figsize=(10, 6))
            tempos = ['tempo_extracao', 'tempo_processamento']
            valores = [self.resultados['pandas'][t] for t in tempos]
            plt.pie(valores, labels=[t.replace('tempo_', '').title() for t in tempos], 
                    autopct='%1.1f%%', colors=['#2ca02c', '#d62728'])  # Verde e vermelho
            plt.title('Distribuição do Tempo de Processamento (Pandas)')
            grafico_tempo_path = os.path.join(docs_dir, 'benchmark_tempo_pandas.png')
            plt.savefig(grafico_tempo_path)
            logger.info(f"Gráfico de tempo para Pandas salvo como '{grafico_tempo_path}'")
        
        # Gráfico de tempo detalhado para Dask
        if self.resultados['dask']['tempo_extracao'] > 0:
            plt.figure(figsize=(10, 6))
            tempos = ['tempo_extracao', 'tempo_processamento']
            valores = [self.resultados['dask'][t] for t in tempos]
            plt.pie(valores, labels=[t.replace('tempo_', '').title() for t in tempos], 
                    autopct='%1.1f%%', colors=['#9467bd', '#8c564b'])  # Roxo e marrom
            plt.title('Distribuição do Tempo de Processamento (Dask)')
            grafico_tempo_path = os.path.join(docs_dir, 'benchmark_tempo_dask.png')
            plt.savefig(grafico_tempo_path)
            logger.info(f"Gráfico de tempo para Dask salvo como '{grafico_tempo_path}'")
    
    def imprimir_relatorio(self):
        """Imprime um relatório detalhado com os resultados do benchmark."""
        comparacao = self.comparar_resultados()
        
        print("\n" + "="*80)
        print(" "*30 + "RELATÓRIO DE BENCHMARK")
        print("="*80)
        
        # Informações dos arquivos originais
        print("\nARQUIVOS ORIGINAIS:")
        for zip_file, info in self.info_arquivo.items():
            print(f"  - {zip_file}: {info['tamanho_mb']:.2f} MB, {info['num_csv']} arquivos CSV")
        
        # Resultados do Pandas
        print("\nRESULTADOS PANDAS:")
        print(f"  - Tempo Total: {self.formatar_tempo(self.resultados['pandas']['tempo_total'])}")
        if self.resultados['pandas']['tempo_extracao'] > 0:
            extracao_tempo = self.formatar_tempo(self.resultados['pandas']['tempo_extracao'])
            processamento_tempo = self.formatar_tempo(self.resultados['pandas']['tempo_processamento'])
            print(f"    - Extração: {extracao_tempo} ({self.resultados['pandas']['tempo_extracao']/self.resultados['pandas']['tempo_total']*100:.1f}%)")
            print(f"    - Processamento: {processamento_tempo} ({self.resultados['pandas']['tempo_processamento']/self.resultados['pandas']['tempo_total']*100:.1f}%)")
        print(f"  - Memória: {self.resultados['pandas']['memoria_pico']:.2f}% (pico), {self.resultados['pandas']['memoria_media']:.2f}% (média)")
        print(f"  - CPU: {self.resultados['pandas']['cpu_pico']:.2f}% (pico), {self.resultados['pandas']['cpu_medio']:.2f}% (média)")
        print(f"  - Espaço em Disco: {self.resultados['pandas']['espaco_disco']:.2f} MB")
        print(f"  - Número de Arquivos: {self.resultados['pandas']['num_arquivos']}")
        print(f"  - Taxa de Compressão: {self.resultados['pandas']['compressao_taxa']:.2f}%")
        
        # Resultados do Dask
        print("\nRESULTADOS DASK:")
        print(f"  - Tempo Total: {self.formatar_tempo(self.resultados['dask']['tempo_total'])}")
        if self.resultados['dask']['tempo_extracao'] > 0:
            extracao_tempo = self.formatar_tempo(self.resultados['dask']['tempo_extracao'])
            processamento_tempo = self.formatar_tempo(self.resultados['dask']['tempo_processamento'])
            print(f"    - Extração: {extracao_tempo} ({self.resultados['dask']['tempo_extracao']/self.resultados['dask']['tempo_total']*100:.1f}%)")
            print(f"    - Processamento: {processamento_tempo} ({self.resultados['dask']['tempo_processamento']/self.resultados['dask']['tempo_total']*100:.1f}%)")
        print(f"  - Memória: {self.resultados['dask']['memoria_pico']:.2f}% (pico), {self.resultados['dask']['memoria_media']:.2f}% (média)")
        print(f"  - CPU: {self.resultados['dask']['cpu_pico']:.2f}% (pico), {self.resultados['dask']['cpu_medio']:.2f}% (média)")
        print(f"  - Espaço em Disco: {self.resultados['dask']['espaco_disco']:.2f} MB")
        print(f"  - Número de Arquivos: {self.resultados['dask']['num_arquivos']}")
        print(f"  - Taxa de Compressão: {self.resultados['dask']['compressao_taxa']:.2f}%")
        
        # Comparação
        print("\nCOMPARAÇÃO:")
        for criterio, resultado in comparacao['comparacao'].items():
            print(f"  - {criterio.replace('_', ' ').title()}: {resultado['melhor'].upper()} é melhor (diferença de {resultado['diferenca_percentual']:.2f}%)")
        
        # Conclusão
        print("\nCONCLUSÃO:")
        print(f"  - {comparacao['melhor_metodo'].upper()} é o método mais adequado para esta máquina e conjunto de dados.")
        print(f"  - Venceu em {comparacao['contagem'][comparacao['melhor_metodo']]} de {len(comparacao['comparacao'])} critérios avaliados.")
        
        # Recomendação final
        print("\nRECOMENDAÇÃO:")
        if comparacao['melhor_metodo'] == 'pandas':
            print("  Utilize o método PANDAS para processar os dados do Simples Nacional.")
            vantagens = []
            if 'tempo_total' in comparacao['comparacao'] and comparacao['comparacao']['tempo_total']['melhor'] == 'pandas':
                vantagens.append("menor tempo de execução")
            if 'memoria_pico' in comparacao['comparacao'] and comparacao['comparacao']['memoria_pico']['melhor'] == 'pandas':
                vantagens.append("menor uso de memória")
            if 'cpu_medio' in comparacao['comparacao'] and comparacao['comparacao']['cpu_medio']['melhor'] == 'pandas':
                vantagens.append("menor uso de CPU")
            if 'espaco_disco' in comparacao['comparacao'] and comparacao['comparacao']['espaco_disco']['melhor'] == 'pandas':
                vantagens.append("menor espaço em disco")
            if 'compressao_taxa' in comparacao['comparacao'] and comparacao['comparacao']['compressao_taxa']['melhor'] == 'pandas':
                vantagens.append("melhor taxa de compressão")
            if 'processamento_sucesso' in comparacao['comparacao'] and comparacao['comparacao']['processamento_sucesso']['melhor'] == 'pandas':
                vantagens.append("processamento bem-sucedido")
            
            if vantagens:
                print(f"  Vantagens principais: {', '.join(vantagens)}")
        else:
            print("  Utilize o método DASK para processar os dados do Simples Nacional.")
            vantagens = []
            if 'tempo_total' in comparacao['comparacao'] and comparacao['comparacao']['tempo_total']['melhor'] == 'dask':
                vantagens.append("menor tempo de execução")
            if 'memoria_pico' in comparacao['comparacao'] and comparacao['comparacao']['memoria_pico']['melhor'] == 'dask':
                vantagens.append("menor uso de memória")
            if 'cpu_medio' in comparacao['comparacao'] and comparacao['comparacao']['cpu_medio']['melhor'] == 'dask':
                vantagens.append("menor uso de CPU")
            if 'espaco_disco' in comparacao['comparacao'] and comparacao['comparacao']['espaco_disco']['melhor'] == 'dask':
                vantagens.append("menor espaço em disco")
            if 'compressao_taxa' in comparacao['comparacao'] and comparacao['comparacao']['compressao_taxa']['melhor'] == 'dask':
                vantagens.append("melhor taxa de compressão")
            if 'processamento_sucesso' in comparacao['comparacao'] and comparacao['comparacao']['processamento_sucesso']['melhor'] == 'dask':
                vantagens.append("processamento bem-sucedido")
            
            if vantagens:
                print(f"  Vantagens principais: {', '.join(vantagens)}")
            
        print("="*80)

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
        relatorio += f"- **Número de Arquivos:** {self.resultados['pandas']['num_arquivos']}\n"
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
        relatorio += f"- **Número de Arquivos:** {self.resultados['dask']['num_arquivos']}\n"
        relatorio += f"- **Taxa de Compressão:** {self.resultados['dask']['compressao_taxa']:.2f}%\n\n"
        
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
        
        # Adicionar gráficos
        relatorio += "## Gráficos\n\n"
        relatorio += "### Gráfico de Comparação\n\n"
        relatorio += "![Gráfico de Comparação](benchmark_comparacao.png)\n\n"
        
        # Salvar o relatório
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(relatorio)
        
        logger.info(f"Relatório completo gerado em formato Markdown: {md_path}")
        return md_path

def main():
    """Função principal."""
    # Adicionar timestamp no início da execução
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Configuração do parser de argumentos
    parser = argparse.ArgumentParser(description='Benchmark para comparar processamento com Pandas e Dask')
    parser.add_argument('--path_zip', type=str, default='dados-abertos-zip', 
                        help='Caminho para o diretório com os arquivos ZIP')
    parser.add_argument('--arquivo_zip', type=str, 
                        help='Arquivo ZIP específico para análise (opcional, se não informado usa todos os arquivos do diretório)')
    parser.add_argument('--path_base', type=str, default='benchmark_temp', 
                        help='Caminho base para criar diretórios temporários')
    parser.add_argument('--parquet_destino', type=str, default='parquet',
                        help='Caminho relativo a path_base para os arquivos parquet gerados')
    parser.add_argument('--limpar', action='store_true', 
                        help='Limpar diretórios temporários antes e depois do benchmark')
    parser.add_argument('--pandas', action='store_true', 
                        help='Executar apenas o benchmark com Pandas')
    parser.add_argument('--dask', action='store_true', 
                        help='Executar apenas o benchmark com Dask')
    parser.add_argument('--graficos', action='store_true', 
                        help='Gerar gráficos comparativos')
    parser.add_argument('--completo', action='store_true',
                        help='Executar ambos os benchmarks (Pandas e Dask) e gerar gráficos comparativos')
    
    # Adicionar argumentos para controle de log
    add_log_level_argument(parser)
    
    args = parser.parse_args()
    
    # Criar diretório base se não existir
    os.makedirs(args.path_base, exist_ok=True)
    
    # Criar diretório para logs
    logs_dir = os.path.join(args.path_base, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    # Criar diretório para documentação (relatórios e gráficos)
    docs_dir = os.path.join(args.path_base, "docs")
    os.makedirs(docs_dir, exist_ok=True)
    
    # Atualizar o caminho do arquivo de log com o timestamp
    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
    
    log_file = os.path.join(logs_dir, f"benchmark_simples_{timestamp}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    
    # Configurar logging com base nos argumentos
    configure_logging(args)
    
    logger.info(f"Iniciando benchmark. Log salvo em {log_file}")
    
    # Verificar se os diretórios existem
    if not os.path.exists(args.path_zip):
        print(f"Diretório {args.path_zip} não encontrado.")
        return
    
    # Verificar se o arquivo ZIP específico existe, caso tenha sido informado
    if args.arquivo_zip:
        arquivo_zip_path = os.path.join(args.path_zip, args.arquivo_zip)
        if not os.path.exists(arquivo_zip_path):
            print(f"Arquivo ZIP {arquivo_zip_path} não encontrado.")
            return
    
    # Coletar informações do sistema
    try:
        info_sistema = InfoSistema.coletar_informacoes()
        InfoSistema.imprimir_informacoes(info_sistema)
    except Exception as e:
        logger.error(f"Erro ao coletar informações do sistema: {str(e)}")
    
    # Flags para controlar quais métodos foram executados
    pandas_executado = False
    dask_executado = False
    
    # Inicializar o benchmark
    try:
        # Modificar para usar apenas o arquivo específico, se informado
        benchmark = BenchmarkSimples(
            path_zip=args.path_zip, 
            path_base=args.path_base, 
            arquivo_zip_especifico=args.arquivo_zip,
            path_parquet_destino=args.parquet_destino,
            executar_limpeza=args.limpar
        )
        
        # Executar os benchmarks
        try:
            if not args.dask or args.pandas:  # Se --dask não for especificado ou --pandas for
                benchmark.executar_benchmark_pandas()
                pandas_executado = True
                
            if not args.pandas or args.dask:  # Se --pandas não for especificado ou --dask for
                benchmark.executar_benchmark_dask()
                dask_executado = True
            
            # Gerar gráficos comparativos somente se ambos os métodos foram executados
            if args.graficos and pandas_executado and dask_executado:
                try:
                    benchmark.gerar_graficos()
                except Exception as e:
                    logger.error(f"Erro ao gerar gráficos: {str(e)}")
            elif args.graficos:
                logger.warning("Não é possível gerar gráficos comparativos: ambos os métodos devem ser executados")
            
            try:
                # Imprimir relatório completo somente se ambos os métodos foram executados
                if pandas_executado and dask_executado:
                    logger.info("Gerando relatório comparativo dos métodos Pandas e Dask")
                    
                    # Gerar e imprimir relatório na tela 
                    benchmark.imprimir_relatorio()
                    
                    # Gerar relatório Markdown
                    md_path = benchmark.gerar_relatorio_markdown(timestamp)
                    logger.info(f"Relatório completo gerado em formato Markdown: {md_path}")
                    print(f"\nRelatório completo gerado em formato Markdown: {md_path}")
                else:
                    # Relatórios individuais - usar o mesmo timestamp da inicialização
                    docs_dir = os.path.join(args.path_base, "docs")
                    
                    if pandas_executado:
                        logger.info("Gerando relatório simplificado para o método Pandas")
                        
                        # Imprimir relatório na tela
                        print("\n" + "="*80)
                        print(" "*30 + "RELATÓRIO PANDAS")
                        print("="*80)
                        print(f"\nTempo Total: {benchmark.formatar_tempo(benchmark.resultados['pandas']['tempo_total'])}")
                        print(f"Memória Pico: {benchmark.resultados['pandas']['memoria_pico']:.2f}%")
                        print(f"CPU Médio: {benchmark.resultados['pandas']['cpu_medio']:.2f}%")
                        print(f"Espaço em Disco: {benchmark.resultados['pandas']['espaco_disco']:.2f} MB")
                        print(f"Taxa de Compressão: {benchmark.resultados['pandas']['compressao_taxa']:.2f}%")
                        
                        # Salvar relatório em Markdown
                        md_path = os.path.join(docs_dir, f"relatorio_pandas_{timestamp}.md")
                        with open(md_path, 'w', encoding='utf-8') as f:
                            f.write("# Relatório de Benchmark - Pandas\n\n")
                            f.write(f"*Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
                            f.write("## Resultados\n\n")
                            f.write(f"- **Tempo Total:** {benchmark.formatar_tempo(benchmark.resultados['pandas']['tempo_total'])}\n")
                            if benchmark.resultados['pandas']['tempo_extracao'] > 0:
                                extracao_percentual = benchmark.resultados['pandas']['tempo_extracao']/benchmark.resultados['pandas']['tempo_total']*100
                                processamento_percentual = benchmark.resultados['pandas']['tempo_processamento']/benchmark.resultados['pandas']['tempo_total']*100
                                f.write(f"  - **Extração:** {benchmark.formatar_tempo(benchmark.resultados['pandas']['tempo_extracao'])} ({extracao_percentual:.1f}%)\n")
                                f.write(f"  - **Processamento:** {benchmark.formatar_tempo(benchmark.resultados['pandas']['tempo_processamento'])} ({processamento_percentual:.1f}%)\n")
                            f.write(f"- **Memória (pico):** {benchmark.resultados['pandas']['memoria_pico']:.2f}%\n")
                            f.write(f"- **CPU (média):** {benchmark.resultados['pandas']['cpu_medio']:.2f}%\n")
                            f.write(f"- **Espaço em Disco:** {benchmark.resultados['pandas']['espaco_disco']:.2f} MB\n")
                            f.write(f"- **Taxa de Compressão:** {benchmark.resultados['pandas']['compressao_taxa']:.2f}%\n\n")
                            
                            # Adicionar gráfico de tempo se disponível
                            if benchmark.resultados['pandas']['tempo_extracao'] > 0 and os.path.exists(os.path.join(docs_dir, 'benchmark_tempo_pandas.png')):
                                f.write("## Gráficos\n\n")
                                f.write("### Distribuição de Tempo\n\n")
                                f.write("![Distribuição de Tempo - Pandas](benchmark_tempo_pandas.png)\n")
                        
                        logger.info(f"Relatório Pandas salvo em formato Markdown: {md_path}")
                        print(f"\nRelatório detalhado gerado em formato Markdown: {md_path}")
                        
                        if dask_executado:
                            logger.info("Gerando relatório simplificado para o método Dask")
                            
                            # Imprimir relatório na tela
                            print("\n" + "="*80)
                            print(" "*30 + "RELATÓRIO DASK")
                            print("="*80)
                            print(f"\nTempo Total: {benchmark.formatar_tempo(benchmark.resultados['dask']['tempo_total'])}")
                            print(f"Memória Pico: {benchmark.resultados['dask']['memoria_pico']:.2f}%")
                            print(f"CPU Médio: {benchmark.resultados['dask']['cpu_medio']:.2f}%")
                            print(f"Espaço em Disco: {benchmark.resultados['dask']['espaco_disco']:.2f} MB")
                            print(f"Taxa de Compressão: {benchmark.resultados['dask']['compressao_taxa']:.2f}%")
                            
                            # Salvar relatório em Markdown
                            md_path = os.path.join(docs_dir, f"relatorio_dask_{timestamp}.md")
                            with open(md_path, 'w', encoding='utf-8') as f:
                                f.write("# Relatório de Benchmark - Dask\n\n")
                                f.write(f"*Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
                                f.write("## Resultados\n\n")
                                f.write(f"- **Tempo Total:** {benchmark.formatar_tempo(benchmark.resultados['dask']['tempo_total'])}\n")
                                if benchmark.resultados['dask']['tempo_extracao'] > 0:
                                    extracao_percentual = benchmark.resultados['dask']['tempo_extracao']/benchmark.resultados['dask']['tempo_total']*100
                                    processamento_percentual = benchmark.resultados['dask']['tempo_processamento']/benchmark.resultados['dask']['tempo_total']*100
                                    f.write(f"  - **Extração:** {benchmark.formatar_tempo(benchmark.resultados['dask']['tempo_extracao'])} ({extracao_percentual:.1f}%)\n")
                                    f.write(f"  - **Processamento:** {benchmark.formatar_tempo(benchmark.resultados['dask']['tempo_processamento'])} ({processamento_percentual:.1f}%)\n")
                                f.write(f"- **Memória (pico):** {benchmark.resultados['dask']['memoria_pico']:.2f}%\n")
                                f.write(f"- **CPU (média):** {benchmark.resultados['dask']['cpu_medio']:.2f}%\n")
                                f.write(f"- **Espaço em Disco:** {benchmark.resultados['dask']['espaco_disco']:.2f} MB\n")
                                f.write(f"- **Taxa de Compressão:** {benchmark.resultados['dask']['compressao_taxa']:.2f}%\n\n")
                                
                                # Adicionar gráfico de tempo se disponível
                                if benchmark.resultados['dask']['tempo_extracao'] > 0 and os.path.exists(os.path.join(docs_dir, 'benchmark_tempo_dask.png')):
                                    f.write("## Gráficos\n\n")
                                    f.write("### Distribuição de Tempo\n\n")
                                    f.write("![Distribuição de Tempo - Dask](benchmark_tempo_dask.png)\n")
                            
                            logger.info(f"Relatório Dask salvo em formato Markdown: {md_path}")
                            print(f"\nRelatório detalhado gerado em formato Markdown: {md_path}")
                
            except Exception as e:
                logger.error(f"Erro ao gerar relatório: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
            
            # Limpar diretórios temporários se solicitado
            if args.limpar:
                benchmark.limpar_diretorios(preservar_parquet=True)  # Preservar diretórios de parquet
                
        except Exception as e:
            logger.error(f"Erro durante o benchmark: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
    except Exception as e:
        logger.error(f"Erro ao inicializar benchmark: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

# Exemplos de como executar o script:
# 
# Para benchmark completo (ambos os métodos + gráficos) usando todos os ZIPs do diretório:
# python benchmark_simples.py --completo --path_zip dados-abertos-zip --path_base benchmark_temp --parquet_destino parquet/2025-03/simples
#
# Para benchmark apenas com um arquivo ZIP específico:
# python benchmark_simples.py --completo --path_zip dados-abertos-zip --arquivo_zip Simples.zip --parquet_destino parquet/2025-03/simples
#
# Para executar apenas o benchmark com Pandas para um arquivo específico:
# python benchmark_simples.py --pandas --path_zip dados-abertos-zip --arquivo_zip Simples.zip --parquet_destino parquet/2025-03/simples
#
# Para executar apenas o benchmark com Dask para um arquivo específico:
# python benchmark_simples.py --dask --path_zip dados-abertos-zip --arquivo_zip Simples.zip --parquet_destino parquet/2025-03/simples

if __name__ == "__main__":
    main()