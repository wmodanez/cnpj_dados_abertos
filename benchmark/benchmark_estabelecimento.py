import sys
import os

# Adiciona o diretório pai (raiz do projeto) ao sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import time
import logging
import glob
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns
from memory_profiler import memory_usage

# Assuming config and processing functions are imported from appropriate modules
# Replace these with actual imports for your project structure
# from src.config import Config
# from src.process.estabelecimento import (
#     process_single_zip_estabelecimento_pandas,
#     process_single_zip_estabelecimento_dask,
#     apply_estabelecimento_transformations_dask,
#     save_estabelecimento_to_parquet_dask
# )
# from src.utils import setup_logging, log_benchmark_results, plot_benchmark_results

# ================== Placeholder Imports/Config ==================
# Remove these placeholders once actual imports are set up
class Config:
    raw_data_dir = os.getenv("RAW_DATA_DIR", "data/raw")
    parquet_dir = os.getenv("PARQUET_DIR", "data/parquet")
    log_dir = os.getenv("LOG_DIR", "logs")
    plot_dir = os.getenv("PLOT_DIR", "plots")
    report_dir = os.getenv("REPORT_DIR", "reports")
    # Example column names - Replace with actual columns from your config
    estabelecimento_columns = [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
        'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
        'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
        'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
        'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro',
        'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
        'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial',
        'data_situacao_especial'
    ]

def process_single_zip_estabelecimento_pandas(zip_path, config):
    """Placeholder: Processa um único arquivo ZIP com Pandas."""
    print(f"Processing {os.path.basename(zip_path)} with Pandas (placeholder)...")
    # Real implementation should read CSVs inside the ZIP, e.g.:
    # import zipfile
    # dfs = []
    # with zipfile.ZipFile(zip_path, 'r') as z:
    #     for filename in z.namelist():
    #         if filename.lower().endswith(('.csv', '.txt')): # Adjust extension
    #             try:
    #                 with z.open(filename) as f:
    #                     # *** CRITICAL: Use names=config.estabelecimento_columns ***
    #                     df = pd.read_csv(f, sep=';', header=None, names=config.estabelecimento_columns, encoding='latin1', low_memory=False, dtype=str)
    #                     dfs.append(df)
    #             except Exception as e:
    #                 logging.error(f"Error reading {filename} in {zip_path} with Pandas: {e}")
    # return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    return pd.DataFrame({'col': [1, 2]}) # Return dummy data

def process_single_zip_estabelecimento_dask(zip_path, config):
    """Placeholder: Processa um único arquivo ZIP com Dask."""
    print(f"Processing {os.path.basename(zip_path)} with Dask (placeholder)...")
    # Real implementation should read CSVs inside the ZIP, e.g.:
    # try:
    #     # *** CRITICAL: Use names=config.estabelecimento_columns ***
    #     ddf = dd.read_csv(
    #         f'zip://{zip_path}::*.ESTABELE', # Adjust wildcard and extension
    #         sep=';',
    #         header=None,
    #         names=config.estabelecimento_columns,
    #         encoding='latin1',
    #         on_bad_lines='warn',
    #         dtype=str, # Specify appropriate dtypes
    #         blocksize='64MB' # Adjust blocksize as needed
    #     )
    #     return ddf
    # except Exception as e:
    #     logging.error(f"Error reading {zip_path} with Dask: {e}")
    #     return dd.from_pandas(pd.DataFrame(), npartitions=1) # Return empty Dask DF on error
    return dd.from_pandas(pd.DataFrame({'col': [1, 2]}), npartitions=1) # Return dummy data


def apply_estabelecimento_transformations_dask(ddf, config):
    """Placeholder: Aplica transformações com Dask."""
    print("Applying Estabelecimento transformations with Dask (placeholder)...")
    # Add transformation logic here (e.g., type conversions, cleaning)
    return ddf

def save_estabelecimento_to_parquet_dask(ddf, output_dir, config):
    """Placeholder: Salva em Parquet com Dask."""
    print(f"Saving Estabelecimento to Parquet in {output_dir} with Dask (placeholder)...")
    # Add saving logic here, potentially with schema definition
    # try:
    #     ddf.to_parquet(output_dir, engine='pyarrow', schema='infer', overwrite=True) # or specify a schema
    # except Exception as e:
    #     logging.error(f"Error saving Dask Parquet to {output_dir}: {e}")
    pass

def setup_logging(log_dir, filename):
    """Placeholder: Configura logging."""
    log_path = os.path.join(log_dir, filename)
    print(f"Setting up logging to {log_path} (placeholder)...")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )

def log_benchmark_results(results, report_path):
    """Placeholder: Loga resultados do benchmark."""
    print(f"Logging benchmark results to {report_path} (placeholder)...")
    try:
        with open(report_path, 'w') as f:
            f.write("Benchmark Results:\n")
            for framework, metrics in results.items():
                f.write(f"\n--- {framework.upper()} ---\n")
                time_str = f"{metrics.get('time', 'N/A'):.2f}" if isinstance(metrics.get('time'), (int, float)) else "N/A"
                mem_str = f"{metrics.get('memory', 'N/A'):.2f}" if isinstance(metrics.get('memory'), (int, float)) else "N/A"

                f.write(f"  Time: {time_str} seconds\n")
                f.write(f"  Max Memory: {mem_str} MiB\n")
    except Exception as e:
        logging.error(f"Failed to write report file {report_path}: {e}")


def plot_benchmark_results(results, plot_path_time, plot_path_mem):
    """Placeholder: Plota resultados do benchmark."""
    print(f"Plotting benchmark results to {plot_path_time} and {plot_path_mem} (placeholder)...")
    # Add plotting logic using matplotlib/seaborn
    frameworks = list(results.keys())
    times = [results[f].get('time', 0) for f in frameworks]
    memories = [results[f].get('memory', 0) for f in frameworks]

    try:
        plt.figure(figsize=(10, 5))
        sns.barplot(x=frameworks, y=times)
        plt.title('Benchmark Time Comparison (Estabelecimento)')
        plt.ylabel('Time (seconds)')
        plt.savefig(plot_path_time)
        plt.close()

        plt.figure(figsize=(10, 5))
        sns.barplot(x=frameworks, y=memories)
        plt.title('Benchmark Max Memory Comparison (Estabelecimento)')
        plt.ylabel('Max Memory (MiB)')
        plt.savefig(plot_path_mem)
        plt.close()
    except Exception as e:
        logging.error(f"Failed to generate plots: {e}")

# ==============================================================

class BenchmarkEstabelecimento:
    """Benchmark para processamento de dados de 'Estabelecimento' usando Pandas e Dask."""

    def __init__(self, config):
        """Inicializa o benchmark."""
        self.config = config
        self.results = {}
        self.log_file = os.path.join(self.config.log_dir, "benchmark_estabelecimento.log")
        self.report_file = os.path.join(self.config.report_dir, "benchmark_estabelecimento_report.txt")
        self.plot_file_time = os.path.join(self.config.plot_dir, "benchmark_estabelecimento_time.png")
        self.plot_file_mem = os.path.join(self.config.plot_dir, "benchmark_estabelecimento_memory.png")

        # Cria diretórios necessários
        os.makedirs(self.config.log_dir, exist_ok=True)
        os.makedirs(self.config.plot_dir, exist_ok=True)
        os.makedirs(self.config.report_dir, exist_ok=True)
        self.parquet_pandas_dir = os.path.join(self.config.parquet_dir, "estabelecimento_pandas")
        self.parquet_dask_dir = os.path.join(self.config.parquet_dir, "estabelecimento_dask")
        os.makedirs(self.parquet_pandas_dir, exist_ok=True)
        os.makedirs(self.parquet_dask_dir, exist_ok=True)

        setup_logging(self.config.log_dir, "benchmark_estabelecimento.log")
        logging.info("="*50)
        logging.info("Iniciando Benchmark Estabelecimento")
        logging.info("="*50)

    def _find_zip_files(self):
        """Encontra arquivos ZIP de 'Estabelecimento' no diretório de dados brutos."""
        # Ajuste o padrão conforme necessário para seus nomes de arquivo
        pattern = os.path.join(self.config.raw_data_dir, "*Estabelec*.zip")
        zip_files = glob.glob(pattern)
        logging.info(f"Encontrados {len(zip_files)} arquivos ZIP de Estabelecimento correspondendo a '{pattern}'.")
        if not zip_files:
            logging.warning(f"Nenhum arquivo ZIP encontrado com o padrão: {pattern}")
            print(f"Aviso: Nenhum arquivo ZIP encontrado com o padrão: {pattern}")
        # Ordenar para processamento consistente (opcional)
        zip_files.sort()
        return zip_files

    def _run_pandas(self, zip_files):
        """Executa o benchmark usando Pandas."""
        logging.info("--- Executando Benchmark: Pandas ---")
        start_time = time.time()

        # Define a função a ser medida
        def pandas_processing():
            output_dir = self.parquet_pandas_dir
            all_data_processed = False
            for zip_file in zip_files:
                base_name = os.path.basename(zip_file)
                logging.info(f"Processando {base_name} com Pandas...")
                parquet_file = os.path.join(output_dir, base_name.replace('.zip', '.parquet'))
                try:
                    # *** IMPORTANTE: Assumindo que process_single_zip_estabelecimento_pandas
                    # *** usa corretamente config.estabelecimento_columns internamente.
                    df = process_single_zip_estabelecimento_pandas(zip_file, self.config)

                    if not df.empty:
                        # Aplicar transformações se houver (talvez adicionar apply_estabelecimento_transformations_pandas?)
                        logging.info(f"Salvando Parquet (Pandas) em {parquet_file}")
                        df.to_parquet(parquet_file, engine='pyarrow')
                        logging.info(f"Salvo {parquet_file}")
                        all_data_processed = True # Marca que pelo menos um arquivo foi processado
                    else:
                         logging.warning(f"Processamento Pandas retornou DataFrame vazio para {base_name}")
                except Exception as e:
                    logging.error(f"Erro processando {base_name} com Pandas: {e}", exc_info=True)
            return all_data_processed # Retorna se algum dado foi processado

        # Mede o tempo de execução e o uso de memória
        mem_usage_pandas = memory_usage((pandas_processing,), interval=0.1, timeout=None, retval=True)
        end_time = time.time()
        elapsed_time_pandas = end_time - start_time
        max_mem_pandas = max(mem_usage_pandas[0]) if mem_usage_pandas[0] else 0
        data_processed = mem_usage_pandas[1] # Obtem o valor de retorno

        if not data_processed:
             logging.warning("Nenhum dado foi processado com sucesso pelo Pandas.")

        logging.info(f"Pandas - Tempo Total: {elapsed_time_pandas:.2f} segundos")
        logging.info(f"Pandas - Pico de Memória: {max_mem_pandas:.2f} MiB")

        self.results['pandas'] = {'time': elapsed_time_pandas, 'memory': max_mem_pandas}

    def _run_dask(self, zip_files):
        """Executa o benchmark usando Dask."""
        logging.info("--- Executando Benchmark: Dask ---")
        start_time = time.time()

        # Define a função a ser medida
        def dask_processing():
            output_dir = self.parquet_dask_dir
            ddfs = []
            for zip_file in zip_files:
                 base_name = os.path.basename(zip_file)
                 logging.info(f"Processando {base_name} com Dask...")
                 try:
                    # *** IMPORTANTE: Assumindo que process_single_zip_estabelecimento_dask
                    # *** usa corretamente config.estabelecimento_columns internamente.
                    ddf = process_single_zip_estabelecimento_dask(zip_file, self.config)

                    # Verifica se o Dask DataFrame é válido (tem partições)
                    if ddf is not None and ddf.npartitions > 0:
                        # Verifica se as colunas esperadas estão presentes (opcional)
                        # expected_cols = set(self.config.estabelecimento_columns)
                        # if not expected_cols.issubset(ddf.columns):
                        #     logging.warning(f"Dask DataFrame para {base_name} não contém todas as colunas esperadas.")
                        # else:
                        ddfs.append(ddf)
                    else:
                        logging.warning(f"Processamento Dask retornou DataFrame vazio/inválido para {base_name}")
                 except Exception as e:
                     logging.error(f"Erro criando Dask DataFrame para {base_name}: {e}", exc_info=True)

            if not ddfs:
                logging.error("Nenhum Dask DataFrame foi criado com sucesso. Abortando benchmark Dask.")
                return False # Indica que nenhum dado foi processado

            logging.info(f"Concatenando {len(ddfs)} Dask DataFrames...")
            final_ddf = None
            try:
                # Concatena todos os Dask DataFrames
                final_ddf = dd.concat(ddfs) if ddfs else None

                if final_ddf is not None:
                    logging.info("Aplicando transformações Dask...")
                    # Aplica transformações usando Dask
                    transformed_ddf = apply_estabelecimento_transformations_dask(final_ddf, self.config)

                    logging.info(f"Salvando Parquet (Dask) em {output_dir}...")
                    # Salva os dados transformados finais em Parquet usando Dask
                    save_estabelecimento_to_parquet_dask(transformed_ddf, output_dir, self.config)
                    # .compute() não é necessário para to_parquet, ele dispara a computação
                    logging.info("Processamento e salvamento Dask concluídos.")
                    return True # Indica sucesso
                else:
                    logging.info("Nenhum dado para processar com Dask.")
                    return False

            except Exception as e:
                logging.error(f"Erro durante concatenação, transformação ou salvamento Dask: {e}", exc_info=True)
                # Tenta computar algo simples para verificar se o Dask está funcional
                # try:
                #     if final_ddf is not None: final_ddf.head()
                # except Exception as client_e:
                #      logging.error(f"Erro adicional ao interagir com Dask: {client_e}", exc_info=True)
                return False # Indica falha


        # Mede o tempo de execução e o uso de memória
        # Nota: Medir a memória do Dask com memory_profiler é limitado.
        # Use o Dask Dashboard para análise detalhada da memória.
        mem_usage_dask = memory_usage((dask_processing,), interval=0.1, timeout=None, retval=True)
        end_time = time.time()
        elapsed_time_dask = end_time - start_time
        max_mem_dask = max(mem_usage_dask[0]) if mem_usage_dask[0] else 0 # Memória máxima do processo principal
        data_processed = mem_usage_dask[1] # Obtem o valor de retorno

        if not data_processed:
             logging.warning("Nenhum dado foi processado com sucesso pelo Dask.")


        logging.info(f"Dask - Tempo Total: {elapsed_time_dask:.2f} segundos")
        logging.info(f"Dask - Pico de Memória (Processo Principal): {max_mem_dask:.2f} MiB")

        self.results['dask'] = {'time': elapsed_time_dask, 'memory': max_mem_dask}


    def run(self):
        """Executa todos os componentes do benchmark."""
        zip_files = self._find_zip_files()
        if not zip_files:
            logging.warning("Nenhum arquivo ZIP de Estabelecimento encontrado. Pulando execuções do benchmark.")
            return

        # Executa benchmarks
        self._run_pandas(zip_files)
        self._run_dask(zip_files) # Garanta que o Dask Client esteja configurado se necessário externamente

        # Loga e plota resultados
        if self.results:
            log_benchmark_results(self.results, self.report_file)
            plot_benchmark_results(self.results, self.plot_file_time, self.plot_file_mem)
        else:
            logging.info("Nenhum resultado de benchmark para reportar.")

        logging.info("Benchmark Estabelecimento Finalizado")
        logging.info("="*50)

if __name__ == "__main__":
    # Configuração básica (substitua pelo carregamento real, se necessário)
    config = Config()

    # Configura Dask Client (opcional, mas recomendado para melhor controle e dashboard)
    # from dask.distributed import Client, LocalCluster
    # try:
    #     # Tenta criar um cluster local. Ajuste workers, memory_limit conforme necessário.
    #     cluster = LocalCluster(n_workers=4, threads_per_worker=2, memory_limit='2GB')
    #     client = Client(cluster)
    #     print(f"Dask Dashboard Link: {client.dashboard_link}")
    #     logging.info(f"Dask Dashboard Link: {client.dashboard_link}")
    # except Exception as e:
    #     print(f"Falha ao iniciar Dask LocalCluster: {e}")
    #     logging.error(f"Falha ao iniciar Dask LocalCluster: {e}", exc_info=True)
    #     client = None # Prossegue sem cliente distribuído se falhar

    benchmark = BenchmarkEstabelecimento(config)
    benchmark.run()

    # Encerra Dask client e cluster se foram iniciados
    # if client:
    #     try:
    #         client.close()
    #         cluster.close()
    #         print("Cliente e cluster Dask encerrados.")
    #         logging.info("Cliente e cluster Dask encerrados.")
    #     except Exception as e:
    #         print(f"Erro ao encerrar Dask: {e}")
    #         logging.error(f"Erro ao encerrar Dask: {e}", exc_info=True)
