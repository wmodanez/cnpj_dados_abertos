# pip install requests progressbar "dask[complete]"
import datetime
import glob
import os
import shutil
import sys
import time
import zipfile
import eventlet
from multiprocessing import freeze_support
from sys import stderr as STREAM

import dask.dataframe as dd
import duckdb as duck
import numpy as np
import progressbar
import pycurl
import requests
from bs4 import BeautifulSoup
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster
from dotenv import load_dotenv

pg_bar_dask = ProgressBar()
pg_bar_dask.register()

progress_bar: progressbar = None

# YYYYMM = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y%m')
YYYYMM: str = (datetime.date.today()).strftime('%Y%m')

KB: int = 1024


def check_basic_folders(folder: str):
    if not os.path.exists(os.path.join(folder)):
        os.makedirs(folder)


def progress(download_t, download_d, upload_t, upload_d):
    STREAM.write('Downloading: {}/{} kiB ({}%)\r'.format(str(int(download_d / KB)), str(int(download_t / KB)),
                                                         str(int(
                                                             download_d / download_t * 100) if download_t > 0 else 0)
                                                         ))
    STREAM.flush()


def get_info_file(path: str, filename: str) -> int:
    if os.path.exists(path + filename):
        return os.stat(path + filename).st_size, os.stat(path + filename).st_mtime
    return 0, 0


def check_download(link, file) -> bool:
    if str(link.get('href')).endswith('.zip') and file in str(link.get('href')):
        file_download: str = link.get('href')
        file_url: str = URL + file_download

        if not file_download.startswith('http'):
            response = requests.head(file_url)

            if response.status_code != 200:
                print(f'Erro ao tentar baixar {file_download} - Status Code: {response.status_code}')
                return False

            file_remote_last_modified: list = response.headers['Last-Modified'].split()

            file_remote_last_modified_time: str = str(file_remote_last_modified[4]).split(':')
            timestamp_last_modified: int = datetime.datetime(int(file_remote_last_modified[3]),
                                                             int(time.strptime(file_remote_last_modified[2],
                                                                               '%b').tm_mon),
                                                             int(file_remote_last_modified[1]),
                                                             int(file_remote_last_modified_time[0]),
                                                             int(file_remote_last_modified_time[1]),
                                                             int(file_remote_last_modified_time[2])).timestamp()
            curl = pycurl.Curl()
            curl.setopt(pycurl.URL, file_url)
            curl.setopt(pycurl.FOLLOWLOCATION, 1)
            curl.setopt(pycurl.MAXREDIRS, 5)
            curl.setopt(pycurl.NOPROGRESS, False)
            curl.setopt(pycurl.XFERINFOFUNCTION, progress)

            file_local_size, file_local_last_modified = get_info_file(PATH_ZIP, file_download)

            file_local = PATH_ZIP + file_download

            print('Baixando o arquivo: ' + file_download)
            if file_local_size == 0:
                f = open(file_local, "wb")
            elif file_local_size > 0:
                if file_local_last_modified >= timestamp_last_modified:
                    if file_local_size != int(response.headers['Content-Length']):
                        f = open(file_local, "ab")
                        curl.setopt(pycurl.RESUME_FROM, file_local_size)
                    else:
                        print(f'Arquivo {file_download} já está atualizado.')
                        return True
                else:
                    f = open(file_local, "wb")
            curl.setopt(pycurl.WRITEDATA, f)

            try:
                curl.perform()
            except Exception:
                print('Não foi possível baixar o arquivo: ' + file_download)
                pass
            finally:
                curl.close()
                f.close()
                os.utime(file_local, (timestamp_last_modified, timestamp_last_modified))
                sys.stdout.flush()
            return True
        else:
            print('Não foi possível baixar o arquivo: ' + file_download)
    else:
        return True


def manipular_empresa() -> bool:
    print('Início da manipulação das Empresas')

    inter_time: datetime = datetime.datetime.now()

    colunas_empresa: list = [
        'cnpj_basico',
        'razao_social',
        'natureza_juridica',
        'qualificacao_responsavel',
        'capital_social',
        'porte_empresa',
        'ente_federativo_responsavel']

    dtype_empresa: dict = {
        'cnpj_basico': 'int',
        'razao_social': 'string',
        'natureza_juridica': 'int',
        'qualificacao_responsavel': 'string',
        'capital_social': 'string',
        'porte_empresa': 'string',
        'ente_federativo_responsavel': 'string'
    }

    try:
        table_name: str = 'empresas'
        for link in [x for x in soup.find_all('a') if str(x.get('href')).endswith('.zip')]:
            if not check_download(link, table_name.capitalize()):
                return None

        file_extractor(PATH_ZIP, PATH_UNZIP, 'Emp*.*')

        dd_empresa: dd = dd.read_csv('dados-abertos/*.EMPRECSV', sep=';', names=colunas_empresa, encoding='latin1',
                                     dtype=dtype_empresa)

        inter_timer: datetime = datetime.datetime.now()
        print('Tempo de manipulação dos dados:', str(datetime.datetime.now() - inter_timer))

        create_parquet(dd_empresa, table_name)

        try:
            # Tratamento para criar as empresas privadas
            dd_empresa_privada: dd = dd_empresa[dd_empresa['natureza_juridica'].between(2046, 2348)]
            dd_empresa_privada = dd_empresa_privada.drop('qualificacao_responsavel', axis=1)
            dd_empresa_privada = dd_empresa_privada.drop('ente_federativo_responsavel', axis=1)

            create_parquet(dd_empresa_privada, 'empresa_privada')
        except Exception as e:
            print(f'Error: {e}')
            return False

    except Exception as e:
        print(f'Error: {e}')
        return False

    file_delete(PATH_UNZIP)
    print('Tempo total de manipulação das Empresas:', str(datetime.datetime.now() - inter_time))
    return True


def manipular_estabelecimento() -> bool:
    print('Início da manipulação dos Estabelecimentos')

    inter_time: datetime = datetime.datetime.now()

    colunas_estabelecimento: list = [
        'cnpj_basico',
        'cnpj_ordem',
        'cnpj_dv',
        'matriz_filial',
        'nome_fantasia',
        'codigo_situacao_cadastral',
        'data_situacao_cadastral',
        'codigo_motivo_situacao_cadastral',
        'nome_cidade_exterior',
        'pais',
        'data_inicio_atividades',
        'codigo_cnae',
        'cnae_secundaria',
        'tipo_logradouro',
        'logradouro',
        'numero',
        'complemento',
        'bairro',
        'cep',
        'uf',
        'codigo_municipio',
        'ddd1',
        'telefone1',
        'ddd2',
        'telefone2',
        'ddd_fax',
        'fax',
        'correio_eletronico',
        'situacao_especial',
        'data_situacao_especial'
    ]

    dtype_estabelecimento: dict = {
        'cnpj_basico': 'string',
        'cnpj_ordem': 'string',
        'cnpj_dv': 'string',
        'matriz_filial': 'int',
        'nome_fantasia': 'string',
        'codigo_situacao_cadastral': 'int',
        'data_situacao_cadastral': 'string',
        'codigo_motivo_situacao_cadastral': 'int',
        'nome_cidade_exterior': 'string',
        'pais': 'string',
        'data_inicio_atividades': 'string',
        'codigo_cnae': 'int',
        'cnae_secundaria': 'string',
        'tipo_logradouro': 'string',
        'logradouro': 'string',
        'numero': 'string',
        'complemento': 'string',
        'bairro': 'string',
        'cep': 'string',
        'uf': 'string',
        'codigo_municipio': 'int',
        'ddd1': 'string',
        'telefone1': 'string',
        'ddd2': 'string',
        'telefone2': 'string',
        'ddd_fax': 'string',
        'fax': 'string',
        'correio_eletronico': 'string',
        'situacao_especial': 'string',
        'data_situacao_especial': 'string'
    }

    try:
        table_name: str = 'estabelecimentos'
        for link in [x for x in soup.find_all('a') if str(x.get('href')).endswith('.zip')]:
            if not check_download(link, table_name.capitalize()):
                return None

        file_extractor(PATH_ZIP, PATH_UNZIP, '*Est*')

        dd_estabelecimento: dd = dd.read_csv('dados-abertos/*.ESTABELE', sep=';',
                                             names=colunas_estabelecimento, encoding='latin1',
                                             dtype=dtype_estabelecimento)

        list_dropped_columns: list = ['cnpj_ordem', 'cnpj_dv', 'tipo_logradouro', 'logradouro', 'numero', 'complemento',
                                      'bairro', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'pais',
                                      'correio_eletronico', 'situacao_especial', 'data_situacao_especial',
                                      'nome_cidade_exterior']

        dd_estabelecimento['cnpj'] = dd_estabelecimento['cnpj_basico'] + dd_estabelecimento['cnpj_ordem'] + \
                                     dd_estabelecimento['cnpj_dv']
        dd_estabelecimento['cnpj'] = dd_estabelecimento.cnpj.astype(np.int64)

        dd_estabelecimento = dd_estabelecimento.drop(columns=list_dropped_columns, axis=1)
        dd_estabelecimento['cnpj_basico'] = dd_estabelecimento.cnpj_basico.astype(np.int64)

        dd_estabelecimento['data_situacao_cadastral'] = dd_estabelecimento.data_situacao_cadastral.replace('0', None)
        dd_estabelecimento['data_situacao_cadastral'] = dd_estabelecimento.data_situacao_cadastral.astype("M8[us]")
        dd_estabelecimento['data_inicio_atividades'] = dd_estabelecimento.data_inicio_atividades.astype("M8[us]")

        dd_estabelecimento = dd_estabelecimento.assign(tipo_situacao_cadastral=None)
        dd_estabelecimento['tipo_situacao_cadastral'] = dd_estabelecimento['tipo_situacao_cadastral'].mask(
            dd_estabelecimento.codigo_situacao_cadastral == 2, 1)
        dd_estabelecimento['tipo_situacao_cadastral'] = dd_estabelecimento['tipo_situacao_cadastral'].mask(
            dd_estabelecimento.codigo_situacao_cadastral != 2, 2)
        dd_estabelecimento['tipo_situacao_cadastral'] = dd_estabelecimento['tipo_situacao_cadastral'].mask(
            (dd_estabelecimento.codigo_situacao_cadastral == 8) & (
                    dd_estabelecimento.codigo_motivo_situacao_cadastral == 1), 3)
        dd_estabelecimento['tipo_situacao_cadastral'] = dd_estabelecimento.tipo_situacao_cadastral.astype('int')

        print('Tempo de manipulação dos dados:', str(datetime.datetime.now() - inter_time))

        create_parquet(dd_estabelecimento, table_name)

        try:
            dd_estabelecimento_go: dd = dd_estabelecimento[dd_estabelecimento['uf'] == 'GO']
            create_parquet(dd_estabelecimento_go, 'estabelecimentos_go')
        except Exception as e:
            print(f'Error: {e}')
            return False

    except Exception as e:
        print(f'Error: {e}')
        return False

    file_delete(PATH_UNZIP)
    print('Tempo total de manipulação dos Estabelecimentos:', str(datetime.datetime.now() - inter_time))
    return True


def manipular_simples() -> bool:
    print('Início da manipulação do Simples')

    start_time: datetime = datetime.datetime.now()

    colunas_simples: list = [
        'cnpj_basico',
        'opcao_simples',
        'data_opcao_simples',
        'data_exclusao_simples',
        'opcao_mei',
        'data_opcao_mei',
        'data_exclusao_mei']

    dtype_simples: dict = {
        'cnpj_basico': 'int',
        'opcao_simples': 'string',
        'data_opcao_simples': 'string',
        'data_exclusao_simples': 'string',
        'opcao_mei': 'string',
        'data_opcao_mei': 'string',
        'data_exclusao_mei': 'string'}

    try:
        table_name: str = 'simples'
        for link in [x for x in soup.find_all('a') if str(x.get('href')).endswith('.zip')]:
            if not check_download(link, table_name.capitalize()):
                return None

        file_extractor(PATH_ZIP, PATH_UNZIP, '*Sim*')

        dd_simples: dd = dd.read_csv('dados-abertos/*SIMPLES*', sep=';', names=colunas_simples, encoding='latin1',
                                     dtype=dtype_simples, na_filter=None)

        inter_timer: datetime = datetime.datetime.now()
        dd_simples['data_opcao_simples'] = dd_simples.data_opcao_simples.replace('00000000', np.nan)
        dd_simples['data_opcao_simples'] = dd_simples.data_opcao_simples.astype("M8[us]")

        dd_simples['data_exclusao_simples'] = dd_simples.data_exclusao_simples.replace('00000000', np.nan)
        dd_simples['data_exclusao_simples'] = dd_simples.data_exclusao_simples.astype("M8[us]")

        dd_simples['data_opcao_mei'] = dd_simples.data_opcao_mei.replace('00000000', np.nan)
        dd_simples['data_opcao_mei'] = dd_simples.data_opcao_mei.astype("M8[us]")

        dd_simples['data_exclusao_mei'] = dd_simples.data_exclusao_mei.replace('00000000', np.nan)
        dd_simples['data_exclusao_mei'] = dd_simples.data_exclusao_mei.astype("M8[us]")

        dd_simples['opcao_simples'] = dd_simples.opcao_simples.replace('N', '0').replace('S', '1')
        dd_simples['opcao_simples'] = dd_simples.opcao_simples.astype('int')

        dd_simples['opcao_mei'] = dd_simples.opcao_mei.replace('N', '0').replace('S', '1')
        dd_simples['opcao_mei'] = dd_simples.opcao_mei.astype('int')

        print('Tempo de manipulação dos dados:', str(datetime.datetime.now() - inter_timer))

        create_parquet(dd_simples, table_name)

    except Exception as e:
        print(f'Error: {e}')
        return False

    file_delete(PATH_UNZIP)
    print('Tempo total de trabalho com o Simples:', str(datetime.datetime.now() - start_time))
    return True


def manipular_socio() -> bool:
    print('Início da manipulação dos Sócios')

    start_time: datetime = datetime.datetime.now()

    colunas_socio: list = [
        'cnpj_basico',
        'identificador_socio',
        'nome_socio',
        'cnpj_cpf_socio',
        'qualificacao_socio',
        'data_entrada_sociedade',
        'pais',
        'representante_legal',
        'nome_representante',
        'qualificacao_representante_legal',
        'faixa_etaria'
    ]

    dtype_socio: dict = {
        'cnpj_basico': 'string',
        'identificador_socio': 'string',
        'nome_socio': 'string',
        'cnpj_cpf_socio': 'string',
        'qualificacao_socio': 'string',
        'data_entrada_sociedade': 'string',
        'pais': 'string',
        'representante_legal': 'string',
        'nome_representante': 'string',
        'qualificacao_representante_legal': 'string',
        'faixa_etaria': 'string'
    }

    try:
        table_name: str = 'socios'
        for link in [x for x in soup.find_all('a') if str(x.get('href')).endswith('.zip')]:
            if not check_download(link, table_name.capitalize()):
                return None

        file_extractor(PATH_ZIP, PATH_UNZIP, '*Soc*')

        dd_socio: dd = dd.read_csv('dados-abertos/*SOCIO*', sep=';', names=colunas_socio, encoding='latin1',
                                   dtype=dtype_socio, na_filter=None)

        inter_timer: datetime = datetime.datetime.now()
        dd_socio['cnpj_basico'] = dd_socio.cnpj_basico.astype('int')
        dd_socio['identificador_socio'] = dd_socio.identificador_socio.astype('int')
        dd_socio['qualificacao_socio'] = dd_socio.qualificacao_socio.astype('int')
        dd_socio['qualificacao_representante_legal'] = dd_socio.qualificacao_representante_legal.astype('int')
        dd_socio['faixa_etaria'] = dd_socio.faixa_etaria.astype('int')
        print('Tempo de manipulação dos dados:', str(datetime.datetime.now() - inter_timer))

        create_parquet(dd_socio, table_name)

    except Exception as e:
        print(f'Error: {e}')
        return False

    file_delete(PATH_UNZIP)
    print('Tempo total de trabalho com os Sócios:', str(datetime.datetime.now() - start_time))
    return True


def create_parquet(dask_dataframe: dd, table_name: str):
    inter_timer: datetime = datetime.datetime.now()
    try:
        dask_dataframe.to_parquet(PATH_PARQUET + '\\' + YYYYMM + '\\' + table_name, engine='pyarrow',
                                  write_metadata_file=True, overwrite=True, write_index=False,
                                  name_function=lambda x: f'{YYYYMM}_{table_name}{x}.parquet')
        print(f'Tempo de criação dos arquivos parquet ({table_name}):', str(datetime.datetime.now() - inter_timer))
    except Exception as e:
        raise


def file_extractor(folder_ori: str, folder_dest: str, filename: str = '*.*'):
    zip_file_list: list = list(glob.glob(os.path.join(folder_ori, filename)))
    for arq in zip_file_list:
        print('Unpacking ' + arq)
        with zipfile.ZipFile(arq, 'r') as zip_ref:
            zip_ref.extractall(folder_dest)


def file_delete(folder: str, filename: str = '*'):
    file_list: list = list(glob.glob(os.path.join(folder, filename)))
    for arq in file_list:
        os.remove(arq)


def create_duckdb_file():
    print('Início da criação das tabelas parquet')
    inter_time: datetime = datetime.datetime.now()
    try:
        os.remove(PATH_PARQUET + FILE_DB_PARQUET)
    except FileNotFoundError:
        pass

    conn: duck.connect() = duck.connect(PATH_PARQUET + FILE_DB_PARQUET, config={'threads': 4})

    list_tabela: list = ['empresas', 'estabelecimentos', 'estabelecimentos_go', 'simples', 'socios']
    for tabela in list_tabela:
        path_parquet: str = PATH_PARQUET + YYYYMM + '/' + tabela + '/*.parquet'
        add_table_parquet(conn, tabela, path_parquet)
        # shutil.rmtree(os.path.dirname(path_parquet))
    list_tabela_fixa: list = ['cnae', 'motivo', 'municipio']
    for tabela in list_tabela_fixa:
        path_parquet = PATH_PARQUET + 'base/' + tabela + '.parquet'
        add_table_parquet(conn, tabela, path_parquet)
    print('Tempo total de carga das tabelas:', str(datetime.datetime.now() - inter_time))

    try:
        shutil.copyfile(PATH_PARQUET + FILE_DB_PARQUET, PATH_REMOTE_PARQUET + FILE_DB_PARQUET)
        print('Tempo gasto para cópia do arquivo para destino:', str(datetime.datetime.now() - inter_time))
    except Exception as e:
        print(f'Error ao copiar o arquivo: {e}')


def add_table_parquet(conn: duck.connect(), table: str, path_parquet: str):
    inter_timer: datetime = datetime.datetime.now()
    sql: str = f'''
        CREATE OR REPLACE TABLE {table} AS 
            (SELECT * FROM '{path_parquet}')
    '''
    conn.execute(sql)
    print(f'Tempo de carregamento da tabela {table}:', str(datetime.datetime.now() - inter_timer))


if __name__ == '__main__':

    print(f'Início da execução dia: {datetime.datetime.now():%d/%m/%Y} às {datetime.datetime.now():%H:%M:%S}')
    start_time: datetime = datetime.datetime.now()
    load_dotenv('.env.local')

    URL: str = os.getenv('URL_ORIGIN')
    PATH_ZIP: str = os.getenv('PATH_ZIP')
    PATH_UNZIP: str = os.getenv('PATH_UNZIP')
    PATH_PARQUET: str = os.getenv('PATH_PARQUET')
    FILE_DB_PARQUET: str = os.getenv('FILE_DB_PARQUET')
    PATH_REMOTE_PARQUET: str = os.getenv('PATH_REMOTE_PARQUET')

    list_folders: list = [PATH_ZIP, PATH_UNZIP, PATH_PARQUET]
    for folder in list_folders:
        check_basic_folders(folder)

    freeze_support()

    cluster: LocalCluster = LocalCluster(n_workers=4, threads_per_worker=1, memory_limit='7GB',
                                         dashboard_address=':1977')
    cluster

    client: Client = Client(cluster)
    print(client)

    soup: BeautifulSoup = BeautifulSoup(requests.get(URL).text, 'html.parser')
    list_folders = []
    for element in soup.find_all('a'):
        if '-' in element.get('href'):
            list_folders.append(element.get('href'))
    URL += max(list_folders)
    soup: BeautifulSoup = BeautifulSoup(requests.get(URL).text, 'html.parser')

    is_create_db_parquet: bool = False

    if manipular_empresa():
        is_create_db_parquet = True
    if manipular_estabelecimento():
        is_create_db_parquet = True
    if manipular_simples():
        is_create_db_parquet = True
#    if manipular_socio():
#        is_create_db_parquet = True

    if is_create_db_parquet:
        create_duckdb_file()

    print(f'Tempo total:', str(datetime.datetime.now() - start_time))
    client.shutdown()
