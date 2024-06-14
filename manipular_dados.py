# pip install requests progressbar "dask[complete]"
import datetime
import glob
import os
import duckdb as duck
import zipfile
from multiprocessing import freeze_support

import dask.dataframe as dd
import numpy as np
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from dotenv import load_dotenv

pgBar = ProgressBar()
pgBar.register()

# YYYYMM = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y%m')
YYYYMM = (datetime.date.today()).strftime('%Y%m')


def manipular_empresa() -> dd:
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
    startTime: datetime = datetime.datetime.now()
    try:
        tableName: str = 'empresa'
        extrairArquivo(PATH_ZIP, PATH_UNZIP, 'Emp*.*')
        print('Início da manipulação das Empresas')
        ddEmpresa = dd.read_csv('dados-abertos/*.EMPRECSV', sep=';', names=colunas_empresa, encoding='latin1',
                                dtype=dtype_empresa)

        interTimer = datetime.datetime.now()
        ddEmpresa = ddEmpresa.set_index('cnpj_basico')
        print('Tempo de manipulação dos dados:', str(datetime.datetime.now() - interTimer))

        createParquet(ddEmpresa, tableName)

        # Tratamento para criar as empresas privadas
        ddEmpresaPrivada = ddEmpresa[ddEmpresa['natureza_juridica'].between(2046, 2348)]
        ddEmpresaPrivada = ddEmpresaPrivada.drop('qualificacao_responsavel', axis=1)
        ddEmpresaPrivada = ddEmpresaPrivada.drop('ente_federativo_responsavel', axis=1)

    except Exception as e:
        print(f'Error: {e}')

        createParquet(ddEmpresaPrivada, 'empresa_privada')

    finally:
        deleteFile(PATH_UNZIP)
        print('Tempo total de manipulação das Empresas:', str(datetime.datetime.now() - startTime))


def manipular_estabelecimento() -> dd:
    startTime: datetime = datetime.datetime.now()
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
        'cnpj': 'string',
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
        'situacao_especial': 'string'
    }
    try:
        extrairArquivo(PATH_ZIP, PATH_UNZIP, '*Est*')
        print('Início da manipulação dos Estabelecimentos')
        ddEstabelecimento = dd.read_csv('dados-abertos/*.ESTABELE', sep=';',
                                        names=colunas_estabelecimento, encoding='latin1', dtype=dtype_estabelecimento)

        listDroppedColumns: list = ['cnpj_ordem', 'cnpj_dv', 'tipo_logradouro', 'logradouro', 'numero', 'complemento',
                                    'bairro', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'pais',
                                    'correio_eletronico', 'situacao_especial', 'data_situacao_especial',
                                    'nome_cidade_exterior']

        ddEstabelecimento['cnpj'] = ddEstabelecimento['cnpj_basico'] + ddEstabelecimento['cnpj_ordem'] + \
                                    ddEstabelecimento['cnpj_dv']
        ddEstabelecimento['cnpj'] = ddEstabelecimento.cnpj.astype(np.int64)

        ddEstabelecimento = ddEstabelecimento.drop(columns=listDroppedColumns, axis=1)
        ddEstabelecimento['cnpj_basico'] = ddEstabelecimento.cnpj_basico.astype(np.int64)

        ddEstabelecimento['data_situacao_cadastral'] = ddEstabelecimento.data_situacao_cadastral.replace('0', None)
        ddEstabelecimento['data_situacao_cadastral'] = ddEstabelecimento.data_situacao_cadastral.astype("M8[us]")
        ddEstabelecimento['data_inicio_atividades'] = ddEstabelecimento.data_inicio_atividades.astype("M8[us]")

        ddEstabelecimento = ddEstabelecimento.assign(tipo_situacao_cadastral=None)
        ddEstabelecimento['tipo_situacao_cadastral'] = ddEstabelecimento['tipo_situacao_cadastral'].mask(
            ddEstabelecimento.codigo_situacao_cadastral == 2, 1)
        ddEstabelecimento['tipo_situacao_cadastral'] = ddEstabelecimento['tipo_situacao_cadastral'].mask(
            ddEstabelecimento.codigo_situacao_cadastral != 2, 2)
        ddEstabelecimento['tipo_situacao_cadastral'] = ddEstabelecimento['tipo_situacao_cadastral'].mask(
            (ddEstabelecimento.codigo_situacao_cadastral == 8) & (
                    ddEstabelecimento.codigo_motivo_situacao_cadastral == 1), 3)
        ddEstabelecimento['tipo_situacao_cadastral'] = ddEstabelecimento.tipo_situacao_cadastral.astype('int')

        ddEstabelecimento = ddEstabelecimento.set_index('cnpj_basico')
        print('Tempo de manipulação dos dados:', str(datetime.datetime.now() - startTime))

        createParquet(ddEstabelecimento, 'estabelecimento')

        ddEstabelecimentoGO = ddEstabelecimento[ddEstabelecimento['uf'] == 'GO']
        createParquet(ddEstabelecimentoGO, 'estabelecimento_go')

        return ddEstabelecimento, ddEstabelecimentoGO

    except Exception as e:
        print(f'Error: {e}')
    finally:
        deleteFile(PATH_UNZIP)
        print('Tempo total de manipulação dos Estabelecimentos:', str(datetime.datetime.now() - startTime))


def manipular_simples() -> dd:
    startTime: datetime = datetime.datetime.now()
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
        extrairArquivo(PATH_ZIP, PATH_UNZIP, '*Sim*')

        print('Início da manipulação do Simples')
        ddSimples = dd.read_csv('dados-abertos/*SIMPLES*', sep=';', names=colunas_simples, encoding='latin1',
                                dtype=dtype_simples, na_filter=None)

        ddSimples = ddSimples.set_index('cnpj_basico')

        interTimer = datetime.datetime.now()
        ddSimples['data_opcao_simples'] = ddSimples.data_opcao_simples.replace('00000000', np.NaN)
        ddSimples['data_exclusao_simples'] = ddSimples.data_exclusao_simples.replace('00000000', np.NaN)
        ddSimples['data_opcao_mei'] = ddSimples.data_opcao_mei.replace('00000000', np.NaN)
        ddSimples['data_exclusao_mei'] = ddSimples.data_exclusao_mei.replace('00000000', np.NaN)

        ddSimples['opcao_simples'] = ddSimples.opcao_simples.replace('N', '0').replace('S', '1')
        ddSimples['opcao_simples'] = ddSimples.opcao_simples.astype('int')

        ddSimples['opcao_mei'] = ddSimples.opcao_mei.replace('N', '0').replace('S', '1')
        ddSimples['opcao_mei'] = ddSimples.opcao_mei.astype('int')

        print('Tempo de manipulação dos dados:', str(datetime.datetime.now() - interTimer))

        createParquet(ddSimples, 'simples')

        return ddSimples
    except Exception as e:
        print(f'Error: {e}')
    finally:
        deleteFile(PATH_UNZIP)
        print('Tempo total de trabalho com o Simples:', str(datetime.datetime.now() - startTime))


def manipular_socio() -> dd:
    startTime: datetime = datetime.datetime.now()
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
        extrairArquivo(PATH_ZIP, PATH_UNZIP, '*Soc*')

        print('Início da manipulação dos Sócios')
        ddSocio = dd.read_csv('dados-abertos/*SOCIO*', sep=';', names=colunas_socio, encoding='latin1',
                              dtype=dtype_socio, na_filter=None)

        interTimer = datetime.datetime.now()
        ddSocio['cnpj_basico'] = ddSocio.cnpj_basico.astype('int')
        ddSocio['identificador_socio'] = ddSocio.identificador_socio.astype('int')
        ddSocio['qualificacao_socio'] = ddSocio.qualificacao_socio.astype('int')
        ddSocio['qualificacao_representante_legal'] = ddSocio.qualificacao_representante_legal.astype('int')
        ddSocio['faixa_etaria'] = ddSocio.faixa_etaria.astype('int')
        print('Tempo de manipulação dos dados:', str(datetime.datetime.now() - interTimer))

        createParquet(ddSocio, 'socio')

        return ddSocio
    except Exception as e:
        print(f'Error: {e}')
    finally:
        deleteFile(PATH_UNZIP)
        print('Tempo total de trabalho com os Sócios:', str(datetime.datetime.now() - startTime))


def createParquet(daskDataframe: dd, tableName: str):
    interTimer = datetime.datetime.now()
    try:
        daskDataframe.to_parquet(PATH_PARQUET + '\\' + YYYYMM + '\\' + tableName, engine='pyarrow',
                                 write_metadata_file=True, overwrite=True,
                                 name_function=lambda x: f'{YYYYMM}_{tableName}{x}.parquet')
        print(f'Tempo de criação dos arquivos parquet ({tableName}):', str(datetime.datetime.now() - interTimer))
    except Exception as e:
        raise


def extrairArquivo(folderOri: str, folderDest: str, fileName: str = '*.*'):
    zipFileList: list = list(glob.glob(os.path.join(folderOri, fileName)))
    for arq in zipFileList:
        print('Unpacking ' + arq)
        with zipfile.ZipFile(arq, 'r') as zip_ref:
            zip_ref.extractall(folderDest)


def deleteFile(folderDelete: str, fileName: str = '*'):
    fileList: list = list(glob.glob(os.path.join(folderDelete, fileName)))
    for arq in fileList:
        os.remove(arq)


def create_db_parquet():
    print('Início da criação das tabelas parquet')
    startTime: datetime = datetime.datetime.now()
    conn = duck.connect(f'parquet/cnpj.duckdb', config={'threads': 4})

    listTabela = ['empresa', 'estabelecimento', 'estabelecimento_go', 'simples', 'socio']
    for tabela in listTabela:
        path_parquet = PATH_PARQUET + '/' + YYYYMM + '/' + tabela + '/*.parquet'
        add_table_parquet(conn, tabela, path_parquet)
    listTabelaFixa = ['cnae', 'motivo', 'municipio']
    for tabela in listTabelaFixa:
        path_parquet = PATH_PARQUET + '/base/' + tabela + '.parquet'
        add_table_parquet(conn, tabela, path_parquet)
    print('Tempo total de carga das tabelas:', str(datetime.datetime.now() - startTime))


def add_table_parquet(conn: duck.connect(), table: str, pathParquet: str):
    interTimer = datetime.datetime.now()
    sql = f'''
        CREATE OR REPLACE TABLE {table} AS 
            (SELECT * FROM '{pathParquet}')
    '''
    conn.execute(sql)
    print(f'Tempo de carregamento da tabela {table}:', str(datetime.datetime.now() - interTimer))


if __name__ == '__main__':
    env = 'hom'
    load_dotenv('.env.local')

    if env == 'hom':
        load_dotenv('.env.hom')
    elif env == 'prod':
        load_dotenv('.env')

    PATH_ZIP: str = os.getenv('PATH_ZIP')
    PATH_UNZIP: str = os.getenv('PATH_UNZIP')
    PATH_PARQUET: str = os.getenv('PATH_PARQUET')

    freeze_support()
    client = Client()
    print(client)

    manipular_empresa()
    manipular_estabelecimento()
    manipular_simples()
    manipular_socio()
    create_db_parquet()