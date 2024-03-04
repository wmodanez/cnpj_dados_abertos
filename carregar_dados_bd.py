import os
import glob
import time
import zipfile
import pandas as pd
import dask.dataframe as dd

from dotenv import load_dotenv
from datetime import datetime as dt
from dask.diagnostics import ProgressBar
from sqlalchemy import create_engine, text

ProgressBar().register()

PASTA_SAIDA: str = r"dados-abertos"  # esta pasta deve estar vazia.

ARQUIVOS_COMPACTADOS: list = list(glob.glob(os.path.join(r"dados-abertos-zip", r'*.zip')))
DATA_REFERENCIA: str = dt.utcfromtimestamp(os.path.getmtime(ARQUIVOS_COMPACTADOS[0])).strftime("%d/%m/%Y")


def carregaTabelaBasica(extensaoArquivo: str, nomeTabela: str, db: str):
    arquivos: list = list(glob.glob(os.path.join(PASTA_SAIDA, '*' + extensaoArquivo)))[0]
    print('carregando tabela ' + arquivos)
    dtab = pd.read_csv(arquivos, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo', 'descricao'])
    ENGINE = create_engine(url=db, echo=True)
    dtab.to_sql(nomeTabela, ENGINE, if_exists='append', index=None)


def carregaTipo(nome_tabela: str, tipo: str, colunas: str, db: str):
    # usando dask, bem mais rápido que pandas
    arquivos: list = list(glob.glob(os.path.join(PASTA_SAIDA, '*' + tipo)))
    for arq in arquivos:
        print(f'carregando: {arq=}')
        print('lendo csv ...', time.asctime())
        ddf = dd.read_csv(arq, sep=';', header=None, names=colunas,
                          encoding='latin1', dtype=str,
                          na_filter=None)
        print('to_sql...', time.asctime())
        # ddf.to_sql(nome_tabela, uri=db, index=None, if_exists='append', parallel=True,
        #            dtype=sqlalchemy.sql.sqltypes.String)
        print('fim parcial...', time.asctime())


def otimiza_tabelas(sqls: str, db: str):
    print('Inicio sqls:', time.asctime())
    ENGINE = create_engine(url=db, echo=True)
    for k, sql in enumerate(sqls.split(';')):
        if not sql.strip():
            continue
        print('-' * 20 + f'\nexecutando parte {k}:\n', sql)
        with ENGINE.connect() as conn:
            result = conn.execute(text(sql))
            print(result)
        conn.close()
        print('fim parcial...', time.asctime())
    ENGINE.dispose()
    print('fim sqls...', time.asctime())


def manutencao_tabelas(sql_tabelas: str, db: str):
    print('Inicio sqlTabelas:', time.asctime())
    ENGINE = create_engine(url=db, echo=True)
    for k, sql in enumerate(sql_tabelas.split(';')):
        if not sql.strip():
            continue
        print('-' * 20 + f'\nexecutando parte {k}:\n', sql)
        with ENGINE.connect() as conn:
            result = conn.execute(text(sql))
            print(result)
        conn.close()
        print('fim parcial...', time.asctime())
    ENGINE.dispose()
    print('fim sqlTabelas...', time.asctime())


def extrair_arquivo():
    for arq in ARQUIVOS_COMPACTADOS:
        print('Descompactando ' + arq)
        with zipfile.ZipFile(arq, 'r') as zip_ref:
            zip_ref.extractall(PASTA_SAIDA)


def main(env=None):
    env = 'prod'
    load_dotenv('.env.local')

    if env == 'hom':
        load_dotenv('.env.hom')
    elif env == 'prod':
        load_dotenv('.env')

    DB_NAME: str = os.getenv('DB_NAME')
    USERNAME: str = os.getenv('DB_USERNAME')
    PASSWORD: str = os.getenv('DB_PASSWORD')
    HOST: str = os.getenv('DB_HOST')

    db = f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}/{DB_NAME}'

    # extrair_arquivo()

    sql_create_tables: str = '''

        CREATE TABLE IF NOT EXISTS empresas (
            cnpj_basico int4,
            razao_social VARCHAR(200) null,
            natureza_juridica VARCHAR(4) null,
            qualificacao_responsavel VARCHAR(2) null,
            capital_social VARCHAR(20) null,
            porte_empresa varchar(2) null,
            ente_federativo_responsavel VARCHAR(50) 
        );
        
        DROP TABLE IF EXISTS estabelecimentos;
        CREATE TABLE IF NOT EXISTS estabelecimentos (
            cnpj_basico int4,
            cnpj_ordem VARCHAR(4),
            cnpj_dv VARCHAR(2),
            cnpj int8,
            nome_fantasia VARCHAR(200),
            matriz_filial int4,
            codigo_situacao_cadastral VARCHAR(2),
            data_situacao_cadastral VARCHAR(8),
            codigo_motivo_situacao_cadastral VARCHAR(2),
            nome_cidade_exterior VARCHAR(200),
            pais VARCHAR(3),
            data_inicio_atividades VARCHAR(8),
            codigo_cnae int4,
            cnae_secundaria VARCHAR(1000),
            tipo_logradouro VARCHAR(20),
            logradouro VARCHAR(200),
            numero VARCHAR(10),
            complemento VARCHAR(200),
            bairro VARCHAR(200),
            cep VARCHAR(8),
            uf VARCHAR(2),
            codigo_municipio VARCHAR(4),
            ddd1 VARCHAR(4),
            telefone1 VARCHAR(8),
            ddd2 VARCHAR(4),
            telefone2 VARCHAR(8),
            ddd_fax VARCHAR(4),
            fax VARCHAR(8),
            correio_eletronico VARCHAR(200),
            situacao_especial VARCHAR(200),
            data_situacao_especial VARCHAR(8),
            tipo_situacao_cadastral int4
            
        );
        
        CREATE TABLE IF NOT EXISTS motivo (
            codigo int4,
            descricao VARCHAR(200),
            CONSTRAINT motivo_pk PRIMARY KEY (codigo)
        );       
       
        CREATE TABLE IF NOT EXISTS natureza_juridica (
            codigo int4,
            descricao VARCHAR(200),
            CONSTRAINT natureza_juridica_pk PRIMARY KEY (codigo)
        );
        
        CREATE TABLE IF NOT EXISTS pais (
            codigo VARCHAR(3),
            descricao VARCHAR(200),
            CONSTRAINT pais_pk PRIMARY KEY (codigo)
        );
        
        CREATE TABLE IF NOT EXISTS qualificacao_socio (
            codigo int4,
            descricao VARCHAR(200),
            CONSTRAINT qualificacao_socio_pk PRIMARY KEY (codigo)
        );
        
        DROP TABLE IF EXISTS simples;
        CREATE TABLE IF NOT EXISTS simples (
            cnpj_basico int4,
            opcao_simples VARCHAR(1),
            data_opcao_simples VARCHAR(8),
            data_exclusao_simples VARCHAR(8),
            opcao_mei VARCHAR(1),
            data_opcao_mei VARCHAR(8),
            data_exclusao_mei VARCHAR(8)
        );
        
        DROP TABLE IF EXISTS socios CASCADE;
        CREATE TABLE IF NOT EXISTS socios (
            cnpj_basico int4,
            identificador_de_socio VARCHAR(1),
            nome_socio VARCHAR(200),
            cnpj_cpf_socio VARCHAR(14),
            qualificacao_socio VARCHAR(2),
            data_entrada_sociedade VARCHAR(8),
            pais VARCHAR(3),
            representante_legal VARCHAR(11),
            nome_representante VARCHAR(200),
            qualificacao_representante_legal VARCHAR(2),
            faixa_etaria VARCHAR(1)
        );
    
        CREATE TABLE IF NOT EXISTS situacao_cadastral (
            codigo int4,
            descricao varchar(25) NOT NULL,
            CONSTRAINT situacao_cadastral_pk PRIMARY KEY (codigo)
        );

        DROP TABLE IF EXISTS referencia;
        CREATE TABLE IF NOT EXISTS referencia (
            referencia varchar(100) NULL,
            valor varchar(100) NULL
        );

        '''

    # manutencao_tabelas(sql_create_tables, db)

    sqls_indexes_basics: str = '''
    
        UPDATE empresas SET porte_empresa = null WHERE porte_empresa = '';        
        CREATE INDEX empresas_cnpj_basico_idx ON empresas USING btree (cnpj_basico);
        
        CREATE INDEX estabelecimentos_cnpj_basico_idx ON estabelecimentos USING btree (cnpj_basico);
        '''
    sqls_update_alter: str = '''
        
        ALTER TABLE empresas ALTER COLUMN cnpj_basico TYPE int4 USING cnpj_basico::int4;
        ALTER TABLE empresas ALTER COLUMN natureza_juridica TYPE int4 USING natureza_juridica::int4;
        ALTER TABLE empresas ALTER COLUMN porte_empresa TYPE int4 USING porte_empresa::int4;

        UPDATE estabelecimentos SET cnpj = LPAD(concat(cnpj_basico::varchar, cnpj_ordem, cnpj_dv), 14, '0')::int8;
        ALTER TABLE estabelecimentos DROP COLUMN cnpj_ordem;
        ALTER TABLE estabelecimentos DROP COLUMN cnpj_dv;      

        UPDATE estabelecimentos SET data_situacao_cadastral = null WHERE length(data_situacao_cadastral) < 8;
        ALTER TABLE estabelecimentos ALTER COLUMN codigo_situacao_cadastral TYPE int4 
            USING codigo_situacao_cadastral::int4;
        ALTER TABLE estabelecimentos ALTER COLUMN codigo_motivo_situacao_cadastral TYPE int4 
            USING codigo_motivo_situacao_cadastral::int4;
        ALTER TABLE estabelecimentos ALTER COLUMN codigo_municipio TYPE int4 USING codigo_municipio::int4;
        ALTER TABLE estabelecimentos ALTER COLUMN data_situacao_cadastral TYPE date USING data_situacao_cadastral::date;
        ALTER TABLE estabelecimentos ALTER COLUMN data_inicio_atividades TYPE date USING data_inicio_atividades::date;

        UPDATE estabelecimentos set tipo_situacao_cadastral = 1
        WHERE codigo_situacao_cadastral = 2;   

        UPDATE estabelecimentos set tipo_situacao_cadastral = 2
        WHERE codigo_situacao_cadastral <> 2;

        UPDATE estabelecimentos set tipo_situacao_cadastral = 3
        WHERE codigo_situacao_cadastral = 8 and codigo_motivo_situacao_cadastral = 1;


        UPDATE simples SET data_opcao_simples=null WHERE data_opcao_simples = '00000000';
        UPDATE simples SET data_exclusao_simples = null WHERE data_exclusao_simples = '00000000';    
        UPDATE simples SET data_opcao_mei = null WHERE data_opcao_mei = '00000000';    
        UPDATE simples SET data_exclusao_mei = null WHERE data_exclusao_mei = '00000000';    

        UPDATE simples SET opcao_simples = '1' WHERE opcao_simples = 'S';
        UPDATE simples SET opcao_simples = '0' WHERE opcao_simples = 'N';
        UPDATE simples SET opcao_mei = '1' WHERE opcao_mei = 'S';
        UPDATE simples SET opcao_mei = '0' WHERE opcao_mei = 'N';

        ALTER TABLE simples ALTER COLUMN data_opcao_simples type date using data_opcao_simples::date;
        ALTER TABLE simples ALTER COLUMN data_exclusao_simples type date using data_exclusao_simples::date;
        ALTER TABLE simples ALTER COLUMN data_opcao_mei type date using data_opcao_mei::date;
        ALTER TABLE simples ALTER COLUMN data_exclusao_mei type date using data_exclusao_mei::date;    

        ALTER TABLE simples ALTER COLUMN opcao_simples TYPE int4 USING opcao_simples::int4;
        ALTER TABLE simples ALTER COLUMN opcao_mei TYPE int4 USING opcao_mei::int4;
        ALTER TABLE simples ALTER COLUMN cnpj_basico TYPE int4 USING cnpj_basico::int4;
        '''
    sqls_tables_aux: str = '''

        DROP TABLE IF EXISTS empresas_privadas CASCADE;
        CREATE TABLE IF NOT EXISTS empresa_privada AS
            SELECT e.cnpj_basico, e.razao_social, e.natureza_juridica, e.porte_empresa 
                FROM empresa e 
                WHERE natureza_juridica BETWEEN '2046' AND '2348'; 
        CREATE INDEX empresa_privada_cnpj_basico_idx ON empresas_privadas (cnpj_basico);

        DROP TABLE IF EXISTS empresa_nao_simples;
        CREATE TABLE IF NOT EXISTS empresas_nao_simples AS
            SELECT ep.* 
                FROM empresa_privada ep 
                    LEFT JOIN simples s on ep.cnpj_basico=s.cnpj_basico
                WHERE s.cnpj_basico IS NOT NULL;

        DROP IF EXISTS estabelecimento_privado
        CREATE TABLE IF NOT EXISTS estabelecimento_privado AS
            SELECT em.cnpj_basico, es.cnpj, em.razao_social, es.nome_fantasia, es.data_inicio_atividades, em.natureza_juridica, 
                es.matriz_filial, es.codigo_situacao_cadastral, es.data_situacao_cadastral, es.tipo_situacao_cadastral,
                es.codigo_motivo_situacao_cadastral, es.codigo_cnae, es.codigo_municipio, es.uf, em.porte_empresa
                FROM empresa_privada em 
                    INNER JOIN estabelecimento es ON em.cnpj_basico = es.cnpj_basico;
        CREATE INDEX estabelecimento_privado_cnpj_basico_idx ON estabelecimento_privado USING btree (cnpj_basico);
                    
        DROP TABLE IF EXITS painel_cnpj;
        CREATE TABLE IF NOT EXISTS painel_cnpj AS
            SELECT emp.porte_empresa, ep.matriz_filial, ep.data_inicio_atividades, ep.data_situacao_cadastral, ep.tipo_situacao_cadastral, ep.codigo_municipio, 
                ep.codigo_cnae, s.opcao_simples, s.data_opcao_simples, s.data_exclusao_simples, s.opcao_mei, s.data_opcao_mei, s.data_exclusao_mei 
                FROM estabelecimento_privado ep
                    INNER JOIN empresa_privada emp ON ep.cnpj_basico = emp.cnpj_basico 
                    LEFT JOIN simples s ON ep.cnpj_basico = s.cnpj_basico; 

        DROP TABLE IF EXISTS painel_rmtc;
        CREATE TABLE IF NOT EXISTS painel_rmtc AS
            SELECT e.porte_empresa, es.matriz_filial, es.data_inicio_atividades, es.data_situacao_cadastral, es.tipo_situacao_cadastral, 
                es.codigo_municipio, es.codigo_cnae, s.opcao_simples, s.data_opcao_simples, s.data_exclusao_simples, s.opcao_mei,
                s.data_opcao_mei, s.data_exclusao_mei
                FROM empresas e
                    INNER JOIN estabelecimentos es ON e.cnpj_basico = es.cnpj_basico
                    INNER JOIN cnae c ON es.codigo_cnae = c.id_subclasse 
                    INNER JOIN municipio_ibge mi ON es.codigo_municipio = mi.cod_mn_dados_abertos 
                    LEFT JOIN simples s ON es.cnpj_basico = s.cnpj_basico 
                    RIGHT JOIN rmtc r ON es.cnpj = r.cnpj	
                WHERE es.cnpj_basico IS NOT NULL;                    
        '''
    sql_tables_dev: str = '''

        DROP TABLE IF EXISTS empresas_privadas_dev;
        CREATE TABLE IF NOT EXISTS empresas_privadas_dev AS
            SELECT * 
            FROM empresas_privadas ep  
            ORDER BY random()
            LIMIT ((SELECT count(*) FROM empresas_privadas) * 0.1); 

        DROP TABLE IF EXISTS estabelecimentos_privados_dev;
        CREATE TABLE IF NOT EXISTS estabelecimentos_privados_dev AS
            SELECT * 
            FROM estabelecimentos_privados
            ORDER BY random()
            LIMIT ((SELECT count(*) FROM estabelecimentos_privados) * 0.1); 

        DROP TABLE IF EXISTS simples_dev;
        CREATE TABLE IF NOT EXISTS simples_dev AS
            SELECT * 
            FROM simples s  
            ORDER BY random()
            LIMIT ((SELECT count(*) FROM simples) * 0.1);     
    '''
    sqls_indexes_other: str = '''
        CREATE INDEX simples_cnpj_basico_idx ON simples USING btree (cnpj_basico);
        CREATE INDEX simples_opcao_mei_idx ON simples USING btree (opcao_mei);
        CREATE INDEX simples_opcao_simples_idx ON simples USING btree (opcao_simples);  
        '''

    sqls: str = '''
    
    CREATE INDEX empresas_cnpj_basico_idx ON empresas USING btree (cnpj_basico);
	UPDATE empresas SET porte_empresa = null WHERE porte_empresa = '';
    
    ALTER TABLE empresas ALTER COLUMN natureza_juridica TYPE int4 USING natureza_juridica::int4;
    ALTER TABLE empresas ALTER COLUMN porte_empresa TYPE int4 USING porte_empresa::int4;
    
    DROP TABLE empresas_privadas CASCADE;
    CREATE TABLE empresas_privadas AS
        SELECT e.cnpj_basico, e.razao_social, e.natureza_juridica, e.porte_empresa 
            FROM empresas e 
            WHERE natureza_juridica BETWEEN '2046' AND '2348'; 
    
    CREATE INDEX empresas_privadas_cnpj_basico_idx ON empresas_privadas (cnpj_basico);
      
    DROP TABLE IF EXISTS empresas_nao_simples;
    CREATE TABLE IF NOT EXISTS empresas_nao_simples AS
        SELECT ep.* 
            FROM empresas_privadas ep 
                LEFT JOIN simples s on ep.cnpj_basico=s.cnpj_basico
            WHERE s.cnpj_basico IS NOT NULL;
    
                     
    DELETE FROM estabelecimentos e WHERE cnpj_basico = 51573369;
    INSERT INTO estabelecimentos (cnpj_basico, cnpj_ordem, cnpj_dv, matriz_filial, nome_fantasia, codigo_situacao_cadastral, data_situacao_cadastral,
                                    codigo_motivo_situacao_cadastral, nome_cidade_exterior, pais, data_inicio_atividades, codigo_cnae, cnae_secundaria,
                                    tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, codigo_municipio)
                                    VALUES(51573369, '0001', '24','1','DUDA LACERDA SHOP','02','20230726','00','','','20230726','4751201',
                                        '4742300,4723700,4756300,4781400,4763604,4729699,4763603,4755503,4763602,4763601,4753900,4782202,4789007,4744001,4789004',
                                        'RUA','ALVARO RIBEIRO','15','BLOCO 01 APT 103','PONTE PRETA','13041730','SP', '6291');

    UPDATE estabelecimentos SET cnpj = LPAD(concat(cnpj_basico::varchar, cnpj_ordem, cnpj_dv), 14, '0')::int8;

    ALTER TABLE estabelecimentos DROP COLUMN cnpj_ordem;
    ALTER TABLE estabelecimentos DROP COLUMN cnpj_dv;      
    
    UPDATE estabelecimentos SET data_situacao_cadastral = null WHERE length(data_situacao_cadastral) < 8;
    
    ALTER TABLE estabelecimentos ALTER COLUMN codigo_situacao_cadastral TYPE int4 USING codigo_situacao_cadastral::int4;
    ALTER TABLE estabelecimentos ALTER COLUMN codigo_motivo_situacao_cadastral TYPE int4 USING 
        codigo_motivo_situacao_cadastral::int4;
    ALTER TABLE estabelecimentos ALTER COLUMN codigo_municipio TYPE int4 USING codigo_municipio::int4;
    ALTER TABLE estabelecimentos ALTER COLUMN data_situacao_cadastral TYPE date USING data_situacao_cadastral::date;
    ALTER TABLE estabelecimentos ALTER COLUMN data_inicio_atividades TYPE date USING data_inicio_atividades::date;

    CREATE INDEX estabelecimento_cnpj_basico_idx ON estabelecimentos USING btree (cnpj_basico);

    UPDATE estabelecimentos set tipo_situacao_cadastral = 1
    WHERE codigo_situacao_cadastral = 2;   
     
    UPDATE estabelecimentos set tipo_situacao_cadastral = 2
    WHERE codigo_situacao_cadastral <> 2;
        
    UPDATE estabelecimentos set tipo_situacao_cadastral = 3
    WHERE codigo_situacao_cadastral = 8 and codigo_motivo_situacao_cadastral = 1;

    DROP IF EXISTS estabelecimentos_privados;
    CREATE TABLE IF NOT EXISTS estabelecimentos_privados AS
        SELECT em.cnpj_basico, es.cnpj, em.razao_social, es.nome_fantasia, es.data_inicio_atividades, em.natureza_juridica, 
            es.matriz_filial, es.codigo_situacao_cadastral, es.data_situacao_cadastral, es.tipo_situacao_cadastral,
            es.codigo_motivo_situacao_cadastral, es.codigo_cnae, es.codigo_municipio, es.uf, em.porte_empresa
            FROM empresas_privadas em 
                INNER JOIN estabelecimentos es ON em.cnpj_basico = es.cnpj_basico;


    UPDATE simples SET data_opcao_simples=null WHERE data_opcao_simples = '00000000';
    UPDATE simples SET data_exclusao_simples = null WHERE data_exclusao_simples = '00000000';    
    UPDATE simples SET data_opcao_mei = null WHERE data_opcao_mei = '00000000';    
    UPDATE simples SET data_exclusao_mei = null WHERE data_exclusao_mei = '00000000';    

    UPDATE simples SET opcao_simples = '1' WHERE opcao_simples = 'S';
    UPDATE simples SET opcao_simples = '0' WHERE opcao_simples = 'N';
    UPDATE simples SET opcao_mei = '1' WHERE opcao_mei = 'S';
    UPDATE simples SET opcao_mei = '0' WHERE opcao_mei = 'N';

    ALTER TABLE simples ALTER COLUMN data_opcao_simples type date using data_opcao_simples::date;
    ALTER TABLE simples ALTER COLUMN data_exclusao_simples type date using data_exclusao_simples::date;
    ALTER TABLE simples ALTER COLUMN data_opcao_mei type date using data_opcao_mei::date;
    ALTER TABLE simples ALTER COLUMN data_exclusao_mei type date using data_exclusao_mei::date;    

    ALTER TABLE simples ALTER COLUMN opcao_simples TYPE int4 USING opcao_simples::int4;
    ALTER TABLE simples ALTER COLUMN opcao_mei TYPE int4 USING opcao_mei::int4;
    ALTER TABLE simples ALTER COLUMN cnpj_basico TYPE int4 USING cnpj_basico::int4;

    CREATE INDEX simples_cnpj_basico_idx ON simples USING btree (cnpj_basico);
    CREATE INDEX simples_opcao_mei_idx ON simples USING btree (opcao_mei);
    CREATE INDEX simples_opcao_simples_idx ON simples USING btree (opcao_simples);  

       
    TRUNCATE empresas_privadas_dev;
    CREATE TABLE IF NOT EXISTS empresas_privadas_dev AS
        SELECT * 
        FROM empresas_privadas ep  
        ORDER BY random()
        LIMIT ((SELECT count(*) FROM empresas_privadas) * 0.1); 

    TRUNCATE estabelecimentos_privados_dev;
    CREATE TABLE IF NOT EXISTS estabelecimentos_privados_dev AS
        SELECT * 
        FROM estabelecimentos_privados
        ORDER BY random()
        LIMIT ((SELECT count(*) FROM estabelecimentos_privados) * 0.1); 
    
    TRUNCATE simples_dev;
    CREATE TABLE IF NOT EXISTS simples_dev AS
        SELECT * 
        FROM simples s  
        ORDER BY random()
        LIMIT ((SELECT count(*) FROM simples) * 0.1); 
 
    '''

    # carregaTabelaBasica('.MOTICSV', 'motivo', db)
    # carregaTabelaBasica('.NATJUCSV', 'natureza_juridica', db)
    # carregaTabelaBasica('.PAISCSV', 'pais', db)
    # carregaTabelaBasica('.QUALSCSV', 'qualificacao_socio', db)

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
        'complemento', 'bairro',
        'cep', 'uf', 'codigo_municipio',
        'ddd1', 'telefone1',
        'ddd2', 'telefone2',
        'ddd_fax', 'fax',
        'correio_eletronico',
        'situacao_especial',
        'data_situacao_especial']

    colunas_empresas: list = [
        'cnpj_basico', 'razao_social',
        'natureza_juridica',
        'qualificacao_responsavel',
        'capital_social',
        'porte_empresa',
        'ente_federativo_responsavel']

    colunas_socios: list = [
        'cnpj_basico',
        'identificador_de_socio',
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

    colunas_simples: list = [
        'cnpj_basico',
        'opcao_simples',
        'data_opcao_simples',
        'data_exclusao_simples',
        'opcao_mei',
        'data_opcao_mei',
        'data_exclusao_mei']

    # carregaTipo('empresas', '.EMPRECSV', colunas_empresas, db)
    # carregaTipo('estabelecimentos', '.ESTABELE', colunas_estabelecimento, db)
    # carregaTipo('simples', '.SIMPLES.CSV.*', colunas_simples, db)
    # carregaTipo('socios', '.SOCIOCSV', colunas_socios, db)

    # otimiza_tabelas(sqls_indexes_basics)
    # otimiza_tabelas(sqls_update_alter)
    # otimiza_tabelas(sqls_tables_aux)
    # otimiza_tabelas(sql_tables_dev)

    # %% inserir na tabela referencia

    # ENGINE.execute(f"INSERT INTO referencia (referencia, valor) VALUES ('DATA_DOWNLOAD', '{DATA_REFERENCIA}')")
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('DATA_EXTRACAO', "
    #     f"'{list(glob.glob(os.path.join(PASTA_SAIDA, '*K*')))[0].split('.')[2]}')")
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('QTDE_ESTABELECIMENTOS', "
    #     f"'{ENGINE.execute('SELECT COUNT(*) FROM estabelecimentos;').fetchone()[0]}')")
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('QTDE_ESTABELECIMENTOS_PRIVADOS', "
    #     f"'{ENGINE.execute('SELECT COUNT(*) FROM estabelecimentos_privados;').fetchone()[0]}')")
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('QTDE_EMPRESAS', "
    #     f"'{ENGINE.execute('SELECT COUNT(*) FROM empresas;').fetchone()[0]}')");
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('QTDE_EMPRESAS_PRIVADAS', "
    #     f"'{ENGINE.execute('SELECT COUNT(*) FROM empresas_privadas;').fetchone()[0]}')");
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('QTDE_SIMPLES', "
    #     f"'{ENGINE.execute('SELECT COUNT(*) FROM simples;').fetchone()[0]}')");

    # arq = 'dados-abertos/*.ESTABELE'
    # dd.read_csv(arq, sep=';', header=None, names=colunas_estabelecimento, encoding='latin1', dtype=str,
    #                           na_filter=None).to_csv()

    dd.read_csv('dados-abertos/*.SIMPLES*', sep=';', header=None, names=colunas_simples, encoding='latin1',
                      dtype=str, na_filter=None, )

    print('FIM!!!', time.asctime())


if __name__ == '__main__':
    main()
