import os
import time
import glob
import zipfile
import sqlalchemy
import pandas as pd
import dask.dataframe as dd

from datetime import datetime as dt

TIPO_BANCO: str = 'postgres'
DB_NAME: str = 'imb_hom'
USERNAME: str = 'imb_hom_user'
PASSWORD: str = 'zd8qGVfTYDVIPef57fXk'
HOST: str = '10.6.2.169:5432'

PASTA_SAIDA: str = r"dados-abertos"  # esta pasta deve estar vazia.

ENGINE = sqlalchemy.create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{HOST}/{DB_NAME}')

ARQUIVOS_COMPACTADOS: list = list(glob.glob(os.path.join(r"dados-abertos-zip", r'*.zip')))
DATA_REFERENCIA: str = dt.utcfromtimestamp(os.path.getmtime(ARQUIVOS_COMPACTADOS[0])).strftime("%d/%m/%Y")


def carregaTabelaCodigo(extensaoArquivo, nomeTabela):
    arquivos: list = list(glob.glob(os.path.join(PASTA_SAIDA, '*' + extensaoArquivo)))[0]
    print('carregando tabela ' + arquivos)
    dtab = pd.read_csv(arquivos, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo', 'descricao'])
    dtab.to_sql(nomeTabela, ENGINE, if_exists='append', index=None)


def carregaTipo(nome_tabela, tipo, colunas):
    # usando dask, bem mais rápido que pandas
    arquivos: list = list(glob.glob(os.path.join(PASTA_SAIDA, '*' + tipo)))
    for arq in arquivos:
        print(f'carregando: {arq=}')
        print('lendo csv ...', time.asctime())
        ddf = dd.read_csv(arq, sep=';', header=None, names=colunas,
                          encoding='latin1', dtype=str,
                          na_filter=None)
        print('to_sql...', time.asctime())
        ddf.to_sql(nome_tabela, str(ENGINE.url), index=None, if_exists='append',
                   parallel=True, dtype=sqlalchemy.sql.sqltypes.String)
        print('fim parcial...', time.asctime())


def otimiza_tabelas(sqls):

    print('Inicio sqls:', time.asctime())
    for k, sql in enumerate(sqls.split(';')):
        if not sql.strip():
            continue
        print('-' * 20 + f'\nexecutando parte {k}:\n', sql)
        ENGINE.execute(sql)
        print('fim parcial...', time.asctime())
    print('fim sqls...', time.asctime())


def manutencao_tabelas(sql_tabelas):
    print('Inicio sqlTabelas:', time.asctime())
    for k, sql in enumerate(sql_tabelas.split(';')):
        if not sql.strip():
            continue
        print('-' * 20 + f'\nexecutando parte {k}:\n', sql)
        ENGINE.execute(sql)
        print('fim parcial...', time.asctime())
    print('fim sqlTabelas...', time.asctime())


def extrair_arquivo():
    for arq in ARQUIVOS_COMPACTADOS:
        print('Descompactando ' + arq)
        with zipfile.ZipFile(arq, 'r') as zip_ref:
            zip_ref.extractall(PASTA_SAIDA)


def main():
    # dotenv.load_dotenv('.env.local')
    sqls: str = '''
    
    CREATE INDEX empresas_cnpj_basico_idx ON empresas USING btree (cnpj_basico);

    DELETE FROM empresas
        WHERE cnpj_basico=42938862 AND razao_social='';
    
    DELETE FROM empresas 
        WHERE cnpj_basico=11895269 AND razao_social='';
    
    DELETE FROM empresas 
        WHERE cnpj_basico=09346122 AND razao_social='';
    	
    DELETE FROM empresas 
    WHERE cnpj_basico = 35442861 or cnpj_basico = 51573369;
        
    INSERT INTO  empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, porte_empresa, ente_federativo_responsavel, capital_social)
        VALUES(35442861, 'ANTONIO CLAUDIO MONTEIRO LIMA', '2135', '50', '01', '', 10000.00),
                (51573369, 'EDUARDO SIQUEIRA LACERDA',	'2135','50','01','', 100.00);            
    
    UPDATE empresas SET porte_empresa = null WHERE porte_empresa = '';
    
    ALTER TABLE empresas ALTER COLUMN natureza_juridica TYPE int4 USING natureza_juridica::int4;
    ALTER TABLE empresas ALTER COLUMN porte_empresa TYPE int4 USING porte_empresa::int4;

                                   
    DELETE FROM estabelecimento e WHERE cnpj_basico = 51573369;
    INSERT INTO estabelecimento (cnpj_basico, cnpj_ordem, cnpj_dv, matriz_filial, nome_fantasia, codigo_situacao_cadastral, data_situacao_cadastral,
                                    codigo_motivo_situacao_cadastral, nome_cidade_exterior, pais, data_inicio_atividades, codigo_cnae, cnae_secundaria,
                                    tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, codigo_municipio, ddd1, telefone1, ddd2, telefone2, ddd_fax, fax,
                                    correio_eletronico, situacao_especial, data_situacao_especial)
                                    VALUES(51573369, '0001', '24','1','DUDA LACERDA SHOP','02','20230726','00','','','20230726','4751201',
                                        '4742300,4723700,4756300,4781400,4763604,4729699,4763603,4755503,4763602,4763601,4753900,4782202,4789007,4744001,4789004',
                                        'RUA','ALVARO RIBEIRO','15','BLOCO 01 APT 103','PONTE PRETA','13041730','SP', '6291','45','99066209','','','','', 
                                        'EDUARDOLACERDA@OUTLOOK.COM','', '');
    
    UPDATE estabelecimento SET data_situacao_cadastral = null WHERE length(data_situacao_cadastral) < 8;
    
    ALTER TABLE estabelecimento ALTER COLUMN cnpj_basico TYPE int4 USING cnpj_basico::int4;
    ALTER TABLE estabelecimento ALTER COLUMN codigo_situacao_cadastral TYPE int4 USING codigo_situacao_cadastral::int4;
    ALTER TABLE estabelecimento ALTER COLUMN codigo_cnae TYPE int4 USING codigo_cnae::int4;
    ALTER TABLE estabelecimento ALTER COLUMN codigo_municipio TYPE int4 USING codigo_municipio::int4;
    ALTER TABLE estabelecimento ALTER COLUMN data_situacao_cadastral TYPE date USING data_situacao_cadastral::date;
    ALTER TABLE estabelecimento ALTER COLUMN data_inicio_atividades TYPE date USING data_inicio_atividades::date;
    ALTER TABLE estabelecimento ALTER COLUMN data_situacao_especial TYPE date USING data_situacao_especial::date;
    
    UPDATE estabelecimento SET tipo_estabelecimento = 1
        WHERE codigo_situacao_cadastral = 1;
    
    
    CREATE INDEX estabelecimento_cnpj_basico_idx ON estabelecimento USING btree (cnpj_basico);
    
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
    
    DROP TABLE IF EXISTS estabelecimentos_nao_simples;
    CREATE TABLE IF NOT EXISTS estabelecimentos_nao_simples AS
        SELECT em.cnpj_basico, es.cnpj, es.cnpj, em.razao_social, es.nome_fantasia, em.natureza_juridica, 
            es.matriz_filial, es.codigo_situacao_cadastral, es.data_situacao_cadastral, 
            es.codigo_motivo_situacao_cadastral, es.data_inicio_atividades, es.codigo_cnae, es.codigo_municipio, es.uf, 
            em.porte_empresa, es.tipo_situacao_cadastral
            FROM empresas_nao_simples em 
                INNER JOIN estabelecimento es ON em.cnpj_basico = es.cnpj_basico;
    

    DROP TABLE IF EXISTS estabelecimentos_privados;
    CREATE TABLE IF NOT EXISTS estabelecimentos_privados AS
        SELECT em.cnpj_basico, es.cnpj, em.razao_social, es.nome_fantasia, em.natureza_juridica, es.matriz_filial, 
            es.codigo_situacao_cadastral, es.data_situacao_cadastral,
            es.codigo_motivo_situacao_cadastral, es.data_inicio_atividades, es.codigo_cnae, es.codigo_municipio, 
            es.uf, em.porte_empresa, es.tipo_situacao_cadastral
            FROM empresas_privadas em 
                INNER JOIN estabelecimento es ON em.cnpj_basico = es.cnpj_basico;
        
    CREATE INDEX estabelecimentos_privados_cnpj_basico_idx ON public.estabelecimentos_privados (cnpj_basico);
    CREATE INDEX estabelecimentos_privados_codigo_cnae_idx ON public.estabelecimentos_privados (codigo_cnae);
    CREATE INDEX estabelecimentos_privados_codigo_motivo_situacao_cadastral_idx ON public.estabelecimentos_privados (codigo_motivo_situacao_cadastral);
    CREATE INDEX estabelecimentos_privados_codigo_municipio_idx ON public.estabelecimentos_privados (codigo_municipio);
    CREATE INDEX estabelecimentos_privados_codigo_situacao_cadastral_idx ON public.estabelecimentos_privados (codigo_situacao_cadastral);
    CREATE INDEX estabelecimentos_privados_data_inicio_atividades_idx ON public.estabelecimentos_privados (data_inicio_atividades);
    CREATE INDEX estabelecimentos_privados_data_situacao_cadastral_idx ON public.estabelecimentos_privados (data_situacao_cadastral);
    CREATE INDEX estabelecimentos_privados_tipo_situacao_cadastral_idx ON public.estabelecimentos_privados (tipo_situacao_cadastral);
    CREATE INDEX estabelecimentos_privados_uf_idx ON public.estabelecimentos_privados (uf);    

    create table empresas_privadas_dev as
    select * 
    from empresas_privadas ep  
    order by random()
    limit ((select count(*) from empresas_privadas)*0.1); 

    create table estabelecimentos_privados_dev as
    select * 
    from estabelecimentos_privados
    order by random()
    limit ((select count(*) from estabelecimentos_privados)*0.1); 
    
    create table simples_dev as
    select * 
    from simples s  
    order by random()
    limit ((select count(*) from simples)*0.1); 
 
    '''

    extrair_arquivo()

    sql_tabelas: str = '''
    
        CREATE TABLE IF NOT EXISTS cnae (
            codigo int4,
            descricao VARCHAR(200),
            CONSTRAINT cnae_pk PRIMARY KEY (codigo)
        );

        CREATE TABLE IF NOT EXISTS empresas (
            cnpj_basico int4,
            razao_social VARCHAR(200) null,
            natureza_juridica VARCHAR(4) null,
            qualificacao_responsavel VARCHAR(2) null,
            capital_social VARCHAR(20) null,
            porte_empresa varchar(2) null,
            ente_federativo_responsavel VARCHAR(50) 
        );
        
        CREATE TABLE IF NOT EXISTS estabelecimento (
            cnpj_basico int4,
            cnpj_ordem VARCHAR(4),
            cnpj_dv VARCHAR(2),
            nome_fantasia VARCHAR(200),
            matriz_filial VARCHAR(1),
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
            data_situacao_especial VARCHAR(8)
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

        CREATE TABLE "referencia" (
            referencia varchar(100) NULL,
            valor varchar(100) NULL
        );

        '''

    sql_tabelas = '''
            DROP TABLE IF EXISTS empresas;
            CREATE TABLE IF NOT EXISTS empresas (
            cnpj_basico int4,
            razao_social VARCHAR(200),
            natureza_juridica VARCHAR(4),
            qualificacao_responsavel VARCHAR(2),
            capital_social VARCHAR(20),
            porte_empresa varchar(2),
            ente_federativo_responsavel VARCHAR(50) 
        );
        
        DROP TABLE IF EXISTS estabelecimento;
        CREATE TABLE IF NOT EXISTS estabelecimento (
            cnpj_basico int4,
            cnpj_ordem VARCHAR(4),
            cnpj_dv VARCHAR(2),
            nome_fantasia VARCHAR(200),
            matriz_filial VARCHAR(1),
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
            cnpj BIGINT,
            tipo_estabelecimento int4
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
'''
    # manutencao_tabelas(sql_tabelas)

    # carregaTabelaCodigo('.CNAECSV', 'cnae')
    # carregaTabelaCodigo('.MOTICSV', 'motivo')
    # carregaTabelaCodigo('.NATJUCSV', 'natureza_juridica')
    # carregaTabelaCodigo('.PAISCSV', 'pais')
    # carregaTabelaCodigo('.QUALSCSV', 'qualificacao_socio')

    colunas_estabelecimento: list = [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial',
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

    # carregaTipo('empresas', '.EMPRECSV', colunas_empresas)
    # carregaTipo('estabelecimento', '.ESTABELE', colunas_estabelecimento)
    # carregaTipo('simples', '.SIMPLES.CSV.*', colunas_simples)
    # carregaTipo('socios', '.SOCIOCSV', colunas_socios)


    otimiza_tabelas(sqls)

    # %% inserir na tabela referencia_

    # ENGINE.execute(f"INSERT INTO referencia (referencia, valor) VALUES ('DATA_DOWNLOAD', '{DATA_REFERENCIA}')")
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('DATA_EXTRACAO', "
    #     f"'{list(glob.glob(os.path.join(PASTA_SAIDA, 'K*')))[0].split('.')[2]}')")
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('QTDE_ESTABELECIMENTOS', "
    #     f"'{ENGINE.execute('SELECT COUNT(*) FROM estabelecimento;').fetchone()[0]}')")
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('QTDE_EMPRESAS', "
    #     f"'{ENGINE.execute('SELECT COUNT(*) FROM empresas;').fetchone()[0]}')");
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('QTDE_SOCIOS', "
    #     f"'{ENGINE.execute('SELECT COUNT(*) FROM socios;').fetchone()[0]}')");
    # ENGINE.execute(
    #     f"INSERT INTO referencia (referencia, valor) VALUES ('QTDE_SIMPLES', "
    #     f"'{ENGINE.execute('SELECT COUNT(*) FROM simples;').fetchone()[0]}')");

    print('FIM!!!', time.asctime())


if __name__ == '__main__':
    main()
