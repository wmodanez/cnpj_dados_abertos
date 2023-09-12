import os
import time
import glob
import zipfile
import sqlalchemy
import pandas as pd
import dask.dataframe as dd

from datetime import datetime as dt

TIPO_BANCO: str = 'postgres'
DB_NAME: str = 'cnpj'
USERNAME: str = 'cnpj'
PASSWORD: str = '123456'
HOST: str = '127.0.0.1:5434'

PASTA_SAIDA: str = r"dados-abertos"  # esta pasta deve estar vazia.

ENGINE = sqlalchemy.create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{HOST}/{DB_NAME}')

ARQUIVOS_COMPACTADOS: list = list(glob.glob(os.path.join(r"dados-abertos-zip", r'*.zip')))
DATA_REFERENCIA: str = dt.utcfromtimestamp(os.path.getmtime(ARQUIVOS_COMPACTADOS[0])).strftime("%d/%m/%Y")


def carregaTabelaCodigo(extensaoArquivo, nomeTabela):
    arquivos: list = list(glob.glob(os.path.join(PASTA_SAIDA, '*' + extensaoArquivo)))[0]
    print('carregando tabela ' + arquivos)
    dtab = pd.read_csv(arquivos, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo', 'descricao'])
    dtab.to_sql(nomeTabela, ENGINE, if_exists='append', index=None)
    ENGINE.execute(f'CREATE INDEX idx_{nomeTabela} ON {nomeTabela}(codigo);')


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
    # extrair_arquivo()

    sql_tabelas: str = '''
    
        DROP TABLE if exists cnae CASCADE;
        CREATE TABLE cnae (
            codigo VARCHAR(7),
            descricao VARCHAR(200),
            CONSTRAINT cnae_pk PRIMARY KEY (codigo)
        );
        
        DROP TABLE if exists empresas CASCADE;
        CREATE TABLE empresas (
            cnpj_basico VARCHAR(8),
            razao_social VARCHAR(200),
            natureza_juridica VARCHAR(4),
            qualificacao_responsavel VARCHAR(2),
            capital_social_str VARCHAR(20),
            porte_empresa VARCHAR(2),
            ente_federativo_responsavel VARCHAR(50)
        );
        
        DROP TABLE if exists estabelecimento CASCADE;
        CREATE TABLE estabelecimento (
            cnpj_basico VARCHAR(8),
            cnpj_ordem VARCHAR(4),
            cnpj_dv VARCHAR(2),
            matriz_filial VARCHAR(1),
            nome_fantasia VARCHAR(200),
            situacao_cadastral VARCHAR(2),
            data_situacao_cadastral VARCHAR(8),
            motivo_situacao_cadastral VARCHAR(2),
            nome_cidade_exterior VARCHAR(200),
            pais VARCHAR(3),
            data_inicio_atividades VARCHAR(8),
            cnae_fiscal VARCHAR(7),
            cnae_fiscal_secundaria VARCHAR(1000),
            tipo_logradouro VARCHAR(20),
            logradouro VARCHAR(200),
            numero VARCHAR(10),
            complemento VARCHAR(200),
            bairro VARCHAR(200),
            cep VARCHAR(8),
            uf VARCHAR(2),
            municipio VARCHAR(4),
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
        
        DROP TABLE if exists motivo CASCADE;
        CREATE TABLE motivo (
            codigo VARCHAR(2),
            descricao VARCHAR(200),
            CONSTRAINT motivo_pk PRIMARY KEY (codigo)
        );
        
        DROP TABLE if exists municipio CASCADE;
        CREATE TABLE municipio (
            codigo VARCHAR(4),
            descricao VARCHAR(200),
            CONSTRAINT municipio_pk PRIMARY KEY (codigo)
        );
        
        DROP TABLE if exists natureza_juridica CASCADE;
        CREATE TABLE natureza_juridica (
            codigo VARCHAR(4),
            descricao VARCHAR(200),
            CONSTRAINT natureza_juridica_pk PRIMARY KEY (codigo)
        );
        
        DROP TABLE if exists pais CASCADE;
        CREATE TABLE pais (
            codigo VARCHAR(3),
            descricao VARCHAR(200),
            CONSTRAINT pais_pk PRIMARY KEY (codigo)
        );
        
        DROP TABLE if exists qualificacao_socio CASCADE;
        CREATE TABLE qualificacao_socio (
            codigo VARCHAR(2),
            descricao VARCHAR(200),
            CONSTRAINT qualificacao_socio_pk PRIMARY KEY (codigo)
        );
        
        DROP TABLE if exists simples CASCADE;
        CREATE TABLE simples (
            cnpj_basico VARCHAR(8),
            opcao_simples VARCHAR(1),
            data_opcao_simples VARCHAR(8),
            data_exclusao_simples VARCHAR(8),
            opcao_mei VARCHAR(1),
            data_opcao_mei VARCHAR(8),
            data_exclusao_mei VARCHAR(8),
            CONSTRAINT simples_pk PRIMARY KEY (cnpj_basico)
        );
        
        DROP TABLE if exists socios_original CASCADE;
        CREATE TABLE socios_original (
            cnpj_basico VARCHAR(8),
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
    
        DROP TABLE if exists situacao_cadastral CASCADE;   
        CREATE TABLE situacao_cadastral (
            codigo varchar(2),
            descricao varchar(25) NOT NULL,
            CONSTRAINT situacao_cadastral_pk PRIMARY KEY (codigo)
        );
    
        '''

    # manutencao_tabelas(sql_tabelas)

    # carregaTabelaCodigo('.CNAECSV', 'cnae')
    # carregaTabelaCodigo('.MOTICSV', 'motivo')
    # carregaTabelaCodigo('.MUNICCSV', 'municipio')
    # carregaTabelaCodigo('.NATJUCSV', 'natureza_juridica')
    # carregaTabelaCodigo('.PAISCSV', 'pais')
    # carregaTabelaCodigo('.QUALSCSV', 'qualificacao_socio')

    colunas_estabelecimento: list = [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial',
        'nome_fantasia',
        'situacao_cadastral',
        'data_situacao_cadastral',
        'motivo_situacao_cadastral',
        'nome_cidade_exterior',
        'pais',
        'data_inicio_atividades',
        'cnae_fiscal',
        'cnae_fiscal_secundaria',
        'tipo_logradouro',
        'logradouro',
        'numero',
        'complemento', 'bairro',
        'cep', 'uf', 'municipio',
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
        'capital_social_str',
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

    carregaTipo('estabelecimento', '.ESTABELE', colunas_estabelecimento)
    carregaTipo('socios_original', '.SOCIOCSV', colunas_socios)
    carregaTipo('empresas', '.EMPRECSV', colunas_empresas)
    carregaTipo('simples', '.SIMPLES.CSV.*', colunas_simples)

    sqls: str = '''
    
    ALTER TABLE empresas ADD COLUMN capital_social DECIMAL(18,2);
    
    UPDATE empresas
    set capital_social = cast(REPLACE(capital_social_str,',', '.') AS DECIMAL(18,2));
    ALTER TABLE empresas DROP COLUMN capital_social_str;
    
    -- Exclusão de registros duplicados na tabela de empresas
    DELETE FROM empresas
        WHERE cnpj_basico='42938862' AND razao_social='';
    
    DELETE FROM empresas 
        WHERE cnpj_basico='11895269' AND razao_social='';
    
    DELETE FROM empresas 
        WHERE cnpj_basico='09346122' AND razao_social='';
    	
    delete from empresas 
    where cnpj_basico = '35442861';
    
    insert into empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, porte_empresa, ente_federativo_responsavel, capital_social)
        values('35442861', 'ANTONIO CLAUDIO MONTEIRO LIMA', '2135', '50', '01', '', 10000.00);
    
    ALTER TABLE estabelecimento ADD COLUMN cnpj VARCHAR(14);
    Update estabelecimento
    set cnpj = CONCAT(cnpj_basico, cnpj_ordem,cnpj_dv);
    
    CREATE INDEX idx_estabelecimento_cnpj ON estabelecimento (cnpj);
    CREATE INDEX idx_estabelecimento_cnpj_basico ON estabelecimento (cnpj_basico);
    
    CREATE INDEX idx_empresas_cnpj_basico ON empresas (cnpj_basico);
    CREATE INDEX idx_empresas_razao_social ON empresas (razao_social);
    CREATE INDEX idx_empresas_natureza_juridica ON public.empresas (natureza_juridica);
    
    CREATE INDEX idx_socios_original_cnpj_basico
    ON socios_original(cnpj_basico);
    
    DROP TABLE IF EXISTS socios CASCADE;
    
    CREATE TABLE socios AS 
    SELECT te.cnpj as cnpj, ts.*
    from socios_original ts
    left join estabelecimento te on te.cnpj_basico = ts.cnpj_basico
    where te.matriz_filial='1';
    
    DROP TABLE IF EXISTS socios_original;
    
    CREATE INDEX idx_socios_cnpj ON socios(cnpj);
    CREATE INDEX idx_socios_cnpj_basico ON socios(cnpj_basico);
    CREATE INDEX idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio);
    CREATE INDEX idx_socios_nome_socio ON socios(nome_socio);
    
    CREATE INDEX idx_simples_cnpj_basico ON simples(cnpj_basico);
    
    DROP TABLE IF EXISTS _referencia;
    CREATE TABLE _referencia (
        referencia	VARCHAR(100),
        valor		VARCHAR(100)
    );
    
    INSERT INTO situacao_cadastral (codigo,descricao)
        VALUES ('01','NULA'),
        ('02','ATIVA'),
        ('03','SUSPENSA'),
        ('04','INAPTA'),
        ('08','BAIXADA');
    
    ALTER TABLE empresas ADD CONSTRAINT empresas_pk PRIMARY KEY (cnpj_basico);
    ALTER TABLE empresas ADD CONSTRAINT empresas_natureza_juridica_fk FOREIGN KEY (natureza_juridica) REFERENCES natureza_juridica(codigo);
    
    ALTER TABLE estabelecimento ADD CONSTRAINT estabelecimento_pk PRIMARY KEY (cnpj_basico,cnpj_ordem,cnpj_dv);
    ALTER TABLE estabelecimento ADD CONSTRAINT estabelecimento_cnae_fk FOREIGN KEY (cnae_fiscal) REFERENCES cnae(codigo);
    ALTER TABLE estabelecimento ADD CONSTRAINT estabelecimento_municipio_fk FOREIGN KEY (municipio) REFERENCES municipio(codigo);
    ALTER TABLE estabelecimento ADD CONSTRAINT estabelecimento_empresa_fk FOREIGN KEY (cnpj_basico) REFERENCES empresas(cnpj_basico);
    ALTER TABLE estabelecimento ADD CONSTRAINT estabelecimento_situacao_cadastral_fk FOREIGN KEY (situacao_cadastral) REFERENCES situacao_cadastral(codigo);
    
    ALTER TABLE public.natureza_juridica ADD CONSTRAINT natureza_juridica_pk PRIMARY KEY (codigo);

    CREATE MATERIALIZED VIEW public.cidades_go
    TABLESPACE pg_default
    AS SELECT c.id AS codigo_ibge,
        c.nome,
        c.uf
       FROM cidade c
      WHERE c.uf = '52'
    WITH DATA;    
    
    CREATE MATERIALIZED VIEW public.estabelecimento_go
        AS SELECT es.*, em.razao_social, em.natureza_juridica, em.qualificacao_responsavel, em.porte_empresa, em.capital_social, m.descricao
           FROM estabelecimento es
         JOIN empresas em ON es.cnpj_basico = em.cnpj_basico
         JOIN municipio m ON es.municipio = m.codigo
      WHERE es.uf = 'GO'
    WITH DATA;
    
    CREATE MATERIALIZED VIEW public.estabelecimento_go_ibge
        AS SELECT eg.*, c.codigo_ibge
           FROM estabelecimento_go eg
         LEFT JOIN cidades_go c ON upper(c.nome) = eg.descricao
    WITH DATA;
    '''

    otimiza_tabelas(sqls)

    # %% inserir na tabela referencia_

    ENGINE.execute(f"insert into _referencia (referencia, valor) values ('DATA_DOWNLOAD', '{DATA_REFERENCIA}')")
    ENGINE.execute(
        f"insert into _referencia (referencia, valor) values ('DATA_EXTRACAO', "
        f"'{list(glob.glob(os.path.join(PASTA_SAIDA, 'K*')))[0].split('.')[2]}')")
    ENGINE.execute(
        f"insert into _referencia (referencia, valor) values ('QTDE_ESTABELECIMENTOS', "
        f"'{ENGINE.execute('select count(*) as contagem from estabelecimento;').fetchone()[0]}')")
    ENGINE.execute(
        f"insert into _referencia (referencia, valor) values ('QTDE_EMPRESAS', "
        f"'{ENGINE.execute('SELECT COUNT(*) FROM empresas;').fetchone()[0]}')");
    ENGINE.execute(
        f"insert into _referencia (referencia, valor) values ('QTDE_SOCIOS', "
        f"'{ENGINE.execute('SELECT COUNT(*) FROM socios;').fetchone()[0]}')");

    print('FIM!!!', time.asctime())


if __name__ == '__main__':
    main()
