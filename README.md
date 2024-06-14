# cnpj-parquet
Este projeto é um fork do projeto (https://github.com/rictom/cnpj-mysql), porém algumas melhorias foram implementadas
agora apenas um script em python é responsável por baixar os arquivos de cnpj dos dados públicos da Receita Federal 
salvar em formato **formato parquet** que apresenta um ganho substâncial em questão de performance sobre um SGBD normal.<br>
O código é compatível com o layout das tabelas disponibilizadas pela Receita Federal a partir de 2021.

## Dados públicos de CNPJ no site da Receita:
Os arquivos csv zipados com os dados de CNPJs estão disponíveis em (http://200.152.38.155/CNPJ/).


## Pré-requisitos:
Python 3.10;<br>
O arquivo requirements.txt contém todas as dependências do projeto, para instalar utilize o código abaixo: <br>
```
pip install -r requirements.txt
```

## Utilizando o script:
O comando abaixo faz todo o trabalho:<br>

```
python manipular_dados.py
```

### Os seguintes passos serão executados pelo script:
<ol>
    <li>Os arquivos serão baixados do site da Receita Federal para a pasta dados-abertos-zip;</li>
    <ol>
        <li>Esta versão baixa apenas as tabelas que sofrem alteração regularmente (Empresa, Estabelecimento, Simples, 
            Socio) e cada uma terá seus arquivos baixados e manipulados no momento em que for sendo trabalhada;</li>
        <li>Para as demais tabelas o projeto já tem os arquivos parquet criados e armazenados na pasta parquet/base</li>
            <ol>
                <li>O arquivo com as CNAEs foi substituído por um com os dados completos não somente a subclasse;</li>
                <li>O arquivo de municípios foi substituído por um contendo os dados completos do IBGE, onde foi incluído 
                    também o georeferenciamento dos municípios. Foi feita a relação para os códigos utilizados na tabela 
                    Estabelecimento</li>
            </ol>
    </ol>
    <li>Os arquivos serão descompactados na pasta dados-abertos;</li>
    <li>Será feita a manipulação dos dados;</li>
    <li>Serão criados os arquivos parquet na pasta parquet/YYYMM, onde: YYYYMM é o ano e mês que o script esta sendo 
        executado; </li>
    <li>Os arquivos descompactados serão removidos;</li>
    <li>Um arquivo chamado cnpj.duckdb será criado e nele ficarão todas as tabelas criadas anteriormente em parquet;</li>
    <li>Por fim os arquivos parquet criados serão apagados.</li>
</ol>

#### Obs.: Esta versão faz a verificação se os arquivos que estão sendo baixados da RF já estão na máquina de destino e se eles são os mais recentes ou não antes de tentar baixar.

Esta versão utiliza bibliotecas como **Dask** para o tratamento dos dados, execução com paralelismo e criação dos arquivos
parquet. Usa também o **DuckDB** para reunir esses arquivos parquet em um só arquivo que pode ser acessado via DuckDB ou DBeaver.

Sem levar em consideração a baixa dos arquivos toda execução durou cerca de 23 minutos em um notebook i5 de 9a geração com <br>
Windows 11 e o dask configurado da seguinte forma: processes=3 threads=6, memory=31.78 GiB e o duckdb com threads=4.

## Histórico de versões
versão 1.0 (julho/2024)
- primeira versão
