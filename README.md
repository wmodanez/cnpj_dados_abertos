# cnpj-dados-abertos
Este projeto é impirado no projeto (https://github.com/rictom/cnpj-mysql), porém algumas melhorias foram implementadas
agora apenas um script em python é responsável por baixar os arquivos de cnpj dos dados públicos da Receita Federal 
salvar em **formato parquet** que apresenta um ganho substâncial em questão de performance sobre um SGBD normal.<br>
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
python3 manipular_dados.py
```

### Os seguintes passos serão executados pelo script:
<ol>
    <li>Os arquivos serão baixados do site da Receita Federal para a pasta dados-abertos-zip;</li>
    <ol>
        <li>Esta versão baixa apenas as tabelas que sofrem alteração regularmente (Empresa, Estabelecimento, Simples, 
            Socio) e cada uma terá seus arquivos baixados e manipulados no momento em que for sendo trabalhada;</li>
        <li>Para as demais tabelas o projeto já tem os arquivos parquet criados e armazenados na pasta parquet;/base</li>
            <ol>
                <li>O arquivo com as CNAEs foi substituído por um com os dados completos não somente a subclasse;</li>
                <li>O arquivo de municípios foi substituído por um contendo os dados completos do IBGE, onde foi incluído 
                    também o georeferenciamento dos municípios. Foi feita a relação para os códigos utilizados na tabela 
                    Estabelecimento;</li>
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

## Dockerfile
Na versão 1.3.0 foi criado o arquivo docker para facilitar a utilização da aplicação, para executála siga os seguintes passos:
* Execute o comando abaixo para criação da imagem com o projeto:

```sudo docker build . -t cnpj```
* Execute o comando abaixo para criação de um container

```sudo docker run -it temp```
* Execute o comando abaixo para executar a aplicação.

```python3 maniupular_dados.py```

## Implementações futuras
* Geração do arquivo com os logs da aplicação
* Download dos arquivos em paralelo

## Histórico de versões
<table><thead>
  <tr>
    <th>Versão</th>
    <th>Data</th>
    <th>Descrição</th>
  </tr></thead>
<tbody>
    <tr>
    <td>1.3.3</td>
    <td>28/08/2024</td>
    <td>Alteração para atender a nova forma de organização dos dados no site do dados abertos</td>
  </tr>
    <tr>
    <td>1.3.2</td>
    <td>09/08/2024</td>
    <td>Remoção da indexação feita no campo cnpj_basico</td>
  </tr>
  <tr>
  <tr>
    <td>1.3.1</td>
    <td>01/08/2024</td>
    <td>Inclusão do arquivo Dockerfile</td>
  </tr>
  <tr>
  <tr>
    <td>1.3.0</td>
    <td>01/08/2024</td>
    <td>Criação do arquivo Dockerfile</td>
  </tr>
    <td>1.2.3</td>
    <td>01/08/2024</td>
    <td>Atualização das dependências</td>
  </tr>
  <tr>
    <td>1.2.2</td>
    <td>26/07/2024</td>
    <td>Correção do nome da variável que permitia ou não a criação do arquivo parquet final</td>
  </tr>
  <tr>
    <td>1.2.1</td>
    <td>24/07/2024</td>
    <td>Update README.md</td>
  </tr>
  <tr>
    <td>1.2.0</td>
    <td>24/07/2024</td>
    <td>Implementação da possibilidade de recomeço do download interrompido de um arquivo</td>
  </tr>
  <tr>
    <td>1.1.0</td>
    <td>20/06/2024</td>
    <td>Padronização PEP8, inclusão da verificação das pastas básicas para o projeto e atualização das dependências</td>
  </tr>
  <tr>
    <td>1.0.0</td>
    <td>14/06/2024</td>
    <td>Versão inicial</td>
  </tr>
</tbody>
</table>
