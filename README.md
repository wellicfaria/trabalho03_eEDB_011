# trabalho03_eEDB_011

# Título do projeto

Um parágrafo da descrição do projeto vai aqui

## 🚀 Começando

Defininos as seguintes camandas: 

- Raw: Pasta de Codigos para dados brutos
- Trusted: Pasta de Codigo para Dados tratados
- Refined: Pasta de Codigo para Dados Dados tratados e modelados
- Data: Pasta para armazenar dados
-- Source: Dados para extração
-- Sink: Para de escrita dos códigos
-- stage: Pasta temporaria para processamento raw

Consulte **Implantação** para saber como implantar o projeto.

### 📋 Requerimentos

De que coisas você precisa para instalar o software e como instalá-lo?

```
        Leitura das Fontes:
            - Leitura de um csv
                - Escreve na RAW 
            - Leitura de uma API
                 - Escreve na RAW
        Limpar os dados:
            - Le os dados das RAW, escreve na  pasta TRUSTED fazendo a LIMPEZA DOS DADOS.
        Consumo:
            - Do TRUSTED, se insere no banco de dados realizando modelagem(star schema). Sendo categoriazado como refined, modelo STAR SCHEMA em SQL no banco nomeado como DW, 
        Camanda de Visualização: 
            - 3 gráficos desenhados no grafana.  
```



### 🔧 Instalação

Uma série de exemplos passo-a-passo que informam o que você deve executar para ter um ambiente de desenvolvimento em execução.

0) Primeiro de tudo, você deve:

* Instalar o [VS Code](https://code.visualstudio.com/)
* Abra o projeto no VS Code.
* Seguir esse tutorial para configurar Docker no VS code: [Developing inside a Container](https://code.visualstudio.com/docs/remote/containers) 
* Apertar 'ctrl + P', dentro da caixa aberta digite '> open Folder in container'. 

1) Pegar o IP do banco de dados Mysql: 

```
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' db_mysql
```

Alterar o arquivo refined/util/dbmysql.py com ip do banco, se necessário. 


2) Instalando as Dependências do projeto:
```

pip install -r /workspaces/trabalho03_eEDB_011/requirements.txt

```

3) Instalar e rodar o AirFlow:


Executar os comandos:
```
    pip install "apache-airflow[celery]==2.3.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.3/constraints-3.7.txt"
    airflow standalone
```

A fim de alterar o local das pastas DAGs e LOGs, cria-se as pastas em sua raíz:
```
    cd /workspaces/trabalho03_eEDB_011
    mkdir dags
    mkdir logs
```
Logo em seguida, altera-se o caminho da pasta DAGs e LOGs no arquivo `airflow.cfg` utilizando o seu editor de preferência (aqui está sendo utilizado o Visual Studio Code):
```
    code /home/vscode/airflow/airflow.cfg
```
Alterar a linha
```
dags_folder = /home/vscode/airflow/dags
```
para:
```
dags_folder = /workspaces/trabalho03_eEDB_011/dags
```

Alterar também:
```
base_log_folder = /home/vscode/airflow/logs
base_log_folder = /workspaces/trabalho03_eEDB_011/logs
```

Abrir um terminal e criar um usuário

```
airflow users create \
    --username airflow \
    --firstname Wellington \
    --lastname Faria \
    --role Admin \
    --password airflow \
    --email wellicfaria@gmail.com
```

* Link: http://localhost:8080/home
* Usuário: airflow
* Senha: airflow

### 🔩 Analise os testes de ponta a ponta


Acessando do banco para fazer SQL: 
Senha=123456\

```
docker exec -it  db_mysql bash

mysql -uroot -p
```

## 🛠️ Construído com

* Docker
* Python
* Pyspark
* Mysql
* Grafana
* dbt
* Airflow

## ✒️ Autores

* [Vitor Marques](https://github.com/vitormrqs)
* [Wellington Cassio Faria](https://github.com/wellicfaria)
* [Rodrigo Vitorino](https://github.com/digaumlv)
* [Thais Nabe](https://github.com/thaisnabe)
* [Wesley Lourenço Barbosa](https://github.com/wesleyloubar)