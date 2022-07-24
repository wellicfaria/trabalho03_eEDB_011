# trabalho03_eEDB_011


Instalando as Dependencias do projeto:
```

pip install -r /workspaces/trabalho03_eEDB_011/requirements.txt

```

AirFlow:


Executar os comandos:
```
    chmod 777 start_airflow.sh
    ./ start_airflow.sh
```

Abrir um terminal e criar um usuário:

```
airflow users create \
    --username airflow \
    --firstname Wellington \
    --lastname Faria \
    --role Admin \
    --password airflow \
    --email wellicfaria@gmail.com
```

Link: http://localhost:8080/home
Usuário: airflow
Senha: airflow