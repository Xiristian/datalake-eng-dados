Warehouse com Iceberg PySpark 
======

## Pré-requisitos:

- Docker Compose

## Roteiro:

O armazenamento de objetos MinIO, o servidor PostgreSQL e o aplicativo PySpark são gerenciados por meio do `docker-compose`,
Você pode iniciar o projeto com o comando `up`:

```
docker-compose up -d
```

- O servidor do Minio estará acessível em http://127.0.0.1:9001. login: `minioadmin`. senha: `minioadmin`.
- O servidor PostgreSQL estará disponível em 127.0.0.1:5432 (`postgres`/`postgres`). O banco de dados é `iceberg_db`.

Você pode editar [main.py](iceberg-pyspark/main.py) e descomentar os métodos que deseja neste arquivo para escrever/ler ou editar a tabela Iceberg.

Para reiniciar o aplicativo PySpark após modificações no código:

```
docker-compose run --rm pyspark_app
```
