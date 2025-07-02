import os

from pymongo import MongoClient
from sqlalchemy import create_engine, exc


def get_postgres_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB")

    if not all([user, password, db_name]):
        raise ValueError(
            "Variáveis de ambiente do PostgreSQL (USER, PASSWORD, DB) não definidas."
        )

    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    retries = 5
    delay = 2
    for i in range(retries):
        try:
            print(f"Tentando conectar ao PostgreSQL ({i + 1}/{retries})...")
            engine = create_engine(db_url)
            with engine.connect():
                print("Conexão com PostgreSQL bem-sucedida!")
            return engine
        except exc.OperationalError as e:
            print(f"Falha na conexão: {e}. Tentando novamente em {delay} segundos...")

    raise ConnectionError(
        "Não foi possível conectar ao PostgreSQL após várias tentativas."
    )


def get_mongo_client():
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")

    mongo_uri = f"mongodb://{host}:{port}/"

    client = MongoClient(mongo_uri)

    return client
