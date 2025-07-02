import os
import time

from sqlalchemy import text

from .database import get_mongo_client, get_postgres_engine
from .etl import (
    criar_parquet,
    load_clientes,
    load_produtos,
    load_vendas,
)
from .reports import gerar_relatorios, melhor_dia_da_semana_top20


def criar_schema_db(engine):
    """Lê o arquivo .sql e executa os comandos CREATE TABLE."""
    print("Iniciando a criação do schema do banco de dados...")
    try:
        sql_file_path = os.path.join(
            os.path.dirname(__file__), "..", "sql", "create_tables.sql"
        )
        with open(sql_file_path, "r") as f:
            query = text(f.read())

        with engine.connect() as conn:
            connection = conn.begin()
            conn.execute(query)
            connection.commit()
        print("Schema do banco de dados criado com sucesso!")
    except FileNotFoundError:
        print(
            f"ERRO: Arquivo de schema não encontrado em '{sql_file_path}'. Verifique a estrutura de pastas."
        )
        raise
    except Exception as e:
        print(f"ERRO ao criar o schema do banco de dados: {e}")
        raise


def main():
    """Função principal que orquestra todo o pipeline."""
    start_time = time.time()

    # --- ETAPA 1: Conexões ---
    print("\n[ETAPA 1/4] Estabelecendo conexões com os bancos de dados...")
    try:
        pg_engine = get_postgres_engine()
        mongo_client = get_mongo_client()
        mongo_client.admin.command("ping")
        print("Conexões com PostgreSQL e MongoDB estabelecidas com sucesso.")
    except Exception as e:
        print(
            f"ERRO: Falha ao conectar com os bancos de dados. Pipeline abortado. Erro: {e}"
        )
        return

    # --- ETAPA 2: ETL (Carga de Dados) ---
    print("\n[ETAPA 2/4] Executando processos de ETL...")
    criar_schema_db(pg_engine)
    load_produtos(pg_engine)
    load_vendas(pg_engine)
    load_clientes(pg_engine, mongo_client)
    criar_parquet(pg_engine)
    print("Processos de ETL concluídos.")

    # --- ETAPA 3: Análise e Relatórios ---
    print("\n[ETAPA 3/4] Gerando análises e relatórios...")
    gerar_relatorios(pg_engine)
    melhor_dia_da_semana_top20(pg_engine)
    print("Análises e relatórios concluídos.")

    # --- ETAPA 4: Conclusão ---
    end_time = time.time()
    print(f"  Tempo total de execução: {end_time - start_time:.2f} segundos.")


if __name__ == "__main__":
    main()
