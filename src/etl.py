import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def load_vendas(engine):
    print("Iniciando carga de vendas...")
    try:
        iterator = pd.read_csv(
            "data/vendas.csv", sep=";", chunksize=50000, dtype={"COD_ID_LOJA": str}
        )
        for chunk in iterator:
            print("Processando chunks de vendas...")

        cols_to_drop = [col for col in chunk.columns if "Unnamed" in col]
        chunk = chunk.drop(columns=cols_to_drop)
        chunk["COD_ID_CLIENTE"] = chunk["COD_ID_CLIENTE"].fillna(0).astype("int64")
        chunk["DES_TIPO_CLIENTE"] = chunk["DES_TIPO_CLIENTE"].fillna("F").astype("str")
        chunk["NUM_ANOMESDIA"] = pd.to_datetime(chunk["NUM_ANOMESDIA"], format="%Y%m%d")
        chunk[["cod_loja_extraido", "cod_cupom_extraido"]] = chunk[
            "COD_ID_VENDA_UNICO"
        ].str.split("|", expand=True)
        chunk["COD_ID_LOJA"] = chunk["COD_ID_LOJA"].astype(str)
        chunk["cod_loja_extraido"] = chunk["cod_loja_extraido"].astype(str)
        divergencias = chunk["COD_ID_LOJA"] != chunk["cod_loja_extraido"]
        chunk.loc[divergencias, "COD_ID_VENDA_UNICO"] = (
            chunk["COD_ID_LOJA"] + "|" + chunk["cod_cupom_extraido"]
        )
        chunk = chunk.drop(columns=["cod_loja_extraido", "cod_cupom_extraido"])
        chunk_sem_duplicatas = chunk.drop_duplicates(
            subset=["COD_ID_VENDA_UNICO", "COD_ID_PRODUTO"], keep="first"
        )
        chunk_final = chunk_sem_duplicatas.rename(
            columns={
                "COD_ID_VENDA_UNICO": "cod_id_venda_unico",
                "NUM_ANOMESDIA": "data_venda",
                "COD_ID_LOJA": "cod_id_loja",
                "COD_ID_CLIENTE": "cod_id_cliente",
                "DES_TIPO_CLIENTE": "des_tipo_cliente",
                "DES_SEXO_CLIENTE": "des_sexo_cliente",
                "COD_ID_PRODUTO": "cod_id_produto",
                "VAL_VALOR_SEM_DESC": "val_valor_sem_desc",
                "VAL_VALOR_DESCONTO": "val_valor_desconto",
                "VAL_VALOR_COM_DESC": "val_valor_com_desc",
                "VAL_QUANTIDADE_KG": "val_quantidade_kg",
            }
        )
        chunk_final.to_sql(
            "vendas",
            con=engine,
            schema="public",
            if_exists="append",
            index=False,
            method="multi",
        )
        print("Carga de vendas concluída com sucesso!")
    except FileNotFoundError:
        print("ERRO: Arquivo 'data/vendas.csv' não encontrado.")
    except Exception as e:
        print(f"ERRO inesperado ao carregar vendas: {e}")


def load_produtos(engine):
    print("Iniciando carga de produtos...")
    try:
        produtos = pd.read_csv("data/produtos.csv", sep=";")
        produtos["DES_UNIDADE"] = produtos["DES_UNIDADE"].str.upper()
        produtos["COD_CODIGO_BARRAS"] = pd.to_numeric(
            produtos["COD_CODIGO_BARRAS"], errors="coerce"
        )
        produtos["COD_CODIGO_BARRAS"] = (
            produtos["COD_CODIGO_BARRAS"].fillna(pd.NA).astype("Int64")
        )
        produtos_rename = produtos.rename(
            columns={
                "COD_ID_PRODUTO": "cod_id_produto",
                "COD_ID_CATEGORIA_PRODUTO": "cod_id_categoria_produto",
                "ARR_CATEGORIAS_PRODUTO": "arr_categorias_produto",
                "DES_PRODUTO": "des_produto",
                "DES_UNIDADE": "des_unidade",
                "COD_CODIGO_BARRAS": "cod_codigo_barras",
            }
        )
        produtos_rename.to_sql(
            "produtos",
            con=engine,
            schema="public",
            if_exists="replace",
            index=False,
            method="multi",
        )
        print("Carga de produtos concluída com sucesso!")
    except FileNotFoundError:
        print("ERRO: Arquivo 'data/produtos.csv' não encontrado.")
    except Exception as e:
        print(f"ERRO inesperado ao carregar produtos: {e}")


def load_clientes(pg_engine, mongo_client):
    print("Iniciando carga de clientes...")
    try:
        df_clientes = pd.read_json("data/clientes.json")
        df_clientes["DAT_DATA_NASCIMENTO"] = pd.to_datetime(
            df_clientes["DAT_DATA_NASCIMENTO"], format="%Y-%m-%d"
        )
        df_clientes_final = df_clientes.rename(
            columns={
                "COD_ID_CLIENTE": "cod_id_cliente",
                "DES_TIPO_CLIENTE": "des_tipo_cliente",
                "NOM_NOME": "nom_nome",
                "DES_SEXO_CLIENTE": "des_sexo_cliente",
                "DAT_DATA_NASCIMENTO": "dat_data_nascimento",
            }
        )
        df_clientes_final.to_sql(
            "clientes",
            con=pg_engine,
            schema="public",
            if_exists="replace",
            index=False,
            method="multi",
        )
        lista_clientes = df_clientes_final.to_dict(orient="records")

        db = mongo_client["mercafacil"]
        collection = db["clientes"]
        collection.delete_many({})
        collection.insert_many(lista_clientes)
    except FileNotFoundError:
        print("ERRO: Arquivo 'data/produtos.csv' não encontrado.")
    except Exception as e:
        print(f"ERRO inesperado ao carregar produtos: {e}")


def criar_parquet(engine):
    """
    Cria um dataset unificado e o salva em Parquet de forma otimizada,
    lendo o banco de dados em chunks para não esgotar a memória.
    """
    print("Iniciando criação do dataset Parquet...")

    query_unificada = """
        SELECT v.*, p.des_produto, c.nom_nome AS nom_cliente
        FROM public.vendas AS v
        LEFT JOIN public.produtos AS p ON v.cod_id_produto = p.cod_id_produto
        LEFT JOIN public.clientes AS c ON v.cod_id_cliente = c.cod_id_cliente;
    """

    chunksize = 100000
    output_path = "output/vendas_enriquecidas"
    parquet_writer = None

    try:
        print("  Lendo dados do banco em chunks...")

        with engine.connect().execution_options(stream_results=True) as connection:
            for i, chunk_df in enumerate(
                pd.read_sql_query(query_unificada, connection, chunksize=chunksize)
            ):
                print(f"    Escrevendo chunk Parquet {i + 1}...")

                chunk_df["data_venda"] = pd.to_datetime(chunk_df["data_venda"])
                chunk_df["ano"] = chunk_df["data_venda"].dt.year
                chunk_df["mes"] = chunk_df["data_venda"].dt.month

                table = pa.Table.from_pandas(chunk_df, preserve_index=False)

                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(
                        output_path, table.schema, partition_cols=["ano", "mes"]
                    )

                parquet_writer.write_table(table)

        print("  Dataset Parquet criado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao criar o dataset Parquet: {e}")
    finally:
        if parquet_writer:
            parquet_writer.close()
