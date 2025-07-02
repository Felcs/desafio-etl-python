import pandas as pd


def gerar_relatorios(engine):
    """
    Executa todas as queries de análise do passo 2 e salva os resultados em arquivos CSV.
    """
    print("Iniciando geração de relatórios...")

    # a. PRODUTOS VENDIDOS
    query_produtos_vendidos = """
        SELECT p.des_produto,
        SUM(val_quantidade_kg) AS total_vendido
        from vendas v
        join produtos p on v.cod_id_produto = p.cod_id_produto 
        group by des_produto  
        order by total_vendido desc;
    """
    nome_arquivo = "output/produtos_mais_vendidos.csv"

    try:
        print(f"  Gerando relatório: {nome_arquivo}...")

        df = pd.read_sql_query(query_produtos_vendidos, engine)

        df.to_csv(nome_arquivo, index=False)

        print(f"  Relatório '{nome_arquivo}' gerado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao gerar o relatório '{nome_arquivo}': {e}")

    # b. TOTAL COMPRAS
    query_total_compras = """
        select c.nom_nome,
        COUNT(v.cod_id_cliente) as total_compras
        from vendas v 
        join clientes c on c.cod_id_cliente = v.cod_id_cliente
        group by c.cod_id_cliente, c.nom_nome 
        order by total_compras desc;
    """
    nome_arquivo = "output/clientes_mais_compras.csv"

    try:
        print(f"  Gerando relatório: {nome_arquivo}...")

        df = pd.read_sql_query(query_total_compras, engine)

        df.to_csv(nome_arquivo, index=False)

        print(f"  Relatório '{nome_arquivo}' gerado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao gerar o relatório '{nome_arquivo}': {e}")

    # b. TOTAL DE COMPRAS ÚNICAS
    query_total_vendas_unicas = """
        select c.nom_nome,
        COUNT(v.cod_id_venda_unico) as total_vendas_unicas
        from vendas v
        join clientes c on c.cod_id_cliente = v.cod_id_cliente 
        group by v.cod_id_venda_unico, c.nom_nome
        order by total_vendas_unicas desc;
    """
    nome_arquivo = "output/clientes_mais_compras_unicas.csv"

    try:
        print(f"  Gerando relatório: {nome_arquivo}...")

        df = pd.read_sql_query(query_total_vendas_unicas, engine)

        df.to_csv(nome_arquivo, index=False)

        print(f"  Relatório '{nome_arquivo}' gerado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao gerar o relatório '{nome_arquivo}': {e}")

    # c. QUANTIDADE DE VENDAS POR DIA
    query_vendas_por_dia = """
        select v.data_venda,
        COUNT(distinct cod_id_venda_unico) as quantidades_vendas_unicas
        from vendas v 
        group by data_venda 
        order by quantidades_vendas_unicas desc;
    """
    nome_arquivo = "output/vendas_por_dia.csv"

    try:
        print(f"  Gerando relatório: {nome_arquivo}...")

        df = pd.read_sql_query(query_vendas_por_dia, engine)

        df.to_csv(nome_arquivo, index=False)

        print(f"  Relatório '{nome_arquivo}' gerado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao gerar o relatório '{nome_arquivo}': {e}")

    # d. QUANTIDADE DE PRODUTOS DISTINTOS VENDIDOS POR DIA
    query_produtos_distintos = """
        select v.data_venda,
        COUNT(distinct v.cod_id_produto ) as produtos_vendas_distintas
        from vendas v 
        group by data_venda 
        order by produtos_vendas_distintas desc;
    """
    nome_arquivo = "output/produtos_distintos_por_dia.csv"

    try:
        print(f"  Gerando relatório: {nome_arquivo}...")

        df = pd.read_sql_query(query_produtos_distintos, engine)

        df.to_csv(nome_arquivo, index=False)

        print(f"  Relatório '{nome_arquivo}' gerado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao gerar o relatório '{nome_arquivo}': {e}")

    # e. TOTAL DE DESCONTO DADO POR PRODUTO
    query_maior_desconto_total = """
        select p.des_produto,
        SUM(v.val_valor_desconto) as total_desconto
        from vendas v 
        join produtos p on p.cod_id_produto = v.cod_id_produto
        group by p.des_produto
        order by total_desconto desc;
    """
    nome_arquivo = "output/maior_desconto_total.csv"

    try:
        print(f"  Gerando relatório: {nome_arquivo}...")

        df = pd.read_sql_query(query_maior_desconto_total, engine)

        df.to_csv(nome_arquivo, index=False)

        print(f"  Relatório '{nome_arquivo}' gerado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao gerar o relatório '{nome_arquivo}': {e}")

    # e. MÁXIMO DE DESCONTO DADO EM UM PRODUTO
    query_max_desconto_item = """
        select p.des_produto,
        MAX(v.val_valor_desconto) as maximo_desconto
        from vendas v 
        join produtos p on p.cod_id_produto = v.cod_id_produto
        group by p.des_produto
        order by maximo_desconto desc;
    """
    nome_arquivo = "output/maximo_desconto_por_produto.csv"

    try:
        print(f"  Gerando relatório: {nome_arquivo}...")

        df = pd.read_sql_query(query_max_desconto_item, engine)

        df.to_csv(nome_arquivo, index=False)

        print(f"  Relatório '{nome_arquivo}' gerado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao gerar o relatório '{nome_arquivo}': {e}")

    # e. MAIOR PORCENTAGEM DE DESCONTO EM UM PRODUTO (TEM UM DE 100% AÍ HEIN XD)
    query_maior_desconto_percentual = """
        select p.des_produto,
        MAX((v.val_valor_desconto / v.val_valor_sem_desc) * 100) AS max_desconto_percentual
        from vendas v 
        join produtos p on p.cod_id_produto = v.cod_id_produto
        where v.val_valor_sem_desc > '0'
        group by p.des_produto
        order by max_desconto_percentual desc;
    """
    nome_arquivo = "output/maior_desconto_percentual.csv"

    try:
        print(f"  Gerando relatório: {nome_arquivo}...")

        df = pd.read_sql_query(query_maior_desconto_percentual, engine)

        df.to_csv(nome_arquivo, index=False)

        print(f"  Relatório '{nome_arquivo}' gerado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao gerar o relatório '{nome_arquivo}': {e}")


def melhor_dia_da_semana_top20(engine):
    """
    Identifica o dia da semana com mais compras para cada um dos 20 melhores clientes
    (definidos pelo maior valor total gasto) e salva o resultado em um CSV.
    """
    print("  Iniciando análise: Melhor dia de compra por cliente...")

    try:
        query_top_20_clientes = """
            select c.cod_id_cliente
            from public.vendas as v
            join public.clientes as c ON v.cod_id_cliente = c.cod_id_cliente
            group by c.cod_id_cliente
            order by SUM(v.val_valor_com_desc) DESC
            limit 20;
        """

        df_top_20 = pd.read_sql_query(query_top_20_clientes, engine)
        lista_top_20_ids = df_top_20["cod_id_cliente"].tolist()

        if not lista_top_20_ids:
            print(
                "  Não foi possível encontrar os 20 melhores clientes. Pulando análise."
            )
            return

        query_contagem_dias = f"""
            select v.cod_id_cliente,
            c.nom_nome,
            TO_CHAR(v.data_venda, 'Day') as dia_da_semana,
            COUNT(DISTINCT v.cod_id_venda_unico) as numero_de_compras
            from public.vendas as v
            join public.clientes as c ON v.cod_id_cliente = c.cod_id_cliente
            where v.cod_id_cliente IN ({",".join(map(str, lista_top_20_ids))})
            group by v.cod_id_cliente, c.nom_nome, dia_da_semana;
        """

        df_contagem_dias = pd.read_sql_query(query_contagem_dias, engine)

        df_ordenado = df_contagem_dias.sort_values(
            by=["cod_id_cliente", "numero_de_compras"], ascending=[True, False]
        )

        df_melhor_dia = df_ordenado.drop_duplicates(
            subset=["cod_id_cliente"], keep="first"
        )

        nome_arquivo = "output/melhor_dia_por_cliente.csv"
        df_melhor_dia.to_csv(nome_arquivo, index=False)

        print(f"  Relatório '{nome_arquivo}' gerado com sucesso.")

    except Exception as e:
        print(f"  ERRO ao analisar o melhor dia de compra: {e}")
