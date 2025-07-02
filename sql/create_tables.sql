CREATE TABLE IF NOT EXISTS public.vendas (
    cod_id_venda_unico VARCHAR(50) PRIMARY KEY,
    cod_id_loja VARCHAR(6),
    data_venda DATE,
    cod_id_cliente INTEGER,
    des_tipo_cliente VARCHAR(1),
    des_sexo_cliente VARCHAR(1),
    cod_id_produto INTEGER,
    val_valor_sem_desc MONEY,
    val_valor_desconto MONEY,
    val_valor_com_desc MONEY,
    val_quantidade_kg REAL
);

CREATE TABLE IF NOT EXISTS public.produtos (
    cod_id_produto INTEGER PRIMARY KEY,
    cod_id_categoria_produto INTEGER,
    arr_categorias_produto TEXT,
    des_produto TEXT,
    des_unidade VARCHAR(2),
    cod_codigo_barras BIGINT
);

CREATE TABLE IF NOT EXISTS public.clientes (
    cod_id_cliente INTEGER PRIMARY KEY,
    des_tipo_cliente VARCHAR(1),
    nom_nome VARCHAR(150),
    des_sexo_cliente VARCHAR(1),
    dat_data_nascimento DATE
);