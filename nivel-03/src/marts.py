from sqlalchemy import text

def create_marts(engine):
    queries = [
        # --- DIMENSÃO GLOBAL: PRODUTOS ---
        """
        DROP TABLE IF EXISTS dim_produtos;
        CREATE TABLE dim_produtos AS
        SELECT 
            id_produto,
            nome_produto,
            codigo_produto,
            cor as cor_produto,
            classe,
            estilo,
            custo_padrao
        FROM stg_product;
        """,
        
        # --- DIMENSÃO GLOBAL: CALENDÁRIO ---
        """
        DROP TABLE IF EXISTS dim_calendario;
        CREATE TABLE dim_calendario AS
        SELECT DISTINCT
            data_pedido AS data_origem,
            CAST(TO_CHAR(data_pedido, 'YYYYMMDD') AS INT) AS data_sk,
            EXTRACT(YEAR FROM data_pedido) AS ano,
            EXTRACT(MONTH FROM data_pedido) AS mes,
            TO_CHAR(data_pedido, 'TMMonth') AS nome_mes,
            EXTRACT(QUARTER FROM data_pedido) AS trimestre,
            TO_CHAR(data_pedido, 'TMDay') AS dia_semana
        FROM stg_salesorderheader
        ORDER BY data_sk;
        """,

        # --- TABELA FATO: VENDAS ---
        """
        DROP TABLE IF EXISTS fct_vendas;
        CREATE TABLE fct_vendas AS
        SELECT 
            d.id_detalhe_pedido,
            h.id_pedido,
            CAST(TO_CHAR(h.data_pedido, 'YYYYMMDD') AS INT) AS data_sk,
            h.id_cliente,
            d.id_produto,             
            d.quantidade,
            d.preco_unitario,
            d.total_linha
        FROM stg_salesorderheader h
        JOIN stg_salesorderdetail d ON h.id_pedido = d.id_pedido;
        """
    ]

    with engine.begin() as conn:
        for query in queries:
            conn.execute(text(query))
        print("\n✅ Camada de Marts (Star Schema) criada com sucesso!")