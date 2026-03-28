with itens_venda as (
    select * from {{ ref('stg_salesorderdetail') }}
),

produtos as (
    select * from {{ ref('stg_product') }}
),

vendas_detalhadas as (
    select
        -- Chaves
        iv.id_detalhe_pedido,
        iv.id_pedido,
        iv.id_produto,
        
        -- Informações do Produto
        p.nome_produto,
        p.codigo_produto,
        p.cor as cor_produto,
        
        -- Métricas de Venda
        iv.quantidade as quantidade_pedido,
        iv.preco_unitario,
        coalesce(iv.desconto_unitario, 0) as desconto_unitario,

        -- Cálculo de Valor do Desconto (Total sem desconto)
        (iv.preco_unitario * iv.quantidade) as valor_bruto_item,

        -- Cálculo de Valor do Desconto (Quanto foi "perdido" em reais)
        (iv.preco_unitario * iv.quantidade) * coalesce(iv.desconto_unitario, 0) as valor_desconto_item,
        
        -- Cálculo de Valor Líquido
        (iv.preco_unitario * iv.quantidade) * (1 - coalesce(iv.desconto_unitario, 0)) as valor_liquido_item,
        
        -- Datas
        iv.data_modificacao
        
    from itens_venda iv
    left join produtos p on iv.id_produto = p.id_produto
)

select * from vendas_detalhadas