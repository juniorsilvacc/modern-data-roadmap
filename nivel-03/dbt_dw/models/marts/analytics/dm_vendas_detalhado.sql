with fct_vendas as (
    select * from {{ ref('fct_vendas') }}
),
dim_produtos as (
    select * from {{ ref('dim_produtos') }}
),
dim_clientes as (
    select * from {{ ref('dim_clientes') }}
),
dim_calendario as (
    select * from {{ ref('dim_calendario') }}
)

select
    -- Informações da Venda
    v.id_pedido,
    v.id_detalhe_pedido,
    v.quantidade_pedido,
    v.valor_liquido_item,
    
    -- Informações do Produto
    p.nome_produto,
    p.cor_produto,
    
    -- Informações do Cliente
    c.categoria_fidelidade,
    c.ltv_cliente,
    
    -- Informações do Tempo
    cal.nome_mes,
    cal.ano,
    cal.is_weekend

from fct_vendas v
left join dim_produtos p on v.id_produto = p.id_produto
left join dim_clientes c on v.id_cliente = c.id_cliente
left join dim_calendario cal on v.data_venda_fk = cal.data_pk