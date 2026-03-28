with vendas_detalhadas as (
    select * from {{ ref('int_vendas__detalhadas') }}
),

vendas_header as (
    select * from {{ ref('stg_salesorderheader') }}
),

vendas_por_cliente as (
    select
        h.id_cliente,
        
        -- Métricas de Tempo
        min(h.data_pedido) as data_primeira_compra,
        max(h.data_pedido) as data_ultima_compra,
        
        -- Métricas de Contagem
        count(distinct h.id_pedido) as total_pedidos,
        sum(d.quantidade_pedido) as total_itens_comprados,
        
        -- Valor Financeiro (LTV)
        sum(d.valor_liquido_item) as valor_total_gasto,
        
        -- Ticket Médio (Valor Total / Total de Pedidos)
        sum(d.valor_liquido_item) / nullif(count(distinct h.id_pedido), 0) as ticket_medio_valor
        
    from vendas_header h
    left join vendas_detalhadas d on h.id_pedido = d.id_pedido
    group by 1
)

select * from vendas_por_cliente