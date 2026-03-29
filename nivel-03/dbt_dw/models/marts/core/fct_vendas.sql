with int_vendas_detalhadas as (
    select * from {{ ref('int_vendas__detalhadas') }}
),

vendas_header as (
    select * from {{ ref('stg_salesorderheader') }}
)

select
    -- Chaves (PK/FK)
    d.id_detalhe_pedido,
    d.id_pedido,
    d.id_produto,
    h.id_cliente,
    h.data_pedido::date as data_venda_fk, -- Chave para a dim_calendario
    
    -- Fatos (Métricas para cálculos)
    d.quantidade_pedido,
    d.preco_unitario,
    d.desconto_unitario,
    d.valor_bruto_item,
    d.valor_desconto_item,
    d.valor_liquido_item,
    
    -- Contexto de Auditoria
    h.status_pedido,
    h.flg_pedido_online
    
from int_vendas_detalhadas d
left join vendas_header h on d.id_pedido = h.id_pedido