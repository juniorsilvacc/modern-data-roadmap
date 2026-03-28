with detalhes as (
    select * from {{ ref('int_vendas__detalhadas') }}
),

agregado as (
    select
        id_pedido,
        count(id_detalhe_pedido) as total_itens,
        sum(quantidade_pedido) as total_quantidade_produtos,
        sum(valor_bruto_item) as subtotal_bruto,
        sum(valor_liquido_item) as subtotal_liquido
    from detalhes
    group by 1
)

select * from agregado