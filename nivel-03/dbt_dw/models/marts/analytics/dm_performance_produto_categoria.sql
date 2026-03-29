with fct_vendas as (
    select * from {{ ref('fct_vendas') }}
),

dim_produtos as (
    select * from {{ ref('dim_produtos') }}
)

select
    p.id_produto,
    p.nome_produto,
    p.cor_produto,

    sum(v.quantidade_pedido) as qtd_total_vendida,
    sum(v.valor_liquido_item) as faturamento_total,
    sum(v.valor_desconto_item) as total_descontos_concedidos,

    -- Margem de Desconto Médio
    (sum(v.valor_desconto_item) / nullif(sum(v.valor_bruto_item), 0)) * 100 as pct_avg_desconto

from fct_vendas v
left join dim_produtos p on v.id_produto = p.id_produto
group by 1, 2, 3
order by faturamento_total desc