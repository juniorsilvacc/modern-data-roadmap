with fct_vendas as (
    select * from {{ ref('fct_vendas') }}
),

dim_clientes as (
    select * from {{ ref('dim_clientes') }}
),

-- Agregando a performance histórica por cliente
metricas_cliente as (
    select
        id_cliente,
        count(distinct id_pedido) as total_pedidos,
        sum(valor_liquido_item) as valor_total_gasto,
        max(data_venda_fk) as data_ultima_compra,
        min(data_venda_fk) as data_primeira_compra,

        -- Ticket Médio (Quanto ele gasta por pedido em média)
        sum(valor_liquido_item) / nullif(count(distinct id_pedido), 0) as ticket_medio_por_pedido
    from fct_vendas
    group by 1
),

-- Unindo com os dados cadastrais e calculamos a saúde
final as (
    select
        c.id_cliente,
        c.categoria_fidelidade,
        
        -- Métricas de Comportamento
        coalesce(m.total_pedidos, 0) as frequencia_compras,
        coalesce(m.valor_total_gasto, 0) as ltv_atual,
        coalesce(m.ticket_medio_por_pedido, 0) as ticket_medio,
        
        -- Métricas de Recência (Saúde)
        m.data_ultima_compra,

        -- Calculando quantos dias ele está sem comprar (baseado na data atual dos dados)
        ('2026-03-29'::date - m.data_ultima_compra) as dias_de_inatividade,
        
        -- Classificação de Saúde do Cliente
        case 
            when ('2026-03-29'::date - m.data_ultima_compra) <= 30 then 'Ativo'
            when ('2026-03-29'::date - m.data_ultima_compra) <= 90 then 'Em Alerta'
            when ('2026-03-29'::date - m.data_ultima_compra) > 90 then 'Churn'
            else 'Novo / Sem Compra'
        end as status_saude_cliente

    from dim_clientes c
    left join metricas_cliente m on c.id_cliente = m.id_cliente
)

select * from final
order by ltv_atual desc