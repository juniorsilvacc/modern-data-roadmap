with int_performance_clientes as (
    select * from {{ ref('int_vendas__performance_clientes') }}
)

select
    id_cliente,
    data_primeira_compra,
    data_ultima_compra,
    total_pedidos,
    valor_total_gasto as ltv_cliente,
    ticket_medio_valor,
    
    -- Classificação de clientes
    case 
        when valor_total_gasto > 5000 then 'VIP'
        when valor_total_gasto > 1000 then 'Regular'
        else 'Bronze'
    end as categoria_fidelidade
from int_performance_clientes