with fct_vendas as (
    select * from {{ ref('fct_vendas') }}
),

stg_header as (
    select 
        id_pedido,
        data_pedido,
        data_vencimento,
        data_envio,
        id_territorio
    from {{ ref('stg_salesorderheader') }}
)

select
    h.id_pedido,
    h.id_territorio,
    h.data_pedido,
    h.data_envio,
    
    -- Cálculo de Lead Time (Dias para enviar)
    (h.data_envio::date - h.data_pedido::date) as dias_para_envio,
    
    -- Verificação de Atraso
    case 
        when h.data_envio > h.data_vencimento then true 
        else false 
    end as flg_pedido_atrasado,

    -- Métricas Financeiras Relacionadas
    v.valor_liquido_item,
    v.quantidade_pedido

from stg_header h
inner join fct_vendas v on h.id_pedido = v.id_pedido