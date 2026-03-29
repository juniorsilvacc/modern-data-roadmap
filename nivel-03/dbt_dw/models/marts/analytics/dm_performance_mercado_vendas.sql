with fct_vendas as (
    select * from {{ ref('fct_vendas') }}
),

dim_calendario as (
    select * from {{ ref('dim_calendario') }}
),

int_crypto as (
    select * from {{ ref('int_crypto__analise_volatilidade') }}
),

-- Agregando as vendas por dia para bater com o grão da Crypto
vendas_diarias as (
    select
        data_venda_fk,
        count(distinct id_pedido) as total_pedidos_dia,
        sum(quantidade_pedido) as total_itens_dia,
        sum(valor_liquido_item) as faturamento_total_dia,
        avg(valor_liquido_item) as ticket_medio_item_dia
    from fct_vendas
    group by 1
),

-- Unindo tudo em uma visão única
final as (
    select
        -- Tempo
        cal.data_pk as data_referencia,
        cal.nome_mes,
        cal.ano,
        cal.is_weekend,

        -- Performance de Vendas
        coalesce(v.total_pedidos_dia, 0) as total_pedidos,
        coalesce(v.faturamento_total_dia, 0) as faturamento_vendas,

        -- Performance de Mercado (Cripto)
        c.simbolo as crypto_simbolo,
        c.preco_atual as preco_crypto_fechamento,
        c.pct_amplitude_diaria as volatilidade_crypto,
        c.pct_queda_do_topo as drawdown_crypto

    from dim_calendario cal
    left join vendas_diarias v on cal.data_pk = v.data_venda_fk

    -- JOIN com a crypto pela data
    left join int_crypto c on cal.data_pk = c.data_extracao::date
    
    -- Filtrando para mostrar apenas dias que tenham ou venda ou dados de crypto
    where v.data_venda_fk is not null or c.data_extracao is not null
)

select * from final
order by data_referencia desc