with fct_vendas as (
    select * from {{ ref('fct_vendas') }}
),

dim_calendario as (
    select * from {{ ref('dim_calendario') }}
),

int_crypto as (
    select * from {{ ref('int_crypto__analise_volatilidade') }}
),

-- Agregando as vendas por dia
vendas_diarias as (
    select
        data_venda_fk,
        count(distinct id_pedido) as total_pedidos_dia,
        sum(valor_liquido_item) as faturamento_total_dia
    from fct_vendas
    group by 1
),

-- Unindo e agrupando para garantir o grão de 1 linha por dia
final as (
    select
        -- Tempo
        cal.data_pk as data_referencia,
        cal.nome_mes,
        cal.ano,
        cal.is_weekend,

        -- Performance de Vendas
        coalesce(max(v.total_pedidos_dia), 0) as total_pedidos,
        coalesce(max(v.faturamento_total_dia), 0) as faturamento_vendas,

        -- Performance de Mercado (Média de todas as Cryptos do dia para evitar duplicidade)
        avg(c.preco_atual) as preco_medio_mercado,
        avg(c.pct_amplitude_diaria) as volatilidade_media_mercado,
        avg(c.pct_queda_do_topo) as drawdown_medio_mercado,
        count(c.simbolo) as qtd_moedas_observadas
        
    from dim_calendario cal
    left join vendas_diarias v on cal.data_pk = v.data_venda_fk
    left join int_crypto c on cal.data_pk = c.data_extracao::date
    
    -- Filtro para garantir que só trazemos datas com atividade
    where v.data_venda_fk is not null or c.data_extracao is not null

    group by 1, 2, 3, 4
)

select * from final
order by data_referencia desc