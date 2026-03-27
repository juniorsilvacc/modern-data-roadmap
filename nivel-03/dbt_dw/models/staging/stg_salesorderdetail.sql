with source as (
    select * from {{ source('dbt_dw', 'raw_salesorderdetail') }}
),

-- Limpeza técnica e Deduplicação
cleaned as (
    select distinct * from source
    where id_detalhe_pedido is not null
),

renamed as (
    select
        -- Identificadores
        id_detalhe_pedido::bigint as id_detalhe_pedido,
        id_pedido::bigint as id_pedido,
        id_produto::bigint as id_produto,
        id_oferta_especial::int as id_oferta_especial,

        -- Quantidades
        quantidade::int as quantidade,

        -- Valores Monetários
        preco_unitario::numeric(18,4) as preco_unitario,
        desconto_unitario::numeric(18,4) as desconto_unitario,
        total_linha::numeric(18,4) as total_linha,

        -- Atributos de Texto
        coalesce(numero_rastreio, 'Sem rastreio') as numero_rastreio,
        guid_linha,

        -- Datas
        data_modificacao::timestamp as data_modificacao

    from cleaned
)

select * from renamed