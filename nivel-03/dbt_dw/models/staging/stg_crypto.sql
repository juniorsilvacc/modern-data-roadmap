with source as (
    select * from {{ source('dbt_dw', 'raw_crypto') }}
),

-- Limpeza técnica e Deduplicação
cleaned as (
    select distinct * from source
    where id is not null
),

-- Transformação, Tipagem e Renomeação
renamed as (
    select
        -- Identificadores
        id as id_crypto,
        trim(symbol) as simbolo,
        trim(name) as nome_moeda,

        -- Valores de Mercado
        current_price::numeric(18,8) as preco_atual,
        market_cap::bigint as capitalizacao_mercado,
        market_cap_rank::int as ranking_mercado,
        fully_diluted_valuation::bigint as valorizacao_total_diluida,
        total_volume::numeric(18,2) as volume_total_24h,

        -- Variações
        high_24h::numeric(18,8) as maxima_24h,
        low_24h::numeric(18,8) as minima_24h,
        price_change_24h::numeric(18,8) as variacao_preco_24h,
        price_change_percentage_24h::numeric(18,4) as pct_variacao_preco_24h,

        -- Suprimento
        circulating_supply::numeric(24,2) as suprimento_circulante,
        total_supply::numeric(24,2) as suprimento_total,
        max_supply::numeric(24,2) as suprimento_maximo,

        -- ATH (All Time High) e ATL (All Time Low)
        ath::numeric(18,8) as preco_maximo_historico,
        ath_change_percentage::numeric(18,4) as pct_variacao_ath,
        atl::numeric(18,8) as preco_minimo_historico,
        atl_change_percentage::numeric(18,4) as pct_variacao_atl,

        -- Retorno sobre Investimento (ROI)
        roi_percentage::numeric(18,4) as pct_roi,
        trim(roi_currency) as moeda_roi,

        -- Datas
        extracted_at as data_extracao,
        last_updated::text::timestamp as data_ultima_atualizacao,
        ath_date::text::timestamp as data_maximo_historico,
        atl_date::text::timestamp as data_minimo_historico,

        -- Links e Metadados
        image as url_imagem

    from cleaned
)

select * from renamed