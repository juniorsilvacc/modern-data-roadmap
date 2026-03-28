with crypto as (
    select * from {{ ref('stg_crypto') }}
),

volatilidade as (
    select
        id_crypto,
        simbolo,
        preco_atual,
        maxima_24h,
        minima_24h,

        -- Métrica: Amplitude de variação no dia
        (maxima_24h - minima_24h) / nullif(minima_24h, 0) * 100 as pct_amplitude_diaria,
        
        -- Métrica: Distância do topo histórico (Drawdown)
        (preco_maximo_historico - preco_atual) / nullif(preco_maximo_historico, 0) * 100 as pct_queda_do_topo
    from crypto
)

select * from volatilidade