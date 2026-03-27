with source as (
    select * from {{ source('dbt_dw', 'raw_product') }}
),

-- Limpeza técnica e Deduplicação
cleaned as (
    select distinct * from source
    where id_produto is not null
),

-- Transformação, Tipagem e Renomeação
renamed as (
    select
        -- Identificadores
        id_produto::bigint as id_produto,
        id_subcategoria_produto::int as id_subcategoria_produto,
        id_modelo_produto::int as id_modelo_produto,
        codigo_produto,
        
        -- Atributos de Texto
        trim(nome_produto) as nome_produto,
        coalesce(cor, 'N/D') as cor,
        coalesce(tamanho, 'N/D') as tamanho,
        classe,
        estilo,
        linha_produto,
        
        -- Medidas e Pesos
        peso::double precision as peso,
        cod_unidade_medida_peso,
        cod_unidade_medida_tamanho,
        
        -- Valores Monetários
        custo_padrao::numeric(18,4) as custo_padrao,
        preco_lista::numeric(18,4) as preco_lista,
        
        -- Regras de Negócio / Estoque
        nivel_estoque_seguranca::int as nivel_estoque_seguranca,
        ponto_reposicao::int as ponto_reposicao,
        dias_fabricacao::int as dias_fabricacao,
        
        -- Flags
        case when flag_fabricacao = 1 then true else false end as flg_fabricacao,
        case when flag_produto_finalizado = 1 then true else false end as flg_produto_finalizado,
        
        -- Datas e Timestamps
        data_inicio_venda::timestamp as data_inicio_venda,
        data_fim_venda::timestamp as data_fim_venda,
        data_descontinuado::text::timestamp as data_descontinuado,
        data_modificacao as data_modificacao,

        -- Auditoria
        guid_linha

    from cleaned
)

select * from renamed