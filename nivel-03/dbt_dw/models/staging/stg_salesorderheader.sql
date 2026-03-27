with source as (
    select * from {{ source('dbt_dw', 'raw_salesorderheader') }}
),

-- Transformação, Tipagem e Renomeação
renamed as (
    select
        -- Identificadores
        id_pedido::bigint as id_pedido,
        id_cliente::bigint as id_cliente,
        id_territorio::bigint as id_territorio,
        id_vendedor::int as id_vendedor,
        id_metodo_envio::int as id_metodo_envio,
        id_cartao_credito::int as id_cartao_credito,
        id_endereco_faturamento::bigint as id_endereco_faturamento,
        id_endereco_envio::bigint as id_endereco_envio,
        id_taxa_cambio::int as id_taxa_cambio,

        -- Informações do Pedido
        numero_pedido,
        numero_pedido_compra,
        numero_revisao::int as numero_revisao,
        numero_conta,
        cod_aprovacao_cartao,
        
        -- Status e Flags
        status_pedido::int as status_pedido,
        case when flag_pedido_online = 1 then true else false end as flg_pedido_online,

        -- Valores Financeiros
        subtotal::numeric(18,4) as subtotal,
        valor_imposto::numeric(18,4) as valor_imposto,
        valor_frete::numeric(18,4) as valor_frete,
        valor_total::numeric(18,4) as valor_total,

        -- Datas
        data_pedido::timestamp as data_pedido,
        data_vencimento::timestamp as data_vencimento,
        data_envio::timestamp as data_envio,
        data_modificacao::timestamp as data_modificacao,
        
        -- Auditoria e Comentários
        guid_linha,
        coalesce(comentario::text, 'Sem comentário') as comentario
        
    from source
)

select * from renamed