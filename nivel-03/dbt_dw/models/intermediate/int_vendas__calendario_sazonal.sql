with datas_base as (
    select distinct
        data_pedido::date as data_referencia
    from {{ ref('stg_salesorderheader') }}
),

calendario as (
    select
        data_referencia,
        extract(year from data_referencia) as ano,
        extract(month from data_referencia) as mes,
  
        to_char(data_referencia, 'Month') as nome_mes,
        extract(day from data_referencia) as dia,
        extract(dow from data_referencia) as dia_da_semana_num, -- 0 (Dom) a 6 (Sab)
        
        -- Flag de Final de Semana
        case 
            when extract(dow from data_referencia) in (0, 6) then true 
            else false 
        end as is_weekend,
        
        -- Trimestre
        extract(quarter from data_referencia) as trimestre
        
    from datas_base
)

select * from calendario