with int_calendario as (
    select * from {{ ref('int_vendas__calendario_sazonal') }}
)

select
    data_referencia as data_pk,
    ano,
    trimestre,
    mes,
    nome_mes,
    dia,
    dia_da_semana_num,
    is_weekend
from int_calendario