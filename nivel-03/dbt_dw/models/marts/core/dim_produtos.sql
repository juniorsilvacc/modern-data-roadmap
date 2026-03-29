with stg_product as (
    select * from {{ ref('stg_product') }}
)

select
    id_produto,
    nome_produto,
    codigo_produto,
    cor as cor_produto,
    data_modificacao
from stg_product