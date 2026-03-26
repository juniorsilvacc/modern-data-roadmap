{{ config(materialized='table') }}

select 
    1 as id, 
    'teste_conexao_airflow' as nome,
    now() as processado_em