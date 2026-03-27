from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta

from src.extract.file_sales import process_sales_csv
from src.extract.api_crypto import extract_crypto_to_staging

dados_brutos_prontos = Dataset("postgres://public/stg_all")

# 2. CONFIGURAÇÕES PADRÃO
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 3. DEFINIÇÃO DA DAG
with DAG(
    'dag_pipeline_vendas_crypto_v1',
    default_args=default_args,
    description='Pipeline: CSV de Vendas e API de Cripto para Staging',
    schedule_interval='@daily', # Roda uma vez por dia
    catchup=False,
    tags=['producao', 'etl'],
) as dag:

    # TASK 1: Processar o CSV de Vendas
    task_vendas = PythonOperator(
        task_id='processar_vendas_csv',
        python_callable=process_sales_csv,
    )

    # TASK 2: Extrair dados de Cripto da API
    task_crypto = PythonOperator(
        task_id='extrair_api_crypto',
        python_callable=extract_crypto_to_staging,
        outlets=[dados_brutos_prontos]
    )

    # 4. DEFINIÇÃO DO FLUXO (DEPENDÊNCIAS)
    task_vendas >> task_crypto