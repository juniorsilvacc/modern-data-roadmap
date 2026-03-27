from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.datasets import Dataset

dados_brutos_prontos = Dataset("postgres://public/stg_all")

# Configuração de conexão
profile_config = ProfileConfig(
    profile_name="dbt_dw", 
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="air_postgres",
        profile_args={"schema": "public"},
    ),
)

dbt_analytics_dag = DbtDag(
    project_config=ProjectConfig("/opt/airflow/dbt_dw"),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,
    },
    render_config=RenderConfig(
        test_behavior="after_each",
    ),
    schedule=[dados_brutos_prontos],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="dbt_medallion_pipeline",
)