# --- 1. IMPORTAÇÕES ---
# Importamos a classe DAG, que é o objeto pai de todo o fluxo
from airflow import DAG 
# Importamos o Operador de Python (o 'trabalhador' que sabe executar funções Python)
from airflow.operators.python import PythonOperator 
# Ferramentas nativas do Python para lidar com datas e intervalos de tempo
from datetime import datetime, timedelta 

# --- 2. A LÓGICA (O "QUÊ" FAZER) ---
# Aqui você define uma função Python normal. 
# O Airflow não sabe o que tem aqui dentro, ele apenas chama essa função quando chegar a hora.
def hello_world():
    print("Sucesso! O Airflow está lendo a pasta de DAGs corretamente.")
    print(f"Executado em: {datetime.now()}")

# --- 3. CONFIGURAÇÕES PADRÃO (DEFAULT ARGS) ---
# Esse dicionário serve para você não ter que repetir configurações em cada tarefa.
default_args = {
    'owner': 'airflow',                         # Quem é o dono/responsável por essa DAG
    'depends_on_past': False,                   # Se a execução de hoje falhar, a de amanhã pode rodar? (False = Sim)
    'start_date': datetime(2023, 1, 1),         # Data retroativa de quando o Airflow acha que a DAG "nasceu"
    'retries': 1,                               # Se a tarefa falhar, quantas vezes o Airflow deve tentar de novo sozinho?
    'retry_delay': timedelta(minutes=5),        # Quanto tempo esperar entre uma tentativa e outra
}

# --- 4. DEFINIÇÃO DA DAG (O "COMO" E "QUANDO" ORGANIZAR) ---
with DAG(
    'dag_teste_mapeamento_v1',                  # ID ÚNICO da DAG. É como ela aparece na interface Web.
    default_args=default_args,                  # Aplica as configurações que criamos acima
    description='Teste de volume',              # Uma descrição boba para te ajudar a lembrar o que ela faz
    schedule_interval=None,                     # Frequência de execução. 'None' significa que ela só roda se você clicar no Play.
    catchup=False,                              # Se você colocar uma data de 2023, o Airflow tentará rodar todas as datas passadas? (False = Não)
    tags=['teste', 'infra'],                    # Etiquetas para filtrar sua DAG na UI quando você tiver centenas delas
) as dag:

    # --- 5. AS TASKS (AS ETAPAS) ---
    # Aqui instanciamos o Operador. O PythonOperator é uma "caixa" que embrulha sua função.
    task_teste = PythonOperator(
        task_id='validar_mapeamento',           # Nome da "caixinha" que aparece no gráfico do Airflow
        python_callable=hello_world,            # Qual função ele deve chamar? (A que criamos lá em cima)
    )

    # --- 6. FLUXO/DEPENDÊNCIAS ---
    # Como só temos uma task, apenas a declaramos. 
    # Se tivéssemos outra, usaríamos: task_teste >> proxima_task
    task_teste