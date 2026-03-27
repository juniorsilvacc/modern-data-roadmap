import os
import time
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text

def get_engine():
    """
    Cria a conexão com o banco e tenta reconectar até 5 vezes 
    caso o banco ainda esteja inicializando.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST", "postgres")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME")
    
    if not password:
        raise ValueError("A variável DB_PASS está vazia! Verifique o .env e o Docker Compose.")

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=database
    )
    
    engine = create_engine(url)
    
    max_retries = 5
    retry_interval = 5

    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Banco de dados pronto para receber conexões!")
            return engine
        except Exception as e:
            print(f"Banco ainda não disponível (Tentativa {i+1}/{max_retries}). Aguardando {retry_interval}s...")
            print(f'Error: {e}')
            time.sleep(retry_interval)
            
    raise Exception("Erro: Não foi possível conectar ao banco de dados após várias tentativas.")