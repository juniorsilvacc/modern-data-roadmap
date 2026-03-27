import os

BASE_DIR = "/opt/airflow/data"

# Definimos os caminhos a partir da raiz absoluta do container
LANDING_DIR = os.path.join(BASE_DIR, 'landing')
RAW_DIR = os.path.join(BASE_DIR, 'raw')

os.makedirs(LANDING_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)
