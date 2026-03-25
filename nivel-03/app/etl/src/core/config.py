import os

BASE_DIR = "/opt/airflow/data"

# Definimos os caminhos a partir da raiz absoluta do container
RAW_DIR = os.path.join(BASE_DIR, 'raw')
STAGING_DIR = os.path.join(BASE_DIR, 'staging')

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)
