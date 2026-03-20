import os

# Forçamos a raiz como /app, que é o WORKDIR do seu Dockerfile
BASE_DIR = "/app"

# Definimos os caminhos a partir da raiz absoluta do container
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw')
STAGING_DIR = os.path.join(BASE_DIR, 'data', 'staging')

# Criamos as pastas
# Como /app/data está mapeado no docker-compose, 
# o que for criado aqui aparecerá na sua máquina real.
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

# Debug para conferir nos logs do Docker
print(f"🚀 Config de caminhos carregada!")
print(f"📂 RAW_DIR: {RAW_DIR}")
print(f"📂 STAGING_DIR: {STAGING_DIR}")