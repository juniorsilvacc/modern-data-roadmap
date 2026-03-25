# 🛠️ Modern Data Pipeline: ELT (Nível 03)

---

## 🏗️ Arquitetura do Projeto

<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-03/arquitetura-nv3.png" />

---

## 🚀Diferenciais Estratégicos

## Execução (Monitoramento + Postgres + ETL)
```bash
# Cria a rede para conectar os containers
docker network create monitor-net

# Rodar o container de monitoramento
cd ~/modern-data-roadmap/nivel-03/infra/prd/monitoring && docker compose up -d

# Rodar o container Postgres apontando para o .env na raiz do projeto
cd ~/modern-data-roadmap/nivel-03/infra/prd/postgres && docker compose --env-file ../../../.env up -d

# Rodar o container ETL apontando para o .env na raiz do projeto
cd ~/modern-data-roadmap/nivel-03/app/etl && docker compose --env-file ../../.env up -d --build
```

## Subir o Airflow
```bash
# Suba APENAS o banco de dados primeiro
docker-compose up -d mysql

# Aguarde uns 15 segundos para o MySQL terminar de inicializar internamente.

# Agora que o MySQL está de pé, rode este comando para criar as tabelas do Airflow
docker-compose run --rm webserver airflow db init

# Crie seu usuário de acesso
docker-compose run --rm webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Reinicie os containers para forçar a leitura do .env da raiz
docker-compose --env-file ../../../.env up -d --force-recreate

# Agora sim, suba o Airflow completo
docker-compose up -d

# Da permissão nas pastas do Airflow
sudo chmod -R 777 ./dags ./logs ./plugins

#Abra o navegador em: http://localhost:8080
#Use o login admin e senha admin.
```