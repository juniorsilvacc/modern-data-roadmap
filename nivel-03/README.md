# 🛠️ Modern Data Pipeline: ELT (Nível 03)

---

## 🏗️ Arquitetura do Projeto

<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-03/arquitetura-nv3.png" />

---

## 🚀Diferenciais Estratégicos

## Execução
```bash
# Cria a rede para conectar os containers
docker network create monitor-net

# Rodar o container de monitoramento
cd ~/modern-data-roadmap/nivel-03/infra/prd/monitoring && docker compose up -d

# Rodar o container Postgres apontando para o .env na raiz do projeto
cd ~/modern-data-roadmap/nivel-03/infra/prd/postgres && docker compose --env-file ../../../.env up -d

# Rodar o container ETL apontando para o .env na raiz do projeto
cd ~/modern-data-roadmap/nivel-03/app/etl && docker compose --env-file ../../.env up -d --build


# Drop
# Rodar o container de monitoramento
cd ~/modern-data-roadmap/nivel-03/infra/prd/monitoring && docker compose down

# Rodar o container Postgres apontando para o .env na raiz do projeto
cd ~/modern-data-roadmap/nivel-03/infra/prd/postgres && docker compose down

# Rodar o container ETL apontando para o .env na raiz do projeto
cd ~/modern-data-roadmap/nivel-03/app/etl && docker compose down

# Drop rede
docker network rm monitor-net
```