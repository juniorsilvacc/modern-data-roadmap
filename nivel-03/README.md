# 🛠️ Modern Data Pipeline: ELT (Nível 03)

---

## 🏗️ Arquitetura do Projeto

<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-03/arquitetura-nv3.png" />

---

## 🚀Diferenciais Estratégicos

## 🏗️ Guia de Execução - Modern Data Roadmap (Nível 03)
Siga esta **`ordem rigorosa`** para garantir que a rede e as dependências de banco de dados estejam prontas antes do orquestrador iniciar.

### 1️⃣ Preparação da Rede e Infra
A rede monitor-net é a ponte entre todos os containers. Sem ela, o Airflow não enxerga o Postgres.
```bash
# Cria a rede global (se não existir)
docker network create monitor-net

# Sobe a stack de Monitoramento (Grafana/Prometheus)
cd ~/modern-data-roadmap/nivel-03/infra/prd/monitoring && docker compose up -d
```

### 2️⃣ Camada de Dados (PostgreSQL)
O Postgres deve subir agora para que o banco modern-data-etl-db-nv3 esteja disponível.
```bash
# Sobe o container do banco de dados (PostgreSQL) apontando para o .env na raiz do projeto
cd ~/modern-data-roadmap/nivel-03/infra/prd/postgres
docker compose --env-file ../../../.env up -d
```

### 3️⃣ Camada de Orquestração (Airflow)
O Airflow depende do MySQL (metadados) e precisa ler o .env da raiz para as credenciais do ETL.
```bash
cd ~/modern-data-roadmap/nivel-03/infra/prd/airflow

# 1. Sobe o banco de metadados e inicializa o Airflow
docker compose up -d mysql
# Aguarda o MySQL inicializar
sleep 15
# Agora que o MySQL está de pé, rode este comando para criar as tabelas do Airflow
docker compose run --rm webserver airflow db init

# 2. Cria o usuário Admin
docker compose run --rm webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# 3. Sobe o Airflow completo lendo o .env da raiz
docker compose --env-file ../../../.env up -d --force-recreate
```

### 4️⃣ Ajustes de Permissão (WSL/Linux)
Fundamental para evitar o erro Permission Denied que tivemos nos arquivos Parquet e logs.
```bash
# Na raiz do projeto
sudo chmod -R 777 data/ logs/ dags/
```

### 5️⃣ Nevegaçções
- Abra o navegador em:
    - http://localhost:8080 (Airflow - Orquestração)
    - http://localhost:3000 (Grafana - Monitoramento)
- Use o login `admin` e senha `admin` para entrar no dashboard do Airflow e Grafana.

---

## 🏗️ Guia de Execução: dbt Analytics Engineering

### 1️⃣ Preparação do Ambiente (Local/WSL)
Antes de rodar o dbt, você precisa garantir que o banco de dados está de pé e que as variáveis de ambiente estão carregadas corretamente para o contexto local.
```bash
# 1. Entre na pasta do projeto dbt
cd ~/modern-data-roadmap/nivel-03/dbt_dw

# 2. Ative o ambiente virtual
source ../venv/bin/activate

# 3. Carregue o .env da raiz e ajuste para o contexto LOCAL (WSL -> Docker)
export $(grep -v '^#' ../.env | xargs)
export DB_HOST=localhost
export DB_PORT=5433
```

### 2️⃣ Validando a Conexão
Sempre rode o `debug` para garantir que o dbt está enxergando o Postgres através da porta mapeada.
```bash
dbt debug --profiles-dir .
```

---

## ⚙️ Comandos Principais de Modelagem

| Comandos             | Descrição                                                       |
|--------------------- |-----------------------------------------------------------------|
| `dbt deps`           | Instala pacotes extras (se houver o arquivo `packages.yml`).    |
| `dbt seed`           | Carrega arquivos CSV da pasta `seeds/` para o banco.            |
| `dbt run`            | Executa todos os modelos SQL e cria as tabelas no Postgres.     |
| `dbt test`           | Roda os testes de qualidade (Unique, Not Null, etc).            |
| `dbt docs generate`  | Gera a documentação e a linhagem dos dados.                     |
| `dbt docs serve`     | Abre o portal de documentação no navegador.                     |

---

## 📂 Estrutura de Camadas (Medallion)
Para manter a Governança que discutimos, organize seus modelos assim:
- `models/staging/`: Camada Bronze. Apenas limpeza básica (renomear colunas, cast de tipos).
- `models/intermediate/`: Camada Silver. Joins complexos e regras de negócio entre tabelas.
- `models/marts/`: Camada Gold. Tabelas agregadas prontas para o Dashboard (ex: `fct_vendas_crypto`).