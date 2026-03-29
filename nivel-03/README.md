# 🛠️ Modern Data Pipeline: ELT (Nível 03)
Nesta etapa, evolui de scripts simples para uma arquitetura robusta de Analytics Engineering, integrando ingestão via Python, orquestração com Airflow e modelagem dimensional com dbt.

---

## 🏗️ Arquitetura do Projeto
O pipeline foi desenhado para ser resiliente e escalável, utilizando containers Docker para isolar cada componente da stack.

<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-03/arquitetura-nv3.png" />

---

## 🧱 Demonstração da Linhagem (Lineage)
A rastreabilidade dos dados é garantida pelo dbt, permitindo visualizar desde as fontes (sources) até os data marts finais.

<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-03/lineage-graph.png" />

--- 

## 🛠️ Stack Tecnológica
- Linguagens: **Python (Ingestão OOP) e SQL (Transformação)**
- Transformação: **dbt Core**
- Banco de Dados: **PostgreSQL (Data Warehouse)**
- Orquestração: **Apache Airflow**
- Infraestrutura: **Docker & Docker Compose**
- Monitoramento: **Grafana & Prometheus**
- Qualidade de Dados: **dbt Tests (46 testes automatizados)**

---

## 💎 Diferenciais de Engenharia de Dados
Diferente de pipelines convencionais, este projeto aplica boas práticas de desenvolvimento de software:
- **Idempotência:** Scripts de ingestão e modelos dbt desenhados para serem reexecutados sem duplicidade ou corrupção de dados.
- **Orientação a Objetos (OOP):** Drivers de extração em Python modularizados, facilitando a manutenção de APIs (NASA e Mercado Livre).
- **Tratamento de Arquivos:** Uso de formato Parquet na camada de Landing para otimização de storage e performance de leitura.
- **Data Quality Gate:** Implementação de testes de unicidade, integridade referencial e regras de negócio singulares (ex: lead time negativo).

---

## 📐 Modelagem de Dados (Medallion Architecture)
O projeto segue a separação lógica em camadas para garantir escalabilidade e governança:
- **Staging (Bronze):** Limpeza inicial, renomeação de colunas e tipagem.
- **Intermediate (Silver):** Camada de transformação onde aplicamos regras de negócio complexas, como o cálculo de volatilidade cripto e métricas de performance de clientes.
- **Core (Gold/Star Schema):** Modelagem dimensional com tabelas Fato (`fct_vendas`) e Dimensões (`dim_produtos, dim_clientes, dim_calendario`).
- **Analytics (Marts):** Tabelas prontas para consumo (OBT e Agregações) focadas em Logística, CRM, Finanças e Correlação de Mercado.

---

## 📂 Estrutura de Camadas (Medallion)
Para manter a Governança que discutimos, organize seus modelos assim:
- `models/staging/`: Camada Bronze. Apenas limpeza básica (renomear colunas, cast de tipos).
- `models/intermediate/`: Camada Silver. Joins complexos e regras de negócio entre tabelas.
- `models/marts/`: Camada Gold. Tabelas agregadas prontas para o Dashboard (ex: `fct_vendas_crypto`).

---

## 💡 Business Insights (Data Marts)
O diferencial deste DW é a entrega de dashboards prontos para responder:
- **Eficiência Logística:** Cálculo de Lead Time e identificação de gargalos por território.
- **Correlação Cripto:** Análise de como o faturamento da empresa se comporta em relação à volatilidade do mercado de criptomoedas.
- **Saúde do Cliente (CRM):** Segmentação de clientes por LTV e identificação automática de Churn baseado em dias de inatividade.
- **Performance de Produto:** Ranking de lucratividade e análise de agressividade de descontos.

---

## 🛡️ Qualidade e Governança
O projeto conta com 46 testes automatizados que garantem:
- Unicidade e não-nulidade de chaves primárias.
- Integridade referencial (Relationships) entre Fatos e Dimensões.
- Documentação completa de colunas e métricas acessível via dbt Docs.

---

## 📈 Perguntas de Negócio Respondidas
Com a implementação deste Data Warehouse, a empresa agora consegue responder:
- Eficiência Logística (`dm_eficiencia_logistica`)
    - Qual é o nosso Lead Time médio (tempo entre pedido e envio) por território?
    - Qual a porcentagem de pedidos que estão saindo com atraso em relação à data de vencimento?
    - Existe algum território específico onde a logística está sobrecarregada e gerando mais atrasos?
    - Qual o impacto financeiro (valor em risco) dos pedidos que estão atualmente atrasados?

- Performance de Mercado e Cripto (`dm_performance_mercado_vendas`)
    - Existe correlação entre a volatilidade do mercado de criptomoedas e o volume de vendas da nossa loja?
    - O faturamento da empresa cai quando o mercado cripto está em Drawdown (queda acentuada)?
    - Como o nosso faturamento se comporta em dias de alta volatilidade financeira externa?
    - O ticket médio das vendas muda de acordo com o "humor" do mercado de ativos digitais?

- Performance de Produto e Desconto (`dm_performance_produto_categoria`)
    - Quais são os Top 10 produtos em faturamento e em volume de vendas?
    - Estamos sendo agressivos demais nos descontos? Qual o percentual médio de desconto por produto?
    - Produtos com maior desconto realmente vendem mais volume, ou estamos apenas sacrificando margem?
    - Quais produtos possuem alto faturamento, mas baixo volume (produtos de alto valor agregado)?

- Saúde e Retenção de Clientes (`dm_saude_cliente`)
    - Quantos clientes estão em risco de Churn (sem comprar há mais de 90 dias)?
    - Qual é o LTV (Lifetime Value) médio dos nossos clientes VIPs versus clientes Bronze?
    - Qual a nossa taxa de retenção? Quantos clientes estão "Ativos" no último mês?
    - Qual o ticket médio de um cliente fiel comparado a um cliente novo?

- Visão 360º de Vendas (`dm_vendas_detalhada`)
    - Como as vendas se comportam nos finais de semana em comparação aos dias úteis?
    - Qual a sazonalidade mensal das vendas por categoria de produto?
    - Qual perfil de cliente (VIP/Regular) prefere comprar determinadas cores de produtos?
    - Qual o faturamento detalhado por mês, ano e perfil de fidelidade em uma única visão?

---

## 🖥️ Monitoramento
<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-03/monitoramento-grafana.png" />

---

## 🔄 Orquestração
<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-03/pipeline-dag-airflow.png" />

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

### 👷 Autor
[Linkedin](https://www.linkedin.com/in/juniiorsilvadev/) 