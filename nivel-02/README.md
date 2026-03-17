# 🛠️ Modern Data Pipeline: Da Infraestrutura Docker ao Analytics (Nível 02)
Nesta etapa, implementei um ecossistema de dados completo e isolado. O pipeline realiza a transição dos dados brutos (Raw/CSV) para uma camada de Staging (PostgreSQL) com carga incremental, culminando na criação de uma Camada de Marts modelada em Star Schema para alta performance analítica no Power BI.

---

## 🏗️ Arquitetura do Projeto

<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-02/arquitetura-nv2.png" />

---

## 🚀Diferenciais Estratégicos
- Pipeline End-to-End: Domínio de todo o ciclo de vida do dado, desde a ingestão até a visualização executiva.
- Orquestração Modern Data Stack: Uso de Docker para garantir que o ambiente seja replicável e isolado, rodando em qualquer máquina com um único comando.
- Foco em Performance: Modelagem dimensional (Marts) que reduz a complexidade do Power BI e acelera o processamento de grandes volumes.

---

## 🐋 Infraestrutura como Código (Docker)
Para garantir que o ambiente seja replicável e isolado, utilizei o Docker para orquestrar os serviços essenciais.

- PostgreSQL Container: Banco de dados relacional que atua como nosso Data Warehouse.
- Persistência de Dados: Uso de Volumes para garantir que as tabelas de Staging e Marts não sejam perdidas ao reiniciar o container.
- Conectividade: Exposição de portas configurada para permitir que o Power BI (Host) consuma os dados do banco (Container) em tempo real.

---

## 📊 Analytics & Business Intelligence (Power BI)

<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-02/reports/dashboard.png" />

---

## 🧠 Camada de Cálculo (DAX)
Desenvolvi métricas personalizadas para responder dores reais de negócio:

- Rentabilidade Dinâmica: Medida de Custo Total via `SUMX` e `RELATED`, permitindo ver a margem real mesmo em grandes volumes de vendas.
- Inteligência de Tempo: Cálculo de Variação Mensal (MoM%), comparando o faturamento atual com o período anterior para detectar sazonalidades.
- Ranking de Performance: Filtros de Top N aplicados via contexto para destacar os produtos que realmente movem o ponteiro da receita.

---

## 🎨 Design e User Experience (UX)
- Layout Executivo: Organização em grid (Z-Pattern) focada na leitura rápida de KPIs.
- Interatividade Total: Filtros cruzados e segmentadores que permitem análises profundas por cor, classe e estilo com apenas um clique.
