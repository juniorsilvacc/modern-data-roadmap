{% docs __overview__ %}

## 📊 Modern Data Warehouse: AdventureWorks + Crypto Analytics
Este projeto demonstra a implementação de um Modern Data Warehouse utilizando dbt (data build tool). O objetivo é transformar dados brutos de um ERP (AdventureWorks) e de uma API de Criptomoedas em uma camada de Analytics de alta performance, utilizando a Arquitetura Medallion e princípios de Software Engineering (SOLID/OOP) aplicados a dados.

---

## 🚀 Tecnologias Utilizadas
- Linguagens: **Python e SQL**
- Transformação: **dbt (Data Build Tool)**
- Banco de Dados: **PostgreSQL**
- Orquestração: **Apache Airflow / Docker**
- Qualidade de Dados: **dbt Tests (Generic & Singular)**

---

## 🏗️ Arquitetura de Dados
O projeto segue a separação lógica em camadas para garantir escalabilidade e governança:
- **Staging (Bronze):** Limpeza inicial, renomeação de colunas e tipagem.
- **Intermediate (Silver):** Camada de transformação onde aplicamos regras de negócio complexas, como o cálculo de volatilidade cripto e métricas de performance de clientes.
- **Core (Gold/Star Schema):** Modelagem dimensional com tabelas Fato (`fct_vendas`) e Dimensões (`dim_produtos, dim_clientes, dim_calendario`).
- **Analytics (Marts):** Tabelas prontas para consumo (OBT e Agregações) focadas em Logística, CRM, Finanças e Correlação de Mercado.

---

## 💡 Business Insights (Data Marts)
O diferencial deste DW é a entrega de dashboards prontos para responder:
- **Eficiência Logística:** Cálculo de Lead Time e identificação de gargalos por território.
- **Correlação Cripto:** Análise de como o faturamento da empresa se comporta em relação à volatilidade do mercado de criptomoedas.
- **Saúde do Cliente (CRM):** Segmentação de clientes por LTV e identificação automática de Churn baseado em dias de inatividade.
- **Performance de Produto:** Ranking de lucratividade e análise de agressividade de descontos.

---

## ✅ Qualidade e Governança
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

---

- Performance de Mercado e Cripto (`dm_performance_mercado_vendas`)
    - Existe correlação entre a volatilidade do mercado de criptomoedas e o volume de vendas da nossa loja?
    - O faturamento da empresa cai quando o mercado cripto está em Drawdown (queda acentuada)?
    - Como o nosso faturamento se comporta em dias de alta volatilidade financeira externa?
    - O ticket médio das vendas muda de acordo com o "humor" do mercado de ativos digitais?

---

- Performance de Produto e Desconto (`dm_performance_produto_categoria`)
    - Quais são os Top 10 produtos em faturamento e em volume de vendas?
    - Estamos sendo agressivos demais nos descontos? Qual o percentual médio de desconto por produto?
    - Produtos com maior desconto realmente vendem mais volume, ou estamos apenas sacrificando margem?
    - Quais produtos possuem alto faturamento, mas baixo volume (produtos de alto valor agregado)?

---

- Saúde e Retenção de Clientes (`dm_saude_cliente`)
    - Quantos clientes estão em risco de Churn (sem comprar há mais de 90 dias)?
    - Qual é o LTV (Lifetime Value) médio dos nossos clientes VIPs versus clientes Bronze?
    - Qual a nossa taxa de retenção? Quantos clientes estão "Ativos" no último mês?
    - Qual o ticket médio de um cliente fiel comparado a um cliente novo?

---

- Visão 360º de Vendas (`dm_vendas_detalhada`)
    - Como as vendas se comportam nos finais de semana em comparação aos dias úteis?
    - Qual a sazonalidade mensal das vendas por categoria de produto?
    - Qual perfil de cliente (VIP/Regular) prefere comprar determinadas cores de produtos?
    - Qual o faturamento detalhado por mês, ano e perfil de fidelidade em uma única visão?

---

**Mantenedor:** Junior Silva - Data Engineer

**Linkedin:** [Linkedin](https://www.linkedin.com/in/juniiorsilvadev/)

**Github:** [Github](https://github.com/juniorsilvacc)

**Última Atualização:** 2026-03-29

{% enddocs %}