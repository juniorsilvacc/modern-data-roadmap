# 🛠️ Camada de Staging & Ingestão Incremental (Nível 01)
Nesta etapa, implementei um pipeline de dados robusto que realiza a transição dos dados da camada Raw (CSV) para a camada Staging (PostgreSQL/Parquet) e, finalmente, para a Camada de Marts (Star Schema). O foco evoluiu da simples ingestão para a entrega de um modelo pronto para análise, priorizando eficiência operacional, integridade e performance analítica.

---

## 🏗️ Arquitetura

<img width="1750" height="874" alt="Image" src="https://github.com/juniorsilvacc/modern-data-roadmap/blob/master/nivel-01/arquitetura-nv1.png" />

---

## 🚀 Diferenciais Técnicos
- **Idempotência Garantida:** O script foi projetado para ser executado múltiplas vezes sem causar efeitos colaterais. Antes de cada inserção, o sistema limpa a "janela" correspondente no banco de dados, garantindo que não existam registros duplicados, independentemente de quantas vezes o pipeline for acionado.
- **Janela Incremental (Delta Load):** Em vez de processar toda a base histórica (Full Load), o pipeline trafega apenas os últimos 60 dias de dados. Isso reduz drasticamente o uso de rede e o estresse no banco de dados, otimizando a performance.
- **Sincronização de Alterações (CDC Manual):** Se um registro de 15 dias atrás for modificado na origem, essa alteração será refletida automaticamente no Data Warehouse. O processo deleta a versão obsoleta e insere a versão mais recente contida no arquivo.
- **Eficiência de Memória:** Através do uso de Pandas e filtros de data, o Python processa apenas a "fatia" necessária dos dados, evitando o estouro de memória e permitindo o escalonamento do pipeline.
- **Tipagem e Padronização:** Implementação de casting rigoroso (tipagem de dados) e renomeação de colunas para o padrão de negócio, facilitando o consumo posterior por ferramentas de BI.

---

## 📊 Estratégia de Carga
| Carga         | Frequência | Descrição                                                                      |
|---------------|------------|--------------------------------------------------------------------------------|
| `Incremental` | Diária     | Atualiza os últimos 60 dias para capturar novos dados e modificações.          |
| `Full Load`   | Automático | Executado apenas na primeira vez ou se a tabela de destino não for encontrada. |

---

## 🏛️ Camada de Marts (Modelagem Dimensional)
Após a ingestão na Staging, os dados são transformados e organizados seguindo a metodologia Star Schema (Kimball). Esta camada é o "coração" do Business Intelligence, garantindo que o Power BI consuma dados limpos e performáticos.

---

## 🧠 Arquitetura de Dados
O modelo foi desenhado para separar entidades descritivas de eventos quantitativos, garantindo escalabilidade:

- Tabela Fato (`fct_vendas`): Consolida os eventos de venda com granularidade ao nível de item. Contém as métricas quantitativas (Quantidade, Valor) e as Surrogate Keys para conexão com as dimensões.
- Dimensões Conformadas (`dim_produtos`, `dim_calendario`): Funcionam como "âncoras" de negócio.
  - Por que: Permitem que qualquer nova tabela fato adicionada ao projeto (ex: Estoque) utilize as mesmas dimensões, garantindo uma única versão da verdade (Bus Architecture).

---

## 💎 Diferenciais da Modelagem
- **Chaves de Performance (Surrogate Keys):** Uso de chaves numéricas inteiras (data_sk no formato YYYYMMDD) para garantir que os JOINs sejam executados na velocidade máxima do banco de dados.
- **Localidade e Padronização:** Tratamento de strings com TMMonth para garantir nomes de meses limpos e em português, prontos para filtros de dashboard.
- **Escalabilidade (Bus Architecture):** O modelo está preparado para o crescimento. Se novos processos de negócio forem adicionados (ex: Estoque), eles compartilharão as mesmas dimensões globais já existentes.

---

## 📈 Estrutura do Modelo 
| Tabela           | Tipo          | Descrição                                          |
|------------------|---------------|----------------------------------------------------|
| `fct_vendas`     | Fato          | Métricas de vendas e chaves de ligação.            |
| `dim_produtos`   | Dimensão      | Detalhes técnicos e comerciais dos produtos.       |
| `dim_calendario` | Dimensão      | Inteligência de tempo para agrupamentos temporais. |

---

## 📊 Analytics: A Ponta do Iceberg
Com a Marts estruturada, o Power BI deixa de fazer "limpeza de dados" e foca em Visualização e DAX. O resultado é um dashboard extremamente rápido e confiável, focado em:

- Rentabilidade: Análise de Custo vs. Faturamento por produto.
- Sazonalidade: Identificação de picos de venda por dia da semana e meses.
- Performance: KPIs de crescimento mensal (MoM%) e Rankings de Top Produtos.

---
