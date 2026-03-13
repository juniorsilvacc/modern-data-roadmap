# 🛠️ Camada de Staging & Ingestão Incremental (Nível 01)
Nesta etapa, implementei um pipeline de dados robusto que realiza a transição dos dados da camada Raw (CSV) para a camada Staging (PostgreSQL/Parquet), focando em eficiência operacional e integridade dos dados.

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
Após a ingestão na Staging, os dados são transformados e organizados seguindo a metodologia Star Schema (Kimball). Esta camada é otimizada para performance analítica e facilidade de uso por ferramentas de visualização (BI).

---

### 🧠 Arquitetura de Dados
O modelo foi desenhado para separar entidades descritivas de eventos quantitativos:

**1. Dimensões Globais (Conformadas):** * dim_produtos: Cadastro central de produtos com atributos de negócio (nome, cor, custo).
- **dim_calendario:** Tabela de tempo inteligente que permite análises por Ano, Mês, Trimestre e Dia da Semana.
- **Por que:** São tabelas "âncoras" que podem ser reutilizadas por qualquer outra área da empresa (como Estoque ou Compras).

**2. Tabela Fato:**
- **fct_vendas:** O coração do Mart. Contém as chaves para as dimensões e as métricas de performance (quantidade, preço unitário, total da linha).
- **Por que:** Focada na granularidade do item do pedido para garantir somas precisas sem duplicação de valores.

A grande magia dessa abordagem é que você consegue responder perguntas que cruzam os dois mundos usando a Dimensão Global como ponte.
- Exemplo de consulta:
  - "Qual a cobertura de estoque? (Quantos dias o meu estoque atual dura baseado na média de vendas?)"

---

### 💎 Diferenciais da Modelagem
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