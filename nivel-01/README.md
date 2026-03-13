# 🛠️ Camada de Staging & Ingestão Incremental (Nível 01)
Nesta etapa, implementei um pipeline de dados robusto que realiza a transição dos dados da camada Raw (CSV) para a camada Staging (PostgreSQL/Parquet), focando em eficiência operacional e integridade dos dados.

## 🚀 Diferenciais Técnicos
**Idempotência Garantida:** O script foi projetado para ser executado múltiplas vezes sem causar efeitos colaterais. Antes de cada inserção, o sistema limpa a "janela" correspondente no banco de dados, garantindo que não existam registros duplicados, independentemente de quantas vezes o pipeline for acionado.
**Janela Incremental (Delta Load):** Em vez de processar toda a base histórica (Full Load), o pipeline trafega apenas os últimos 60 dias de dados. Isso reduz drasticamente o uso de rede e o estresse no banco de dados, otimizando a performance.
**Sincronização de Alterações (CDC Manual):** Se um registro de 15 dias atrás for modificado na origem, essa alteração será refletida automaticamente no Data Warehouse. O processo deleta a versão obsoleta e insere a versão mais recente contida no arquivo.
**Eficiência de Memória:** Através do uso de Pandas e filtros de data, o Python processa apenas a "fatia" necessária dos dados, evitando o estouro de memória e permitindo o escalonamento do pipeline.
**Tipagem e Padronização:** Implementação de casting rigoroso (tipagem de dados) e renomeação de colunas para o padrão de negócio, facilitando o consumo posterior por ferramentas de BI.

Tipo de Carga,Frequência,Descrição
Incremental,Diária,Atualiza os últimos 60 dias para capturar novos dados e modificações.
Full Load,Automático,Executado apenas na primeira vez ou se a tabela de destino não for encontrada.

## 📊 Estratégia de Carga
| Carga         | Frequência | Descrição                                                                      |
|---------------|------------|--------------------------------------------------------------------------------|
| `Incremental` | Diária     | Atualiza os últimos 60 dias para capturar novos dados e modificações.          |
| `Full Load`   | Automático | Executado apenas na primeira vez ou se a tabela de destino não for encontrada. |
