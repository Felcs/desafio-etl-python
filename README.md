# Desafio Técnico: Pipeline de Dados com Python

Este projeto implementa um pipeline de ETL (Extração, Transformação e Carga) e análise de dados.

O pipeline processa dados históricos de vendas, produtos e clientes, realizando a limpeza, transformação e carga em um ambiente multi-banco (PostgreSQL e MongoDB). Além disso, gera uma série de relatórios analíticos e datasets otimizados.

## Funcionalidades Implementadas

-   **Pipeline de ETL Robusto:** Processamento de arquivos grandes (`.csv`, `.json`) com uma abordagem de streaming (chunks) para garantir performance e baixo consumo de memória.
-   **Limpeza e Qualidade de Dados:** Validações e correções em tempo de execução, como a verificação de IDs de venda e remoção de duplicatas.
-   **Armazenamento Híbrido:** Os dados são carregados em um banco de dados relacional (PostgreSQL) para análises estruturadas e replicados em um banco NoSQL (MongoDB).
-   **Geração de Indicadores:** Criação de múltiplos relatórios de negócio via queries SQL, como produtos mais vendidos, clientes com mais compras, etc.
-   **Análise Avançada:** Identificação do dia da semana de compra preferido para os 20 melhores clientes.
-   **Validação de Integridade:** Geração de um relatório de "registros órfãos" para identificar vendas de produtos ou clientes não cadastrados.
-   **Saída para Data Lake:** Criação de um dataset otimizado em formato Parquet, particionado por ano e mês para performance em futuras análises.
-   **Containerização Completa:** Todo o ambiente é orquestrado com Docker e Docker Compose, garantindo fácil execução e consistência.

## Tecnologias Utilizadas

-   **Linguagem:** Python 3.12
-   **Bibliotecas Principais:** Pandas, SQLAlchemy (para PostgreSQL), PyMongo (para MongoDB), PyArrow (para Parquet)
-   **Bancos de Dados:** PostgreSQL 17, MongoDB 8
-   **Containerização:** Docker, Docker Compose

## Estrutura do Projeto

O projeto foi organizado em módulos com responsabilidades claras para facilitar a manutenção e o entendimento.

```
.
├── data/                 # Local para os arquivos de dados de entrada
├── output/               # Destino dos relatórios (CSV) e datasets (Parquet)
├── sql/                  # Scripts .sql (ex: criação de tabelas)
├── src/                  # Código fonte principal da aplicação
│   ├── __init__.py
│   ├── main.py           # Orquestrador principal do pipeline
│   ├── database.py       # Gerencia as conexões com os bancos
│   ├── etl.py            # Contém a lógica de Extração, Transformação e Carga
│   └── reports.py        # Contém a lógica de análise e geração de relatórios
├── .dockerignore
├── .gitignore
├── Dockerfile            # Define a imagem da aplicação Python
├── docker-compose.yml    # Orquestra todos os serviços (app, postgres, mongo)
└── requirements.txt
```

---

## Como Executar o Projeto

Siga os passos abaixo para executar o pipeline completo em seu ambiente local.

### Pré-requisitos

-   [Docker](https://docs.docker.com/get-docker/) instalado e em execução.
-   [Docker Compose](https://docs.docker.com/compose/install/) (geralmente já vem com o Docker Desktop).
-   Git para clonar o repositório.

### 1. Clonar o Repositório

```bash
git clone [[URL_DO_SEU_REPOSITORIO]](https://github.com/Felcs/desafio-etl-python.git)
cd [NOME_DA_PASTA_DO_PROJETO]
```

### 2. Preparar os Arquivos de Dados

-   Crie uma pasta chamada `data` na raiz do projeto, caso não exista.
-   Coloque os arquivos de dados fornecidos (`vendas.csv`, `produtos.csv`, `clientes.json`) dentro desta pasta `data/`.

### 3. Construir e Executar a Pipeline

Abra um terminal na pasta raiz do projeto e execute o seguinte comando:

```bash
docker-compose up --build
```

Este único comando irá:
1.  **Construir a imagem Docker** para a aplicação Python, instalando todas as dependências do `requirements.txt`.
2.  **Iniciar os contêineres** para a aplicação (`app`), o banco de dados PostgreSQL (`postgres`) e o banco de dados MongoDB (`mongo`).
3.  **Esperar os bancos de dados estarem prontos** para aceitar conexões.
4.  **Executar o `main.py`**, que orquestra todo o pipeline:
    -   Cria as tabelas no PostgreSQL.
    -   Carrega e transforma os dados dos arquivos de origem para os bancos.
    -   Gera todos os relatórios de análise.
    -   Cria o dataset Parquet final.

A execução pode levar vários minutos, principalmente devido ao grande volume de dados do arquivo de vendas. Você pode acompanhar o progresso pelos logs no terminal.

### 4. Verificar os Resultados

Ao final da execução, você encontrará:
-   **Relatórios CSV:** Dentro da pasta `output/`, estarão todos os arquivos `.csv` com os indicadores solicitados.
-   **Dataset Parquet:** Dentro de `output/vendas_enriquecidas/`, você verá uma estrutura de pastas particionada por ano e mês contendo os arquivos `.parquet`.
-   **Bancos de Dados:** Você pode se conectar aos bancos usando uma ferramenta como DBeaver ou pgAdmin para inspecionar os dados carregados:
    -   **PostgreSQL:** `host=localhost`, `port=5432`, `user=mercafacil_adm`, `password=testemercafacil`, `database=mercafacil`.
    -   **MongoDB:** `host=localhost`, `port=27017`.

### 5. Parar os Contêineres

Para parar todos os serviços, pressione `Ctrl + C` no terminal onde o compose está rodando, ou abra um novo terminal e execute:

```bash
docker-compose down
```
