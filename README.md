# Tech Challenge - Fase 2: Pipeline de Dados Bovespa (FIAP)

Este projeto apresenta um pipeline de dados **Serverless na AWS** para extração, tratamento e análise de dados históricos da B3 (PETR4.SA). O objetivo é automatizar o fluxo de dados desde a captura bruta até a geração de indicadores de negócio, como a **Média Móvel de 3 dias**.

### Integrantes:
* Gabriel Leite (Owner)
* Raquel Miranda
* Carlos Henrique Neves Júnior
* bruno Bento
* Victor Hugo
---

### #### Resumo do Projeto
O pipeline foi desenhado para ser escalável e de baixo custo, utilizando serviços gerenciados da AWS. O foco principal é a transformação de dados brutos em informações prontas para análise (camada Refined), garantindo a integridade matemática dos indicadores calculados.

### #### Arquitetura
* **Ingestão:** Script Python utilizando a biblioteca `yfinance` rodando no CloudShell, salvando em **Amazon S3 (Camada RAW)** em formato Parquet particionado por data.
* **Processamento:** Job em **AWS Glue (PySpark)** para limpeza de colunas, conversão de tipos e cálculo de **Média Móvel de 3 dias**, salvando em **Amazon S3 (Camada REFINED)**.
* **Consumo:** **Amazon Athena** para criação de tabela externa e consultas SQL sobre os dados processados.

### #### Tecnologias Utilizadas
* **Linguagens:** Python 3.x, PySpark, SQL.
- **AWS Services:** S3, Glue, Lambda, Athena, IAM, CloudWatch.
- **Bibliotecas:** `yfinance`, `pandas`, `pyarrow`, `boto3`.

---

### #### Como Executar

1. **Extração:** Execute o script `scripts/extrair_dados.py` para alimentar a pasta `raw/` no S3.
2. **ETL:** No AWS Glue Studio, crie um Job Spark e utilize o código contido em `scripts/job_glue_spark.py`.
3. **Consulta:** No Amazon Athena, execute o DDL contido em `scripts/queries_athena.sql` para criar a base de dados e a tabela.
4. **Validação:** Rode o comando de validação para comparar a média móvel calculada no Spark com o SQL:
   ```sql
   SELECT * FROM fiap_finance.bovespa_detalhado ORDER BY data_pregao DESC LIMIT 20;
