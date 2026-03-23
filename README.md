# Tech Challenge - Fase 2: Pipeline de Dados Bovespa (FIAP)

Este projeto apresenta um pipeline de dados **Serverless na AWS** para extração, tratamento e análise de dados históricos da B3 (PETR4.SA). O objetivo é automatizar o fluxo de dados desde a captura bruta até a geração de indicadores de negócio, como a **Média Móvel de 3 dias**.

### Integrantes:
* Gabriel Leite (Owner)
* Raquel Miranda
* Carlos Henrique Neves Júnior
* Bruno Bento
* Victor Hugo

---

## Arquitetura

```
yfinance → S3 RAW (Parquet) → AWS Lambda → AWS Glue (PySpark) → S3 REFINED (Parquet) → Amazon Athena
```

* **Ingestão:** Script Python (`src/ingestion/extrair_dados.py`) utilizando `yfinance`, rodando no CloudShell ou localmente, salvando em **Amazon S3 (camada RAW)** em formato Parquet com path `raw/dt={YYYY-MM-DD}/ticker=PETR4.SA/dados.parquet`.
* **Automação:** Função **AWS Lambda** (`src/lambda/lambda_function.py`) disparada por eventos S3, que inicia automaticamente o Glue Job ao detectar novos arquivos Parquet na camada RAW.
* **Processamento:** Job **AWS Glue PySpark** (`src/glue/job_spark_etl.py`) — limpeza de colunas, conversão de tipos e cálculo de **Média Móvel de 3 dias**, salvando na camada REFINED. ⚠️ **A lógica PySpark ETL ainda precisa ser implementada** neste arquivo (atualmente contém um script de extração como placeholder).
* **Consulta:** **Amazon Athena** com tabela externa criada via DDL (`src/sql/athena_table.sql`) sobre os dados processados.

---

## Estrutura do Projeto

```
fiap-aws-finance/
├── README.md
├── requirements.txt
├── src/
│   ├── ingestion/
│   │   └── extrair_dados.py       # Extrai dados da PETR4.SA e envia ao S3 RAW
│   ├── lambda/
│   │   └── lambda_function.py     # Lambda: dispara o Glue Job via eventos S3
│   ├── glue/
│   │   └── job_spark_etl.py       # ETL PySpark (stub): lógica ainda a implementar
│   ├── sql/
│   │   └── athena_table.sql       # DDL da tabela externa no Athena
│   └── scraping/
│       ├── scp_acoes.py           # Scraper de ações (Fundamentus)
│       └── scp_fii.py             # Scraper de FIIs (Fundamentus)
├── notebooks/
│   └── analise.ipynb              # Análise exploratória
└── data/
    ├── raw/
    │   ├── fundamentus_acoes.csv
    │   └── fundamentus_fii.csv
    └── processed/
        ├── clean_acoes.csv
        └── clean_fii.csv
```

---

## Tecnologias Utilizadas

* **Linguagens:** Python 3.x, PySpark, SQL
* **AWS Services:** S3, Glue, Lambda, Athena, IAM, CloudWatch
* **Bibliotecas:** `yfinance`, `pandas`, `pyarrow`, `boto3`, `requests`, `beautifulsoup4`, `lxml`

---

## Como Executar

### Pré-requisitos

```bash
pip install -r requirements.txt
```

Configure as credenciais AWS antes de executar scripts que acessem S3:

```bash
aws configure
```

### 1. Ingestão de Dados

Execute localmente ou no AWS CloudShell para extrair dados da PETR4.SA e enviar ao S3:

```bash
python src/ingestion/extrair_dados.py
```

### 2. ETL (AWS Glue)

No AWS Glue Studio, crie um Job Spark e utilize o código em `src/glue/job_spark_etl.py`.

O job recebe os argumentos `--source_bucket` e `--source_keys`, que são passados automaticamente pela Lambda.

> ⚠️ **Atenção:** `src/glue/job_spark_etl.py` atualmente contém um script de extração como placeholder. A lógica PySpark ETL (renomeação de colunas RAW → REFINED, conversão de tipos e cálculo de `media_movel_3_dias` com window function) ainda precisa ser implementada.

### 3. Consulta (Amazon Athena)

Execute os seguintes passos no editor do Athena:

1. Crie o banco de dados (se ainda não existir):

```sql
CREATE DATABASE IF NOT EXISTS fiap_finance;
```

2. Execute o DDL em `src/sql/athena_table.sql` para registrar a tabela externa sobre a camada REFINED.

3. Consulte os dados:

```sql
SELECT * FROM fiap_finance.bovespa_refined ORDER BY date DESC LIMIT 20;
```

### 4. Scraping Fundamentus (opcional)

```bash
python src/scraping/scp_acoes.py   # → fundamentus.csv (diretório atual)
python src/scraping/scp_fii.py     # → fundamentus_fii.csv (diretório atual)
```

> Os arquivos são salvos no diretório de execução. Mova-os manualmente para `data/raw/` se necessário.

---

## Schema de Dados

**RAW** (`src/ingestion/extrair_dados.py`):

| Coluna | Descrição |
|--------|-----------|
| `data_pregao` | Data do pregão |
| `preco_fechamento` | Preço de fechamento |
| `high_petr4_sa` | Máxima do dia |
| `low_petr4_sa` | Mínima do dia |
| `open_petr4_sa` | Abertura |
| `volume_negociado` | Volume negociado |

**REFINED** (Athena `bovespa_refined`):

| Coluna | Descrição |
|--------|-----------|
| `date` | Data do pregão |
| `close` | Preço de fechamento |
| `high` | Máxima |
| `low` | Mínima |
| `open` | Abertura |
| `volume` | Volume |
| `media_movel_3_dias` | Média móvel de 3 dias |
| `data_processamento` | Timestamp do processamento |

---

## Configuração da Lambda

A Lambda (`src/lambda/lambda_function.py`) automatiza o disparo do Glue Job ao detectar novos arquivos Parquet na camada RAW.

### 1. IAM Role

Crie uma Role com a seguinte policy inline (substitua `ACCOUNT_ID` e `SEU_GLUE_JOB`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJob"],
      "Resource": "arn:aws:glue:us-east-1:ACCOUNT_ID:job/SEU_GLUE_JOB"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

### 2. Variável de Ambiente

| Chave | Valor |
|-------|-------|
| `GLUE_JOB_NAME` | Nome do Glue Job |

### 3. Trigger S3

1. **S3 → Bucket → Properties → Event notifications → Create event notification**
2. Configure:
   - **Event types:** `s3:ObjectCreated:*`
   - **Prefix:** `raw/`
   - **Suffix:** `.parquet`
   - **Destination:** Lambda Function → selecione sua Lambda
