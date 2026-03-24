# Tech Challenge - Fase 2: Pipeline de Dados Bovespa (FIAP)

Este projeto apresenta um pipeline de dados **Serverless na AWS** para extração, tratamento e análise de dados de **Fundos de Investimento Imobiliário (FIIs)** via web scraping do Fundamentus. O objetivo é automatizar o fluxo de dados desde a captura bruta até a geração de indicadores de negócio, como **médias móveis, variações diárias e histórico por papel**.

### Integrantes:
* Gabriel Leite (Owner)
* Raquel Miranda
* Carlos Henrique Neves Júnior
* Bruno Bento
* Victor Hugo

---

## Arquitetura

```
Fundamentus (web scraping) → S3 RAW (Parquet) → Glue Catalog → AWS Lambda → AWS Glue (PySpark) → S3 REFINED (Parquet) → Amazon Athena
```

* **Ingestão:** Script Python (`src/ingestion/extrair_dados.py`) realizando web scraping do **Fundamentus FII**, salvando em **Amazon S3 (camada RAW)** em formato Parquet com path `Projetos/job_finance/inputs/dt={YYYY-MM-DD}/fundamentus_fii.parquet`.
* **Automação:** Função **AWS Lambda** (`src/lambda/lambda_function.py`) disparada por eventos S3, que inicia automaticamente o Glue Job ao detectar novos arquivos Parquet na camada RAW.
* **Processamento:** Job **AWS Glue PySpark** (`src/glue/tech-challenge-fase2-fiap.py`) — lê da tabela catalogada `job_finance.fundamentus_fii` no Glue Catalog, aplica limpeza de tipos, agregações e análise temporal (média móvel 7 períodos, delta diário, histórico por papel) com window functions, salvando três tabelas na camada REFINED.
* **Consulta:** **Amazon Athena** com tabelas externas sobre os dados processados no banco `job_finance`.

---

## Estrutura do Projeto

```
fiap-aws-finance/
├── README.md
├── requirements.txt
├── src/
│   ├── ingestion/
│   │   └── extrair_dados.py       # Scraping Fundamentus FII e upload ao S3 RAW
│   ├── lambda/
│   │   └── lambda_function.py     # Lambda: dispara o Glue Job via eventos S3
│   ├── glue/
│   │   └── tech-challenge-fase2-fiap.py  # ETL PySpark: agregações, renomeações e análise temporal
│   ├── athena/
│   │   └── athena_table.sql       # DDL de referência para tabela externa no Athena
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
* **Bibliotecas:** `pandas`, `pyarrow`, `boto3`, `requests`, `beautifulsoup4`, `lxml`

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

Execute localmente ou no AWS CloudShell para extrair dados de FIIs do Fundamentus e enviar ao S3 RAW:

```bash
python src/ingestion/extrair_dados.py
```

### 2. ETL (AWS Glue)

No AWS Glue Studio, crie um Job Spark e utilize o código em `src/glue/tech-challenge-fase2-fiap.py`.

O job lê da tabela catalogada `job_finance.fundamentus_fii` no Glue Catalog e requer apenas o argumento padrão `--JOB_NAME`. As três tabelas de saída são registradas automaticamente no Glue Catalog.

### 3. Consulta (Amazon Athena)

Execute os seguintes passos no editor do Athena:

1. Crie o banco de dados (se ainda não existir):

```sql
CREATE DATABASE IF NOT EXISTS job_finance;
```

2. Consulte as tabelas geradas pelo Glue Job:

```sql
-- Agregações por papel/segmento
SELECT * FROM job_finance.fundamentus_fii_req5a_agrupado ORDER BY dt DESC LIMIT 20;

-- Dados com colunas renomeadas (cotacao → preco_cota, liquidez → liquidez_diaria)
SELECT * FROM job_finance.fundamentus_fii_req5b_renomeado LIMIT 20;

-- Análise temporal com médias móveis e variações
SELECT * FROM job_finance.fundamentus_fii_req5c_temporal ORDER BY dt DESC LIMIT 20;
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
| `Papel` | Código do FII |
| `Segmento` | Segmento do FII |
| `Cotacao` | Cotação atual |
| `FFO_Yield` | FFO Yield |
| `Dividend_Yield` | Dividend Yield |
| `PVP` | Preço sobre Valor Patrimonial |
| `Valor_de_Mercado` | Valor de mercado total |
| `Liquidez` | Liquidez diária |
| `Qtd_de_Imoveis` | Quantidade de imóveis |
| `Preco_m2` | Preço por m² |
| `Aluguel_m2` | Aluguel por m² |
| `Cap_Rate` | Cap Rate |
| `Vacancia_Media` | Vacância média |
| `data_ingestao` | Data de ingestão |
| `timestamp_ingestao` | Timestamp de ingestão |
| `fonte` | Fonte dos dados (`fundamentus`) |

**REFINED** — tabelas geradas pelo Glue Job no banco `job_finance`:

**`fundamentus_fii_req5a_agrupado`** — Agregações por dt/papel/segmento:

| Coluna | Descrição |
|--------|-----------|
| `dt` | Data de referência |
| `papel` | Código do FII |
| `segmento` | Segmento |
| `qtd_registros` | Contagem de registros |
| `cotacao_media` | Cotação média |
| `valor_mercado_total` | Soma do valor de mercado |
| `ffo_yield_medio` | FFO Yield médio |
| `dividend_yield_medio` | Dividend Yield médio |
| `pvp_medio` | PVP médio |
| `liquidez_total` | Liquidez total |
| `cap_rate_medio` | Cap Rate médio |
| `vacancia_media_max` | Vacância máxima |
| `preco_m2_medio` | Preço m² médio |
| `aluguel_m2_medio` | Aluguel m² médio |

**`fundamentus_fii_req5b_renomeado`** — Dados com colunas renomeadas:

| Coluna | Descrição |
|--------|-----------|
| `preco_cota` | Cotação (renomeada de `cotacao`) |
| `liquidez_diaria` | Liquidez (renomeada de `liquidez`) |
| _(demais colunas inalteradas)_ | |

**`fundamentus_fii_req5c_temporal`** — Análise temporal com window functions:

| Coluna | Descrição |
|--------|-----------|
| `dt` | Data de referência |
| `papel` | Código do FII |
| `segmento` | Segmento |
| `cotacao_media_dia` | Cotação média do dia |
| `valor_mercado_total_dia` | Valor de mercado total do dia |
| `dividend_yield_medio_dia` | Dividend Yield médio do dia |
| `media_movel_7d_cotacao` | Média móvel de 7 períodos por papel |
| `diferenca_vs_dia_anterior` | Delta em relação ao dia anterior |
| `cotacao_max_periodo` | Máxima histórica por papel |
| `cotacao_min_periodo` | Mínima histórica por papel |
| `dias_desde_primeiro_registro` | Dias desde o primeiro registro |

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
