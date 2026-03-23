import sys
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================================================
# ARGUMENTOS
# =========================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# =========================================================
# CONTEXTO GLUE / SPARK
# =========================================================
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# =========================================================
# CONFIGURAÇÕES
# =========================================================
SOURCE_DATABASE = "job_finance"
SOURCE_TABLE = "fundamentus_fii"

TARGET_DATABASE = "job_finance"

S3_BASE_OUTPUT = "s3://tech-challenge-fase2-fiap/Projetos/job_finance/outputs/"

TABLE_REQ5A = "fundamentus_fii_req5a_agrupado"
TABLE_REQ5B = "fundamentus_fii_req5b_renomeado"
TABLE_REQ5C = "fundamentus_fii_req5c_temporal"

PATH_REQ5A = f"{S3_BASE_OUTPUT}fundamentus_fii_req5a_agrupado/"
PATH_REQ5B = f"{S3_BASE_OUTPUT}fundamentus_fii_req5b_renomeado/"
PATH_REQ5C = f"{S3_BASE_OUTPUT}fundamentus_fii_req5c_temporal/"

# =========================================================
# GARANTE DATABASE NO GLUE CATALOG
# =========================================================
glue_client = boto3.client("glue")

try:
    glue_client.get_database(Name=TARGET_DATABASE)
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_database(
        DatabaseInput={
            "Name": TARGET_DATABASE,
            "Description": "Banco de dados refinado do Tech Challenge"
        }
    )

# =========================================================
# LEITURA DA TABELA DE ORIGEM
# =========================================================
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE,
    transformation_ctx="source_dyf"
)

df = source_dyf.toDF()

# =========================================================
# LIMPEZA / TIPAGEM
# =========================================================
df = (
    df.withColumn("papel", F.col("papel").cast("string"))
      .withColumn("segmento", F.col("segmento").cast("string"))
      .withColumn("cotacao", F.col("cotacao").cast("double"))
      .withColumn("ffo_yield", F.col("ffo_yield").cast("double"))
      .withColumn("dividend_yield", F.col("dividend_yield").cast("double"))
      .withColumn("pvp", F.col("pvp").cast("double"))
      .withColumn("valor_de_mercado", F.col("valor_de_mercado").cast("double"))
      .withColumn("liquidez", F.col("liquidez").cast("double"))
      .withColumn("qtd_de_imoveis", F.col("qtd_de_imoveis").cast("double"))
      .withColumn("preco_m2", F.col("preco_m2").cast("double"))
      .withColumn("aluguel_m2", F.col("aluguel_m2").cast("double"))
      .withColumn("cap_rate", F.col("cap_rate").cast("double"))
      .withColumn("vacancia_media", F.col("vacancia_media").cast("double"))
      .withColumn("data_ingestao", F.to_date("data_ingestao"))
      .withColumn("dt", F.to_date("dt"))
      .withColumn("timestamp_ingestao", F.to_timestamp("timestamp_ingestao"))
)

df = df.filter(
    F.col("papel").isNotNull() &
    F.col("segmento").isNotNull() &
    F.col("dt").isNotNull()
)

# =========================================================
# REQUISITO 5A
# AGRUPAMENTO NUMÉRICO + SUMARIZAÇÃO + CONTAGEM
# TABELA 1
# =========================================================
df_req5a = (
    df.groupBy("dt", "papel", "segmento")
      .agg(
          F.count("*").alias("qtd_registros"),
          F.avg("cotacao").alias("cotacao_media"),
          F.sum("valor_de_mercado").alias("valor_mercado_total"),
          F.avg("ffo_yield").alias("ffo_yield_medio"),
          F.avg("dividend_yield").alias("dividend_yield_medio"),
          F.avg("pvp").alias("pvp_medio"),
          F.sum("liquidez").alias("liquidez_total"),
          F.avg("cap_rate").alias("cap_rate_medio"),
          F.max("vacancia_media").alias("vacancia_media_max"),
          F.avg("preco_m2").alias("preco_m2_medio"),
          F.avg("aluguel_m2").alias("aluguel_m2_medio")
      )
)

# =========================================================
# REQUISITO 5B
# RENOMEAR DUAS COLUNAS EXISTENTES
# TABELA 2
# =========================================================
df_req5b = (
    df.withColumnRenamed("cotacao", "preco_cota")
      .withColumnRenamed("liquidez", "liquidez_diaria")
)

# =========================================================
# REQUISITO 5C
# CÁLCULO TEMPORAL COM BASE NA DATA
# TABELA 3
# - média móvel 7 períodos por papel
# - diferença para dia anterior
# - máximo/mínimo histórico por papel
# =========================================================
# primeiro consolida por papel/dt para evitar duplicidade por dia
df_temporal_base = (
    df.groupBy("dt", "papel", "segmento")
      .agg(
          F.avg("cotacao").alias("cotacao_media_dia"),
          F.sum("valor_de_mercado").alias("valor_mercado_total_dia"),
          F.avg("dividend_yield").alias("dividend_yield_medio_dia")
      )
)

w_order = Window.partitionBy("papel").orderBy("dt")
w_7 = Window.partitionBy("papel").orderBy("dt").rowsBetween(-6, 0)
w_full = Window.partitionBy("papel")

df_req5c = (
    df_temporal_base
      .withColumn(
          "media_movel_7d_cotacao",
          F.avg("cotacao_media_dia").over(w_7)
      )
      .withColumn(
          "diferenca_vs_dia_anterior",
          F.col("cotacao_media_dia") - F.lag("cotacao_media_dia", 1).over(w_order)
      )
      .withColumn(
          "cotacao_max_periodo",
          F.max("cotacao_media_dia").over(w_full)
      )
      .withColumn(
          "cotacao_min_periodo",
          F.min("cotacao_media_dia").over(w_full)
      )
      .withColumn(
          "dias_desde_primeiro_registro",
          F.datediff(F.col("dt"), F.min("dt").over(w_full))
      )
)

# =========================================================
# FUNÇÃO AUXILIAR PARA ESCREVER S3 + CATALOG
# =========================================================
def write_parquet_catalog(df_spark, path, database, table_name, partition_keys):
    dyf = DynamicFrame.fromDF(df_spark, glueContext, f"dyf_{table_name}")

    sink = glueContext.getSink(
        path=path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
        enableUpdateCatalog=True,
        transformation_ctx=f"sink_{table_name}"
    )

    sink.setCatalogInfo(
        catalogDatabase=database,
        catalogTableName=table_name
    )

    sink.setFormat("glueparquet")
    sink.writeFrame(dyf)

# =========================================================
# ESCRITA DAS 3 TABELAS
# =========================================================
# Req 5A
write_parquet_catalog(
    df_spark=df_req5a,
    path=PATH_REQ5A,
    database=TARGET_DATABASE,
    table_name=TABLE_REQ5A,
    partition_keys=["dt", "papel"]
)

# Req 5B
write_parquet_catalog(
    df_spark=df_req5b,
    path=PATH_REQ5B,
    database=TARGET_DATABASE,
    table_name=TABLE_REQ5B,
    partition_keys=["dt", "papel"]
)

# Req 5C
write_parquet_catalog(
    df_spark=df_req5c,
    path=PATH_REQ5C,
    database=TARGET_DATABASE,
    table_name=TABLE_REQ5C,
    partition_keys=["dt", "papel"]
)

job.commit()