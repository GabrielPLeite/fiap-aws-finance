import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

S3_RAW = "s3://tech-challenge-fase2-raquel-miranda/raw/"
S3_REFINED = "s3://tech-challenge-fase2-raquel-miranda/refined/"

print("Lendo dados da camada RAW...")
df = spark.read.parquet(S3_RAW)

# Limpeza dos nomes de colunas estranhos vindos do yfinance
for col_name in df.columns:
    new_name = col_name.replace("('", "").replace("', '", "_").replace("')", "").replace("', '')", "").lower()
    clean_name = new_name.split("_")[0]
    df = df.withColumnRenamed(col_name, clean_name)

# Garantir tipos
if "date" in df.columns:
    df = df.withColumn("date", F.to_date(F.col("date")))
if "close" in df.columns:
    df = df.withColumn("close", F.col("close").cast("double"))

# Média móvel de 3 dias no fechamento
windowSpec = Window.orderBy("date").rowsBetween(-2, 0)
df = df.withColumn("media_movel_3_dias", F.avg("close").over(windowSpec))

# Timestamp de processamento
df = df.withColumn("data_processamento", F.current_timestamp())

print("Mostrando as 5 primeiras linhas:")
df.show(5, truncate=False)

print("Gravando no REFINED em parquet...")
df.write.mode("overwrite").parquet(S3_REFINED)

print("Job finalizado.")
job.commit()