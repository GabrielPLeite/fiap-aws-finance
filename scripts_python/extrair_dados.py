# Script de extração para rodar no CloudShell ou local
import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime
import os

TICKER = "PETR4.SA"
BUCKET = "tech-challenge-fase2-raquel-miranda"  # <- ajuste se necessário
S3_KEY = f"raw/dt={datetime.today().strftime('%Y-%m-%d')}/dados.parquet"
LOCAL_FILE = "dados.parquet"

def main():
    print("Baixando dados com yfinance...")
    df = yf.download(TICKER, period="1mo", interval="1d")
    df.reset_index(inplace=True)
    print(f"Linhas baixadas: {len(df)}")

    print("Convertendo para parquet localmente...")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, LOCAL_FILE)

    print(f"Enviando {LOCAL_FILE} para s3://{BUCKET}/{S3_KEY} ...")
    s3 = boto3.client('s3')
    try:
        s3.upload_file(LOCAL_FILE, BUCKET, S3_KEY)
        print("Upload concluído.")
    except Exception as e:
        print("Erro no upload:", e)
        raise

    # opcional: remover arquivo local
    try:
        os.remove(LOCAL_FILE)
    except:
        pass

if __name__ == "__main__":
    main()