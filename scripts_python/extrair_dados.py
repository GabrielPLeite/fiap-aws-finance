# scripts_python/extrair_dados.py
import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime
import os

TICKER = "PETR4.SA"
BUCKET = "tech-challenge-fase2-raquel-miranda"  # ajuste se necessário
DATE_STR = datetime.today().strftime("%Y-%m-%d")
S3_KEY = f"raw/dt={DATE_STR}/ticker={TICKER}/dados.parquet"
LOCAL_FILE = "dados.parquet"

def main():
    print(f"Extraindo dados de {TICKER} com yfinance...")
    df = yf.download(TICKER, period="1mo", interval="1d")
    df.reset_index(inplace=True)
    print(f"Linhas baixadas: {len(df)}")

    # Se quiser renomear colunas para combinar com o pipeline:
    # Por exemplo, transformar 'Date' -> 'data_pregao', 'Close' -> 'preco_fechamento'
    rename_map = {
        'Date': 'data_pregao',
        'Adj Close': 'preco_fechamento',  # caso exista
        'Close': 'preco_fechamento',
        'High': 'high_petr4_sa',
        'Low': 'low_petr4_sa',
        'Open': 'open_petr4_sa',
        'Volume': 'volume_negociado'
    }
    df.rename(columns=rename_map, inplace=True)

    # Converter data para formato date
    if 'data_pregao' in df.columns:
        df['data_pregao'] = pd.to_datetime(df['data_pregao']).dt.date

    print("Gravando Parquet localmente...")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, LOCAL_FILE, compression='SNAPPY')

    print(f"Enviando {LOCAL_FILE} para s3://{BUCKET}/{S3_KEY} ...")
    s3 = boto3.client('s3')
    try:
        s3.upload_file(LOCAL_FILE, BUCKET, S3_KEY)
        print("Upload concluído.")
    except Exception as e:
        print("Erro no upload:", e)
        raise
    finally:
        # remover arquivo local
        if os.path.exists(LOCAL_FILE):
            os.remove(LOCAL_FILE)

if __name__ == "__main__":
    main()
