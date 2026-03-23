import pandas as pd
import requests
from bs4 import BeautifulSoup
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime, UTC
import os

# =========================
# CONFIGURAÇÕES
# =========================
BUCKET = "tech-challenge-fase2-fiap"
PREFIX = "Projetos/job_finance/inputs"
LOCAL_FILE = "fundamentus_fii.parquet"
AWS_REGION = "us-east-1"

agora_utc = datetime.now(UTC)
DATE_STR = agora_utc.strftime("%Y-%m-%d")
TIMESTAMP_STR = agora_utc.strftime("%Y-%m-%d %H:%M:%S")

S3_KEY = f"{PREFIX}/dt={DATE_STR}/fundamentus_fii.parquet"


def limpar_numero(valor):
    if pd.isna(valor):
        return None

    valor = str(valor).strip()
    valor = valor.replace(".", "").replace(",", ".").replace("%", "")
    return pd.to_numeric(valor, errors="coerce")


def main():
    print("Extraindo dados do Fundamentus...")

    url = "https://www.fundamentus.com.br/fii_resultado.php"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    tabela = soup.find("table", {"id": "tabelaResultado"})
    if tabela is None:
        raise Exception("Tabela 'tabelaResultado' não encontrada na página.")

    linhas = tabela.find_all("tr")
    dados = []

    for linha in linhas[1:]:
        colunas = linha.find_all("td")

        if len(colunas) > 0:
            valores = []

            for col in colunas:
                if "endereco" not in col.get("class", []):
                    valores.append(col.text.strip())

            dados.append(valores)

    colunas_df = [
        "Papel",
        "Segmento",
        "Cotacao",
        "FFO_Yield",
        "Dividend_Yield",
        "PVP",
        "Valor_de_Mercado",
        "Liquidez",
        "Qtd_de_Imoveis",
        "Preco_m2",
        "Aluguel_m2",
        "Cap_Rate",
        "Vacancia_Media"
    ]

    df = pd.DataFrame(dados, columns=colunas_df)

    print(f"Linhas extraídas: {len(df)}")

    # Tratamento de colunas numéricas
    colunas_numericas = [
        "Cotacao",
        "FFO_Yield",
        "Dividend_Yield",
        "PVP",
        "Valor_de_Mercado",
        "Liquidez",
        "Qtd_de_Imoveis",
        "Preco_m2",
        "Aluguel_m2",
        "Cap_Rate",
        "Vacancia_Media"
    ]

    for col in colunas_numericas:
        df[col] = df[col].apply(limpar_numero)

    # Metadados
    df["data_ingestao"] = DATE_STR
    df["timestamp_ingestao"] = TIMESTAMP_STR
    df["fonte"] = "fundamentus"

    print("Gravando Parquet localmente...")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, LOCAL_FILE, compression="SNAPPY")

    print(f"Enviando {LOCAL_FILE} para s3://{BUCKET}/{S3_KEY} ...")

    # Usa credenciais padrão da AWS (~/.aws/credentials ou variáveis de ambiente)
    s3 = boto3.client("s3", region_name=AWS_REGION)

    try:
        s3.upload_file(LOCAL_FILE, BUCKET, S3_KEY)
        print("Upload concluído com sucesso.")
        print(f"Arquivo salvo em: s3://{BUCKET}/{S3_KEY}")
    except Exception as e:
        print("Erro no upload:", e)
        raise
    finally:
        if os.path.exists(LOCAL_FILE):
            os.remove(LOCAL_FILE)


if __name__ == "__main__":
    main()
