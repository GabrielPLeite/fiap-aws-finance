
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

# Nome do bucket S3 onde o arquivo será salvo
BUCKET = "tech-challenge-fase2-fiap"

# Prefixo/pasta base dentro do bucket
PREFIX = "Projetos/job_finance/inputs"

# Nome do arquivo parquet local gerado temporariamente
LOCAL_FILE = "fundamentus_fii.parquet"

# Credenciais AWS (não é recomendado deixar no código em produção)
AWS_ACCESS_KEY_ID = "SUA_ACCESS_KEY"
AWS_SECRET_ACCESS_KEY = "SUA_SECRET_KEY"
AWS_REGION = "us-east-1"

# Obtém a data/hora atual em UTC
agora_utc = datetime.now(UTC)

# Gera a string da data no formato YYYY-MM-DD para usar na partição
DATE_STR = agora_utc.strftime("%Y-%m-%d")

# Gera a string do timestamp completo para metadado de ingestão
TIMESTAMP_STR = agora_utc.strftime("%Y-%m-%d %H:%M:%S")

# Monta a chave final do objeto no S3 com partição diária
S3_KEY = f"{PREFIX}/dt={DATE_STR}/fundamentus_fii.parquet"


# Função para converter valores textuais do site em números
def limpar_numero(valor):
    # Se for nulo, retorna None
    if pd.isna(valor):
        return None

    # Garante que o valor é string e remove espaços
    valor = str(valor).strip()

    # Remove separador de milhar ".", troca vírgula por ponto e remove "%"
    valor = valor.replace(".", "").replace(",", ".").replace("%", "")

    # Tenta converter para número; se não conseguir, retorna NaN
    return pd.to_numeric(valor, errors="coerce")


def main():
    # Log inicial
    print("Extraindo dados do Fundamentus...")

    # URL da página do Fundamentus que contém a tabela dos FIIs
    url = "https://www.fundamentus.com.br/fii_resultado.php"

    # Cabeçalhos HTTP para simular um navegador
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
    }

    # Faz a requisição da página
    response = requests.get(url, headers=headers)

    # Gera exceção caso a resposta venha com erro HTTP
    response.raise_for_status()

    # Faz o parse do HTML retornado
    soup = BeautifulSoup(response.text, "html.parser")

    # Busca a tabela principal pelo id
    tabela = soup.find("table", {"id": "tabelaResultado"})

    # Se não encontrar a tabela, interrompe o processo
    if tabela is None:
        raise Exception("Tabela 'tabelaResultado' não encontrada na página.")

    # Busca todas as linhas da tabela
    linhas = tabela.find_all("tr")

    # Lista que armazenará todas as linhas extraídas
    dados = []

    # Percorre as linhas, ignorando a primeira (cabeçalho)
    for linha in linhas[1:]:
        # Busca todas as colunas td da linha
        colunas = linha.find_all("td")

        # Só processa se a linha realmente tiver colunas
        if len(colunas) > 0:
            valores = []

            # Percorre cada coluna da linha
            for col in colunas:
                # Ignora colunas com classe "endereco"
                if "endereco" not in col.get("class", []):
                    # Adiciona o texto limpo da coluna
                    valores.append(col.text.strip())

            # Adiciona a linha processada à lista final
            dados.append(valores)

    # Define os nomes das colunas do DataFrame
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

    # Cria o DataFrame com os dados extraídos
    df = pd.DataFrame(dados, columns=colunas_df)

    # Exibe a quantidade de linhas extraídas
    print(f"Linhas extraídas: {len(df)}")

    # Lista das colunas que devem ser convertidas para tipo numérico
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

    # Aplica a função de limpeza/conversão em cada coluna numérica
    for col in colunas_numericas:
        df[col] = df[col].apply(limpar_numero)

    # Adiciona a data da ingestão como metadado
    df["data_ingestao"] = DATE_STR

    # Adiciona o timestamp da ingestão como metadado
    df["timestamp_ingestao"] = TIMESTAMP_STR

    # Adiciona a fonte dos dados
    df["fonte"] = "fundamentus"

    # Log de gravação local
    print("Gravando Parquet localmente...")

    # Converte o DataFrame pandas para tabela pyarrow
    table = pa.Table.from_pandas(df)

    # Grava o arquivo parquet local com compressão SNAPPY
    pq.write_table(table, LOCAL_FILE, compression="SNAPPY")

    # Log de upload para o S3
    print(f"Enviando {LOCAL_FILE} para s3://{BUCKET}/{S3_KEY} ...")

    # Cria o cliente S3 autenticado
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    try:
        # Faz o upload do arquivo local para o bucket e chave especificados
        s3.upload_file(LOCAL_FILE, BUCKET, S3_KEY)

        # Logs de sucesso
        print("Upload concluído com sucesso.")
        print(f"Arquivo salvo em: s3://{BUCKET}/{S3_KEY}")

    except Exception as e:
        # Exibe eventual erro no upload
        print("Erro no upload:", e)
        raise

    finally:
        # Remove o arquivo local temporário, se existir
        if os.path.exists(LOCAL_FILE):
            os.remove(LOCAL_FILE)


# Garante que a função principal seja executada apenas quando
# o arquivo for rodado diretamente
if __name__ == "__main__":
    main()
