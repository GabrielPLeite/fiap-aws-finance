import pandas as pd
import requests
from bs4 import BeautifulSoup

url = "https://www.fundamentus.com.br/fii_resultado.php"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
}

response = requests.get(url, headers=headers)


soup = BeautifulSoup(response.text, "lxml")

tabela = soup.find("table", {"id": "tabelaResultado"})
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

colunas = [
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

df = pd.DataFrame(dados, columns=colunas)

print(df.head())

df.to_csv("fundamentus_fii.csv", index=False)