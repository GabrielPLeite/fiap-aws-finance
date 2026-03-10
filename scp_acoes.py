import pandas as pd
import requests
from bs4 import BeautifulSoup

url = "https://www.fundamentus.com.br/resultado.php"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
}

response = requests.get(url, headers=headers)

soup = BeautifulSoup(response.text, "lxml")

# encontra a tabela principal
tabela = soup.find("table", {"id": "resultado"})

linhas = tabela.find_all("tr")

dados = []

for linha in linhas[1:]:
    colunas = linha.find_all("td")
    
    if len(colunas) > 0:
        dados.append([col.text.strip() for col in colunas])

# nomes das colunas (copiados da tabela)
colunas = [
    "Papel","Cotacao","PL","PVP","PSR","DY","PAtivo","PCapGiro",
    "PEBIT","PAtivCircLiq","EVEBIT","EVEBITDA","MargEBIT",
    "MargLiq","LiqCorr","ROIC","ROE","Liq2meses","PatrimLiquido",
    "DivBrutaPatrim","CrescRec5a"
]

df = pd.DataFrame(dados, columns=colunas)

print(df.head())

# salvar
df.to_csv("fundamentus.csv", index=False)