import requests
import json
import pandas as pd


token = 'sk_a6903d094d3f49c5b1c95e4e4185811f'

api_url = f'https://cloud.iexapis.com/stable/ref-data/crypto/symbols/?token={token}'

data = requests.get(api_url).json()

df = pd.DataFrame.from_records(data)

df.to_csv("iexsymbols.csv", encoding='utf-8', index=False)

print(type(data))