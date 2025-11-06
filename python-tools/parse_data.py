import pandas as pd


symbols = []

df = pd.read_csv('profitable_symbols.csv')
print(df['symbol'].head(37).tolist())