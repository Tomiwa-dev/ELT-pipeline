#https://docs.delta.io/latest/best-practices.html#compact-files



import pandas as pd

df = pd.read_csv("/Users/emmanuel/Downloads/ecommerce_data.csv", encoding="latin1")


df_slice = df[:300000].copy()

df_slice.to_csv("first_set.csv", index=False)