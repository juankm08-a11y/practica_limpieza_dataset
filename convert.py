import pandas as pd 

archivo_excel = "Online_Retail.xlsx"

df = pd.read_excel(archivo_excel)

print("---Datos cargados desde excel---")
print(df.head())

archivo_csv = "online_retail_sample.csv"
df.to_csv(archivo_csv,index=False,encoding="utf-8")

print(f"\n Archivo convertido y guardado como: {archivo_csv}")