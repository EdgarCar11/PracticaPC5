#PROBLEMA3
import os
import requests
import zipfile
import pandas as pd
from pymongo import MongoClient

#Descargar el archivo ZIP
url = "https://netsg.cs.sfu.ca/youtubedata/"
file_name = "0333.zip" 

# Descargar el archivo
response = requests.get(url + file_name)
with open(file_name, "wb") as file:
    file.write(response.content)

#Descomprimir el archivo
extract_folder = "youtube_data"
if not os.path.exists(extract_folder):
    os.makedirs(extract_folder)

with zipfile.ZipFile(file_name, 'r') as zip_ref:
    zip_ref.extractall(extract_folder)

# Leer el archivo .tsv con pandas
# Suponiendo que hay archivos .tsv en la carpeta descomprimida
files = [f for f in os.listdir(extract_folder) if f.endswith('.tsv')]
file_path = os.path.join(extract_folder, files[0]) 

# Leer el archivo .tsv 
df = pd.read_csv(file_path, sep='\t')

# 4. Asignar los nombres de columna 
column_names = [
    'VideoID', 'Uploader', 'Age', 'Category', 'Length', 'Views', 'Rate', 'Ratings', 
    'Comments', 'RelatedIDs'
]
df.columns = column_names

# Filtrar las columnas que necesitamos
df_filtered = df[['VideoID', 'Age', 'Category', 'Views', 'Rate']]

# Filtrar por categoría 
df_filtered = df_filtered[df_filtered['Category'].isin(['Music', 'Sports'])]  

# Procesar datos antes de exportar 
df_filtered['Age'] = df_filtered['Age'].astype(int)  

# Conectar con MongoDB y exportar los datos
client = MongoClient('mongodb+srv://carhuaye:L24YbryQ8UqVZbt0@dev.ongthij.mongodb.net/?retryWrites=true&w=majority&appName=Dev') 
db = client['youtube_data_db']
collection = db['videos']

# Convertir el DataFrame a diccionarios y exportar a MongoDB
data_dict = df_filtered.to_dict(orient='records')
collection.insert_many(data_dict)

print(f"Datos exportados a MongoDB en la colección 'videos' de la base de datos 'youtube_data_db'")


