#PROBLEMA2

import pandas as pd

df_winemag = pd.read_csv('./data/winemag-data-130k-v2.csv')

# Ver las primeras filas del DataFrame
print(df_winemag.head())

#Explorar
# Verificar los tipos de datos
print(df_winemag.dtypes)

# Estadísticas descriptivas
print(df_winemag.describe())

# Verificar valores nulos
print(df_winemag.isnull().sum())

# Ver las primeras filas
print(df_winemag.head())

#Ultimas filas
print(df_winemag.tail())

#renombrar 4 columnas
df_winemag = df_winemag.rename(columns={
    'price': 'winemag_price',
    'country': 'winemag_country',
    'points': 'winemag_points',
    'description': 'winemag_description'
})

print(df_winemag.head())

#creando nuevas columnas
import pandas as pd

# Leer el archivo CSV directamente desde la URL
url_paises = "https://gist.githubusercontent.com/kintero/7d1db891401f56256c79/raw/a61f6d0dda82c3f04d2e6e76c3870552ef6cf0c6/paises.csv"
df_paises = pd.read_csv(url_paises)
print(df_paises.head())

#Agregando a mi data
# Unir df_winemag con df_paises para agregar la columna 'continente'
df_winemag = df_winemag.merge(df_paises[['nombre', 'continente']], left_on='winemag_country', right_on='nombre', how='left')

# columna de precio por punto
df_winemag['precio_per_punto'] = df_winemag['winemag_price'] / df_winemag['winemag_points']

# columna para etiquetar vinos baratos/expensivos 
average_price = df_winemag['winemag_price'].mean()
df_winemag['precio_etiqueta'] = df_winemag['winemag_price'].apply(lambda x: 'Caro' if x > average_price else 'Barato')

# Verificar las primeras filas con las nuevas columnas
print(df_winemag.head())





#generamos reportes
#reporte1   VINOS MEJOR PUNTUADOS POR CONTINENTE
# Agrupar por continente y obtener el vino mejor puntuado por continente
df_Mejor_vino_por_continente = df_winemag.groupby('continente').apply(lambda x: x.loc[x['winemag_points'].idxmax()]).reset_index(drop=True)

# Ver el reporte
print(df_Mejor_vino_por_continente)

# Exportar a CSV
df_Mejor_vino_por_continente.to_csv('df_Mejor_vino_por_continente.csv', index=False)




#reporte2 Promedio de precio de vino y cantidad de reviews obtenidos según país y ordenado de mejor a peor
# Agrupar por país, calcular el promedio de precio y la cantidad de puntos, y ordenarlos
df_precio_puntos_por_country = df_winemag.groupby('winemag_country').agg(
    average_price=('winemag_price', 'mean'),
    average_points=('winemag_points', 'mean')
).reset_index()

# Ordenar de mejor a peor (por puntos de vino primero y luego por precio si hay empate)
df_precio_puntos_por_country = df_precio_puntos_por_country.sort_values(by=['average_points', 'average_price'], ascending=[False, False])

# Ver el reporte
print(df_precio_puntos_por_country)

# Exportar a CSV
df_precio_puntos_por_country.to_csv('price_points_by_country.csv', index=False)
df_precio_puntos_por_country.to_excel('df_precio_puntos_por_country.xlsx', index=False, engine='openpyxl')





#reporte3


# Contar cuántos vinos hay de cada variedad
df_wine_variety_count = df_winemag['variety'].value_counts().reset_index()

# Renombrar columnas para mayor claridad
df_wine_variety_count.columns = ['variety', 'count']

# Ver el reporte
print(df_wine_variety_count)

# Exportar a SQLite
import sqlite3

# Crear la conexión a la base de datos SQLite
conn = sqlite3.connect('winemag_reports.db')

# Guardar el reporte en una tabla llamada "wine_variety_count"
df_wine_variety_count.to_sql('wine_variety_count', conn, if_exists='replace', index=False)

# Verificar si la tabla fue creada correctamente
print(pd.read_sql('SELECT * FROM wine_variety_count', conn))

# Cerrar la conexión
conn.close()


#REPORTE4 MDB

from pymongo import MongoClient

# Conectar a MongoDB
client = MongoClient('mongodb+srv://carhuaye:L24YbryQ8UqVZbt0@dev.ongthij.mongodb.net/?retryWrites=true&w=majority&appName=Dev')
db = client['winemag_db']

# Insertar los mejores vinos por puntuación por país en MongoDB
best_wine_by_country = df_winemag.groupby('winemag_country').apply(lambda x: x.loc[x['winemag_points'].idxmax()]).reset_index(drop=True)
db.best_wine_by_country.insert_many(best_wine_by_country.to_dict('records'))

# Verificar que los datos se insertaron correctamente
print(db.best_wine_by_country.find_one())



#ENVIO DE CORREO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


# Configuración del servidor y credenciales
import os 

smtp_server = 'smtp.gmail.com'  # Cambia esto al servidor SMTP que estés utilizando
smtp_port = 587  # Cambia esto al puerto adecuado
sender_email = 'carhuaye@gmail.com'
sender_password = open('token.txt').read().strip() #os.environ['gmail_pass'] #

# Detalles del correo electrónico
receiver_email = 'ec8034292@gmail.com'
subject = 'Reporte de los mejores vinos por continente'
body = 'Adjunto lo solicitado'

# Crear el objeto MIMEMultipart
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = receiver_email
msg['Subject'] = subject
msg.attach(MIMEText(body, 'plain'))


# Adjuntar archivo
file_path = 'df_Mejor_vino_por_continente.csv'  # Cambia la ruta al archivo que quieras adjuntar
with open(file_path, 'rb') as file:
    attachment = MIMEApplication(file.read(), _subtype="csv")
    attachment.add_header('Content-Disposition', 'attachment', filename=file_path)
    msg.attach(attachment)
    
# Iniciar la conexión con el servidor SMTP
with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.starttls()  # Iniciar el modo seguro
    server.login(sender_email, sender_password)
    server.sendmail(sender_email, receiver_email, msg.as_string())

print('Correo enviado exitosamente')






