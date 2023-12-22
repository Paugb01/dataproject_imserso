import random
from faker import Faker
import unicodedata
import pandas as pd
import datetime
import numpy as np
import psycopg2

#CREACIÓN DATOS


def generar_datos_fake(cantidad):
    faker = Faker(['es-ES'])
    Faker.seed(1000)
    random.seed(1000)
    np.random.seed(1000)
    jubilados = []
    for j in range(cantidad):
        nombre = unicodedata.normalize('NFKD', faker.first_name()).encode('ascii', 'ignore').decode('utf-8')
        apellido = unicodedata.normalize('NFKD', faker.last_name()).encode('ascii', 'ignore').decode('utf-8')
        nif = j

        jubilados.append([nombre,apellido,nif]) 
    return jubilados 

jubilados = generar_datos_fake(100) 


#DATAFRAME PRINCIPAL
df = pd.DataFrame(jubilados,columns =['nombre','apellido','nif'])
print(df)


#DATADRAMEs SECUNDARIOS
df_usuarios = df[['nombre', 'apellido', 'nif']]

 #CONEXIÓN BBDD
conn = psycopg2.connect(
    database="postgres", 
    user='postgres',
    password="E&uoj)~'8z-'!r}&", 
    host='localhost', 
    port= '5432'
)

try:
    cursor = conn.cursor()
    print("Conexión exitosa a la base de datos.")

    for index, row in df_usuarios.iterrows():
        cursor.execute(
            """
            INSERT INTO usuarios (usuario_id,nombre,apellidos)
            VALUES (%s, %s, %s)
            """,
            (row['nif'],row['nombre'], row['apellido'])  
        )
    conn.commit()
    print("Inserción exitosa en la base de datos.")

except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)

conn.close()
