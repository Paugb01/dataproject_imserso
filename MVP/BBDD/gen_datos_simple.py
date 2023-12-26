import random
from faker import Faker
import unicodedata
import pandas as pd
import datetime
import numpy as np
import psycopg2
import time

# Antes de ejecutar el scrip va a retrasar 15 segundos para que de tiempo que se levante la base de datos "imserso_jmp.sql" y que no hayan errores por ejecutar todo rapido

time.sleep(15)

#CREACIÓN DATOS


def generar_datos_fake(cantidad):
    faker = Faker(['es-ES'])
    hoy = datetime.date(2023,12,22)
    tp_familia = [0,1,2] # Tipos de familias: 0 familias no numerosa, 1 numerosa general 2 numerosas especial
    tipo_discapacidad = [0,1,2] # Tipos discapacidad: 0 no presentan discapacidad o es leve, 1 entre 33<x<49 , 2 x>50
    Faker.seed(1000)
    random.seed(1000)
    np.random.seed(1000)
    jubilados = []
    for j in range(cantidad):
        nombre = unicodedata.normalize('NFKD', faker.first_name()).encode('ascii', 'ignore').decode('utf-8')
        apellido = unicodedata.normalize('NFKD', faker.last_name()).encode('ascii', 'ignore').decode('utf-8')
        nif = faker.nif()
        fecha_nacimiento = faker.date_of_birth(minimum_age=55, maximum_age=110)
        edad = hoy.year - fecha_nacimiento.year - ((hoy.month, hoy.day) < (fecha_nacimiento.month, fecha_nacimiento.day))
        discapacidad = np.random.choice(tipo_discapacidad, p=(0.75, 0.20, 0.05), size=1)[0]
        renta = random.uniform(484.61,3175)
        tipo_familia = np.random.choice(tp_familia, p=(0.75, 0.20, 0.05), size=1)[0] #Selecciona un valor según el tipo de familia
        jubilados.append([nombre,apellido,nif,fecha_nacimiento,edad,discapacidad,renta,tipo_familia]) 
    return jubilados 

jubilados = generar_datos_fake(100) 


#DATAFRAME PRINCIPAL
df = pd.DataFrame(jubilados,columns =['nombre','apellido','nif','fecha_nacimiento','edad','discapacidad','renta','tipo_familia'])
print(df)


#DATADRAMEs SECUNDARIOS
df_usuarios = df[['nombre','apellido','nif','fecha_nacimiento','edad','discapacidad','renta','tipo_familia']]


 #CONEXIÓN BBDD
conn = psycopg2.connect(
    database="postgres", 
    user='postgres',
    password="Welcome01", 
    host='postgres', 
    port= '5432'
)

try:
    cursor = conn.cursor()
    print("Conexión exitosa a la base de datos.")

    for index, row in df_usuarios.iterrows():
        cursor.execute(
            """
            INSERT INTO usuarios (usuario_id,nombre,apellidos, edad, fecha_nacimiento, renta, tipo_discapacidad, tipo_familia)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (row['nif'],row['nombre'], row['apellido'], row['edad'], row['fecha_nacimiento'], row['renta'], row['discapacidad'], row['tipo_familia'])  
        )
    conn.commit()
    print("Inserción exitosa en la base de datos.")

except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)

conn.close()
