'''
gen_datos_v2.py: Este script genera un número solicitado de usuarios con distintos campos en un dataframe
para poder manipular y los inserta en la tabla 'usuarios' de la BD
'''
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
    tipo_discapacidad = [0,1,2] # Tipos discapacidad: 0 - No presentan discapacidad o es leve (0% < x < 32%) , 1 - Entre 33% < x < 49% , 2 - Mayor al 50%
    prob_viajes21_22 =[2,3] # Viajes realizados: 2: 1 viaje 3: 2 viajes o mas
    prob_viajes22_23 =[2,3] # Viajes realizados: 2: 1 viaje 3: 2 viajes o mas
    esper_viajes21_22 = [0,1] # 0:No realizo viajes , 1 :Quedo en lista de espera
    espera_viajes22_23 = [0,1] # 0:No realizo viajes , 1 :Quedo en lista de espera
    viud = [0,1] # 0 = no ser viudo, 1 = viudo
    tipo_antecedentes = [0,1,2] # Tipo antecedentes: 0 - No tiene antecedentes  , 1 Leves, 2 Graves
    Faker.seed(1000)
    random.seed(1000)
    np.random.seed(1000)
    jubilados = []

    # Bucle for para generar los campos de cada entrada
    for j in range(cantidad):
        nombre = unicodedata.normalize('NFKD', faker.first_name()).encode('ascii', 'ignore').decode('utf-8')
        apellido = unicodedata.normalize('NFKD', faker.last_name()).encode('ascii', 'ignore').decode('utf-8')
        nif = faker.unique.nif()
        beta_values = np.random.beta(2, 5) # Utilizamos beta para generar variables aleatorias de edad que se ajusten a la distribución real.
        edad = round(beta_values * (110 - 55) + 55) # Distribuye edad
        
        # Forzamos que los usuarios con un rango de edad entre 55 (incluido) y 60 (excluido) sean viudos, para poder usar el Programa de Viajes de IMSERSO:
        if 55 <= edad < 60:
            viudedad = viud[1]
        else:
            viudedad = np.random.choice(viud, p=(0.707, 0.293), size=1)[0]
        
        year_nacimiento = 2023 - round(edad)
        start = datetime.date(year_nacimiento,1,1)
        end = datetime.date(year_nacimiento,12,31)
        fecha_nacimiento = faker.date_between_dates(date_start=start, date_end=end) # Calcula fecha nacimiento 
        discapacidad = np.random.choice(tipo_discapacidad, p=(0.57, 0.36, 0.07), size=1)[0]
        renta = np.random.pareto(3, size=None) * 1400 + 500 
        enfermedad = np.random.choice([True, False], p=(0.083, 0.917), size=1)[0]
        
        # Participación en años anteriores:
        # sea true elige un valor de prob_viajes con una probabilidad 70% , 30% . 
        # 70% para el 2, que representa 1 viajes, y 30% para el 3 que representa 2 viajes o más.
        # Participación año 21-22
        pa21_22 = random.choice([True, False])
        if pa21_22 is True:
            viajes_21_22 = np.random.choice(prob_viajes21_22, p=(0.70, 0.30), size=1)[0]
        else:
            viajes_21_22 = np.random.choice(esper_viajes21_22, p=(0.70, 0.30), size=1)[0]
        # Participación año 22-23
        pa22_23 = random.choice([True, False])
        if pa22_23 is True:
            viajes_22_23 = np.random.choice(prob_viajes22_23, p=(0.70, 0.30), size=1)[0]
        else:
            viajes_22_23 = np.random.choice(espera_viajes22_23, p=(0.70, 0.30), size=1)[0]
        tipo_familia = np.random.choice(tp_familia, p=(0.75, 0.20, 0.05), size=1)[0] #Selecciona un valor según el tipo de familia
        antecedentes = np.random.choice(tipo_antecedentes, p=(0.84, 0.10, 0.06), size=1)[0]
        # Añadimos todos los campos calculados a la lista:
        jubilados.append([nombre,apellido,nif,fecha_nacimiento,edad,discapacidad,renta,enfermedad,viudedad,pa21_22,viajes_21_22,pa22_23,viajes_22_23,tipo_familia,antecedentes]) 
    return jubilados 

jubilados = generar_datos_fake(10000) 


# Dataframe principal: generamos el df a partir de la lista 'jubilados' y asignamos la cabecera
df = pd.DataFrame(jubilados, columns=['nombre', 'apellido', 'nif', 'fecha_nacimiento', 'edad', 'discapacidad', 'renta', 'enfermedad', 'viudedad', 'pa21_22', 'viajes_21_22', 'pa22_23', 'viajes_22_23', 'tipo_familia', 'antecedentes'])
print(df)

# Dataframe secundario: generamos el df a partir del df principal con las columnas que queremos (por ahora todos)
df_usuarios = df[['nombre', 'apellido', 'nif', 'fecha_nacimiento', 'edad', 'discapacidad', 'renta', 'enfermedad', 'viudedad', 'pa21_22', 'viajes_21_22', 'pa22_23', 'viajes_22_23', 'tipo_familia', 'antecedentes']]

# Conectamos a la BD (host: localhost en local, postgres en docker compose!!!)
conn = psycopg2.connect(
    database="postgres",
    user='postgres',
    password="Welcome01",
    host='postgres',
    port='5432'
)

try:
    cursor = conn.cursor()
    print("Conexión exitosa a la base de datos.")

    for index, row in df_usuarios.iterrows():
        cursor.execute(
            """
            INSERT INTO usuarios (usuario_id,nombre,apellidos, edad, fecha_nacimiento, renta, tipo_discapacidad, tipo_familia, condicion_medica, viudedad, antecedentes, participacion21_22, viajes_realizados_21_22, participacion22_23, viajes_realizados_22_23)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (row['nif'], row['nombre'], row['apellido'], row['edad'], row['fecha_nacimiento'], row['renta'], row['discapacidad'], row['tipo_familia'], row['enfermedad'], row['viudedad'], row['antecedentes'], row['pa21_22'], row['viajes_21_22'], row['pa22_23'], row['viajes_22_23'])
        )
    conn.commit()
    print("Inserción exitosa en la base de datos.")

except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)

conn.close()
