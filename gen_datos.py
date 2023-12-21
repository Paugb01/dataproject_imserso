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
    hoy = datetime.date.today()
    Faker.seed(1000)
    random.seed(1000)
    np.random.seed(1000)
    tipo_discapacidad = [0,1,2] # Tipos discapacidad: 0 no presentan discapacidad o es leve, 1 entre 33<x<49 , 2 x>50
    tp_familia = [0,1,2] # Tipos de familias: 0 familias no numerosa, 1 numerosa general 2 numerosas especial
    prob_viajes21_22 =[1,2] # Viajes realizados: 1: 1 viaje 2: 2 viajes o mas
    prob_viajes22_23 =[1,2] # Viajes realizados: 1: 1 viaje 2: 2 viajes o mas
    jubilados = []

    for j in range(cantidad):
        nombre = unicodedata.normalize('NFKD', faker.first_name()).encode('ascii', 'ignore').decode('utf-8')
        apellido = unicodedata.normalize('NFKD', faker.last_name()).encode('ascii', 'ignore').decode('utf-8')
        nif = faker.nif()
        fecha_nacimiento = faker.date_of_birth(minimum_age=55, maximum_age=110)
        edad = hoy.year - fecha_nacimiento.year - ((hoy.month, hoy.day) < (fecha_nacimiento.month, fecha_nacimiento.day))
        telefono = faker.phone_number()
        email = faker.ascii_email()
        discapacidad = np.random.choice(tipo_discapacidad, p=(0.75, 0.20, 0.05), size=1)[0]
        renta = random.randint(484,3175)
        enfermedad = random.choice([True, False])
        pa21_22 = random.choice([True, False]) # Participación año 21-22
        if pa21_22 is True:
            viajes_21_22 = np.random.choice(prob_viajes21_22, p=(0.70, 0.30,), size=1)[0]
        else:
            viajes_21_22 = 0
        pa22_23 = random.choice([True, False]) # Participación año 22-23
        if pa22_23 is True:
            viajes_22_23 = np.random.choice(prob_viajes22_23, p=(0.70, 0.30), size=1)[0]
        else:
            viajes_22_23 = 0
        tipo_familia = np.random.choice(tp_familia, p=(0.70, 0.25, 0.05), size=1)[0] #Selecciona un valor según el tipo de familia
        id_solicitante = random.randint(1,100) 
    
        jubilados.append([nombre,apellido,nif,fecha_nacimiento,edad,telefono,email,discapacidad,renta,enfermedad,pa21_22,viajes_21_22,pa22_23,viajes_22_23,tipo_familia,id_solicitante])
    
    for i, jubilado in enumerate(jubilados, start=1): # No necesario, para ver datos por consola
        print(f'Jubilado {i}: {jubilado}')
    
    return jubilados 

jubilados = generar_datos_fake(1500) 


#DATAFRAME PRINCIPAL
df = pd.DataFrame(jubilados,columns =['nombre','apellido','nif','fecha_nacimiento','edad','telefono','email','discapacidad','renta','enfermedad','pa21_22','viajes_21_22','pa22_23','viajes_22_23','tipo_familia','id_solicitante'])
print(df)


#DATADRAMEs SECUNDARIOS
df_usuarios = df[['id_solicitante''nombre', 'apellido', 'nif', 'fecha_nacimiento', 'edad','telefono','mail']]
df_renta = df[['id_solicitante', 'renta']]
df_familia = df[['id_solicitante', 'tipo_familia']]
df_discapcidad = df[['id_solicitante', 'discapacidad']]
df_enfermedad = df[['id_solicitante','enfermedad']]
df_vta = df[['id_solicitante','pa21_22','viajes_21_22','pa22_23','viajes_22_23']]

'''
#CONEXIÓN BBDD
conn = psycopg2.connect(
    database="postgres", 
    user='postgres',
    password='password', 
    host='127.0.0.1', 
    port= '5432'
)

try:
    connection = psycopg2.connect(**conn)
    cursor = conn.cursor()
    print("Conexión exitosa a la base de datos.")

#FALTA INSERTAR DATOS

except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)

connection.close()'''
