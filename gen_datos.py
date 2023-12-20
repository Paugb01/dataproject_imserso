import random
from faker import Faker
import unicodedata
import pandas as pd
import datetime


def generar_datos_fake():
    faker = Faker(['es-ES'])
    hoy = datetime.date.today()
    Faker.seed(1000)
    jubilados = []

    for j in range(100):
        nombre = unicodedata.normalize('NFKD', faker.first_name()).encode('ascii', 'ignore').decode('utf-8')
        apellido = unicodedata.normalize('NFKD', faker.last_name()).encode('ascii', 'ignore').decode('utf-8')
        nif = faker.nif()
        fecha_nacimiento = faker.date_of_birth(minimum_age=55, maximum_age=110)
        edad = hoy.year - fecha_nacimiento.year - ((hoy.month, hoy.day) < (fecha_nacimiento.month, fecha_nacimiento.day))
        telefono = faker.phone_number()
        email = faker.ascii_email()
        discapacidad = random.choice([True, False])
        renta = random.randint(484,3175)
        enfermedad = random.choice([True, False])
        pa21_22 = random.choice([True, False])
        pa22_23 = random.choice([True, False])
        tipo_fam = random.randint(0,2) #0 no es familia numerosa , 1 general y 2 especial 
        id_solicitante = random.randint(1,100) 
    
        jubilados.append([nombre,apellido,nif,fecha_nacimiento,edad,telefono,email,discapacidad,renta,enfermedad,pa21_22,pa22_23,tipo_fam,id_solicitante])
    
    for i, jubilado in enumerate(jubilados, start=1): #No necesario, para ver datos por consola
        print(f'Jubilado {i}: {jubilado}')
    
    return jubilados 

jubilados = generar_datos_fake() 

df = pd.DataFrame(jubilados,columns =['nombre','apellido','nif','fecha_nacimiento','edad','telefono','email','discapacidad','renta','enfermedad','pa21_22','pa22_23','tipo_fam','id_solicitante'])
print(df)