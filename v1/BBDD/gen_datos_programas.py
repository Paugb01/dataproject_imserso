import random
from faker import Faker
import pandas as pd
import datetime
import psycopg2

# CREACIÓN DATOS


def generar_datos_fake_programas(cantidad):
    faker = Faker(['es-ES'])
    Faker.seed(1000)
    random.seed(1000)

    programas = []

    for j in range(1, cantidad + 1):
        tipo_turismo = random.choice(['costa insular', 'costa peninsular', 'interior'])
        
        if tipo_turismo == 'interior':
            destinos = ['Andalucía', 'Aragón', 'Canarias', 'Baleares', 'Asturias', 'Cantabria', 'Castilla y León', 'Castilla-La Mancha', 'Cataluña', 'Comunitat Valenciana', 'Extremadura', 'Galicia', 'Madrid', 'Comunidad de Murcia', 'Región de Navarra', 'Comunidad Foral de País Vasco', 'La Rioja', 'Ceuta', 'Melilla']
        elif tipo_turismo == 'costa peninsular':
            destinos = ['Andalucía', 'Cataluña', 'Comunidad Valenciana', 'Murcia']
        elif tipo_turismo == 'costa insular':
            destinos = ['Baleares', 'Canarias']
        else:
            destinos = []
        
        destino = random.choice(destinos)
        nombre_programa = f'{faker.city()} - {destino.capitalize()}'
        plazas = random.randint(20, 50)
        origen = faker.city()

        if tipo_turismo == 'interior':
            fecha_salida = faker.date_between_dates(
                date_start=datetime.date(2023, 12, 1),
                date_end=datetime.date(2024, 2, 29)
            )
            fecha_vuelta = faker.date_between_dates(
                date_start=fecha_salida,
                date_end=datetime.date(2024, 2, 29)
            )
        elif tipo_turismo == 'costa peninsular':
            fecha_salida = faker.date_between_dates(
                date_start=datetime.date(2023, 12, 1),
                date_end=datetime.date(2024, 2, 29)
            )
            fecha_vuelta = faker.date_between_dates(
                date_start=fecha_salida,
                date_end=datetime.date(2024, 2, 29)
            )
        elif tipo_turismo == 'costa insular':
            fecha_salida = faker.date_between_dates(
                date_start=datetime.date(2023, 12, 1),
                date_end=datetime.date(2024, 2, 29)
            )
            fecha_vuelta = faker.date_between_dates(
                date_start=fecha_salida,
                date_end=datetime.date(2024, 2, 29)
            )

        programas.append([j, nombre_programa, plazas, origen, destino.capitalize(), fecha_salida, fecha_vuelta])

    return programas

def generar_destino(tipo_turismo):
    if tipo_turismo == 'interior':
        destinos = ['Andalucía', 'Aragón', 'Canarias', 'Baleares', 'Asturias', 'Cantabria', 'Castilla y León', 'Castilla-La Mancha', 'Cataluña', 'Comunitat Valenciana', 'Extremadura', 'Galicia', 'Madrid', 'Comunidad de Murcia', 'Región de Navarra', 'Comunidad Foral de País Vasco', 'La Rioja', 'Ceuta', 'Melilla']
    elif tipo_turismo == 'costa peninsular':
        destinos = ['Andalucía', 'Cataluña', 'Comunidad Valenciana', 'Murcia']
    elif tipo_turismo == 'costa insular':
        destinos = ['Baleares', 'Canarias']
    else:
        destinos = []
    


# Generate fake data for programas
cantidad_programas = 10  # Specify the number of rows you want
programas = generar_datos_fake_programas(cantidad_programas)

# DATAFRAME PRINCIPAL
df_programas = pd.DataFrame(programas, columns=['programa_id', 'nombre_programa', 'plazas', 'origen', 'destino', 'fecha_salida', 'fecha_vuelta'])
print(df_programas)


# CONEXIÓN BBDD
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

    for index, row in df_programas.iterrows():
        cursor.execute(
            """
            INSERT INTO programas (programa_id, nombre_programa, plazas, origen, destino, fecha_salida, fecha_vuelta)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (row['programa_id'], row['nombre_programa'], row['plazas'], row['origen'], row['destino'], row['fecha_salida'], row['fecha_vuelta'])
        )
    conn.commit()
    print("Inserción exitosa en la base de datos.")

except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)

conn.close()
