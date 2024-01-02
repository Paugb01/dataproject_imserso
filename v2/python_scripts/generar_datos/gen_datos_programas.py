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
    plazas_por_destino = {'Andalucía': 164589, 'Aragón': 12414, 'Canarias': 89100, 'Baleares': 141389, 'Asturias': 12414, 'Cantabria': 12414,
                          'Castilla y León': 12414, 'Castilla-La Mancha': 12414, 'Cataluña': 106322, 'Comunidad Valenciana': 155099,
                          'Extremadura': 12414, 'Galicia': 12414, 'Madrid': 12414, 'Comunidad de Murcia': 21877,
                          'Región de Navarra': 12414, 'Comunidad Foral de País Vasco': 12414, 'La Rioja': 12414, 'Ceuta': 550, 'Melilla': 550}

    for j in range(1, cantidad + 1):
        tipo_turismo = random.choice(['costa insular', 'costa peninsular', 'interior'])
        
        if tipo_turismo == 'interior':
            destinos = ['Andalucía', 'Aragón', 'Canarias', 'Baleares', 'Asturias', 'Cantabria', 'Castilla y León', 'Castilla-La Mancha', 'Cataluña', 'Comunitat Valenciana', 'Extremadura', 'Galicia', 'Madrid', 'Comunidad de Murcia', 'Región de Navarra', 'Comunidad Foral de País Vasco', 'La Rioja', 'Ceuta', 'Melilla']
        elif tipo_turismo == 'costa peninsular':
            destinos = ['Andalucía', 'Cataluña', 'Comunidad Valenciana', 'Comunidad de Murcia']
        elif tipo_turismo == 'costa insular':
            destinos = ['Baleares', 'Canarias']
        else:
            destinos = []
        
        destino = random.choice(destinos)

     # Verificar si hay plazas disponibles para el destino
        while plazas_por_destino[destino] > 0:
            # Asegurarse de no asignar más plazas de las disponibles
            plazas_disponibles = min(plazas_por_destino[destino],100)  # Máximo 10 plazas por vez
            plazas = min(random.randint(1, plazas_disponibles), plazas_disponibles)
            plazas_por_destino[destino] -= plazas  # Actualizar el contador de plazas para el destino

        nombre_programa = f'{tipo_turismo.capitalize()} - {destino.capitalize()}'
        origen = faker.city()

        if tipo_turismo == 'costa peninsular' or tipo_turismo == 'costa insular':
            fecha_salida = faker.date_between_dates(
                date_start=datetime.date(2023, 11, 1),
                date_end=datetime.date(2024, 6, 30)
            )
    
            # Randomly choose between 8 and 10 days for the duration of the trip
            trip_duration = random.choice([8, 10])
            
            # Calculate fecha_vuelta based on fecha_salida and trip_duration
            fecha_vuelta = fecha_salida + datetime.timedelta(days=trip_duration)
        
        elif tipo_turismo == 'interior':
            fecha_salida = faker.date_between_dates(
                date_start=datetime.date(2023, 11, 1),
                date_end=datetime.date(2024, 6, 30)
            )
    
            # Randomly choose between 8 and 10 days for the duration of the trip
            trip_duration = random.choice([4, 5, 6])
            
            # Calculate fecha_vuelta based on fecha_salida and trip_duration
            fecha_vuelta = fecha_salida + datetime.timedelta(days=trip_duration)


        programas.append([j, nombre_programa,tipo_turismo, plazas, origen, destino.capitalize(), fecha_salida, fecha_vuelta])

    return programas
    
# Generate fake data for programas
cantidad_programas = 25  # Specify the number of rows you want
programas = generar_datos_fake_programas(cantidad_programas)

# DATAFRAME PRINCIPAL
df_programas = pd.DataFrame(programas, columns=['programa_id','nombre_programa','tipo_turismo', 'plazas', 'origen', 'destino', 'fecha_salida', 'fecha_vuelta'])
print(df_programas)


# CONEXIÓN BBDD
conn = psycopg2.connect(
    database="postgres",
    user='postgres',
    password="Welcome01",
    host='localhost',
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
