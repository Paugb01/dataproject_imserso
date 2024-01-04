'''
gen_datos_programas.py: Este script genera un número de programas de viaje en función de las plazas disponibles
en los distintos destinos, con un máximo de plazas por programa y fechas de salida y vuelta. Dos programas pueden
tener el mismo origen y destino, pero las fechas cambiarán (salida, llegada o ambas), y tendrán un programa_id
diferente
'''
import random
from faker import Faker
import pandas as pd
import datetime
import psycopg2

# Función para generar los datos

def generar_datos_fake_programas():
    faker = Faker(['es-ES'])
    Faker.seed(1000)
    random.seed(1000)

    programas = []
    # Hemos tomado los datos de la distribución real de plazas por región y escalado 1:50
    plazas_por_destino = {'Andalucía': 3290, 'Aragón': 248, 'Canarias': 1782, 'Baleares': 2826, 'Asturias': 248, 'Cantabria': 248,
                          'Castilla y León': 248, 'Castilla-La Mancha': 248, 'Cataluña': 2126, 'Comunitat Valenciana': 3100,
                          'Extremadura': 248, 'Galicia': 248, 'Madrid': 248, 'Región de Murcia': 436,
                          'Comunidad Foral de Navarra': 248, 'País Vasco': 248, 'La Rioja': 248, 'Ceuta': 10, 'Melilla': 10}

    # Distribuimos ciudades por región/tipo de programa:
    while any(plazas > 0 for plazas in plazas_por_destino.values()):
        tipo_turismo = random.choice(['costa insular', 'costa peninsular', 'interior'])
        
        if tipo_turismo == 'interior':
            destinos = ['Andalucía', 'Aragón', 'Canarias', 'Baleares', 'Asturias', 'Cantabria', 'Castilla y León', 'Castilla-La Mancha', 'Cataluña', 'Comunitat Valenciana', 'Extremadura', 'Galicia', 'Madrid', 'Región de Murcia', 'Comunidad Foral de Navarra', 'País Vasco', 'La Rioja', 'Ceuta', 'Melilla']
        elif tipo_turismo == 'costa peninsular':
            destinos = ['Andalucía', 'Cataluña', 'Comunitat Valenciana', 'Región de Murcia']
        elif tipo_turismo == 'costa insular':
            destinos = ['Baleares', 'Canarias']
        else:
            destinos = []
        
        destino = random.choice(destinos)

        # Verificar si hay plazas disponibles para el destino
        if plazas_por_destino[destino] > 0:
            # Asegurarse de no asignar más plazas de las disponibles
            plazas_asignadas = min(plazas_por_destino[destino], 10)  # Máximo 10 plazas por vez
            plazas_por_destino[destino] -= plazas_asignadas  # Actualizar el contador de plazas para el destino

            nombre_programa = f'{tipo_turismo.capitalize()} - {destino.capitalize()}'
            origen = faker.city()

            if tipo_turismo == 'costa peninsular' or tipo_turismo == 'costa insular':
                fecha_salida = faker.date_between_dates(
                    date_start=datetime.date(2023, 11, 1),
                    date_end=datetime.date(2024, 6, 30)
                )
                # Aleatoriamente crea programas de 8 o 10 días de duración
                trip_duration = random.choice([8, 10])
                # Calcula fecha_vuelta basado en fecha_salida y trip_duration
                fecha_vuelta = fecha_salida + datetime.timedelta(days=trip_duration)
            elif tipo_turismo == 'interior':
                fecha_salida = faker.date_between_dates(
                    date_start=datetime.date(2023, 11, 1),
                    date_end=datetime.date(2024, 6, 30)
                )
                # Aleatoriamente crea programas de 8 o 10 días de duración
                trip_duration = random.choice([4, 5, 6])
                # Calcula fecha_vuelta basado en fecha_salida y trip_duration
                fecha_vuelta = fecha_salida + datetime.timedelta(days=trip_duration)

            programas.append([nombre_programa, tipo_turismo, plazas_asignadas, origen, destino.capitalize(), fecha_salida, fecha_vuelta])

    # Convertimos a Dataframe y asignamos cabeceras
    df_programas = pd.DataFrame(programas, columns=['nombre_programa', 'tipo_turismo', 'plazas_asignadas', 'origen', 'destino', 'fecha_salida', 'fecha_vuelta'])

    # Añadimos la columna programa_id como DataFrame index + 1, para luego convertirlo en primary key
    df_programas.insert(0, 'programa_id', range(1, len(df_programas) + 1))

    return df_programas

# Generamos datos
df_programas = generar_datos_fake_programas()

# Comprobamos el DF
print(df_programas)

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

    for index, row in df_programas.iterrows():
        cursor.execute(
            """
            INSERT INTO programas (programa_id, nombre_programa, plazas, origen, destino, fecha_salida, fecha_vuelta)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (row['programa_id'], row['nombre_programa'], row['plazas_asignadas'], row['origen'], row['destino'], row['fecha_salida'], row['fecha_vuelta'])
        )
    conn.commit()
    print("Inserción exitosa en la base de datos.")

except psycopg2.Error as e:
    print("Error al conectar a la base de datos:", e)

conn.close()
