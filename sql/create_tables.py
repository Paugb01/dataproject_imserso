import psycopg2
from psycopg2 import sql

# Parámetros de conexión
conection_parameters = {
    'dbname': 'postgres', 
    'user': 'paugarciabardisa',
    'password': 'Welcome01',
    'host': 'localhost',
    'port': '5432',
}

# Ruta al script SQL
script_sql_path = './imserso.sql'

try:
    # Establecer conexión a la base de datos
    conection = psycopg2.connect(**conection_parameters)

    # Crear un cursor
    cursor = conection.cursor()

    # Leer el contenido del script SQL
    with open(script_sql_path, 'r') as file:
        script_sql = file.read()

    # Ejecutar el script SQL
    cursor.execute(script_sql)

    # Confirmar los cambios en la base de datos
    conection.commit()

    print("Script SQL ejecutado con éxito.")

except psycopg2.Error as e:
    print(f"Error al ejecutar el script SQL: {e}")

finally:
    # Cerrar el cursor y la conexión
    if cursor:
        cursor.close()
    if conection:
        conection.close()
