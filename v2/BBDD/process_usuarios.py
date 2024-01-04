'''
process_usuarios.py: Este script copia la tabla 'usuarios' de la BD a un dataframe y calcula las puntuaciones en
de cada campo en función del criterio de justicia. Después, genera solicitudes en base al dataframe de usuarios
puntuado y añade parámetros de 'acompanante', con un factor de acompañante (acomp_factor) para solicitudes de pareja.
'''
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

# Conexión a la BD para obtener la tabla 'usuarios'. host: localhost en local, postgres en compose!!!
# Datos de conexión
db_params = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Welcome01',
    'host': 'postgres',
    'port': '5432'
}

# Conexión a la BD
conn = psycopg2.connect(**db_params)

try:
    # Query para obtener la tabla 'usuarios'
    query = "SELECT * FROM usuarios"

    # Pasamos 'usuarios' al df
    df_usuarios_from_db = pd.read_sql_query(query, conn)

except psycopg2.Error as e:
    print("Error executing SQL query:", e)

finally:
    # Cerramos conexión a la BD
    conn.close()

# Añadimos columnas para acompañante 'puntos', 'acompanante', 'acompanante_edad' and 'acompanante_renta' al dataframe:
df_usuarios_from_db['puntos'] = 0

# ACOMPAÑANTE
# Definimos un factor acomp_factor para asignar aleatoriamente True or False a un % de entradas:
acomp_factor = 0.25
df_usuarios_from_db['acompanante'] = np.random.choice([True, False], size=len(df_usuarios_from_db), p=[acomp_factor, 1 - acomp_factor])

df_usuarios_from_db['acompanante_edad'] = 0
# Aleatoriamente asigna valores entre 18 y 110 a 'acompanante_edad' en las entradas donde 'acompanante' es True
df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'acompanante_edad'] = np.random.randint(18, 111, sum(df_usuarios_from_db['acompanante']))
# Reemplazamos los valores NaN con 0 en 'acompanante_edad' para poder pasar la columna como int luego.
df_usuarios_from_db['acompanante_edad'].fillna(0, inplace=True)
# Pasamos los valores a int
df_usuarios_from_db['acompanante_edad'] = df_usuarios_from_db['acompanante_edad'].astype(int)

# Asignamos valores aleatorios entre 200 y 3000 a 'acompanante_renta' donde 'acompanante' es True:
df_usuarios_from_db['acompanante_renta'] = np.where(
    df_usuarios_from_db['acompanante'],
    np.random.randint(200, 3001, len(df_usuarios_from_db)),
    0
)

# Recalculamos la columna 'renta' en las entradas donde 'acompanante' = True y pasamos a int:
df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'renta'] = ((
    df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'renta'] +
    df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'acompanante_renta']
) / 1.33).astype(int)

# Recalculamos la columna 'edad' en las entradas donde 'acompanante' = True y pasamos a int:
df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'edad'] = ((
    df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'edad'] +
    df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'acompanante_edad']
) / 2).astype(int)

# Pasamos 'edad' a un tipo compatible (int) - Esto estaba dando problemas si no lo aplicábamos!!!
df_usuarios_from_db['edad'] = df_usuarios_from_db['edad'].astype(int)

# Función para evaluar (puntuar) la variable 'edad':
def puntuar_edad(row):
    if row['edad'] >= 78:
        return row['puntos'] + 20
    elif row['edad'] < 60:
        return row['puntos'] + 1
    elif row['edad'] == 60:
        return row['puntos'] + 2
    elif row['edad'] > 60:
        return row['puntos'] + 2 + (78 - row['edad'])

# Pasamos la función al df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_edad, axis=1)

# # Función para evaluar (puntuar) la variable 'discapacidad':
def puntuar_discapacidad(row):
    if row['tipo_discapacidad'] == 2:
        return row['puntos'] + 20
    elif row['tipo_discapacidad'] == 1:
        return row['puntos'] + 10
    else:
        return row['puntos'] + 0

# Pasamos la función al df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_discapacidad, axis=1)

# # Función para evaluar (puntuar) la variable 'renta':
def puntuar_renta(row):
    if row['renta'] <= 484.62:
        return row['puntos'] + 50
    elif 900 >= row['renta'] > 484.62:
        return row['puntos'] + 45
    elif 1050 >= row['renta'] > 900:
        return row['puntos'] + 40
    elif 1200 >= row['renta'] > 1050:
        return row['puntos'] + 35
    elif 1350 >= row['renta'] > 1200:
        return row['puntos'] + 30
    elif 1500 >= row['renta'] > 1350:
        return row['puntos'] + 25
    elif 1650 >= row['renta'] > 1500:
        return row['puntos'] + 20
    elif 1800 >= row['renta'] > 1650:
        return row['puntos'] + 15
    elif 1950 >= row['renta'] > 1800:
        return row['puntos'] + 10
    elif 2100 >= row['renta'] > 1950:
        return row['puntos'] + 5
    else:
        return row['puntos'] + 0

# Pasamos la función al df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_renta, axis=1)

# Función para evaluar (puntuar) la variable 'condicion_medica':
def puntuar_condicion_medica(row):
    if row['condicion_medica'] == True:
        return row['puntos'] + 20
    else:
        return row['puntos'] + 0

# Pasamos la función al df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_condicion_medica, axis=1)

# Función para evaluar (puntuar) la variable 'viudedad':
def puntuar_viudez(row):
    if row['viudedad'] == 1:
        return row['puntos'] + 10
    else:
        return row['puntos'] + 0

# Pasamos la función al df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_viudez, axis=1)

# Función para evaluar (puntuar) las variables de participación en años anteriores:
def puntuar_participacion_previa(row):
    if row['participacion21_22'] == False and row['participacion22_23'] == False:
        '''
        This IF ELSE loop assigns points to those who didn't participate in the past two years because
        they 1) didn't/want or couldn't participate OR 2) because they weren't awarded a place.
        '''
        if row['viajes_realizados_22_23'] == 1:
            return row['puntos'] + 100
        else:
            return row['puntos'] + 50
    elif row['participacion21_22'] == True and row['participacion22_23'] == False:
        return row['puntos'] + 40
    elif row['participacion21_22'] == False and row['participacion22_23'] == True:
        return row['puntos'] + 20
    elif row['participacion21_22'] == True and row['participacion22_23'] == True:
        if row['viajes_realizados_21_22'] == 3 or row['viajes_realizados_22_23'] == 3:
            return row['puntos'] + 0
        else:
            return row['puntos'] + 10

# Pasamos la función al df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_participacion_previa, axis=1)

# Función para evaluar (puntuar) la variable 'tipo_familia':
def puntuar_familia(row):
    if row['tipo_familia'] == 2:
        return row['puntos'] + 10
    elif row['tipo_familia'] == 1:
        return row['puntos'] + 5
    else:
        return row['puntos'] + 0

# Pasamos la función al df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_familia, axis=1)

# Función para evaluar (puntuar) la variable 'antecedentes':
def puntuar_antecedentes(row):
    if row['antecedentes'] == 0:
        return row['puntos']
    elif row['antecedentes'] == 1:
        return row['puntos'] - 10
    else:
        return row['puntos'] - 500

# Pasamos la función al df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_antecedentes, axis=1)

# Comprobamos el df
print(df_usuarios_from_db)

# Generamos las peticiones
# Conexión URI a la BD #(localhost@5432 en local, postgres@5432 compose!!!)

db_uri = 'postgresql+psycopg2://postgres:Welcome01@postgres:5432/postgres' 

try:
    # Ejecutamos conexión
    engine = create_engine(db_uri)

    # Query para leer los datos de la tabla 'programas'
    programas_query = "SELECT programa_id FROM programas"
    df_programas_from_db = pd.read_sql_query(programas_query, engine)

    # Pasamos programas a un DF y asignamos cabeceras
    solicitudes_df = pd.DataFrame(columns=['solicitud_id', 'usuario_id', 'programa_id', 'acompanante', 'acompanante_renta', 'acompanante_edad', 'puntuacion', 'prioridad'])

    # Asignamos el tipo de variables en cada columna del df:
    solicitudes_df = solicitudes_df.astype({
        'solicitud_id': int,
        'usuario_id': int,
        'programa_id': int,
        'acompanante': bool,
        'acompanante_renta': int,
        'acompanante_edad': int,
        'puntuacion': int,
        'prioridad': int
    })

    # Diccionario para llevar la cuenta de cada combinación usuario_id-programa_id:
    combination_count = {}

    # Generamos n combinaciones únicas de usuario_id and programa_id
    n = 50000
    for i in range(n):
        usuario_id = np.random.choice(df_usuarios_from_db['usuario_id'])
        puntos_usuario = df_usuarios_from_db.loc[df_usuarios_from_db['usuario_id'] == usuario_id, 'puntos'].values[0]

        # Generamos un programa_id único para cada usuario_id
        programa_id_options = np.setdiff1d(df_programas_from_db['programa_id'].values,
                                           solicitudes_df.loc[solicitudes_df['usuario_id'] == usuario_id, 'programa_id'].values)

        # Comprueba si 'programa_id_options' está vacío para asignar o no id nueva
        if len(programa_id_options) == 0:
            continue

        programa_id = np.random.choice(programa_id_options)

        # Comprubeba si esa combinación para ese 'usuario_id' ya existe:
        key = usuario_id
        if key not in combination_count:
            combination_count[key] = 1
        else:
            combination_count[key] += 1

        # Generamos la columna 'prioridad' y asignamos valor incremental y distinto para cada 'programa_id' asociado a un 'usuario_id'
        # De esta forma podremos ordenar solicitudes por orden de preferencia a la hora de asignar plazas.
        prioridad = combination_count[key]

        # Pasamos los campos a las columnas del df
        solicitudes_df = pd.concat([solicitudes_df, pd.DataFrame({
            'solicitud_id': [i + 1],
            'usuario_id': [usuario_id],
            'programa_id': [programa_id],
            'acompanante': [df_usuarios_from_db.loc[df_usuarios_from_db['usuario_id'] == usuario_id, 'acompanante'].values[0]],
            'acompanante_edad': [df_usuarios_from_db.loc[df_usuarios_from_db['usuario_id'] == usuario_id, 'acompanante_edad'].values[0]],
            'acompanante_renta': [df_usuarios_from_db.loc[df_usuarios_from_db['usuario_id'] == usuario_id, 'acompanante_renta'].values[0]],
            'puntuacion': [puntos_usuario],
            'prioridad': [prioridad]
        })], ignore_index=True)

    # Pasamos 'puntuacion' a int:
    solicitudes_df['puntuacion'] = solicitudes_df['puntuacion'].astype(int)
    # Verificamos 'solicitudes_df':
    solicitudes_df['puntuacion'] = solicitudes_df['puntuacion'].astype(int)
    print(solicitudes_df)

    # Insertamos 'solicitudes_df' en tabla 'solicitudes' en la BD:
    solicitudes_df.to_sql('solicitudes', engine, index=False, if_exists='replace', method='multi', chunksize=1000)

except Exception as e:
    print("Error ejecutando consulta:", e)

finally:
    # Cerramos la conexión
    engine.dispose()

# Ahora ejecutamos queries para establecer primary keys y foreign keys en la tabla 'solicitudes'
# Por alguna razón sólo deja utilizar este método, y no URI - hay que investigar!
connection = psycopg2.connect(**db_params)

cursor = connection.cursor()
# Queries de SQL
alter_query_solicitudes = """
    ALTER TABLE solicitudes
    ADD PRIMARY KEY (solicitud_id),
    ADD FOREIGN KEY (usuario_id) REFERENCES usuarios(usuario_id),
    ADD FOREIGN KEY (programa_id) REFERENCES programas(programa_id);
"""

# Ejecutamos queries:
cursor.execute(alter_query_solicitudes)

# Enviamos y cerramos conexión
connection.commit()
connection.close()
