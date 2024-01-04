import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

# Database connection parameters
db_params = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Welcome01',
    'host': 'localhost',
    'port': '5432'
}

# Connect to the database
conn = psycopg2.connect(**db_params)

try:
    # Query to retrieve data from the usuarios table
    query = "SELECT * FROM usuarios"

    # Read data into a Pandas DataFrame
    df_usuarios_from_db = pd.read_sql_query(query, conn)

except psycopg2.Error as e:
    print("Error executing SQL query:", e)

finally:
    # Close the database connection
    conn.close()

# We add the columns 'puntos', 'acompanante', 'acompanante_edad' and 'acompanante_renta' to the dataframe:
df_usuarios_from_db['puntos'] = 0

# ACOMPAÑANTE
# We define an acomp_factor to randomly assign True or False to a % of the entries:
acomp_factor = 0.25
df_usuarios_from_db['acompanante'] = np.random.choice([True, False], size=len(df_usuarios_from_db), p=[acomp_factor, 1 - acomp_factor])

df_usuarios_from_db['acompanante_edad'] = 0
# Randomly assign values between 18 and 110 to the 'acompanante_edad' column where 'acompanante' is True
df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'acompanante_edad'] = np.random.randint(18, 111, sum(df_usuarios_from_db['acompanante']))
# Replace NaN values with 0 in 'acompanante_edad'
df_usuarios_from_db['acompanante_edad'].fillna(0, inplace=True)
# We cast the entire column to int
df_usuarios_from_db['acompanante_edad'] = df_usuarios_from_db['acompanante_edad'].astype(int)

# Randomly assign values between 200 and 3000 to 'acompanante_renta' where 'acompanante' is True
df_usuarios_from_db['acompanante_renta'] = np.where(
    df_usuarios_from_db['acompanante'],
    np.random.randint(200, 3001, len(df_usuarios_from_db)),
    0
)

# Recalculate the column renta for entries where 'acompanante' = True and cast into int
df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'renta'] = ((
    df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'renta'] +
    df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'acompanante_renta']
) / 1.33).astype(int)

# Recalculate the column edad for entries where 'acompanante' = True and cast into int
df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'edad'] = ((
    df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'edad'] +
    df_usuarios_from_db.loc[df_usuarios_from_db['acompanante'], 'acompanante_edad']
) / 2).astype(int)

# Cast the 'edad' column to a compatible dtype
df_usuarios_from_db['edad'] = df_usuarios_from_db['edad'].astype(int)

# Function to evaluate the age of each of the users in usuarios:
def puntuar_edad(row):
    if row['edad'] >= 78:
        return row['puntos'] + 20
    elif row['edad'] < 60:
        return row['puntos'] + 1
    elif row['edad'] == 60:
        return row['puntos'] + 2
    elif row['edad'] > 60:
        return row['puntos'] + 2 + (78 - row['edad'])

# Pass puntuar_edad function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_edad, axis=1)

# Function to evaluate the disability of each of the users in usuarios:
def puntuar_discapacidad(row):
    if row['tipo_discapacidad'] == 2:
        return row['puntos'] + 20
    elif row['tipo_discapacidad'] == 1:
        return row['puntos'] + 10
    else:
        return row['puntos'] + 0

# Pass puntuar_discapacidad function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_discapacidad, axis=1)

# Function to evaluate the income of each of the users in usuarios:
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

# Pass puntuar_income function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_renta, axis=1)

# Function to evaluate the health status of users in usuarios:
def puntuar_condicion_medica(row):
    if row['condicion_medica'] == True:
        return row['puntos'] + 20
    else:
        return row['puntos'] + 0

# Pass puntuar_condicion_medica function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_condicion_medica, axis=1)

# Function to evaluate assess widowhood of users in usuarios:
def puntuar_viudez(row):
    if row['viudedad'] == 1:
        return row['puntos'] + 10
    else:
        return row['puntos'] + 0

# Pass puntuar_viudez function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_viudez, axis=1)

# Function to evaluate participation in previous years of users in usuarios:
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

# Pass puntuar_participacio_previa function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_participacion_previa, axis=1)

# Function to evaluate the type of family of each of the users in usuarios:
def puntuar_familia(row):
    if row['tipo_familia'] == 2:
        return row['puntos'] + 10
    elif row['tipo_familia'] == 1:
        return row['puntos'] + 5
    else:
        return row['puntos'] + 0

# Pass puntuar_familia function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_familia, axis=1)

# Function to evaluate the criminal record of the users in usuarios:
def puntuar_antecedentes(row):
    if row['antecedentes'] == 0:
        return row['puntos']
    elif row['antecedentes'] == 1:
        return row['puntos'] - 5
    else:
        return row['puntos'] - 10

# Pass puntuar_antecedentes function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_antecedentes, axis=1)

# with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#     print(df_usuarios_from_db)
# df_usuarios_from_db.info()
print(df_usuarios_from_db)

# Now we generate petitions

# Database connection URI
db_uri = 'postgresql+psycopg2://postgres:Welcome01@localhost:5432/postgres'

try:
    # Connect to the database
    engine = create_engine(db_uri)

    # Query to retrieve data from the programas table
    programas_query = "SELECT programa_id FROM programas"
    df_programas_from_db = pd.read_sql_query(programas_query, engine)

    solicitudes_df = pd.DataFrame(columns=['solicitud_id', 'usuario_id', 'programa_id', 'acompanante', 'acompanante_renta', 'acompanante_edad', 'puntuacion', 'prioridad'])

    # Set data types for each column
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

    # Dictionary to store the count of combinations for each usuario_id-programa_id pair
    combination_count = {}

    # Generate n unique combinations of usuario_id and programa_id
    n = 30000
    for i in range(n):
        usuario_id = np.random.choice(df_usuarios_from_db['usuario_id'])
        puntos_usuario = df_usuarios_from_db.loc[df_usuarios_from_db['usuario_id'] == usuario_id, 'puntos'].values[0]

        # Generate unique programa_id for each usuario_id
        programa_id_options = np.setdiff1d(df_programas_from_db['programa_id'].values,
                                           solicitudes_df.loc[solicitudes_df['usuario_id'] == usuario_id, 'programa_id'].values)

        # Check if programa_id_options is empty
        if len(programa_id_options) == 0:
            continue

        programa_id = np.random.choice(programa_id_options)

        # Check if the combination has been encountered before for this usuario_id
        key = usuario_id
        if key not in combination_count:
            combination_count[key] = 1
        else:
            combination_count[key] += 1

        # Assign the count as the 'prioridad' value
        prioridad = combination_count[key]

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

    # Cast 'puntuacion' to int
    solicitudes_df['puntuacion'] = solicitudes_df['puntuacion'].astype(int)
    # Print the 'solicitudes_df' DataFrame for verification
    solicitudes_df['puntuacion'] = solicitudes_df['puntuacion'].astype(int)
    print(solicitudes_df)

    # Insert the 'solicitudes_df' DataFrame into the 'solicitudes' table in the database
    solicitudes_df.to_sql('solicitudes', engine, index=False, if_exists='replace', method='multi', chunksize=1000)

except Exception as e:
    print("Error executing SQL query:", e)

finally:
    # Close the database connection
    engine.dispose()

# SQL queries to set primary keys and foreign keys
# Connect to PostgreSQL database
connection = psycopg2.connect(**db_params)

cursor = connection.cursor()
alter_query_solicitudes = """
    ALTER TABLE solicitudes
    ADD PRIMARY KEY (solicitud_id),
    ADD FOREIGN KEY (usuario_id) REFERENCES usuarios(usuario_id),
    ADD FOREIGN KEY (programa_id) REFERENCES programas(programa_id);
"""

# Execute SQL queries
cursor.execute(alter_query_solicitudes)

# Commit and close the connection
connection.commit()
connection.close()
