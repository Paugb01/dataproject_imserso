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

    # Print the DataFrame
    #print(df_usuarios_from_db)

except psycopg2.Error as e:
    print("Error executing SQL query:", e)

finally:
    # Close the database connection
    conn.close()

#We add the column 'puntos' to the dataframe:
df_usuarios_from_db['puntos'] = 0

#Function to evaluate the age of each of the users in usuarios:
def puntuar_edad(row):
    if row['edad'] >= 78:
        return row['puntos'] + 20
    elif row['edad'] < 60:
        return row['puntos'] + 1
    elif row['edad'] == 60:
        return row['puntos'] + 2
    elif row['edad'] > 60:
        return row['puntos'] + 2 + (78 - row['edad'])
    
#Pass puntuar_edad function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_edad, axis = 1)

#print (df_usuarios_from_db)

#Function to evaluate the disability of each of the users in usuarios:
def puntuar_discapacidad(row):
    if row['tipo_discapacidad'] == 2:
        return row['puntos'] + 20
    elif row['tipo_discapacidad'] == 1:
        return row['puntos'] + 10
    else:
        return row['puntos'] + 0
    
#Pass puntuar_edad function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_discapacidad, axis = 1)

#Function to evaluate the income of each of the users in usuarios:
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
    elif 2100>= row['renta'] > 1950:
        return row['puntos'] + 5
    else:
        return row['puntos'] + 0
    
#Pass puntuar_income function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_renta, axis = 1)

print(df_usuarios_from_db)

#Function to evaluate the type of family of each of the users in usuarios:
def puntuar_familia(row):
    if row['tipo_familia'] == 2:
        return row['puntos'] + 10
    elif row['tipo_familia'] == 1:
        return row['puntos'] + 5
    else:
        return row['puntos'] + 0
    
#Pass puntuar_familia function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_familia, axis = 1)


#with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#    print(df_usuarios_from_db)
#df_usuarios_from_db.info()
print(df_usuarios_from_db)


#Now we generate petitions

# Database connection URI
db_uri = 'postgresql+psycopg2://postgres:Welcome01@localhost:5432/postgres'

try:
    # Connect to the database
    engine = create_engine(db_uri)
    
    # Query to retrieve data from the programas table
    programas_query = "SELECT programa_id FROM programas"
    df_programas_from_db = pd.read_sql_query(programas_query, engine)

    # Create the 'solicitudes_df' DataFrame
    solicitudes_df = pd.DataFrame(columns=['solicitud_id', 'usuario_id', 'programa_id', 'puntuacion'])

    # Generate 500 unique combinations of usuario_id and programa_id
    for i in range(500):
        usuario_id = np.random.choice(df_usuarios_from_db['usuario_id'])
        puntos_usuario = df_usuarios_from_db.loc[df_usuarios_from_db['usuario_id'] == usuario_id, 'puntos'].values[0]

        # Generate unique programa_id for each usuario_id
        programa_id_options = np.setdiff1d(df_programas_from_db['programa_id'].values, solicitudes_df.loc[solicitudes_df['usuario_id'] == usuario_id, 'programa_id'].values)
        
        # Check if programa_id_options is empty
        if len(programa_id_options) == 0:
            continue

        programa_id = np.random.choice(programa_id_options)

        solicitudes_df = pd.concat([solicitudes_df, pd.DataFrame({
            'solicitud_id': [i + 1],
            'usuario_id': [usuario_id],
            'programa_id': [programa_id],
            'puntuacion': [puntos_usuario]
        })], ignore_index=True)

    # Print the 'solicitudes_df' DataFrame for verification
    print(solicitudes_df) 

    # Insert the 'solicitudes_df' DataFrame into the 'solicitudes' table in the database
    solicitudes_df.to_sql('solicitudes', engine, index=False, if_exists='append', method='multi', chunksize=1000)

except Exception as e:
    print("Error executing SQL query:", e)

finally:
    # Close the database connection
    engine.dispose()

