import pandas as pd
from sqlalchemy import create_engine, Integer
import numpy as np
import psycopg2

# Database connection parameters
db_params = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'Welcome01',
    'database': 'postgres',  # Replace with your actual database name
}

# Create a SQLAlchemy engine
db_uri = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
engine = create_engine(db_uri)

# Read 'plazas' from 'programas' table and store it in a dictionary
plazas_dict = pd.read_sql_table('programas', engine, columns=['programa_id', 'plazas']).set_index('programa_id').to_dict()['plazas']

# Initialize DataFrames with additional columns
df_asignado = pd.DataFrame()
df_lista_espera = pd.DataFrame()

# Assign initial values to asignada_id and espera_id
next_asignada_id = 1
next_espera_id = 1

# Penalty factor
penalty_factor = 1

# Fetch solicitudes data from the database
query_solicitudes = """
    SELECT *
    FROM solicitudes
    ORDER BY puntuacion DESC, prioridad ASC;
"""

df_solicitudes = pd.read_sql_query(query_solicitudes, engine)

# Sort df_solicitudes by puntuacion in descending order
df_solicitudes.sort_values(by='puntuacion', ascending=False, inplace=True)

# Iterate through each row of the sorted DataFrame
while not df_solicitudes.empty:
    top_entry = df_solicitudes.iloc[0]
    solicitud_id = top_entry['solicitud_id']
    programa_id = top_entry['programa_id']
    usuario_id = top_entry['usuario_id']

    available_plazas = plazas_dict.get(programa_id, 0)

    if available_plazas > 1:
        # Check if 'acompanante' is True
        is_acompanante = top_entry['acompanante']

        # Subtract 2 from available_plazas if 'acompanante' is True, otherwise subtract 1
        allocation_count = 2 if is_acompanante else 1

        # Allocate to df_asignado with asignada_id column
        for _ in range(allocation_count):
            df_asignado = pd.concat([df_asignado, top_entry.to_frame().T], ignore_index=True)
            df_asignado.loc[df_asignado.index[-1], 'asignada_id'] = next_asignada_id
            df_asignado['asignada_id'] = df_asignado['asignada_id'].astype(int)
            available_plazas -= 1
            plazas_dict[programa_id] = available_plazas  # Reduce available places
            print(f"Solicitud {solicitud_id}: Allocated to df_asignado. Available plazas for programa_id {programa_id}: {available_plazas}")
            next_asignada_id += 1  # Increment asignada_id

        # Apply penalty factor to remaining entries for the same usuario_id
        penalty_mask = (df_solicitudes['usuario_id'] == usuario_id)
        df_solicitudes.loc[penalty_mask, 'puntuacion'] = (df_solicitudes.loc[penalty_mask, 'puntuacion'] * penalty_factor).astype(int)

        # Drop the processed row
        df_solicitudes.drop(df_solicitudes.index[0], inplace=True)

        # Sort df_solicitudes by puntuacion and prioridad
        df_solicitudes.sort_values(by=['puntuacion', 'prioridad'], ascending=[False, True], inplace=True)



    else:
        # Move to df_lista_espera with espera_id column
        top_entry_copy = top_entry.copy()
        top_entry_copy['espera_id'] = next_espera_id
        df_lista_espera = pd.concat([df_lista_espera, top_entry_copy.to_frame().T], ignore_index=True)
        print(f"Solicitud {solicitud_id}: Moved to df_lista_espera. No available plazas for programa_id {programa_id}")
        next_espera_id += 1  # Increment espera_id

        # Drop the processed row
        df_solicitudes.drop(df_solicitudes.index[0], inplace=True)

# Drop unnecessary columns from df_asignado and df_lista_espera
df_asignado.drop(columns=['row_num'], inplace=True, errors='ignore')
df_lista_espera.drop(columns=['row_num'], inplace=True, errors='ignore')

# Convert 'solicitud_id' and other 'numpy.int64' columns to Python integers
int_columns = ['solicitud_id', 'programa_id', 'puntuacion', 'prioridad', 'acompanante', 'acompanante_edad', 'acompanante_renta']
df_asignado[int_columns] = df_asignado[int_columns].astype(int)
df_lista_espera[int_columns] = df_lista_espera[int_columns].astype(int)

# Display the DataFrames
print("\nDataFrame df_asignado:")
print(df_asignado)

print("\nDataFrame df_lista_espera:")
print(df_lista_espera)

# Store df_asignado in 'plazas_asignadas' table with asignadas_id as SERIAL
df_asignado.reset_index(drop=True, inplace=True)
df_asignado['puntuacion'] = df_asignado['puntuacion'].astype(int)
df_asignado.to_sql('plazas_asignadas_nopen', engine, if_exists='replace', index=False, dtype={'puntuacion': Integer})

# Store df_lista_espera in 'lista_espera' table with espera_id as SERIAL
df_lista_espera.reset_index(drop=True, inplace=True)
df_lista_espera.to_sql('lista_espera_nopen', engine, if_exists='replace', index=False)

# Connect to PostgreSQL database
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# SQL queries to set primary keys and foreign keys
alter_query_asignadas = """
    ALTER TABLE plazas_asignadas_nopen
    ADD PRIMARY KEY (asignada_id),
    ADD FOREIGN KEY (solicitud_id) REFERENCES solicitudes(solicitud_id);
"""

alter_query_espera = """
    ALTER TABLE lista_espera_nopen
    ADD PRIMARY KEY (espera_id),
    ADD FOREIGN KEY (solicitud_id) REFERENCES solicitudes(solicitud_id);
"""

# Execute SQL queries
cursor.execute(alter_query_asignadas)
cursor.execute(alter_query_espera)

# Commit and close the connection
connection.commit()
connection.close()
