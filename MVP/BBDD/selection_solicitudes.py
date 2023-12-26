import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2

# Assuming you already have a SQLAlchemy engine
db_uri = 'postgresql+psycopg2://postgres:Welcome01@postgres:5432/postgres'
engine = create_engine(db_uri)

# Read 'plazas' from 'programas' table and store it in a dictionary
plazas_dict = pd.read_sql_table('programas', engine, columns=['programa_id', 'plazas']).set_index('programa_id').to_dict()['plazas']

# Initialize DataFrames
df_asignado = pd.DataFrame()
df_lista_espera = pd.DataFrame()

# Iterate through unique programa_ids
for programa_id in plazas_dict.keys():
    available_plazas = plazas_dict[programa_id]

    # Query to select all rows for each programa_id and rank by puntuacion
    query = f"""
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY programa_id ORDER BY puntuacion DESC) as row_num
            FROM solicitudes
            WHERE programa_id = {programa_id}
        ) AS s
        ORDER BY puntuacion DESC
    """

    # Execute the query and create the DataFrame
    df_programa = pd.read_sql_query(query, engine)

    # Iterate through all selected rows and allocate to df_asignado or df_lista_espera
    for index, row in df_programa.iterrows():
        solicitud_id = row['solicitud_id']
        if available_plazas > 0:
            df_asignado = pd.concat([df_asignado, row.to_frame().T], ignore_index=True)
            available_plazas -= 1
            print(f"Solicitud {solicitud_id}: Allocated to df_asignado. Available plazas for programa_id {programa_id}: {available_plazas}")
        else:
            df_lista_espera = pd.concat([df_lista_espera, row.to_frame().T], ignore_index=True)
            print(f"Solicitud {solicitud_id}: Moved to df_lista_espera. No available plazas for programa_id {programa_id}")

# Drop the 'row_num' column from both DataFrames
df_asignado.drop(columns=['row_num'], inplace=True)
df_lista_espera.drop(columns=['row_num'], inplace=True)

# Display the DataFrame df_asignado
print("\nDataFrame df_asignado:")
print(df_asignado)

# Display the DataFrame df_lista_espera
print("\nDataFrame df_lista_espera:")
print(df_lista_espera)

# Store df_asignado in 'plazas_asignadas' table with asignadas_id as SERIAL
df_asignado.reset_index(drop=True, inplace=True)
df_asignado.to_sql('plazas_asignadas', engine, if_exists='replace', index=True, index_label='asignadas_id')

# Store df_lista_espera in 'lista_espera' table with espera_id as SERIAL
df_lista_espera.reset_index(drop=True, inplace=True)
df_lista_espera.to_sql('lista_espera', engine, if_exists='replace', index=True, index_label='espera_id')

# Database connection parameters
db_params = {
    'host': 'postgres',
    'port': 5432,
    'user': 'postgres',
    'password': 'Welcome01',
    'database': 'postgres',  # Replace with your actual database name
}

# Connect to PostgreSQL database
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# SQL queries to set primary keys and foreign keys
alter_query_asignadas = """
    ALTER TABLE plazas_asignadas
    ADD PRIMARY KEY (asignadas_id),
    ADD FOREIGN KEY (solicitud_id) REFERENCES solicitudes(solicitud_id);
"""

alter_query_espera = """
    ALTER TABLE lista_espera
    ADD PRIMARY KEY (espera_id),
    ADD FOREIGN KEY (solicitud_id) REFERENCES solicitudes(solicitud_id);
"""

# Execute SQL queries
cursor.execute(alter_query_asignadas)
cursor.execute(alter_query_espera)

# Commit and close the connection
connection.commit()
connection.close()

# Display the count of selected entries in df_asignado and df_lista_espera
print("\nCount of selected entries in df_asignado:", len(df_asignado))
print("Count of entries in df_lista_espera:", len(df_lista_espera))
