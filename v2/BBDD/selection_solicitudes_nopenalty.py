'''
slection_solicitudes_nopenalty.py: Este script ordena las solicitudes (copiando la tabla 'solicitudes' de la BD)
por puntuación y preferencia, y asigna cada solicitud a un programa mientras queden plazas. Cuando se agoten las
plazas o si quedan menos de 2 plazas, se envía la solicitud a la lista de espera. Cada solicitud exitosa NO GENERA 
PENALIZACIÓN de puntos al resto de solicitudes de ese usuario_id. Este script se ha hecho para poder generar tablas
de resultados sin penalización (penalty_factor = 1) y poder comparar este criterio de justicia.
'''
import pandas as pd
from sqlalchemy import create_engine, Integer
import numpy as np
import psycopg2

# Parámetros de conexión a la BD. host:localhost en local, postgres en compose!!!
db_params = {
    'host': 'postgres',
    'port': 5432,
    'user': 'postgres',
    'password': 'Welcome01',
    'database': 'postgres',  # Replace with your actual database name
}

# SQLAlchemy
db_uri = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
engine = create_engine(db_uri)

# Lee 'plazas' de 'programas' y la guarda en un diccionario para ir restando plazas asignadas.
plazas_dict = pd.read_sql_table('programas', engine, columns=['programa_id', 'plazas']).set_index('programa_id').to_dict()['plazas']

# Inicializa DFs:
df_asignado = pd.DataFrame()
df_lista_espera = pd.DataFrame()

# Inicializa asignada_id and espera_id
next_asignada_id = 1
next_espera_id = 1

# Factor de penalización
penalty_factor = 1

# Baja 'solicitudes' de la BD ordenada por 'puntuacion' (DESC) y  y asigna a DF:
query_solicitudes = """
    SELECT *
    FROM solicitudes
    ORDER BY puntuacion DESC, prioridad ASC;
"""

df_solicitudes = pd.read_sql_query(query_solicitudes, engine)

# Ordena el DF de solicitudes por 'puntiuacion' (DESC)
df_solicitudes.sort_values(by='puntuacion', ascending=False, inplace=True)

# Itera por cada entrada del DF mientras queden entradas:
while not df_solicitudes.empty:
    top_entry = df_solicitudes.iloc[0]
    solicitud_id = top_entry['solicitud_id']
    programa_id = top_entry['programa_id']
    usuario_id = top_entry['usuario_id']
    # Lee las plazas disponibles para ese programa
    available_plazas = plazas_dict.get(programa_id, 0)

    if available_plazas > 1:
        # Comprueba si 'acompanante' = True
        is_acompanante = top_entry['acompanante']

        # Asigna valor 2 si 'acompanante' = True, si no, asigna 1
        allocation_count = 2 if is_acompanante else 1

        # Pasa la solicitud a 'df_asignado' con una 'asignada_id'
        for _ in range(allocation_count):
            df_asignado = pd.concat([df_asignado, top_entry.to_frame().T], ignore_index=True)
            df_asignado.loc[df_asignado.index[-1], 'asignada_id'] = next_asignada_id
            df_asignado['asignada_id'] = df_asignado['asignada_id'].astype(int)
            available_plazas -= 1
            plazas_dict[programa_id] = available_plazas  # Reduce las plazas disponibles
            print(f"Solicitud {solicitud_id}: Allocated to df_asignado. Available plazas for programa_id {programa_id}: {available_plazas}")
            next_asignada_id += 1  # Incrementa asignada_id

        # Aplica el factor de penalización al resto de entradas de 'usuario_id'
        penalty_mask = (df_solicitudes['usuario_id'] == usuario_id)
        df_solicitudes.loc[penalty_mask, 'puntuacion'] = (df_solicitudes.loc[penalty_mask, 'puntuacion'] * penalty_factor).astype(int)

        # Elimina la entrada analizada
        df_solicitudes.drop(df_solicitudes.index[0], inplace=True)

        # Reordena el DF por 'puntuacion' (DESC) y 'prioridad' (ASC)
        df_solicitudes.sort_values(by=['puntuacion', 'prioridad'], ascending=[False, True], inplace=True)



    else:
        # Mueve la entrada a 'df_lista_espera' con 'espera_id' único
        top_entry_copy = top_entry.copy()
        top_entry_copy['espera_id'] = next_espera_id
        df_lista_espera = pd.concat([df_lista_espera, top_entry_copy.to_frame().T], ignore_index=True)
        print(f"Solicitud {solicitud_id}: Moved to df_lista_espera. No available plazas for programa_id {programa_id}")
        next_espera_id += 1  # Incrementa espera_id

        # Elimina la entrada procesada
        df_solicitudes.drop(df_solicitudes.index[0], inplace=True)

# Elimina columnas innecesarias para las tablas de df_asignado y df_lista_espera
df_asignado.drop(columns=['row_num'], inplace=True, errors='ignore')
df_lista_espera.drop(columns=['row_num'], inplace=True, errors='ignore')

# Convierte 'solicitud_id' y otras columnas tipo 'numpy.int64' a ints de python (daba error si no al subir a la BD)
int_columns = ['solicitud_id', 'programa_id', 'puntuacion', 'prioridad', 'acompanante', 'acompanante_edad', 'acompanante_renta']
df_asignado[int_columns] = df_asignado[int_columns].astype(int)
df_lista_espera[int_columns] = df_lista_espera[int_columns].astype(int)

# Verificamos DataFrames
print("\nDataFrame df_asignado:")
print(df_asignado)

print("\nDataFrame df_lista_espera:")
print(df_lista_espera)

# Guardamos df_asignado en la tabla 'plazas_asignadas_nopen' de la BD
df_asignado.reset_index(drop=True, inplace=True)
df_asignado['puntuacion'] = df_asignado['puntuacion'].astype(int)
df_asignado.to_sql('plazas_asignadas_nopen', engine, if_exists='replace', index=False, dtype={'puntuacion': Integer})

# Guardamos df_lista_espera en la tabla 'lista_espera_nopen' de la BD
df_lista_espera.reset_index(drop=True, inplace=True)
df_lista_espera.to_sql('lista_espera_nopen', engine, if_exists='replace', index=False)

# Conectamos a la BD
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Queries SQL para asignar primary keys y foreign keys en la tablas 'plazas_asignadas' y 'lista_espera'
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

# Ejecutamos queries SQL
cursor.execute(alter_query_asignadas)
cursor.execute(alter_query_espera)

# Enviamos y cerramos conexión a la BD
connection.commit()
connection.close()
