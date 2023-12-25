import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'Welcome01',
    'database': 'postgres',  # Replace with your actual database name
}

# Connect to PostgreSQL database
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# SQL queries to set primary keys
alter_query_asignadas = """
    ALTER TABLE plazas_asignadas
    ADD PRIMARY KEY (asignadas_id);
"""

alter_query_espera = """
    ALTER TABLE lista_espera
    ADD PRIMARY KEY (espera_id);
"""

# Execute SQL queries
cursor.execute(alter_query_asignadas)
cursor.execute(alter_query_espera)

# Commit and close the connection
connection.commit()
connection.close()
