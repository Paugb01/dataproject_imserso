import pandas as pd
import psycopg2

# Database connection parameters
db_params = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Welcome01',
    'host': 'postgres',
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


#Function to evaluate the health status of users in usuarios:
def puntuar_condicion_medica(row):
    if row['condicion_medica'] == True:
        return row['puntos'] + 20
    else:
        return row['puntos'] + 0
    
#Pass puntuar_condicion_medica function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_condicion_medica, axis = 1)


#Function to evaluate assess widowhood of users in usuarios:
def puntuar_viudez(row):
    if row['viudedad'] == 1:
        return row['puntos'] + 10
    else:
        return row['puntos'] + 0
    
#Pass puntuar_viudez function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_viudez, axis = 1)


#Function to evaluate participation in previous years of users in usuarios:
def puntuar_participacion_previa(row):
    if row['participacion21_22'] == False and row['participacion22_23'] == False:
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

#Pass puntuar_participacio_previa function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_participacion_previa, axis = 1)

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

#Function to evaluate the criminal record of the users in usuarios:
def puntuar_antecedentes(row):
    if row['antecedentes'] == 0:
        return row['puntos'] 
    elif row['antecedentes'] == 1:
        return row['puntos'] - 5
    else:
        return row['puntos'] -10
    
#Pass puntuar_antecedentes function to the df:
df_usuarios_from_db['puntos'] = df_usuarios_from_db.apply(puntuar_antecedentes, axis = 1)
#with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#    print(df_usuarios_from_db)
#df_usuarios_from_db.info()
print(df_usuarios_from_db)