import pandas as pd

# Para mvp de momento solo programas capitales de provincia
# Datos de provincias y capitales
data = {
    'Provincia': [
        'Álava', 'Albacete', 'Alicante', 'Almería', 'Asturias', 'Ávila', 'Badajoz',
        'Barcelona', 'Burgos', 'Cáceres', 'Cádiz', 'Cantabria', 'Castellón',
        'Ciudad Real', 'Córdoba', 'Cuenca', 'Gerona', 'Granada', 'Guadalajara',
        'Guipúzcoa', 'Huelva', 'Huesca', 'Islas Balears', 'Jaén', 'La Coruña',
        'La Rioja', 'Las Palmas', 'León', 'Lérida', 'Lugo', 'Madrid', 'Málaga',
        'Murcia', 'Navarra', 'Orense', 'Palencia', 'Pontevedra', 'Salamanca',
        'Santa Cruz de Tenerife', 'Segovia', 'Sevilla', 'Soria', 'Tarragona',
        'Teruel', 'Toledo', 'Valencia', 'Valladolid', 'Vizcaya', 'Zamora', 'Zaragoza'
    ],
    'Capital': [
        'Vitoria-Gasteiz', 'Albacete', 'Alicante', 'Almería', 'Oviedo', 'Ávila', 'Badajoz',
        'Barcelona', 'Burgos', 'Cáceres', 'Cádiz', 'Santander', 'Castellón',
        'Ciudad Real', 'Córdoba', 'Cuenca', 'Gerona', 'Granada', 'Guadalajara',
        'San Sebastián', 'Huelva', 'Huesca', 'Palma de Mallorca', 'Jaén', 'La Coruña',
        'Logroño', 'Las Palmas de Gran Canaria', 'León', 'Lérida', 'Lugo', 'Madrid', 'Málaga',
        'Murcia', 'Pamplona', 'Orense', 'Palencia', 'Pontevedra', 'Salamanca',
        'Santa Cruz de Tenerife', 'Segovia', 'Sevilla', 'Soria', 'Tarragona',
        'Teruel', 'Toledo', 'Valencia', 'Valladolid', 'Bilbao', 'Zamora', 'Zaragoza'
    ]
}

# Crear DataFrame
df = pd.DataFrame(data)

# Crear tabla de programas
tabla_programas = {'programa_id': [], 'origen': [], 'destino': []}
id = 1

for origen in data['Capital']:
    for destino in data['Capital']:
        if origen != destino:
            tabla_programas['programa_id'].append(id)
            tabla_programas['origen'].append(origen)
            tabla_programas['destino'].append(destino)
            id += 1

# Convertir la tabla de programas a DataFrame
df_programas = pd.DataFrame(tabla_programas)

# Mostrar el DataFrame de programas
print(df_programas)
