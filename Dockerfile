# Imagen de Python 3.9
FROM python:3.9

# Creando directorio de trabajo

WORKDIR /app

RUN pip install --upgrade pip

# Copiando directorio SQL y requirements.txt

COPY ./sql .
COPY requirements.txt .

# Instalando las dependencias necesarias 

RUN pip install -r requirements.txt    

# Copiando los scripts 

COPY gen_datos_simple.py .
COPY gen_datos_programas.py .

# Ejecutando scripts 

CMD [ "sh", "-c", "python3 gen_datos_simple.py && python3 gen_datos_programas.py" ]









