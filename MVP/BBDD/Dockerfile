# Imagen de Python 3.9
FROM python:3.12

# Creando directorio de trabajo

WORKDIR /app

RUN pip install --upgrade pip

# Copiando directorio SQL y requirements.txt

COPY requirements.txt .

# Instalando las dependencias necesarias 

RUN pip install -r requirements.txt    

# Copiando los scripts 

COPY gen_datos_simple.py .
COPY gen_datos_programas.py .
COPY process_usuarios.py .
COPY selection_solicitudes.py .


# Ejecutando scripts 

CMD [ "bash", "-c", "python gen_datos_simple.py && python gen_datos_programas.py && python process_usuarios.py && python selection_solicitudes.py" ]









