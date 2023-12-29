# Proyecto de Asignación de Plazas Hoteleras para el Imserso

Este proyecto tiene como objetivo modernizar el proceso de asignación de plazas hoteleras para el Imserso, a través de la creación de una plataforma eficiente y justa. A continuación, se detallan los pasos y consideraciones clave para el desarrollo de este producto, así como el equipo responsable de su ejecución.

## Objetivo del Proyecto

El Imserso busca agilizar su proceso de asignación de plazas hoteleras, y para ello, hemos sido encomendados con la tarea de diseñar una plataforma que garantice una distribución justa de las mismas. El proyecto no solo consiste en la creación de la plataforma, sino también en la presentación y venta de la solución como si estuviera participando en un concurso público.

## Desarrollo del Producto

### Pasos a Seguir:

1. **Análisis de Requisitos:**
   - Comprender a fondo las necesidades y requisitos del Imserso.
   - Identificar los criterios clave para una asignación justa de plazas hoteleras.

2. **Diseño de la Plataforma:**
   - Desarrollar una interfaz intuitiva y fácil de usar.
   - Implementar algoritmos de asignación que cumplan con los criterios establecidos.

3. **Desarrollo Técnico:**
   - Utilizar tecnologías modernas y robustas para garantizar la eficiencia y la seguridad.
   - Integrar la plataforma con sistemas existentes del Imserso, si es necesario.

4. **Presentación y Venta:**
   - Preparar una propuesta sólida que destaque los beneficios y la innovación de la plataforma.
   - Demostrar la eficacia y la equidad del sistema de asignación.

## Equipo de Desarrollo

Este proyecto está siendo llevado a cabo por un equipo altamente competente, compuesto por:

- Pau Garcia
- Julián Merino
- Toni Faura
- Hugo Fernando Maria Rodriguez
- Paco Tudela

Este readme sirve como guía inicial para el desarrollo del proyecto. ¡Éxito en la creación de una solución innovadora y eficiente para el Imserso!

## Detalles de desarrollo - MVP

- Nos basamos en la siguiente estructura SQL para la MVP
- ![Alt text](images/tables_mvp.png)


- Fin de MVP: 
  - Procesar usuarios, programas y solicitudes con los criterios actuales de IMSERSO (es decir, sin incluir las nuestras).
  - Generar una puntuación por usuario.
  - Rankear las solicitudes.

- Tabla programas:
  - Contiene programa_id y el resto de características del programa.
  - Dos programas pueden tener idénticas características y diferentes fechas de entrada y/o salida.

-  Tabla solicitudes:
   -  Enlaza usuarios mediante usuario_id y programas mediante programa_id
   -  No contiene fechas, ya que las fechas son únicas para cada programa: un usuario se inscribe en una programa con unas fechas predeterminadas ofrecidas.
   -  En un futuro posterior a MVP se puede considerar enlazar más de un usuario a un programa/solicitud - problemas del futuro.

-  Trabajo inmediato:
   -  Inyectar datos en tabla usuarios (100 basta para probar)
   -  Inyectar datos en tabla programas:
      -  2 programas por región de destino (costa insular, costa peninsular, interior y ya iremos ampliando y definiendo cuando funcione) y origen diferente.
   - Inyectar datos en la tabla solicitudes:
     - Hacer combinaciones de usuario_id y programa_id (que no se repitan, es decir un mismo usuario no puede solicitar lo mismo dos veces)
     - Como idea: cada usuario pida todos los programas una vez, en esta MVP serían 100 usuarios * 6 programas (3 regiones, 2 programas por región) = 600 solicitudes, ¿no?

-  23/12/2023
   -  jumepe: he creado un fichero imserso_jmp.sql nuevo que cambia usuario_id a string y le mete el random de NIF del fichero de generación e inserción de datos gen_datos_simple.py
   -  He cambiado el fichero de generacion e inserción de datos gen_datos_simple.py
   -  He creado un fichero de generación e inserción de datos a la tabla programas gen_datos_programas.py que hace:
      -  Genera 10 programas
      -  programa_id int correlativo de 1-10
      -  nombre_programa con la sintaxis origen - destino
      -  fecha_salida y fecha_vuelta limitado a meses de invierno y fecha_vuelta es posterior a fecha_salida
      -  destino es aleatorio entre tres opciones: Costa insular, Costa peninsular e interior
      -  Todo esto se puede cambiar!
      -  ![Alt text](images/programas_01.png)
      -  ![Alt text](images/usuarios_01.png)
-  24/12/2023
   - jumepe: 
     - He ordenado un poco de  la estructura de directorios
     - He creado el script process_usuarios que:
       - Lee la tabla usuarios de la BD y crea un dataframe
       - Añade la columna. 'puntos' al dataframe
       - Evalua tipo de familia, edad, renta y discapacidad y asigna puntos.
       - No evalúa si se ha participado anteriormente en el programa ni si hay más solicitudes para este año (post MVP).
   - Hugo: 
     - crea directorio BBDD y copia archivos necesarios para dockerfile.
   - jumepe: 
     - Pequeña modificación de sql/sql_jmp.sql para añadir la columna puntuacion (mucho problema por tema fkeys al insertar, así va) y cambiar solicitudes_id a solicitud_id
     - He añadido código para generar solicitudes mediante la combinación de usuario_id y programa_id
       - Un usuario_id puede combinar con más de un programa_id, pero no dos veces el mismo.
       - Por cada solicitud, hay una puntuación correspondiente al usuario. Esto en verdad se podría haber hecho con algún query supongo, pero creo que en la tabla será más fácil evaluar las solicitudes. Lo podemos cambiar en el futuro.
       - ![Alt text](images/solicitudes_01.png)
   - 25/12/2023:
     - jumepe: creado código selection_solicitudes.py que evalúa las solicitudes por puntuación, para cada programa_id:
       - Va por cada solicitud para cada programa_id y rankea las solicitudes por puntos.
       - Lee de la tabla programas las plazas que tiene cada programa_id
       - La más alta la mete en un dataframe llamado 'plazas_asignadas' SI quedan plazas
       - Pasa a la siguiente y hace lo mismo, pero si no quedan plazas lo mete en un dataframe 'espera'
       - Cuando ya ha evaluado todo, sube estos dataframes a la base de datos en dos tablas.
       - PROBLEMA A RESOLVER: no es crítico, pero no he conseguido que establezca asignadas_id y espera_id como primary keys...lo miraremos más tarde.
       - ![Alt text](images/plazas_asignadas_01.png)
       - ![Alt text](images/lista_espera_01.png)
       - RESUELTO problema estableciendo asignadas_id y espera_id como primary keys:
         - El problema debe de estar relacionado con cómo sqlalchemy ejecuta el código SQL. Sin embargo, sqlalchemy es necesario para realizar las operaciones con Pandas, así que al final del código vuelvo a conectar a la BD con psycopg2 y el código SQL pasa correctamente.
       - Añadida solicitud_id como foreign keys a lista_espera y plazas_asignadas. Ahora todas las tablas están interconectadas a través de la tabla solicitudes.
       - ![Alt text](images/irdiagram_01.png)
- 28/12/2023:
     - tumup: 
       - Se añaden los criterios viudedad, participación años anteriores, enfermedad y antecedentes a la tabla gen_datos_v1
       - Se modifica docker y conexión a BBDD en función de los nuevos nombres de los ficheros
       - Se asignan porcentajes mas reales, contrastados con estudios, para las variables enfermedad, viudedad y discapacidad.
       - Se añade lógica para generar usuarios que quedaran en lista de espera en la temporada anterior.
       - Se modifica fichero sql para que la nueva base levante con todos los campos necesarios
       - Se añade lógica en el fichero gen_datos_programas para que los destinos generados sean los destinos reales del imserso
       - Se crean funciones en fichero process_usuario para puntuar los nuevos criterios
       - 
       - 
       - 
       - 
       - 
