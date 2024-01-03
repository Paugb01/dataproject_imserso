DROP TABLE IF EXISTS solicitudes;
DROP TABLE IF EXISTS programas;
DROP TABLE IF EXISTS usuarios;

CREATE TABLE public.usuarios (
    usuario_id varchar(50) NOT NULL,
    ---nif varchar(50) NULL, 
    nombre varchar(50) NULL,
    apellidos varchar(50) NULL,
    edad int4 NULL,
    fecha_nacimiento date NULL,
    renta int4 NULL, 
    tipo_discapacidad int4 NULL, 
    tipo_familia int4 NULL,
    condicion_medica BOOLEAN,
    viudedad int4 NULL,
    antecedentes int4 NULL,
    participacion21_22 BOOLEAN, 
    viajes_realizados_21_22 int4 NULL, 
    participacion22_23 BOOLEAN, 
    viajes_realizados_22_23 int4 NULL,
    CONSTRAINT usuarios_pkey PRIMARY KEY (usuario_id)
);

-- CREATE TABLE public.grupos (
--     grupo_id int4 NOT NULL,
--     CONSTRAINT grupos_pkey PRIMARY KEY (grupo_id)
-- );

CREATE TABLE public.programas (
    programa_id int4 NOT NULL,
    nombre_programa varchar(100) NULL,
    plazas int4 NULL,
    origen varchar(50) NULL,
    destino varchar(50) NULL,
    fecha_salida date NULL,
    fecha_vuelta date NULL,
    CONSTRAINT programas_pkey PRIMARY KEY (programa_id)
);

CREATE TABLE public.solicitudes (
    solicitud_id int4 NOT NULL, 
    usuario_id varchar(50) NOT NULL,
    programa_id int4 NULL,
    puntuacion int4 null,
    CONSTRAINT solicitud_pkey PRIMARY KEY (solicitud_id),
    CONSTRAINT solicitudes_usuario_id_fkey FOREIGN KEY (usuario_id) REFERENCES public.usuarios (usuario_id),
    CONSTRAINT solicitudes_programa_id_fkey FOREIGN KEY (programa_id) REFERENCES public.programas (programa_id)
);



