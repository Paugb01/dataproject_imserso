DROP TABLE IF EXISTS solicitudes;
DROP TABLE IF EXISTS programas;
DROP TABLE IF EXISTS usuarios;

CREATE TABLE public.usuarios (
    usuario_id int4 NOT NULL,
    nif varchar(50) NOT NULL, 
    nombre varchar(50) NULL,
    apellidos varchar(50) NULL,
    edad int4 NULL,
    fecha_nacimiento date NULL,
    renta int4 NULL, 
    tipo_discapacidad int4 NULL, 
    tipo_familia int4 NULL, 
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

-- CREATE TABLE public.programassociales (
--     programasocial_id int4 NOT NULL,
--     nombre_programa varchar(50) NULL,
--     ponderacion_programa int4 NULL,
--     CONSTRAINT programassociales_pkey PRIMARY KEY (programasocial_id)
-- );

-- CREATE TABLE public.programausuario (
--     usuario_id int4 NOT NULL,
--     programasocial_id int4 NOT NULL,
--     CONSTRAINT programausuario_pkey PRIMARY KEY (usuario_id, programasocial_id),
--     CONSTRAINT programausuario_programasocial_id_fkey FOREIGN KEY (programasocial_id) REFERENCES public.programassociales (programasocial_id),
--     CONSTRAINT programausuario_usuario_id_fkey FOREIGN KEY (usuario_id) REFERENCES public.usuarios (usuario_id)
-- );

-- CREATE TABLE public.usuariosgrupos (
--     usuario_id int4 NOT NULL,
--     grupo_id int4 NOT NULL,
--     CONSTRAINT usuariosgrupos_pkey PRIMARY KEY (usuario_id, grupo_id),
--     CONSTRAINT usuariosgrupos_grupo_id_fkey FOREIGN KEY (grupo_id) REFERENCES public.grupos (grupo_id),
--     CONSTRAINT usuariosgrupos_usuario_id_fkey FOREIGN KEY (usuario_id) REFERENCES public.usuarios (usuario_id)
-- );

CREATE TABLE public.solicitudes (
    solicitudes_id int4 NOT NULL, 
    usuario_id int4 NOT NULL,
    programa_id int4 NULL,
    CONSTRAINT solicitudes_pkey PRIMARY KEY (solicitudes_id),
    CONSTRAINT solicitudes_usuario_id_fkey FOREIGN KEY (usuario_id) REFERENCES public.usuarios (usuario_id),
    CONSTRAINT solicitudes_programa_id_fkey FOREIGN KEY (programa_id) REFERENCES public.programas (programa_id)
);



