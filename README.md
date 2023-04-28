# Prueba específica unidad 9

> Se evaluará el RA9


## 1. Problema
El equipo de ciberseguridad del IESRA quiere llevar un histórico de la puntuación que consigue cada equipo en los CTFs que se realizan a lo largo del curso.

Para ello han diseñado una base de datos muy simple en la que tiene dos entidades:
- entidad `GRUPOS`: Los grupos participantes.
    - `grupoid`: Identificador del grupo.
    - `grupodesc`: Descripción del grupo.
    - `mejorposCTFid`: Id del CTF en el que ha logrado la mejor posición.
- entidad `CTFS`: La información sobre la participación de cada grupo en un `CTFS`
    - `CTFid`: Identificador del CTF
    - `grupoid`: Identificador del grupo que participa en el CTF
    - `puntuacion`: Puntuación lograda.

El programa tendrá que soportar las siguientes operaciones, que se pasarán a través de línea de comandos:

| id | **comando**                       	| **Descripción**                                                                    	|
|---|-----------------------------------	|------------------------------------------------------------------------------------	|
| 1 | `-a <ctfid> <grupoId> <puntuacion>` 	| Añade una participación del grupo `<grupoid>` en el CTF `<ctfid>` con la puntuación `<puntuacion>`. Recalcula el campo `mejorposCTFid` de los grupos en la tabla `GRUPOS`. |
| 2 | `-d <ctfid> <grupoId>`              	| Elimina la participación del grupo `<grupoid>` en el CTF `<ctfid>`. Recalcula el campo `mejorposCTFid` de los grupos en la tabla `GRUPOS`. |
| 3 | `-l <grupoId>`              	        | Si `<grupoId>` esta presente muestra la información del grupo `<grupoId>`, sino muestra la información de todos los grupos.    |

### Entrada
La ejecución de cualquiera de las operaciones 1, 2 o 3 descritas en la tabla. 

### Salida
- Mensaje adecuado si el comando suministrado no es correcto en cualquiera de las operaciones 1, 2 o 3.
  ```
  $ un9pe -a 1 1 
  $ ERROR: El número de parametros no es adecuado.
  ```
- Mensaje de confirmación de realización correcta de la operaciones 1, 2 o 3. 
  ```
  $ un9pe -a 1 1 100
  $ Procesado: Añadida participación del grupo "1DAM-1" en el CTF 1 con una puntuación de 100 puntos.
  ```
  
  ```
  $ un9pe -d 1 1
  $ Procesado: Eliminada participación del grupo "1DAM-1" en el CTF 1.
  ```

- Listado de información en la operacion 3.
  ```
  $ un9pe -l 1
  $ Procesado: Listado participación del grupo "1DAM-1"
    GRUPO: 1   1DAM-G1  MEJORCTF: 1
  ```

### Esquema de base de base de datos.

Tabla `GRUPOS`
```
CREATE TABLE GRUPOS (
    grupoid INT NOT NULL AUTO_INCREMENT,
    grupodesc VARCHAR(100) NOT NULL,
    mejorposCTFid INT,
    PRIMARY KEY (grupoid)
);
```

Tabla `CTFS`
```
CREATE TABLE CTFS (
    CTFid INT NOT NULL,
    grupoid INT NOT NULL,
    puntuacion INT NOT NULL,
    PRIMARY KEY (CTFid,grupoid)
);
```

Restricciones de clave foránea para la tabla `GRUPOS`
```
ALTER TABLE GRUPOS
ADD FOREIGN KEY (mejorposCTFid, grupoid)
REFERENCES CTFS(CTFid,grupoid);
```

Valores para la tabla `GRUPOS`
```
insert into grupos(grupoid, grupodesc) values(1, '1DAM-G1');
insert into grupos(grupoid, grupodesc) values(2, '1DAM-G2');
insert into grupos(grupoid, grupodesc) values(3, '1DAM-G3');
insert into grupos(grupoid, grupodesc) values(4, '1DAW-G1');
insert into grupos(grupoid, grupodesc) values(5, '1DAW-G2');
insert into grupos(grupoid, grupodesc) values(6, '1DAW-G3');
```

## 2. ¿Qué se pide?

a. Realizar el programa haciendo uso de la base de datos H2. No olvidar configurar adecuadamente el archivo `build.gradle`.

b. Poder ejecutar las operaciones, haciendo uso de los objetos DAO necesarios que accedan a la BBDD h2

1. Ejecutar la operación 1 para añadir una participación de un grupo en un CTF. Actualización del CTF en el que quedó mejor situado de todos los grupos. 
2. Ejecutar la operación 2 para eliminar una participación de un grupo en un CTF. Actualización del CTF en el que quedó mejor situado de todos los grupos.
3. Ejecutar la operación 3 para listar la información de todos los grupos o un grupo en concreto.

## 3. Ejecución y test
Para probar el programa se realizará distintas llamadas al programa, comparando la salida con la salida esperada.
- Deberá comprobar los distintos casos de error. 
- Deberá mostrar la información sobre las operaciones realizadas como se muestra en el apartado Salida.

## 4. Evaluación




###### Conexión a base de datos.
0. No lo hace; 1. Funciona adecuadamente, pero básico; 2. Funciona adecuadamente, y usado pool de conexiones.
###### Almacena información en base de datos. 
0. No lo hace; 3. Funciona adecuadamente, pero básico; 6. Funciona adecuadamente, controla errores y de forma avanzada.
###### Consulta y Recupera información de la base de datos.
0. No lo hace; 4. Funciona adecuadamente, pero básico; 8. Funciona adecuadamente, controla errores y de forma avanzada.
###### Actualiza información de la base de datos.
0. No lo hace; 3. Funciona adecuadamente, pero básico; 6. Funciona adecuadamente, controla errores y de forma avanzada.
###### Gestión de la información
0. No lo hace; 5. Funciona adecuadamente, pero básico; 10. Funciona adecuadamente, controla errores y de forma avanzada. Teniendo en cuenta aspectos como:
    - El código realizado es óptimo.
    - El código realizado es limpio y está comentado. 
    - Creación de código con responsabilidades bien identificadas.
    - Creación de código con haciendo uso de clases, y de jerarquía de clases propias si son necesarias o ya conocidas y que nos las proporcionan kotlin, como por ejemplo List, Map, Set. etc.
    - Se cumple requisitos de entrega.

## 5. Condiciones de entrega

## 4. Bibliografía