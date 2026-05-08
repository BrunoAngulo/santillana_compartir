# SUMUN

## Que hace

El flujo SUMUN convierte una matriz curricular en Excel a una plantilla plana de carga.

La entrada es un `.xlsx` con una o varias hojas.
La salida es otro `.xlsx` con una hoja principal lista para carga y una hoja `Datos` con referencias.

## Archivos clave

- `santillana_format/sumun.py`: motor completo de deteccion, lectura, transformacion y escritura.
- `app.py`: interfaz Streamlit para subir el Excel, inspeccionar hojas y descargar el resultado.
- `convert_sumun_matrix.py`: entrada CLI para convertir desde terminal.
- `tests/test_sumun.py`: pruebas basicas del parser de estaciones y de la reutilizacion de IDs.

## Librerias usadas

### Librerias externas

- `openpyxl`
  - Lee el Excel fuente.
  - Escribe el Excel de salida.
  - Expande celdas combinadas.
  - Aplica formato, anchos y tabla Excel.
- `streamlit`
  - Muestra la UI web para SUMUN.

### Librerias standard de Python

- `argparse`: parametros de la version CLI.
- `collections.defaultdict`: contador de microhabilidades especificas por itinerario y estacion.
- `dataclasses`: estructuras de resumen e inspeccion.
- `io.BytesIO`: trabajo en memoria con archivos Excel.
- `pathlib.Path`: rutas de archivos.
- `re`: deteccion de encabezados y parseo de textos.
- `unicodedata`: normalizacion de acentos para comparar encabezados.

Nota: `pandas` y `requests` existen en el proyecto, pero el flujo SUMUN no depende de ellas.

## Entradas esperadas

La matriz debe contener, por encabezado o por posicion fallback, estas piezas:

- `ITINERARIO`
- `COMPETENCIA`
- `MACROHABILIDAD`
- `MICROHABILIDAD`
- `ESTACION`
- `CONOCIMIENTOS`
- una o mas columnas de procesos cognitivos:
  - `RECORDAR`
  - `COMPRENDER`
  - `APLICAR`
  - `ANALIZAR`
  - `EVALUAR`
  - `CREAR`

El formato mas comun usa dos filas de encabezado:

- fila 1: columnas base y el bloque general `NANOHABILIDADES`
- fila 2: subencabezados de proceso `RECORDAR` a `CREAR`

### Formatos que entiende

- Itinerario:
  - `1`
  - `1.0`
  - `Itinerario 1. La celula`
- Estacion:
  - `1. La celula`
  - `E1 - La celula`
  - `Estacion 1 - La celula`
  - `La celula`
  - `Primera estacion`

### Casos soportados

- Todas las filas en una sola hoja.
- Un itinerario o hito por hoja.
- Celdas combinadas.
- Hojas visibles mezcladas con hojas ocultas.

## Flujo general

```text
UI o CLI
  -> carga bytes del .xlsx
  -> inspeccion opcional de hojas
  -> deteccion de layout por hoja
  -> lectura fila por fila
  -> expansion de procesos cognitivos a filas planas
  -> asignacion de IDs y contadores
  -> escritura de nuevo .xlsx
```

## Como entra por la UI

La vista SUMUN esta en `app.py`.

El flujo es:

1. El usuario sube un `.xlsx`.
2. La UI llama `inspect_sumun_workbook_sheets(...)`.
3. Se muestra una tabla con:
   - indice de hoja
   - nombre
   - si fue detectada como matriz
   - filas estimadas
   - detalle del diagnostico
4. El usuario elige hojas a procesar:
   - todas las detectadas
   - todas las hojas
   - una hoja concreta
5. El usuario puede forzar:
   - codigo de curso
   - grado
   - nivel
   - area
6. La UI llama `generate_sumun_template_from_excel(...)`.
7. Se guarda el Excel generado en memoria y se habilita la descarga.

## Como entra por CLI

Script: `convert_sumun_matrix.py`

Ejemplo:

```bash
python convert_sumun_matrix.py matriz.xlsx salida.xlsx --course-code MA --grade 4 --level Secundaria
```

Parametros disponibles:

- `matrix`: archivo fuente.
- `output`: archivo destino.
- `--area`
- `--grade`
- `--level`
- `--course-code`
- `--sheet`: se puede repetir para elegir hojas especificas.

## Como inspecciona una hoja

Funcion principal: `inspect_sumun_workbook_sheets(...)`

Por cada hoja:

1. Omite hojas ocultas.
2. Lee todas las celdas con `_fill_merged_values(...)`.
   - Si una celda estaba combinada, replica el valor a todo el rango.
3. Intenta detectar el layout con `_detect_matrix_layout(...)`.
4. Si encuentra layout, calcula filas potenciales con `_scan_sheet_rows(...)`.
5. Devuelve un objeto `SumunSheetInspection` con:
   - indice
   - nombre
   - detectada o no
   - filas estimadas
   - razon textual

## Como detecta el layout

Primero intenta deteccion por encabezados:

- busca `ITINERARIO`, `COMPETENCIA`, `MACROHABILIDAD`, `MICROHABILIDAD`, `ESTACION`, `CONOCIMIENTOS`
- busca procesos cognitivos en la fila del encabezado o en las dos siguientes
- exige al menos 2 procesos para considerar valida la matriz
- si los encabezados estan repartidos entre las dos primeras filas, combina ambas para detectar mejor las columnas

La comparacion de encabezados es tolerante:

- ignora mayusculas/minusculas
- ignora acentos
- ignora separadores raros

Si eso falla, usa un fallback estructural:

- columna A: itinerario
- B: competencia
- C: macro
- D: micro
- E: estacion
- F: conocimientos
- G:L: procesos cognitivos

## Como decide si una fila sirve

La fila solo genera salida si tiene todo esto:

- itinerario detectable o heredable desde el nombre de hoja
- estacion detectable o heredable desde una fila anterior del mismo itinerario
- `COMPETENCIA`
- `MACROHABILIDAD`
- `MICROHABILIDAD`
- al menos una microhabilidad especifica en los procesos cognitivos

## Como obtiene cada dato

### Itinerario

Sale de:

- la celda `ITINERARIO`, o
- el nombre de la hoja si contiene algo como `ITI1`, `ITINERARIO 1` o `HITO 1`

Regla importante:

- si la celda solo trae `1`, el sistema intenta reutilizar el mejor nombre conocido para ese mismo itinerario en el workbook
- si no existe un nombre mas descriptivo en ninguna hoja, entonces el nombre visible queda como `1`
- si trae `Itinerario 1. La celula`, el numero queda `1` y el nombre queda `La celula`

### Estacion

Sale de la columna `ESTACION`.

Puede venir de dos formas:

- numerada, por ejemplo `E1 - Fracciones`
- solo por nombre, por ejemplo `Fracciones` o `Primera estacion`

Si viene solo por nombre, el sistema compara el contenido de la celda y:

- reutiliza la misma estacion si ese texto ya aparecio antes en el mismo itinerario
- o crea un numero interno nuevo para esa estacion en su primera aparicion

Solo se omite la fila si la columna `ESTACION` no tiene un dato util y tampoco existe una estacion previa heredable para ese itinerario.
Eso se reporta en `nonnumber_station_rows`.

### Macro y micro

Se limpian con `_clean_text(...)`.
Si tienen bullets al inicio, se remueven.

### Microhabilidades especificas

Cada celda de proceso cognitivo genera como maximo una fila.
Se lee con `_specific_skill_cell_value(...)`.

Regla actual:

- si una celda tiene texto, genera una fila
- el contenido interno de la celda se conserva en una sola microhabilidad especifica
- los saltos de linea, vinetas o numeraciones no hacen que una celda se divida en varias filas
- si la celda de proceso esta combinada con otras filas, solo cuenta una vez desde la celda origen del merge

## Como ordena las filas

El orden final sale de estas reglas:

1. Respeta el orden original de hojas del workbook.
2. Dentro de cada hoja, respeta el orden original de filas.
3. Dentro de una fila, recorre los procesos en este orden fijo:
   - `RECORDAR`
   - `COMPRENDER`
   - `APLICAR`
   - `ANALIZAR`
   - `EVALUAR`
   - `CREAR`
4. Si una fila tiene varios procesos con valor, genera una fila por cada celda de proceso no vacia siguiendo ese orden fijo.

## Como genera los IDs y contadores

### Prefijo base

Se arma con:

- abreviatura de nivel
- codigo de curso
- grado

Ejemplo:

- `SECMA4`

### ID de fila

Formato:

```text
{PREFIJO}_I{itinerario}_E{estacion}_MA{macro}_MI{micro}_ME{micro_especifica}
```

Ejemplo:

```text
SECMA4_I01_E02_MA03_MI05_ME04
```

### Reglas de numeracion

- `# MACROHABILIDAD`
  - se define en la primera aparicion del texto de macro
  - luego se reutiliza si la misma macro vuelve a aparecer mas adelante
  - es global al archivo procesado, no por estacion
- `# MICROHABILIDAD`
  - misma regla que macro
  - tambien es global al archivo procesado
- `# MICROHABILIDADES ESPECIFICAS`
  - se reinicia por combinacion `(itinerario, estacion)`
- `# ITINERARIO`
  - lleva el numero de itinerario
- `ITINERARIO`
  - lleva solo el nombre descriptivo
  - si no existe un nombre descriptivo, puede quedar vacio

## Como infiere curso, grado, area y nivel

### Curso y grado

Si el usuario no los fuerza desde UI o CLI, el sistema intenta inferirlos desde:

1. nombre del archivo
2. nombres de hoja

Busca patrones como:

- `MA4`
- `CT2`
- `CCSS5`

### Area

Si el usuario no la fuerza, se infiere por mapa de codigo:

- `CO` / `COM` -> `Comunicacion`
- `MA` / `MAT` -> `Matematica`
- `CCSS` -> `Ciencias sociales`
- `CT` / `CCT` -> `Ciencia y Tecnologia`
- `PS` -> `Personal Social`

### Nivel

Mapa actual:

- `Primaria` -> `PRI`
- `Secundaria` -> `SEC`

## Como se escribe el Excel de salida

Funcion: `_write_template_workbook(...)`

Hace esto:

1. Crea un workbook nuevo.
2. Crea una hoja principal con nombre seguro, por ejemplo `MA4SEC`.
3. Escribe `OUTPUT_HEADERS`.
4. Escribe todas las filas planas.
5. Congela la primera fila.
6. Aplica negrita y `wrap_text`.
7. Ajusta anchos de columna.
8. Convierte el rango en una tabla Excel llamada `Tabla1`.
9. Agrega una hoja `Datos` con referencias auxiliares.

## Columnas de salida

Las columnas producidas son:

1. `ID MICRO HABILIDAD ESPECIFICA`
2. `AREA`
3. `GRADO`
4. `Nivel`
5. `BIMESTRE`
6. `# ITINERARIO`
7. `ITINERARIO`
8. `COMPETENCIA`
9. `# MACROHABILIDAD`
10. `MACROHABILIDAD`
11. `# MICROHABILIDAD`
12. `MICROHABILIDAD`
13. `# ESTACION`
14. `ESTACION`
15. `CONOCIMIENTOS`
16. `CONTENIDO ESPECIFICO EVALUADO`
17. `# MICROHABILIDADES ESPECIFICAS`
18. `MICROHABILIDADES ESPECIFICAS`
19. `PROCESO COGNITIVO`
20. `# MICRO TEST`

## Resumen que devuelve el motor

`generate_sumun_template_from_excel(...)` devuelve:

- bytes del Excel generado
- `SumunTemplateSummary`

Ese resumen incluye:

- prefijo usado
- area
- grado
- nivel
- nombre de hoja de salida
- numero total de filas
- cantidad de macros unicas
- cantidad de micros procesadas
- cantidad de micros unicas
- hojas procesadas
- hojas omitidas
- filas generadas por hoja
- detalle de filas generadas por itinerario
- detalle de filas generadas por conocimientos
- filas donde la estacion fue heredada

## Errores y diagnostico

Si no se generan filas, el sistema no falla con un error generico.
Primero vuelve a inspeccionar las hojas y arma un detalle por hoja.

Mensajes comunes:

- hoja oculta
- no se reconocieron encabezados
- se detecto estructura pero no hay microhabilidades en procesos
- se detecto estructura y microhabilidades, pero la columna `ESTACION` no tiene formato valido

## Limitaciones actuales

- Solo acepta `.xlsx`.
- La deteccion fallback asume estructura A:L.
- La separacion de microhabilidades especificas depende de bloques en blanco o marcadores explicitos.
- Si una estacion no tiene numero ni hay una estacion previa heredable, la fila se descarta.
- El sistema no usa formulas calculadas por Excel; lee el contenido de la celda tal como `openpyxl` lo entrega.

## Resumen practico

En la practica, SUMUN hace esto:

1. abre el Excel
2. detecta que hojas parecen matrices validas
3. identifica columnas base y columnas de procesos
4. recorre cada fila util
5. toma el contexto base: itinerario, competencia, macro, micro, estacion, conocimientos
6. explota cada texto de proceso cognitivo en una fila plana
7. asigna IDs y contadores consistentes
8. construye un nuevo Excel listo para descarga o guardado

Si quieres, puedo tomar esta documentacion y ademas:

- integrarla al `README.md`
- convertirla en comentarios dentro de `sumun.py`
- o hacerte un diagrama mas corto del flujo con ejemplos de entrada y salida
