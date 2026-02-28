# Santillana_format

Genera la hoja "Plantilla alta de clases" a partir del Excel de detalle y gestiona
profesores/alumnos con Pegasus.

## Estructura
```
Santillana_format/
|- app.py                    # Interfaz web (Streamlit)
|- main.py                   # CLI (wrapper)
|- santillana_format/
|  |- processor.py           # L贸gica de carga/filtrado/transformaci贸n/exportaci贸n
|  |- alumnos.py             # Helpers Pegasus
|  |- cli.py                 # CLI real
|  |- __init__.py
|- requirements.txt
|- PlantillaClases.xlsx      # Plantilla base (opcional)
|- salidas/                  # Archivos generados
|- *.xlsx                    # Insumos de ejemplo
```

## Requisitos
- Python 3.10+
- Dependencias: `pip install -r requirements.txt`

## Ejecutar la web
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
streamlit run app.py
```
En la web puedes:
- Crear clases desde un Excel.
- Listar profesores y generar Excel base.
- Asignar profesores a clases (con opci贸n de simular o aplicar).
- Descargar plantilla de alumnos registrados y comparar Plantilla_BD vs Plantilla_Actualizada.
- Listar y eliminar clases desde el API de gesti贸n escolar.
- Buscar clases asociadas a un alumno por login (token + login + colegio clave).
- Generar Excel de alumnos por nivel/grado/secci贸n usando Censo (nivelesGradosGrupos + alumnos).

## Ejecutar CLI
```bash
python main.py ruta_al_excel.xlsx 00001053
```
Opcional, grupos por letra (A,B,C,D):
```bash
python main.py ruta_al_excel.xlsx 00001053 A,B,C,D
```
Alternativa:
```bash
python -m santillana_format.cli ruta_al_excel.xlsx 00001053
```
Salida por defecto: `salidas/Clases/`.
Listar/eliminar clases (Pegasus gesti贸n escolar):
```bash
set PEGASUS_TOKEN=tu_token
python main.py clases-api --colegio-id 4230 --ciclo-id 207
python main.py clases-api --colegio-id 4230 --ciclo-id 207 --confirm-delete
```

Listar profesores (Pegasus censo) y generar Excel:
```bash
set PEGASUS_TOKEN=tu_token
python main.py profesores --colegio-id 4230 --ciclo-id 207
```
Opcional, filtrar niveles:
```bash
python main.py profesores --colegio-id 4230 --niveles Inicial,Primaria
python main.py profesores --colegio-id 4230 --niveles 38,39
```
Salida por defecto: `salidas/Profesores/`.

Asignar profesores a clases desde un Excel (modo simulaci贸n por defecto):
```bash
python main.py profesores-clases ruta.xlsx --colegio-id 4230 --token 'tu_token'
```
Para aplicar cambios:
```bash
python main.py profesores-clases ruta.xlsx --colegio-id 4230 --token 'tu_token' --apply
```
Para eliminar del staff a profesores que no est谩n en el Excel (solo clases evaluadas):
```bash
python main.py profesores-clases ruta.xlsx --colegio-id 4230 --token 'tu_token' --apply --remove-missing
```
Por defecto, se inactiva por Estado a IDs que est醤 en `Profesores` pero no en `Profesores_clases`.
Para desactivar ese comportamiento:
```bash
python main.py profesores-clases ruta.xlsx --colegio-id 4230 --token 'tu_token' --apply --no-inactivar-no-en-clases
```
El Excel puede usar la columna `Clases` o `CURSO`; si hay m鷏tiples cursos, separamos por coma.

Descargar plantilla de alumnos registrados:
```bash
python main.py alumnos-plantilla --colegio-id 4230 --token 'tu_token'
```
Buscar clases asociadas a un login de alumno:
```bash
python main.py alumno-clases per25-alumno01 --colegio-id 2326 --ciclo-id 207 --token 'tu_token'
```
Opcional, solo coincidencias activas:
```bash
python main.py alumno-clases per25-alumno01 --colegio-id 2326 --token 'tu_token' --solo-activos
```
Comparar Plantilla_BD vs Plantilla_Actualizada:
```bash
python main.py alumnos-comparar ruta.xlsx
```
Salida por defecto: `salidas/Alumnos/alumnos_resultados_<archivo>.xlsx` con hojas
`Plantilla alta de alumnos` y `Plantilla edici贸n masiva`.

## Flujo de procesamiento
- Carga la hoja `Export` por defecto y detecta encabezados si hace falta.
- Filtra por c贸digo exacto en la columna `CRM` (configurable).
- Aplica reglas de plataforma/estado y mapea nivel, grado y materia.
- Genera la hoja `Plantilla alta de clases` y respeta los encabezados de la plantilla si existe.
