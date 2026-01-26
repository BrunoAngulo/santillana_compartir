# Santillana_format

Genera la hoja "Plantilla alta de clases" a partir del Excel de detalle.

## Estructura
```
Santillana_format/
|- app.py                    # Interfaz web (Streamlit)
|- main.py                   # CLI (wrapper)
|- santillana_format/
|  |- processor.py           # Logica de carga/filtrado/transformacion/exportacion
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
En la web puedes elegir entre crear clases o depurar alumnos.

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
Listar/eliminar clases (Pegasus gestion escolar):
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

Sincronizar profesores desde un Excel (activar/inactivar y asignar niveles):
```bash
python main.py profesores-sync ruta.xlsx --colegio-id 4230 --token 'tu_token'
```
Opcional, hoja y modo prueba:
```bash
python main.py profesores-sync ruta.xlsx --colegio-id 4230 --sheet Profesores --dry-run
```

Asignar profesores a clases desde un Excel (modo simulacion por defecto):
```bash
python main.py profesores-clases ruta.xlsx --colegio-id 4230 --token 'tu_token'
```
Para aplicar cambios:
```bash
python main.py profesores-clases ruta.xlsx --colegio-id 4230 --token 'tu_token' --apply
```
Para eliminar del staff a profesores que no estan en el Excel (solo clases evaluadas):
```bash
python main.py profesores-clases ruta.xlsx --colegio-id 4230 --token 'tu_token' --apply --remove-missing
```
El Excel puede usar la columna `Clases` o `CURSO`; si hay multiples cursos, separamos por coma.

Depurar alumnos (duplicados):
```bash
python main.py depurar ruta_base.xlsx ruta_nuevo.xlsx
```
Atajo (detecta dos rutas de archivo):
```bash
python main.py ruta_base.xlsx ruta_nuevo.xlsx
```
Por defecto busca:
- base en `alumnos_oldList/`
- nuevo en `alumnos_newList/`
Y genera en `alumnos_registerList/`.

## Flujo de procesamiento
- Carga la hoja `Export` por defecto y detecta encabezados si hace falta.
- Filtra por codigo exacto en la columna `CRM` (configurable).
- Aplica reglas de plataforma/estado y mapea nivel, grado y materia.
- Genera la hoja `Plantilla alta de clases` y respeta los encabezados de la plantilla si existe.
