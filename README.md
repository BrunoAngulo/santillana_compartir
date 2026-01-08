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
Listar/eliminar clases (Pegasus gestion escolar):
```bash
set PEGASUS_TOKEN=tu_token
python main.py clases-api --colegio-id 4230 --ciclo-id 207
python main.py clases-api --colegio-id 4230 --ciclo-id 207 --confirm-delete
```

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
