# Santillana_format

Herramienta web (Streamlit) para operar flujos Pegasus y Jira Focus desde una sola interfaz.

## Arquitectura (web-only)

```text
Santillana_format/
|-- app.py                         # Punto de entrada unico de la app web
|-- santillana_format/
|   |-- processor.py               # Logica de clases (carga, transformacion, exportacion)
|   |-- profesores.py              # Consulta y exportacion de profesores
|   |-- profesores_clases.py       # Asignacion de profesores a clases
|   |-- profesores_password.py     # Actualizacion de login/password docentes
|   |-- alumnos.py                 # Descarga de plantilla de alumnos registrados
|   |-- alumnos_compare.py         # Comparacion de plantillas de alumnos
|   |-- jira_focus_web.py          # Render del frontend Jira Focus (HTML embebido)
|   |-- jira_focus_web.html        # Frontend Jira Focus (OAuth/worklogs/reportes)
|   |-- __init__.py
|-- requirements.txt
|-- runtime.txt
```

## Requisitos

- Python 3.10+
- Dependencias: `pip install -r requirements.txt`

## Ejecutar

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
streamlit run app.py
```

## Modulos funcionales en la web

- Procesos Pegasus:
  - Crear clases desde Excel.
  - Listar/asignar profesores.
  - Actualizar password de docentes.
  - Descargar plantilla de alumnos registrados.
  - Comparar plantillas de alumnos.
  - Operaciones de clases API y clases + alumnos.
- Jira Focus Web:
  - OAuth con Jira Cloud.
  - Worklogs, timeline, dashboard y reportes.

## Configuracion

- Token Pegasus global por UI o variable de entorno `PEGASUS_TOKEN`.
- Jira Focus usa OAuth y persistencia local del navegador para sesion/config.

## Criterios de estructura

- Entrada unica: `app.py`.
- Logica de negocio en `santillana_format/`.
- Sin entrypoints CLI ni paginas Streamlit duplicadas.
