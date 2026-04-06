# Santillana_format

Herramienta web (Streamlit) para operar flujos Pegasus y Jira Focus desde una sola interfaz.

## Arquitectura (web-only)

```text
Santillana_format/
|-- app.py                         # Punto de entrada unico de la app web
|-- santillana_format/
|   |-- pegasus/                   # Dominio Pegasus (servicios y utilidades)
|   |   |-- __init__.py            # Facade del dominio
|   |   |-- processor.py
|   |   |-- alumnos.py
|   |   |-- alumnos_compare.py
|   |   |-- clases_api.py
|   |   |-- profesores.py
|   |   |-- profesores_clases.py
|   |   |-- profesores_compare.py
|   |   |-- profesores_manual.py
|   |   |-- profesores_password.py
|   |-- jira/                      # Dominio Jira
|   |   |-- __init__.py            # Facade del dominio
|   |   |-- view.py
|   |   |-- jira_focus_web.html
|   |-- richmond/                  # Dominio Richmond Studio
|   |   |-- __init__.py            # Facade del dominio
|   |   |-- view.py
|   |-- loqueleo/                  # Dominio Loqueleo
|   |   |-- __init__.py
|   |   |-- view.py
|   |-- alumnos.py                 # Wrapper de compatibilidad
|   |-- alumnos_compare.py         # Wrapper de compatibilidad
|   |-- clases_api.py              # Wrapper de compatibilidad
|   |-- jira_focus_web.py          # Wrapper de compatibilidad
|   |-- processor.py               # Wrapper de compatibilidad
|   |-- profesores.py              # Wrapper de compatibilidad
|   |-- profesores_clases.py       # Wrapper de compatibilidad
|   |-- profesores_compare.py      # Wrapper de compatibilidad
|   |-- profesores_manual.py       # Wrapper de compatibilidad
|   |-- profesores_password.py     # Wrapper de compatibilidad
|   |-- __init__.py                # Facade general del paquete
|-- requirements.txt
|-- runtime.txt
```

## Patron de diseno aplicado

- Organizacion por dominio: `pegasus`, `jira`, `richmond`, `loqueleo`.
- Facade por dominio: cada carpeta expone un `__init__.py` como punto de entrada estable.
- Wrappers de compatibilidad: los modulos planos antiguos se mantienen como puente para no romper imports durante la migracion.
- `app.py` queda como orquestador de vistas y flujo, no como repositorio definitivo de toda la logica.

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
- Richmond Studio:
  - Gestion y creacion de clases.
  - Alta y administracion de usuarios.
  - Sincronizacion de clases y actualizacion de passwords.
- Jira Focus Web:
  - OAuth con Jira Cloud.
  - Worklogs, timeline, dashboard y reportes.

## Configuracion

- Token Pegasus global por UI o variable de entorno `PEGASUS_TOKEN`.
- Jira Focus usa OAuth y persistencia local del navegador para sesion/config.

## Criterios de estructura

- Entrada unica: `app.py`.
- Logica de negocio agrupada por dominio en `santillana_format/`.
- Sin entrypoints CLI ni paginas Streamlit duplicadas.
