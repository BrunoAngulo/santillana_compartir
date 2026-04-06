"""Facade for the Pegasus domain."""

from .alumnos import (
    DEFAULT_CICLO_ID as ALUMNOS_CICLO_ID_DEFAULT,
    DEFAULT_EMPRESA_ID,
    descargar_plantilla_edicion_masiva,
)
from .alumnos_compare import (
    COMPARE_MODE_AMBOS,
    COMPARE_MODE_APELLIDOS,
    COMPARE_MODE_DNI,
    comparar_plantillas_detalle,
)
from .clases_api import listar_y_mapear_clases
from .processor import (
    CODE_COLUMN_NAME,
    OUTPUT_FILENAME,
    SHEET_NAME,
    process_excel,
)
from .profesores import (
    DEFAULT_CICLO_ID as PROFESORES_CICLO_ID_DEFAULT,
    build_profesores_bd_filename,
    export_profesores_bd_excel,
    export_profesores_excel,
    listar_profesores_bd_data,
    listar_profesores_data,
    listar_profesores_filters_data,
)
from .profesores_clases import asignar_profesores_clases
from .profesores_password import actualizar_passwords_docentes

__all__ = [
    "ALUMNOS_CICLO_ID_DEFAULT",
    "CODE_COLUMN_NAME",
    "COMPARE_MODE_AMBOS",
    "COMPARE_MODE_APELLIDOS",
    "COMPARE_MODE_DNI",
    "DEFAULT_EMPRESA_ID",
    "OUTPUT_FILENAME",
    "PROFESORES_CICLO_ID_DEFAULT",
    "SHEET_NAME",
    "actualizar_passwords_docentes",
    "asignar_profesores_clases",
    "build_profesores_bd_filename",
    "comparar_plantillas_detalle",
    "descargar_plantilla_edicion_masiva",
    "export_profesores_bd_excel",
    "export_profesores_excel",
    "listar_profesores_bd_data",
    "listar_profesores_data",
    "listar_profesores_filters_data",
    "listar_y_mapear_clases",
    "process_excel",
]
