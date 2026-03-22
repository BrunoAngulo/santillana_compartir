import os
import re
import tempfile
import threading
import unicodedata
from datetime import date, datetime
from html import escape
from io import BytesIO
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import unquote, urljoin
from uuid import uuid4

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
from st_keyup import st_keyup

from santillana_format.alumnos import (
    DEFAULT_CICLO_ID as ALUMNOS_CICLO_ID_DEFAULT,
    DEFAULT_EMPRESA_ID,
    descargar_plantilla_edicion_masiva,
    parse_id_list,
)
from santillana_format.alumnos_compare import (
    COMPARE_MODE_AMBOS,
    COMPARE_MODE_APELLIDOS,
    COMPARE_MODE_DNI,
    comparar_plantillas_detalle,
)
from santillana_format.processor import (
    CODE_COLUMN_NAME,
    OUTPUT_FILENAME,
    SHEET_NAME,
    process_excel,
)
from santillana_format.jira_focus_web import render_jira_focus_web
from santillana_format.profesores import (
    DEFAULT_CICLO_ID as PROFESORES_CICLO_ID_DEFAULT,
    build_profesores_bd_filename,
    export_profesores_bd_excel,
    export_profesores_excel,
    listar_profesores_bd_data,
    listar_profesores_data,
    listar_profesores_filters_data,
)
from santillana_format.profesores_clases import asignar_profesores_clases
from santillana_format.profesores_password import actualizar_passwords_docentes
from santillana_format.clases_api import listar_y_mapear_clases

PROFESORES_COMPARE_IMPORT_ERROR = ""
try:
    from santillana_format.profesores_compare import (
        build_profesores_base_filename,
        build_profesores_crear_filename,
        build_profesores_reference_catalog,
        compare_profesores_bd_excel,
        compare_profesores_sistema_excel,
        export_profesores_base_excel,
        export_profesores_crear_excel,
        merge_profesores_reference_base_record,
    )
except Exception as exc:  # pragma: no cover - arranque defensivo
    PROFESORES_COMPARE_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

    def _raise_profesores_compare_import_error(*args, **kwargs):
        raise ImportError(PROFESORES_COMPARE_IMPORT_ERROR)

    def build_profesores_base_filename(*args, **kwargs) -> str:
        return "profesores_base.xlsx"

    def build_profesores_crear_filename(*args, **kwargs) -> str:
        return "profesores_crear.xlsx"

    def build_profesores_reference_catalog(*args, **kwargs) -> List[Dict[str, object]]:
        return []

    compare_profesores_bd_excel = _raise_profesores_compare_import_error
    compare_profesores_sistema_excel = _raise_profesores_compare_import_error
    export_profesores_base_excel = _raise_profesores_compare_import_error
    export_profesores_crear_excel = _raise_profesores_compare_import_error
    merge_profesores_reference_base_record = _raise_profesores_compare_import_error

PROFESORES_MANUAL_IMPORT_ERROR = ""
try:
    from santillana_format.profesores_manual import (
        asignar_clases_profesor_manual,
        listar_profesores_clases_panel_data,
    )
except Exception as exc:  # pragma: no cover - arranque defensivo
    PROFESORES_MANUAL_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

    def _raise_profesores_manual_import_error(*args, **kwargs):
        raise ImportError(PROFESORES_MANUAL_IMPORT_ERROR)

    asignar_clases_profesor_manual = _raise_profesores_manual_import_error
    listar_profesores_clases_panel_data = _raise_profesores_manual_import_error


GESTION_ESCOLAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases"
)
GESTION_ESCOLAR_ALUMNOS_CLASE_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases/{clase_id}/alumnos"
)
GESTION_ESCOLAR_CLASE_PARTICIPANTES_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/niveles/{nivel_id}/grados/{grado_id}"
    "/clases/{clase_id}/participantes"
)
CENSO_ALUMNOS_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/alumnos"
)
CENSO_ALUMNOS_BY_FILTERS_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/alumnosByFilters"
)
CENSO_ALUMNOS_CREATE_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/grados/{grado_id}/grupos/{grupo_id}/alumnos"
)
CENSO_ALUMNO_DETALLE_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/grados/{grado_id}/grupos/{grupo_id}/alumnos/{alumno_id}"
)
CENSO_NIVELES_GRADOS_GRUPOS_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/alumnos/nivelesGradosGrupos"
)
CENSO_ALUMNO_ACTIVAR_INACTIVAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/grados/{grado_id}/grupos/{grupo_id}/alumnos/{alumno_id}/activarInactivar"
)
CENSO_ALUMNO_MOVER_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/grados/{grado_id}/grupos/{grupo_id}/alumnos/{alumno_id}/mover"
)
CENSO_ALUMNO_UPDATE_LOGIN_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/grados/{grado_id}/grupos/{grupo_id}/alumnos/{alumno_id}/updateLogin"
)
CENSO_PROFESOR_DETALLE_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/profesores/{persona_id}"
)
CENSO_PROFESOR_UPDATE_LOGIN_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/profesores/{persona_id}/updateLoginProfesor"
)
CENSO_PLANTILLA_EDICION_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/descargarPlantillaEdicionMasiva"
)
DASHBOARD_VALIDAR_IDENTIFICADOR_URL = (
    "https://www.uno-internacional.com/pegasus-api/dashboard/empresas/{empresa_id}"
    "/validarIdentificador"
)
DASHBOARD_VALIDAR_LOGIN_URL = (
    "https://www.uno-internacional.com/pegasus-api/dashboard/empresas/{empresa_id}"
    "/validarLogin"
)
DASHBOARD_COLEGIOS_URL = (
    "https://www.uno-internacional.com/pegasus-api/dashboard/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios"
)
GESTION_ESCOLAR_CICLO_ID_DEFAULT = 207
AUTO_MOVE_SECCION_ORIGEN = "Y"
AUTO_MOVE_MULTI_DEFAULT_SCHOOLS: List[Dict[str, object]] = [
    {"Nombre del colegio": "HENRI LA FONTAINE - Comas", "Clave ID": 4220},
    {"Nombre del colegio": "INDEPENDENCIA - Miraflores", "Clave ID": 9039},
    {"Nombre del colegio": "PANAMERICANA", "Clave ID": 11115},
    {"Nombre del colegio": "LA REPARACION", "Clave ID": 7384},
    {"Nombre del colegio": "NUESTRA SEÑORA DE LA ASUNCION - San Miguel", "Clave ID": 7389},
    {"Nombre del colegio": "SHONA GARCIA VALLE", "Clave ID": 4187},
    {"Nombre del colegio": "SANTA MARÍA DE LA PROVIDENCIA", "Clave ID": 16130},
    {"Nombre del colegio": "JOSE GALVEZ - Callao", "Clave ID": 11114},
    {"Nombre del colegio": "SAN GERARDO - San Juan de Lurigancho", "Clave ID": 5492},
    {"Nombre del colegio": "ALBERTO BENJAMIN SIMPSON - Lince", "Clave ID": 4156},
    {"Nombre del colegio": "SAN MARTIN DE PORRES - Santa Anita", "Clave ID": 4146},
    {"Nombre del colegio": "ATENEO - La Molina", "Clave ID": 4209},
    {"Nombre del colegio": "PARROQUIAL MONSEÑOR MARCOS LIBARDONI", "Clave ID": 6313},
    {"Nombre del colegio": "NUESTRA SEÑORA DEL ROSARIO DE FATIMA - Chaclacayo", "Clave ID": 19953},
    {"Nombre del colegio": "MARÍA MOLINARI - San Borja", "Clave ID": 19701},
    {"Nombre del colegio": "ALBORADA - Lince", "Clave ID": 19713},
    {"Nombre del colegio": "SAN LUCAS - Pueblo Libre", "Clave ID": 13020},
    {"Nombre del colegio": "SAN ANTONIO DE PADUA - Jesus Maria", "Clave ID": 4138},
    {"Nombre del colegio": "SAN JUDAS TADEO - Santa Anita", "Clave ID": 19954},
    {"Nombre del colegio": "EL FUNDAMENTO", "Clave ID": 22804},
    {"Nombre del colegio": "MEDALLA DE MARÍA", "Clave ID": 11117},
    {"Nombre del colegio": "SAN RAFAEL - San Juan de Lurigancho", "Clave ID": 3262},
    {"Nombre del colegio": "NIÑO JESUS MARISCAL CHAPERITO", "Clave ID": 20216},
    {"Nombre del colegio": "SAN VICENTE DE PAÚL - La Molina", "Clave ID": 5512},
    {"Nombre del colegio": "LOS ROSALES DE SANTA ROSA", "Clave ID": 7040},
    {"Nombre del colegio": "JESUALDO (PRIMARIA)", "Clave ID": 11118},
    {"Nombre del colegio": "RICARDO PALMA DE LOS PORTALES", "Clave ID": 16136},
    {"Nombre del colegio": "JESÚS DE NAZARETH - La Victoria", "Clave ID": 26206},
    {"Nombre del colegio": "NUESTRA SEÑORA DE LA RECONCILIACIÓN", "Clave ID": 12245},
    {"Nombre del colegio": "DOMINGO SAVIO - Santiago de Surco", "Clave ID": 25645},
    {"Nombre del colegio": "JESUALDO (SECUNDARIA)", "Clave ID": 26754},
    {"Nombre del colegio": "CRISTO REY - Pueblo Libre", "Clave ID": 16453},
    {"Nombre del colegio": "JUAN JACOBO ROUSSEAU - Santiago de Surco", "Clave ID": 25498},
]
AUTO_MOVE_MULTI_DEFAULT_COLEGIO_IDS = [
    int(row["Clave ID"]) for row in AUTO_MOVE_MULTI_DEFAULT_SCHOOLS
]
AUTO_MOVE_MULTI_DEFAULT_COLEGIO_NAME_BY_ID = {
    int(row["Clave ID"]): str(row["Nombre del colegio"] or "").strip()
    for row in AUTO_MOVE_MULTI_DEFAULT_SCHOOLS
}
# La lista activa del flujo masivo usa todo el catalogo por defecto.
AUTO_MOVE_MULTI_ACTIVE_COLEGIO_IDS = list(AUTO_MOVE_MULTI_DEFAULT_COLEGIO_IDS)
AUTO_MOVE_MULTI_ACTIVE_SCHOOLS = list(AUTO_MOVE_MULTI_DEFAULT_SCHOOLS)
PEGASUS_NIVEL_LABEL_BY_ID = {
    38: "Inicial",
    39: "Primaria",
    40: "Secundaria",
}
RICHMONDSTUDIO_USERS_URL = "https://richmondstudio.global/api/users"
RICHMONDSTUDIO_GROUPS_URL = "https://richmondstudio.global/api/groups"
RICHMONDSTUDIO_CURRENT_USER_URL = "https://richmondstudio.global/api/users/current"
RESTRICTED_SECTIONS_PASSWORD = "Ted2026"
RESTRICTED_SECTIONS_ENABLED = True
JIRA_ADMIN_DISPLAY_NAME = "Bruno Ricardo Adrian Angulo Perez"
JIRA_ADMIN_QUERY_PARAM = "jira_admin"
JIRA_ADMIN_COOKIE_NAME = "jira_focus_admin_access"
JIRA_USER_QUERY_PARAM = "jira_user"
JIRA_USER_COOKIE_NAME = "jira_focus_user_display_name"
JIRA_LOGIN_QUERY_PARAM = "jira_login"
JIRA_LOGIN_COOKIE_NAME = "jira_focus_user_login"
JIRA_UNLOCK_LOGIN = "bangulo@santillana.com"
JIRA_LOGIN_BRIDGE_PENDING = "__pending__"
JIRA_LOGIN_BRIDGE_COMPONENT = components.declare_component(
    "jira_login_bridge",
    path=str(Path(__file__).resolve().parent / "components" / "jira_login_bridge"),
)
PEGASUS_TOKEN_BRIDGE_PENDING = "__pending__"
PEGASUS_TOKEN_BRIDGE_COMPONENT = components.declare_component(
    "pegasus_token_bridge",
    path=str(Path(__file__).resolve().parent / "components" / "pegasus_token_bridge"),
)
RICHMONDSTUDIO_TEST_LEVEL_OPTIONS: List[Tuple[str, str]] = [
    ("lower primary", "lower_primary"),
    ("upper primary", "upper_primary"),
    ("lower secondary", "lower_secondary"),
    ("upper secondary", "upper_secondary"),
]
RICHMONDSTUDIO_TEST_LEVEL_LABELS = [item[0] for item in RICHMONDSTUDIO_TEST_LEVEL_OPTIONS]
RICHMONDSTUDIO_TEST_LEVEL_BY_LABEL = {
    label: value for label, value in RICHMONDSTUDIO_TEST_LEVEL_OPTIONS
}
RICHMONDSTUDIO_TEST_LEVEL_LABEL_BY_VALUE = {
    value: label for label, value in RICHMONDSTUDIO_TEST_LEVEL_OPTIONS
}
RICHMONDSTUDIO_LEVEL_SHORT_BY_VALUE = {
    "preschool": "PRE",
    "preprimary": "PRE",
    "primary": "PRI",
    "secondary": "SEC",
}
RICHMONDSTUDIO_GRADE_OPTIONS: List[Tuple[str, str]] = [
    ("grade12", "2 anos"),
    ("grade13", "3 anos"),
    ("grade14", "4 anos"),
    ("grade15", "5 anos"),
    ("grade1", "1 grado de primaria"),
    ("grade2", "2 grado de primaria"),
    ("grade3", "3 grado de primaria"),
    ("grade4", "4 grado de primaria"),
    ("grade5", "5 grado de primaria"),
    ("grade6", "6 grado de primaria"),
    ("grade7", "1 ano de secundaria"),
    ("grade8", "2 ano de secundaria"),
    ("grade9", "3 ano de secundaria"),
    ("grade10", "4 ano de secundaria"),
    ("grade11", "5 ano de secundaria"),
]
RICHMONDSTUDIO_GRADE_CODE_OPTIONS = [code for code, _label in RICHMONDSTUDIO_GRADE_OPTIONS]
RICHMONDSTUDIO_GRADE_TEXT_BY_CODE = {
    code: label for code, label in RICHMONDSTUDIO_GRADE_OPTIONS
}
RICHMONDSTUDIO_GRADE_OPTION_BY_CODE = {
    code: label for code, label in RICHMONDSTUDIO_GRADE_OPTIONS
}
RICHMONDSTUDIO_GRADE_LABELS = [label for _code, label in RICHMONDSTUDIO_GRADE_OPTIONS]
RICHMONDSTUDIO_GRADE_SUGGESTION_BY_LABEL = {
    label: code for code, label in RICHMONDSTUDIO_GRADE_OPTIONS
}
RICHMONDSTUDIO_USER_IMPORT_LAST_NAME = "Last name* MANDATORY"
RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME = "First name* MANDATORY"
RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME = "Class OPTIONAL"
RICHMONDSTUDIO_USER_IMPORT_EMAIL = "Email* MANDATORY"
RICHMONDSTUDIO_USER_IMPORT_ROLE = "Role* MANDATORY"
RICHMONDSTUDIO_USER_IMPORT_LEVEL = "level"
RICHMONDSTUDIO_USER_LEVEL_OPTIONS = ("preschool", "primary", "secondary", "adult")
_PARTICIPANTES_SYNC_LOCK = threading.Lock()
_PARTICIPANTES_SYNC_JOBS: Dict[str, Dict[str, object]] = {}
_PARTICIPANTES_SYNC_SCOPE_TO_JOB: Dict[Tuple[int, int, int], str] = {}
_PARTICIPANTES_SYNC_STATUS_LIMIT = 12


def _richmondstudio_grade_option_from_code(grade_code: object) -> str:
    code = str(grade_code or "").strip()
    return str(RICHMONDSTUDIO_GRADE_OPTION_BY_CODE.get(code, code)).strip()


def _richmondstudio_grade_code_from_value(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text in RICHMONDSTUDIO_GRADE_CODE_OPTIONS:
        return text
    mapped = str(RICHMONDSTUDIO_GRADE_SUGGESTION_BY_LABEL.get(text, "")).strip()
    if mapped:
        return mapped
    prefix = text.split("|", 1)[0].strip()
    if prefix in RICHMONDSTUDIO_GRADE_CODE_OPTIONS:
        return prefix
    return ""


def _richmondstudio_new_create_row_id() -> str:
    return f"rs-row-{uuid4().hex}"


def _normalize_display_name(value: object) -> str:
    return str(value or "").strip().lower()


def _normalize_login(value: object) -> str:
    return str(value or "").strip().lower()


def _read_browser_jira_login() -> str:
    try:
        browser_value = JIRA_LOGIN_BRIDGE_COMPONENT(
            key="jira_login_bridge_component",
            default=JIRA_LOGIN_BRIDGE_PENDING,
        )
    except Exception:
        return ""
    if str(browser_value or "") == JIRA_LOGIN_BRIDGE_PENDING:
        return JIRA_LOGIN_BRIDGE_PENDING
    return _normalize_login(browser_value)


def _read_browser_pegasus_token(mode: str = "read", value: object = "") -> str:
    try:
        browser_value = PEGASUS_TOKEN_BRIDGE_COMPONENT(
            key="pegasus_token_bridge_component",
            default=PEGASUS_TOKEN_BRIDGE_PENDING,
            mode=str(mode or "read").strip().lower() or "read",
            value=_clean_token_value(value),
        )
    except Exception:
        return ""
    if str(browser_value or "") == PEGASUS_TOKEN_BRIDGE_PENDING:
        return PEGASUS_TOKEN_BRIDGE_PENDING
    return _clean_token_value(browser_value)


def _get_jira_login_candidates() -> Set[str]:
    login_values: Set[str] = set()
    jira_login_query = st.query_params.get(JIRA_LOGIN_QUERY_PARAM, "")
    if isinstance(jira_login_query, list):
        jira_login_query = jira_login_query[0] if jira_login_query else ""
    jira_login_cookie = ""
    try:
        jira_login_cookie = st.context.cookies.get(JIRA_LOGIN_COOKIE_NAME, "") or ""
    except Exception:
        jira_login_cookie = ""
    session_login = st.session_state.get("jira_focus_user_login", "")
    for raw in (jira_login_query, jira_login_cookie, session_login):
        normalized = _normalize_login(unquote(str(raw or "").strip()))
        if normalized:
            login_values.add(normalized)
    return login_values


def _has_unlock_login() -> bool:
    return _normalize_login(JIRA_UNLOCK_LOGIN) in _get_jira_login_candidates()


def _sync_jira_user_identity() -> None:
    jira_login_value = st.query_params.get(JIRA_LOGIN_QUERY_PARAM, "")
    if isinstance(jira_login_value, list):
        jira_login_value = jira_login_value[0] if jira_login_value else ""
    try:
        if not jira_login_value:
            jira_login_value = st.context.cookies.get(JIRA_LOGIN_COOKIE_NAME, "") or ""
    except Exception:
        pass
    jira_login_text = _normalize_login(unquote(str(jira_login_value or "").strip()))
    if jira_login_text:
        st.session_state["jira_focus_user_login"] = jira_login_text


def _clear_restricted_unlock_browser_check() -> None:
    st.session_state.pop("restricted_unlock_browser_check_target", None)


def _handle_restricted_unlock_browser_check(key_suffix: str) -> bool:
    active_target = st.session_state.get("restricted_unlock_browser_check_target")
    if active_target != key_suffix:
        return False

    browser_login = _read_browser_jira_login()
    if browser_login == JIRA_LOGIN_BRIDGE_PENDING:
        st.info("Validando acceso...")
        return True

    _clear_restricted_unlock_browser_check()
    if _normalize_login(browser_login) == _normalize_login(JIRA_UNLOCK_LOGIN):
        st.session_state["jira_focus_user_login"] = _normalize_login(browser_login)
        st.session_state["restricted_sections_unlocked"] = True
        st.rerun()

    _show_restricted_unlock_dialog()
    return True


def _restricted_sections_unlocked() -> bool:
    if not RESTRICTED_SECTIONS_ENABLED:
        return True
    return bool(st.session_state.get("restricted_sections_unlocked", False)) or _has_unlock_login()


@st.dialog("Acceso restringido", width="small")
def _show_restricted_unlock_dialog() -> None:
    if _has_unlock_login():
        st.session_state["restricted_sections_unlocked"] = True
        st.rerun()
    st.markdown("### Ingresar contrasena")
    pwd_unlock = st.text_input(
        "Contrasena",
        type="password",
        key="restricted_sections_unlock_input",
        placeholder="password",
    )
    col_ok, col_cancel = st.columns(2)
    if col_ok.button("Desbloquear", key="restricted_sections_unlock_ok"):
        if str(pwd_unlock or "") == RESTRICTED_SECTIONS_PASSWORD:
            st.session_state["restricted_sections_unlocked"] = True
            st.rerun()
        else:
            st.error("Contrasena incorrecta.")
    if col_cancel.button("Cancelar", key="restricted_sections_unlock_cancel"):
        st.rerun()


def _render_restricted_blur(section_name: str, key_suffix: str) -> None:
    st.warning("Funcion bloqueada. Acceso restringido por contrasena.")
    st.caption(f"{section_name} requiere desbloqueo.")
    if _handle_restricted_unlock_browser_check(key_suffix):
        return
    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        if st.button(
            "Desbloquear funciones restringidas",
            key=f"restricted_unlock_open_{key_suffix}",
            use_container_width=True,
        ):
            if _has_unlock_login():
                st.session_state["restricted_sections_unlocked"] = True
                st.rerun()
            st.session_state["restricted_unlock_browser_check_target"] = key_suffix
            st.rerun()


_sync_jira_user_identity()
if _restricted_sections_unlocked():
    st.session_state["restricted_sections_unlocked"] = True


def _inject_professional_theme() -> None:
    st.markdown(
        """
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css"
        />
        """,
        unsafe_allow_html=True,
    )


def _clean_token_value(token: object) -> str:
    text = str(token or "").strip()
    return re.sub(r"^bearer\s+", "", text, flags=re.IGNORECASE).strip()


def _normalize_school_search_text(value: object) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _clear_shared_colegios_cache(clear_selection: bool = False) -> None:
    for state_key in (
        "shared_colegios_rows",
        "shared_colegios_error",
        "shared_colegios_token_loaded",
    ):
        st.session_state.pop(state_key, None)
    if clear_selection:
        for state_key in ("shared_colegio_selected_id", "shared_colegio_id", "shared_colegio_label"):
            st.session_state.pop(state_key, None)


def _build_shared_colegio_label(row: Dict[str, object]) -> str:
    colegio = str(row.get("colegio") or "").strip() or "Colegio sin nombre"
    colegio_id = str(row.get("colegio_id") or "").strip()
    municipio = str(row.get("municipio") or "").strip()
    estado = str(row.get("estado") or "").strip()
    location = ", ".join(part for part in (municipio, estado) if part).strip()
    if colegio_id and location:
        return f"{colegio} | ID {colegio_id} | {location}"
    if colegio_id:
        return f"{colegio} | ID {colegio_id}"
    return colegio


def _fetch_dashboard_colegios(
    token: str,
    empresa_id: int,
    ciclo_id: int,
    timeout: int = 30,
) -> List[Dict[str, object]]:
    url = DASHBOARD_COLEGIOS_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers, timeout=int(timeout))
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        raise RuntimeError(message or "Respuesta invalida")

    data = payload.get("data") if isinstance(payload, dict) else None
    colegios_raw = data.get("colegios") if isinstance(data, dict) else None
    if not isinstance(colegios_raw, list):
        raise RuntimeError("Campo data.colegios no es lista")

    rows: List[Dict[str, object]] = []
    for item in colegios_raw:
        if not isinstance(item, dict):
            continue
        colegio = item.get("colegio") if isinstance(item.get("colegio"), dict) else {}
        contrato = item.get("contrato") if isinstance(item.get("contrato"), dict) else {}
        municipio = colegio.get("municipio") if isinstance(colegio.get("municipio"), dict) else {}
        estado = municipio.get("estado") if isinstance(municipio.get("estado"), dict) else {}

        colegio_id = colegio.get("colegioId")
        try:
            colegio_id_int = int(colegio_id)
        except (TypeError, ValueError):
            continue

        row = {
            "colegio_id": int(colegio_id_int),
            "colegio": str(colegio.get("colegio") or "").strip(),
            "colegio_clave": str(colegio.get("colegioClave") or "").strip(),
            "sap_id": str(colegio.get("sapId") or "").strip(),
            "crm_id": str(colegio.get("crmId") or "").strip(),
            "payment_code": str(colegio.get("paymentCode") or "").strip(),
            "cliente": str(colegio.get("cliente") or "").strip(),
            "municipio": str(municipio.get("municipio") or "").strip(),
            "estado": str(estado.get("estado") or "").strip(),
            "telefono": str(colegio.get("telefono") or "").strip(),
            "contrato_estatus": str(contrato.get("estatus") or "").strip(),
            "contrato_ciclo": str(contrato.get("cicloEscolar") or "").strip(),
            "quien_paga": str(item.get("quienPaga") or "").strip(),
            "demo": bool(colegio.get("demo")),
            "tiene_configuracion": bool(item.get("tieneConfiguracion")),
        }
        row["label"] = _build_shared_colegio_label(row)
        row["search_text"] = _normalize_school_search_text(
            " ".join(
                [
                    str(row.get("colegio") or ""),
                    str(row.get("colegio_id") or ""),
                    str(row.get("colegio_clave") or ""),
                    str(row.get("sap_id") or ""),
                    str(row.get("crm_id") or ""),
                    str(row.get("payment_code") or ""),
                    str(row.get("cliente") or ""),
                    str(row.get("municipio") or ""),
                    str(row.get("estado") or ""),
                ]
            )
        )
        rows.append(row)

    rows.sort(
        key=lambda row: (
            str(row.get("colegio") or "").upper(),
            int(row.get("colegio_id") or 0),
        )
    )
    return rows


def _sync_shared_colegio_from_select() -> None:
    selected = st.session_state.get("shared_colegio_selected_id")
    if selected in (None, "", "None"):
        st.session_state["shared_colegio_id"] = ""
        st.session_state["shared_colegio_label"] = ""
        return
    try:
        selected_int = int(selected)
    except (TypeError, ValueError):
        st.session_state["shared_colegio_id"] = ""
        st.session_state["shared_colegio_label"] = ""
        return

    st.session_state["shared_colegio_id"] = str(selected_int)
    for row in st.session_state.get("shared_colegios_rows") or []:
        if int(row.get("colegio_id") or 0) == selected_int:
            st.session_state["shared_colegio_label"] = str(row.get("label") or "").strip()
            break


def _ensure_shared_colegios_loaded(
    token: str,
    empresa_id: int,
    ciclo_id: int,
    timeout: int = 30,
) -> None:
    token_clean = _clean_token_value(token)
    loaded_for_token = str(st.session_state.get("shared_colegios_token_loaded") or "")
    if not token_clean:
        _clear_shared_colegios_cache(clear_selection=False)
        return
    if token_clean == loaded_for_token and "shared_colegios_rows" in st.session_state:
        return

    st.session_state["shared_colegios_error"] = ""
    try:
        rows = _fetch_dashboard_colegios(
            token=token_clean,
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
    except Exception as exc:
        st.session_state["shared_colegios_rows"] = []
        st.session_state["shared_colegios_error"] = str(exc)
        st.session_state["shared_colegios_token_loaded"] = token_clean
        return

    st.session_state["shared_colegios_rows"] = rows
    st.session_state["shared_colegios_token_loaded"] = token_clean
    current_raw = str(st.session_state.get("shared_colegio_id") or "").strip()
    current_selected = st.session_state.get("shared_colegio_selected_id")
    valid_ids = {int(row.get("colegio_id") or 0) for row in rows}

    current_id_int: Optional[int] = None
    try:
        if current_raw:
            current_id_int = int(current_raw)
    except ValueError:
        current_id_int = None

    if current_id_int is not None and current_id_int in valid_ids:
        st.session_state["shared_colegio_selected_id"] = int(current_id_int)
        _sync_shared_colegio_from_select()
        return
    if current_selected not in valid_ids:
        st.session_state["shared_colegio_selected_id"] = None
        st.session_state["shared_colegio_id"] = ""
        st.session_state["shared_colegio_label"] = ""


def _sync_shared_token_from_input() -> None:
    old_token = _clean_token_value(st.session_state.get("shared_pegasus_token", ""))
    token_input = _clean_token_value(st.session_state.get("shared_pegasus_token_input", ""))
    st.session_state["shared_pegasus_token"] = token_input
    if token_input != old_token:
        _clear_shared_colegios_cache(clear_selection=not bool(token_input))


st.set_page_config(page_title="Generador de Plantilla", layout="wide")
_inject_professional_theme()
st.markdown("**Menu principal**")
menu_option = st.radio(
    "Menu",
    ["Procesos Pegasus", "Richmond Studio", "Jira Focus Web"],
    horizontal=True,
    label_visibility="collapsed",
    key="main_top_menu",
)
if menu_option == "Jira Focus Web":
    st.markdown(
        """
        <section class="bg-white border border-gray-200 rounded-lg px-4 py-3 mb-3 shadow-sm">
          <div class="text-xs font-semibold uppercase tracking-wider text-blue-700 mb-1">Panel Operativo</div>
          <h1 class="text-2xl font-bold text-gray-900 m-0">Jira Focus Web</h1>
          <p class="text-sm text-gray-600 mt-1 mb-0">
            Seguimiento operativo de tickets, subtareas, etiquetas y worklogs desde una sola vista.
          </p>
        </section>
        """,
        unsafe_allow_html=True,
    )
    render_jira_focus_web()
    st.stop()

if menu_option != "Richmond Studio":
    st.markdown(
        """
        <section class="bg-white border border-gray-200 rounded-lg px-4 py-3 mb-3 shadow-sm">
          <div class="text-xs font-semibold uppercase tracking-wider text-blue-700 mb-1">Panel Operativo</div>
          <h1 class="text-2xl font-bold text-gray-900 m-0">Procesos Pegasus</h1>
          <p class="text-sm text-gray-600 mt-1 mb-0">
            Gestion integrada de clases, profesores y alumnos con ejecucion directa desde web.
          </p>
        </section>
        """,
        unsafe_allow_html=True,
    )
    bridge_mode = str(
        st.session_state.get("shared_pegasus_token_bridge_mode") or "read"
    ).strip().lower() or "read"
    bridge_value = _clean_token_value(
        st.session_state.get("shared_pegasus_token_bridge_value", "")
    )
    browser_pegasus_token = _read_browser_pegasus_token(
        mode=bridge_mode,
        value=bridge_value,
    )
    if bridge_mode != "read":
        st.session_state["shared_pegasus_token_bridge_mode"] = "read"
        st.session_state["shared_pegasus_token_bridge_value"] = ""

    if "shared_pegasus_token" not in st.session_state:
        initial_token = ""
        if browser_pegasus_token not in ("", PEGASUS_TOKEN_BRIDGE_PENDING):
            initial_token = browser_pegasus_token
        else:
            initial_token = _clean_token_value(os.environ.get("PEGASUS_TOKEN", ""))
        st.session_state["shared_pegasus_token"] = initial_token
    elif (
        not _clean_token_value(st.session_state.get("shared_pegasus_token", ""))
        and browser_pegasus_token not in ("", PEGASUS_TOKEN_BRIDGE_PENDING)
    ):
        st.session_state["shared_pegasus_token"] = browser_pegasus_token

    if "shared_pegasus_token_input" not in st.session_state:
        st.session_state["shared_pegasus_token_input"] = str(
            st.session_state.get("shared_pegasus_token", "")
        )
    elif (
        not _clean_token_value(st.session_state.get("shared_pegasus_token_input", ""))
        and browser_pegasus_token not in ("", PEGASUS_TOKEN_BRIDGE_PENDING)
        and _clean_token_value(st.session_state.get("shared_pegasus_token", ""))
        == browser_pegasus_token
    ):
        st.session_state["shared_pegasus_token_input"] = browser_pegasus_token

    st.markdown("**Configuracion global**")
    global_col_token, global_col_colegio = st.columns([2.7, 1.1])
    with global_col_token:
        token_col_input, token_col_save = st.columns([5.1, 1], gap="small")
        with token_col_input:
            st.text_input(
                "Token",
                key="shared_pegasus_token_input",
                help="Acepta token solo o con prefijo Bearer. Guarda primero para cargar la lista de colegios.",
            )
        with token_col_save:
            if st.button("Guardar", key="shared_token_save_btn", use_container_width=True):
                _sync_shared_token_from_input()
                st.session_state["shared_pegasus_token_bridge_mode"] = "write"
                st.session_state["shared_pegasus_token_bridge_value"] = str(
                    st.session_state.get("shared_pegasus_token", "")
                )
                st.rerun()
        if st.session_state.get("shared_pegasus_token"):
            st.caption("Token guardado en sesion y navegador.")
    with global_col_colegio:
        shared_token_current = _clean_token_value(
            str(st.session_state.get("shared_pegasus_token", ""))
        )
        if shared_token_current:
            _ensure_shared_colegios_loaded(
                token=shared_token_current,
                empresa_id=int(DEFAULT_EMPRESA_ID),
                ciclo_id=int(PROFESORES_CICLO_ID_DEFAULT),
                timeout=30,
            )

        colegio_rows_global = st.session_state.get("shared_colegios_rows") or []
        colegio_error_global = str(st.session_state.get("shared_colegios_error") or "").strip()
        row_by_id_global = {
            int(row["colegio_id"]): row
            for row in colegio_rows_global
            if row.get("colegio_id") is not None
        }
        selected_colegio_current = st.session_state.get("shared_colegio_selected_id")
        select_options_global: List[Optional[int]] = [None]
        if selected_colegio_current is not None:
            try:
                selected_colegio_current = int(selected_colegio_current)
            except (TypeError, ValueError):
                selected_colegio_current = None
        if (
            selected_colegio_current is not None
            and selected_colegio_current in row_by_id_global
            and selected_colegio_current
            not in [int(row["colegio_id"]) for row in colegio_rows_global if row.get("colegio_id") is not None]
        ):
            select_options_global.append(int(selected_colegio_current))
        select_options_global.extend(
            [
                int(row["colegio_id"])
                for row in colegio_rows_global
                if row.get("colegio_id") is not None
                and int(row["colegio_id"]) not in select_options_global
            ]
        )
        if selected_colegio_current not in select_options_global:
            st.session_state["shared_colegio_selected_id"] = None
        st.selectbox(
            "Colegio (global)",
            options=select_options_global,
            key="shared_colegio_selected_id",
            on_change=_sync_shared_colegio_from_select,
            format_func=lambda value: (
                "Selecciona un colegio"
                if value in (None, "", "None")
                else str((row_by_id_global.get(int(value)) or {}).get("label") or f"Colegio {value}")
            ),
            disabled=not bool(colegio_rows_global),
            help="Se reutiliza en las funciones que requieren colegio.",
        )

        if colegio_error_global:
            st.caption(f"No se pudo cargar la lista de colegios: {colegio_error_global}")
        elif not shared_token_current:
            st.caption("Guarda un token para cargar tu lista de colegios.")
        elif colegio_rows_global:
            if st.session_state.get("shared_colegio_label"):
                st.caption(str(st.session_state.get("shared_colegio_label")))
            st.caption(
                f"Colegios disponibles: {len(colegio_rows_global)}"
            )
        else:
            st.caption("No se encontraron colegios para este token.")
    tab_crud_clases, tab_crud_profesores, tab_crud_alumnos = st.tabs(
        [
            "CRUD Clases",
            "CRUD Profesores",
            "CRUD Alumnos",
        ]
    )


def _clean_token(token: str) -> str:
    return _clean_token_value(token)


def _get_shared_token() -> str:
    token_saved = _clean_token(str(st.session_state.get("shared_pegasus_token", "")))
    if token_saved:
        return token_saved
    return _clean_token(os.environ.get("PEGASUS_TOKEN", ""))


def _get_richmondstudio_token() -> str:
    for key in ("rs_groups_bearer_token", "rs_bearer_token"):
        token_value = _clean_token(str(st.session_state.get(key, "")))
        if token_value:
            return token_value
    return _clean_token(os.environ.get("RICHMONDSTUDIO_BEARER_TOKEN", ""))


def _richmondstudio_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "Origin": "https://richmondstudio.global",
        "Referer": "https://richmondstudio.global/settings/classes",
        "x-pwa-origin": "browser",
    }


def _richmondstudio_error_detail(payload: object, status_code: int) -> str:
    detail = ""
    if isinstance(payload, dict):
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            first_error = errors[0]
            if isinstance(first_error, dict):
                detail = str(
                    first_error.get("detail")
                    or first_error.get("title")
                    or first_error.get("message")
                    or ""
                ).strip()
        if not detail:
            detail = str(
                payload.get("detail")
                or payload.get("message")
                or payload.get("error_description")
                or ""
            ).strip()
    return detail or f"HTTP {status_code}"


def _richmondstudio_response_error(
    response: requests.Response,
    status_code: int,
    body: object = None,
) -> str:
    detail = _richmondstudio_error_detail(body, status_code)
    if detail and detail != f"HTTP {status_code}":
        return detail
    response_text = str(getattr(response, "text", "") or "").strip()
    if response_text:
        return f"HTTP {status_code}: {response_text[:300]}"
    return f"HTTP {status_code}"


def _fetch_richmondstudio_users(token: str, timeout: int = 30) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    next_url = RICHMONDSTUDIO_USERS_URL
    next_params: Optional[Dict[str, object]] = {"include": "groups", "sort": "firstName"}
    visited_urls = set()
    users: List[Dict[str, object]] = []

    while next_url:
        if next_url in visited_urls:
            break
        visited_urls.add(next_url)
        try:
            response = requests.get(
                next_url,
                headers=headers,
                params=next_params,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Error de red: {exc}") from exc
        next_params = None

        status_code = response.status_code
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

        if not response.ok:
            detail = ""
            if isinstance(payload, dict):
                errors = payload.get("errors")
                if isinstance(errors, list) and errors:
                    first_error = errors[0]
                    if isinstance(first_error, dict):
                        detail = str(
                            first_error.get("detail")
                            or first_error.get("title")
                            or ""
                        ).strip()
                if not detail:
                    detail = str(payload.get("message") or "").strip()
            raise RuntimeError(detail or f"HTTP {status_code}")

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            raise RuntimeError("Respuesta invalida: campo data no es lista.")
        for item in data:
            if isinstance(item, dict):
                users.append(item)

        next_candidate = None
        if isinstance(payload, dict):
            links = payload.get("links")
            if isinstance(links, dict):
                next_candidate = links.get("next")
                if isinstance(next_candidate, dict):
                    next_candidate = next_candidate.get("href")

        if isinstance(next_candidate, str) and next_candidate.strip():
            next_url = urljoin(RICHMONDSTUDIO_USERS_URL, next_candidate.strip())
        else:
            next_url = ""

    return users


def _fetch_richmondstudio_groups(
    token: str, timeout: int = 30, include_users: bool = False
) -> List[Dict[str, object]]:
    headers = _richmondstudio_headers(token)
    next_url = RICHMONDSTUDIO_GROUPS_URL
    next_params: Optional[Dict[str, object]] = {"include": "users"} if include_users else None
    visited_urls = set()
    groups: List[Dict[str, object]] = []

    while next_url:
        if next_url in visited_urls:
            break
        visited_urls.add(next_url)
        try:
            response = requests.get(
                next_url,
                headers=headers,
                params=next_params,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Error de red: {exc}") from exc
        next_params = None

        status_code = response.status_code
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

        if not response.ok:
            raise RuntimeError(_richmondstudio_error_detail(payload, status_code))

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            raise RuntimeError("Respuesta invalida: campo data no es lista.")
        for item in data:
            if isinstance(item, dict):
                groups.append(item)

        next_candidate = None
        if isinstance(payload, dict):
            links = payload.get("links")
            if isinstance(links, dict):
                next_candidate = links.get("next")
                if isinstance(next_candidate, dict):
                    next_candidate = next_candidate.get("href")

        if isinstance(next_candidate, str) and next_candidate.strip():
            next_url = urljoin(RICHMONDSTUDIO_GROUPS_URL, next_candidate.strip())
        else:
            next_url = ""

    return groups


def _create_richmondstudio_group(
    token: str, payload: Dict[str, object], timeout: int = 30
) -> Dict[str, object]:
    try:
        response = requests.post(
            RICHMONDSTUDIO_GROUPS_URL,
            headers=_richmondstudio_headers(token),
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = None

    if not response.ok:
        raise RuntimeError(_richmondstudio_response_error(response, status_code, body))
    if body is None:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})")
    if not isinstance(body, dict):
        raise RuntimeError("Respuesta invalida al crear clase en RS.")
    return body


def _update_richmondstudio_group(
    token: str, group_id: str, payload: Dict[str, object], timeout: int = 30
) -> Dict[str, object]:
    try:
        response = requests.put(
            f"{RICHMONDSTUDIO_GROUPS_URL}/{group_id}",
            headers=_richmondstudio_headers(token),
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    if not response.content:
        if not response.ok:
            raise RuntimeError(f"HTTP {status_code}")
        return {}

    try:
        body = response.json()
    except ValueError:
        body = None

    if not response.ok:
        raise RuntimeError(_richmondstudio_response_error(response, status_code, body))
    if body is None:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})")
    if not isinstance(body, dict):
        return {}
    return body


def _delete_richmondstudio_group(token: str, group_id: str, timeout: int = 30) -> None:
    try:
        response = requests.delete(
            f"{RICHMONDSTUDIO_GROUPS_URL}/{group_id}",
            headers=_richmondstudio_headers(token),
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    if response.ok:
        return

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = None
    raise RuntimeError(_richmondstudio_response_error(response, status_code, body))


def _normalize_richmondstudio_import_column(value: object) -> str:
    text = _normalize_plain_text(value)
    return re.sub(r"[^A-Z0-9]+", "", text)


def _richmondstudio_user_import_template_rows() -> List[Dict[str, str]]:
    return [
        {
            RICHMONDSTUDIO_USER_IMPORT_LAST_NAME: "ATOCHE LEVANO",
            RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME: "ADRIEL AMIR",
            RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME: "2025 - 1S Blanca",
            RICHMONDSTUDIO_USER_IMPORT_EMAIL: "78156540@boniffatti.edu.pe",
            RICHMONDSTUDIO_USER_IMPORT_ROLE: "Student",
            RICHMONDSTUDIO_USER_IMPORT_LEVEL: "secondary",
        }
    ]


def _resolve_richmondstudio_import_column(
    normalized_columns: Dict[str, str],
    aliases: List[str],
    required: bool = True,
) -> str:
    for alias in aliases:
        match = normalized_columns.get(_normalize_richmondstudio_import_column(alias))
        if match:
            return match
    if required:
        raise ValueError(
            "Falta la columna requerida: {col}.".format(col=aliases[0])
        )
    return ""


def _load_richmondstudio_user_rows_from_excel(excel_bytes: bytes) -> List[Dict[str, object]]:
    try:
        df = pd.read_excel(BytesIO(excel_bytes), dtype=str)
    except Exception as exc:
        raise ValueError(f"No se pudo leer el Excel: {exc}") from exc

    normalized_columns = {
        _normalize_richmondstudio_import_column(column): str(column)
        for column in list(df.columns)
    }
    last_name_column = _resolve_richmondstudio_import_column(
        normalized_columns,
        [
            RICHMONDSTUDIO_USER_IMPORT_LAST_NAME,
            "Last name",
            "Apellido",
            "Apellidos",
        ],
        required=True,
    )
    first_name_column = _resolve_richmondstudio_import_column(
        normalized_columns,
        [
            RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME,
            "First name",
            "Nombre",
            "Nombres",
        ],
        required=True,
    )
    class_column = _resolve_richmondstudio_import_column(
        normalized_columns,
        [
            RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME,
            "Class",
            "Class name",
            "Clase",
            "Grupo",
        ],
        required=False,
    )
    email_column = _resolve_richmondstudio_import_column(
        normalized_columns,
        [
            RICHMONDSTUDIO_USER_IMPORT_EMAIL,
            "Email",
            "Correo",
            "Correo electronico",
        ],
        required=True,
    )
    role_column = _resolve_richmondstudio_import_column(
        normalized_columns,
        [
            RICHMONDSTUDIO_USER_IMPORT_ROLE,
            "Role",
            "Rol",
        ],
        required=True,
    )
    level_column = _resolve_richmondstudio_import_column(
        normalized_columns,
        [
            RICHMONDSTUDIO_USER_IMPORT_LEVEL,
            "Level",
            "Nivel",
        ],
        required=True,
    )

    rows: List[Dict[str, object]] = []
    for idx, item in enumerate(df.fillna("").to_dict("records"), start=2):
        if not isinstance(item, dict):
            continue
        normalized_row = {
            RICHMONDSTUDIO_USER_IMPORT_LAST_NAME: str(item.get(last_name_column) or "").strip(),
            RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME: str(item.get(first_name_column) or "").strip(),
            RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME: str(item.get(class_column) or "").strip()
            if class_column
            else "",
            RICHMONDSTUDIO_USER_IMPORT_EMAIL: str(item.get(email_column) or "").strip(),
            RICHMONDSTUDIO_USER_IMPORT_ROLE: str(item.get(role_column) or "").strip(),
            RICHMONDSTUDIO_USER_IMPORT_LEVEL: str(item.get(level_column) or "").strip(),
            "_row_number": int(idx),
        }
        values = [
            str(normalized_row.get(RICHMONDSTUDIO_USER_IMPORT_LAST_NAME) or "").strip(),
            str(normalized_row.get(RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME) or "").strip(),
            str(normalized_row.get(RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME) or "").strip(),
            str(normalized_row.get(RICHMONDSTUDIO_USER_IMPORT_EMAIL) or "").strip(),
            str(normalized_row.get(RICHMONDSTUDIO_USER_IMPORT_ROLE) or "").strip(),
            str(normalized_row.get(RICHMONDSTUDIO_USER_IMPORT_LEVEL) or "").strip(),
        ]
        if not any(values):
            continue
        rows.append(normalized_row)

    if not rows:
        raise ValueError("El Excel no tiene filas con datos para procesar.")
    return rows


def _normalize_richmondstudio_user_role(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Falta Role.")
    normalized = _normalize_compare_text(raw)
    mapping = {
        "STUDENT": "student",
        "ESTUDIANTE": "student",
        "ALUMNO": "student",
        "TEACHER": "teacher",
        "DOCENTE": "teacher",
        "PROFESOR": "teacher",
        "MAESTRO": "teacher",
    }
    role = mapping.get(normalized, "")
    if not role:
        raise ValueError(f"Role invalido: {raw}. Usa Student o Teacher.")
    return role


def _normalize_richmondstudio_user_level(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Falta level.")
    normalized = _normalize_compare_text(raw)
    mapping = {
        "PRESCHOOL": "preschool",
        "PRESCHOOLER": "preschool",
        "PREPRIMARY": "preschool",
        "PRESCOLAR": "preschool",
        "PREESCOLAR": "preschool",
        "INICIAL": "preschool",
        "KINDER": "preschool",
        "PRIMARY": "primary",
        "PRIMARIA": "primary",
        "SECONDARY": "secondary",
        "SECUNDARIA": "secondary",
        "ADULT": "adult",
        "ADULTS": "adult",
        "ADULTO": "adult",
        "ADULTOS": "adult",
    }
    level = mapping.get(normalized, "")
    if not level:
        raise ValueError(
            f"level invalido: {raw}. Usa preschool, primary, secondary o adult."
        )
    return level


def _build_richmondstudio_groups_lookup(
    groups: List[Dict[str, object]]
) -> Dict[str, Dict[str, object]]:
    by_id: Dict[str, Dict[str, object]] = {}
    by_name: Dict[str, List[Dict[str, object]]] = {}

    for item in groups:
        if not isinstance(item, dict):
            continue
        group_id = str(item.get("id") or "").strip()
        attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
        class_name = str(attrs.get("name") or attrs.get("description") or "").strip()
        group_code = str(attrs.get("code") or "").strip()
        if not group_id or not class_name:
            continue
        meta = {
            "id": group_id,
            "class_name": class_name,
            "code": group_code,
        }
        by_id[group_id] = meta
        name_key = _normalize_compare_text(class_name)
        if name_key:
            by_name.setdefault(name_key, []).append(meta)

    return {
        "by_id": by_id,
        "by_name": by_name,
    }


def _resolve_richmondstudio_group_for_user_row(
    class_value: object,
    groups_lookup: Dict[str, Dict[str, object]],
) -> Optional[Dict[str, object]]:
    raw = str(class_value or "").strip()
    if not raw:
        return None

    group_by_id = groups_lookup.get("by_id") if isinstance(groups_lookup.get("by_id"), dict) else {}
    group_by_name = groups_lookup.get("by_name") if isinstance(groups_lookup.get("by_name"), dict) else {}

    by_id_match = group_by_id.get(raw)
    if isinstance(by_id_match, dict):
        return by_id_match

    name_key = _normalize_compare_text(raw)
    matches = group_by_name.get(name_key) if isinstance(group_by_name, dict) else None
    if not isinstance(matches, list) or not matches:
        raise ValueError(f"Clase no encontrada en RS: {raw}")
    if len(matches) > 1:
        raise ValueError(f"Clase ambigua en RS: {raw}")
    match = matches[0]
    return dict(match) if isinstance(match, dict) else None


def _build_richmondstudio_user_payload(
    row: Dict[str, object],
    group_meta: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    last_name = str(row.get(RICHMONDSTUDIO_USER_IMPORT_LAST_NAME) or "").strip()
    first_name = str(row.get(RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME) or "").strip()
    email = str(row.get(RICHMONDSTUDIO_USER_IMPORT_EMAIL) or "").strip()
    role = _normalize_richmondstudio_user_role(row.get(RICHMONDSTUDIO_USER_IMPORT_ROLE))
    level = _normalize_richmondstudio_user_level(
        row.get(RICHMONDSTUDIO_USER_IMPORT_LEVEL)
    )

    if not last_name:
        raise ValueError("Falta Last name.")
    if not first_name:
        raise ValueError("Falta First name.")
    if not email:
        raise ValueError("Falta Email.")
    if "@" not in email:
        raise ValueError(f"Email invalido: {email}")

    payload: Dict[str, object] = {
        "data": {
            "type": "users",
            "attributes": {
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "role": role,
                "level": level,
            },
            "relationships": {
                "groups": {
                    "data": [],
                }
            },
        }
    }

    group_id = str(group_meta.get("id") or "").strip() if isinstance(group_meta, dict) else ""
    if group_id:
        payload["data"]["relationships"]["groups"]["data"] = [
            {
                "type": "groups",
                "id": group_id,
            }
        ]
    return payload


def _create_richmondstudio_user(
    token: str, payload: Dict[str, object], timeout: int = 30
) -> Dict[str, object]:
    try:
        response = requests.post(
            RICHMONDSTUDIO_USERS_URL,
            headers=_richmondstudio_headers(token),
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = None

    if not response.ok:
        raise RuntimeError(_richmondstudio_response_error(response, status_code, body))
    if body is None:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})")
    if not isinstance(body, dict):
        raise RuntimeError("Respuesta invalida al crear usuario en RS.")
    return body


def _fetch_richmondstudio_user_detail(
    token: str,
    user_id: str,
    timeout: int = 30,
) -> Dict[str, object]:
    user_id_txt = str(user_id or "").strip()
    if not user_id_txt:
        raise ValueError("Falta user_id de RS.")
    try:
        response = requests.get(
            f"{RICHMONDSTUDIO_USERS_URL}/{user_id_txt}",
            headers=_richmondstudio_headers(token),
            params={"include": "groups,subscriptions"},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = None

    if not response.ok:
        raise RuntimeError(_richmondstudio_response_error(response, status_code, body))
    if body is None:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})")
    if not isinstance(body, dict):
        raise RuntimeError("Respuesta invalida al consultar usuario en RS.")
    return body


def _update_richmondstudio_user(
    token: str,
    user_id: str,
    payload: Dict[str, object],
    timeout: int = 30,
) -> Dict[str, object]:
    user_id_txt = str(user_id or "").strip()
    if not user_id_txt:
        raise ValueError("Falta user_id de RS.")
    try:
        response = requests.put(
            f"{RICHMONDSTUDIO_USERS_URL}/{user_id_txt}",
            headers=_richmondstudio_headers(token),
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        body = response.json() if response.content else {}
    except ValueError:
        body = None

    if not response.ok:
        raise RuntimeError(_richmondstudio_response_error(response, status_code, body))
    if body is None:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})")
    if not isinstance(body, dict):
        raise RuntimeError("Respuesta invalida al actualizar usuario en RS.")
    return body


def _patch_richmondstudio_user(
    token: str,
    user_id: str,
    payload: Dict[str, object],
    timeout: int = 30,
) -> Dict[str, object]:
    user_id_txt = str(user_id or "").strip()
    if not user_id_txt:
        raise ValueError("Falta user_id de RS.")
    try:
        response = requests.patch(
            f"{RICHMONDSTUDIO_USERS_URL}/{user_id_txt}",
            headers=_richmondstudio_headers(token),
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        body = response.json() if response.content else {}
    except ValueError:
        body = None

    if not response.ok:
        raise RuntimeError(_richmondstudio_response_error(response, status_code, body))
    if body is None:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})")
    if not isinstance(body, dict):
        raise RuntimeError("Respuesta invalida al actualizar usuario en RS.")
    return body


def _richmondstudio_relationship_ids(
    resource: Dict[str, object],
    relationship_name: str,
) -> List[str]:
    relationships = (
        resource.get("relationships")
        if isinstance(resource.get("relationships"), dict)
        else {}
    )
    relation = (
        relationships.get(relationship_name)
        if isinstance(relationships.get(relationship_name), dict)
        else {}
    )
    relation_data = relation.get("data") if isinstance(relation.get("data"), list) else []
    ids: List[str] = []
    seen = set()
    for item in relation_data:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id") or "").strip()
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        ids.append(item_id)
    return ids


def _richmondstudio_parse_year(value: object) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(text).year)
    except ValueError:
        match = re.match(r"^(\d{4})-", text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
    return None


def _richmondstudio_subscription_rows_from_detail(
    detail_body: Dict[str, object],
) -> List[Dict[str, str]]:
    included = detail_body.get("included") if isinstance(detail_body.get("included"), list) else []
    rows: List[Dict[str, str]] = []
    for item in included:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").strip() != "subscriptions":
            continue
        attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
        rows.append(
            {
                "id": str(item.get("id") or "").strip(),
                "created_at": str(attrs.get("createdAt") or "").strip(),
                "expiration_date": str(attrs.get("expirationDate") or "").strip(),
                "product_name": str(attrs.get("productName") or "").strip(),
            }
        )
    return rows


def _richmondstudio_subscription_ids_by_year(
    detail_body: Dict[str, object],
    year: int,
) -> List[str]:
    ids: List[str] = []
    seen: Set[str] = set()
    for row in _richmondstudio_subscription_rows_from_detail(detail_body):
        subscription_id = str(row.get("id") or "").strip()
        if not subscription_id or subscription_id in seen:
            continue
        created_year = _richmondstudio_parse_year(row.get("created_at"))
        if created_year != int(year):
            continue
        seen.add(subscription_id)
        ids.append(subscription_id)
    return ids


def _build_richmondstudio_user_patch_payload_from_detail(
    detail_body: Dict[str, object],
    subscription_ids: Sequence[object],
) -> Dict[str, object]:
    data = detail_body.get("data") if isinstance(detail_body.get("data"), dict) else {}
    attrs = data.get("attributes") if isinstance(data.get("attributes"), dict) else {}
    user_id = str(data.get("id") or "").strip()
    if not user_id:
        raise ValueError("No se encontro el user_id en el detalle RS.")

    first_name = str(attrs.get("firstName") or attrs.get("first_name") or "").strip()
    last_name = str(attrs.get("lastName") or attrs.get("last_name") or "").strip()
    email = str(attrs.get("email") or attrs.get("identifier") or "").strip()
    role = str(attrs.get("role") or "").strip().lower()

    if not first_name:
        raise ValueError(f"Falta first_name en el detalle del usuario {user_id}.")
    if not last_name:
        raise ValueError(f"Falta last_name en el detalle del usuario {user_id}.")
    if not email:
        raise ValueError(f"Falta email en el detalle del usuario {user_id}.")
    if not role:
        raise ValueError(f"Falta role en el detalle del usuario {user_id}.")

    group_ids = _richmondstudio_relationship_ids(data, "groups")
    normalized_subscription_ids: List[str] = []
    seen_subscription_ids = set()
    for item in subscription_ids or []:
        subscription_id = str(item or "").strip()
        if not subscription_id or subscription_id in seen_subscription_ids:
            continue
        seen_subscription_ids.add(subscription_id)
        normalized_subscription_ids.append(subscription_id)

    return {
        "data": {
            "type": "users",
            "id": user_id,
            "attributes": {
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "role": role,
            },
            "relationships": {
                "groups": {
                    "data": [
                        {"type": "groups", "id": group_id}
                        for group_id in group_ids
                    ]
                },
                "subscriptions": {
                    "data": [
                        {"type": "subscriptions", "id": subscription_id}
                        for subscription_id in normalized_subscription_ids
                    ]
                },
            },
        }
    }


def _remove_richmondstudio_subscriptions_2025_for_multiclass_students(
    token: str,
    rows: List[Dict[str, object]],
    timeout: int = 30,
    target_year: int = 2025,
) -> Tuple[Dict[str, int], List[Dict[str, str]]]:
    summary = {
        "eligible_total": 0,
        "processed_total": 0,
        "updated_total": 0,
        "skipped_total": 0,
        "error_total": 0,
        "removed_total": 0,
    }
    result_rows: List[Dict[str, str]] = []

    eligible_rows = [
        row
        for row in rows
        if int(_safe_int(row.get("CLASSES COUNT")) or 0) > 2
    ]
    summary["eligible_total"] = int(len(eligible_rows))

    for row in eligible_rows:
        user_id = str(row.get("RS USER ID") or "").strip()
        student_name = str(row.get("STUDENT NAME") or "").strip()
        identifier = str(row.get("IDENTIFIER") or "").strip()
        classes_count = int(_safe_int(row.get("CLASSES COUNT")) or 0)

        result_row = {
            "RS USER ID": user_id,
            "STUDENT NAME": student_name,
            "IDENTIFIER": identifier,
            "CLASSES COUNT": str(classes_count),
            "TOTAL SUBSCRIPTIONS": "0",
            f"SUBSCRIPTIONS {target_year}": "0",
            "REMOVED SUBSCRIPTIONS": "0",
            "STATUS": "",
            "DETAIL": "",
        }

        if not user_id:
            summary["error_total"] += 1
            result_row["STATUS"] = "ERROR"
            result_row["DETAIL"] = "Falta RS USER ID."
            result_rows.append(result_row)
            continue

        try:
            detail_body = _fetch_richmondstudio_user_detail(
                token=token,
                user_id=user_id,
                timeout=int(timeout),
            )
            data = detail_body.get("data") if isinstance(detail_body.get("data"), dict) else {}
            current_subscription_ids = _richmondstudio_relationship_ids(
                data,
                "subscriptions",
            )
            subscription_ids_2025 = _richmondstudio_subscription_ids_by_year(
                detail_body,
                year=int(target_year),
            )
            keep_subscription_ids = [
                subscription_id
                for subscription_id in current_subscription_ids
                if subscription_id not in set(subscription_ids_2025)
            ]
            result_row["TOTAL SUBSCRIPTIONS"] = str(len(current_subscription_ids))
            result_row[f"SUBSCRIPTIONS {target_year}"] = str(len(subscription_ids_2025))
            summary["processed_total"] += 1

            if not subscription_ids_2025:
                summary["skipped_total"] += 1
                result_row["STATUS"] = "SIN CAMBIOS"
                result_row["DETAIL"] = f"No tiene suscripciones {target_year}."
                result_rows.append(result_row)
                continue

            payload = _build_richmondstudio_user_patch_payload_from_detail(
                detail_body,
                subscription_ids=keep_subscription_ids,
            )
            _patch_richmondstudio_user(
                token=token,
                user_id=user_id,
                payload=payload,
                timeout=int(timeout),
            )
            removed_count = len(subscription_ids_2025)
            summary["updated_total"] += 1
            summary["removed_total"] += int(removed_count)
            result_row["REMOVED SUBSCRIPTIONS"] = str(removed_count)
            result_row["STATUS"] = "ACTUALIZADO"
            result_row["DETAIL"] = f"Se removieron {removed_count} suscripciones {target_year}."
        except Exception as exc:
            summary["error_total"] += 1
            result_row["STATUS"] = "ERROR"
            result_row["DETAIL"] = str(exc)

        result_rows.append(result_row)

    result_rows = sorted(
        result_rows,
        key=lambda item: (
            str(item.get("STATUS") or "").upper() != "ERROR",
            -int(_safe_int(item.get("REMOVED SUBSCRIPTIONS")) or 0),
            str(item.get("STUDENT NAME") or "").upper(),
        ),
    )
    return summary, result_rows


def _richmondstudio_group_label(group_meta: Optional[Dict[str, object]]) -> str:
    if not isinstance(group_meta, dict):
        return ""
    class_name = str(group_meta.get("class_name") or "").strip()
    code = str(group_meta.get("code") or "").strip()
    if class_name and code:
        return f"{class_name} | {code}"
    return class_name or code or str(group_meta.get("id") or "").strip()


def _normalize_richmondstudio_teacher_row(
    user_item: Dict[str, object],
    groups_lookup: Dict[str, Dict[str, object]],
) -> Optional[Dict[str, object]]:
    if not isinstance(user_item, dict):
        return None
    user_id = str(user_item.get("id") or "").strip()
    attrs = (
        user_item.get("attributes")
        if isinstance(user_item.get("attributes"), dict)
        else {}
    )
    role = str(attrs.get("role") or "").strip().lower()
    if role != "teacher":
        return None

    first_name = str(
        attrs.get("firstName")
        or attrs.get("first_name")
        or ""
    ).strip()
    last_name = str(
        attrs.get("lastName")
        or attrs.get("last_name")
        or ""
    ).strip()
    email = str(
        attrs.get("email")
        or attrs.get("identifier")
        or ""
    ).strip()
    teachermatic = bool(attrs.get("teachermatic", False))
    group_ids = _richmondstudio_relationship_ids(user_item, "groups")
    group_by_id = groups_lookup.get("by_id") if isinstance(groups_lookup.get("by_id"), dict) else {}
    group_labels = []
    for group_id in group_ids:
        label = _richmondstudio_group_label(group_by_id.get(group_id))
        if not label:
            label = group_id
        if label not in group_labels:
            group_labels.append(label)

    full_name = " ".join(part for part in (first_name, last_name) if part).strip()
    if not full_name:
        full_name = email or user_id or "Docente sin nombre"

    grupos_txt = ", ".join(group_labels[:4])
    if len(group_labels) > 4:
        grupos_txt = f"{grupos_txt}, +{len(group_labels) - 4}"

    return {
        "ID": user_id,
        "Docente": full_name,
        "First name": first_name,
        "Last name": last_name,
        "Email": email,
        "Role": role,
        "Teachermatic": teachermatic,
        "Grupos": len(group_ids),
        "Clases": grupos_txt,
        "_group_ids": group_ids,
    }


def _build_richmondstudio_teacher_payload(
    first_name: object,
    last_name: object,
    email: object,
    group_ids: Sequence[object],
    user_id: object = "",
    teachermatic: Optional[bool] = None,
    subscription_ids: Optional[Sequence[object]] = None,
) -> Dict[str, object]:
    first_name_txt = str(first_name or "").strip()
    last_name_txt = str(last_name or "").strip()
    email_txt = str(email or "").strip()
    user_id_txt = str(user_id or "").strip()

    if not first_name_txt:
        raise ValueError("Falta First name.")
    if not last_name_txt:
        raise ValueError("Falta Last name.")
    if not email_txt:
        raise ValueError("Falta Email.")
    if "@" not in email_txt:
        raise ValueError(f"Email invalido: {email_txt}")

    normalized_group_ids: List[str] = []
    seen_group_ids = set()
    for item in group_ids or []:
        group_id = str(item or "").strip()
        if not group_id or group_id in seen_group_ids:
            continue
        seen_group_ids.add(group_id)
        normalized_group_ids.append(group_id)

    payload: Dict[str, object] = {
        "data": {
            "type": "users",
            "attributes": {
                "first_name": first_name_txt,
                "last_name": last_name_txt,
                "email": email_txt,
                "role": "teacher",
            },
            "relationships": {
                "groups": {
                    "data": [
                        {"type": "groups", "id": group_id}
                        for group_id in normalized_group_ids
                    ]
                }
            },
        }
    }

    if user_id_txt:
        payload["data"]["id"] = user_id_txt
        payload["data"]["attributes"]["teachermatic"] = bool(teachermatic)
        normalized_subscription_ids: List[str] = []
        seen_subscription_ids = set()
        for item in subscription_ids or []:
            subscription_id = str(item or "").strip()
            if not subscription_id or subscription_id in seen_subscription_ids:
                continue
            seen_subscription_ids.add(subscription_id)
            normalized_subscription_ids.append(subscription_id)
        payload["data"]["relationships"]["subscriptions"] = {
            "data": [
                {"type": "subscriptions", "id": subscription_id}
                for subscription_id in normalized_subscription_ids
            ]
        }

    return payload


def _load_richmondstudio_teacher_panel_data(
    token: str,
    timeout: int = 30,
) -> Dict[str, object]:
    users = _fetch_richmondstudio_users(token, timeout=timeout)
    groups = _fetch_richmondstudio_groups(
        token,
        timeout=timeout,
        include_users=False,
    )
    groups_lookup = _build_richmondstudio_groups_lookup(groups)

    teacher_rows = []
    for item in users:
        normalized = _normalize_richmondstudio_teacher_row(item, groups_lookup)
        if isinstance(normalized, dict):
            teacher_rows.append(normalized)

    teacher_rows = sorted(
        teacher_rows,
        key=lambda row: (
            str(row.get("Docente") or "").upper(),
            str(row.get("Email") or "").upper(),
        ),
    )
    return {
        "rows": teacher_rows,
        "groups": groups,
        "groups_lookup": groups_lookup,
    }


def _build_richmondstudio_registered_listing_data(
    rs_users: List[Dict[str, object]],
    rs_groups: List[Dict[str, object]],
) -> Dict[str, object]:
    allowed_roles = {"student", "teacher"}
    excluded_roles: Dict[str, int] = {}
    filtered_users: List[Dict[str, object]] = []
    group_lookup: Dict[str, Dict[str, str]] = {}

    for group_item in rs_groups:
        group_id = str(group_item.get("id") or "").strip()
        if not group_id:
            continue
        attrs = (
            group_item.get("attributes")
            if isinstance(group_item.get("attributes"), dict)
            else {}
        )
        group_lookup[group_id] = {
            "class_name": str(
                attrs.get("name") or attrs.get("description") or ""
            ).strip(),
            "class_code": str(attrs.get("code") or "").strip(),
        }

    registered_rows: List[Dict[str, str]] = []
    multi_class_students_rows: List[Dict[str, str]] = []

    for item in rs_users:
        attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
        role = str(attrs.get("role") or "").strip().lower()
        if role not in allowed_roles:
            role_key = role or "sin_rol"
            excluded_roles[role_key] = int(excluded_roles.get(role_key, 0)) + 1
            continue

        filtered_users.append(item)
        first_name = str(attrs.get("firstName") or "").strip()
        last_name = str(attrs.get("lastName") or "").strip()
        student_name = " ".join(part for part in [first_name, last_name] if part).strip()
        identifier = str(attrs.get("identifier") or "").strip()
        email = str(attrs.get("email") or "").strip()
        created_at = _richmondstudio_date_display(attrs.get("createdAt"))
        last_sign_in_at = _richmondstudio_date_display(attrs.get("lastSignInAt"))
        user_id = str(item.get("id") or "").strip()

        group_ids: List[str] = []
        seen_group_ids = set()
        for rel in _richmondstudio_relationship_ids(item, "groups"):
            group_id = str(rel or "").strip()
            if not group_id or group_id in seen_group_ids:
                continue
            seen_group_ids.add(group_id)
            group_ids.append(group_id)

        class_names: List[str] = []
        class_codes: List[str] = []
        for group_id in group_ids:
            group_meta = group_lookup.get(group_id) or {}
            class_name = str(group_meta.get("class_name") or "").strip()
            class_code = str(group_meta.get("class_code") or "").strip()
            if class_name and class_name not in class_names:
                class_names.append(class_name)
            if class_code and class_code not in class_codes:
                class_codes.append(class_code)
            registered_rows.append(
                {
                    "CLASS NAME": class_name,
                    "CLASS CODE": class_code,
                    "STUDENT NAME": student_name,
                    "IDENTIFIER": identifier,
                    "createdAt": created_at,
                    "lastSignInAt": last_sign_in_at,
                }
            )

        if not group_ids:
            registered_rows.append(
                {
                    "CLASS NAME": "",
                    "CLASS CODE": "",
                    "STUDENT NAME": student_name,
                    "IDENTIFIER": identifier,
                    "createdAt": created_at,
                    "lastSignInAt": last_sign_in_at,
                }
            )

        if role == "student" and len(group_ids) > 1:
            multi_class_students_rows.append(
                {
                    "RS USER ID": user_id,
                    "STUDENT NAME": student_name,
                    "IDENTIFIER": identifier,
                    "EMAIL": email,
                    "CLASSES COUNT": str(len(group_ids)),
                    "REMOVE 2025 SUBSCRIPTIONS": "Si" if len(group_ids) > 2 else "",
                    "CLASS NAMES": " | ".join(class_names),
                    "CLASS CODES": " | ".join(class_codes),
                    "createdAt": created_at,
                    "lastSignInAt": last_sign_in_at,
                }
            )

    registered_rows = [
        row
        for row in registered_rows
        if row.get("CLASS NAME")
        or row.get("CLASS CODE")
        or row.get("STUDENT NAME")
        or row.get("IDENTIFIER")
        or row.get("createdAt")
        or row.get("lastSignInAt")
    ]
    registered_rows = sorted(
        registered_rows,
        key=lambda row: (
            str(row.get("CLASS NAME") or "").lower(),
            str(row.get("CLASS CODE") or "").lower(),
            str(row.get("STUDENT NAME") or "").lower(),
            str(row.get("IDENTIFIER") or "").lower(),
            str(row.get("createdAt") or "").lower(),
            str(row.get("lastSignInAt") or "").lower(),
        ),
    )
    multi_class_students_rows = sorted(
        multi_class_students_rows,
        key=lambda row: (
            -int(str(row.get("CLASSES COUNT") or "0") or "0"),
            str(row.get("STUDENT NAME") or "").lower(),
            str(row.get("IDENTIFIER") or "").lower(),
        ),
    )
    return {
        "registered_rows": registered_rows,
        "multi_class_students_rows": multi_class_students_rows,
        "excluded_roles": excluded_roles,
        "valid_users_count": int(len(filtered_users)),
        "total_users_count": int(len(rs_users)),
    }


def _extract_richmondstudio_user_create_result(
    body: Dict[str, object],
    fallback_email: object = "",
) -> Dict[str, str]:
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    attrs = data.get("attributes") if isinstance(data.get("attributes"), dict) else {}
    meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    password = ""
    for value in (
        body.get("password"),
        meta.get("password"),
        data.get("password"),
        attrs.get("password"),
        attrs.get("generatedPassword"),
        attrs.get("temporaryPassword"),
    ):
        password = str(value or "").strip()
        if password:
            break

    login = ""
    for value in (
        attrs.get("email"),
        attrs.get("login"),
        attrs.get("username"),
        body.get("email"),
        fallback_email,
    ):
        login = str(value or "").strip()
        if login:
            break

    user_id = ""
    for value in (data.get("id"), body.get("id")):
        user_id = str(value or "").strip()
        if user_id:
            break

    return {
        "user_id": user_id,
        "login": login,
        "password": password,
    }


def _fetch_richmondstudio_current_user_context(
    token: str,
    timeout: int = 30,
) -> Dict[str, str]:
    try:
        response = requests.get(
            RICHMONDSTUDIO_CURRENT_USER_URL,
            headers=_richmondstudio_headers(token),
            params={"include": "institution"},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = None

    if not response.ok:
        raise RuntimeError(_richmondstudio_response_error(response, status_code, body))
    if body is None or not isinstance(body, dict):
        raise RuntimeError(f"Respuesta no JSON (status {status_code})")

    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    attrs = data.get("attributes") if isinstance(data.get("attributes"), dict) else {}
    relationships = (
        data.get("relationships") if isinstance(data.get("relationships"), dict) else {}
    )
    institution_rel = (
        relationships.get("institution")
        if isinstance(relationships.get("institution"), dict)
        else {}
    )
    institution_data = (
        institution_rel.get("data") if isinstance(institution_rel.get("data"), dict) else {}
    )
    institution_id = str(institution_data.get("id") or "").strip()

    institution_name = ""
    included = body.get("included") if isinstance(body.get("included"), list) else []
    for item in included:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").strip() != "institutions":
            continue
        if institution_id and str(item.get("id") or "").strip() != institution_id:
            continue
        included_attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
        institution_name = str(included_attrs.get("name") or "").strip()
        if institution_name:
            break

    first_name = str(attrs.get("firstName") or attrs.get("first_name") or "").strip()
    last_name = str(attrs.get("lastName") or attrs.get("last_name") or "").strip()
    user_name = " ".join(part for part in (first_name, last_name) if part).strip()

    return {
        "user_name": user_name,
        "institution_name": institution_name,
        "institution_id": institution_id,
        "email": str(attrs.get("email") or "").strip(),
    }


def _set_richmondstudio_pending_confirmation(
    action_key: str,
    action_label: str,
    context: Dict[str, str],
) -> None:
    st.session_state["rs_pending_confirmation"] = {
        "action": str(action_key or "").strip(),
        "label": str(action_label or "").strip(),
        "user_name": str(context.get("user_name") or "").strip(),
        "institution_name": str(context.get("institution_name") or "").strip(),
        "institution_id": str(context.get("institution_id") or "").strip(),
        "email": str(context.get("email") or "").strip(),
    }


def _consume_richmondstudio_confirmed_action(action_key: str) -> bool:
    approved = st.session_state.get("rs_confirm_approved_action")
    if not isinstance(approved, dict):
        return False
    if str(approved.get("action") or "").strip() != str(action_key or "").strip():
        return False
    st.session_state.pop("rs_confirm_approved_action", None)
    return True


def _build_richmondstudio_users_output_filename(institution_name: object) -> str:
    raw = str(institution_name or "").strip()
    cleaned = re.sub(r'[\\/:*?"<>|]+', " ", raw)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if cleaned:
        return f"alumnos_RS_{cleaned}.xlsx"
    return "alumnos_RS.xlsx"


def _build_richmondstudio_users_export_rows(
    rows: List[Dict[str, object]]
) -> List[Dict[str, str]]:
    export_rows: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        export_rows.append(
            {
                "Last name": str(row.get("Last name") or "").strip(),
                "First name": str(row.get("First name") or "").strip(),
                "Class": str(row.get("Class") or "").strip(),
                "Email": str(row.get("Email") or "").strip(),
                "Role": str(row.get("Role") or "").strip(),
                "level": str(row.get("level") or row.get("Level") or "").strip(),
                "Login": str(row.get("Login") or "").strip(),
                "Password": str(row.get("Password") or "").strip(),
            }
        )
    return export_rows


def _richmondstudio_display_bool(value: object) -> str:
    return "Si" if bool(value) else "No"


def _richmondstudio_group_users_count(group_item: Dict[str, object]) -> int:
    relationships = group_item.get("relationships")
    if not isinstance(relationships, dict):
        return 0
    users_rel = relationships.get("users")
    if not isinstance(users_rel, dict):
        return 0
    users_data = users_rel.get("data")
    if not isinstance(users_data, list):
        return 0
    return len(users_data)


def _richmondstudio_grade_label(grade_code: object, level_value: object) -> str:
    grade_text = str(grade_code or "").strip().lower()
    level_text = str(level_value or "").strip().lower()
    direct_label = str(RICHMONDSTUDIO_GRADE_TEXT_BY_CODE.get(grade_text, "")).strip()
    if direct_label:
        return direct_label
    match = re.fullmatch(r"grade(\d+)", grade_text)
    if not match:
        return str(grade_code or "").strip()

    grade_num = int(match.group(1))
    if level_text == "secondary":
        mapping = {
            7: "Primer año de secundaria",
            8: "Segundo año de secundaria",
            9: "Tercer año de secundaria",
            10: "Cuarto año de secundaria",
            11: "Quinto año de secundaria",
        }
        return mapping.get(grade_num, f"Secundaria {grade_num}")
    if level_text == "primary":
        mapping = {
            1: "Primer grado de primaria",
            2: "Segundo grado de primaria",
            3: "Tercer grado de primaria",
            4: "Cuarto grado de primaria",
            5: "Quinto grado de primaria",
            6: "Sexto grado de primaria",
        }
        return mapping.get(grade_num, f"Primaria {grade_num}")
    if level_text in {"preschool", "preprimary"}:
        mapping = {
            2: "2 años",
            3: "3 años",
            4: "4 años",
            5: "5 años",
        }
        return mapping.get(grade_num, f"Inicial {grade_num}")
    return str(grade_code or "").strip()


def _richmondstudio_grade_display(attrs: Dict[str, object]) -> str:
    level_value = str(attrs.get("level") or "").strip().lower()
    level_short = str(RICHMONDSTUDIO_LEVEL_SHORT_BY_VALUE.get(level_value, "")).strip()
    grade_label = _richmondstudio_grade_label(attrs.get("grade"), level_value)
    if level_short and grade_label:
        return f"{level_short} | {grade_label}"
    return grade_label or level_short


def _richmondstudio_date_display(date_text: object) -> str:
    raw = str(date_text or "").strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.date().strftime("%d/%m/%Y")
    except ValueError:
        try:
            parsed = date.fromisoformat(raw.split("T", 1)[0])
            return parsed.strftime("%d/%m/%Y")
        except ValueError:
            return raw


def _richmondstudio_default_dates() -> Tuple[date, date]:
    today = date.today()
    return today, date(today.year, 12, 31)


def _normalize_richmondstudio_group_row(group_item: Dict[str, object]) -> Dict[str, object]:
    attrs = group_item.get("attributes") if isinstance(group_item.get("attributes"), dict) else {}
    start_date = str(attrs.get("startDate") or "").strip()
    end_date = str(attrs.get("endDate") or "").strip()
    grade_level_value = str(attrs.get("gradeLevel") or "").strip()
    grade_level_label = (
        RICHMONDSTUDIO_TEST_LEVEL_LABEL_BY_VALUE.get(grade_level_value, grade_level_value)
        if grade_level_value
        else ""
    )
    return {
        "ID": str(group_item.get("id") or "").strip(),
        "Class name": str(attrs.get("name") or "").strip(),
        "Grade": _richmondstudio_grade_display(attrs),
        "Dates": " | ".join(
            part
            for part in (
                f"Start: {_richmondstudio_date_display(start_date)}" if start_date else "",
                f"End: {_richmondstudio_date_display(end_date)}" if end_date else "",
            )
            if part
        ),
        "iRead": _richmondstudio_display_bool(attrs.get("iread")),
        "Code": str(attrs.get("code") or "").strip(),
        "Test level": grade_level_label,
        "Students": int(attrs.get("numberOfStudents") or 0),
        "Users": _richmondstudio_group_users_count(group_item),
    }


def _coerce_iso_date(value: object, field_name: str) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"Falta {field_name}.")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} invalida: {text}") from exc


def _default_richmondstudio_group_row() -> Dict[str, object]:
    return {
        "Crear": True,
        "Class name": "",
        "Description": "",
        "Grade": "Primer año de secundaria",
        "Grade code": RICHMONDSTUDIO_GRADE_SUGGESTION_BY_LABEL.get(
            "Primer año de secundaria", "grade7"
        ),
        "Test level": "lower secondary",
        "iRead": False,
    }


def _normalize_richmondstudio_create_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    normalized: List[Dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        grade_label = str(row.get("Grade") or "").strip()
        grade_code = str(row.get("Grade code") or "").strip()
        if not grade_code and grade_label:
            grade_code = str(
                RICHMONDSTUDIO_GRADE_SUGGESTION_BY_LABEL.get(grade_label, "")
            ).strip()
        if grade_code not in RICHMONDSTUDIO_GRADE_CODE_OPTIONS:
            grade_code = str(
                RICHMONDSTUDIO_GRADE_SUGGESTION_BY_LABEL.get(
                    grade_label or "Primer aÃ±o de secundaria", "grade7"
                )
            ).strip()
        normalized.append(
            {
                "Crear": bool(row.get("Crear", True)),
                "Class name": str(row.get("Class name") or "").strip(),
                "Description": str(row.get("Description") or "").strip(),
                "Grade": grade_label or "Primer año de secundaria",
                "Grade code": grade_code,
                "Test level": str(row.get("Test level") or "").strip()
                or "lower secondary",
                "iRead": bool(row.get("iRead", False)),
            }
        )
    return normalized


def _build_richmondstudio_group_payload(row: Dict[str, object]) -> Dict[str, object]:
    class_name = str(row.get("Class name") or "").strip()
    if not class_name:
        raise ValueError("Falta Class name.")
    description = str(row.get("Description") or "").strip() or class_name
    grade_label = str(row.get("Grade") or "").strip()
    grade_code = str(row.get("Grade code") or "").strip()
    if not grade_code and grade_label:
        grade_code = str(
            RICHMONDSTUDIO_GRADE_SUGGESTION_BY_LABEL.get(grade_label, "")
        ).strip()
    if not grade_code:
        raise ValueError(f"Falta Grade code para {class_name}.")

    test_level_label = str(row.get("Test level") or "").strip()
    grade_level = str(
        RICHMONDSTUDIO_TEST_LEVEL_BY_LABEL.get(test_level_label, "")
    ).strip()
    if not grade_level:
        raise ValueError(f"Test level invalido para {class_name}.")

    start_date_obj, end_date_obj = _richmondstudio_default_dates()
    start_date = start_date_obj.isoformat()
    end_date = end_date_obj.isoformat()

    return {
        "data": {
            "type": "groups",
            "attributes": {
                "name": class_name,
                "description": description,
                "grade": grade_code,
                "gradeLevel": grade_level,
                "startDate": start_date,
                "endDate": end_date,
                "iread": bool(row.get("iRead", False)),
            },
            "relationships": {"users": {"data": []}},
        }
    }


def _default_richmondstudio_group_row() -> Dict[str, object]:
    default_grade_code = "grade7"
    return {
        "_row_id": _richmondstudio_new_create_row_id(),
        "Crear": True,
        "Class name": "",
        "Description": "",
        "Grade": _richmondstudio_grade_option_from_code(default_grade_code),
        "Grade code": default_grade_code,
        "Test level": "",
        "iRead": False,
    }


def _normalize_richmondstudio_create_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    normalized: List[Dict[str, object]] = []
    default_grade_code = "grade7"
    default_grade_option = _richmondstudio_grade_option_from_code(default_grade_code)
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("_row_id") or "").strip() or _richmondstudio_new_create_row_id()
        grade_option = str(row.get("Grade") or "").strip() or default_grade_option
        grade_code = _richmondstudio_grade_code_from_value(grade_option) or _richmondstudio_grade_code_from_value(row.get("Grade code"))
        if not grade_code or grade_code not in RICHMONDSTUDIO_GRADE_CODE_OPTIONS:
            grade_code = default_grade_code
        normalized.append(
            {
                "_row_id": row_id,
                "Crear": bool(row.get("Crear", True)),
                "Class name": str(row.get("Class name") or "").strip(),
                "Description": str(row.get("Description") or "").strip(),
                "Grade": _richmondstudio_grade_option_from_code(grade_code),
                "Grade code": grade_code,
                "Test level": str(
                    row.get("Test level")
                    if "Test level" in row and row.get("Test level") is not None
                    else ""
                ).strip(),
                "iRead": bool(row.get("iRead", False)),
            }
        )
    return normalized


def _render_richmondstudio_create_rows_form(
    state_key: str,
    widget_prefix: str,
) -> List[Dict[str, object]]:
    rows = _normalize_richmondstudio_create_rows(st.session_state.get(state_key) or [])
    if not rows:
        rows = [_default_richmondstudio_group_row()]
        st.session_state[state_key] = rows

    add_col, info_col = st.columns([1, 2.2], gap="small")
    if add_col.button(
        "Agregar otra clase",
        key=f"{widget_prefix}_add_row_btn",
        use_container_width=True,
    ):
        rows.append(_default_richmondstudio_group_row())
        st.session_state[state_key] = _normalize_richmondstudio_create_rows(rows)
        st.rerun()
    info_col.caption(
        "Cada fila crea una clase. Puedes duplicar una fila para cambiar solo lo necesario."
    )

    header_cols = st.columns([0.35, 0.55, 1.8, 1.8, 1.5, 1.35, 0.8, 0.8], gap="small")
    header_cols[0].caption("#")
    header_cols[1].caption("Crear")
    header_cols[2].caption("Class name")
    header_cols[3].caption("Description")
    header_cols[4].caption("Grado")
    header_cols[5].caption("Test level")
    header_cols[6].caption(" ")
    header_cols[7].caption(" ")

    updated_rows: List[Dict[str, object]] = []
    duplicate_after_row_id = ""
    remove_row_id = ""

    for idx, row in enumerate(rows, start=1):
        row_id = str(row.get("_row_id") or "").strip() or _richmondstudio_new_create_row_id()
        current_grade = str(row.get("Grade") or "").strip()
        current_test_level = str(row.get("Test level") or "").strip()
        grade_index = (
            RICHMONDSTUDIO_GRADE_LABELS.index(current_grade)
            if current_grade in RICHMONDSTUDIO_GRADE_LABELS
            else 0
        )
        test_level_options = [""] + RICHMONDSTUDIO_TEST_LEVEL_LABELS
        test_level_index = (
            test_level_options.index(current_test_level)
            if current_test_level in test_level_options
            else 0
        )
        row_cols = st.columns([0.35, 0.55, 1.8, 1.8, 1.5, 1.35, 0.8, 0.8], gap="small")
        row_cols[0].markdown(f"**{idx}**")
        create_flag = row_cols[1].checkbox(
            "Crear",
            value=bool(row.get("Crear", True)),
            key=f"{widget_prefix}_create_{row_id}",
            label_visibility="collapsed",
        )
        class_name = row_cols[2].text_input(
            "Class name",
            value=str(row.get("Class name") or "").strip(),
            key=f"{widget_prefix}_class_name_{row_id}",
            placeholder="2026 Ingles 2SB",
            label_visibility="collapsed",
        )
        description = row_cols[3].text_input(
            "Description",
            value=str(row.get("Description") or "").strip(),
            key=f"{widget_prefix}_description_{row_id}",
            placeholder="Se completa con Class name si lo dejas vacio",
            label_visibility="collapsed",
        )
        grade_label = row_cols[4].selectbox(
            "Grado",
            options=RICHMONDSTUDIO_GRADE_LABELS,
            index=grade_index,
            key=f"{widget_prefix}_grade_{row_id}",
            label_visibility="collapsed",
        )
        test_level = row_cols[5].selectbox(
            "Test level",
            options=test_level_options,
            index=test_level_index,
            key=f"{widget_prefix}_test_level_{row_id}",
            label_visibility="collapsed",
        )
        if row_cols[6].button(
            "Duplicar",
            key=f"{widget_prefix}_duplicate_{row_id}",
            use_container_width=True,
        ):
            duplicate_after_row_id = row_id
        if row_cols[7].button(
            "Eliminar",
            key=f"{widget_prefix}_remove_{row_id}",
            use_container_width=True,
            disabled=len(rows) <= 1,
        ):
            remove_row_id = row_id

        updated_rows.append(
            {
                "_row_id": row_id,
                "Crear": create_flag,
                "Class name": str(class_name or "").strip(),
                "Description": str(description or "").strip(),
                "Grade": str(grade_label or "").strip(),
                "Grade code": _richmondstudio_grade_code_from_value(grade_label),
                "Test level": str(test_level or "").strip(),
                "iRead": bool(row.get("iRead", False)),
            }
        )
        st.divider()

    if remove_row_id:
        updated_rows = [
            row for row in updated_rows if str(row.get("_row_id") or "").strip() != remove_row_id
        ]
        if not updated_rows:
            updated_rows = [_default_richmondstudio_group_row()]
        st.session_state[state_key] = _normalize_richmondstudio_create_rows(updated_rows)
        st.rerun()

    if duplicate_after_row_id:
        duplicated_rows: List[Dict[str, object]] = []
        for row in updated_rows:
            duplicated_rows.append(dict(row))
            if str(row.get("_row_id") or "").strip() == duplicate_after_row_id:
                duplicated_row = dict(row)
                duplicated_row["_row_id"] = _richmondstudio_new_create_row_id()
                duplicated_row["Crear"] = True
                duplicated_rows.append(duplicated_row)
        st.session_state[state_key] = _normalize_richmondstudio_create_rows(duplicated_rows)
        st.rerun()

    st.session_state[state_key] = _normalize_richmondstudio_create_rows(updated_rows)
    return st.session_state[state_key]


def _richmondstudio_level_from_test_level(
    test_level_value: object, fallback_level: object = ""
) -> str:
    raw = str(test_level_value or "").strip()
    normalized = str(RICHMONDSTUDIO_TEST_LEVEL_BY_LABEL.get(raw, raw)).strip().lower()
    mapping = {
        "lower_primary": "primary",
        "upper_primary": "primary",
        "lower_secondary": "secondary",
        "upper_secondary": "secondary",
    }
    if normalized in mapping:
        return mapping[normalized]
    return str(fallback_level or "").strip().lower()


def _richmondstudio_level_from_grade(
    grade_code: object,
    fallback_level: object = "",
) -> str:
    code = str(grade_code or "").strip().lower()
    if code in {"grade12", "grade13", "grade14", "grade15"}:
        return "preschool"
    if code in {"grade1", "grade2", "grade3", "grade4", "grade5", "grade6"}:
        return "primary"
    if code in {"grade7", "grade8", "grade9", "grade10", "grade11"}:
        return "secondary"
    return str(fallback_level or "").strip().lower()


def _richmondstudio_group_level(
    grade_code: object,
    test_level_value: object = "",
    fallback_level: object = "",
) -> str:
    level_from_test = _richmondstudio_level_from_test_level(
        test_level_value,
        fallback_level,
    )
    if level_from_test:
        return level_from_test
    return _richmondstudio_level_from_grade(grade_code, fallback_level)


def _richmondstudio_group_users_data(group_item: Dict[str, object]) -> List[Dict[str, str]]:
    relationships = group_item.get("relationships")
    if not isinstance(relationships, dict):
        return []
    users_rel = relationships.get("users")
    if not isinstance(users_rel, dict):
        return []
    users_data = users_rel.get("data")
    if not isinstance(users_data, list):
        return []

    normalized_users: List[Dict[str, str]] = []
    for item in users_data:
        if not isinstance(item, dict):
            continue
        user_id = str(item.get("id") or "").strip()
        if not user_id:
            continue
        normalized_users.append(
            {
                "id": user_id,
                "type": str(item.get("type") or "users").strip() or "users",
            }
        )
    return normalized_users


def _richmondstudio_dates_summary(start_date: object, end_date: object) -> str:
    return " | ".join(
        part
        for part in (
            f"Start: {_richmondstudio_date_display(start_date)}"
            if str(start_date or "").strip()
            else "",
            f"End: {_richmondstudio_date_display(end_date)}"
            if str(end_date or "").strip()
            else "",
        )
        if part
    )


def _richmondstudio_group_grade_display(
    grade_code: object, test_level_label: object, fallback_level: object = ""
) -> str:
    direct_option = _richmondstudio_grade_option_from_code(grade_code)
    if direct_option and direct_option != str(grade_code or "").strip():
        return direct_option
    level_value = _richmondstudio_level_from_test_level(test_level_label, fallback_level)
    return _richmondstudio_grade_display(
        {
            "grade": str(grade_code or "").strip(),
            "level": level_value,
        }
    )


def _normalize_richmondstudio_group_row(group_item: Dict[str, object]) -> Dict[str, object]:
    attrs = group_item.get("attributes") if isinstance(group_item.get("attributes"), dict) else {}
    start_date = str(attrs.get("startDate") or "").strip()
    end_date = str(attrs.get("endDate") or "").strip()
    grade_code = str(attrs.get("grade") or "").strip()
    grade_level_value = str(attrs.get("gradeLevel") or "").strip()
    grade_level_label = (
        RICHMONDSTUDIO_TEST_LEVEL_LABEL_BY_VALUE.get(grade_level_value, grade_level_value)
        if grade_level_value
        else ""
    )
    level_value = _richmondstudio_group_level(
        grade_code,
        grade_level_value,
        attrs.get("level"),
    )
    class_name = str(attrs.get("name") or "").strip()
    description = str(attrs.get("description") or "").strip() or class_name
    return {
        "Seleccionar": False,
        "ID": str(group_item.get("id") or "").strip(),
        "Class name": class_name,
        "Description": description,
        "Grade": _richmondstudio_group_grade_display(
            grade_code,
            grade_level_label,
            level_value or attrs.get("level"),
        ),
        "Grade code": grade_code,
        "Dates": _richmondstudio_dates_summary(start_date, end_date),
        "Start date": start_date,
        "End date": end_date,
        "iRead": bool(attrs.get("iread")),
        "Code": str(attrs.get("code") or "").strip(),
        "Test level": grade_level_label,
        "Students": int(attrs.get("numberOfStudents") or 0),
        "Users": _richmondstudio_group_users_count(group_item),
        "_level_value": level_value,
        "_users_data": _richmondstudio_group_users_data(group_item),
    }


def _normalize_richmondstudio_loaded_rows(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    normalized: List[Dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        grade_code = _richmondstudio_grade_code_from_value(row.get("Grade")) or _richmondstudio_grade_code_from_value(row.get("Grade code"))
        test_level_label = str(row.get("Test level") or "").strip()
        start_date_raw = row.get("Start date")
        end_date_raw = row.get("End date")
        if start_date_raw in ("", None):
            start_date = ""
        else:
            try:
                start_date = _coerce_iso_date(start_date_raw, "Start date")
            except ValueError:
                start_date = str(start_date_raw).strip()
        if end_date_raw in ("", None):
            end_date = ""
        else:
            try:
                end_date = _coerce_iso_date(end_date_raw, "End date")
            except ValueError:
                end_date = str(end_date_raw).strip()
        users_data = row.get("_users_data")
        if not isinstance(users_data, list):
            users_data = []
        level_value = _richmondstudio_group_level(
            grade_code,
            test_level_label,
            row.get("_level_value"),
        )
        normalized.append(
            {
                "Seleccionar": bool(row.get("Seleccionar", False)),
                "ID": str(row.get("ID") or "").strip(),
                "Class name": str(row.get("Class name") or "").strip(),
                "Description": str(row.get("Description") or "").strip(),
                "Grade": _richmondstudio_grade_option_from_code(grade_code)
                or _richmondstudio_group_grade_display(
                    grade_code,
                    test_level_label,
                    level_value,
                ),
                "Grade code": grade_code,
                "Dates": _richmondstudio_dates_summary(start_date, end_date),
                "Start date": start_date,
                "End date": end_date,
                "iRead": bool(row.get("iRead", False)),
                "Code": str(row.get("Code") or "").strip(),
                "Test level": test_level_label,
                "Students": int(row.get("Students") or 0),
                "Users": int(row.get("Users") or 0),
                "_level_value": level_value,
                "_users_data": users_data,
            }
        )
    return normalized


def _richmondstudio_loaded_editor_df(
    rows: List[Dict[str, object]], columns: List[str]
) -> pd.DataFrame:
    prepared_rows: List[Dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        prepared: Dict[str, object] = {column: row.get(column) for column in columns}
        for date_key in ("Start date", "End date"):
            raw_value = prepared.get(date_key)
            if raw_value in ("", None):
                prepared[date_key] = None
                continue
            try:
                prepared[date_key] = date.fromisoformat(
                    _coerce_iso_date(raw_value, date_key)
                )
            except ValueError:
                prepared[date_key] = None
        prepared["Seleccionar"] = bool(prepared.get("Seleccionar", False))
        prepared["iRead"] = bool(prepared.get("iRead", False))
        try:
            prepared["Students"] = int(prepared.get("Students") or 0)
        except (TypeError, ValueError):
            prepared["Students"] = 0
        prepared_rows.append(prepared)
    return pd.DataFrame(prepared_rows, columns=columns)


def _build_richmondstudio_group_payload(row: Dict[str, object]) -> Dict[str, object]:
    class_name = str(row.get("Class name") or "").strip()
    if not class_name:
        raise ValueError("Falta Class name.")

    description = str(row.get("Description") or "").strip() or class_name
    grade_code = _richmondstudio_grade_code_from_value(row.get("Grade")) or _richmondstudio_grade_code_from_value(row.get("Grade code"))
    if not grade_code:
        raise ValueError(f"Falta Grade para {class_name}.")

    test_level_label = str(row.get("Test level") or "").strip()
    grade_level = str(RICHMONDSTUDIO_TEST_LEVEL_BY_LABEL.get(test_level_label, "")).strip()
    level_value = _richmondstudio_group_level(grade_code, test_level_label)
    start_date_obj, end_date_obj = _richmondstudio_default_dates()
    attributes: Dict[str, object] = {
        "name": class_name,
        "description": description,
        "grade": grade_code,
        "level": level_value,
        "startDate": start_date_obj.isoformat(),
        "endDate": end_date_obj.isoformat(),
    }
    if grade_level:
        attributes["gradeLevel"] = grade_level

    return {
        "data": {
            "type": "groups",
            "attributes": attributes,
            "relationships": {"users": {"data": []}},
        }
    }


def _build_richmondstudio_group_update_payload(row: Dict[str, object]) -> Dict[str, object]:
    group_id = str(row.get("ID") or "").strip()
    if not group_id:
        raise ValueError("Falta ID de la clase.")

    class_name = str(row.get("Class name") or "").strip()
    if not class_name:
        raise ValueError(f"Falta Class name para {group_id}.")

    description = str(row.get("Description") or "").strip() or class_name
    grade_code = _richmondstudio_grade_code_from_value(row.get("Grade")) or _richmondstudio_grade_code_from_value(row.get("Grade code"))
    if not grade_code:
        raise ValueError(f"Falta Grade para {class_name}.")

    test_level_label = str(row.get("Test level") or "").strip()
    grade_level = str(RICHMONDSTUDIO_TEST_LEVEL_BY_LABEL.get(test_level_label, "")).strip()
    level_value = _richmondstudio_group_level(
        grade_code,
        test_level_label,
        row.get("_level_value"),
    )
    users_data = row.get("_users_data")
    if not isinstance(users_data, list):
        users_data = []
    attributes = {
        "name": class_name,
        "description": description,
        "grade": grade_code,
        "level": level_value,
        "startDate": _coerce_iso_date(row.get("Start date"), "Start date"),
        "endDate": _coerce_iso_date(row.get("End date"), "End date"),
    }
    if grade_level:
        attributes["gradeLevel"] = grade_level

    return {
        "data": {
            "type": "groups",
            "id": group_id,
            "attributes": attributes,
            "relationships": {"users": {"data": users_data}},
        }
    }


def _export_simple_excel(rows: List[Dict[str, object]], sheet_name: str = "data") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.getvalue()


def _show_dataframe(data: object, use_container_width: bool = True) -> None:
    if isinstance(data, pd.DataFrame):
        df_view = data.copy()
    else:
        df_view = pd.DataFrame(data)
    if not df_view.empty:
        df_view.index = range(1, len(df_view) + 1)
    st.dataframe(df_view, use_container_width=use_container_width)


def _normalize_censo_activos_export_rows(
    rows: List[Dict[str, object]]
) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "Nivel": str(row.get("Nivel") or row.get("nivel") or "").strip(),
                "Grado": str(row.get("Grado") or row.get("grado") or "").strip(),
                "Grupo": str(
                    row.get("Grupo")
                    or row.get("Seccion")
                    or row.get("seccion")
                    or ""
                ).strip(),
                "Nombre del alumno": str(
                    row.get("Nombre del alumno")
                    or row.get("Nombre completo")
                    or row.get("nombre_completo")
                    or ""
                ).strip(),
                "Login": str(row.get("Login") or row.get("login") or "").strip(),
                "Password": str(row.get("Password") or row.get("password") or "").strip(),
            }
        )
    normalized.sort(
        key=lambda row: (
            str(row.get("Nivel") or ""),
            str(row.get("Grado") or ""),
            str(row.get("Grupo") or ""),
            str(row.get("Nombre del alumno") or ""),
        )
    )
    return normalized


def _normalize_censo_compare_header(value: object) -> str:
    text = _normalize_plain_text(value)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return text.strip()


def _resolve_censo_compare_sheet_name(
    available: List[str],
    desired: str = "Plantilla_Actualizada",
) -> str:
    if desired in available:
        return desired
    desired_lower = str(desired).lower()
    for sheet_name in available:
        if str(sheet_name).lower() == desired_lower:
            return str(sheet_name)
    desired_norm = _normalize_censo_compare_header(desired)
    for sheet_name in available:
        if _normalize_censo_compare_header(sheet_name) == desired_norm:
            return str(sheet_name)
    available_list = ", ".join(available) if available else "(sin hojas)"
    raise ValueError(
        "No se encontro la hoja 'Plantilla_Actualizada'. "
        f"Hojas disponibles: {available_list}."
    )


def _read_censo_compare_excel(uploaded_file) -> List[Dict[str, str]]:
    with pd.ExcelFile(uploaded_file, engine="openpyxl") as excel:
        sheet_name = _resolve_censo_compare_sheet_name(excel.sheet_names)
        raw_df = pd.read_excel(excel, sheet_name=sheet_name, dtype=str).fillna("")
    alias_map = {
        "NIVEL": "nivel",
        "GRADO": "grado",
        "GRUPO": "grupo",
        "NOMBRE": "nombre",
        "APELLIDO PATERNO": "apellido_paterno",
        "APELLIDO MATERNO": "apellido_materno",
        "SEXO": "sexo",
        "FECHA DE NACIMIENTO": "fecha_nacimiento",
        "FECHA NACIMIENTO": "fecha_nacimiento",
        "NUIP": "nuip",
        "DNI": "nuip",
        "LOGIN": "login",
        "PASSWORD": "password",
    }
    renamed_columns: Dict[str, str] = {}
    used_columns: Set[str] = set()
    for column in raw_df.columns:
        canonical = alias_map.get(_normalize_censo_compare_header(column))
        if canonical and canonical not in used_columns:
            renamed_columns[str(column)] = canonical
            used_columns.add(canonical)
    df = raw_df.rename(columns=renamed_columns)

    rows: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        normalized_row = {
            "nivel": str(row.get("nivel") or "").strip(),
            "grado": str(row.get("grado") or "").strip(),
            "grupo": str(row.get("grupo") or "").strip(),
            "nombre": str(row.get("nombre") or "").strip(),
            "apellido_paterno": str(row.get("apellido_paterno") or "").strip(),
            "apellido_materno": str(row.get("apellido_materno") or "").strip(),
            "sexo": str(row.get("sexo") or "").strip(),
            "fecha_nacimiento": str(row.get("fecha_nacimiento") or "").strip(),
            "nuip": str(row.get("nuip") or "").strip(),
            "login": str(row.get("login") or "").strip(),
            "password": str(row.get("password") or "").strip(),
        }
        if any(str(value).strip() for value in normalized_row.values()):
            rows.append(normalized_row)
    return rows


def _collect_colegio_alumnos_censo_rows(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> Tuple[List[Dict[str, object]], List[str]]:
    niveles = _fetch_niveles_grados_grupos_censo(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
    )
    contexts = _build_contexts_for_nivel_grado(niveles=niveles)
    alumnos_all_raw: List[Dict[str, object]] = []
    errors: List[str] = []
    for ctx in contexts:
        try:
            alumnos_ctx = _fetch_alumnos_censo(
                token=token,
                colegio_id=int(colegio_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                nivel_id=int(ctx.get("nivel_id") or 0),
                grado_id=int(ctx.get("grado_id") or 0),
                grupo_id=int(ctx.get("grupo_id") or 0),
                timeout=int(timeout),
            )
        except Exception as exc:  # pragma: no cover - UI
            errors.append(
                "Error en {nivel} | {grado} ({seccion}): {err}".format(
                    nivel=str(ctx.get("nivel") or ""),
                    grado=str(ctx.get("grado") or ""),
                    seccion=str(ctx.get("seccion") or ""),
                    err=str(exc),
                )
            )
            continue
        for item in alumnos_ctx:
            if isinstance(item, dict):
                alumnos_all_raw.append(_flatten_censo_alumno_for_auto_plan(item=item, fallback=ctx))

    alumnos_by_key: Dict[str, Dict[str, object]] = {}
    for row in alumnos_all_raw:
        alumno_id = _safe_int(row.get("alumno_id"))
        persona_id = _safe_int(row.get("persona_id"))
        grupo_id = _safe_int(row.get("grupo_id"))
        if alumno_id is not None:
            dedupe_key = f"alumno:{int(alumno_id)}"
        elif persona_id is not None and grupo_id is not None:
            dedupe_key = f"persona_grupo:{int(persona_id)}:{int(grupo_id)}"
        elif persona_id is not None:
            dedupe_key = f"persona:{int(persona_id)}"
        else:
            dedupe_key = (
                f"firma:{_normalize_compare_text(row.get('nombre'))}|"
                f"{_normalize_compare_text(row.get('apellido_paterno'))}|"
                f"{_normalize_compare_text(row.get('apellido_materno'))}|"
                f"{_normalize_compare_id(row.get('id_oficial'))}"
            )
        if dedupe_key not in alumnos_by_key:
            alumnos_by_key[dedupe_key] = row

    alumnos_all = sorted(
        alumnos_by_key.values(),
        key=lambda row: (
            str(row.get("apellido_paterno") or "").upper(),
            str(row.get("apellido_materno") or "").upper(),
            str(row.get("nombre") or "").upper(),
            str(row.get("nivel") or "").upper(),
            str(row.get("grado") or "").upper(),
            str(row.get("seccion_norm") or row.get("seccion") or "").upper(),
        ),
    )
    return alumnos_all, errors


def _format_censo_compare_reference(row: Dict[str, object]) -> str:
    nombre = str(row.get("nombre_completo") or "").strip()
    if not nombre:
        nombre = " ".join(
            part
            for part in (
                str(row.get("nombre") or "").strip(),
                str(row.get("apellido_paterno") or "").strip(),
                str(row.get("apellido_materno") or "").strip(),
            )
            if part
        ).strip()
    ubicacion = " | ".join(
        part
        for part in (
            str(row.get("nivel") or "").strip(),
            str(row.get("grado") or "").strip(),
            str(row.get("seccion_norm") or row.get("seccion") or "").strip(),
        )
        if part
    )
    dni = str(row.get("id_oficial") or "").strip()
    estado = "Activo" if _to_bool(row.get("activo")) else "Inactivo"
    parts = [nombre]
    if ubicacion:
        parts.append(ubicacion)
    if dni:
        parts.append(f"DNI {dni}")
    parts.append(estado)
    return " | ".join(part for part in parts if part)


def _censo_compare_display_name(row: Dict[str, object]) -> str:
    nombre = str(row.get("nombre_completo") or "").strip()
    if nombre:
        return nombre
    return " ".join(
        part
        for part in (
            str(row.get("nombre") or "").strip(),
            str(row.get("apellido_paterno") or "").strip(),
            str(row.get("apellido_materno") or "").strip(),
        )
        if part
    ).strip()


def _censo_compare_location_matches(
    uploaded_row: Dict[str, str],
    colegio_row: Dict[str, object],
) -> bool:
    expected_nivel = _normalize_compare_text(uploaded_row.get("nivel"))
    expected_grado = _normalize_compare_text(uploaded_row.get("grado"))
    expected_seccion = _normalize_seccion_key(uploaded_row.get("grupo"))
    current_nivel = _normalize_compare_text(colegio_row.get("nivel"))
    current_grado = _normalize_compare_text(colegio_row.get("grado"))
    current_seccion = _normalize_seccion_key(
        colegio_row.get("seccion_norm") or colegio_row.get("seccion") or ""
    )
    return (
        expected_nivel == current_nivel
        and expected_grado == current_grado
        and expected_seccion == current_seccion
    )


def _build_censo_compare_matches(
    uploaded_rows: List[Dict[str, str]],
    colegio_rows: List[Dict[str, object]],
) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    colegio_by_dni_apellidos: Dict[Tuple[str, str, str], List[Dict[str, object]]] = {}
    for row in colegio_rows:
        dni_key = _normalize_compare_id(row.get("id_oficial"))
        if dni_key:
            ap_pat_key = _normalize_compare_apellido(row.get("apellido_paterno"))
            ap_mat_key = _normalize_compare_apellido(row.get("apellido_materno"))
            if ap_pat_key and ap_mat_key:
                colegio_by_dni_apellidos.setdefault(
                    (dni_key, ap_pat_key, ap_mat_key), []
                ).append(row)

    result_rows: List[Dict[str, object]] = []
    total_reconocidos = 0
    total_ubicacion_ok = 0
    for row in uploaded_rows:
        dni_key = _normalize_compare_id(row.get("nuip"))
        ap_pat_key = _normalize_compare_apellido(row.get("apellido_paterno"))
        ap_mat_key = _normalize_compare_apellido(row.get("apellido_materno"))
        combined_key = (
            dni_key,
            ap_pat_key,
            ap_mat_key,
        )
        combined_matches = (
            colegio_by_dni_apellidos.get(combined_key, [])
            if dni_key and ap_pat_key and ap_mat_key
            else []
        )
        combined_reference = " ; ".join(
            _format_censo_compare_reference(item) for item in combined_matches
        )
        reconocido = bool(combined_reference)
        matched_row = combined_matches[0] if combined_matches else {}
        ubicacion_ok = bool(reconocido) and _censo_compare_location_matches(
            row, matched_row
        )
        total_reconocidos += int(reconocido)
        total_ubicacion_ok += int(ubicacion_ok)
        nombre_completo = " ".join(
            part
            for part in (
                str(row.get("nombre") or "").strip(),
                str(row.get("apellido_paterno") or "").strip(),
                str(row.get("apellido_materno") or "").strip(),
            )
            if part
        ).strip()
        dni_excel = str(row.get("nuip") or "").strip()
        dni_bd = str(matched_row.get("id_oficial") or "").strip()
        apellido_paterno_bd = str(matched_row.get("apellido_paterno") or "").strip()
        apellido_materno_bd = str(matched_row.get("apellido_materno") or "").strip()
        result_rows.append(
            {
                "Nombre completo": nombre_completo,
                "Nombre": row.get("nombre", ""),
                "Apellido Paterno": row.get("apellido_paterno", ""),
                "Apellido Materno": row.get("apellido_materno", ""),
                "Sexo": row.get("sexo", ""),
                "Fecha de Nacimiento": row.get("fecha_nacimiento", ""),
                "DNI Excel": dni_excel,
                "Nombre BD": str(matched_row.get("nombre") or "").strip(),
                "Alumno BD": _censo_compare_display_name(matched_row),
                "DNI BD": dni_bd,
                "Apellido Paterno BD": apellido_paterno_bd,
                "Apellido Materno BD": apellido_materno_bd,
                "Coincidencia": combined_reference,
                "Reconocido": reconocido,
                "Ubicacion correcta": ubicacion_ok,
                "Activo BD": _to_bool(matched_row.get("activo")),
                "Nivel esperado": row.get("nivel", ""),
                "Grado esperado": row.get("grado", ""),
                "Seccion esperada": row.get("grupo", ""),
                "Nivel actual": str(matched_row.get("nivel") or "").strip(),
                "Grado actual": str(matched_row.get("grado") or "").strip(),
                "Seccion actual": str(
                    matched_row.get("seccion_norm") or matched_row.get("seccion") or ""
                ).strip(),
                "AlumnoId BD": _safe_int(matched_row.get("alumno_id")),
                "NivelId actual": _safe_int(matched_row.get("nivel_id")),
                "GradoId actual": _safe_int(matched_row.get("grado_id")),
                "GrupoId actual": _safe_int(matched_row.get("grupo_id")),
            }
        )

    return result_rows, {
        "subidos_total": len(uploaded_rows),
        "colegio_total": len(colegio_rows),
        "reconocidos_total": total_reconocidos,
        "no_reconocidos_total": max(len(uploaded_rows) - total_reconocidos, 0),
        "ubicacion_ok_total": total_ubicacion_ok,
        "por_mover_total": max(total_reconocidos - total_ubicacion_ok, 0),
    }

def _render_censo_compare_name_html(
    nombres: object,
    apellido_paterno: object,
    apellido_materno: object,
) -> str:
    nombres_txt = str(nombres or "").strip()
    ap_pat_txt = str(apellido_paterno or "").strip()
    ap_mat_txt = str(apellido_materno or "").strip()

    parts: List[str] = []
    if nombres_txt:
        parts.append(f"<span>{escape(nombres_txt)}</span>")
    if ap_pat_txt:
        parts.append(f'<span class="censo-compare-surname">{escape(ap_pat_txt)}</span>')
    if ap_mat_txt:
        parts.append(f'<span class="censo-compare-surname">{escape(ap_mat_txt)}</span>')
    if not parts:
        return "-"
    return " ".join(parts)


def _render_censo_compare_preview(rows: List[Dict[str, object]]) -> None:
    bd_dni_counts: Dict[str, int] = {}
    excel_dni_counts: Dict[str, int] = {}
    for row in rows:
        dni_bd_key = _normalize_compare_id(row.get("DNI BD"))
        dni_excel_key = _normalize_compare_id(row.get("DNI Excel"))
        if dni_bd_key:
            bd_dni_counts[dni_bd_key] = int(bd_dni_counts.get(dni_bd_key, 0)) + 1
        if dni_excel_key:
            excel_dni_counts[dni_excel_key] = int(excel_dni_counts.get(dni_excel_key, 0)) + 1

    html_rows: List[str] = []
    for row in rows:
        dni_bd = str(row.get("DNI BD") or "").strip()
        dni_excel = str(row.get("DNI Excel") or "").strip()
        dni_bd_key = _normalize_compare_id(dni_bd)
        dni_excel_key = _normalize_compare_id(dni_excel)
        dni_bd_class = "censo-compare-dni-duplicate" if bd_dni_counts.get(dni_bd_key, 0) > 1 else ""
        dni_excel_class = (
            "censo-compare-dni-duplicate" if excel_dni_counts.get(dni_excel_key, 0) > 1 else ""
        )
        html_rows.append(
            "<tr>"
            f"<td>{_render_censo_compare_name_html(row.get('Nombre BD'), row.get('Apellido Paterno BD'), row.get('Apellido Materno BD'))}</td>"
            f"<td class=\"{dni_bd_class}\">{escape(dni_bd) or '-'}</td>"
            f"<td>{_render_censo_compare_name_html(row.get('Nombre'), row.get('Apellido Paterno'), row.get('Apellido Materno'))}</td>"
            f"<td class=\"{dni_excel_class}\">{escape(dni_excel) or '-'}</td>"
            "</tr>"
        )

    rows_html = "".join(html_rows)
    table_html = """
<style>
.censo-compare-table-wrap { overflow-x: auto; }
.censo-compare-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.95rem;
}
.censo-compare-table th,
.censo-compare-table td {
  border: 1px solid rgba(49, 51, 63, 0.2);
  padding: 0.5rem 0.65rem;
  vertical-align: top;
}
.censo-compare-table th {
  background: rgba(240, 242, 246, 0.85);
  font-weight: 600;
  text-align: left;
}
.censo-compare-surname {
  background: #fef3c7;
  border-radius: 0.3rem;
  padding: 0.05rem 0.18rem;
}
.censo-compare-dni-duplicate {
  background: #dbeafe;
}
</style>
<div class="censo-compare-table-wrap">
  <table class="censo-compare-table">
    <thead>
      <tr>
        <th>Nombre completo DB</th>
        <th>DNI BD</th>
        <th>Nombre completo Excel</th>
        <th>DNI Excel</th>
      </tr>
    </thead>
    <tbody>
      __ROWS_HTML__
    </tbody>
  </table>
</div>
""".replace("__ROWS_HTML__", rows_html)
    st.markdown(table_html, unsafe_allow_html=True)


def _build_censo_compare_export_rows(rows: List[Dict[str, object]]) -> List[Dict[str, str]]:
    export_rows: List[Dict[str, str]] = []
    for row in rows:
        export_rows.append(
            {
                "Nivel": str(row.get("Nivel") or row.get("Nivel esperado") or "").strip(),
                "Grado": str(row.get("Grado") or row.get("Grado esperado") or "").strip(),
                "Grupo": str(row.get("Grupo") or row.get("Seccion esperada") or "").strip(),
                "Nombre": str(row.get("Nombre") or "").strip(),
                "Apellido Paterno": str(row.get("Apellido Paterno") or "").strip(),
                "Apellido Materno": str(row.get("Apellido Materno") or "").strip(),
                "Sexo": str(row.get("Sexo") or "").strip(),
                "Fecha de Nacimiento": str(row.get("Fecha de Nacimiento") or "").strip(),
                "NUIP": str(row.get("NUIP") or row.get("DNI Excel") or "").strip(),
                "Login": "",
                "Password": "",
            }
        )
    return export_rows


def _build_censo_compare_move_plan(
    compare_rows: List[Dict[str, object]],
    niveles_data: List[Dict[str, object]],
) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    destination_catalog = _build_manual_move_destination_catalog(niveles_data)
    destino_lookup: Dict[Tuple[str, str, str], Dict[str, object]] = {}
    for payload in (destination_catalog.get("grupo_payload_by_key") or {}).values():
        if not isinstance(payload, dict):
            continue
        destino_key = (
            _normalize_compare_text(payload.get("nivel")),
            _normalize_compare_text(payload.get("grado")),
            _normalize_seccion_key(payload.get("seccion") or ""),
        )
        if all(destino_key):
            destino_lookup[destino_key] = payload

    move_rows: List[Dict[str, object]] = []
    ready_total = 0
    unresolved_total = 0
    for row in compare_rows:
        if not bool(row.get("Reconocido")) or bool(row.get("Ubicacion correcta")):
            continue
        destino_key = (
            _normalize_compare_text(row.get("Nivel esperado")),
            _normalize_compare_text(row.get("Grado esperado")),
            _normalize_seccion_key(row.get("Seccion esperada") or ""),
        )
        destino_payload = destino_lookup.get(destino_key) or {}
        ready = bool(destino_payload)
        ready_total += int(ready)
        unresolved_total += int(not ready)
        move_rows.append(
            {
                "Nombre completo": str(row.get("Nombre completo") or "").strip(),
                "Alumno BD": str(row.get("Alumno BD") or "").strip(),
                "DNI BD": str(row.get("DNI BD") or "").strip(),
                "Nivel actual": str(row.get("Nivel actual") or "").strip(),
                "Grado actual": str(row.get("Grado actual") or "").strip(),
                "Seccion actual": str(row.get("Seccion actual") or "").strip(),
                "Nivel esperado": str(row.get("Nivel esperado") or "").strip(),
                "Grado esperado": str(row.get("Grado esperado") or "").strip(),
                "Seccion esperada": str(row.get("Seccion esperada") or "").strip(),
                "Estado": "Listo para mover" if ready else "Destino no encontrado",
                "Activo BD": _to_bool(row.get("Activo BD")),
                "AlumnoId BD": _safe_int(row.get("AlumnoId BD")),
                "NivelId actual": _safe_int(row.get("NivelId actual")),
                "GradoId actual": _safe_int(row.get("GradoId actual")),
                "GrupoId actual": _safe_int(row.get("GrupoId actual")),
                "Nuevo NivelId": _safe_int(destino_payload.get("nivel_id")),
                "Nuevo GradoId": _safe_int(destino_payload.get("grado_id")),
                "Nuevo GrupoId": _safe_int(destino_payload.get("grupo_id")),
                "Nueva Seccion": str(destino_payload.get("seccion") or "").strip(),
            }
        )

    return move_rows, {
        "move_total": len(move_rows),
        "move_ready_total": ready_total,
        "move_unresolved_total": unresolved_total,
    }


def _parse_colegio_id(raw: object, field_name: str = "Colegio Clave") -> int:
    text = str(raw or "").strip()
    if not text:
        raise ValueError(f"{field_name} es obligatorio.")
    compact = re.sub(r"\s+", "", text)
    if not compact.isdigit():
        raise ValueError(
            f"{field_name} invÃ¡lido: '{text}'. Usa un ID numÃ©rico (ej: 2326)."
        )
    value = int(compact)
    if value <= 0:
        raise ValueError(f"{field_name} invÃ¡lido: '{text}'. Debe ser mayor a 0.")
    return value


def _parse_colegio_ids(raw: object, field_name: str = "Lista de colegios") -> List[int]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError(f"{field_name} es obligatoria.")
    colegio_ids = parse_id_list(text)
    if not colegio_ids:
        raise ValueError(
            f"{field_name} invalida. Usa IDs numericos separados por coma, espacio o salto de linea."
        )
    return [int(colegio_id) for colegio_id in colegio_ids if int(colegio_id) > 0]


def _fetch_clases_gestion_escolar(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    ordered: bool = False,
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id)
    params: Dict[str, object] = {"colegioId": colegio_id}
    if ordered:
        params["ordered"] = 1
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invÃ¡lida"
        raise RuntimeError(message or "Respuesta invÃ¡lida")

    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("alumnos", "items", "rows", "data"):
            value = data.get(key)
            if isinstance(value, list):
                return value
        for value in data.values():
            if isinstance(value, list):
                return value
    raise RuntimeError("Campo data no es lista")


def _fetch_alumnos_clase_gestion_escolar(
    token: str, clase_id: int, empresa_id: int, ciclo_id: int, timeout: int
) -> Dict[str, object]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_ALUMNOS_CLASE_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        clase_id=clase_id,
    )
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invÃ¡lida"
        raise RuntimeError(message or "Respuesta invÃ¡lida")

    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise RuntimeError("Campo data no es objeto")
    return data


def _fetch_niveles_grados_grupos_censo(
    token: str, colegio_id: int, empresa_id: int, ciclo_id: int, timeout: int
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = CENSO_NIVELES_GRADOS_GRUPOS_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
    )
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invÃ¡lida"
        raise RuntimeError(message or "Respuesta invÃ¡lida")

    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise RuntimeError("Campo data no es objeto")
    niveles = data.get("niveles") or []
    if not isinstance(niveles, list):
        raise RuntimeError("Campo data.niveles no es lista")
    return niveles


def _fetch_alumnos_censo(
    token: str,
    colegio_id: int,
    nivel_id: int,
    grado_id: Optional[int],
    grupo_id: Optional[int],
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = CENSO_ALUMNOS_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
    )
    params: Dict[str, int] = {"nivelId": int(nivel_id)}
    if grado_id is not None:
        params["gradoId"] = int(grado_id)
    if grupo_id is not None:
        params["grupoId"] = int(grupo_id)
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invÃ¡lida"
        raise RuntimeError(message or "Respuesta invÃ¡lida")

    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("alumnos", "items", "rows", "data"):
            value = data.get(key)
            if isinstance(value, list):
                return value
        for value in data.values():
            if isinstance(value, list):
                return value
    raise RuntimeError("Campo data no es lista")


def _fetch_alumnos_censo_by_filters(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = CENSO_ALUMNOS_BY_FILTERS_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
    )
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        raise RuntimeError(message or "Respuesta invalida")

    data = payload.get("data") or []
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")
    return data


def _fetch_login_password_lookup_censo(
    token: str, colegio_id: int, empresa_id: int, ciclo_id: int, timeout: int
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = CENSO_PLANTILLA_EDICION_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
    )
    try:
        response = requests.get(url, headers=headers, params={"descargar": 0}, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invÃ¡lida"
        raise RuntimeError(message or "Respuesta invÃ¡lida")

    data = payload.get("data") or []
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")

    by_alumno_id: Dict[str, Dict[str, str]] = {}
    by_persona_id: Dict[str, Dict[str, str]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        login = str(item.get("login") or "").strip()
        password = str(item.get("password") or "").strip()
        alumno_id = str(item.get("alumnoId") or "").strip()
        persona_id = str(item.get("personaId") or "").strip()

        if login or password:
            value = {"login": login, "password": password}
            if alumno_id:
                by_alumno_id[alumno_id] = value
            if persona_id:
                by_persona_id[persona_id] = value

    return by_alumno_id, by_persona_id


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        text = unicodedata.normalize("NFD", text)
        text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
        return text in {"true", "1", "si", "s", "yes", "y"}
    return False


def _extract_alumno_payload(item: Dict[str, object]) -> Dict[str, object]:
    nested = item.get("alumno")
    if isinstance(nested, dict):
        return nested
    return item


def _resolve_alumno_login_password(
    item: Dict[str, object],
    by_alumno_id: Dict[str, Dict[str, str]],
    by_persona_id: Dict[str, Dict[str, str]],
) -> Tuple[str, str]:
    source = _extract_alumno_payload(item)
    persona = source.get("persona") if isinstance(source.get("persona"), dict) else {}
    login = ""
    password = str(source.get("password") or item.get("password") or "").strip()

    persona_login = persona.get("personaLogin") if isinstance(persona, dict) else None
    if isinstance(persona_login, dict):
        login = str(persona_login.get("login") or "").strip()
    if not login:
        login = str(source.get("login") or item.get("login") or "").strip()

    alumno_id = str(source.get("alumnoId") or item.get("alumnoId") or "").strip()
    persona_id = str(
        persona.get("personaId") or source.get("personaId") or item.get("personaId") or ""
    ).strip()

    if (not login or not password) and alumno_id and alumno_id in by_alumno_id:
        lookup = by_alumno_id[alumno_id]
        if not login:
            login = str(lookup.get("login") or "").strip()
        if not password:
            password = str(lookup.get("password") or "").strip()

    if (not login or not password) and persona_id and persona_id in by_persona_id:
        lookup = by_persona_id[persona_id]
        if not login:
            login = str(lookup.get("login") or "").strip()
        if not password:
            password = str(lookup.get("password") or "").strip()

    return login, password


def _resolve_alumno_dni(item: Dict[str, object]) -> str:
    source = _extract_alumno_payload(item)
    persona = source.get("persona") if isinstance(source.get("persona"), dict) else {}
    for raw in (
        persona.get("idOficial"),
        source.get("idOficial"),
        item.get("idOficial"),
    ):
        value = str(raw or "").strip()
        if value:
            return value
    return ""


def _to_int_or_default(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _grupo_sort_key(grupo_clave: str, grupo_nombre: str) -> Tuple[int, str]:
    clave = (grupo_clave or "").strip().upper()
    if len(clave) == 1 and clave.isalpha():
        return 0, clave
    return 1, (grupo_nombre or "").strip().upper()


def _normalize_plain_text(value: object) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text


def _is_quinto_secundaria_z(
    nivel_id: Optional[int],
    nivel_name: object,
    grado_name: object,
    seccion: object,
) -> bool:
    seccion_txt = _normalize_plain_text(seccion)
    if seccion_txt != "Z":
        return False
    nivel_txt = _normalize_plain_text(nivel_name)
    grado_txt = _normalize_plain_text(grado_name)
    is_secundaria = int(nivel_id or 0) == 40 or "SECUNDARIA" in nivel_txt
    if not is_secundaria:
        return False
    if any(tag in grado_txt for tag in ("QUINTO", "QUINTA", "5TO", "5TA")):
        return True
    return bool(re.search(r"(^|\\D)5(\\D|$)", grado_txt))


def _delete_clase_gestion_escolar(
    token: str, clase_id: int, empresa_id: int, ciclo_id: int, timeout: int
) -> None:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id)
    url = f"{url}/{clase_id}"
    try:
        response = requests.delete(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    if not response.ok:
        status_code = response.status_code
        try:
            payload = response.json()
            message = payload.get("message") if isinstance(payload, dict) else ""
        except ValueError:
            message = ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if response.content:
        try:
            payload = response.json()
        except ValueError:
            return
        if isinstance(payload, dict) and payload.get("success") is False:
            message = payload.get("message") or "Respuesta invÃ¡lida"
            raise RuntimeError(message)


def _delete_alumno_clase_gestion_escolar(
    token: str,
    clase_id: int,
    alumno_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> None:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    base_url = GESTION_ESCOLAR_ALUMNOS_CLASE_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        clase_id=clase_id,
    )
    url = f"{base_url}/{alumno_id}"
    try:
        response = requests.delete(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or payload.get("success") is False:
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invï¿½fÂ¡lida"
        raise RuntimeError(message or "Respuesta invï¿½fÂ¡lida")


def _post_clase_participantes_gestion_escolar(
    token: str,
    clase_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_ids: List[int],
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> None:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_CLASE_PARTICIPANTES_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        nivel_id=nivel_id,
        grado_id=grado_id,
        clase_id=clase_id,
    )
    payload = {"grupos": [int(group_id) for group_id in grupo_ids]}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = data.get("message") if isinstance(data, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(data, dict) or data.get("success") is False:
        message = data.get("message") if isinstance(data, dict) else "Respuesta invalida"
        raise RuntimeError(message or "Respuesta invalida")


def _safe_int(value: object) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_clase_meta(item: Dict[str, object]) -> Optional[Dict[str, object]]:
    base_meta = _extract_clase_base_meta(item)
    if not isinstance(base_meta, dict):
        return None
    if base_meta.get("nivel_id") is None or base_meta.get("grado_id") is None:
        return None
    return base_meta


def _extract_clase_base_meta(item: Dict[str, object]) -> Optional[Dict[str, object]]:
    clase_id = _safe_int(item.get("geClaseId"))
    if clase_id is None:
        return None

    cnc = item.get("colegioNivelCiclo") if isinstance(item.get("colegioNivelCiclo"), dict) else {}
    nivel = cnc.get("nivel") if isinstance(cnc.get("nivel"), dict) else {}
    nivel_id = _safe_int(nivel.get("nivelId"))

    cgg = item.get("colegioGradoGrupo") if isinstance(item.get("colegioGradoGrupo"), dict) else {}
    grado = cgg.get("grado") if isinstance(cgg.get("grado"), dict) else {}
    grupo = cgg.get("grupo") if isinstance(cgg.get("grupo"), dict) else {}
    grado_id = _safe_int(grado.get("gradoId"))
    grupo_id_actual = _safe_int(grupo.get("grupoId"))

    clase_nombre = str(item.get("geClase") or item.get("geClaseClave") or "")
    nivel_nombre = str(nivel.get("nivel") or "")
    grado_nombre = str(grado.get("grado") or grado.get("gradoClave") or "")
    if not grado_nombre:
        grado_nombre = clase_nombre
    grupo_clave_actual = str(grupo.get("grupoClave") or grupo.get("grupo") or "")
    return {
        "clase_id": clase_id,
        "clase_nombre": clase_nombre,
        "nivel_id": nivel_id,
        "nivel_nombre": nivel_nombre,
        "grado_id": grado_id,
        "grado_nombre": grado_nombre,
        "grupo_id_actual": grupo_id_actual,
        "grupo_clave_actual": grupo_clave_actual,
    }


def _is_santillana_inclusiva_class(item: Dict[str, object]) -> bool:
    ge_clase = _normalize_plain_text(item.get("geClase"))
    ge_clase_clave = _normalize_plain_text(item.get("geClaseClave"))
    target = "SANTILLANA INCLUSIVA"
    return target in ge_clase or target in ge_clase_clave


def _is_ingles_por_niveles_class(item: Dict[str, object]) -> bool:
    ge_clase = _normalize_plain_text(item.get("geClase"))
    ge_clase_clave = _normalize_plain_text(item.get("geClaseClave"))
    alias = _normalize_plain_text(item.get("alias"))
    search_text = " ".join(
        part for part in (ge_clase, ge_clase_clave, alias) if part
    )
    return "INGLES" in search_text or "ENGLISH" in search_text


def _participantes_ingles_grade_key(nivel_id: object, grado_id: object) -> str:
    nivel_id_int = _safe_int(nivel_id)
    grado_id_int = _safe_int(grado_id)
    if nivel_id_int is None or grado_id_int is None:
        return ""
    return f"{int(nivel_id_int)}:{int(grado_id_int)}"


def _participantes_ingles_option_key_from_meta(meta: Dict[str, object]) -> str:
    option_key = _participantes_ingles_grade_key(
        meta.get("nivel_id"),
        meta.get("grado_id"),
    )
    if option_key:
        return option_key
    nivel_txt = _normalize_compare_text(meta.get("nivel_nombre"))
    grado_txt = _normalize_compare_text(meta.get("grado_nombre"))
    if not (nivel_txt or grado_txt):
        return ""
    return f"label:{nivel_txt}:{grado_txt}"


def _build_ingles_grade_options_for_participantes(
    clases: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    options_by_key: Dict[str, Dict[str, object]] = {}
    for item in clases:
        if not isinstance(item, dict) or not _is_ingles_por_niveles_class(item):
            continue
        meta = _extract_clase_base_meta(item)
        if not meta:
            continue
        option_key = _participantes_ingles_option_key_from_meta(meta)
        if not option_key:
            continue
        option = options_by_key.setdefault(
            option_key,
            {
                "key": option_key,
                "nivel_id": _safe_int(meta.get("nivel_id")),
                "grado_id": _safe_int(meta.get("grado_id")),
                "nivel_nombre": str(meta.get("nivel_nombre") or "").strip(),
                "grado_nombre": str(meta.get("grado_nombre") or "").strip(),
                "class_names": [],
            },
        )
        class_names = options_by_key[option_key].setdefault("class_names", [])
        class_name = str(meta.get("clase_nombre") or "").strip()
        if class_name and class_name not in class_names:
            class_names.append(class_name)

    options = list(options_by_key.values())
    for option in options:
        class_names = option.get("class_names")
        if isinstance(class_names, list):
            class_names.sort(key=lambda value: _normalize_compare_text(value))
    options.sort(
        key=lambda row: (
            _participantes_nivel_sort_rank(row.get("nivel_nombre")),
            _participantes_grado_sort_rank(row.get("grado_nombre")),
            _normalize_compare_text(row.get("grado_nombre")),
        )
    )
    return options


def _participantes_ingles_grade_checkbox_key(option_key: object) -> str:
    option_key_txt = str(option_key or "").strip().replace(":", "_")
    return f"clases_auto_group_ingles_grade_checkbox_{option_key_txt}"


def _extract_grupo_contratados_count(grupo_entry: Dict[str, object]) -> Optional[int]:
    grupo = grupo_entry.get("grupo") if isinstance(grupo_entry.get("grupo"), dict) else {}
    keys = (
        "alumnosContratados",
        "alumnos_contratados",
        "cantidadAlumnosContratados",
        "cantidadAlumnos",
        "totalAlumnos",
        "alumnos",
        "matriculados",
    )
    for source in (grupo_entry, grupo):
        if not isinstance(source, dict):
            continue
        for key in keys:
            value = _safe_int(source.get(key))
            if value is not None:
                return max(value, 0)
    return None


def _build_grupos_disponibles_por_grado(
    niveles_data: List[Dict[str, object]]
) -> Dict[Tuple[int, int], List[Dict[str, object]]]:
    grouped: Dict[Tuple[int, int], Dict[int, Dict[str, object]]] = {}
    for nivel_entry in niveles_data:
        if not isinstance(nivel_entry, dict):
            continue
        nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
        nivel_id = _safe_int(nivel.get("nivelId"))
        if nivel_id is None:
            continue

        grados = nivel_entry.get("grados") or []
        if not isinstance(grados, list):
            continue
        for grado_entry in grados:
            if not isinstance(grado_entry, dict):
                continue
            grado = grado_entry.get("grado") if isinstance(grado_entry.get("grado"), dict) else {}
            grado_id = _safe_int(grado.get("gradoId"))
            if grado_id is None:
                continue
            key = (nivel_id, grado_id)
            grouped.setdefault(key, {})

            grupos = grado_entry.get("grupos") or []
            if not isinstance(grupos, list):
                continue
            for grupo_entry in grupos:
                if not isinstance(grupo_entry, dict):
                    continue
                grupo = grupo_entry.get("grupo") if isinstance(grupo_entry.get("grupo"), dict) else {}
                grupo_id = _safe_int(grupo.get("grupoId"))
                if grupo_id is None:
                    continue
                contratados = _extract_grupo_contratados_count(grupo_entry)
                if contratados is not None and contratados <= 0:
                    continue
                grouped[key][grupo_id] = {
                    "grupo_id": grupo_id,
                    "grupo_clave": str(grupo.get("grupoClave") or ""),
                    "grupo_nombre": str(grupo.get("grupo") or ""),
                    "alumnos_contratados": contratados,
                }

    result: Dict[Tuple[int, int], List[Dict[str, object]]] = {}
    for key, values in grouped.items():
        result[key] = sorted(
            values.values(),
            key=lambda row: (
                str(row.get("grupo_clave", "")).upper(),
                str(row.get("grupo_nombre", "")).upper(),
            ),
        )
    return result


def _extract_group_hint_from_class_name(clase_nombre: object) -> str:
    text = _normalize_plain_text(clase_nombre)
    if not text:
        return ""
    match = re.search(r"([A-Z])\s*$", text)
    if not match:
        return ""
    return match.group(1)


def _pick_default_group_id(
    clase_nombre: object,
    options: List[Dict[str, object]],
    grupo_id_actual: Optional[int],
) -> Optional[int]:
    if not options:
        return None
    hint = _extract_group_hint_from_class_name(clase_nombre)
    if hint:
        for option in options:
            clave = _normalize_plain_text(option.get("grupo_clave"))
            nombre = _normalize_plain_text(option.get("grupo_nombre"))
            if clave == hint:
                return int(option["grupo_id"])
            match_nombre = re.search(r"GRUPO\s+([A-Z])\b", nombre)
            if match_nombre and match_nombre.group(1) == hint:
                return int(option["grupo_id"])
        # Fallback consecutivo: A->1er grupo, B->2do, ... Z->26vo.
        # Se usa solo si no hubo match directo por clave/nombre.
        if len(hint) == 1 and "A" <= hint <= "Z":
            sorted_options = sorted(
                options,
                key=lambda row: _grupo_sort_key(
                    str(row.get("grupo_clave") or ""),
                    str(row.get("grupo_nombre") or ""),
                ),
            )
            idx = ord(hint) - ord("A")
            if 0 <= idx < len(sorted_options):
                return int(sorted_options[idx]["grupo_id"])
    if grupo_id_actual is not None:
        for option in options:
            if int(option["grupo_id"]) == int(grupo_id_actual):
                return int(option["grupo_id"])
    return int(options[0]["grupo_id"])


def _fetch_grupo_alumnos_count(
    token: str,
    colegio_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    cache: Dict[Tuple[int, int, int], int],
) -> int:
    key = (int(nivel_id), int(grado_id), int(grupo_id))
    if key in cache:
        return int(cache[key])
    alumnos = _fetch_alumnos_censo(
        token=token,
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_id=int(grupo_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
    )
    count = sum(1 for item in alumnos if isinstance(item, dict))
    cache[key] = int(count)
    return int(count)


def _collect_colegios(clases: List[Dict[str, object]]) -> List[Dict[str, object]]:
    colegios: Dict[int, str] = {}
    for item in clases:
        cnc = item.get("colegioNivelCiclo") if isinstance(item, dict) else None
        colegio = cnc.get("colegio") if isinstance(cnc, dict) else None
        if isinstance(colegio, dict):
            colegio_id = colegio.get("colegioId")
            colegio_nombre = colegio.get("colegio", "")
            if colegio_id is not None:
                colegios[int(colegio_id)] = str(colegio_nombre or "")
    return [
        {"colegioId": colegio_id, "colegio": nombre}
        for colegio_id, nombre in sorted(colegios.items())
    ]


def _build_alumno_export_key(
    item: Dict[str, object],
    source: Dict[str, object],
    persona: Dict[str, object],
) -> str:
    for raw in (
        source.get("alumnoId"),
        item.get("alumnoId"),
        persona.get("personaId"),
        persona.get("idOficial"),
        source.get("uuid"),
        item.get("uuid"),
    ):
        text = str(raw or "").strip()
        if text:
            return f"id:{text}"

    persona_login = (
        persona.get("personaLogin") if isinstance(persona.get("personaLogin"), dict) else {}
    )
    login_txt = _normalize_plain_text(
        source.get("login")
        or persona_login.get("login")
        or item.get("login")
    )
    nombre_txt = _normalize_plain_text(persona.get("nombre"))
    ap_pat_txt = _normalize_plain_text(persona.get("apellidoPaterno"))
    ap_mat_txt = _normalize_plain_text(persona.get("apellidoMaterno"))
    if nombre_txt or ap_pat_txt or ap_mat_txt or login_txt:
        return f"sig:{nombre_txt}|{ap_pat_txt}|{ap_mat_txt}|{login_txt}"
    return ""


def _normalize_compare_text(value: object) -> str:
    text = _normalize_plain_text(value)
    # Treat hyphens, accents, and punctuation variants as equivalent during compare.
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return text.strip()


def _normalize_compare_apellido(value: object) -> str:
    # For surnames, compare "RIZOPATRON", "RIZO PATRON", and "RIZO-PATRON" as equal.
    return re.sub(r"[^A-Z0-9]+", "", _normalize_plain_text(value))


def _normalize_compare_id(value: object) -> str:
    return re.sub(r"\W+", "", _normalize_compare_text(value))


def _normalize_seccion_key(value: object) -> str:
    text = _normalize_compare_text(value)
    if text.startswith("GRUPO "):
        text = text[6:].strip()
    if len(text) > 1:
        text = text[-1]
    return text


def _build_contexts_for_nivel_grado(
    niveles: List[Dict[str, object]],
    nivel_id: Optional[int] = None,
    grado_id: Optional[int] = None,
) -> List[Dict[str, object]]:
    contexts: List[Dict[str, object]] = []
    seen: Set[Tuple[int, int, int]] = set()
    for nivel_entry in niveles:
        if not isinstance(nivel_entry, dict):
            continue
        nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
        nivel_id_tmp = _safe_int(nivel.get("nivelId"))
        if nivel_id_tmp is None:
            continue
        if nivel_id is not None and nivel_id_tmp != int(nivel_id):
            continue
        nivel_nombre = str(nivel.get("nivel") or "").strip()
        grados = nivel_entry.get("grados") or []
        if not isinstance(grados, list):
            continue
        for grado_entry in grados:
            if not isinstance(grado_entry, dict):
                continue
            grado = grado_entry.get("grado") if isinstance(grado_entry.get("grado"), dict) else {}
            grado_id_tmp = _safe_int(grado.get("gradoId"))
            if grado_id_tmp is None:
                continue
            if grado_id is not None and grado_id_tmp != int(grado_id):
                continue
            grado_nombre = str(grado.get("grado") or "").strip()
            grupos = grado_entry.get("grupos") or []
            if not isinstance(grupos, list):
                continue
            for grupo_entry in grupos:
                if not isinstance(grupo_entry, dict):
                    continue
                grupo = grupo_entry.get("grupo") if isinstance(grupo_entry.get("grupo"), dict) else {}
                grupo_id = _safe_int(grupo.get("grupoId"))
                if grupo_id is None:
                    continue
                key = (int(nivel_id_tmp), int(grado_id_tmp), int(grupo_id))
                if key in seen:
                    continue
                seen.add(key)
                seccion = str(grupo.get("grupoClave") or grupo.get("grupo") or "").strip()
                seccion_norm = _normalize_seccion_key(seccion)
                contexts.append(
                    {
                        "nivel_id": int(nivel_id_tmp),
                        "nivel": nivel_nombre,
                        "grado_id": int(grado_id_tmp),
                        "grado": grado_nombre,
                        "grupo_id": int(grupo_id),
                        "seccion": seccion,
                        "seccion_norm": seccion_norm,
                    }
                )
    contexts.sort(
        key=lambda row: (
            int(row.get("nivel_id") or 0),
            int(row.get("grado_id") or 0),
            _grupo_sort_key(
                str(row.get("seccion_norm") or ""),
                str(row.get("seccion") or ""),
            ),
            int(row.get("grupo_id") or 0),
        )
    )
    return contexts


def _flatten_censo_alumno_for_auto_plan(
    item: Dict[str, object],
    fallback: Dict[str, object],
) -> Dict[str, object]:
    persona = item.get("persona") if isinstance(item.get("persona"), dict) else {}
    persona_login = (
        persona.get("personaLogin") if isinstance(persona.get("personaLogin"), dict) else {}
    )
    nivel = item.get("nivel") if isinstance(item.get("nivel"), dict) else {}
    grado = item.get("grado") if isinstance(item.get("grado"), dict) else {}
    grupo = item.get("grupo") if isinstance(item.get("grupo"), dict) else {}
    seccion = str(
        grupo.get("grupoClave")
        or grupo.get("grupo")
        or fallback.get("seccion")
        or ""
    ).strip()
    seccion_norm = _normalize_seccion_key(seccion)
    return {
        "alumno_id": _safe_int(item.get("alumnoId")),
        "persona_id": _safe_int(persona.get("personaId")),
        "nombre": str(persona.get("nombre") or "").strip(),
        "apellido_paterno": str(persona.get("apellidoPaterno") or "").strip(),
        "apellido_materno": str(persona.get("apellidoMaterno") or "").strip(),
        "nombre_completo": str(persona.get("nombreCompleto") or "").strip(),
        "id_oficial": str(persona.get("idOficial") or "").strip(),
        "login": str(persona_login.get("login") or item.get("login") or "").strip(),
        "password": str(item.get("password") or "").strip(),
        "nivel_id": _safe_int(nivel.get("nivelId")) or _safe_int(fallback.get("nivel_id")),
        "grado_id": _safe_int(grado.get("gradoId")) or _safe_int(fallback.get("grado_id")),
        "grupo_id": _safe_int(grupo.get("grupoId")) or _safe_int(fallback.get("grupo_id")),
        "nivel": str(nivel.get("nivel") or fallback.get("nivel") or "").strip(),
        "grado": str(grado.get("grado") or fallback.get("grado") or "").strip(),
        "seccion": seccion,
        "seccion_norm": seccion_norm,
        "activo": _to_bool(item.get("activo")),
        "con_pago": _to_bool(item.get("conPago")),
        "fecha_desde": str(item.get("fechaDesde") or "").strip(),
    }


def _participantes_nivel_sort_rank(nivel_nombre: object) -> int:
    text = _normalize_plain_text(nivel_nombre)
    if any(tag in text for tag in ("INICIAL", "PREESCOLAR", "PRESCHOOL", "PREPRIMARY", "PRE PRIMARY")):
        return 0
    if any(tag in text for tag in ("PRIMARIA", "PRIMARY")):
        return 1
    if any(tag in text for tag in ("SECUNDARIA", "SECONDARY")):
        return 2
    return 9


def _participantes_grado_sort_rank(grado_nombre: object) -> int:
    text = _normalize_plain_text(grado_nombre)
    match = re.search(r"(?<!\d)(\d{1,2})(?!\d)", text)
    if match:
        return int(match.group(1))

    word_to_num = {
        "PRIMER": 1,
        "PRIMERO": 1,
        "SEGUNDO": 2,
        "TERCERO": 3,
        "CUARTO": 4,
        "QUINTO": 5,
        "SEXTO": 6,
        "SEPTIMO": 7,
        "SETIMO": 7,
        "OCTAVO": 8,
        "NOVENO": 9,
        "DECIMO": 10,
    }
    for word, number in word_to_num.items():
        if re.search(rf"\b{word}\b", text):
            return number
    return 99


def _participantes_row_target_seccion(row: Dict[str, object]) -> str:
    target_group_id = _safe_int(row.get("selected_group_id"))
    if target_group_id is None:
        target_group_id = _safe_int(row.get("grupo_id_actual"))

    options = row.get("options") if isinstance(row.get("options"), list) else []
    if target_group_id is not None:
        for option in options:
            if not isinstance(option, dict):
                continue
            option_group_id = _safe_int(option.get("grupo_id"))
            if option_group_id is None or int(option_group_id) != int(target_group_id):
                continue
            seccion = _normalize_seccion_key(
                option.get("grupo_clave") or option.get("grupo_nombre") or ""
            )
            if seccion:
                return seccion

    seccion_actual = _normalize_seccion_key(row.get("grupo_clave_actual") or "")
    if seccion_actual:
        return seccion_actual
    return _normalize_seccion_key(row.get("clase_nombre") or "")


def _participantes_auto_row_sort_key(row: Dict[str, object]) -> Tuple[object, ...]:
    seccion = _participantes_row_target_seccion(row)
    clase_nombre = str(row.get("clase_nombre") or "").strip()
    return (
        _participantes_nivel_sort_rank(row.get("nivel_nombre")),
        _participantes_grado_sort_rank(row.get("grado_nombre")),
        _grupo_sort_key(seccion, seccion),
        _normalize_compare_text(clase_nombre),
        int(row.get("clase_id") or 0),
    )


def _build_auto_group_rows_for_participantes(
    clases: List[Dict[str, object]],
    niveles_data: List[Dict[str, object]],
    exclude_ingles_por_niveles: bool = False,
    ingles_grade_keys: Optional[Set[str]] = None,
) -> Tuple[List[Dict[str, object]], List[str]]:
    grupos_por_grado = _build_grupos_disponibles_por_grado(niveles_data)
    rows_auto: List[Dict[str, object]] = []
    warnings_auto: List[str] = []

    for item in clases:
        if not isinstance(item, dict):
            continue
        if _is_santillana_inclusiva_class(item):
            warnings_auto.append(
                f"Clase omitida por exclusion Santillana inclusiva: {item.get('geClaseId')}"
            )
            continue
        base_meta = _extract_clase_base_meta(item)
        if not base_meta:
            warnings_auto.append(
                f"Clase omitida por metadata incompleta: {item.get('geClaseId')}"
            )
            continue
        ingles_grade_key = _participantes_ingles_option_key_from_meta(base_meta)
        if (
            exclude_ingles_por_niveles
            and _is_ingles_por_niveles_class(item)
            and ingles_grade_key
            and ingles_grade_key in (ingles_grade_keys or set())
        ):
            rows_auto.append(
                {
                    **base_meta,
                    "options": [],
                    "selected_group_id": base_meta.get("grupo_id_actual"),
                    "clear_current_students": True,
                }
            )
            continue
        meta = _extract_clase_meta(item)
        if not meta:
            warnings_auto.append(
                f"Clase omitida por metadata incompleta: {item.get('geClaseId')}"
            )
            continue

        key_grado = (int(meta["nivel_id"]), int(meta["grado_id"]))
        options = grupos_por_grado.get(key_grado) or []
        if not options:
            warnings_auto.append(
                f"Clase {meta['clase_id']} sin grupos disponibles para su grado."
            )
            continue

        default_group_id = _pick_default_group_id(
            meta["clase_nombre"],
            options,
            meta.get("grupo_id_actual"),
        )
        if default_group_id is None:
            warnings_auto.append(f"Clase {meta['clase_id']} sin grupo sugerido.")
            continue

        rows_auto.append(
            {
                **meta,
                "options": options,
                "selected_group_id": int(default_group_id),
                "clear_current_students": False,
            }
        )

    rows_auto.sort(key=_participantes_auto_row_sort_key)
    return rows_auto, warnings_auto


def _resolve_auto_group_selection(
    row: Dict[str, object],
    prefer_session_state: bool = True,
) -> Optional[int]:
    options = row.get("options") if isinstance(row.get("options"), list) else []
    option_ids = [int(opt["grupo_id"]) for opt in options if _safe_int(opt.get("grupo_id")) is not None]
    if not option_ids:
        return None

    selected_default = _safe_int(row.get("selected_group_id"))
    auto_group_id = _pick_default_group_id(
        row.get("clase_nombre"),
        options,
        _safe_int(row.get("grupo_id_actual")),
    )
    clase_id = _safe_int(row.get("clase_id"))
    selected_group_id = None
    if prefer_session_state and clase_id is not None:
        selected_group_id = _safe_int(
            st.session_state.get(
                f"clases_auto_group_select_{int(clase_id)}",
                row.get("selected_group_id"),
            )
        )
    if selected_group_id is None:
        selected_group_id = selected_default
    if selected_group_id is None:
        selected_group_id = _safe_int(auto_group_id)
    if selected_group_id is not None and int(selected_group_id) in option_ids:
        return int(selected_group_id)
    if auto_group_id is not None and int(auto_group_id) in option_ids:
        return int(auto_group_id)
    if selected_default is not None and int(selected_default) in option_ids:
        return int(selected_default)
    return int(option_ids[0])


def _extract_alumno_ids_from_clase_data(clase_data: Dict[str, object]) -> Set[int]:
    alumno_ids: Set[int] = set()
    clase_alumnos = clase_data.get("claseAlumnos") if isinstance(clase_data, dict) else []
    if not isinstance(clase_alumnos, list):
        return alumno_ids
    for entry in clase_alumnos:
        if not isinstance(entry, dict):
            continue
        alumno = entry.get("alumno") if isinstance(entry.get("alumno"), dict) else {}
        alumno_id = _safe_int(alumno.get("alumnoId"))
        if alumno_id is None:
            continue
        alumno_ids.add(int(alumno_id))
    return alumno_ids


def _make_participantes_sync_summary(total_clases: int = 0) -> Dict[str, int]:
    return {
        "clases_total": int(total_clases),
        "clases_ok": 0,
        "clases_skip": 0,
        "clases_error": 0,
        "grupos_consultados": 0,
        "alumnos_objetivo": 0,
        "alumnos_sin_cambios": 0,
        "eliminados_ok": 0,
        "eliminados_error": 0,
        "agregados_ok": 0,
        "agregados_error": 0,
    }


def _build_participantes_group_error_lines(
    group_errors: Dict[Tuple[int, int, int], str]
) -> List[str]:
    return [
        "nivelId={nivel} gradoId={grado} grupoId={grupo}: {err}".format(
            nivel=key[0],
            grado=key[1],
            grupo=key[2],
            err=message,
        )
        for key, message in sorted(group_errors.items(), key=lambda item: item[0])
    ]


class _ParticipantesSyncCancelled(RuntimeError):
    def __init__(
        self,
        summary: Dict[str, int],
        detail_rows: List[Dict[str, object]],
        group_errors: Dict[Tuple[int, int, int], str],
    ) -> None:
        super().__init__("Proceso cancelado por el usuario.")
        self.summary = dict(summary)
        self.detail_rows = [dict(item) for item in detail_rows]
        self.group_error_lines = _build_participantes_group_error_lines(group_errors)


def _copy_participantes_sync_job(job: Dict[str, object]) -> Dict[str, object]:
    snapshot = dict(job)
    for key in (
        "status_messages",
        "warnings",
        "group_error_lines",
        "detail_rows",
        "ingles_grade_keys",
    ):
        value = snapshot.get(key)
        snapshot[key] = list(value) if isinstance(value, list) else []
    summary = snapshot.get("summary")
    snapshot["summary"] = dict(summary) if isinstance(summary, dict) else {}
    return snapshot


def _set_participantes_sync_job(job_id: str, **fields: object) -> None:
    if not str(job_id or "").strip():
        return
    with _PARTICIPANTES_SYNC_LOCK:
        job = _PARTICIPANTES_SYNC_JOBS.get(str(job_id))
        if not isinstance(job, dict):
            return
        for key, value in fields.items():
            if isinstance(value, dict):
                job[key] = dict(value)
            elif isinstance(value, list):
                job[key] = list(value)
            else:
                job[key] = value


def _append_participantes_sync_job_message(job_id: str, message: object) -> None:
    msg = str(message or "").strip()
    if not msg:
        return
    with _PARTICIPANTES_SYNC_LOCK:
        job = _PARTICIPANTES_SYNC_JOBS.get(str(job_id))
        if not isinstance(job, dict):
            return
        messages = list(job.get("status_messages") or [])
        if not messages or messages[-1] != msg:
            messages.append(msg)
        job["status_messages"] = messages[-_PARTICIPANTES_SYNC_STATUS_LIMIT:]


def _get_participantes_sync_job(job_id: object) -> Optional[Dict[str, object]]:
    job_key = str(job_id or "").strip()
    if not job_key:
        return None
    with _PARTICIPANTES_SYNC_LOCK:
        job = _PARTICIPANTES_SYNC_JOBS.get(job_key)
        if not isinstance(job, dict):
            return None
        return _copy_participantes_sync_job(job)


def _get_participantes_sync_job_id_for_scope(
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
) -> Optional[str]:
    scope = (int(empresa_id), int(ciclo_id), int(colegio_id))
    with _PARTICIPANTES_SYNC_LOCK:
        job_id = _PARTICIPANTES_SYNC_SCOPE_TO_JOB.get(scope)
        if not str(job_id or "").strip():
            return None
        if not isinstance(_PARTICIPANTES_SYNC_JOBS.get(str(job_id)), dict):
            return None
        return str(job_id)


def _is_participantes_sync_job_active(job: Optional[Dict[str, object]]) -> bool:
    if not isinstance(job, dict):
        return False
    return str(job.get("state") or "").strip() in {"starting", "running"}


def _request_cancel_participantes_sync_job(job_id: object) -> bool:
    job_key = str(job_id or "").strip()
    if not job_key:
        return False
    with _PARTICIPANTES_SYNC_LOCK:
        job = _PARTICIPANTES_SYNC_JOBS.get(job_key)
        if not isinstance(job, dict):
            return False
        state = str(job.get("state") or "").strip()
        if state not in {"starting", "running"}:
            return False
        if bool(job.get("cancel_requested")):
            return True
        job["cancel_requested"] = True
    _append_participantes_sync_job_message(
        job_key,
        "Cancelacion solicitada. Cerrando el proceso en segundo plano...",
    )
    return True


def _is_participantes_sync_job_cancel_requested(job_id: object) -> bool:
    job_key = str(job_id or "").strip()
    if not job_key:
        return False
    with _PARTICIPANTES_SYNC_LOCK:
        job = _PARTICIPANTES_SYNC_JOBS.get(job_key)
        if not isinstance(job, dict):
            return False
        return bool(job.get("cancel_requested"))


def _run_participantes_sync_job(
    job_id: str,
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    exclude_ingles_por_niveles: bool,
    ingles_grade_keys: Tuple[str, ...],
) -> None:
    _set_participantes_sync_job(job_id, state="running")
    _append_participantes_sync_job_message(job_id, "Preparando sincronizacion automatica...")

    summary_auto = _make_participantes_sync_summary()
    detail_rows_auto: List[Dict[str, object]] = []
    warnings_auto: List[str] = []
    group_error_lines: List[str] = []

    try:
        clases = _fetch_clases_gestion_escolar(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
            ordered=True,
        )
        if _is_participantes_sync_job_cancel_requested(job_id):
            raise _ParticipantesSyncCancelled(summary_auto, detail_rows_auto, {})

        niveles_data = _fetch_niveles_grados_grupos_censo(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
        if _is_participantes_sync_job_cancel_requested(job_id):
            raise _ParticipantesSyncCancelled(summary_auto, detail_rows_auto, {})

        rows_auto, warnings_auto = _build_auto_group_rows_for_participantes(
            clases=clases,
            niveles_data=niveles_data,
            exclude_ingles_por_niveles=bool(exclude_ingles_por_niveles),
            ingles_grade_keys={str(item) for item in ingles_grade_keys if str(item).strip()},
        )
        summary_auto = _make_participantes_sync_summary(len(rows_auto))
        _set_participantes_sync_job(
            job_id,
            summary=summary_auto,
            warnings=warnings_auto,
        )
        _append_participantes_sync_job_message(
            job_id,
            "Clases detectadas={total} | Sincronizables={sync} | Advertencias={warn} | Ingles por niveles={ingles}".format(
                total=len(clases),
                sync=len(rows_auto),
                warn=len(warnings_auto),
                ingles="Si" if exclude_ingles_por_niveles else "No",
            ),
        )

        if rows_auto:
            summary_auto, detail_rows_auto, group_error_lines = _sync_participantes_por_grado_seccion(
                token=token,
                colegio_id=int(colegio_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
                rows_auto=rows_auto,
                niveles_data=niveles_data,
                on_status=lambda message: _append_participantes_sync_job_message(job_id, message),
                on_summary=lambda summary: _set_participantes_sync_job(job_id, summary=summary),
                prefer_session_state=False,
                is_cancelled=lambda: _is_participantes_sync_job_cancel_requested(job_id),
            )
        else:
            _append_participantes_sync_job_message(
                job_id,
                "No se encontraron clases con grado y seccion resolubles para sincronizar.",
            )
    except _ParticipantesSyncCancelled as exc:
        _set_participantes_sync_job(
            job_id,
            state="cancelled",
            summary=exc.summary,
            detail_rows=exc.detail_rows,
            warnings=warnings_auto,
            group_error_lines=exc.group_error_lines,
            error="",
        )
        _append_participantes_sync_job_message(job_id, "Proceso cancelado por el usuario.")
        return
    except Exception as exc:
        _set_participantes_sync_job(
            job_id,
            state="error",
            summary=summary_auto,
            detail_rows=detail_rows_auto,
            warnings=warnings_auto,
            group_error_lines=group_error_lines,
            error=str(exc),
        )
        _append_participantes_sync_job_message(job_id, f"Error: {exc}")
        return

    _set_participantes_sync_job(
        job_id,
        state="done",
        summary=summary_auto,
        detail_rows=detail_rows_auto,
        warnings=warnings_auto,
        group_error_lines=group_error_lines,
        error="",
    )
    _append_participantes_sync_job_message(job_id, "Proceso completado.")


def _start_participantes_sync_job(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    exclude_ingles_por_niveles: bool = False,
    ingles_grade_keys: Optional[Sequence[str]] = None,
) -> str:
    scope = (int(empresa_id), int(ciclo_id), int(colegio_id))
    ingles_grade_keys_tuple = tuple(
        str(item).strip()
        for item in (ingles_grade_keys or [])
        if str(item).strip()
    )
    with _PARTICIPANTES_SYNC_LOCK:
        existing_id = _PARTICIPANTES_SYNC_SCOPE_TO_JOB.get(scope)
        existing_job = _PARTICIPANTES_SYNC_JOBS.get(str(existing_id)) if existing_id else None
        if isinstance(existing_job, dict) and str(existing_job.get("state") or "").strip() in {
            "starting",
            "running",
        }:
            return str(existing_id)

        job_id = uuid4().hex
        _PARTICIPANTES_SYNC_JOBS[job_id] = {
            "job_id": job_id,
            "scope": scope,
            "state": "starting",
            "cancel_requested": False,
            "exclude_ingles_por_niveles": bool(exclude_ingles_por_niveles),
            "ingles_grade_keys": list(ingles_grade_keys_tuple),
            "status_messages": [],
            "summary": _make_participantes_sync_summary(),
            "warnings": [],
            "group_error_lines": [],
            "detail_rows": [],
            "error": "",
        }
        _PARTICIPANTES_SYNC_SCOPE_TO_JOB[scope] = job_id

    worker = threading.Thread(
        target=_run_participantes_sync_job,
        args=(
            job_id,
            str(token or "").strip(),
            int(colegio_id),
            int(empresa_id),
            int(ciclo_id),
            int(timeout),
            bool(exclude_ingles_por_niveles),
            ingles_grade_keys_tuple,
        ),
        daemon=True,
        name=f"participantes-sync-{job_id[:8]}",
    )
    with _PARTICIPANTES_SYNC_LOCK:
        job = _PARTICIPANTES_SYNC_JOBS.get(job_id)
        if isinstance(job, dict):
            job["thread"] = worker
    worker.start()
    return job_id


def _sync_participantes_por_grado_seccion(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    rows_auto: List[Dict[str, object]],
    niveles_data: List[Dict[str, object]],
    on_status: Optional[Callable[[str], None]] = None,
    on_summary: Optional[Callable[[Dict[str, int]], None]] = None,
    prefer_session_state: bool = False,
    is_cancelled: Optional[Callable[[], bool]] = None,
) -> Tuple[Dict[str, int], List[Dict[str, object]], List[str]]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message or ""))
            except Exception:
                pass

    def _emit_summary() -> None:
        if callable(on_summary):
            try:
                on_summary(dict(summary))
            except Exception:
                pass

    def _raise_if_cancelled() -> None:
        if not callable(is_cancelled):
            return
        try:
            cancelled = bool(is_cancelled())
        except Exception:
            cancelled = False
        if cancelled:
            raise _ParticipantesSyncCancelled(summary, detail_rows, group_errors)

    contexts = _build_contexts_for_nivel_grado(niveles=niveles_data)
    context_by_key: Dict[Tuple[int, int, int], Dict[str, object]] = {}
    for ctx in contexts:
        nivel_id = _safe_int(ctx.get("nivel_id"))
        grado_id = _safe_int(ctx.get("grado_id"))
        grupo_id = _safe_int(ctx.get("grupo_id"))
        if nivel_id is None or grado_id is None or grupo_id is None:
            continue
        context_by_key[(int(nivel_id), int(grado_id), int(grupo_id))] = ctx

    activos_by_group: Dict[Tuple[int, int, int], Dict[int, Dict[str, object]]] = {}
    group_errors: Dict[Tuple[int, int, int], str] = {}
    detail_rows: List[Dict[str, object]] = []
    summary = _make_participantes_sync_summary(len(rows_auto))
    _emit_summary()

    total_clases = len(rows_auto)
    for idx, row in enumerate(rows_auto, start=1):
        _raise_if_cancelled()
        clase_id = _safe_int(row.get("clase_id"))
        nivel_id = _safe_int(row.get("nivel_id"))
        grado_id = _safe_int(row.get("grado_id"))
        clear_current_students = bool(row.get("clear_current_students"))
        clase_nombre = str(row.get("clase_nombre") or "").strip()
        if clase_id is None:
            summary["clases_error"] += 1
            detail_rows.append(
                {
                    "Clase ID": row.get("clase_id") or "",
                    "Clase": clase_nombre,
                    "Nivel": row.get("nivel_nombre") or "",
                    "Grado": row.get("grado_nombre") or "",
                    "Seccion": "",
                    "Activos objetivo": 0,
                    "Actuales": 0,
                    "Agregar": 0,
                    "Eliminar": 0,
                    "Resultado": "Error",
                    "Detalle": "Metadata incompleta de clase.",
                }
            )
            _emit_summary()
            continue

        if clear_current_students:
            _status(
                "Vaciando clase de Ingles {idx}/{total}: {clase_id} | {clase}".format(
                    idx=idx,
                    total=total_clases,
                    clase_id=int(clase_id),
                    clase=clase_nombre or "-",
                )
            )
            seccion_destino = _normalize_seccion_key(row.get("grupo_clave_actual") or "")
            try:
                clase_data = _fetch_alumnos_clase_gestion_escolar(
                    token=token,
                    clase_id=int(clase_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as exc:
                summary["clases_error"] += 1
                detail_rows.append(
                    {
                        "Clase ID": int(clase_id),
                        "Clase": clase_nombre,
                        "Nivel": row.get("nivel_nombre") or "",
                        "Grado": row.get("grado_nombre") or "",
                        "Seccion": seccion_destino,
                        "Activos objetivo": 0,
                        "Actuales": 0,
                        "Agregar": 0,
                        "Eliminar": 0,
                        "Resultado": "Error",
                        "Detalle": f"No se pudo listar alumnos actuales: {exc}",
                    }
                )
                _emit_summary()
                continue

            alumnos_actuales_ids = _extract_alumno_ids_from_clase_data(clase_data)
            to_remove = sorted(alumnos_actuales_ids)
            remove_errors: List[str] = []
            for alumno_id in to_remove:
                _raise_if_cancelled()
                try:
                    _delete_alumno_clase_gestion_escolar(
                        token=token,
                        clase_id=int(clase_id),
                        alumno_id=int(alumno_id),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                    )
                    summary["eliminados_ok"] += 1
                except Exception as exc:
                    summary["eliminados_error"] += 1
                    remove_errors.append(f"{int(alumno_id)}: {exc}")

            if remove_errors:
                summary["clases_error"] += 1
                resultado = "Parcial" if to_remove else "Error"
                detalle_txt = (
                    f"Vaciado de clase de Ingles con errores al eliminar={len(remove_errors)}"
                )
            elif not to_remove:
                summary["clases_skip"] += 1
                resultado = "Sin cambios"
                detalle_txt = "La clase de Ingles ya no tenia alumnos."
            else:
                summary["clases_ok"] += 1
                resultado = "OK"
                detalle_txt = "Se quitaron todos los alumnos de la clase de Ingles."

            detail_rows.append(
                {
                    "Clase ID": int(clase_id),
                    "Clase": clase_nombre,
                    "Nivel": row.get("nivel_nombre") or "",
                    "Grado": row.get("grado_nombre") or "",
                    "Seccion": seccion_destino,
                    "Activos objetivo": 0,
                    "Actuales": len(alumnos_actuales_ids),
                    "Agregar": 0,
                    "Eliminar": len(to_remove),
                    "Resultado": resultado,
                    "Detalle": detalle_txt,
                }
            )
            _emit_summary()
            continue

        if nivel_id is None or grado_id is None:
            summary["clases_error"] += 1
            detail_rows.append(
                {
                    "Clase ID": int(clase_id),
                    "Clase": clase_nombre,
                    "Nivel": row.get("nivel_nombre") or "",
                    "Grado": row.get("grado_nombre") or "",
                    "Seccion": "",
                    "Activos objetivo": 0,
                    "Actuales": 0,
                    "Agregar": 0,
                    "Eliminar": 0,
                    "Resultado": "Error",
                    "Detalle": "Metadata incompleta de clase.",
                }
            )
            _emit_summary()
            continue

        selected_group_id = _resolve_auto_group_selection(
            row,
            prefer_session_state=prefer_session_state,
        )
        if selected_group_id is None:
            summary["clases_error"] += 1
            detail_rows.append(
                {
                    "Clase ID": int(clase_id),
                    "Clase": clase_nombre,
                    "Nivel": row.get("nivel_nombre") or "",
                    "Grado": row.get("grado_nombre") or "",
                    "Seccion": "",
                    "Activos objetivo": 0,
                    "Actuales": 0,
                    "Agregar": 0,
                    "Eliminar": 0,
                    "Resultado": "Error",
                    "Detalle": "No se pudo resolver el grupo destino.",
                }
            )
            _emit_summary()
            continue

        context_key = (int(nivel_id), int(grado_id), int(selected_group_id))
        ctx = context_by_key.get(context_key)
        seccion_destino = ""
        if isinstance(ctx, dict):
            seccion_destino = _normalize_seccion_key(
                ctx.get("seccion_norm") or ctx.get("seccion") or ""
            )
        if not seccion_destino:
            for opt in row.get("options", []):
                if _safe_int(opt.get("grupo_id")) == int(selected_group_id):
                    seccion_destino = _normalize_seccion_key(
                        opt.get("grupo_clave") or opt.get("grupo_nombre") or ""
                    )
                    break

        _status(
            "Sincronizando clase {idx}/{total}: {clase_id} | {clase}".format(
                idx=idx,
                total=total_clases,
                clase_id=int(clase_id),
                clase=clase_nombre or "-",
            )
        )

        if ctx is None:
            summary["clases_error"] += 1
            detail_rows.append(
                {
                    "Clase ID": int(clase_id),
                    "Clase": clase_nombre,
                    "Nivel": row.get("nivel_nombre") or "",
                    "Grado": row.get("grado_nombre") or "",
                    "Seccion": seccion_destino,
                    "Activos objetivo": 0,
                    "Actuales": 0,
                    "Agregar": 0,
                    "Eliminar": 0,
                    "Resultado": "Error",
                    "Detalle": "No existe contexto de censo para el grupo destino.",
                }
            )
            _emit_summary()
            continue

        if context_key not in activos_by_group and context_key not in group_errors:
            try:
                _status(
                    "Consultando alumnos activos: nivel={nivel} grado={grado} grupo={grupo}".format(
                        nivel=int(nivel_id),
                        grado=int(grado_id),
                        grupo=int(selected_group_id),
                    )
                )
                alumnos_ctx = _fetch_alumnos_censo(
                    token=token,
                    colegio_id=int(colegio_id),
                    nivel_id=int(nivel_id),
                    grado_id=int(grado_id),
                    grupo_id=int(selected_group_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as exc:
                group_errors[context_key] = str(exc)
            else:
                activos_tmp: Dict[int, Dict[str, object]] = {}
                for item in alumnos_ctx:
                    if not isinstance(item, dict):
                        continue
                    flat = _flatten_censo_alumno_for_auto_plan(item=item, fallback=ctx)
                    if not _to_bool(flat.get("activo")):
                        continue
                    alumno_id = _safe_int(flat.get("alumno_id"))
                    if alumno_id is None:
                        continue
                    activos_tmp[int(alumno_id)] = flat
                activos_by_group[context_key] = activos_tmp
                summary["grupos_consultados"] += 1

        if context_key in group_errors:
            summary["clases_error"] += 1
            detail_rows.append(
                {
                    "Clase ID": int(clase_id),
                    "Clase": clase_nombre,
                    "Nivel": row.get("nivel_nombre") or "",
                    "Grado": row.get("grado_nombre") or "",
                    "Seccion": seccion_destino,
                    "Activos objetivo": 0,
                    "Actuales": 0,
                    "Agregar": 0,
                    "Eliminar": 0,
                    "Resultado": "Error",
                    "Detalle": f"Error al listar alumnos activos: {group_errors[context_key]}",
                }
            )
            _emit_summary()
            continue

        alumnos_objetivo = activos_by_group.get(context_key) or {}
        alumnos_objetivo_ids = set(alumnos_objetivo.keys())
        summary["alumnos_objetivo"] += len(alumnos_objetivo_ids)

        try:
            clase_data = _fetch_alumnos_clase_gestion_escolar(
                token=token,
                clase_id=int(clase_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
            )
        except Exception as exc:
            summary["clases_error"] += 1
            detail_rows.append(
                {
                    "Clase ID": int(clase_id),
                    "Clase": clase_nombre,
                    "Nivel": row.get("nivel_nombre") or "",
                    "Grado": row.get("grado_nombre") or "",
                    "Seccion": seccion_destino,
                    "Activos objetivo": len(alumnos_objetivo_ids),
                    "Actuales": 0,
                    "Agregar": 0,
                    "Eliminar": 0,
                    "Resultado": "Error",
                    "Detalle": f"No se pudo listar alumnos actuales: {exc}",
                }
            )
            _emit_summary()
            continue

        alumnos_actuales_ids = _extract_alumno_ids_from_clase_data(clase_data)
        alumnos_comunes = alumnos_actuales_ids & alumnos_objetivo_ids
        to_remove = sorted(alumnos_actuales_ids - alumnos_objetivo_ids)
        to_add = sorted(alumnos_objetivo_ids - alumnos_actuales_ids)
        summary["alumnos_sin_cambios"] += len(alumnos_comunes)

        remove_errors: List[str] = []
        add_errors: List[str] = []

        for alumno_id in to_remove:
            _raise_if_cancelled()
            try:
                _delete_alumno_clase_gestion_escolar(
                    token=token,
                    clase_id=int(clase_id),
                    alumno_id=int(alumno_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
                summary["eliminados_ok"] += 1
            except Exception as exc:
                summary["eliminados_error"] += 1
                remove_errors.append(f"{int(alumno_id)}: {exc}")

        for alumno_id in to_add:
            _raise_if_cancelled()
            ok_assign, msg_assign = _asignar_alumno_a_clase_web(
                token=token,
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                clase_id=int(clase_id),
                alumno_id=int(alumno_id),
                timeout=int(timeout),
            )
            if ok_assign:
                summary["agregados_ok"] += 1
            else:
                summary["agregados_error"] += 1
                add_errors.append(f"{int(alumno_id)}: {msg_assign}")

        if remove_errors or add_errors:
            summary["clases_error"] += 1
            resultado = "Parcial" if (to_remove or to_add) else "Error"
            detalle = []
            if remove_errors:
                detalle.append(f"Eliminar error={len(remove_errors)}")
            if add_errors:
                detalle.append(f"Agregar error={len(add_errors)}")
            detalle_txt = " | ".join(detalle)
        elif not to_remove and not to_add:
            summary["clases_skip"] += 1
            resultado = "Sin cambios"
            detalle_txt = "La clase ya estaba sincronizada."
        else:
            summary["clases_ok"] += 1
            resultado = "OK"
            detalle_txt = "Sincronizacion aplicada."

        detail_rows.append(
            {
                "Clase ID": int(clase_id),
                "Clase": clase_nombre,
                "Nivel": row.get("nivel_nombre") or "",
                "Grado": row.get("grado_nombre") or "",
                "Seccion": seccion_destino,
                "Activos objetivo": len(alumnos_objetivo_ids),
                "Actuales": len(alumnos_actuales_ids),
                "Agregar": len(to_add),
                "Eliminar": len(to_remove),
                "Resultado": resultado,
                "Detalle": detalle_txt,
            }
        )

        row["selected_group_id"] = int(selected_group_id)
        row["grupo_id_actual"] = int(selected_group_id)
        if seccion_destino:
            row["grupo_clave_actual"] = seccion_destino
        _emit_summary()

    group_error_lines = _build_participantes_group_error_lines(group_errors)
    return summary, detail_rows, group_error_lines


def _build_grupo_id_by_seccion_from_contexts(
    contexts: List[Dict[str, object]]
) -> Dict[str, int]:
    mapping: Dict[str, int] = {}
    for ctx in contexts:
        grupo_id = _safe_int(ctx.get("grupo_id"))
        seccion = _normalize_seccion_key(ctx.get("seccion_norm") or ctx.get("seccion") or "")
        if grupo_id is None or not seccion:
            continue
        mapping[seccion] = int(grupo_id)
    return mapping


def _pick_default_destino(
    grupo_id_by_seccion: Dict[str, int],
    origen_seccion: str,
) -> Tuple[str, Optional[int]]:
    origen = _normalize_seccion_key(origen_seccion)
    if "A" in grupo_id_by_seccion and "A" != origen:
        return "A", int(grupo_id_by_seccion["A"])
    secciones_ordenadas = sorted(grupo_id_by_seccion.keys())
    for seccion in secciones_ordenadas:
        if seccion and seccion != origen:
            return seccion, int(grupo_id_by_seccion[seccion])
    if origen and origen in grupo_id_by_seccion:
        return origen, int(grupo_id_by_seccion[origen])
    if secciones_ordenadas:
        seccion = secciones_ordenadas[0]
        return seccion, int(grupo_id_by_seccion[seccion])
    return "", None


def _build_clases_destino_for_plan(
    clases_rows: List[Dict[str, object]],
    nivel_id: int,
    grado_id: int,
    grupo_destino_id: int,
    seccion_destino: str,
) -> List[Dict[str, object]]:
    seccion_norm = _normalize_seccion_key(seccion_destino)
    clases: List[Dict[str, object]] = []
    seen: Set[int] = set()
    for clase in clases_rows:
        clase_id = _safe_int(clase.get("clase_id"))
        clase_nivel_id = _safe_int(clase.get("nivel_id"))
        clase_grado_id = _safe_int(clase.get("grado_id"))
        if clase_id is None or clase_nivel_id != int(nivel_id) or clase_grado_id != int(grado_id):
            continue
        clase_grupo_id = _safe_int(clase.get("grupo_id"))
        clase_seccion = _normalize_seccion_key(clase.get("seccion"))
        if clase_grupo_id is not None:
            if int(clase_grupo_id) != int(grupo_destino_id):
                continue
        elif seccion_norm and clase_seccion != seccion_norm:
            continue
        if int(clase_id) in seen:
            continue
        seen.add(int(clase_id))
        clases.append(
            {
                "clase_id": int(clase_id),
                "clase": str(clase.get("clase") or "").strip(),
            }
        )
    clases.sort(key=lambda item: (str(item.get("clase") or "").upper(), int(item.get("clase_id") or 0)))
    return clases


def _format_alumno_label(row: Dict[str, object]) -> str:
    nombre = str(row.get("nombre_completo") or "").strip()
    if not nombre:
        nombre = "SIN NOMBRE"
    dni = str(row.get("id_oficial") or "").strip()
    return f"{nombre}|{dni or '-'}"


def _add_auto_move_removed_ref(plan_id: int) -> None:
    plan_id_int = _safe_int(plan_id)
    if plan_id_int is None:
        return
    removed_raw = st.session_state.get("auto_move_removed_ref_ids", [])
    removed_ref_ids: Set[int] = set()
    if isinstance(removed_raw, (list, tuple, set)):
        for item in removed_raw:
            item_int = _safe_int(item)
            if item_int is not None:
                removed_ref_ids.add(int(item_int))
    removed_ref_ids.add(int(plan_id_int))
    st.session_state["auto_move_removed_ref_ids"] = sorted(removed_ref_ids)


def _build_auto_move_simulation(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    on_status: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message))
            except Exception:
                pass

    _status("Paso 1/5: listando niveles, grados y secciones del colegio...")
    niveles = _fetch_niveles_grados_grupos_censo(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
    )
    contexts_all = _build_contexts_for_nivel_grado(niveles=niveles)
    if not contexts_all:
        raise RuntimeError("No hay niveles/grados/secciones configurados para este colegio.")

    contexts_by_grade: Dict[Tuple[int, int], List[Dict[str, object]]] = {}
    for ctx in contexts_all:
        nivel_id_ctx = _safe_int(ctx.get("nivel_id"))
        grado_id_ctx = _safe_int(ctx.get("grado_id"))
        if nivel_id_ctx is None or grado_id_ctx is None:
            continue
        grade_key = (int(nivel_id_ctx), int(grado_id_ctx))
        contexts_by_grade.setdefault(grade_key, []).append(ctx)

    grade_keys_with_y = sorted(
        [
            key
            for key, ctxs in contexts_by_grade.items()
            if any(
                _normalize_seccion_key(ctx.get("seccion_norm") or ctx.get("seccion"))
                == AUTO_MOVE_SECCION_ORIGEN
                for ctx in ctxs
            )
        ],
        key=lambda item: (int(item[0]), int(item[1])),
    )
    if not grade_keys_with_y:
        raise RuntimeError(
            "No hay seccion Y disponible en los grados configurados para este colegio."
        )
    _status(
        f"Paso 1/5 completo: seccion Y detectada en {len(grade_keys_with_y)} grado(s)."
    )
    contexts: List[Dict[str, object]] = []
    for grade_key in grade_keys_with_y:
        contexts.extend(contexts_by_grade.get(grade_key, []))

    _status("Paso 2/5: listando clases del colegio...")
    try:
        clases_rows, _ = listar_y_mapear_clases(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
            ordered=True,
            on_log=None,
        )
    except Exception:
        clases_rows = []
        _status("Paso 2/5: no se pudieron listar clases; continuo con alumnos...")
    else:
        _status(f"Paso 2/5 completo: clases listadas ({len(clases_rows)}).")

    errores_fetch: List[str] = []
    alumnos_all_raw: List[Dict[str, object]] = []
    total_contexts = len(contexts)
    _status(f"Paso 3/5: listando alumnos por seccion ({total_contexts} consultas)...")
    for idx_ctx, ctx in enumerate(contexts, start=1):
        _status(
            "Paso 3/5: listando alumnos {idx}/{total} | nivelId={nivel} gradoId={grado} grupoId={grupo}".format(
                idx=idx_ctx,
                total=total_contexts,
                nivel=ctx.get("nivel_id"),
                grado=ctx.get("grado_id"),
                grupo=ctx.get("grupo_id"),
            )
        )
        try:
            alumnos_ctx = _fetch_alumnos_censo(
                token=token,
                colegio_id=int(colegio_id),
                nivel_id=int(ctx["nivel_id"]),
                grado_id=int(ctx["grado_id"]),
                grupo_id=int(ctx["grupo_id"]),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
            )
        except Exception as exc:
            errores_fetch.append(
                "nivelId={nivel} gradoId={grado} grupoId={grupo}: {err}".format(
                    nivel=ctx.get("nivel_id"),
                    grado=ctx.get("grado_id"),
                    grupo=ctx.get("grupo_id"),
                    err=exc,
                )
            )
            continue
        for item in alumnos_ctx:
            if not isinstance(item, dict):
                continue
            alumnos_all_raw.append(_flatten_censo_alumno_for_auto_plan(item=item, fallback=ctx))

    alumnos_by_id: Dict[str, Dict[str, object]] = {}
    for row in alumnos_all_raw:
        alumno_id = _safe_int(row.get("alumno_id"))
        persona_id = _safe_int(row.get("persona_id"))
        grupo_id = _safe_int(row.get("grupo_id"))
        if alumno_id is not None:
            key = f"alumno:{int(alumno_id)}"
        elif persona_id is not None and grupo_id is not None:
            key = f"persona_grupo:{int(persona_id)}:{int(grupo_id)}"
        elif persona_id is not None:
            key = f"persona:{int(persona_id)}"
        else:
            continue
        if key in alumnos_by_id:
            continue
        alumnos_by_id[key] = row

    alumnos_all = sorted(
        alumnos_by_id.values(),
        key=lambda row: (
            _grupo_sort_key(
                str(row.get("seccion_norm") or ""),
                str(row.get("seccion") or ""),
            ),
            str(row.get("apellido_paterno") or "").upper(),
            str(row.get("apellido_materno") or "").upper(),
            str(row.get("nombre") or "").upper(),
        ),
    )
    _status(f"Paso 3/5 completo: alumnos consolidados ({len(alumnos_all)}).")
    grupo_id_by_seccion_by_grade: Dict[Tuple[int, int], Dict[str, int]] = {}
    default_destino_by_grade: Dict[Tuple[int, int], Tuple[str, Optional[int]]] = {}
    for grade_key in grade_keys_with_y:
        grade_contexts = contexts_by_grade.get(grade_key) or []
        mapping = _build_grupo_id_by_seccion_from_contexts(grade_contexts)
        if not mapping:
            continue
        grupo_id_by_seccion_by_grade[grade_key] = mapping
        default_destino_by_grade[grade_key] = _pick_default_destino(
            grupo_id_by_seccion=mapping,
            origen_seccion=AUTO_MOVE_SECCION_ORIGEN,
        )

    no_pagados = [row for row in alumnos_all if not _to_bool(row.get("con_pago"))]
    no_pagados_index: Dict[Tuple[int, int, str, str], List[Dict[str, object]]] = {}
    for row in no_pagados:
        nivel_id_row = _safe_int(row.get("nivel_id"))
        grado_id_row = _safe_int(row.get("grado_id"))
        apellido_paterno = _normalize_compare_text(row.get("apellido_paterno"))
        apellido_materno = _normalize_compare_text(row.get("apellido_materno"))
        if (
            nivel_id_row is None
            or grado_id_row is None
            or not apellido_paterno
            or not apellido_materno
        ):
            continue
        key = (int(nivel_id_row), int(grado_id_row), apellido_paterno, apellido_materno)
        no_pagados_index.setdefault(key, []).append(row)

    pagados_y = [
        row
        for row in alumnos_all
        if _to_bool(row.get("con_pago"))
        and _normalize_seccion_key(row.get("seccion_norm") or row.get("seccion")) == AUTO_MOVE_SECCION_ORIGEN
        and (
            _safe_int(row.get("nivel_id")),
            _safe_int(row.get("grado_id")),
        )
        in grade_keys_with_y
    ]
    pagados_y.sort(
        key=lambda row: (
            int(_safe_int(row.get("nivel_id")) or 0),
            int(_safe_int(row.get("grado_id")) or 0),
            str(row.get("nombre_completo") or "").upper(),
        )
    )
    _status(
        "Paso 4/5: comparando alumnos pagados de seccion Y con no pagados "
        "(apellidos + DNI)..."
    )
    _status(f"Paso 4/5: alumnos pagados en seccion Y detectados ({len(pagados_y)}).")

    plan_rows: List[Dict[str, object]] = []
    for idx, pagado in enumerate(pagados_y, start=1):
        nivel_id = _safe_int(pagado.get("nivel_id"))
        grado_id = _safe_int(pagado.get("grado_id"))
        if nivel_id is None or grado_id is None:
            continue
        grade_key = (int(nivel_id), int(grado_id))

        apellido_paterno = _normalize_compare_text(pagado.get("apellido_paterno"))
        apellido_materno = _normalize_compare_text(pagado.get("apellido_materno"))
        dni_pagado = _normalize_compare_id(pagado.get("id_oficial"))
        match_no_pagado: Dict[str, object] = {}
        if apellido_paterno and apellido_materno:
            compare_key = (int(nivel_id), int(grado_id), apellido_paterno, apellido_materno)
            for candidato in no_pagados_index.get(compare_key, []):
                if _safe_int(candidato.get("alumno_id")) == _safe_int(pagado.get("alumno_id")):
                    continue
                dni_candidato = _normalize_compare_id(candidato.get("id_oficial"))
                if dni_pagado and dni_candidato and dni_pagado == dni_candidato:
                    match_no_pagado = candidato
                    break

        grupo_id_by_seccion = grupo_id_by_seccion_by_grade.get(grade_key, {})
        default_seccion, default_grupo_id = default_destino_by_grade.get(grade_key, ("", None))
        seccion_destino = ""
        grupo_destino_id = None
        motivo = ""
        if match_no_pagado:
            seccion_destino = _normalize_seccion_key(
                match_no_pagado.get("seccion_norm") or match_no_pagado.get("seccion")
            )
            if seccion_destino:
                grupo_destino_id = _safe_int(grupo_id_by_seccion.get(seccion_destino))
            if grupo_destino_id is None:
                seccion_destino = default_seccion
                grupo_destino_id = _safe_int(default_grupo_id)
            motivo = "Coincide por apellido paterno+materno y DNI con alumno no pagado."
        else:
            seccion_destino = default_seccion
            grupo_destino_id = _safe_int(default_grupo_id)
            motivo = "Sin parecido no pagado (apellidos + DNI): solo movimiento de seccion."

        if grupo_destino_id is None:
            grupo_destino_id = _safe_int(pagado.get("grupo_id"))
            if not seccion_destino:
                seccion_destino = _normalize_seccion_key(pagado.get("seccion_norm") or pagado.get("seccion"))
        clases_destino = []
        if grupo_destino_id is not None and nivel_id is not None and grado_id is not None:
            clases_destino = _build_clases_destino_for_plan(
                clases_rows=clases_rows,
                nivel_id=int(nivel_id),
                grado_id=int(grado_id),
                grupo_destino_id=int(grupo_destino_id),
                seccion_destino=seccion_destino,
            )

        comparacion = ""
        if match_no_pagado:
            comparacion = (
                f"Este alumno se parece a: {_format_alumno_label(match_no_pagado)} "
                "(apellidos + DNI)."
            )
        else:
            comparacion = "No se encontro alumno no pagado parecido (apellidos + DNI)."

        grupo_origen_id = _safe_int(pagado.get("grupo_id"))
        plan_rows.append(
            {
                "plan_id": int(idx),
                "colegio_id": int(colegio_id),
                "alumno_pagado": pagado,
                "alumno_parecido": match_no_pagado,
                "alumno_inactivar": match_no_pagado,
                "nivel_id": int(nivel_id),
                "grado_id": int(grado_id),
                "grupo_origen_id": int(grupo_origen_id) if grupo_origen_id is not None else None,
                "grupo_destino_id": int(grupo_destino_id) if grupo_destino_id is not None else None,
                "seccion_origen": _normalize_seccion_key(pagado.get("seccion_norm") or pagado.get("seccion")),
                "seccion_destino": seccion_destino,
                "motivo": motivo,
                "comparacion": comparacion,
                "clases_destino": clases_destino,
                "requiere_inactivar": bool(match_no_pagado),
                "requiere_mover": (
                    grupo_origen_id is not None
                    and grupo_destino_id is not None
                    and int(grupo_origen_id) != int(grupo_destino_id)
                ),
            }
        )

    alumnos_grid: List[Dict[str, object]] = []
    for row in alumnos_all:
        alumnos_grid.append(
            {
                "ColegioId": int(colegio_id),
                "NivelId": row.get("nivel_id"),
                "GradoId": row.get("grado_id"),
                "AlumnoId": row.get("alumno_id"),
                "PersonaId": row.get("persona_id"),
                "Apellido Paterno": row.get("apellido_paterno"),
                "Apellido Materno": row.get("apellido_materno"),
                "Nombre": row.get("nombre"),
                "DNI": row.get("id_oficial"),
                "Seccion": row.get("seccion_norm") or row.get("seccion"),
                "GrupoId": row.get("grupo_id"),
                "Activo": "SI" if _to_bool(row.get("activo")) else "NO",
                "ConPago": "SI" if _to_bool(row.get("con_pago")) else "NO",
                "Fecha Desde": row.get("fecha_desde"),
            }
        )

    editor_rows: List[Dict[str, object]] = []
    for plan in plan_rows:
        pagado = plan.get("alumno_pagado") if isinstance(plan.get("alumno_pagado"), dict) else {}
        inactivar = plan.get("alumno_inactivar") if isinstance(plan.get("alumno_inactivar"), dict) else {}
        alumno_y = _format_alumno_label(pagado)
        if inactivar:
            alumno_cambiar = _format_alumno_label(inactivar)
        else:
            alumno_cambiar = "-|SIN COINCIDENCIA|-"
        editor_rows.append(
            {
                "PlanId": int(plan.get("plan_id") or 0),
                "Alumno Y": alumno_y,
                "Alumno a cambiar": alumno_cambiar,
            }
        )
    _status(
        f"Paso 5/5: simulacion lista. Cambios sugeridos para revisar: {len(plan_rows)}."
    )

    return {
        "niveles": niveles,
        "contexts": contexts,
        "errors": errores_fetch,
        "alumnos_all_grid": alumnos_grid,
        "plan_rows": plan_rows,
        "editor_rows": editor_rows,
        "grupo_id_by_seccion_by_grade": grupo_id_by_seccion_by_grade,
    }


def _build_auto_move_simulation_multi(
    token: str,
    colegio_ids: List[int],
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    on_status: Optional[Callable[[str], None]] = None,
    on_progress: Optional[Callable[[Dict[str, object]], None]] = None,
) -> Dict[str, object]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message))
            except Exception:
                pass

    def _progress(current_colegio_id: int, current_status: str) -> None:
        if callable(on_progress):
            try:
                on_progress(
                    {
                        "processed": len(per_colegio_rows),
                        "total": total_colegios,
                        "current_colegio_id": int(current_colegio_id),
                        "current_status": str(current_status or "").strip(),
                        "summary_rows": [dict(row) for row in per_colegio_rows],
                        "plan_rows_total": len(plan_rows),
                        "errors_total": len(errors),
                    }
                )
            except Exception:
                pass

    plan_rows: List[Dict[str, object]] = []
    alumnos_grid: List[Dict[str, object]] = []
    errors: List[str] = []
    group_map_by_scope: Dict[Tuple[int, int, int], Dict[str, int]] = {}
    per_colegio_rows: List[Dict[str, object]] = []
    next_plan_id = 1
    total_colegios = len(colegio_ids)

    for idx, colegio_id in enumerate(colegio_ids, start=1):
        _status(f"[{idx}/{total_colegios}] Colegio {colegio_id}: preparando simulacion...")
        try:
            simulation = _build_auto_move_simulation(
                token=token,
                colegio_id=int(colegio_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
                on_status=(
                    lambda message, colegio=colegio_id: _status(
                        f"Colegio {colegio}: {str(message or '').strip()}"
                    )
                ),
            )
        except Exception as exc:
            errors.append(f"Colegio {colegio_id}: {exc}")
            per_colegio_rows.append(
                {
                    "ColegioId": int(colegio_id),
                    "Cambios sugeridos": 0,
                    "Errores de consulta": 1,
                    "Estado": f"Error: {exc}",
                }
            )
            _progress(int(colegio_id), f"Error: {exc}")
            continue

        plan_rows_colegio = simulation.get("plan_rows") or []
        for raw_plan in plan_rows_colegio:
            if not isinstance(raw_plan, dict):
                continue
            plan = dict(raw_plan)
            plan["colegio_id"] = int(colegio_id)
            plan["plan_id"] = int(next_plan_id)
            plan_rows.append(plan)
            next_plan_id += 1

        alumnos_rows_colegio = simulation.get("alumnos_all_grid") or []
        for raw_row in alumnos_rows_colegio:
            if not isinstance(raw_row, dict):
                continue
            row = dict(raw_row)
            row["ColegioId"] = int(colegio_id)
            alumnos_grid.append(row)

        mapping_by_grade = simulation.get("grupo_id_by_seccion_by_grade") or {}
        if isinstance(mapping_by_grade, dict):
            for grade_key, mapping in mapping_by_grade.items():
                if not isinstance(grade_key, tuple) or len(grade_key) != 2:
                    continue
                nivel_id, grado_id = grade_key
                if not isinstance(mapping, dict):
                    continue
                group_map_by_scope[(int(colegio_id), int(nivel_id), int(grado_id))] = {
                    str(seccion): int(grupo_id)
                    for seccion, grupo_id in mapping.items()
                    if _safe_int(grupo_id) is not None
                }

        simulation_errors = simulation.get("errors") or []
        for err in simulation_errors:
            err_txt = str(err or "").strip()
            if err_txt:
                errors.append(f"Colegio {colegio_id}: {err_txt}")

        per_colegio_rows.append(
            {
                "ColegioId": int(colegio_id),
                "Cambios sugeridos": len(plan_rows_colegio),
                "Errores de consulta": len(simulation_errors),
                "Estado": "OK",
            }
        )
        _progress(
            int(colegio_id),
            "OK ({changes} cambios sugeridos)".format(changes=len(plan_rows_colegio)),
        )

    return {
        "plan_rows": plan_rows,
        "alumnos_all_grid": alumnos_grid,
        "errors": errors,
        "group_map_by_scope": group_map_by_scope,
        "colegio_summary_rows": per_colegio_rows,
        "colegios_total": total_colegios,
        "colegios_ok": sum(
            1 for row in per_colegio_rows if str(row.get("Estado") or "").strip() == "OK"
        ),
    }


def _build_auto_move_multi_editor_state(
    plan_rows: List[Dict[str, object]],
    group_map_by_scope: Dict[Tuple[int, int, int], Dict[str, int]],
) -> Tuple[List[Dict[str, object]], Dict[str, Dict[str, object]], List[str]]:
    destino_payload_by_option: Dict[str, Dict[str, object]] = {}
    destino_options: List[str] = []
    table_rows: List[Dict[str, object]] = []

    sorted_plans = sorted(
        [
            plan
            for plan in plan_rows
            if isinstance(plan, dict) and _safe_int(plan.get("plan_id")) is not None
        ],
        key=lambda plan: (
            int(_safe_int(plan.get("colegio_id")) or 0),
            int(_safe_int(plan.get("plan_id")) or 0),
        ),
    )

    for plan in sorted_plans:
        plan_id = int(_safe_int(plan.get("plan_id")) or 0)
        colegio_id = _safe_int(plan.get("colegio_id"))
        colegio_nombre = str(
            AUTO_MOVE_MULTI_DEFAULT_COLEGIO_NAME_BY_ID.get(int(colegio_id or 0), "")
        ).strip() or (
            f"Colegio {int(colegio_id)}" if colegio_id is not None else ""
        )
        pagado = plan.get("alumno_pagado") if isinstance(plan.get("alumno_pagado"), dict) else {}
        referencial = (
            plan.get("alumno_inactivar")
            if isinstance(plan.get("alumno_inactivar"), dict)
            else {}
        )
        nivel_id = _safe_int(plan.get("nivel_id"))
        grado_id = _safe_int(plan.get("grado_id"))
        mapping: Dict[str, int] = {}
        if colegio_id is not None and nivel_id is not None and grado_id is not None:
            mapping_raw = group_map_by_scope.get((int(colegio_id), int(nivel_id), int(grado_id)))
            if isinstance(mapping_raw, dict):
                mapping = mapping_raw

        nivel_txt = str(pagado.get("nivel") or plan.get("nivel") or "").strip()
        grado_txt = str(pagado.get("grado") or plan.get("grado") or "").strip()
        seccion_origen_txt = _normalize_seccion_key(
            plan.get("seccion_origen")
            or pagado.get("seccion_norm")
            or pagado.get("seccion")
            or AUTO_MOVE_SECCION_ORIGEN
        )
        seccion_destino_txt = _normalize_seccion_key(plan.get("seccion_destino") or "")

        if mapping and not seccion_destino_txt:
            picked_sec, picked_gid = _pick_default_destino(
                grupo_id_by_seccion=mapping,
                origen_seccion=AUTO_MOVE_SECCION_ORIGEN,
            )
            if picked_sec and picked_gid is not None:
                seccion_destino_txt = _normalize_seccion_key(picked_sec)
                plan["seccion_destino"] = seccion_destino_txt
                plan["grupo_destino_id"] = int(picked_gid)

        for seccion_key, grupo_destino_id in sorted(mapping.items(), key=lambda item: str(item[0])):
            sec = _normalize_seccion_key(seccion_key)
            option_text = f"{int(colegio_id or 0)} | {nivel_txt} | {grado_txt} ({sec})"
            if option_text not in destino_payload_by_option:
                destino_payload_by_option[option_text] = {
                    "colegio_id": int(colegio_id) if colegio_id is not None else None,
                    "nivel_id": int(nivel_id) if nivel_id is not None else None,
                    "grado_id": int(grado_id) if grado_id is not None else None,
                    "grupo_destino_id": int(grupo_destino_id),
                    "seccion_destino": sec,
                }
                destino_options.append(option_text)

        default_option = ""
        if colegio_id is not None and (nivel_txt or grado_txt or seccion_destino_txt):
            default_option = (
                f"{int(colegio_id)} | {nivel_txt} | {grado_txt} ({seccion_destino_txt})"
            )
        if default_option and default_option not in destino_payload_by_option:
            destino_payload_by_option[default_option] = {
                "colegio_id": int(colegio_id) if colegio_id is not None else None,
                "nivel_id": int(nivel_id) if nivel_id is not None else None,
                "grado_id": int(grado_id) if grado_id is not None else None,
                "grupo_destino_id": _safe_int(plan.get("grupo_destino_id")),
                "seccion_destino": seccion_destino_txt,
            }
            destino_options.append(default_option)

        alumno_col = (
            f"{_format_alumno_label(pagado)} | "
            f"{nivel_txt} | {grado_txt} ({seccion_origen_txt})"
        )
        referencia_col = (
            _format_alumno_label(referencial)
            if isinstance(referencial, dict) and referencial
            else "SIN REFERENCIA"
        )
        requiere_inactivar = bool(
            _to_bool(plan.get("requiere_inactivar"))
            and _safe_int(referencial.get("alumno_id")) is not None
        )
        table_rows.append(
            {
                "_plan_id": int(plan_id),
                "Colegio": colegio_nombre,
                "Alumno | Grado y seccion": alumno_col,
                "Referencia": referencia_col,
                "Inactivar referencia": requiere_inactivar,
                "Nuevo grado y seccion": default_option,
            }
        )

    return table_rows, destino_payload_by_option, sorted(destino_options)


def _build_auto_move_multi_summary_preview(
    plan_rows: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    preview_rows: List[Dict[str, object]] = []
    for plan in sorted(
        [row for row in plan_rows if isinstance(row, dict)],
        key=lambda row: (
            str(
                AUTO_MOVE_MULTI_DEFAULT_COLEGIO_NAME_BY_ID.get(
                    int(_safe_int(row.get("colegio_id")) or 0),
                    "",
                )
            ).upper(),
            str(
                (
                    row.get("alumno_pagado", {}).get("nombre_completo")
                    if isinstance(row.get("alumno_pagado"), dict)
                    else ""
                )
            ).upper(),
        ),
    ):
        colegio_id = _safe_int(plan.get("colegio_id"))
        pagado = plan.get("alumno_pagado") if isinstance(plan.get("alumno_pagado"), dict) else {}
        nivel_txt = str(pagado.get("nivel") or plan.get("nivel") or "").strip()
        grado_txt = str(pagado.get("grado") or plan.get("grado") or "").strip()
        seccion_origen_txt = _normalize_seccion_key(
            plan.get("seccion_origen")
            or pagado.get("seccion_norm")
            or pagado.get("seccion")
            or AUTO_MOVE_SECCION_ORIGEN
        )
        preview_rows.append(
            {
                "Colegio": str(
                    AUTO_MOVE_MULTI_DEFAULT_COLEGIO_NAME_BY_ID.get(
                        int(colegio_id or 0),
                        f"Colegio {int(colegio_id)}" if colegio_id is not None else "",
                    )
                ).strip(),
                "Alumno | Grado y seccion": (
                    f"{_format_alumno_label(pagado)} | "
                    f"{nivel_txt} | {grado_txt} ({seccion_origen_txt})"
                ),
            }
        )
    return preview_rows


def _materialize_auto_move_multi_plans(
    base_plan_rows: List[Dict[str, object]],
    edited_rows: List[Dict[str, object]],
    destino_payload_by_option: Dict[str, Dict[str, object]],
) -> Tuple[List[Dict[str, object]], Set[int], List[str]]:
    authorized_plans: List[Dict[str, object]] = []
    removed_ref_ids_current: Set[int] = set()
    validation_errors: List[str] = []
    edited_rows_by_plan_id: Dict[int, Dict[str, object]] = {}
    for row in edited_rows:
        plan_id = _safe_int(row.get("_plan_id")) if isinstance(row, dict) else None
        if plan_id is None:
            continue
        edited_rows_by_plan_id[int(plan_id)] = row

    for base_plan in base_plan_rows:
        plan_id = _safe_int(base_plan.get("plan_id"))
        if plan_id is None or not isinstance(base_plan, dict):
            continue
        plan = dict(base_plan)
        edited_row = edited_rows_by_plan_id.get(int(plan_id), {})

        keep_reference = bool(_to_bool(edited_row.get("Inactivar referencia")))
        if not keep_reference:
            removed_ref_ids_current.add(int(plan_id))
            plan["alumno_parecido"] = {}
            plan["alumno_inactivar"] = {}
            plan["requiere_inactivar"] = False
            plan["comparacion"] = (
                "Referencia eliminada manualmente: solo movimiento de seccion."
            )
            plan["motivo"] = (
                "Referencia eliminada manualmente: no se inactiva alumno parecido."
            )

        selected_destino = str(edited_row.get("Nuevo grado y seccion") or "").strip()
        payload = destino_payload_by_option.get(selected_destino)
        if isinstance(payload, dict):
            plan_colegio_id = _safe_int(plan.get("colegio_id"))
            payload_colegio_id = _safe_int(payload.get("colegio_id"))
            if (
                plan_colegio_id is not None
                and payload_colegio_id is not None
                and int(plan_colegio_id) != int(payload_colegio_id)
            ):
                validation_errors.append(
                    "Plan {plan_id}: el destino '{destino}' no pertenece al colegio {colegio_id}.".format(
                        plan_id=int(plan_id),
                        destino=selected_destino,
                        colegio_id=int(plan_colegio_id),
                    )
                )
                continue
            nivel_id_val = _safe_int(payload.get("nivel_id"))
            grado_id_val = _safe_int(payload.get("grado_id"))
            grupo_id_val = _safe_int(payload.get("grupo_destino_id"))
            seccion_val = str(payload.get("seccion_destino") or "").strip()
            if payload_colegio_id is not None:
                plan["colegio_id"] = int(payload_colegio_id)
            if nivel_id_val is not None:
                plan["nivel_id"] = int(nivel_id_val)
            if grado_id_val is not None:
                plan["grado_id"] = int(grado_id_val)
            if grupo_id_val is not None:
                plan["grupo_destino_id"] = int(grupo_id_val)
            if seccion_val:
                plan["seccion_destino"] = seccion_val

        authorized_plans.append(plan)

    return authorized_plans, removed_ref_ids_current, validation_errors


def _set_alumno_activo_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    alumno_id: int,
    activo: int,
    observaciones: str,
    timeout: int,
) -> Tuple[bool, str]:
    url = CENSO_ALUMNO_ACTIVAR_INACTIVAR_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_id=int(grupo_id),
        alumno_id=int(alumno_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "activo": int(activo),
        "razonInactivoId": 0,
        "observaciones": str(observaciones or ""),
    }
    method_calls = [
        ("PUT", requests.put),
        ("POST", requests.post),
        ("PATCH", requests.patch),
    ]
    last_error = "HTTP 405"
    for method_name, method_call in method_calls:
        try:
            response = method_call(url, headers=headers, json=payload, timeout=int(timeout))
        except requests.RequestException as exc:
            return False, f"Error de red: {exc}"

        status_code = response.status_code
        try:
            body = response.json()
        except ValueError:
            body = {}

        if response.ok:
            if isinstance(body, dict) and body.get("success", True) is False:
                message = str(body.get("message") or "Respuesta invalida").strip()
                return False, message
            return True, method_name

        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        if status_code == 405:
            last_error = message or f"{method_name} HTTP 405"
            continue
        return False, message or f"{method_name} HTTP {status_code}"
    return False, last_error


def _mover_alumno_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    alumno_id: int,
    nuevo_nivel_id: int,
    nuevo_grado_id: int,
    nuevo_grupo_id: int,
    timeout: int,
) -> Tuple[bool, str]:
    url = CENSO_ALUMNO_MOVER_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_id=int(grupo_id),
        alumno_id=int(alumno_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "nuevoNivelId": int(nuevo_nivel_id),
        "nuevoGradoId": int(nuevo_grado_id),
        "nuevoGrupoId": int(nuevo_grupo_id),
        "observaciones": " ",
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, message or f"HTTP {status_code}"

    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return False, message
    return True, ""


def _alumno_birthdate_to_api(value: object) -> str:
    if isinstance(value, datetime):
        raw_date = value.date()
    elif isinstance(value, date):
        raw_date = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Fecha de nacimiento obligatoria.")
        try:
            raw_date = date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError("Fecha de nacimiento invalida.") from exc
    return f"{raw_date.isoformat()}T00:00:00.000Z"


def _validacion_dashboard_ok(payload: object) -> Tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "Respuesta invalida"
    if not payload.get("success", False):
        return False, str(payload.get("message") or "Respuesta invalida").strip()
    data = payload.get("data") or {}
    if isinstance(data, dict):
        status = _safe_int(data.get("status"))
        mensajes = data.get("mensajes")
        if isinstance(mensajes, list):
            mensaje = " | ".join(str(item).strip() for item in mensajes if str(item).strip())
        else:
            mensaje = str(payload.get("message") or "").strip()
        if status is not None and int(status) != 0:
            return False, mensaje or "Validacion rechazada."
        return True, mensaje or str(payload.get("message") or "success").strip()
    return True, str(payload.get("message") or "success").strip()


def _validar_login_reglas(login: str) -> Optional[str]:
    login_txt = str(login or "").strip()
    if len(login_txt) < 6:
        return "El login debe tener minimo 6 caracteres."
    if not re.fullmatch(r"[A-Za-z0-9@._-]+", login_txt):
        return (
            "El login solo puede tener letras, numeros y estos caracteres: "
            "@ . - _"
        )
    return None


def _validar_password_reglas(password: str) -> Optional[str]:
    password_txt = str(password or "")
    if len(password_txt) < 6:
        return "La password debe tener minimo 6 caracteres."
    if not re.fullmatch(r"[A-Za-z0-9]+", password_txt):
        return "La password solo puede tener letras y numeros."
    return None


def _validar_identificador_alumno_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    identificador: str,
    timeout: int,
    persona_id: int = 0,
) -> Tuple[bool, str]:
    url = DASHBOARD_VALIDAR_IDENTIFICADOR_URL.format(empresa_id=int(empresa_id))
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "colegioId": int(colegio_id),
        "cicloEscolarId": int(ciclo_id),
        "nivelId": str(int(nivel_id)),
        "personaId": int(persona_id),
        "identificador": str(identificador or "").strip(),
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, message or f"HTTP {status_code}"
    return _validacion_dashboard_ok(body)


def _crear_alumno_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    nombre: str,
    apellido_paterno: str,
    apellido_materno: str,
    sexo: str,
    fecha_nacimiento: object,
    id_oficial: str,
    extranjero: bool,
    timeout: int,
) -> Tuple[bool, Dict[str, object], str]:
    url = CENSO_ALUMNOS_CREATE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_id=int(grupo_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "nombre": str(nombre or "").strip(),
        "apellidoPaterno": str(apellido_paterno or "").strip(),
        "apellidoMaterno": str(apellido_materno or "").strip(),
        "sexo": str(sexo or "").strip(),
        "fechaNacimiento": _alumno_birthdate_to_api(fecha_nacimiento),
        "idOficial": str(id_oficial or "").strip(),
        "extranjero": bool(extranjero),
    }
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=int(timeout),
        )
    except requests.RequestException as exc:
        return False, {}, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, {}, message or f"HTTP {status_code}"
    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return False, {}, message
    data = body.get("data") if isinstance(body, dict) else {}
    if not isinstance(data, dict):
        return False, {}, "Respuesta invalida"
    return True, data, ""


def _validar_login_alumno_web(
    token: str,
    empresa_id: int,
    login: str,
    timeout: int,
    persona_id: int = 0,
    grado_id: int = 0,
) -> Tuple[bool, str]:
    url = DASHBOARD_VALIDAR_LOGIN_URL.format(empresa_id=int(empresa_id))
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "login": str(login or "").strip(),
        "personaId": int(persona_id),
        "gradoId": int(grado_id),
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, message or f"HTTP {status_code}"
    return _validacion_dashboard_ok(body)


def _update_login_alumno_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    alumno_id: int,
    login: str,
    password: str,
    timeout: int,
) -> Tuple[bool, Dict[str, object], str]:
    url = CENSO_ALUMNO_UPDATE_LOGIN_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_id=int(grupo_id),
        alumno_id=int(alumno_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "login": str(login or "").strip(),
        "password": str(password or ""),
    }
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, {}, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, {}, message or f"HTTP {status_code}"
    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return False, {}, message
    data = body.get("data") if isinstance(body, dict) else {}
    if not isinstance(data, dict):
        return False, {}, "Respuesta invalida"
    return True, data, ""


def _alumno_edit_option_label(row: Dict[str, object]) -> str:
    nombre = str(
        row.get("nombre_completo")
        or " ".join(
            part
            for part in (
                str(row.get("nombre") or "").strip(),
                str(row.get("apellido_paterno") or "").strip(),
                str(row.get("apellido_materno") or "").strip(),
            )
            if part
        )
    ).strip() or "SIN NOMBRE"
    dni = str(row.get("id_oficial") or "").strip() or "-"
    login = str(row.get("login") or "").strip()
    nivel = str(row.get("nivel") or "").strip()
    grado = str(row.get("grado") or "").strip()
    seccion = str(row.get("seccion") or "").strip()
    label = f"{nombre} | DNI {dni}"
    if login:
        label = f"{label} | {login}"
    if nivel or grado or seccion:
        label = f"{label} | {nivel} | {grado} | {seccion or '-'}"
    return label


def _alumno_edit_api_date_to_widget(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return date.today()
    text = text.split("T", 1)[0].strip()
    try:
        return date.fromisoformat(text)
    except ValueError:
        return date.today()


def _clear_alumnos_edit_state() -> None:
    for state_key in (
        "alumnos_edit_rows",
        "alumnos_edit_errors",
        "alumnos_edit_niveles",
        "alumnos_edit_colegio_id",
        "alumnos_edit_selected_alumno_id",
        "alumnos_edit_loaded_alumno_id",
        "alumnos_edit_context",
        "alumnos_edit_detail",
        "alumnos_edit_fetch_error",
        "alumnos_edit_nombre",
        "alumnos_edit_apellido_paterno",
        "alumnos_edit_apellido_materno",
        "alumnos_edit_sexo",
        "alumnos_edit_dni",
        "alumnos_edit_fecha",
        "alumnos_edit_extranjero",
        "alumnos_edit_login",
        "alumnos_edit_original_login",
        "alumnos_edit_password",
        "alumnos_edit_notice",
        "alumnos_edit_pending_detail_refresh",
        "alumnos_edit_move_dialog_alumno_id",
        "alumnos_edit_move_last_alumno_id",
        "alumnos_edit_move_nivel_id",
        "alumnos_edit_move_grado_id",
        "alumnos_edit_move_seccion",
    ):
        st.session_state.pop(state_key, None)


def _alumno_edit_context_from_row(row: Dict[str, object]) -> Optional[Dict[str, int]]:
    alumno_id = _safe_int(row.get("alumno_id"))
    persona_id = _safe_int(row.get("persona_id"))
    nivel_id = _safe_int(row.get("nivel_id"))
    grado_id = _safe_int(row.get("grado_id"))
    grupo_id = _safe_int(row.get("grupo_id"))
    if None in {alumno_id, persona_id, nivel_id, grado_id, grupo_id}:
        return None
    return {
        "alumno_id": int(alumno_id),
        "persona_id": int(persona_id),
        "nivel_id": int(nivel_id),
        "grado_id": int(grado_id),
        "grupo_id": int(grupo_id),
    }


def _fetch_alumno_edit_detail_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    alumno_id: int,
    timeout: int,
) -> Tuple[Optional[Dict[str, object]], str]:
    url = CENSO_ALUMNO_DETALLE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_id=int(grupo_id),
        alumno_id=int(alumno_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers, timeout=int(timeout))
    except requests.RequestException as exc:
        return None, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return None, message or f"HTTP {status_code}"
    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return None, message
    data = body.get("data") if isinstance(body, dict) else {}
    if not isinstance(data, dict):
        return None, "Respuesta invalida"
    return data, ""


def _update_alumno_edit_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    alumno_id: int,
    nombre: str,
    apellido_paterno: str,
    apellido_materno: str,
    sexo: str,
    fecha_nacimiento: object,
    id_oficial: str,
    extranjero: bool,
    timeout: int,
) -> Tuple[bool, Dict[str, object], str]:
    url = CENSO_ALUMNO_DETALLE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_id=int(grupo_id),
        alumno_id=int(alumno_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "nombre": str(nombre or "").strip(),
        "apellidoPaterno": str(apellido_paterno or "").strip(),
        "apellidoMaterno": str(apellido_materno or "").strip(),
        "sexo": str(sexo or "").strip(),
        "fechaNacimiento": _alumno_birthdate_to_api(fecha_nacimiento),
        "idOficial": str(id_oficial or "").strip(),
        "extranjero": bool(extranjero),
    }
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, {}, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, {}, message or f"HTTP {status_code}"
    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return False, {}, message
    data = body.get("data") if isinstance(body, dict) else {}
    if data is None:
        data = {}
    if not isinstance(data, dict):
        return False, {}, "Respuesta invalida"
    return True, data, ""


def _store_alumno_edit_detail_state(detail: Dict[str, object], context: Dict[str, int]) -> None:
    persona = detail.get("persona") if isinstance(detail.get("persona"), dict) else {}
    persona_login_raw = persona.get("personaLogin") if isinstance(persona, dict) else None
    login_txt = str(detail.get("login") or "").strip()
    if not login_txt and isinstance(persona_login_raw, dict):
        login_txt = str(persona_login_raw.get("login") or "").strip()
    st.session_state["alumnos_edit_context"] = dict(context)
    st.session_state["alumnos_edit_detail"] = detail
    st.session_state["alumnos_edit_loaded_alumno_id"] = int(context["alumno_id"])
    st.session_state["alumnos_edit_nombre"] = str(persona.get("nombre") or "").strip()
    st.session_state["alumnos_edit_apellido_paterno"] = str(
        persona.get("apellidoPaterno") or ""
    ).strip()
    st.session_state["alumnos_edit_apellido_materno"] = str(
        persona.get("apellidoMaterno") or ""
    ).strip()
    st.session_state["alumnos_edit_sexo"] = str(persona.get("sexoMoral") or "").strip()
    st.session_state["alumnos_edit_dni"] = str(persona.get("idOficial") or "").strip()
    st.session_state["alumnos_edit_fecha"] = _alumno_edit_api_date_to_widget(
        persona.get("fechaNacimiento")
    )
    st.session_state["alumnos_edit_extranjero"] = bool(persona.get("extranjero", False))
    st.session_state["alumnos_edit_login"] = login_txt
    st.session_state["alumnos_edit_original_login"] = login_txt
    st.session_state["alumnos_edit_password"] = ""
    st.session_state["alumnos_edit_fetch_error"] = ""


def _clear_alumnos_edit_move_state(close_dialog: bool = True) -> None:
    for state_key in (
        "alumnos_edit_move_last_alumno_id",
        "alumnos_edit_move_nivel_id",
        "alumnos_edit_move_grado_id",
        "alumnos_edit_move_seccion",
    ):
        st.session_state.pop(state_key, None)
    if close_dialog:
        st.session_state.pop("alumnos_edit_move_dialog_alumno_id", None)


def _refresh_alumnos_edit_catalog_and_detail(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    alumno_id: int,
    fallback_context: Dict[str, int],
) -> str:
    refresh_warning = ""
    try:
        alumnos_catalog_edit_refresh = _fetch_alumnos_catalog_for_manual_move(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
    except Exception as exc:  # pragma: no cover - UI
        refresh_warning = f" No se pudo refrescar la lista: {exc}"
        return refresh_warning

    refreshed_students = alumnos_catalog_edit_refresh.get("students") or []
    refreshed_errors = alumnos_catalog_edit_refresh.get("errors") or []
    refreshed_niveles = alumnos_catalog_edit_refresh.get("niveles") or []
    st.session_state["alumnos_edit_rows"] = refreshed_students
    st.session_state["alumnos_edit_errors"] = refreshed_errors
    st.session_state["alumnos_edit_niveles"] = refreshed_niveles
    st.session_state["alumnos_edit_colegio_id"] = int(colegio_id)

    refreshed_row = next(
        (
            row
            for row in refreshed_students
            if int(_safe_int(row.get("alumno_id")) or 0) == int(alumno_id)
        ),
        None,
    )
    refreshed_context = (
        _alumno_edit_context_from_row(refreshed_row or {})
        if isinstance(refreshed_row, dict)
        else None
    ) or dict(fallback_context)

    refreshed_detail, refreshed_detail_msg = _fetch_alumno_edit_detail_web(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        nivel_id=int(refreshed_context["nivel_id"]),
        grado_id=int(refreshed_context["grado_id"]),
        grupo_id=int(refreshed_context["grupo_id"]),
        alumno_id=int(refreshed_context["alumno_id"]),
        timeout=int(timeout),
    )
    if refreshed_detail is None:
        st.session_state["alumnos_edit_pending_detail_refresh"] = {
            "detail": None,
            "context": dict(refreshed_context),
            "fetch_error": str(
                refreshed_detail_msg or "No se pudo refrescar el detalle."
            ),
        }
    else:
        st.session_state["alumnos_edit_pending_detail_refresh"] = {
            "detail": refreshed_detail,
            "context": dict(refreshed_context),
            "fetch_error": "",
        }
    return refresh_warning


def _profesor_edit_level_label(nivel_id: object) -> str:
    nivel_id_int = _safe_int(nivel_id)
    if nivel_id_int is None:
        return "Nivel"
    return str(PEGASUS_NIVEL_LABEL_BY_ID.get(int(nivel_id_int)) or f"Nivel {nivel_id_int}")


def _profesor_edit_level_ids(row: Dict[str, object]) -> List[int]:
    level_ids: Set[int] = set()

    niveles_activos = row.get("niveles_activos")
    if isinstance(niveles_activos, dict):
        for nivel_id, activo in niveles_activos.items():
            nivel_id_int = _safe_int(nivel_id)
            if nivel_id_int is None or not bool(activo):
                continue
            level_ids.add(int(nivel_id_int))

    if not level_ids:
        niveles_presentes = row.get("niveles_presentes")
        if isinstance(niveles_presentes, (list, tuple, set)):
            for nivel_id in niveles_presentes:
                nivel_id_int = _safe_int(nivel_id)
                if nivel_id_int is not None:
                    level_ids.add(int(nivel_id_int))

    if not level_ids:
        niveles_detalle_activos = row.get("niveles_detalle_activos")
        if isinstance(niveles_detalle_activos, (list, tuple, set)):
            for nivel_id in niveles_detalle_activos:
                nivel_id_int = _safe_int(nivel_id)
                if nivel_id_int is not None:
                    level_ids.add(int(nivel_id_int))

    if not level_ids:
        niveles_detalle = row.get("niveles_detalle")
        if isinstance(niveles_detalle, (list, tuple, set)):
            for nivel_id in niveles_detalle:
                nivel_id_int = _safe_int(nivel_id)
                if nivel_id_int is not None:
                    level_ids.add(int(nivel_id_int))

    return sorted(level_ids)


def _profesor_edit_option_label(row: Dict[str, object]) -> str:
    parts = [
        str(row.get("nombre") or "").strip(),
        str(row.get("apellido_paterno") or "").strip(),
        str(row.get("apellido_materno") or "").strip(),
    ]
    nombre = " ".join(part for part in parts if part).strip() or "SIN NOMBRE"
    dni = str(row.get("dni") or "").strip() or "-"
    login = str(row.get("login") or "").strip()
    email = str(row.get("email") or "").strip()
    niveles = ", ".join(
        _profesor_edit_level_label(nivel_id)
        for nivel_id in _profesor_edit_level_ids(row)
    )
    label = f"{nombre} | DNI {dni}"
    if login:
        label = f"{label} | {login}"
    if email:
        label = f"{label} | {email}"
    if niveles:
        label = f"{label} | {niveles}"
    return label


def _profesor_edit_matches_filter(row: Dict[str, object], search_text: object) -> bool:
    search_norm = _normalize_compare_text(search_text)
    if not search_norm:
        return True

    tokens = [_normalize_compare_id(token) for token in search_norm.split() if token]
    if not tokens:
        return True

    searchable_values = [
        _normalize_compare_id(row.get("persona_id")),
        _normalize_compare_id(row.get("dni")),
        _normalize_compare_id(row.get("login")),
        _normalize_compare_text(row.get("login")),
        _normalize_compare_text(row.get("email")),
        _normalize_compare_text(row.get("nombre")),
        _normalize_compare_text(row.get("apellido_paterno")),
        _normalize_compare_text(row.get("apellido_materno")),
        _normalize_compare_text(_profesor_edit_option_label(row)),
    ]
    searchable = " ".join(value for value in searchable_values if value)
    return all(token in searchable for token in tokens)


def _clear_profesores_edit_state() -> None:
    for state_key in (
        "profesores_edit_rows",
        "profesores_edit_errors",
        "profesores_edit_summary",
        "profesores_edit_colegio_id",
        "profesores_edit_selected_persona_id",
        "profesores_edit_selected_nivel_id",
        "profesores_edit_loaded_persona_id",
        "profesores_edit_loaded_nivel_id",
        "profesores_edit_detail",
        "profesores_edit_search",
        "profesores_edit_nombre",
        "profesores_edit_apellido_paterno",
        "profesores_edit_apellido_materno",
        "profesores_edit_sexo",
        "profesores_edit_dni",
        "profesores_edit_email",
        "profesores_edit_login",
        "profesores_edit_original_login",
        "profesores_edit_password",
        "profesores_edit_fetch_error",
        "profesores_edit_notice",
        "profesores_edit_pending_detail_refresh",
    ):
        st.session_state.pop(state_key, None)


def _store_profesor_edit_detail_state(
    detail: Dict[str, object],
    persona_id: int,
    nivel_id: int,
) -> None:
    persona_login = detail.get("personaLogin") if isinstance(detail.get("personaLogin"), dict) else {}
    login_txt = str(persona_login.get("login") or "").strip()
    st.session_state["profesores_edit_detail"] = detail
    st.session_state["profesores_edit_loaded_persona_id"] = int(persona_id)
    st.session_state["profesores_edit_loaded_nivel_id"] = int(nivel_id)
    st.session_state["profesores_edit_nombre"] = str(detail.get("nombre") or "").strip()
    st.session_state["profesores_edit_apellido_paterno"] = str(
        detail.get("apellidoPaterno") or ""
    ).strip()
    st.session_state["profesores_edit_apellido_materno"] = str(
        detail.get("apellidoMaterno") or ""
    ).strip()
    st.session_state["profesores_edit_sexo"] = str(
        detail.get("sexoMoral") or detail.get("sexo") or ""
    ).strip()
    st.session_state["profesores_edit_dni"] = str(detail.get("idOficial") or "").strip()
    st.session_state["profesores_edit_email"] = str(detail.get("email") or "").strip()
    st.session_state["profesores_edit_login"] = login_txt
    st.session_state["profesores_edit_original_login"] = login_txt
    st.session_state["profesores_edit_password"] = ""
    st.session_state["profesores_edit_fetch_error"] = ""


def _fetch_profesor_edit_detail_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    persona_id: int,
    timeout: int,
) -> Tuple[Optional[Dict[str, object]], str]:
    url = CENSO_PROFESOR_DETALLE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        persona_id=int(persona_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers, timeout=int(timeout))
    except requests.RequestException as exc:
        return None, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return None, message or f"HTTP {status_code}"
    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return None, message
    data = body.get("data") if isinstance(body, dict) else {}
    if not isinstance(data, dict):
        return None, "Respuesta invalida"
    return data, ""


def _update_profesor_edit_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    persona_id: int,
    nombre: str,
    apellido_paterno: str,
    apellido_materno: str,
    sexo: str,
    email: str,
    id_oficial: str,
    timeout: int,
) -> Tuple[bool, Dict[str, object], str]:
    url = CENSO_PROFESOR_DETALLE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        persona_id=int(persona_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "nombre": str(nombre or "").strip(),
        "apellidoPaterno": str(apellido_paterno or "").strip(),
        "apellidoMaterno": str(apellido_materno or "").strip(),
        "sexo": str(sexo or "").strip(),
        "email": str(email or "").strip(),
        "idOficial": str(id_oficial or "").strip(),
    }
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, {}, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, {}, message or f"HTTP {status_code}"
    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return False, {}, message
    data = body.get("data") if isinstance(body, dict) else {}
    if data is None:
        data = {}
    if not isinstance(data, dict):
        return False, {}, "Respuesta invalida"
    return True, data, ""


def _validar_login_profesor_web(
    token: str,
    empresa_id: int,
    login: str,
    persona_id: int,
    timeout: int,
) -> Tuple[bool, str]:
    url = DASHBOARD_VALIDAR_LOGIN_URL.format(empresa_id=int(empresa_id))
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "login": str(login or "").strip(),
        "personaId": int(persona_id),
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, message or f"HTTP {status_code}"
    return _validacion_dashboard_ok(body)


def _update_login_profesor_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    persona_id: int,
    login: str,
    password: str,
    timeout: int,
) -> Tuple[bool, Dict[str, object], str]:
    url = CENSO_PROFESOR_UPDATE_LOGIN_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        persona_id=int(persona_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "login": str(login or "").strip(),
        "password": str(password or ""),
    }
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, {}, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, {}, message or f"HTTP {status_code}"
    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return False, {}, message
    data = body.get("data") if isinstance(body, dict) else {}
    if data is None:
        data = {}
    if not isinstance(data, dict):
        return False, {}, "Respuesta invalida"
    return True, data, ""


def _build_profesor_edit_group_rows(detail: Dict[str, object], nivel_id: int) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for nivel_entry in detail.get("niveles") or []:
        if not isinstance(nivel_entry, dict):
            continue
        nivel_info = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
        nivel_id_current = _safe_int(nivel_info.get("nivelId"))
        if nivel_id_current is None or int(nivel_id_current) != int(nivel_id):
            continue
        for group_entry in nivel_entry.get("colegioGradoGrupos") or []:
            if not isinstance(group_entry, dict):
                continue
            grado_info = group_entry.get("grado") if isinstance(group_entry.get("grado"), dict) else {}
            grupo_info = group_entry.get("grupo") if isinstance(group_entry.get("grupo"), dict) else {}
            alias_txt = str(group_entry.get("alias") or "").strip()
            grupo_txt = str(grupo_info.get("grupo") or "").strip()
            rows.append(
                {
                    "ColegioGradoGrupo ID": _safe_int(group_entry.get("colegioGradoGrupoId")) or "",
                    "Grado": str(grado_info.get("grado") or "").strip(),
                    "Grupo": grupo_txt,
                    "Alias": alias_txt,
                    "Seccion visible": alias_txt or grupo_txt,
                }
            )
    rows.sort(
        key=lambda row: (
            str(row.get("Grado") or "").upper(),
            str(row.get("Seccion visible") or "").upper(),
            int(_safe_int(row.get("ColegioGradoGrupo ID")) or 0),
        )
    )
    return rows


def _asignar_alumno_a_clase_web(
    token: str,
    empresa_id: int,
    ciclo_id: int,
    clase_id: int,
    alumno_id: int,
    timeout: int,
) -> Tuple[bool, str]:
    url = GESTION_ESCOLAR_ALUMNOS_CLASE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        clase_id=int(clase_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {"alumnoId": int(alumno_id)}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, message or f"HTTP {status_code}"

    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return False, message
    return True, ""


def _apply_auto_move_changes(
    token: str,
    colegio_id: Optional[int],
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    plan_rows: List[Dict[str, object]],
) -> Tuple[Dict[str, int], List[Dict[str, object]]]:
    summary = {
        "total": len(plan_rows),
        "inactivar_ok": 0,
        "inactivar_error": 0,
        "mover_ok": 0,
        "mover_error": 0,
        "asignar_ok": 0,
        "asignar_error": 0,
        "asignar_skip": 0,
    }
    resultados: List[Dict[str, object]] = []
    inactivados_seen: Set[int] = set()
    for plan in plan_rows:
        pagado = plan.get("alumno_pagado") if isinstance(plan.get("alumno_pagado"), dict) else {}
        inactivar = plan.get("alumno_inactivar") if isinstance(plan.get("alumno_inactivar"), dict) else {}
        alumno_pagado_id = _safe_int(pagado.get("alumno_id"))
        plan_colegio_id = _safe_int(plan.get("colegio_id"))
        if plan_colegio_id is None:
            plan_colegio_id = _safe_int(colegio_id)
        label_pagado = _format_alumno_label(pagado)
        result_row = {
            "Colegio": int(plan_colegio_id) if plan_colegio_id is not None else "",
            "Alumno pagado": label_pagado,
            "Comparacion": str(plan.get("comparacion") or ""),
            "Inactivar no pagado": "No aplica",
            "Mover": "No aplica",
            "Asignar clases": "No aplica",
            "Detalle": "",
        }

        if plan_colegio_id is None:
            if _to_bool(plan.get("requiere_inactivar")):
                summary["inactivar_error"] += 1
                result_row["Inactivar no pagado"] = "ERROR (sin colegio_id)"
            if alumno_pagado_id is not None:
                summary["mover_error"] += 1
                result_row["Mover"] = "ERROR (sin colegio_id)"
            result_row["Asignar clases"] = "SKIP (sin colegio_id)"
            resultados.append(result_row)
            continue

        alumno_inactivar_id = _safe_int(inactivar.get("alumno_id"))
        if _to_bool(plan.get("requiere_inactivar")) and alumno_inactivar_id is not None:
            if int(alumno_inactivar_id) in inactivados_seen:
                result_row["Inactivar no pagado"] = "SKIP repetido"
            else:
                nivel_inactivar_id = _safe_int(inactivar.get("nivel_id"))
                grado_inactivar_id = _safe_int(inactivar.get("grado_id"))
                grupo_inactivar_id = _safe_int(inactivar.get("grupo_id"))
                if (
                    nivel_inactivar_id is None
                    or grado_inactivar_id is None
                    or grupo_inactivar_id is None
                ):
                    result_row["Inactivar no pagado"] = "SKIP datos incompletos"
                else:
                    inactivar_ok, inactivar_msg = _set_alumno_activo_web(
                        token=token,
                        colegio_id=int(plan_colegio_id),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        nivel_id=int(nivel_inactivar_id),
                        grado_id=int(grado_inactivar_id),
                        grupo_id=int(grupo_inactivar_id),
                        alumno_id=int(alumno_inactivar_id),
                        activo=0,
                        observaciones="Inactivado por comparacion automatica (no pagado).",
                        timeout=int(timeout),
                    )
                    if inactivar_ok:
                        summary["inactivar_ok"] += 1
                        inactivados_seen.add(int(alumno_inactivar_id))
                        result_row["Inactivar no pagado"] = f"OK ({inactivar_msg})"
                    else:
                        summary["inactivar_error"] += 1
                        result_row["Inactivar no pagado"] = f"ERROR ({inactivar_msg})"

        move_done = False
        grupo_origen_id = _safe_int(plan.get("grupo_origen_id"))
        grupo_destino_id = _safe_int(plan.get("grupo_destino_id"))
        nivel_id = _safe_int(plan.get("nivel_id"))
        grado_id = _safe_int(plan.get("grado_id"))
        if (
            alumno_pagado_id is not None
            and grupo_origen_id is not None
            and grupo_destino_id is not None
            and nivel_id is not None
            and grado_id is not None
        ):
            if int(grupo_origen_id) == int(grupo_destino_id):
                result_row["Mover"] = "SKIP mismo grupo"
            else:
                move_ok, move_msg = _mover_alumno_web(
                    token=token,
                    colegio_id=int(plan_colegio_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    nivel_id=int(nivel_id),
                    grado_id=int(grado_id),
                    grupo_id=int(grupo_origen_id),
                    alumno_id=int(alumno_pagado_id),
                    nuevo_nivel_id=int(nivel_id),
                    nuevo_grado_id=int(grado_id),
                    nuevo_grupo_id=int(grupo_destino_id),
                    timeout=int(timeout),
                )
                if move_ok:
                    summary["mover_ok"] += 1
                    move_done = True
                    result_row["Mover"] = "OK"
                else:
                    summary["mover_error"] += 1
                    result_row["Mover"] = f"ERROR ({move_msg})"
        elif alumno_pagado_id is not None:
            result_row["Mover"] = "SKIP datos incompletos"

        clases_destino = plan.get("clases_destino") if isinstance(plan.get("clases_destino"), list) else []
        if move_done and clases_destino and alumno_pagado_id is not None:
            seen_clase_ids: Set[int] = set()
            assign_ok_count = 0
            assign_err_count = 0
            for clase in clases_destino:
                clase_id = _safe_int(clase.get("clase_id")) if isinstance(clase, dict) else None
                if clase_id is None or int(clase_id) in seen_clase_ids:
                    continue
                seen_clase_ids.add(int(clase_id))
                ok_assign, err_assign = _asignar_alumno_a_clase_web(
                    token=token,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    clase_id=int(clase_id),
                    alumno_id=int(alumno_pagado_id),
                    timeout=int(timeout),
                )
                if ok_assign:
                    assign_ok_count += 1
                    summary["asignar_ok"] += 1
                else:
                    assign_err_count += 1
                    summary["asignar_error"] += 1
                    if result_row["Detalle"]:
                        result_row["Detalle"] = f"{result_row['Detalle']} | {err_assign}"
                    else:
                        result_row["Detalle"] = str(err_assign)
            result_row["Asignar clases"] = f"OK {assign_ok_count} | ERROR {assign_err_count}"
        elif move_done and alumno_pagado_id is not None:
            result_row["Asignar clases"] = "Sin clases destino"
            summary["asignar_skip"] += 1
        else:
            result_row["Asignar clases"] = "SKIP (sin movimiento)"
            summary["asignar_skip"] += len(clases_destino)

        resultados.append(result_row)

    return summary, resultados


def _build_manual_move_grade_catalog(
    niveles_data: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    catalog: List[Dict[str, object]] = []
    for nivel_entry in niveles_data:
        if not isinstance(nivel_entry, dict):
            continue
        nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
        nivel_id = _safe_int(nivel.get("nivelId"))
        if nivel_id is None:
            continue
        nivel_nombre = str(nivel.get("nivel") or "").strip()
        grados = nivel_entry.get("grados") if isinstance(nivel_entry.get("grados"), list) else []
        for grado_entry in grados:
            if not isinstance(grado_entry, dict):
                continue
            grado = grado_entry.get("grado") if isinstance(grado_entry.get("grado"), dict) else {}
            grado_id = _safe_int(grado.get("gradoId"))
            if grado_id is None:
                continue
            grado_nombre = str(grado.get("grado") or "").strip()
            grupos_raw = grado_entry.get("grupos") if isinstance(grado_entry.get("grupos"), list) else []
            grupos: List[Dict[str, object]] = []
            seen_grupo_ids: Set[int] = set()
            for grupo_entry in grupos_raw:
                if not isinstance(grupo_entry, dict):
                    continue
                grupo = grupo_entry.get("grupo") if isinstance(grupo_entry.get("grupo"), dict) else {}
                grupo_id = _safe_int(grupo.get("grupoId"))
                if grupo_id is None or int(grupo_id) in seen_grupo_ids:
                    continue
                seen_grupo_ids.add(int(grupo_id))
                grupo_nombre = str(grupo.get("grupo") or "").strip()
                seccion = _normalize_seccion_key(grupo.get("grupoClave") or grupo_nombre)
                if not seccion:
                    seccion = str(grupo.get("grupoClave") or grupo_nombre or "").strip()
                grupos.append(
                    {
                        "grupo_id": int(grupo_id),
                        "seccion": seccion,
                        "grupo": grupo_nombre,
                    }
                )
            grupos.sort(
                key=lambda row: _grupo_sort_key(
                    str(row.get("seccion") or ""),
                    str(row.get("grupo") or ""),
                )
            )
            if not grupos:
                continue
            catalog.append(
                {
                    "nivel_id": int(nivel_id),
                    "nivel": nivel_nombre,
                    "grado_id": int(grado_id),
                    "grado": grado_nombre,
                    "grupos": grupos,
                }
            )

    catalog.sort(
        key=lambda row: (
            int(row.get("nivel_id") or 0),
            int(row.get("grado_id") or 0),
            str(row.get("nivel") or "").upper(),
            str(row.get("grado") or "").upper(),
        )
    )
    return catalog


def _build_manual_move_destination_catalog(
    niveles_data: List[Dict[str, object]],
) -> Dict[str, object]:
    grade_catalog = _build_manual_move_grade_catalog(niveles_data)
    nivel_name_by_id: Dict[int, str] = {}
    grado_ids: List[int] = []
    grado_payload_by_id: Dict[int, Dict[str, object]] = {}
    grado_ids_by_nivel: Dict[int, List[int]] = {}
    grado_name_by_key: Dict[Tuple[int, int], str] = {}
    grupo_ids_by_grade: Dict[Tuple[int, int], List[int]] = {}
    grupo_payload_by_key: Dict[Tuple[int, int, int], Dict[str, object]] = {}
    grupo_payload_by_grado_seccion: Dict[Tuple[int, str], Dict[str, object]] = {}
    seccion_options: List[str] = []

    for grade in grade_catalog:
        nivel_id = _safe_int(grade.get("nivel_id"))
        grado_id = _safe_int(grade.get("grado_id"))
        if nivel_id is None or grado_id is None:
            continue
        nivel_key = int(nivel_id)
        grado_key = int(grado_id)
        nivel_name_by_id[nivel_key] = str(grade.get("nivel") or "").strip()
        if grado_key not in grado_ids:
            grado_ids.append(grado_key)
        grado_payload_by_id[grado_key] = {
            "nivel_id": nivel_key,
            "nivel": str(grade.get("nivel") or "").strip(),
            "grado_id": grado_key,
            "grado": str(grade.get("grado") or "").strip(),
        }
        grado_ids_by_nivel.setdefault(nivel_key, []).append(grado_key)
        grado_name_by_key[(nivel_key, grado_key)] = str(grade.get("grado") or "").strip()
        grupos = grade.get("grupos") if isinstance(grade.get("grupos"), list) else []
        grupo_ids_by_grade[(nivel_key, grado_key)] = []
        for group in grupos:
            if not isinstance(group, dict):
                continue
            grupo_id = _safe_int(group.get("grupo_id"))
            if grupo_id is None:
                continue
            grupo_key = int(grupo_id)
            grupo_ids_by_grade[(nivel_key, grado_key)].append(grupo_key)
            grupo_payload_by_key[(nivel_key, grado_key, grupo_key)] = {
                "grupo_id": grupo_key,
                "seccion": str(group.get("seccion") or "").strip(),
                "grupo": str(group.get("grupo") or "").strip(),
                "nivel_id": nivel_key,
                "grado_id": grado_key,
                "nivel": str(grade.get("nivel") or "").strip(),
                "grado": str(grade.get("grado") or "").strip(),
            }
            seccion_key = _normalize_seccion_key(group.get("seccion") or group.get("grupo") or "")
            if seccion_key:
                grupo_payload_by_grado_seccion[(grado_key, seccion_key)] = grupo_payload_by_key[
                    (nivel_key, grado_key, grupo_key)
                ]
                if seccion_key not in seccion_options:
                    seccion_options.append(seccion_key)

    return {
        "nivel_ids": sorted(nivel_name_by_id.keys()),
        "grado_ids": sorted(grado_ids),
        "grado_payload_by_id": grado_payload_by_id,
        "nivel_name_by_id": nivel_name_by_id,
        "grado_ids_by_nivel": grado_ids_by_nivel,
        "grado_name_by_key": grado_name_by_key,
        "grupo_ids_by_grade": grupo_ids_by_grade,
        "grupo_payload_by_key": grupo_payload_by_key,
        "grupo_payload_by_grado_seccion": grupo_payload_by_grado_seccion,
        "seccion_options": sorted(
            seccion_options,
            key=lambda value: _grupo_sort_key(str(value), str(value)),
        ),
    }


def _manual_move_group_label(payload: Dict[str, object]) -> str:
    seccion = _normalize_seccion_key(payload.get("seccion") or "")
    if seccion:
        return seccion
    grupo = str(payload.get("grupo") or "").strip()
    if grupo:
        return grupo
    grupo_id = _safe_int(payload.get("grupo_id"))
    if grupo_id is None:
        return "-"
    return str(int(grupo_id))


def _update_manual_move_cached_student(
    students: List[Dict[str, object]],
    alumno_id: int,
    destino_payload: Dict[str, object],
) -> None:
    for current in students:
        if _safe_int(current.get("alumno_id")) != int(alumno_id):
            continue
        current["nivel_id"] = _safe_int(destino_payload.get("nivel_id"))
        current["grado_id"] = _safe_int(destino_payload.get("grado_id"))
        current["grupo_id"] = _safe_int(destino_payload.get("grupo_id"))
        current["nivel"] = str(destino_payload.get("nivel") or "").strip()
        current["grado"] = str(destino_payload.get("grado") or "").strip()
        current["seccion"] = str(destino_payload.get("seccion") or "").strip()
        current["seccion_norm"] = _normalize_seccion_key(destino_payload.get("seccion") or "")
        break


def _clear_manual_move_selection(*keys: str) -> None:
    for key in keys:
        if key:
            st.session_state[key] = None


def _queue_manual_move_reset(*keys: str) -> None:
    pending = st.session_state.get("alumnos_manual_move_pending_reset_keys")
    if not isinstance(pending, list):
        pending = []
    for key in keys:
        if key and key not in pending:
            pending.append(key)
    st.session_state["alumnos_manual_move_pending_reset_keys"] = pending


@st.dialog("Mover alumno", width="large")
def _show_alumno_edit_move_dialog(
    alumno_row: Dict[str, object],
    alumno_context: Dict[str, int],
    niveles_data: List[Dict[str, object]],
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> None:
    alumno_id = _safe_int(alumno_context.get("alumno_id"))
    persona_id = _safe_int(alumno_context.get("persona_id"))
    nivel_id_actual = _safe_int(alumno_context.get("nivel_id"))
    grado_id_actual = _safe_int(alumno_context.get("grado_id"))
    grupo_id_actual = _safe_int(alumno_context.get("grupo_id"))
    if None in {alumno_id, persona_id, nivel_id_actual, grado_id_actual, grupo_id_actual}:
        st.error("No se pudo resolver el contexto actual del alumno.")
        if st.button("Cerrar", key="alumnos_edit_move_close_invalid", use_container_width=True):
            _clear_alumnos_edit_move_state(close_dialog=True)
            st.rerun()
        return

    alumno_id_int = int(alumno_id)
    persona_id_int = int(persona_id)
    nivel_id_actual_int = int(nivel_id_actual)
    grado_id_actual_int = int(grado_id_actual)
    grupo_id_actual_int = int(grupo_id_actual)
    seccion_actual = _normalize_seccion_key(
        alumno_row.get("seccion_norm") or alumno_row.get("seccion") or ""
    )

    destination_catalog = _build_manual_move_destination_catalog(niveles_data)
    nivel_ids = destination_catalog.get("nivel_ids") or []
    nivel_name_by_id = destination_catalog.get("nivel_name_by_id") or {}
    grado_payload_by_id = destination_catalog.get("grado_payload_by_id") or {}
    grado_ids_by_nivel = destination_catalog.get("grado_ids_by_nivel") or {}
    grupo_payload_by_grado_seccion = (
        destination_catalog.get("grupo_payload_by_grado_seccion") or {}
    )

    if not nivel_ids:
        st.warning("No hay niveles, grados o secciones disponibles para destino.")
        if st.button("Cerrar", key="alumnos_edit_move_close_empty", use_container_width=True):
            _clear_alumnos_edit_move_state(close_dialog=True)
            st.rerun()
        return

    secciones_by_grado: Dict[int, List[str]] = {}
    for (grado_id_tmp, seccion), payload in grupo_payload_by_grado_seccion.items():
        grado_id_int = _safe_int(grado_id_tmp)
        if grado_id_int is None or not isinstance(payload, dict) or not payload:
            continue
        secciones_by_grado.setdefault(int(grado_id_int), []).append(str(seccion))
    for grado_id_int, secciones in list(secciones_by_grado.items()):
        secciones_by_grado[grado_id_int] = sorted(
            list(dict.fromkeys(secciones)),
            key=lambda value: _grupo_sort_key(str(value), str(value)),
        )

    nivel_key = "alumnos_edit_move_nivel_id"
    grado_key = "alumnos_edit_move_grado_id"
    seccion_key = "alumnos_edit_move_seccion"
    last_alumno_id = _safe_int(st.session_state.get("alumnos_edit_move_last_alumno_id"))
    if last_alumno_id != alumno_id_int:
        st.session_state[nivel_key] = nivel_id_actual_int
        st.session_state[grado_key] = grado_id_actual_int
        st.session_state[seccion_key] = seccion_actual
        st.session_state["alumnos_edit_move_last_alumno_id"] = alumno_id_int

    selected_nivel_id = _safe_int(st.session_state.get(nivel_key))
    if selected_nivel_id not in nivel_ids:
        st.session_state[nivel_key] = nivel_id_actual_int
        selected_nivel_id = nivel_id_actual_int

    grado_options = [
        int(value)
        for value in (grado_ids_by_nivel.get(int(selected_nivel_id)) or [])
    ] if selected_nivel_id is not None else []
    selected_grado_id = _safe_int(st.session_state.get(grado_key))
    if selected_grado_id not in grado_options:
        fallback_grado_id = grado_id_actual_int if grado_id_actual_int in grado_options else None
        st.session_state[grado_key] = fallback_grado_id
        selected_grado_id = fallback_grado_id

    secciones_grado = (
        secciones_by_grado.get(int(selected_grado_id), [])
        if selected_grado_id is not None
        else []
    )
    selected_seccion = _normalize_seccion_key(st.session_state.get(seccion_key) or "")
    if not selected_seccion or selected_seccion not in secciones_grado:
        fallback_seccion = seccion_actual if seccion_actual in secciones_grado else ""
        st.session_state[seccion_key] = fallback_seccion or None
        selected_seccion = fallback_seccion

    st.markdown(f"### {_manual_move_alumno_option_label(alumno_row)}")
    st.caption(
        "Actual: {nivel} | {grado} | {seccion}".format(
            nivel=str(alumno_row.get("nivel") or "").strip() or "-",
            grado=str(alumno_row.get("grado") or "").strip() or "-",
            seccion=seccion_actual or "-",
        )
    )

    form_cols = st.columns(3, gap="small")
    with form_cols[0]:
        st.selectbox(
            "Nuevo nivel",
            options=nivel_ids,
            index=None,
            placeholder="Nivel",
            format_func=lambda value: str(nivel_name_by_id.get(int(value), value)).strip(),
            key=nivel_key,
            on_change=_clear_manual_move_selection,
            args=(grado_key, seccion_key),
        )

    selected_nivel_id = _safe_int(st.session_state.get(nivel_key))
    grado_options = [
        int(value)
        for value in (grado_ids_by_nivel.get(int(selected_nivel_id)) or [])
    ] if selected_nivel_id is not None else []
    with form_cols[1]:
        st.selectbox(
            "Nuevo grado",
            options=grado_options,
            index=None,
            placeholder="Grado",
            format_func=lambda value: str(
                (grado_payload_by_id.get(int(value), {}) or {}).get("grado") or value
            ).strip(),
            key=grado_key,
            on_change=_clear_manual_move_selection,
            args=(seccion_key,),
            disabled=not grado_options,
        )

    selected_grado_id = _safe_int(st.session_state.get(grado_key))
    secciones_grado = (
        secciones_by_grado.get(int(selected_grado_id), [])
        if selected_grado_id is not None
        else []
    )
    with form_cols[2]:
        st.selectbox(
            "Nueva seccion",
            options=secciones_grado,
            index=None,
            placeholder="Seccion",
            key=seccion_key,
            disabled=not secciones_grado,
        )

    action_cols = st.columns(2, gap="small")
    if action_cols[0].button(
        "Mover",
        type="primary",
        key=f"alumnos_edit_move_apply_btn_{alumno_id_int}",
        use_container_width=True,
    ):
        token = _get_shared_token()
        if not token:
            st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
            return

        selected_nivel_id = _safe_int(st.session_state.get(nivel_key))
        selected_grado_id = _safe_int(st.session_state.get(grado_key))
        selected_seccion = _normalize_seccion_key(st.session_state.get(seccion_key) or "")
        if selected_nivel_id is None or selected_grado_id is None or not selected_seccion:
            st.warning("Completa nivel, grado y seccion destino.")
            return
        if (
            int(selected_nivel_id) == nivel_id_actual_int
            and int(selected_grado_id) == grado_id_actual_int
            and selected_seccion == seccion_actual
        ):
            st.warning("Selecciona un destino distinto al actual.")
            return

        grado_payload = grado_payload_by_id.get(int(selected_grado_id), {})
        grado_nivel_id = _safe_int(grado_payload.get("nivel_id"))
        if grado_nivel_id is None or int(grado_nivel_id) != int(selected_nivel_id):
            st.warning("El nivel seleccionado no corresponde al grado elegido.")
            return

        destino_payload = grupo_payload_by_grado_seccion.get(
            (int(selected_grado_id), selected_seccion)
        )
        if not isinstance(destino_payload, dict) or not destino_payload:
            st.warning("No se pudo resolver la seccion destino.")
            return

        try:
            with st.spinner(f"Moviendo alumno {alumno_id_int}..."):
                result = _apply_single_alumno_move_and_reassign(
                    token=token,
                    colegio_id=int(colegio_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                    alumno_row=alumno_row,
                    nuevo_nivel_id=int(destino_payload.get("nivel_id") or 0),
                    nuevo_grado_id=int(destino_payload.get("grado_id") or 0),
                    nuevo_grupo_id=int(destino_payload.get("grupo_id") or 0),
                    nueva_seccion=str(destino_payload.get("seccion") or ""),
                )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"No se pudo mover el alumno: {exc}")
            return

        if not _to_bool(result.get("move_ok")):
            st.warning(
                "No se pudo mover el alumno: {msg}".format(
                    msg=str(result.get("move_msg") or "sin detalle")
                )
            )
            return

        refresh_warning = _refresh_alumnos_edit_catalog_and_detail(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
            alumno_id=alumno_id_int,
            fallback_context={
                "alumno_id": alumno_id_int,
                "persona_id": persona_id_int,
                "nivel_id": int(destino_payload.get("nivel_id") or 0),
                "grado_id": int(destino_payload.get("grado_id") or 0),
                "grupo_id": int(destino_payload.get("grupo_id") or 0),
            },
        )
        st.session_state["alumnos_edit_notice"] = {
            "type": "success",
            "message": (
                "Alumno movido: {alumno} -> {nivel} | {grado} ({seccion}).{refresh}".format(
                    alumno=_manual_move_alumno_option_label(alumno_row),
                    nivel=str(destino_payload.get("nivel") or "").strip() or "-",
                    grado=str(destino_payload.get("grado") or "").strip() or "-",
                    seccion=_manual_move_group_label(destino_payload),
                    refresh=refresh_warning,
                )
            ).strip(),
        }
        _clear_alumnos_edit_move_state(close_dialog=True)
        st.rerun()

    if action_cols[1].button(
        "Cancelar",
        key=f"alumnos_edit_move_cancel_btn_{alumno_id_int}",
        use_container_width=True,
    ):
        _clear_alumnos_edit_move_state(close_dialog=True)
        st.rerun()


def _render_crud_menu(
    title: str,
    items: List[Tuple[str, str, str]],
    state_key: str,
) -> str:
    options = [item[0] for item in items]
    labels = {item[0]: item[1] for item in items}
    captions = [item[2] for item in items]
    with st.container(border=True):
        st.caption(title)
        selected = st.radio(
            title,
            options=options,
            index=0,
            format_func=lambda value: labels.get(value, value),
            captions=captions,
            key=state_key,
            label_visibility="collapsed",
            width="stretch",
        )
    return str(selected or options[0])


def _manual_move_alumno_option_label(row: Dict[str, object]) -> str:
    nombre = str(row.get("nombre_completo") or "").strip()
    if not nombre:
        nombre = "SIN NOMBRE"
    dni = str(row.get("id_oficial") or "").strip() or "-"
    login = str(row.get("login") or "").strip()
    base = nombre
    if dni:
        base = f"{base} | {dni}"
    if login:
        base = f"{base} | {login}"
    return base


def _manual_move_alumno_matches_filter(row: Dict[str, object], search_text: object) -> bool:
    search_norm = _normalize_compare_text(search_text)
    if not search_norm:
        return True

    tokens = [_normalize_compare_id(token) for token in search_norm.split() if token]
    if not tokens:
        return True

    searchable_values = [
        _normalize_compare_id(row.get("id_oficial")),
        _normalize_compare_id(row.get("login")),
        _normalize_compare_text(row.get("login")),
        _normalize_compare_text(row.get("nombre_completo")),
        _normalize_compare_text(row.get("nombre")),
        _normalize_compare_text(row.get("apellido_paterno")),
        _normalize_compare_text(row.get("apellido_materno")),
    ]
    searchable_values = [value for value in searchable_values if value]
    if not searchable_values:
        return False

    return all(any(token in value for value in searchable_values) for token in tokens)


def _find_existing_alumno_by_identificador(
    rows: List[Dict[str, object]],
    identificador: object,
) -> Optional[Dict[str, object]]:
    identificador_norm = re.sub(r"\D", "", str(identificador or ""))
    if not identificador_norm:
        return None
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = re.sub(r"\D", "", str(row.get("id_oficial") or ""))
        if row_id and row_id == identificador_norm:
            return row
    return None


def _dedupe_and_sort_censo_students(
    rows: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    by_key: Dict[str, Dict[str, object]] = {}
    for row in rows:
        alumno_id = _safe_int(row.get("alumno_id"))
        persona_id = _safe_int(row.get("persona_id"))
        grupo_id = _safe_int(row.get("grupo_id"))
        if alumno_id is not None:
            key = f"alumno:{int(alumno_id)}"
        elif persona_id is not None and grupo_id is not None:
            key = f"persona_grupo:{int(persona_id)}:{int(grupo_id)}"
        elif persona_id is not None:
            key = f"persona:{int(persona_id)}"
        else:
            key = (
                f"anon:{_normalize_plain_text(row.get('nombre_completo'))}:"
                f"{_normalize_plain_text(row.get('id_oficial'))}"
            )
        if key in by_key:
            continue
        by_key[key] = row

    return sorted(
        by_key.values(),
        key=lambda row: (
            int(_safe_int(row.get("nivel_id")) or 0),
            int(_safe_int(row.get("grado_id")) or 0),
            _grupo_sort_key(
                str(row.get("seccion_norm") or ""),
                str(row.get("seccion") or ""),
            ),
            str(row.get("apellido_paterno") or "").upper(),
            str(row.get("apellido_materno") or "").upper(),
            str(row.get("nombre") or "").upper(),
        ),
    )


def _fetch_alumnos_catalog_for_manual_move(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    on_status: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message))
            except Exception:
                pass

    _status("Listando niveles, grados y secciones del colegio...")
    niveles = _fetch_niveles_grados_grupos_censo(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
    )
    _status("Consultando login de alumnos...")
    try:
        login_lookup_by_alumno, login_lookup_by_persona = _fetch_login_password_lookup_censo(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
    except Exception:
        login_lookup_by_alumno = {}
        login_lookup_by_persona = {}
    if not _build_contexts_for_nivel_grado(niveles=niveles):
        raise RuntimeError("No hay niveles/grados/secciones configurados para este colegio.")

    alumnos_raw: List[Dict[str, object]] = []
    errors: List[str] = []
    _status("Consultando alumnos del colegio con alumnosByFilters...")
    try:
        alumnos_ctx = _fetch_alumnos_censo_by_filters(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
        for item in alumnos_ctx:
            if not isinstance(item, dict):
                continue
            flat = _flatten_censo_alumno_for_auto_plan(item=item, fallback={})
            login_txt, password_txt = _resolve_alumno_login_password(
                item,
                login_lookup_by_alumno,
                login_lookup_by_persona,
            )
            if login_txt and not str(flat.get("login") or "").strip():
                flat["login"] = login_txt
            if password_txt and not str(flat.get("password") or "").strip():
                flat["password"] = password_txt
            alumnos_raw.append(flat)
    except Exception as exc:
        errors.append(f"alumnosByFilters: {exc}")
        _status("alumnosByFilters fallo. Reintentando por nivel, grado y seccion...")
        contexts = _build_contexts_for_nivel_grado(niveles=niveles)
        total_contexts = len(contexts)
        for idx_ctx, ctx in enumerate(contexts, start=1):
            _status(
                "Listando alumnos {idx}/{total} | nivelId={nivel} gradoId={grado} grupoId={grupo}".format(
                    idx=idx_ctx,
                    total=total_contexts,
                    nivel=ctx.get("nivel_id"),
                    grado=ctx.get("grado_id"),
                    grupo=ctx.get("grupo_id"),
                )
            )
            try:
                alumnos_ctx = _fetch_alumnos_censo(
                    token=token,
                    colegio_id=int(colegio_id),
                    nivel_id=int(ctx["nivel_id"]),
                    grado_id=int(ctx["grado_id"]),
                    grupo_id=int(ctx["grupo_id"]),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as inner_exc:
                errors.append(
                    "nivelId={nivel} gradoId={grado} grupoId={grupo}: {err}".format(
                        nivel=ctx.get("nivel_id"),
                        grado=ctx.get("grado_id"),
                        grupo=ctx.get("grupo_id"),
                        err=inner_exc,
                    )
                )
                continue
            for item in alumnos_ctx:
                if not isinstance(item, dict):
                    continue
                flat = _flatten_censo_alumno_for_auto_plan(item=item, fallback=ctx)
                login_txt, password_txt = _resolve_alumno_login_password(
                    item,
                    login_lookup_by_alumno,
                    login_lookup_by_persona,
                )
                if login_txt and not str(flat.get("login") or "").strip():
                    flat["login"] = login_txt
                if password_txt and not str(flat.get("password") or "").strip():
                    flat["password"] = password_txt
                alumnos_raw.append(flat)

    students = _dedupe_and_sort_censo_students(alumnos_raw)
    _status(f"Listado completo. Alumnos unicos: {len(students)}.")
    return {
        "niveles": niveles,
        "students": students,
        "errors": errors,
    }


def _apply_single_alumno_move_and_reassign(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    alumno_row: Dict[str, object],
    nuevo_nivel_id: int,
    nuevo_grado_id: int,
    nuevo_grupo_id: int,
    nueva_seccion: str,
    on_status: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message))
            except Exception:
                pass

    alumno_id = _safe_int(alumno_row.get("alumno_id"))
    if alumno_id is None:
        raise RuntimeError("Alumno sin alumnoId valido.")
    nivel_origen_id = _safe_int(alumno_row.get("nivel_id"))
    grado_origen_id = _safe_int(alumno_row.get("grado_id"))
    grupo_origen_id = _safe_int(alumno_row.get("grupo_id"))
    if nivel_origen_id is None or grado_origen_id is None or grupo_origen_id is None:
        raise RuntimeError("Alumno sin datos completos de nivel/grado/grupo origen.")
    seccion_origen = str(
        alumno_row.get("seccion_norm") or alumno_row.get("seccion") or ""
    ).strip()

    _status("Listando clases del colegio...")
    clases_rows, _grouped = listar_y_mapear_clases(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
        ordered=True,
        on_log=None,
    )
    clases_unicas: List[Dict[str, object]] = []
    seen_class_ids: Set[int] = set()
    for row in clases_rows:
        clase_id = _safe_int(row.get("clase_id"))
        if clase_id is None or int(clase_id) in seen_class_ids:
            continue
        seen_class_ids.add(int(clase_id))
        clases_unicas.append(
            {
                "clase_id": int(clase_id),
                "clase": str(row.get("clase") or "").strip(),
                "nivel_id": _safe_int(row.get("nivel_id")),
                "grado_id": _safe_int(row.get("grado_id")),
                "grupo_id": _safe_int(row.get("grupo_id")),
                "seccion": str(row.get("seccion") or "").strip(),
            }
        )

    source_classes = _build_clases_destino_for_plan(
        clases_rows=clases_unicas,
        nivel_id=int(nivel_origen_id),
        grado_id=int(grado_origen_id),
        grupo_destino_id=int(grupo_origen_id),
        seccion_destino=seccion_origen,
    )
    target_classes = _build_clases_destino_for_plan(
        clases_rows=clases_unicas,
        nivel_id=int(nuevo_nivel_id),
        grado_id=int(nuevo_grado_id),
        grupo_destino_id=int(nuevo_grupo_id),
        seccion_destino=str(nueva_seccion or ""),
    )
    _status(
        "Clases origen detectadas: {source} | Clases destino detectadas: {target}".format(
            source=len(source_classes),
            target=len(target_classes),
        )
    )

    assigned_classes: List[Dict[str, object]] = list(source_classes)
    scan_errors: List[str] = []
    _status(f"Clases del grupo actual a quitar: {len(assigned_classes)}.")

    activo_raw = alumno_row.get("activo")
    activate_required = activo_raw is not None and not _to_bool(activo_raw)

    activation_ok = True
    activation_msg = "SKIP ya activo"
    if activate_required:
        _status("Alumno inactivo detectado. Activando antes del movimiento...")
        activation_ok, activation_msg = _set_alumno_activo_web(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            nivel_id=int(nivel_origen_id),
            grado_id=int(grado_origen_id),
            grupo_id=int(grupo_origen_id),
            alumno_id=int(alumno_id),
            activo=1,
            observaciones="Activado automaticamente antes de mover de seccion.",
            timeout=int(timeout),
        )

    move_required = not (
        int(nivel_origen_id) == int(nuevo_nivel_id)
        and int(grado_origen_id) == int(nuevo_grado_id)
        and int(grupo_origen_id) == int(nuevo_grupo_id)
    )
    if not activation_ok:
        move_ok = False
        move_msg = f"No se pudo activar el alumno: {activation_msg}"
    elif move_required:
        _status("Moviendo alumno al nuevo grado/seccion...")
        move_ok, move_msg = _mover_alumno_web(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            nivel_id=int(nivel_origen_id),
            grado_id=int(grado_origen_id),
            grupo_id=int(grupo_origen_id),
            alumno_id=int(alumno_id),
            nuevo_nivel_id=int(nuevo_nivel_id),
            nuevo_grado_id=int(nuevo_grado_id),
            nuevo_grupo_id=int(nuevo_grupo_id),
            timeout=int(timeout),
        )
    else:
        move_ok = True
        move_msg = "SKIP mismo destino"

    result: Dict[str, object] = {
        "activation_ok": bool(activation_ok),
        "activation_msg": str(activation_msg or ""),
        "move_ok": bool(move_ok),
        "move_msg": str(move_msg or ""),
        "assigned_before_count": len(assigned_classes),
        "removed_ok": 0,
        "removed_error": 0,
        "removed_errors": [],
        "target_classes_total": 0,
        "assigned_ok": 0,
        "assigned_error": 0,
        "assigned_errors": [],
        "scan_errors": scan_errors,
    }
    if not move_ok:
        return result

    remove_errors: List[str] = []
    removed_ok = 0
    total_assigned = len(assigned_classes)
    for idx, clase in enumerate(assigned_classes, start=1):
        clase_id = int(clase["clase_id"])
        _status(f"Eliminando clases actuales {idx}/{total_assigned}...")
        try:
            _delete_alumno_clase_gestion_escolar(
                token=token,
                clase_id=int(clase_id),
                alumno_id=int(alumno_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
            )
            removed_ok += 1
        except Exception as exc:
            remove_errors.append(f"clase {clase_id}: {exc}")

    _status("Buscando clases del nuevo grado/seccion...")
    target_total = len(target_classes)
    assign_errors: List[str] = []
    assign_ok = 0
    assign_err = 0
    seen_target: Set[int] = set()
    for idx, clase in enumerate(target_classes, start=1):
        clase_id = _safe_int(clase.get("clase_id")) if isinstance(clase, dict) else None
        if clase_id is None or int(clase_id) in seen_target:
            continue
        seen_target.add(int(clase_id))
        _status(f"Asignando clases nuevas {idx}/{target_total}...")
        ok_assign, msg_assign = _asignar_alumno_a_clase_web(
            token=token,
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            clase_id=int(clase_id),
            alumno_id=int(alumno_id),
            timeout=int(timeout),
        )
        if ok_assign:
            assign_ok += 1
        else:
            assign_err += 1
            assign_errors.append(f"clase {int(clase_id)}: {msg_assign}")

    result.update(
        {
            "removed_ok": int(removed_ok),
            "removed_error": len(remove_errors),
            "removed_errors": remove_errors,
            "target_classes_total": int(target_total),
            "assigned_ok": int(assign_ok),
            "assigned_error": int(assign_err),
            "assigned_errors": assign_errors,
        }
    )
    return result


def render_richmond_studio_view() -> None:
    st.markdown(
        """
        <section class="bg-white border border-gray-200 rounded-lg px-4 py-3 mb-3 shadow-sm">
          <div class="text-xs font-semibold uppercase tracking-wider text-blue-700 mb-1">Panel Operativo</div>
          <h1 class="text-2xl font-bold text-gray-900 m-0">Richmond Studio</h1>
          <p class="text-sm text-gray-600 mt-1 mb-0">
            Gestion de clases y exportes de RS en una vista separada de Pegasus.
          </p>
        </section>
        """,
        unsafe_allow_html=True,
    )

    timeout = 30
    rs_token_default = _get_richmondstudio_token()
    if "rs_groups_bearer_token" not in st.session_state:
        st.session_state["rs_groups_bearer_token"] = rs_token_default
    if "rs_bearer_token" not in st.session_state:
        st.session_state["rs_bearer_token"] = rs_token_default

    st.markdown("**Configuracion RS**")
    rs_token = _clean_token(
        st.text_input(
            "Bearer token RS",
            key="rs_groups_bearer_token",
            help="Se usa para clases RS y EXCEL RS.",
        )
    )
    st.session_state["rs_bearer_token"] = rs_token

    def _request_richmondstudio_confirmation(action_key: str, action_label: str) -> None:
        if not rs_token:
            st.error("Ingresa el bearer token de Richmond Studio.")
            return
        try:
            with st.spinner("Validando institucion RS..."):
                current_context = _fetch_richmondstudio_current_user_context(
                    rs_token,
                    timeout=int(timeout),
                )
        except Exception as exc:
            st.error(f"No se pudo obtener la institucion actual de RS: {exc}")
            return
        _set_richmondstudio_pending_confirmation(
            action_key=action_key,
            action_label=action_label,
            context=current_context,
        )

    @st.dialog("Confirmar cambios RS")
    def _render_richmondstudio_confirmation_dialog() -> None:
        pending_confirmation = st.session_state.get("rs_pending_confirmation")
        if not isinstance(pending_confirmation, dict):
            return

        user_name = str(pending_confirmation.get("user_name") or "").strip()
        institution_name = str(pending_confirmation.get("institution_name") or "").strip()
        action_label = str(pending_confirmation.get("label") or "").strip()
        if not institution_name:
            institution_name = "Institucion no identificada"

        greeting = f"Hola {user_name}" if user_name else "Hola"
        st.markdown(greeting)
        st.markdown("El cambio se aplicara a la institucion")
        st.markdown(f"### {institution_name}")
        if action_label:
            st.markdown(f"Accion: {action_label}")

        col_apply, col_cancel = st.columns(2, gap="small")
        if col_apply.button(
            "Aplicar",
            type="primary",
            key="rs_confirm_apply_btn",
            use_container_width=True,
        ):
            st.session_state["rs_confirm_approved_action"] = dict(pending_confirmation)
            st.session_state.pop("rs_pending_confirmation", None)
            st.rerun()
        if col_cancel.button(
            "Cancelar",
            key="rs_confirm_cancel_btn",
            use_container_width=True,
        ):
            st.session_state.pop("rs_pending_confirmation", None)
            st.session_state["rs_confirm_notice"] = "Operacion cancelada."
            st.rerun()

    rs_notice = str(st.session_state.pop("rs_confirm_notice", "") or "").strip()
    if rs_notice:
        st.info(rs_notice)

    tab_rs_clases, tab_rs_usuarios, tab_rs_docentes, tab_rs_excel = st.tabs(
        ["Clases RS", "Usuarios RS", "Asignar clases a docentes", "Listar alumnos registrados"]
    )
    with tab_rs_clases:
        st.markdown("**RS | Listado y creacion masiva de clases**")
        st.caption(
            "Lista clases de Richmond Studio, filtralas y crea varias filas en una sola grilla."
        )
        with st.container(border=True):
            if "rs_groups_create_rows" not in st.session_state:
                st.session_state["rs_groups_create_rows"] = [
                    _default_richmondstudio_group_row()
                ]
            run_rs_groups_load = st.button(
                "Cargar clases RS",
                key="rs_rs_groups_load_btn",
                use_container_width=True,
            )

            if run_rs_groups_load:
                if not rs_token:
                    st.error("Ingresa el bearer token de Richmond Studio.")
                    st.stop()
                try:
                    with st.spinner("Consultando clases RS..."):
                        rs_groups_loaded = _fetch_richmondstudio_groups(
                            rs_token,
                            timeout=int(timeout),
                            include_users=True,
                        )
                except Exception as exc:  # pragma: no cover - UI
                    st.error(f"Error RS: {exc}")
                    st.stop()

                rs_group_rows = [
                    _normalize_richmondstudio_group_row(item)
                    for item in rs_groups_loaded
                    if isinstance(item, dict)
                ]
                st.session_state["rs_groups_loaded_rows"] = _normalize_richmondstudio_loaded_rows(
                    sorted(
                        rs_group_rows,
                        key=lambda row: (
                            str(row.get("Class name") or "").upper(),
                            str(row.get("Code") or "").upper(),
                        ),
                    )
                )
                st.success(f"Clases RS cargadas: {len(rs_group_rows)}.")

            rs_loaded_rows = _normalize_richmondstudio_loaded_rows(
                st.session_state.get("rs_groups_loaded_rows") or []
            )
            st.session_state["rs_groups_loaded_rows"] = rs_loaded_rows
            if rs_loaded_rows:
                st.markdown("**Listado RS**")
                col_rs_filter_a, col_rs_filter_b, col_rs_filter_c = st.columns(
                    [2.4, 1.2, 1],
                    gap="small",
                )
                rs_filter_text = col_rs_filter_a.text_input(
                    "Filtrar por Class name o Code",
                    key="rs_rs_groups_filter_text",
                    placeholder="Ej: 2026 Ingles 2SA",
                )
                rs_filter_level = col_rs_filter_b.selectbox(
                    "Test level",
                    options=["Todos"] + RICHMONDSTUDIO_TEST_LEVEL_LABELS,
                    key="rs_rs_groups_filter_level",
                )
                rs_filter_iread = col_rs_filter_c.selectbox(
                    "iRead",
                    options=["Todos", "Si", "No"],
                    key="rs_rs_groups_filter_iread",
                )

                rs_filter_text_norm = str(rs_filter_text or "").strip().lower()
                rs_filtered_rows = []
                rs_filtered_edit_rows = []
                for row in rs_loaded_rows:
                    class_name_txt = str(row.get("Class name") or "")
                    code_txt = str(row.get("Code") or "")
                    level_txt = str(row.get("Test level") or "")
                    iread_txt = _richmondstudio_display_bool(row.get("iRead"))
                    hay_texto = not rs_filter_text_norm or (
                        rs_filter_text_norm in class_name_txt.lower()
                        or rs_filter_text_norm in code_txt.lower()
                    )
                    hay_level = rs_filter_level == "Todos" or level_txt == rs_filter_level
                    hay_iread = rs_filter_iread == "Todos" or iread_txt == rs_filter_iread
                    if hay_texto and hay_level and hay_iread:
                        rs_filtered_edit_rows.append(dict(row))
                        rs_filtered_rows.append(
                            {
                                "Class name": class_name_txt,
                                "Grade": str(row.get("Grade") or ""),
                                "Dates": str(row.get("Dates") or ""),
                                "iRead": iread_txt,
                                "Code": code_txt,
                                "Students": int(row.get("Students") or 0),
                            }
                        )
                st.caption(
                    f"Mostrando {len(rs_filtered_rows)} de {len(rs_loaded_rows)} clases RS."
                )
                _show_dataframe(rs_filtered_rows, use_container_width=True)
                st.markdown("**RS | Editar o eliminar clases cargadas**")
                rs_edit_columns = [
                    "Seleccionar",
                    "ID",
                    "Class name",
                    "Description",
                    "Grade",
                    "Test level",
                    "Start date",
                    "End date",
                    "iRead",
                    "Code",
                    "Students",
                ]
                rs_edit_df = _richmondstudio_loaded_editor_df(
                    rs_filtered_edit_rows,
                    rs_edit_columns,
                )
                edited_rs_loaded_df = st.data_editor(
                    rs_edit_df,
                    key="rs_rs_groups_loaded_editor",
                    hide_index=True,
                    use_container_width=True,
                    disabled=["ID", "Code", "Students"],
                    column_config={
                        "Seleccionar": st.column_config.CheckboxColumn("Seleccionar"),
                        "ID": st.column_config.TextColumn("ID"),
                        "Class name": st.column_config.TextColumn(
                            "Class name",
                            required=True,
                            width="large",
                        ),
                        "Description": st.column_config.TextColumn(
                            "Description",
                            width="large",
                        ),
                        "Grade": st.column_config.SelectboxColumn(
                            "Grade",
                            options=RICHMONDSTUDIO_GRADE_LABELS,
                            required=True,
                        ),
                        "Test level": st.column_config.SelectboxColumn(
                            "Test level",
                            options=[""] + RICHMONDSTUDIO_TEST_LEVEL_LABELS,
                            required=False,
                        ),
                        "Start date": st.column_config.DateColumn(
                            "Start date",
                            format="YYYY-MM-DD",
                            required=True,
                        ),
                        "End date": st.column_config.DateColumn(
                            "End date",
                            format="YYYY-MM-DD",
                            required=True,
                        ),
                        "iRead": st.column_config.CheckboxColumn("iRead"),
                        "Code": st.column_config.TextColumn("Code"),
                        "Students": st.column_config.NumberColumn("Students", format="%d"),
                    },
                )
                if isinstance(edited_rs_loaded_df, pd.DataFrame):
                    edited_lookup = {
                        str(item.get("ID") or "").strip(): item
                        for item in edited_rs_loaded_df.to_dict("records")
                        if str(item.get("ID") or "").strip()
                    }
                    merged_rows: List[Dict[str, object]] = []
                    for row in rs_loaded_rows:
                        row_id = str(row.get("ID") or "").strip()
                        if row_id and row_id in edited_lookup:
                            merged_row = dict(row)
                            merged_row.update(edited_lookup[row_id])
                            merged_rows.append(merged_row)
                        else:
                            merged_rows.append(dict(row))
                    rs_loaded_rows = _normalize_richmondstudio_loaded_rows(merged_rows)
                    st.session_state["rs_groups_loaded_rows"] = rs_loaded_rows

                col_rs_update, col_rs_delete = st.columns([1, 1], gap="small")
                run_rs_groups_update = col_rs_update.button(
                    "Actualizar clases RS",
                    key="rs_rs_groups_update_btn",
                    use_container_width=True,
                )
                run_rs_groups_delete = col_rs_delete.button(
                    "Eliminar clases RS",
                    key="rs_rs_groups_delete_btn",
                    use_container_width=True,
                )
                run_rs_groups_update_confirmed = _consume_richmondstudio_confirmed_action(
                    "rs_groups_update"
                )
                run_rs_groups_delete_confirmed = _consume_richmondstudio_confirmed_action(
                    "rs_groups_delete"
                )
                confirm_rs_delete = st.checkbox(
                    "Confirmar eliminacion de clases RS seleccionadas",
                    key="rs_rs_groups_delete_confirm",
                    value=False,
                )
                if run_rs_groups_update:
                    rows_to_update = [
                        row for row in rs_loaded_rows if bool(row.get("Seleccionar"))
                    ]
                    if not rows_to_update:
                        st.error("Selecciona al menos una clase RS para actualizar.")
                    else:
                        _request_richmondstudio_confirmation(
                            "rs_groups_update",
                            f"actualizar {len(rows_to_update)} clases RS",
                        )

                if run_rs_groups_update_confirmed:
                    rows_to_update = [
                        row for row in rs_loaded_rows if bool(row.get("Seleccionar"))
                    ]
                    if not rs_token:
                        st.error("Ingresa el bearer token de Richmond Studio.")
                    elif not rows_to_update:
                        st.error("Selecciona al menos una clase RS para actualizar.")
                    else:
                        resultados_rs_update: List[Dict[str, str]] = []
                        ok_rs_update = 0
                        err_rs_update = 0
                        progress_rs_update = st.progress(0)
                        status_rs_update = st.empty()

                        for idx_rs, row in enumerate(rows_to_update, start=1):
                            class_name = str(row.get("Class name") or "").strip()
                            group_id = str(row.get("ID") or "").strip()
                            try:
                                payload_rs = _build_richmondstudio_group_update_payload(row)
                                status_rs_update.write(
                                    f"Actualizando {idx_rs}/{len(rows_to_update)}: {class_name}"
                                )
                                _update_richmondstudio_group(
                                    rs_token,
                                    group_id,
                                    payload_rs,
                                    timeout=int(timeout),
                                )
                                resultados_rs_update.append(
                                    {
                                        "Class name": class_name,
                                        "Resultado": "OK",
                                        "ID": group_id,
                                        "Detalle": "Actualizada correctamente.",
                                    }
                                )
                                ok_rs_update += 1
                            except Exception as exc:  # pragma: no cover - UI
                                resultados_rs_update.append(
                                    {
                                        "Class name": class_name,
                                        "Resultado": "Error",
                                        "ID": group_id,
                                        "Detalle": str(exc),
                                    }
                                )
                                err_rs_update += 1
                            progress_rs_update.progress(
                                int((idx_rs / len(rows_to_update)) * 100)
                            )

                        status_rs_update.empty()
                        progress_rs_update.empty()
                        if ok_rs_update:
                            try:
                                rs_groups_loaded = _fetch_richmondstudio_groups(
                                    rs_token,
                                    timeout=int(timeout),
                                    include_users=True,
                                )
                                rs_group_rows = [
                                    _normalize_richmondstudio_group_row(item)
                                    for item in rs_groups_loaded
                                    if isinstance(item, dict)
                                ]
                                st.session_state["rs_groups_loaded_rows"] = _normalize_richmondstudio_loaded_rows(
                                    sorted(
                                        rs_group_rows,
                                        key=lambda row: (
                                            str(row.get("Class name") or "").upper(),
                                            str(row.get("Code") or "").upper(),
                                        ),
                                    )
                                )
                            except Exception:
                                pass

                        if ok_rs_update and not err_rs_update:
                            st.success(f"Clases RS actualizadas correctamente: {ok_rs_update}.")
                        elif ok_rs_update and err_rs_update:
                            st.warning(
                                f"Resultado parcial RS: OK {ok_rs_update} | Error {err_rs_update}."
                            )
                        else:
                            st.error("No se pudo actualizar ninguna clase RS.")
                        _show_dataframe(resultados_rs_update, use_container_width=True)

                if run_rs_groups_delete:
                    if not confirm_rs_delete:
                        st.error("Marca la confirmacion para eliminar clases RS.")
                    else:
                        rows_to_delete = [
                            row for row in rs_loaded_rows if bool(row.get("Seleccionar"))
                        ]
                        if not rows_to_delete:
                            st.error("Selecciona al menos una clase RS para eliminar.")
                        else:
                            _request_richmondstudio_confirmation(
                                "rs_groups_delete",
                                f"eliminar {len(rows_to_delete)} clases RS",
                            )

                if run_rs_groups_delete_confirmed:
                    rows_to_delete = [
                        row for row in rs_loaded_rows if bool(row.get("Seleccionar"))
                    ]
                    if not rs_token:
                        st.error("Ingresa el bearer token de Richmond Studio.")
                    elif not confirm_rs_delete:
                        st.error("Marca la confirmacion para eliminar clases RS.")
                    elif not rows_to_delete:
                        st.error("Selecciona al menos una clase RS para eliminar.")
                    else:
                        resultados_rs_delete: List[Dict[str, str]] = []
                        ok_rs_delete = 0
                        err_rs_delete = 0
                        progress_rs_delete = st.progress(0)
                        status_rs_delete = st.empty()

                        for idx_rs, row in enumerate(rows_to_delete, start=1):
                            class_name = str(row.get("Class name") or "").strip()
                            group_id = str(row.get("ID") or "").strip()
                            try:
                                status_rs_delete.write(
                                    f"Eliminando {idx_rs}/{len(rows_to_delete)}: {class_name}"
                                )
                                _delete_richmondstudio_group(
                                    rs_token,
                                    group_id,
                                    timeout=int(timeout),
                                )
                                resultados_rs_delete.append(
                                    {
                                        "Class name": class_name,
                                        "Resultado": "OK",
                                        "ID": group_id,
                                        "Detalle": "Eliminada correctamente.",
                                    }
                                )
                                ok_rs_delete += 1
                            except Exception as exc:  # pragma: no cover - UI
                                resultados_rs_delete.append(
                                    {
                                        "Class name": class_name,
                                        "Resultado": "Error",
                                        "ID": group_id,
                                        "Detalle": str(exc),
                                    }
                                )
                                err_rs_delete += 1
                            progress_rs_delete.progress(
                                int((idx_rs / len(rows_to_delete)) * 100)
                            )

                        status_rs_delete.empty()
                        progress_rs_delete.empty()
                        if ok_rs_delete:
                            try:
                                rs_groups_loaded = _fetch_richmondstudio_groups(
                                    rs_token,
                                    timeout=int(timeout),
                                    include_users=True,
                                )
                                rs_group_rows = [
                                    _normalize_richmondstudio_group_row(item)
                                    for item in rs_groups_loaded
                                    if isinstance(item, dict)
                                ]
                                st.session_state["rs_groups_loaded_rows"] = _normalize_richmondstudio_loaded_rows(
                                    sorted(
                                        rs_group_rows,
                                        key=lambda row: (
                                            str(row.get("Class name") or "").upper(),
                                            str(row.get("Code") or "").upper(),
                                        ),
                                    )
                                )
                            except Exception:
                                st.session_state["rs_groups_loaded_rows"] = [
                                    row
                                    for row in rs_loaded_rows
                                    if not bool(row.get("Seleccionar"))
                                ]

                        if ok_rs_delete and not err_rs_delete:
                            st.success(f"Clases RS eliminadas correctamente: {ok_rs_delete}.")
                        elif ok_rs_delete and err_rs_delete:
                            st.warning(
                                f"Resultado parcial RS: OK {ok_rs_delete} | Error {err_rs_delete}."
                            )
                        else:
                            st.error("No se pudo eliminar ninguna clase RS.")
                        _show_dataframe(resultados_rs_delete, use_container_width=True)
            else:
                st.caption("Aun no has cargado clases RS.")

        with st.container(border=True):
            st.markdown("**RS | Crear clases en bloque**")
            st.caption(
                "Llena una clase por fila. Description se completa con Class name si lo dejas vacio. Al crear: inicio = hoy, fin = 31/12 del ano actual y Test level vacio no se manda."
            )
            rs_create_rows = _render_richmondstudio_create_rows_form(
                state_key="rs_groups_create_rows",
                widget_prefix="rs_rs_groups_create_form",
            )

            run_rs_groups_create = st.button(
                "Crear clases RS",
                type="primary",
                key="rs_rs_groups_create_btn",
                use_container_width=True,
            )
            run_rs_groups_create_confirmed = _consume_richmondstudio_confirmed_action(
                "rs_groups_create"
            )
            if run_rs_groups_create:
                rows_to_create = _normalize_richmondstudio_create_rows(
                    st.session_state.get("rs_groups_create_rows") or []
                )
                selected_rows = [
                    row
                    for row in rows_to_create
                    if bool(row.get("Crear")) and str(row.get("Class name") or "").strip()
                ]
                if not selected_rows:
                    st.error("No hay filas marcadas con Class name para crear.")
                else:
                    _request_richmondstudio_confirmation(
                        "rs_groups_create",
                        f"crear {len(selected_rows)} clases RS",
                    )
            if run_rs_groups_create_confirmed:
                rows_to_create = _normalize_richmondstudio_create_rows(
                    st.session_state.get("rs_groups_create_rows") or []
                )
                selected_rows = [
                    row
                    for row in rows_to_create
                    if bool(row.get("Crear")) and str(row.get("Class name") or "").strip()
                ]
                if not rs_token:
                    st.error("Ingresa el bearer token de Richmond Studio.")
                elif not selected_rows:
                    st.error("No hay filas marcadas con Class name para crear.")
                else:
                    resultados_rs: List[Dict[str, object]] = []
                    ok_rs = 0
                    err_rs = 0
                    progress_rs = st.progress(0)
                    status_rs = st.empty()

                    for idx_rs, row in enumerate(selected_rows, start=1):
                        class_name = str(row.get("Class name") or "").strip()
                        try:
                            payload_rs = _build_richmondstudio_group_payload(row)
                            status_rs.write(
                                f"Creando {idx_rs}/{len(selected_rows)}: {class_name}"
                            )
                            created_rs = _create_richmondstudio_group(
                                rs_token,
                                payload_rs,
                                timeout=int(timeout),
                            )
                            created_data = (
                                created_rs.get("data")
                                if isinstance(created_rs.get("data"), dict)
                                else {}
                            )
                            created_attrs = (
                                created_data.get("attributes")
                                if isinstance(created_data.get("attributes"), dict)
                                else {}
                            )
                            resultados_rs.append(
                                {
                                    "Class name": class_name,
                                    "Resultado": "OK",
                                    "ID": str(created_data.get("id") or "").strip(),
                                    "Code": str(created_attrs.get("code") or "").strip(),
                                    "Detalle": "Creada correctamente.",
                                }
                            )
                            ok_rs += 1
                        except Exception as exc:  # pragma: no cover - UI
                            resultados_rs.append(
                                {
                                    "Class name": class_name,
                                    "Resultado": "Error",
                                    "ID": "",
                                    "Code": "",
                                    "Detalle": str(exc),
                                }
                            )
                            err_rs += 1
                        progress_rs.progress(int((idx_rs / len(selected_rows)) * 100))

                    status_rs.empty()
                    progress_rs.empty()
                    if ok_rs:
                        try:
                            rs_groups_loaded = _fetch_richmondstudio_groups(
                                rs_token,
                                timeout=int(timeout),
                                include_users=True,
                            )
                            rs_group_rows = [
                                _normalize_richmondstudio_group_row(item)
                                for item in rs_groups_loaded
                                if isinstance(item, dict)
                            ]
                            st.session_state["rs_groups_loaded_rows"] = _normalize_richmondstudio_loaded_rows(
                                sorted(
                                    rs_group_rows,
                                    key=lambda row: (
                                        str(row.get("Class name") or "").upper(),
                                        str(row.get("Code") or "").upper(),
                                    ),
                                )
                            )
                        except Exception:
                            pass

                    if ok_rs and not err_rs:
                        st.success(f"Clases RS creadas correctamente: {ok_rs}.")
                    elif ok_rs and err_rs:
                        st.warning(f"Resultado parcial RS: OK {ok_rs} | Error {err_rs}.")
                    else:
                        st.error("No se pudo crear ninguna clase RS.")
                    _show_dataframe(resultados_rs, use_container_width=True)
    with tab_rs_usuarios:
        with st.container(border=True):
            st.markdown("**RS | Crear usuarios desde Excel**")
            uploaded_rs_users_bytes = b""
            uploaded_rs_users_excel = None
            rs_user_import_rows: List[Dict[str, object]] = []
            rs_user_import_error = ""
            run_rs_users_create = False

            if "rs_users_import_excel_version" not in st.session_state:
                st.session_state["rs_users_import_excel_version"] = 0
            if not st.session_state.get("rs_users_create_output_bytes"):
                st.session_state["rs_users_create_download_only"] = False

            rs_users_download_only = bool(
                st.session_state.get("rs_users_create_download_only")
            ) and bool(st.session_state.get("rs_users_create_output_bytes"))

            if not rs_users_download_only:
                st.caption(
                    "Sube un Excel con columnas Last name, First name, Class, Email, Role y level. "
                    "La clase se resuelve contra /api/groups y cada usuario se crea con una "
                    "peticion individual a /api/users. level acepta: preschool, primary, secondary, adult."
                )
                template_rs_users_bytes = _export_simple_excel(
                    _richmondstudio_user_import_template_rows(),
                    sheet_name="rs_users_import",
                )
                col_rs_users_template, col_rs_users_file = st.columns([1, 2], gap="small")
                col_rs_users_template.download_button(
                    label="Descargar plantilla",
                    data=template_rs_users_bytes,
                    file_name="plantilla_usuarios_rs.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="rs_users_import_template_download",
                    use_container_width=True,
                )
                uploaded_rs_users_excel = col_rs_users_file.file_uploader(
                    "Excel de usuarios RS",
                    type=["xlsx"],
                    key=(
                        "rs_users_import_excel_"
                        f"{int(st.session_state.get('rs_users_import_excel_version', 0))}"
                    ),
                    help=(
                        "Columnas esperadas: "
                        f"{RICHMONDSTUDIO_USER_IMPORT_LAST_NAME}, "
                        f"{RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME}, "
                        f"{RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME}, "
                        f"{RICHMONDSTUDIO_USER_IMPORT_EMAIL}, "
                        f"{RICHMONDSTUDIO_USER_IMPORT_ROLE}, "
                        f"{RICHMONDSTUDIO_USER_IMPORT_LEVEL}. "
                        "Valores validos de level: preschool, primary, secondary, adult."
                    ),
                )

                if uploaded_rs_users_excel is not None:
                    uploaded_rs_users_bytes = uploaded_rs_users_excel.getvalue()
                    try:
                        rs_user_import_rows = _load_richmondstudio_user_rows_from_excel(
                            uploaded_rs_users_bytes
                        )
                    except Exception as exc:
                        rs_user_import_error = str(exc)
                        st.error(f"Error en Excel RS: {exc}")
                    else:
                        st.caption(f"Filas detectadas para crear: {len(rs_user_import_rows)}")
                        preview_rows = [
                            {
                                RICHMONDSTUDIO_USER_IMPORT_LAST_NAME: row.get(
                                    RICHMONDSTUDIO_USER_IMPORT_LAST_NAME
                                ),
                                RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME: row.get(
                                    RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME
                                ),
                                RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME: row.get(
                                    RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME
                                ),
                                RICHMONDSTUDIO_USER_IMPORT_EMAIL: row.get(
                                    RICHMONDSTUDIO_USER_IMPORT_EMAIL
                                ),
                                RICHMONDSTUDIO_USER_IMPORT_ROLE: row.get(
                                    RICHMONDSTUDIO_USER_IMPORT_ROLE
                                ),
                                RICHMONDSTUDIO_USER_IMPORT_LEVEL: row.get(
                                    RICHMONDSTUDIO_USER_IMPORT_LEVEL
                                ),
                            }
                            for row in rs_user_import_rows[:100]
                        ]
                        if preview_rows:
                            _show_dataframe(preview_rows, use_container_width=True)

                run_rs_users_create = st.button(
                    "Crear usuarios RS",
                    type="primary",
                    key="rs_users_create_btn",
                    use_container_width=True,
                )
            run_rs_users_create_confirmed = _consume_richmondstudio_confirmed_action(
                "rs_users_create"
            )

            if run_rs_users_create:
                if not rs_token:
                    st.error("Ingresa el bearer token de Richmond Studio.")
                elif uploaded_rs_users_excel is None:
                    st.error("Sube el Excel de usuarios RS.")
                elif rs_user_import_error:
                    st.error(f"Corrige el Excel antes de continuar: {rs_user_import_error}")
                else:
                    if not rs_user_import_rows and uploaded_rs_users_bytes:
                        try:
                            rs_user_import_rows = _load_richmondstudio_user_rows_from_excel(
                                uploaded_rs_users_bytes
                            )
                        except Exception as exc:
                            st.error(f"Error en Excel RS: {exc}")
                            rs_user_import_rows = []

                    if rs_user_import_rows:
                        _request_richmondstudio_confirmation(
                            "rs_users_create",
                            f"crear {len(rs_user_import_rows)} usuarios RS",
                        )

            if run_rs_users_create_confirmed:
                if not rs_token:
                    st.error("Ingresa el bearer token de Richmond Studio.")
                elif uploaded_rs_users_excel is None:
                    st.error("Sube el Excel de usuarios RS.")
                elif rs_user_import_error:
                    st.error(f"Corrige el Excel antes de continuar: {rs_user_import_error}")
                else:
                    if not rs_user_import_rows and uploaded_rs_users_bytes:
                        try:
                            rs_user_import_rows = _load_richmondstudio_user_rows_from_excel(
                                uploaded_rs_users_bytes
                            )
                        except Exception as exc:
                            st.error(f"Error en Excel RS: {exc}")
                            rs_user_import_rows = []

                    if rs_user_import_rows:
                        institution_name_for_file = ""
                        try:
                            current_context_users = _fetch_richmondstudio_current_user_context(
                                rs_token,
                                timeout=int(timeout),
                            )
                        except Exception:
                            current_context_users = {}
                        institution_name_for_file = str(
                            current_context_users.get("institution_name") or ""
                        ).strip()

                        try:
                            with st.spinner("Consultando clases RS..."):
                                rs_groups_for_users = _fetch_richmondstudio_groups(
                                    rs_token,
                                    timeout=int(timeout),
                                    include_users=False,
                                )
                        except Exception as exc:  # pragma: no cover - UI
                            st.error(f"Error RS: {exc}")
                        else:
                            groups_lookup = _build_richmondstudio_groups_lookup(
                                rs_groups_for_users
                            )
                            resultados_rs_users: List[Dict[str, object]] = []
                            ok_rs_users = 0
                            err_rs_users = 0
                            progress_rs_users = st.progress(0)
                            status_rs_users = st.empty()

                            total_rows = len(rs_user_import_rows)
                            for idx_rs_user, row in enumerate(rs_user_import_rows, start=1):
                                first_name = str(
                                    row.get(RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME) or ""
                                ).strip()
                                last_name = str(
                                    row.get(RICHMONDSTUDIO_USER_IMPORT_LAST_NAME) or ""
                                ).strip()
                                class_name_requested = str(
                                    row.get(RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME) or ""
                                ).strip()
                                email = str(
                                    row.get(RICHMONDSTUDIO_USER_IMPORT_EMAIL) or ""
                                ).strip()
                                role_raw = str(
                                    row.get(RICHMONDSTUDIO_USER_IMPORT_ROLE) or ""
                                ).strip()
                                level_raw = str(
                                    row.get(RICHMONDSTUDIO_USER_IMPORT_LEVEL) or ""
                                ).strip()
                                row_number = int(row.get("_row_number") or idx_rs_user + 1)
                                display_name = " ".join(
                                    part for part in (first_name, last_name) if part
                                ).strip() or email or f"fila {row_number}"

                                try:
                                    status_rs_users.write(
                                        f"Creando {idx_rs_user}/{total_rows}: {display_name}"
                                    )
                                    group_meta = _resolve_richmondstudio_group_for_user_row(
                                        class_name_requested,
                                        groups_lookup,
                                    )
                                    payload_rs_user = _build_richmondstudio_user_payload(
                                        row,
                                        group_meta=group_meta,
                                    )
                                    created_rs_user = _create_richmondstudio_user(
                                        rs_token,
                                        payload_rs_user,
                                        timeout=int(timeout),
                                    )
                                    created_meta = _extract_richmondstudio_user_create_result(
                                        created_rs_user,
                                        fallback_email=email,
                                    )
                                    password_txt = str(
                                        created_meta.get("password") or ""
                                    ).strip()
                                    detalle_txt = "Creado correctamente."
                                    if not password_txt:
                                        detalle_txt = (
                                            "Creado, pero RS no devolvio password."
                                        )
                                    resultados_rs_users.append(
                                        {
                                            "Row": row_number,
                                            "Last name": last_name,
                                            "First name": first_name,
                                            "Class": str(
                                                (
                                                    group_meta.get("class_name")
                                                    if isinstance(group_meta, dict)
                                                    else class_name_requested
                                                )
                                                or ""
                                            ).strip(),
                                            "Email": email,
                                            "Role": _normalize_richmondstudio_user_role(
                                                role_raw
                                            ),
                                            "level": _normalize_richmondstudio_user_level(
                                                level_raw
                                            ),
                                            "Login": str(
                                                created_meta.get("login") or email
                                            ).strip(),
                                            "Password": password_txt,
                                            "RS User ID": str(
                                                created_meta.get("user_id") or ""
                                            ).strip(),
                                            "Resultado": "OK",
                                            "Detalle": detalle_txt,
                                        }
                                    )
                                    ok_rs_users += 1
                                except Exception as exc:  # pragma: no cover - UI
                                    resultados_rs_users.append(
                                        {
                                            "Row": row_number,
                                            "Last name": last_name,
                                            "First name": first_name,
                                            "Class": class_name_requested,
                                            "Email": email,
                                            "Role": role_raw,
                                            "level": level_raw,
                                            "Login": email,
                                            "Password": "",
                                            "RS User ID": "",
                                            "Resultado": "Error",
                                            "Detalle": str(exc),
                                        }
                                    )
                                    err_rs_users += 1

                                progress_rs_users.progress(
                                    int((idx_rs_user / total_rows) * 100)
                                )

                            status_rs_users.empty()
                            progress_rs_users.empty()
                            st.session_state["rs_users_create_output_rows"] = (
                                resultados_rs_users
                            )
                            st.session_state["rs_users_create_output_bytes"] = (
                                _export_simple_excel(
                                    _build_richmondstudio_users_export_rows(
                                        resultados_rs_users
                                    ),
                                    sheet_name="usuarios_rs",
                                )
                            )
                            st.session_state["rs_users_create_output_count"] = int(
                                len(resultados_rs_users)
                            )
                            st.session_state["rs_users_create_output_filename"] = (
                                _build_richmondstudio_users_output_filename(
                                    institution_name_for_file
                                )
                            )

                            if ok_rs_users and not err_rs_users:
                                st.session_state["rs_users_create_download_only"] = True
                                st.session_state["rs_users_import_excel_version"] = int(
                                    st.session_state.get("rs_users_import_excel_version", 0)
                                ) + 1
                                st.rerun()
                            elif ok_rs_users and err_rs_users:
                                st.session_state["rs_users_create_download_only"] = False
                                st.warning(
                                    "Resultado parcial RS: "
                                    f"OK {ok_rs_users} | Error {err_rs_users}."
                                )
                            else:
                                st.session_state["rs_users_create_download_only"] = False
                                st.error("No se pudo crear ningun usuario RS.")

                            if resultados_rs_users:
                                _show_dataframe(
                                    resultados_rs_users[:200],
                                    use_container_width=True,
                                )

            if st.session_state.get("rs_users_create_output_bytes"):
                st.download_button(
                    label="Descargar resultado usuarios RS",
                    data=st.session_state["rs_users_create_output_bytes"],
                    file_name=str(
                        st.session_state.get("rs_users_create_output_filename")
                        or "alumnos_RS.xlsx"
                    ).strip()
                    or "alumnos_RS.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="rs_users_create_output_download",
                    use_container_width=True,
                )

    with tab_rs_docentes:
        with st.container(border=True):
            st.markdown("**Asignar clases a docentes**")
            st.caption(
                "Carga docentes y clases de Richmond Studio para crear o editar docentes y sincronizar sus clases."
            )

            teacher_notice = str(
                st.session_state.pop("rs_teacher_save_notice", "") or ""
            ).strip()
            teacher_meta = st.session_state.pop("rs_teacher_save_meta", None)
            if teacher_notice:
                st.success(teacher_notice)
            if isinstance(teacher_meta, dict):
                login_txt = str(teacher_meta.get("login") or "").strip()
                password_txt = str(teacher_meta.get("password") or "").strip()
                if login_txt or password_txt:
                    parts = []
                    if login_txt:
                        parts.append(f"Login: {login_txt}")
                    if password_txt:
                        parts.append(f"Password: {password_txt}")
                    st.caption(" | ".join(parts))

            if "rs_teachers_loaded_rows" not in st.session_state:
                st.session_state["rs_teachers_loaded_rows"] = []
            if "rs_teachers_groups_lookup" not in st.session_state:
                st.session_state["rs_teachers_groups_lookup"] = {
                    "by_id": {},
                    "by_name": {},
                }

            run_rs_teachers_load = st.button(
                "Cargar docentes RS",
                key="rs_teachers_load_btn",
                use_container_width=True,
            )
            if run_rs_teachers_load:
                if not rs_token:
                    st.error("Ingresa el bearer token de Richmond Studio.")
                else:
                    try:
                        with st.spinner("Consultando docentes RS..."):
                            teacher_panel_data = _load_richmondstudio_teacher_panel_data(
                                rs_token,
                                timeout=int(timeout),
                            )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error RS: {exc}")
                    else:
                        st.session_state["rs_teachers_loaded_rows"] = (
                            teacher_panel_data.get("rows") or []
                        )
                        st.session_state["rs_teachers_groups_lookup"] = (
                            teacher_panel_data.get("groups_lookup")
                            if isinstance(
                                teacher_panel_data.get("groups_lookup"), dict
                            )
                            else {"by_id": {}, "by_name": {}}
                        )
                        st.session_state["rs_teachers_form_loaded_user_id"] = ""

            teacher_rows = [
                dict(row)
                for row in st.session_state.get("rs_teachers_loaded_rows") or []
                if isinstance(row, dict)
            ]
            groups_lookup = (
                st.session_state.get("rs_teachers_groups_lookup")
                if isinstance(st.session_state.get("rs_teachers_groups_lookup"), dict)
                else {"by_id": {}, "by_name": {}}
            )
            groups_by_id = (
                groups_lookup.get("by_id")
                if isinstance(groups_lookup.get("by_id"), dict)
                else {}
            )
            group_options = sorted(
                list(groups_by_id.keys()),
                key=lambda group_id: _richmondstudio_group_label(
                    groups_by_id.get(group_id)
                ).upper(),
            )
            valid_group_ids = set(group_options)

            teacher_rows_by_id = {
                str(row.get("ID") or "").strip(): row
                for row in teacher_rows
                if str(row.get("ID") or "").strip()
            }
            teacher_option_labels = {"__new__": "+ Nuevo docente"}
            for teacher_id, row in teacher_rows_by_id.items():
                teacher_label = str(row.get("Docente") or "").strip()
                teacher_email = str(row.get("Email") or "").strip()
                if teacher_email:
                    teacher_option_labels[teacher_id] = (
                        f"{teacher_label} | {teacher_email}"
                    ).strip(" |")
                else:
                    teacher_option_labels[teacher_id] = teacher_label or teacher_id

            if teacher_rows:
                teacher_search = st.text_input(
                    "Filtrar docentes RS",
                    key="rs_teacher_search_text",
                    placeholder="Nombre, email o clase",
                )
                teacher_search_norm = _normalize_plain_text(teacher_search)
                filtered_teacher_rows = []
                for row in teacher_rows:
                    haystack = " ".join(
                        [
                            str(row.get("Docente") or ""),
                            str(row.get("Email") or ""),
                            str(row.get("Clases") or ""),
                        ]
                    )
                    if teacher_search_norm and (
                        teacher_search_norm not in _normalize_plain_text(haystack)
                    ):
                        continue
                    filtered_teacher_rows.append(
                        {
                            "Docente": str(row.get("Docente") or "").strip(),
                            "Email": str(row.get("Email") or "").strip(),
                            "Grupos": int(row.get("Grupos") or 0),
                            "Clases": str(row.get("Clases") or "").strip(),
                        }
                    )
                st.caption(
                    f"Mostrando {len(filtered_teacher_rows)} de {len(teacher_rows)} docentes RS."
                )
                if filtered_teacher_rows:
                    _show_dataframe(filtered_teacher_rows, use_container_width=True)

            teacher_select_options = ["__new__"] + list(teacher_rows_by_id.keys())
            if (
                str(st.session_state.get("rs_teacher_selected_user_id") or "").strip()
                not in teacher_select_options
            ):
                st.session_state["rs_teacher_selected_user_id"] = "__new__"

            selected_teacher_id = st.selectbox(
                "Docente RS",
                options=teacher_select_options,
                key="rs_teacher_selected_user_id",
                format_func=lambda value: teacher_option_labels.get(
                    str(value or "").strip(),
                    str(value or "").strip(),
                ),
            )

            current_loaded_teacher_id = str(
                st.session_state.get("rs_teachers_form_loaded_user_id") or ""
            ).strip()
            selected_teacher_id = str(selected_teacher_id or "").strip()
            if current_loaded_teacher_id != selected_teacher_id:
                selected_teacher_row = (
                    teacher_rows_by_id.get(selected_teacher_id)
                    if selected_teacher_id != "__new__"
                    else None
                )
                current_group_ids = []
                if isinstance(selected_teacher_row, dict):
                    for item in selected_teacher_row.get("_group_ids") or []:
                        group_id = str(item or "").strip()
                        if group_id and group_id in valid_group_ids:
                            current_group_ids.append(group_id)
                st.session_state["rs_teacher_first_name"] = str(
                    (
                        selected_teacher_row.get("First name")
                        if isinstance(selected_teacher_row, dict)
                        else ""
                    )
                    or ""
                ).strip()
                st.session_state["rs_teacher_last_name"] = str(
                    (
                        selected_teacher_row.get("Last name")
                        if isinstance(selected_teacher_row, dict)
                        else ""
                    )
                    or ""
                ).strip()
                st.session_state["rs_teacher_email"] = str(
                    (
                        selected_teacher_row.get("Email")
                        if isinstance(selected_teacher_row, dict)
                        else ""
                    )
                    or ""
                ).strip()
                st.session_state["rs_teacher_group_ids"] = current_group_ids
                st.session_state["rs_teacher_hidden_teachermatic"] = bool(
                    (
                        selected_teacher_row.get("Teachermatic")
                        if isinstance(selected_teacher_row, dict)
                        else False
                    )
                )
                st.session_state["rs_teachers_form_loaded_user_id"] = selected_teacher_id

            is_existing_teacher = selected_teacher_id != "__new__"
            col_rs_teacher_a, col_rs_teacher_b = st.columns(2, gap="small")
            col_rs_teacher_a.text_input(
                "First name",
                key="rs_teacher_first_name",
            )
            col_rs_teacher_b.text_input(
                "Last name",
                key="rs_teacher_last_name",
            )
            st.text_input(
                "Email",
                key="rs_teacher_email",
            )
            st.multiselect(
                "Clases RS",
                options=group_options,
                key="rs_teacher_group_ids",
                format_func=lambda group_id: _richmondstudio_group_label(
                    groups_by_id.get(str(group_id or "").strip())
                )
                or str(group_id or "").strip(),
                placeholder="Selecciona una o varias clases",
            )

            run_rs_teacher_save = st.button(
                "Actualizar docente RS" if is_existing_teacher else "Crear docente RS",
                type="primary",
                key="rs_teacher_save_btn",
                use_container_width=True,
            )
            run_rs_teacher_create_confirmed = _consume_richmondstudio_confirmed_action(
                "rs_teacher_create"
            )
            run_rs_teacher_update_confirmed = _consume_richmondstudio_confirmed_action(
                "rs_teacher_update"
            )

            if run_rs_teacher_save:
                teacher_first_name = str(
                    st.session_state.get("rs_teacher_first_name") or ""
                ).strip()
                teacher_last_name = str(
                    st.session_state.get("rs_teacher_last_name") or ""
                ).strip()
                teacher_email = str(
                    st.session_state.get("rs_teacher_email") or ""
                ).strip()
                teacher_group_ids = list(
                    st.session_state.get("rs_teacher_group_ids") or []
                )
                teacher_teachermatic = bool(
                    st.session_state.get("rs_teacher_hidden_teachermatic")
                )
                teacher_action_key = (
                    "rs_teacher_update" if is_existing_teacher else "rs_teacher_create"
                )
                teacher_action_label = (
                    f"actualizar docente RS: {teacher_first_name} {teacher_last_name}".strip()
                    if is_existing_teacher
                    else f"crear docente RS: {teacher_first_name} {teacher_last_name}".strip()
                )

                try:
                    _build_richmondstudio_teacher_payload(
                        first_name=teacher_first_name,
                        last_name=teacher_last_name,
                        email=teacher_email,
                        group_ids=teacher_group_ids,
                        user_id=selected_teacher_id if is_existing_teacher else "",
                        teachermatic=teacher_teachermatic if is_existing_teacher else None,
                    )
                except Exception as exc:
                    st.error(str(exc))
                else:
                    st.session_state["rs_teacher_pending_save"] = {
                        "mode": "update" if is_existing_teacher else "create",
                        "user_id": selected_teacher_id if is_existing_teacher else "",
                        "first_name": teacher_first_name,
                        "last_name": teacher_last_name,
                        "email": teacher_email,
                        "group_ids": teacher_group_ids,
                        "teachermatic": teacher_teachermatic,
                    }
                    _request_richmondstudio_confirmation(
                        teacher_action_key,
                        teacher_action_label,
                    )

            confirmed_teacher_mode = ""
            if run_rs_teacher_create_confirmed:
                confirmed_teacher_mode = "create"
            elif run_rs_teacher_update_confirmed:
                confirmed_teacher_mode = "update"

            if confirmed_teacher_mode:
                teacher_pending_save = st.session_state.pop(
                    "rs_teacher_pending_save",
                    None,
                )
                if not isinstance(teacher_pending_save, dict):
                    st.error("No se encontro el borrador del docente RS.")
                elif str(teacher_pending_save.get("mode") or "").strip() != confirmed_teacher_mode:
                    st.error("La confirmacion no coincide con la accion del docente RS.")
                elif not rs_token:
                    st.error("Ingresa el bearer token de Richmond Studio.")
                else:
                    try:
                        with st.spinner("Guardando docente RS..."):
                            if confirmed_teacher_mode == "update":
                                teacher_detail = _fetch_richmondstudio_user_detail(
                                    rs_token,
                                    str(teacher_pending_save.get("user_id") or "").strip(),
                                    timeout=int(timeout),
                                )
                                teacher_detail_data = (
                                    teacher_detail.get("data")
                                    if isinstance(teacher_detail.get("data"), dict)
                                    else {}
                                )
                                subscription_ids = _richmondstudio_relationship_ids(
                                    teacher_detail_data,
                                    "subscriptions",
                                )
                                teacher_payload = _build_richmondstudio_teacher_payload(
                                    first_name=teacher_pending_save.get("first_name"),
                                    last_name=teacher_pending_save.get("last_name"),
                                    email=teacher_pending_save.get("email"),
                                    group_ids=teacher_pending_save.get("group_ids") or [],
                                    user_id=teacher_pending_save.get("user_id"),
                                    teachermatic=bool(
                                        teacher_pending_save.get("teachermatic")
                                    ),
                                    subscription_ids=subscription_ids,
                                )
                                _update_richmondstudio_user(
                                    rs_token,
                                    str(teacher_pending_save.get("user_id") or "").strip(),
                                    teacher_payload,
                                    timeout=int(timeout),
                                )
                                teacher_result_meta = {
                                    "user_id": str(
                                        teacher_pending_save.get("user_id") or ""
                                    ).strip(),
                                }
                            else:
                                teacher_payload = _build_richmondstudio_teacher_payload(
                                    first_name=teacher_pending_save.get("first_name"),
                                    last_name=teacher_pending_save.get("last_name"),
                                    email=teacher_pending_save.get("email"),
                                    group_ids=teacher_pending_save.get("group_ids") or [],
                                )
                                created_teacher = _create_richmondstudio_user(
                                    rs_token,
                                    teacher_payload,
                                    timeout=int(timeout),
                                )
                                teacher_result_meta = (
                                    _extract_richmondstudio_user_create_result(
                                        created_teacher,
                                        fallback_email=teacher_pending_save.get("email"),
                                    )
                                )

                            refreshed_teacher_panel = (
                                _load_richmondstudio_teacher_panel_data(
                                    rs_token,
                                    timeout=int(timeout),
                                )
                            )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error RS: {exc}")
                    else:
                        refreshed_rows = (
                            refreshed_teacher_panel.get("rows") or []
                            if isinstance(refreshed_teacher_panel, dict)
                            else []
                        )
                        st.session_state["rs_teachers_loaded_rows"] = refreshed_rows
                        st.session_state["rs_teachers_groups_lookup"] = (
                            refreshed_teacher_panel.get("groups_lookup")
                            if isinstance(
                                refreshed_teacher_panel.get("groups_lookup"), dict
                            )
                            else {"by_id": {}, "by_name": {}}
                        )

                        target_teacher_id = str(
                            teacher_result_meta.get("user_id") or ""
                        ).strip()
                        if not target_teacher_id:
                            pending_email_norm = _normalize_compare_text(
                                teacher_pending_save.get("email")
                            )
                            pending_first_name_norm = _normalize_compare_text(
                                teacher_pending_save.get("first_name")
                            )
                            pending_last_name_norm = _normalize_compare_text(
                                teacher_pending_save.get("last_name")
                            )
                            for item in refreshed_rows:
                                if not isinstance(item, dict):
                                    continue
                                same_email = (
                                    _normalize_compare_text(item.get("Email"))
                                    == pending_email_norm
                                )
                                same_first_name = (
                                    _normalize_compare_text(item.get("First name"))
                                    == pending_first_name_norm
                                )
                                same_last_name = (
                                    _normalize_compare_text(item.get("Last name"))
                                    == pending_last_name_norm
                                )
                                if same_email and same_first_name and same_last_name:
                                    target_teacher_id = str(item.get("ID") or "").strip()
                                    if target_teacher_id:
                                        break

                        if target_teacher_id:
                            st.session_state["rs_teacher_selected_user_id"] = (
                                target_teacher_id
                            )
                        else:
                            st.session_state["rs_teacher_selected_user_id"] = "__new__"
                        st.session_state["rs_teachers_form_loaded_user_id"] = ""
                        st.session_state["rs_teacher_save_notice"] = (
                            "Docente RS actualizado correctamente."
                            if confirmed_teacher_mode == "update"
                            else "Docente RS creado correctamente."
                        )
                        st.session_state["rs_teacher_save_meta"] = teacher_result_meta
                        st.rerun()

    with tab_rs_excel:
        with st.container(border=True):
            st.markdown("**Listar alumnos registrados**")
            st.caption(
                "Richmond Studio: CLASS NAME, CLASS CODE, STUDENT NAME, IDENTIFIER, createdAt y lastSignInAt. Solo roles student/teacher."
            )
            run_rs_cleanup_subscriptions_confirmed = _consume_richmondstudio_confirmed_action(
                "rs_multi_class_remove_subscriptions_2025"
            )
            run_rs_excel = st.button(
                "Listar alumnos registrados",
                type="primary",
                key="rs_rs_excel_generate",
            )

            if run_rs_excel:
                if not rs_token:
                    st.error("Ingresa el bearer token de Richmond Studio.")
                    st.stop()
                try:
                    with st.spinner("Consultando Richmond Studio..."):
                        rs_users = _fetch_richmondstudio_users(rs_token, timeout=30)
                        rs_groups = _fetch_richmondstudio_groups(rs_token, timeout=30)
                except Exception as exc:  # pragma: no cover - UI
                    st.error(f"Error: {exc}")
                    st.stop()

                listing_data = _build_richmondstudio_registered_listing_data(
                    rs_users,
                    rs_groups,
                )
                rows_rs = list(listing_data.get("registered_rows") or [])
                multi_class_students_rows = list(
                    listing_data.get("multi_class_students_rows") or []
                )
                excluded_roles = (
                    listing_data.get("excluded_roles")
                    if isinstance(listing_data.get("excluded_roles"), dict)
                    else {}
                )
                rs_excel_bytes = _export_simple_excel(rows_rs, sheet_name="users")
                st.session_state["rs_excel_bytes"] = rs_excel_bytes
                st.session_state["rs_excel_count"] = int(len(rows_rs))
                st.session_state["rs_multi_class_students_rows"] = (
                    multi_class_students_rows
                )
                st.session_state["rs_multi_class_students_bytes"] = (
                    _export_simple_excel(
                        multi_class_students_rows,
                        sheet_name="students_multi_class",
                    )
                    if multi_class_students_rows
                    else b""
                )
                for state_key in (
                    "rs_multi_class_cleanup_summary",
                    "rs_multi_class_cleanup_rows",
                    "rs_multi_class_cleanup_bytes",
                ):
                    st.session_state.pop(state_key, None)
                st.success(
                    "Listado RS listo. Filas: {filas} | Usuarios validos: {validos}/{total}.".format(
                        filas=len(rows_rs),
                        validos=int(listing_data.get("valid_users_count") or 0),
                        total=int(listing_data.get("total_users_count") or 0),
                    )
                )
                if excluded_roles:
                    excluded_txt = ", ".join(
                        f"{role}: {count}"
                        for role, count in sorted(excluded_roles.items(), key=lambda item: item[0])
                    )
                    st.caption(f"Roles excluidos: {excluded_txt}")
                if rows_rs:
                    _show_dataframe(rows_rs[:200], use_container_width=True)
                if multi_class_students_rows:
                    st.markdown("**Alumnos inscritos en varias clases**")
                    st.caption(
                        f"Alumnos detectados en mas de una clase: {len(multi_class_students_rows)}"
                    )
                    _show_dataframe(
                        multi_class_students_rows[:200],
                        use_container_width=True,
                    )
                else:
                    st.caption("No se detectaron alumnos inscritos en varias clases.")

            multi_class_students_rows_cached = list(
                st.session_state.get("rs_multi_class_students_rows") or []
            )
            multi_class_gt2_rows = [
                row
                for row in multi_class_students_rows_cached
                if int(_safe_int(row.get("CLASSES COUNT")) or 0) > 2
            ]

            if run_rs_cleanup_subscriptions_confirmed:
                if not rs_token:
                    st.error("Ingresa el bearer token de Richmond Studio.")
                elif not multi_class_gt2_rows:
                    st.warning("No hay alumnos con mas de dos clases para limpiar.")
                else:
                    try:
                        with st.spinner("Quitando suscripciones 2025 en RS..."):
                            cleanup_summary, cleanup_rows = (
                                _remove_richmondstudio_subscriptions_2025_for_multiclass_students(
                                    token=rs_token,
                                    rows=multi_class_students_rows_cached,
                                    timeout=int(timeout),
                                    target_year=2025,
                                )
                            )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error RS: {exc}")
                    else:
                        st.session_state["rs_multi_class_cleanup_summary"] = dict(
                            cleanup_summary
                        )
                        st.session_state["rs_multi_class_cleanup_rows"] = list(
                            cleanup_rows
                        )
                        st.session_state["rs_multi_class_cleanup_bytes"] = (
                            _export_simple_excel(
                                cleanup_rows,
                                sheet_name="cleanup_2025_subscriptions",
                            )
                            if cleanup_rows
                            else b""
                        )
                        st.success(
                            "Limpieza RS completada. Elegibles: {eligible_total} | "
                            "Actualizados: {updated_total} | Sin cambios: {skipped_total} | "
                            "Errores: {error_total} | Suscripciones removidas: {removed_total}.".format(
                                **cleanup_summary
                            )
                        )

            if multi_class_gt2_rows:
                st.markdown("**Alumnos con mas de dos clases**")
                st.caption(
                    "Estos alumnos son candidatos para quitar suscripciones creadas en 2025."
                )
                _show_dataframe(
                    multi_class_gt2_rows[:200],
                    use_container_width=True,
                )
                if st.button(
                    "Quitar suscripciones 2025 (>2 clases)",
                    type="primary",
                    key="rs_multi_class_cleanup_request_btn",
                    use_container_width=True,
                ):
                    _request_richmondstudio_confirmation(
                        "rs_multi_class_remove_subscriptions_2025",
                        (
                            f"quitar suscripciones 2025 a {len(multi_class_gt2_rows)} "
                            "alumnos con mas de dos clases"
                        ),
                    )

            cleanup_summary_cached = (
                st.session_state.get("rs_multi_class_cleanup_summary") or {}
            )
            cleanup_rows_cached = (
                st.session_state.get("rs_multi_class_cleanup_rows") or []
            )
            cleanup_bytes_cached = (
                st.session_state.get("rs_multi_class_cleanup_bytes") or b""
            )
            if cleanup_summary_cached:
                st.markdown("**Resultado de limpieza de suscripciones 2025**")
                st.info(
                    "Elegibles: {eligible_total} | Procesados: {processed_total} | "
                    "Actualizados: {updated_total} | Sin cambios: {skipped_total} | "
                    "Errores: {error_total} | Suscripciones removidas: {removed_total}".format(
                        **cleanup_summary_cached
                    )
                )
                if cleanup_rows_cached:
                    _show_dataframe(cleanup_rows_cached[:200], use_container_width=True)
                if cleanup_bytes_cached:
                    st.download_button(
                        label="Descargar resultado limpieza suscripciones 2025",
                        data=cleanup_bytes_cached,
                        file_name="rs_limpieza_suscripciones_2025.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="rs_multi_class_cleanup_download",
                        use_container_width=True,
                    )

            if st.session_state.get("rs_excel_bytes"):
                col_rs_download_a, col_rs_download_b = st.columns(2, gap="small")
                col_rs_download_a.download_button(
                    label="Descargar listado RS",
                    data=st.session_state["rs_excel_bytes"],
                    file_name="excel_rs.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="rs_rs_excel_download",
                    use_container_width=True,
                )
                if st.session_state.get("rs_multi_class_students_bytes"):
                    col_rs_download_b.download_button(
                        label="Descargar alumnos en varias clases",
                        data=st.session_state["rs_multi_class_students_bytes"],
                        file_name="alumnos_varias_clases_rs.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="rs_rs_multi_students_download",
                        use_container_width=True,
                    )

    if isinstance(st.session_state.get("rs_pending_confirmation"), dict):
        _render_richmondstudio_confirmation_dialog()


if menu_option == "Richmond Studio":
    render_richmond_studio_view()
    st.stop()

with tab_crud_clases:
    if not _restricted_sections_unlocked():
        _render_restricted_blur("CRUD Clases", "clases_1")
    else:
        st.subheader("CRUD Clases")
        st.caption("Selecciona una funcion a la izquierda y trabaja en el panel derecho.")
        colegio_id_raw = str(st.session_state.get("shared_colegio_id", "")).strip()
        ciclo_id = GESTION_ESCOLAR_CICLO_ID_DEFAULT
        token = _get_shared_token()
        empresa_id = DEFAULT_EMPRESA_ID
        timeout = 30

        @st.fragment(run_every="2s")
        def _render_asignacion_clases_usuarios_section() -> None:
            colegio_id_int: Optional[int] = None
            colegio_error = ""
            if str(colegio_id_raw).strip():
                try:
                    colegio_id_int = _parse_colegio_id(colegio_id_raw)
                except ValueError as exc:
                    colegio_error = str(exc)
            current_ingles_scope = (
                int(empresa_id),
                int(ciclo_id),
                int(colegio_id_int),
            ) if colegio_id_int is not None else None
            cached_ingles_scope = st.session_state.get(
                "clases_auto_group_ingles_grades_scope"
            )
            if cached_ingles_scope != current_ingles_scope:
                cached_ingles_options = (
                    st.session_state.get("clases_auto_group_ingles_grade_options") or []
                )
                for option in cached_ingles_options:
                    if not isinstance(option, dict):
                        continue
                    st.session_state.pop(
                        _participantes_ingles_grade_checkbox_key(option.get("key")),
                        None,
                    )
                for state_key in (
                    "clases_auto_group_ingles_grades_scope",
                    "clases_auto_group_ingles_grade_options",
                    "clases_auto_group_ingles_grade_error",
                    "clases_auto_group_ingles_grade_selected_keys",
                ):
                    st.session_state.pop(state_key, None)

            current_job_id = ""
            if colegio_id_int is not None:
                current_job_id = (
                    _get_participantes_sync_job_id_for_scope(
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        colegio_id=int(colegio_id_int),
                    )
                    or ""
                )
                if current_job_id:
                    st.session_state["clases_auto_group_job_id"] = current_job_id

            current_job = _get_participantes_sync_job(current_job_id)
            is_running = _is_participantes_sync_job_active(current_job)

            with st.container(border=True):
                st.markdown("**Asignacion de clases a usuarios**")
                st.caption(
                    "Sincroniza automaticamente alumnos activos por grado y seccion: "
                    "agrega faltantes y elimina sobrantes en cada clase. El proceso "
                    "sigue corriendo en segundo plano aunque cambies de ventana."
                )
                st.caption(
                    "Excluye automaticamente clases cuyo geClase o geClaseClave contenga "
                    "'Santillana inclusiva'."
                )
                exclude_ingles_por_niveles = st.checkbox(
                    "Ingles por niveles",
                    key="clases_auto_group_exclude_ingles_checkbox",
                    help=(
                        "Si esta activo, las clases cuyo geClase o geClaseClave "
                        "contenga 'Ingles' se vaciaran y no recibiran asignacion "
                        "automatica por grado y seccion."
                    ),
                )
                ingles_grade_options = (
                    st.session_state.get("clases_auto_group_ingles_grade_options") or []
                )
                ingles_grade_error = str(
                    st.session_state.get("clases_auto_group_ingles_grade_error") or ""
                ).strip()
                selected_ingles_grade_keys: List[str] = []
                if exclude_ingles_por_niveles:
                    if (
                        current_ingles_scope is not None
                        and not ingles_grade_options
                        and not ingles_grade_error
                        and token
                    ):
                        try:
                            with st.spinner("Cargando grados con clases de Ingles..."):
                                clases_ingles = _fetch_clases_gestion_escolar(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                    ordered=True,
                                )
                        except Exception as exc:  # pragma: no cover - UI
                            ingles_grade_error = str(exc)
                            st.session_state[
                                "clases_auto_group_ingles_grade_error"
                            ] = ingles_grade_error
                            st.session_state[
                                "clases_auto_group_ingles_grades_scope"
                            ] = current_ingles_scope
                        else:
                            ingles_grade_options = (
                                _build_ingles_grade_options_for_participantes(
                                    clases_ingles
                                )
                            )
                            st.session_state[
                                "clases_auto_group_ingles_grade_options"
                            ] = ingles_grade_options
                            st.session_state[
                                "clases_auto_group_ingles_grade_error"
                            ] = ""
                            st.session_state[
                                "clases_auto_group_ingles_grades_scope"
                            ] = current_ingles_scope
                            st.session_state[
                                "clases_auto_group_ingles_grade_selected_keys"
                            ] = []

                    st.caption(
                        "Las clases cuyo geClase o geClaseClave contenga 'Ingles' "
                        "se vaciaran de alumnos solo en los grados que selecciones."
                    )
                    if ingles_grade_error:
                        st.error(
                            f"No se pudieron cargar los grados de Ingles: {ingles_grade_error}"
                        )
                    elif ingles_grade_options:
                        ingles_grade_option_by_key = {
                            str(item.get("key") or "").strip(): item
                            for item in ingles_grade_options
                            if str(item.get("key") or "").strip()
                        }
                        valid_ingles_option_keys = list(
                            ingles_grade_option_by_key.keys()
                        )
                        current_selected_ingles_keys = [
                            str(item).strip()
                            for item in (
                                st.session_state.get(
                                    "clases_auto_group_ingles_grade_selected_keys"
                                )
                                or []
                            )
                            if str(item).strip() in valid_ingles_option_keys
                        ]
                        st.markdown("**Grados con Ingles por niveles**")
                        checkbox_cols = st.columns(2, gap="small")
                        selected_ingles_grade_keys = []
                        for idx_option, option_key in enumerate(valid_ingles_option_keys):
                            option_row = ingles_grade_option_by_key.get(
                                str(option_key), {}
                            )
                            class_names = (
                                option_row.get("class_names")
                                if isinstance(option_row.get("class_names"), list)
                                else []
                            )
                            checkbox_label = (
                                f"{str(option_row.get('nivel_nombre') or '').strip() or '-'} | "
                                f"{str(option_row.get('grado_nombre') or '').strip() or '-'}"
                            )
                            if class_names:
                                checkbox_label = (
                                    f"{checkbox_label} ({len(class_names)} clase(s))"
                                )
                            checkbox_key = _participantes_ingles_grade_checkbox_key(
                                option_key
                            )
                            if checkbox_key not in st.session_state:
                                st.session_state[checkbox_key] = (
                                    option_key in current_selected_ingles_keys
                                )
                            with checkbox_cols[idx_option % 2]:
                                is_selected = st.checkbox(
                                    checkbox_label,
                                    key=checkbox_key,
                                )
                                if class_names:
                                    st.caption(
                                        "Clases: " + " | ".join(class_names[:6])
                                    )
                                    pending_class_names = len(class_names) - 6
                                    if pending_class_names > 0:
                                        st.caption(
                                            f"... y {pending_class_names} clase(s) mas."
                                        )
                            if is_selected:
                                selected_ingles_grade_keys.append(str(option_key))
                        st.session_state[
                            "clases_auto_group_ingles_grade_selected_keys"
                        ] = list(selected_ingles_grade_keys)
                        st.caption(
                            "Si el checkbox de un grado esta marcado, se borran los "
                            "alumnos de sus clases de Ingles. Si no esta marcado, ese "
                            "grado se asigna normal."
                        )
                    else:
                        st.caption(
                            "No se detectaron clases de Ingles para seleccionar por grado."
                        )
                col_run, col_cancel = st.columns([4, 1], gap="small")
                with col_run:
                    run_actualizar_participantes_auto = st.button(
                        "Actualizar asignacion",
                        key="clases_auto_group_sync_auto_btn",
                        type="primary",
                        use_container_width=True,
                        disabled=is_running,
                    )
                with col_cancel:
                    run_cancelar_participantes_auto = st.button(
                        "Cancelar",
                        key="clases_auto_group_sync_cancel_btn",
                        use_container_width=True,
                        disabled=not is_running,
                    )

                if run_actualizar_participantes_auto:
                    if not token:
                        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                    elif colegio_error:
                        st.error(f"Error: {colegio_error}")
                    elif colegio_id_int is None:
                        st.error("Ingresa un Colegio Clave (global) valido.")
                    else:
                        current_job_id = _start_participantes_sync_job(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                            exclude_ingles_por_niveles=bool(
                                exclude_ingles_por_niveles
                            ),
                            ingles_grade_keys=selected_ingles_grade_keys,
                        )
                        st.session_state["clases_auto_group_job_id"] = current_job_id
                        current_job = _get_participantes_sync_job(current_job_id)
                        is_running = _is_participantes_sync_job_active(current_job)
                        st.success("Asignacion iniciada en segundo plano.")

                if run_cancelar_participantes_auto:
                    if _request_cancel_participantes_sync_job(current_job_id):
                        current_job = _get_participantes_sync_job(current_job_id)
                        is_running = _is_participantes_sync_job_active(current_job)
                        st.warning("Cancelacion solicitada.")
                    else:
                        st.info("No hay un proceso activo para cancelar.")

                if colegio_error:
                    st.caption(f"Colegio actual invalido: {colegio_error}")

                if not isinstance(current_job, dict):
                    st.caption(
                        "Usa este bloque para sincronizar en segundo plano los alumnos "
                        "activos del colegio actual."
                    )
                    return

                state = str(current_job.get("state") or "").strip()
                summary_auto = (
                    dict(current_job.get("summary"))
                    if isinstance(current_job.get("summary"), dict)
                    else {}
                )
                warnings_auto = list(current_job.get("warnings") or [])
                group_error_lines = list(current_job.get("group_error_lines") or [])
                exclude_ingles_job = bool(
                    current_job.get("exclude_ingles_por_niveles", False)
                )
                ingles_grade_keys_job = [
                    str(item).strip()
                    for item in list(current_job.get("ingles_grade_keys") or [])
                    if str(item).strip()
                ]
                status_messages = [
                    str(item).strip()
                    for item in list(current_job.get("status_messages") or [])
                    if str(item).strip()
                ]
                cancel_requested = bool(current_job.get("cancel_requested"))
                error_text = str(current_job.get("error") or "").strip()

                if state in {"starting", "running"}:
                    if cancel_requested:
                        st.warning(
                            "Cancelacion solicitada. El proceso terminara al cerrar el "
                            "bloque actual."
                        )
                    else:
                        st.info("Proceso en ejecucion en segundo plano.")
                elif state == "done":
                    if (
                        summary_auto.get("clases_error", 0) == 0
                        and not group_error_lines
                        and not warnings_auto
                    ):
                        st.success("Asignacion automatica completada.")
                    else:
                        st.warning("Asignacion automatica completada con observaciones.")
                elif state == "cancelled":
                    st.warning("Proceso cancelado. Se conserva el resumen parcial.")
                elif state == "error":
                    st.error(error_text or "No se pudo completar la sincronizacion.")

                if status_messages:
                    st.info("\n".join(f"- {item}" for item in status_messages[-8:]))

                st.caption(
                    "Resumen: "
                    f"Alumnos asignados={summary_auto.get('agregados_ok', 0)} | "
                    f"Alumnos eliminados={summary_auto.get('eliminados_ok', 0)} | "
                    f"Clases sin cambios={summary_auto.get('clases_skip', 0)} | "
                    f"Clases con error={summary_auto.get('clases_error', 0)} | "
                    f"Ingles por niveles={'Si' if exclude_ingles_job else 'No'} | "
                    f"Grados ingles={len(ingles_grade_keys_job)}"
                )
                if warnings_auto:
                    st.caption(f"Advertencias de mapeo de clases: {len(warnings_auto)}")
                if group_error_lines:
                    st.caption(f"Errores al consultar secciones: {len(group_error_lines)}")

        @st.fragment
        def _render_clases_gestion_section() -> None:
            listed_class_rows = st.session_state.get("clases_gestion_rows") or []
            selected_class_ids_state = {
                int(item)
                for item in (st.session_state.get("clases_gestion_selected_ids") or [])
                if _safe_int(item) is not None
            }

            col_list, col_selected = st.columns([2.2, 1.4], gap="large")
            with col_list:
                with st.container(border=True):
                    st.markdown("**Clases disponibles**")
                    run_listar_clases = st.button("Listar clases", key="clases_listar_btn")
                    valid_class_rows = [
                        {
                            "ID": item.get("ID"),
                            "Clase": item.get("Clase") or "",
                            "Nivel": item.get("Nivel") or "",
                            "Grado": item.get("Grado") or "",
                            "Grupo": item.get("Grupo") or "",
                        }
                        for item in listed_class_rows
                        if isinstance(item, dict) and _safe_int(item.get("ID")) is not None
                    ]
                    if valid_class_rows:
                        _show_dataframe(valid_class_rows, use_container_width=True)

                        class_name_by_id = {
                            int(item["ID"]): item["Clase"] or "Clase sin nombre"
                            for item in valid_class_rows
                        }
                        selected_ids = st.multiselect(
                            "Clases a eliminar",
                            options=list(class_name_by_id.keys()),
                            default=[
                                class_id
                                for class_id in selected_class_ids_state
                                if class_id in class_name_by_id
                            ],
                            format_func=lambda class_id: class_name_by_id.get(
                                int(class_id), str(class_id)
                            ),
                            placeholder="Selecciona una o varias clases.",
                        )
                        selected_class_ids_state = {
                            int(class_id)
                            for class_id in selected_ids
                            if _safe_int(class_id) is not None
                        }
                        st.session_state["clases_gestion_selected_ids"] = sorted(
                            selected_class_ids_state
                        )
                    elif listed_class_rows:
                        st.info("No hay filas validas para mostrar.")
                    else:
                        st.caption("Aun no hay clases listadas.")

            with col_selected:
                with st.container(border=True):
                    st.markdown("**Clases a eliminar**")
                    selected_class_rows = [
                        item
                        for item in listed_class_rows
                        if isinstance(item, dict)
                        and _safe_int(item.get("ID")) is not None
                        and int(item.get("ID")) in selected_class_ids_state
                    ]
                    if selected_class_rows:
                        _show_dataframe(selected_class_rows, use_container_width=True)
                    else:
                        st.caption("Selecciona clases en la tabla de la izquierda.")
                    confirm_delete = st.checkbox(
                        "Confirmo eliminar las clases seleccionadas.",
                        key="clases_confirm_delete",
                    )
                    run_eliminar_clases = st.button(
                        "Eliminar clases",
                        key="clases_eliminar_btn",
                        disabled=not selected_class_rows,
                    )

            if run_listar_clases:
                if not token:
                    st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                else:
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                        st.stop()
                    try:
                        clases = _fetch_clases_gestion_escolar(
                            token=token,
                            colegio_id=colegio_id_int,
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                    else:
                        if not clases:
                            st.info("No se encontraron clases.")
                        else:
                            tabla = [
                                {
                                    "ID": item.get("geClaseId"),
                                    "Clase": item.get("geClase") or item.get("geClaseClave") or "",
                                    "Nivel": (
                                        ((item.get("colegioNivelCiclo") or {}).get("nivel") or {}).get("nivel")
                                        if isinstance(item, dict)
                                        else ""
                                    )
                                    or "",
                                    "Grado": (
                                        ((item.get("colegioGradoGrupo") or {}).get("grado") or {}).get("grado")
                                        if isinstance(item, dict)
                                        else ""
                                    )
                                    or "",
                                    "Grupo": (
                                        ((item.get("colegioGradoGrupo") or {}).get("grupo") or {}).get("grupoClave")
                                        or ((item.get("colegioGradoGrupo") or {}).get("grupo") or {}).get("grupo")
                                        if isinstance(item, dict)
                                        else ""
                                    )
                                    or "",
                                }
                                for item in clases
                                if isinstance(item, dict)
                            ]
                            st.session_state["clases_gestion_rows"] = tabla
                            st.session_state["clases_gestion_selected_ids"] = []
                            st.success(f"Clases encontradas: {len(tabla)}")

            if run_eliminar_clases:
                if not token:
                    st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                    st.stop()
                if not confirm_delete:
                    st.error("Debes confirmar antes de eliminar.")
                    st.stop()
                selected_class_rows = [
                    item
                    for item in (st.session_state.get("clases_gestion_rows") or [])
                    if isinstance(item, dict)
                    and _safe_int(item.get("ID")) is not None
                    and int(item.get("ID")) in {
                        int(selected_id)
                        for selected_id in (st.session_state.get("clases_gestion_selected_ids") or [])
                        if _safe_int(selected_id) is not None
                    }
                ]
                if not selected_class_rows:
                    st.error("No hay clases seleccionadas.")
                    st.stop()

                errores: List[str] = []
                eliminadas_ids: Set[int] = set()
                for item in selected_class_rows:
                    clase_id = item.get("ID") if isinstance(item, dict) else None
                    if clase_id is None:
                        errores.append("Clase sin ID.")
                        continue
                    try:
                        _delete_clase_gestion_escolar(
                            token=token,
                            clase_id=int(clase_id),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                        eliminadas_ids.add(int(clase_id))
                    except Exception as exc:  # pragma: no cover - UI
                        errores.append(f"{clase_id}: {exc}")

                remaining_rows = [
                    item
                    for item in (st.session_state.get("clases_gestion_rows") or [])
                    if not (
                        isinstance(item, dict)
                        and _safe_int(item.get("ID")) is not None
                        and int(item.get("ID")) in eliminadas_ids
                    )
                ]
                st.session_state["clases_gestion_rows"] = remaining_rows
                st.session_state["clases_gestion_selected_ids"] = [
                    int(item)
                    for item in (st.session_state.get("clases_gestion_selected_ids") or [])
                    if _safe_int(item) is not None and int(item) not in eliminadas_ids
                ]
                eliminadas = len(eliminadas_ids)
                st.success(f"Clases eliminadas: {eliminadas}")
                if errores:
                    st.error("Errores al eliminar:")
                    st.write("\n".join(f"- {item}" for item in errores))

        clases_nav_col, clases_body_col = st.columns([1.15, 4.85], gap="large")
        with clases_nav_col:
            clases_crud_view = _render_crud_menu(
                "Funciones de clases",
                [
                    ("crear", "Crear", "Genera clases desde Excel"),
                    ("gestion", "Gestion", "Lista, vacia o elimina clases"),
                    ("otros", "Asignacion de clases a usuarios", "Asignacion de clases a usuarios"),
                    ("simulador", "Actualizar users Payments", "Prepara y aplica cambios de users payments"),
                ],
                state_key="clases_crud_nav",
            )
        with clases_body_col:
            if clases_crud_view == "crear":
                st.markdown("**1) Crear clases**")
                st.caption("Solo necesitas Excel, codigo CRM y secciones.")
                uploaded_excel = st.file_uploader(
                    "Excel de entrada",
                    type=["xlsx"],
                    help="Ejemplo: PreOnboarding_Detalle_20251212.xlsx",
                )
                col1, col2 = st.columns(2)
                codigo = col1.text_input("Codigo CRM", placeholder="00001053")
                grupos = col2.text_input(
                    "Secciones (A,B,C,D)",
                    value="A",
                    help="Letras separadas por coma para crear secciones.",
                )

                if st.button("Generar clases", type="primary"):
                    if not uploaded_excel:
                        st.error("Sube un Excel de entrada.")
                        st.stop()
                    if not codigo.strip():
                        st.error("Ingresa un cÃ³digo.")
                        st.stop()
                    if not grupos.strip():
                        st.error("Ingresa las secciones (A,B,C,D).")
                        st.stop()

                    excel_bytes = uploaded_excel.read()
                    plantilla_path = Path(OUTPUT_FILENAME) if Path(OUTPUT_FILENAME).exists() else None

                    try:
                        with st.spinner("Procesando..."):
                            output_bytes, summary = process_excel(
                                excel_bytes,
                                codigo=codigo,
                                columna_codigo=CODE_COLUMN_NAME,
                                hoja=SHEET_NAME,
                                plantilla_path=plantilla_path,
                                grupos=grupos,
                            )
                        st.success(
                            f"Listo. Filtradas: {summary['filas_filtradas']}, Salida: {summary['filas_salida']} filas."
                        )
                        download_name = f"{Path(OUTPUT_FILENAME).stem}_{codigo}.xlsx"
                        st.download_button(
                            label="Descargar Excel",
                            data=output_bytes,
                            file_name=download_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
            if clases_crud_view == "gestion":
                st.markdown("**2) Gestion de clases**")
                _render_clases_gestion_section()
            if clases_crud_view == "otros":
                _render_asignacion_clases_usuarios_section()
            if clases_crud_view == "simulador":
                with st.container(border=True):
                    st.markdown("**3) Actualizar users Payments**")
                    st.caption(
                        "Usa el Colegio Clave global para preparar y aplicar cambios de users payments."
                    )
                    if not _restricted_sections_unlocked():
                        _render_restricted_blur(
                            "Actualizar users Payments",
                            "simulador_web_y",
                        )
                        st.stop()

                    col_prepare, col_clear = st.columns([2, 1], gap="small")
                    run_prepare_auto_plan = col_prepare.button(
                        "Analizar y preparar lista de cambios",
                        type="primary",
                        key="auto_move_prepare_btn",
                        use_container_width=True,
                    )
                    clear_auto_plan = col_clear.button(
                        "Limpiar lista",
                        key="auto_move_clear_btn",
                        use_container_width=True,
                    )

                    if clear_auto_plan:
                        for state_key in (
                            "auto_move_plan_rows",
                            "auto_move_editor_rows",
                            "auto_move_alumnos_grid",
                            "auto_move_errors",
                            "auto_move_colegio_id",
                            "auto_move_removed_ref_ids",
                            "auto_move_group_map_by_grade",
                            "auto_move_status_messages",
                        ):
                            st.session_state.pop(state_key, None)
                        st.rerun()

                    if run_prepare_auto_plan:
                        token = _get_shared_token()
                        if not token:
                            st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                            st.stop()
                        try:
                            colegio_id_int = _parse_colegio_id(colegio_id_raw)
                        except ValueError as exc:
                            st.error(f"Error: {exc}")
                            st.stop()

                        try:
                            status_box = st.empty()

                            def _on_status(message: str) -> None:
                                msg = str(message or "").strip()
                                if not msg:
                                    return
                                status_box.info(msg)

                            with st.spinner("Preparando simulacion de cambios..."):
                                simulation = _build_auto_move_simulation(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                    on_status=_on_status,
                                )
                            status_box.empty()
                        except Exception as exc:  # pragma: no cover - UI
                            st.error(f"Error: {exc}")
                            st.stop()

                        st.session_state["auto_move_plan_rows"] = simulation.get("plan_rows") or []
                        st.session_state["auto_move_editor_rows"] = simulation.get("editor_rows") or []
                        st.session_state["auto_move_alumnos_grid"] = simulation.get("alumnos_all_grid") or []
                        st.session_state["auto_move_errors"] = simulation.get("errors") or []
                        st.session_state["auto_move_colegio_id"] = int(colegio_id_int)
                        st.session_state["auto_move_group_map_by_grade"] = (
                            simulation.get("grupo_id_by_seccion_by_grade") or {}
                        )
                        st.session_state["auto_move_removed_ref_ids"] = []

                        total_plan = len(st.session_state["auto_move_plan_rows"])
                        st.success(f"Simulacion lista. Alumnos candidatos a modificar: {total_plan}")

                    errors_cached = st.session_state.get("auto_move_errors") or []
                    if errors_cached:
                        st.warning("Hubo errores consultando algunas secciones.")
                        st.write("\n".join(f"- {item}" for item in errors_cached[:20]))
                        pending = len(errors_cached) - 20
                        if pending > 0:
                            st.caption(f"... y {pending} errores mas.")

                    plan_rows_cached = st.session_state.get("auto_move_plan_rows") or []
                    if plan_rows_cached:
                        st.markdown("**Lista de cambios para autorizar**")
                        plan_by_id = {
                            int(plan.get("plan_id")): plan
                            for plan in plan_rows_cached
                            if _safe_int(plan.get("plan_id")) is not None
                        }
                        sorted_plan_ids = sorted(plan_by_id.keys())
                        if not sorted_plan_ids:
                            st.info("No hay alumnos para modificar.")
                        group_map_by_grade = st.session_state.get("auto_move_group_map_by_grade", {})
                        destino_payload_by_option: Dict[str, Dict[str, object]] = {}
                        destino_options: List[str] = []
                        table_rows: List[Dict[str, object]] = []

                        for plan_id in sorted_plan_ids:
                            plan = plan_by_id.get(int(plan_id)) or {}
                            pagado = (
                                plan.get("alumno_pagado")
                                if isinstance(plan.get("alumno_pagado"), dict)
                                else {}
                            )
                            referencial = (
                                plan.get("alumno_inactivar")
                                if isinstance(plan.get("alumno_inactivar"), dict)
                                else {}
                            )
                            nivel_id = _safe_int(plan.get("nivel_id"))
                            grado_id = _safe_int(plan.get("grado_id"))
                            mapping: Dict[str, int] = {}
                            if (
                                isinstance(group_map_by_grade, dict)
                                and nivel_id is not None
                                and grado_id is not None
                            ):
                                mapping_raw = group_map_by_grade.get((int(nivel_id), int(grado_id)))
                                if not isinstance(mapping_raw, dict):
                                    mapping_raw = group_map_by_grade.get(
                                        f"{int(nivel_id)}:{int(grado_id)}"
                                    )
                                if isinstance(mapping_raw, dict):
                                    mapping = mapping_raw

                            nivel_txt = str(pagado.get("nivel") or plan.get("nivel") or "").strip()
                            grado_txt = str(pagado.get("grado") or plan.get("grado") or "").strip()
                            seccion_origen_txt = _normalize_seccion_key(
                                plan.get("seccion_origen")
                                or pagado.get("seccion_norm")
                                or pagado.get("seccion")
                                or AUTO_MOVE_SECCION_ORIGEN
                            )
                            seccion_destino_txt = _normalize_seccion_key(plan.get("seccion_destino") or "")

                            if mapping and not seccion_destino_txt:
                                picked_sec, picked_gid = _pick_default_destino(
                                    grupo_id_by_seccion=mapping,
                                    origen_seccion=AUTO_MOVE_SECCION_ORIGEN,
                                )
                                if picked_sec and picked_gid is not None:
                                    seccion_destino_txt = _normalize_seccion_key(picked_sec)
                                    plan["seccion_destino"] = seccion_destino_txt
                                    plan["grupo_destino_id"] = int(picked_gid)

                            for seccion_key, grupo_destino_id in sorted(mapping.items(), key=lambda item: str(item[0])):
                                sec = _normalize_seccion_key(seccion_key)
                                option_text = f"{nivel_txt} | {grado_txt} ({sec})"
                                if option_text not in destino_payload_by_option:
                                    destino_payload_by_option[option_text] = {
                                        "nivel_id": int(nivel_id) if nivel_id is not None else None,
                                        "grado_id": int(grado_id) if grado_id is not None else None,
                                        "grupo_destino_id": int(grupo_destino_id),
                                        "seccion_destino": sec,
                                    }
                                    destino_options.append(option_text)

                            default_option = ""
                            if nivel_txt or grado_txt or seccion_destino_txt:
                                default_option = f"{nivel_txt} | {grado_txt} ({seccion_destino_txt})"
                            if default_option and default_option not in destino_payload_by_option:
                                destino_payload_by_option[default_option] = {
                                    "nivel_id": int(nivel_id) if nivel_id is not None else None,
                                    "grado_id": int(grado_id) if grado_id is not None else None,
                                    "grupo_destino_id": _safe_int(plan.get("grupo_destino_id")),
                                    "seccion_destino": seccion_destino_txt,
                                }
                                destino_options.append(default_option)

                            alumno_col = (
                                f"{_format_alumno_label(pagado)} | "
                                f"{nivel_txt} | {grado_txt} ({seccion_origen_txt})"
                            )
                            referencia_col = (
                                _format_alumno_label(referencial)
                                if isinstance(referencial, dict) and referencial
                                else "SIN REFERENCIA"
                            )
                            requiere_inactivar = bool(
                                _to_bool(plan.get("requiere_inactivar"))
                                and _safe_int(referencial.get("alumno_id")) is not None
                            )
                            table_rows.append(
                                {
                                    "PlanId": int(plan_id),
                                    "Alumno | Grado y seccion": alumno_col,
                                    "Referencia": referencia_col,
                                    "Inactivar referencia": requiere_inactivar,
                                    "Nuevo grado y seccion": default_option,
                                }
                            )

                        destino_options = sorted(destino_options)
                        if not destino_options:
                            destino_options = [""]

                        table_df = pd.DataFrame(table_rows)
                        edited_table_df = st.data_editor(
                            table_df,
                            key="auto_move_plan_editor_table",
                            hide_index=True,
                            use_container_width=True,
                            disabled=["PlanId", "Alumno | Grado y seccion", "Referencia"],
                            column_config={
                                "PlanId": st.column_config.NumberColumn("PlanId", format="%d"),
                                "Alumno | Grado y seccion": st.column_config.TextColumn(
                                    "Alumno | Grado y seccion"
                                ),
                                "Referencia": st.column_config.TextColumn("Referencia"),
                                "Inactivar referencia": st.column_config.CheckboxColumn(
                                    "Inactivar referencia"
                                ),
                                "Nuevo grado y seccion": st.column_config.SelectboxColumn(
                                    "Nuevo grado y seccion",
                                    options=destino_options,
                                    required=True,
                                ),
                            },
                        )

                        edited_rows = (
                            edited_table_df.to_dict("records")
                            if isinstance(edited_table_df, pd.DataFrame)
                            else table_rows
                        )

                        authorized_plans: List[Dict[str, object]] = []
                        removed_ref_ids_current: Set[int] = set()
                        for base_plan in plan_rows_cached:
                            plan_id = _safe_int(base_plan.get("plan_id"))
                            if plan_id is None:
                                continue
                            plan = dict(base_plan) if isinstance(base_plan, dict) else {}
                            if not plan:
                                continue

                            edited_row = next(
                                (
                                    row
                                    for row in edited_rows
                                    if _safe_int(row.get("PlanId")) == int(plan_id)
                                ),
                                {},
                            )

                            keep_reference = bool(_to_bool(edited_row.get("Inactivar referencia")))
                            if not keep_reference:
                                removed_ref_ids_current.add(int(plan_id))
                                plan["alumno_parecido"] = {}
                                plan["alumno_inactivar"] = {}
                                plan["requiere_inactivar"] = False
                                plan["comparacion"] = (
                                    "Referencia eliminada manualmente: solo movimiento de seccion."
                                )
                                plan["motivo"] = (
                                    "Referencia eliminada manualmente: no se inactiva alumno parecido."
                                )

                            selected_destino = str(edited_row.get("Nuevo grado y seccion") or "").strip()
                            payload = destino_payload_by_option.get(selected_destino)
                            if isinstance(payload, dict):
                                nivel_id_val = _safe_int(payload.get("nivel_id"))
                                grado_id_val = _safe_int(payload.get("grado_id"))
                                grupo_id_val = _safe_int(payload.get("grupo_destino_id"))
                                seccion_val = str(payload.get("seccion_destino") or "").strip()
                                if nivel_id_val is not None:
                                    plan["nivel_id"] = int(nivel_id_val)
                                if grado_id_val is not None:
                                    plan["grado_id"] = int(grado_id_val)
                                if grupo_id_val is not None:
                                    plan["grupo_destino_id"] = int(grupo_id_val)
                                if seccion_val:
                                    plan["seccion_destino"] = seccion_val
                            authorized_plans.append(plan)

                        st.session_state["auto_move_removed_ref_ids"] = sorted(removed_ref_ids_current)

                        st.caption(
                            "Cambios listos para guardar: {total} | Referencias quitadas: {removed}".format(
                                total=len(authorized_plans),
                                removed=len(removed_ref_ids_current),
                            )
                        )

                        run_apply_auto = st.button(
                            "Guardar cambios autorizados",
                            key="auto_move_apply_btn",
                            use_container_width=True,
                        )

                        if run_apply_auto:
                            token = _get_shared_token()
                            if not token:
                                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                                st.stop()
                            if not authorized_plans:
                                st.warning("No hay cambios autorizados para guardar.")
                                st.stop()
                            colegio_id_exec = _safe_int(st.session_state.get("auto_move_colegio_id"))
                            if colegio_id_exec is None:
                                try:
                                    colegio_id_exec = _parse_colegio_id(colegio_id_raw)
                                except ValueError as exc:
                                    st.error(f"Error: {exc}")
                                    st.stop()
                            try:
                                st.info(
                                    "Iniciando guardado de cambios autorizados: "
                                    f"{len(authorized_plans)} alumno(s)."
                                )
                                with st.spinner(
                                    "Guardando cambios (inactivar referencia, mover seccion y asignar clases)..."
                                ):
                                    summary_apply, results_apply = _apply_auto_move_changes(
                                        token=token,
                                        colegio_id=int(colegio_id_exec),
                                        empresa_id=int(empresa_id),
                                        ciclo_id=int(ciclo_id),
                                        timeout=int(timeout),
                                        plan_rows=authorized_plans,
                                    )
                            except Exception as exc:  # pragma: no cover - UI
                                st.error(f"No se pudieron guardar los cambios: {exc}")
                                st.stop()

                            inactivar_ok = int(summary_apply.get("inactivar_ok", 0))
                            inactivar_error = int(summary_apply.get("inactivar_error", 0))
                            mover_ok = int(summary_apply.get("mover_ok", 0))
                            mover_error = int(summary_apply.get("mover_error", 0))
                            asignar_ok = int(summary_apply.get("asignar_ok", 0))
                            asignar_error = int(summary_apply.get("asignar_error", 0))
                            asignar_skip = int(summary_apply.get("asignar_skip", 0))
                            total_errors = inactivar_error + mover_error + asignar_error

                            if total_errors == 0:
                                st.success("Cambios guardados correctamente.")
                            else:
                                st.warning("Guardado completado con observaciones.")

                            st.caption(
                                "Resumen: "
                                f"Inactivar OK={inactivar_ok}, ERROR={inactivar_error} | "
                                f"Mover OK={mover_ok}, ERROR={mover_error} | "
                                f"Asignar clases OK={asignar_ok}, ERROR={asignar_error}, SKIP={asignar_skip}"
                            )
                            if results_apply:
                                details = []
                                for item in results_apply[:80]:
                                    if not isinstance(item, dict):
                                        continue
                                    details.append(
                                        "- {alumno} | Inactivar: {inactivar} | Mover: {mover} | Asignar: {asignar}".format(
                                            alumno=str(item.get("Alumno pagado") or ""),
                                            inactivar=str(item.get("Inactivar no pagado") or ""),
                                            mover=str(item.get("Mover") or ""),
                                            asignar=str(item.get("Asignar clases") or ""),
                                        )
                                    )
                                if details:
                                    st.markdown("**Detalle por alumno**")
                                    st.markdown("\n".join(details))
                                if len(results_apply) > 80:
                                    st.caption(f"... y {len(results_apply) - 80} filas mas.")

                with st.container(border=True):
                    st.markdown("**4) Verificar colegios Payments**")
                    st.caption(
                        "Colegios incluidos: {total}".format(
                            total=len(AUTO_MOVE_MULTI_ACTIVE_COLEGIO_IDS)
                        )
                    )
                    st.dataframe(
                        pd.DataFrame(AUTO_MOVE_MULTI_ACTIVE_SCHOOLS),
                        use_container_width=True,
                        hide_index=True,
                        height=260,
                    )

                    col_prepare_multi, col_clear_multi = st.columns([2, 1], gap="small")
                    run_prepare_auto_multi = col_prepare_multi.button(
                        "Analizar colegios predefinidos",
                        type="primary",
                        key="auto_move_multi_prepare_btn",
                        use_container_width=True,
                    )
                    clear_auto_multi = col_clear_multi.button(
                        "Limpiar lista masiva",
                        key="auto_move_multi_clear_btn",
                        use_container_width=True,
                    )

                    if clear_auto_multi:
                        for state_key in (
                            "auto_move_multi_plan_rows",
                            "auto_move_multi_errors",
                            "auto_move_multi_group_map_by_scope",
                            "auto_move_multi_summary_rows",
                            "auto_move_multi_colegio_ids",
                        ):
                            st.session_state.pop(state_key, None)
                        st.rerun()

                    if run_prepare_auto_multi:
                        token = _get_shared_token()
                        if not token:
                            st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                            st.stop()
                        colegio_ids_multi = list(AUTO_MOVE_MULTI_ACTIVE_COLEGIO_IDS)

                        try:
                            progress_bar_multi = st.progress(
                                0,
                                text="Iniciando analisis de colegios...",
                            )
                            progress_status_box_multi = st.empty()
                            progress_metrics_box_multi = st.empty()
                            progress_table_box_multi = st.empty()
                            status_box_multi = st.empty()

                            def _on_status_multi(message: str) -> None:
                                msg = str(message or "").strip()
                                if msg:
                                    status_box_multi.info(msg)

                            def _on_progress_multi(payload: Dict[str, object]) -> None:
                                processed = int(payload.get("processed") or 0)
                                total = max(int(payload.get("total") or 0), 1)
                                current_colegio_id = int(payload.get("current_colegio_id") or 0)
                                current_status = str(payload.get("current_status") or "").strip()
                                plan_rows_total = int(payload.get("plan_rows_total") or 0)
                                errors_total = int(payload.get("errors_total") or 0)
                                summary_rows = payload.get("summary_rows") or []
                                progress_value = min(processed / total, 1.0)
                                progress_bar_multi.progress(
                                    progress_value,
                                    text=(
                                        f"Procesados {processed}/{total} colegios | "
                                        f"Ultimo: {current_colegio_id}"
                                    ),
                                )
                                progress_status_box_multi.info(
                                    f"Colegio {current_colegio_id} finalizado: {current_status}"
                                )
                                progress_metrics_box_multi.caption(
                                    "Avance acumulado: "
                                    f"{processed}/{total} colegios | "
                                    f"Cambios sugeridos: {plan_rows_total} | "
                                    f"Errores acumulados: {errors_total}"
                                )
                                if isinstance(summary_rows, list) and summary_rows:
                                    progress_table_box_multi.dataframe(
                                        pd.DataFrame(summary_rows),
                                        use_container_width=True,
                                        hide_index=True,
                                        height=260,
                                    )

                            with st.spinner("Preparando simulacion masiva por colegios..."):
                                simulation_multi = _build_auto_move_simulation_multi(
                                    token=token,
                                    colegio_ids=colegio_ids_multi,
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                    on_status=_on_status_multi,
                                    on_progress=_on_progress_multi,
                                )
                            progress_bar_multi.progress(
                                1.0,
                                text=(
                                    "Analisis completado: "
                                    f"{len(colegio_ids_multi)}/{len(colegio_ids_multi)} colegios"
                                ),
                            )
                            status_box_multi.empty()
                        except Exception as exc:  # pragma: no cover - UI
                            st.error(f"Error: {exc}")
                            st.stop()

                        st.session_state["auto_move_multi_plan_rows"] = (
                            simulation_multi.get("plan_rows") or []
                        )
                        st.session_state["auto_move_multi_errors"] = (
                            simulation_multi.get("errors") or []
                        )
                        st.session_state["auto_move_multi_group_map_by_scope"] = (
                            simulation_multi.get("group_map_by_scope") or {}
                        )
                        st.session_state["auto_move_multi_summary_rows"] = (
                            simulation_multi.get("colegio_summary_rows") or []
                        )
                        st.session_state["auto_move_multi_colegio_ids"] = colegio_ids_multi

                        total_plan_multi = len(st.session_state["auto_move_multi_plan_rows"])
                        colegios_ok_multi = int(simulation_multi.get("colegios_ok") or 0)
                        st.success(
                            "Analisis masivo listo. Colegios OK: {ok}/{total} | Cambios sugeridos: {changes}".format(
                                ok=colegios_ok_multi,
                                total=len(colegio_ids_multi),
                                changes=total_plan_multi,
                            )
                        )

                    summary_rows_multi_cached = (
                        st.session_state.get("auto_move_multi_summary_rows") or []
                    )
                    summary_preview_rows_multi = _build_auto_move_multi_summary_preview(
                        st.session_state.get("auto_move_multi_plan_rows") or []
                    )
                    if summary_preview_rows_multi:
                        st.markdown("**Resumen por colegio**")
                        st.dataframe(
                            pd.DataFrame(summary_preview_rows_multi),
                            use_container_width=True,
                            hide_index=True,
                        )

                    errors_multi_cached = st.session_state.get("auto_move_multi_errors") or []
                    if errors_multi_cached:
                        st.warning("Hubo errores consultando algunos colegios o secciones.")
                        st.write("\n".join(f"- {item}" for item in errors_multi_cached[:20]))
                        pending_multi = len(errors_multi_cached) - 20
                        if pending_multi > 0:
                            st.caption(f"... y {pending_multi} errores mas.")

                    plan_rows_multi_cached = st.session_state.get("auto_move_multi_plan_rows") or []
                    if not plan_rows_multi_cached:
                        st.caption("No hay lista masiva preparada aun.")
                        st.caption(
                            "Presiona 'Analizar colegios predefinidos' para iniciar."
                        )
                    else:
                        st.markdown("**Lista masiva de cambios para autorizar**")
                        group_map_by_scope_cached = (
                            st.session_state.get("auto_move_multi_group_map_by_scope") or {}
                        )
                        (
                            table_rows_multi,
                            destino_payload_by_option_multi,
                            destino_options_multi,
                        ) = _build_auto_move_multi_editor_state(
                            plan_rows=plan_rows_multi_cached,
                            group_map_by_scope=group_map_by_scope_cached,
                        )

                        if not destino_options_multi:
                            destino_options_multi = [""]

                        table_df_multi = pd.DataFrame(table_rows_multi)
                        edited_table_df_multi = st.data_editor(
                            table_df_multi,
                            key="auto_move_multi_plan_editor_table",
                            hide_index=True,
                            use_container_width=True,
                            disabled=[
                                "_plan_id",
                                "Colegio",
                                "Alumno | Grado y seccion",
                                "Referencia",
                            ],
                            column_config={
                                "_plan_id": None,
                                "Colegio": st.column_config.TextColumn("Colegio"),
                                "Alumno | Grado y seccion": st.column_config.TextColumn(
                                    "Alumno | Grado y seccion"
                                ),
                                "Referencia": st.column_config.TextColumn("Referencia"),
                                "Inactivar referencia": st.column_config.CheckboxColumn(
                                    "Inactivar referencia"
                                ),
                                "Nuevo grado y seccion": st.column_config.SelectboxColumn(
                                    "Nuevo grado y seccion",
                                    options=destino_options_multi,
                                    required=True,
                                ),
                            },
                        )

                        edited_rows_multi = (
                            edited_table_df_multi.to_dict("records")
                            if isinstance(edited_table_df_multi, pd.DataFrame)
                            else table_rows_multi
                        )
                        (
                            authorized_plans_multi,
                            removed_ref_ids_multi,
                            validation_errors_multi,
                        ) = _materialize_auto_move_multi_plans(
                            base_plan_rows=plan_rows_multi_cached,
                            edited_rows=edited_rows_multi,
                            destino_payload_by_option=destino_payload_by_option_multi,
                        )

                        st.caption(
                            "Cambios listos para guardar: {total} | Referencias quitadas: {removed}".format(
                                total=len(authorized_plans_multi),
                                removed=len(removed_ref_ids_multi),
                            )
                        )
                        if validation_errors_multi:
                            st.error(
                                "Hay destinos seleccionados que no pertenecen al mismo colegio."
                            )
                            st.write(
                                "\n".join(
                                    f"- {item}" for item in validation_errors_multi[:10]
                                )
                            )
                            pending_validation_multi = len(validation_errors_multi) - 10
                            if pending_validation_multi > 0:
                                st.caption(
                                    f"... y {pending_validation_multi} validaciones pendientes."
                                )

                        run_apply_auto_multi = st.button(
                            "Aplicar cambios autorizados de la lista",
                            key="auto_move_multi_apply_btn",
                            use_container_width=True,
                        )

                        if run_apply_auto_multi:
                            if validation_errors_multi:
                                st.error("Corrige los destinos marcados antes de guardar.")
                                st.stop()
                            if not authorized_plans_multi:
                                st.warning("No hay cambios autorizados para guardar.")
                                st.stop()
                            token = _get_shared_token()
                            if not token:
                                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                                st.stop()
                            try:
                                st.info(
                                    "Iniciando guardado masivo de cambios autorizados: "
                                    f"{len(authorized_plans_multi)} alumno(s)."
                                )
                                with st.spinner(
                                    "Guardando cambios por colegio (inactivar referencia, mover seccion y asignar clases)..."
                                ):
                                    summary_apply_multi, results_apply_multi = _apply_auto_move_changes(
                                        token=token,
                                        colegio_id=None,
                                        empresa_id=int(empresa_id),
                                        ciclo_id=int(ciclo_id),
                                        timeout=int(timeout),
                                        plan_rows=authorized_plans_multi,
                                    )
                            except Exception as exc:  # pragma: no cover - UI
                                st.error(f"No se pudieron guardar los cambios: {exc}")
                                st.stop()

                            inactivar_ok_multi = int(summary_apply_multi.get("inactivar_ok", 0))
                            inactivar_error_multi = int(summary_apply_multi.get("inactivar_error", 0))
                            mover_ok_multi = int(summary_apply_multi.get("mover_ok", 0))
                            mover_error_multi = int(summary_apply_multi.get("mover_error", 0))
                            asignar_ok_multi = int(summary_apply_multi.get("asignar_ok", 0))
                            asignar_error_multi = int(summary_apply_multi.get("asignar_error", 0))
                            asignar_skip_multi = int(summary_apply_multi.get("asignar_skip", 0))
                            total_errors_multi = (
                                inactivar_error_multi + mover_error_multi + asignar_error_multi
                            )

                            if total_errors_multi == 0:
                                st.success("Cambios masivos guardados correctamente.")
                            else:
                                st.warning("Guardado masivo completado con observaciones.")

                            st.caption(
                                "Resumen: "
                                f"Inactivar OK={inactivar_ok_multi}, ERROR={inactivar_error_multi} | "
                                f"Mover OK={mover_ok_multi}, ERROR={mover_error_multi} | "
                                f"Asignar clases OK={asignar_ok_multi}, ERROR={asignar_error_multi}, SKIP={asignar_skip_multi}"
                            )
                            if results_apply_multi:
                                details_multi = []
                                for item in results_apply_multi[:120]:
                                    if not isinstance(item, dict):
                                        continue
                                    details_multi.append(
                                        "- Colegio {colegio} | {alumno} | Inactivar: {inactivar} | Mover: {mover} | Asignar: {asignar}".format(
                                            colegio=str(item.get("Colegio") or "-"),
                                            alumno=str(item.get("Alumno pagado") or ""),
                                            inactivar=str(item.get("Inactivar no pagado") or ""),
                                            mover=str(item.get("Mover") or ""),
                                            asignar=str(item.get("Asignar clases") or ""),
                                        )
                                    )
                                if details_multi:
                                    st.markdown("**Detalle por alumno**")
                                    st.markdown("\n".join(details_multi))
                                if len(results_apply_multi) > 120:
                                    st.caption(
                                        f"... y {len(results_apply_multi) - 120} filas mas."
                                    )

with tab_crud_profesores:
    if not _restricted_sections_unlocked():
        _render_restricted_blur("CRUD Profesores", "profesores")
    else:
        st.subheader("CRUD Profesores")
        st.caption("Selecciona una funcion a la izquierda y trabaja en el panel derecho.")
        st.caption("Usando el token global configurado arriba.")
        colegio_id_raw = str(st.session_state.get("shared_colegio_id", "")).strip()
        ciclo_id = PROFESORES_CICLO_ID_DEFAULT
        timeout = 30
        loaded_profesores_manual_colegio_id = _safe_int(
            st.session_state.get("profesores_manual_colegio_id")
        )
        current_profesores_manual_colegio_id = _safe_int(colegio_id_raw)
        if (
            loaded_profesores_manual_colegio_id is not None
            and current_profesores_manual_colegio_id is not None
            and loaded_profesores_manual_colegio_id != current_profesores_manual_colegio_id
        ):
            for state_key in (
                "profesores_manual_rows",
                "profesores_manual_clases",
                "profesores_manual_summary",
                "profesores_manual_errors",
                "profesores_manual_colegio_id",
            ):
                st.session_state.pop(state_key, None)
        loaded_profesores_edit_colegio_id = _safe_int(
            st.session_state.get("profesores_edit_colegio_id")
        )
        current_profesores_edit_colegio_id = _safe_int(colegio_id_raw)
        if (
            loaded_profesores_edit_colegio_id is not None
            and current_profesores_edit_colegio_id is not None
            and loaded_profesores_edit_colegio_id != current_profesores_edit_colegio_id
        ):
            _clear_profesores_edit_state()
        profesores_nav_col, profesores_body_col = st.columns([1.15, 4.85], gap="large")
        with profesores_nav_col:
            profesores_crud_view = _render_crud_menu(
                "Funciones de profesores",
                [
                    ("bd", "BD", "Consulta, exporta y compara ProfesoresBD"),
                    ("manual", "Manual", "Asigna clases por docente"),
                    ("editar", "Editar", "Edita datos, login y password"),
                    ("base", "Base", "Genera Excel operativo"),
                    ("asignar", "Asignar", "Aplica cambios a clases"),
                ],
                state_key="profesores_crud_nav",
            )
        with profesores_body_col:
            if profesores_crud_view == "bd":
                with st.container(border=True):
                    st.markdown("**BD**")
                    st.caption(
                        "Consulta todos los profesores y genera un Excel con hojas Profesores_BD y Plantilla_Actualizada."
                    )
                    run_generar_bd = st.button(
                        "Generar Profesores_BD",
                        type="primary",
                        key="profesores_generar_bd",
                    )

                if run_generar_bd:
                    for state_key in (
                        "profesores_bd_rows",
                        "profesores_bd_excel",
                        "profesores_bd_excel_name",
                    ):
                        st.session_state.pop(state_key, None)
                    token = _get_shared_token()
                    if not token:
                        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                        st.stop()
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                        st.stop()
                    try:
                        data_bd, summary_bd, errores_bd = listar_profesores_bd_data(
                            token=token,
                            colegio_id=colegio_id_int,
                            empresa_id=DEFAULT_EMPRESA_ID,
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                        st.stop()

                    if errores_bd:
                        st.error("Errores al obtener profesores:")
                        _show_dataframe(errores_bd, use_container_width=True)
                    if not data_bd:
                        st.warning("No se encontraron profesores registrados.")
                    else:
                        output_bytes_bd = export_profesores_bd_excel(data_bd)
                        file_name_bd = build_profesores_bd_filename(colegio_id_int)
                        st.session_state["profesores_bd_rows"] = data_bd
                        st.session_state["profesores_bd_excel"] = output_bytes_bd
                        st.session_state["profesores_bd_excel_name"] = file_name_bd
                        st.success(
                            "Profesores_BD listo. Profesores: {profesores_total}, Errores: {consultas_error}.".format(
                                **summary_bd
                            )
                        )

                if st.session_state.get("profesores_bd_excel"):
                    st.download_button(
                        label="Descargar Profesores_BD",
                        data=st.session_state["profesores_bd_excel"],
                        file_name=st.session_state["profesores_bd_excel_name"],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="profesores_bd_excel_download",
                    )
                if st.session_state.get("profesores_bd_rows"):
                    _show_dataframe(st.session_state["profesores_bd_rows"], use_container_width=True)

                with st.container(border=True):
                    st.markdown("**Comparar**")
                    st.caption(
                        "Sube el Excel con Profesores_BD y Plantilla_Actualizada para detectar profesores registrados y separar los que se deben crear."
                    )
                    if PROFESORES_COMPARE_IMPORT_ERROR:
                        st.error(
                            "La comparacion de profesores no esta disponible en este despliegue: "
                            f"{PROFESORES_COMPARE_IMPORT_ERROR}"
                        )
                    uploaded_profesores_compare = st.file_uploader(
                        "Excel con Profesores_BD y Plantilla_Actualizada",
                        type=["xlsx"],
                        key="profesores_compare_excel",
                        disabled=bool(PROFESORES_COMPARE_IMPORT_ERROR),
                    )
                    run_compare_profesores = st.button(
                        "Analizar coincidencias",
                        type="primary",
                        key="profesores_compare_run",
                        disabled=bool(PROFESORES_COMPARE_IMPORT_ERROR),
                    )

                if run_compare_profesores:
                    for state_key in (
                        "profesores_compare_rows",
                        "profesores_compare_summary",
                        "profesores_compare_source_name",
                        "profesores_compare_editor",
                    ):
                        st.session_state.pop(state_key, None)
                    if not uploaded_profesores_compare:
                        st.error("Sube un Excel con las hojas Profesores_BD y Plantilla_Actualizada.")
                        st.stop()

                    suffix = Path(uploaded_profesores_compare.name).suffix or ".xlsx"
                    tmp_path = None
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                            tmp.write(uploaded_profesores_compare.read())
                            tmp_path = Path(tmp.name)
                        compare_rows, compare_summary = compare_profesores_bd_excel(
                            excel_path=tmp_path,
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                        st.stop()
                    finally:
                        if tmp_path:
                            try:
                                tmp_path.unlink()
                            except OSError:
                                pass

                    st.session_state["profesores_compare_rows"] = compare_rows
                    st.session_state["profesores_compare_summary"] = compare_summary
                    st.session_state["profesores_compare_source_name"] = str(
                        uploaded_profesores_compare.name or "profesores.xlsx"
                    )
                    st.success(
                        "Comparacion lista. BD: {bd_total}, Actualizada: {actualizada_total}, "
                        "Coincidencias: {coincidencias_total}, Sin referencia: {sin_referencia_total}.".format(
                            **compare_summary
                        )
                    )

                compare_rows_cached = st.session_state.get("profesores_compare_rows") or []
                compare_summary_cached = st.session_state.get("profesores_compare_summary") or {}
                compare_source_name_cached = str(
                    st.session_state.get("profesores_compare_source_name") or "profesores.xlsx"
                )
                if compare_rows_cached:
                    matched_rows = [
                        row for row in compare_rows_cached if bool(row.get("_tiene_referencia"))
                    ]
                    unmatched_rows = [
                        row for row in compare_rows_cached if not bool(row.get("_tiene_referencia"))
                    ]
                    st.info(
                        "Coincidencias detectadas: {coincidencias_total} | Sin referencia BD: {sin_referencia_total}".format(
                            **compare_summary_cached
                        )
                    )

                    edited_match_rows: List[Dict[str, object]] = []
                    if matched_rows:
                        st.markdown("**Vista previa de coincidencias**")
                        edited_matches_df = st.data_editor(
                            pd.DataFrame(
                                [
                                    {
                                        "Profesor Colegio": row.get("Profesor Colegio", ""),
                                        "Profesor referencia de la BD": row.get(
                                            "Profesor referencia de la BD", ""
                                        ),
                                        "Coincidencia por": row.get("Coincidencia por", ""),
                                        "Usar referencia BD": bool(
                                            row.get("Usar referencia BD", False)
                                        ),
                                    }
                                    for row in matched_rows
                                ]
                            ),
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Usar referencia BD": st.column_config.CheckboxColumn(
                                    "Usar referencia BD",
                                    help="Desmarca para incluir este profesor en el Excel de creacion.",
                                    default=True,
                                ),
                            },
                            disabled=[
                                "Profesor Colegio",
                                "Profesor referencia de la BD",
                                "Coincidencia por",
                            ],
                            key="profesores_compare_editor",
                        )
                        edited_match_rows = edited_matches_df.to_dict("records")
                    else:
                        st.caption("No se detectaron coincidencias contra la BD.")

                    base_rows: List[Dict[str, object]] = []
                    create_rows: List[Dict[str, object]] = [dict(row) for row in unmatched_rows]
                    for index, base_row in enumerate(matched_rows):
                        edited_row = (
                            edited_match_rows[index]
                            if index < len(edited_match_rows)
                            else {"Usar referencia BD": True}
                        )
                        if bool(edited_row.get("Usar referencia BD", True)):
                            base_rows.append(dict(base_row))
                        else:
                            create_rows.append(dict(base_row))

                    base_rows.sort(
                        key=lambda row: (
                            str(row.get("Apellido Paterno") or "").upper(),
                            str(row.get("Apellido Materno") or "").upper(),
                            str(row.get("Nombre") or "").upper(),
                            str(row.get("DNI") or ""),
                        )
                    )
                    create_rows.sort(
                        key=lambda row: (
                            str(row.get("Apellido Paterno") or "").upper(),
                            str(row.get("Apellido Materno") or "").upper(),
                            str(row.get("Nombre") or "").upper(),
                            str(row.get("DNI") or ""),
                        )
                    )

                    st.markdown("**Profesores encontrados para base**")
                    if base_rows:
                        base_preview_rows = []
                        for row in base_rows:
                            reference_row = row.get("_reference_base_record")
                            if not isinstance(reference_row, dict):
                                continue
                            base_preview_rows.append(
                                {
                                    "Id": reference_row.get("Id", ""),
                                    "Nombre": reference_row.get("Nombre", ""),
                                    "Apellido Paterno": reference_row.get(
                                        "Apellido Paterno", ""
                                    ),
                                    "Apellido Materno": reference_row.get(
                                        "Apellido Materno", ""
                                    ),
                                    "Estado": reference_row.get("Estado", ""),
                                    "Sexo": reference_row.get("Sexo", ""),
                                    "DNI": reference_row.get("DNI", ""),
                                    "E-mail": reference_row.get("E-mail", ""),
                                    "Login": reference_row.get("Login", ""),
                                    "Coincidencia por": row.get("Coincidencia por", ""),
                                }
                            )
                        _show_dataframe(base_preview_rows, use_container_width=True)
                        st.download_button(
                            label="Descargar profesores base",
                            data=export_profesores_base_excel(base_rows),
                            file_name=build_profesores_base_filename(compare_source_name_cached),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="profesores_base_download",
                        )
                    else:
                        st.caption("No hay profesores registrados para llevar a Profesores_clases.")

                    st.markdown("**Profesores a crear**")
                    if create_rows:
                        create_preview_rows = [
                            {
                                "Nombre": row.get("Nombre", ""),
                                "Apellido Paterno": row.get("Apellido Paterno", ""),
                                "Apellido Materno": row.get("Apellido Materno", ""),
                                "DNI": row.get("DNI", ""),
                                "E-mail": row.get("E-mail", ""),
                                "Login": row.get("Login", ""),
                            }
                            for row in create_rows
                        ]
                        _show_dataframe(create_preview_rows, use_container_width=True)
                        st.download_button(
                            label="Descargar profesores a crear",
                            data=export_profesores_crear_excel(create_rows),
                            file_name=build_profesores_crear_filename(compare_source_name_cached),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="profesores_compare_download",
                        )
                    else:
                        st.caption(
                            "No hay profesores para crear. Si quieres forzar uno, desmarca 'Usar referencia BD'."
                        )

                with st.container(border=True):
                    st.markdown("**Cruce directo contra sistema**")
                    st.caption(
                        "Sube el Excel simple de profesores para cruzarlo directo contra Pegasus usando profesoresByFilters."
                    )
                    st.caption(
                        "Puedes dejar el match automatico o seleccionar manualmente cualquier docente del sistema como referencia."
                    )
                    if PROFESORES_COMPARE_IMPORT_ERROR:
                        st.error(
                            "El cruce directo contra sistema no esta disponible en este despliegue: "
                            f"{PROFESORES_COMPARE_IMPORT_ERROR}"
                        )
                    uploaded_profesores_compare_system = st.file_uploader(
                        "Excel simple de profesores",
                        type=["xlsx"],
                        key="profesores_compare_system_excel",
                        disabled=bool(PROFESORES_COMPARE_IMPORT_ERROR),
                    )
                    run_compare_profesores_system = st.button(
                        "Analizar contra sistema",
                        type="primary",
                        key="profesores_compare_system_run",
                        disabled=bool(PROFESORES_COMPARE_IMPORT_ERROR),
                    )

                if run_compare_profesores_system:
                    for state_key in (
                        "profesores_compare_system_rows",
                        "profesores_compare_system_summary",
                        "profesores_compare_system_source_name",
                        "profesores_compare_system_editor",
                        "profesores_compare_system_errors",
                        "profesores_compare_system_catalog",
                    ):
                        st.session_state.pop(state_key, None)
                    if not uploaded_profesores_compare_system:
                        st.error("Sube un Excel simple de profesores.")
                        st.stop()

                    token = _get_shared_token()
                    if not token:
                        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                        st.stop()
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                        st.stop()

                    suffix = Path(uploaded_profesores_compare_system.name).suffix or ".xlsx"
                    tmp_path = None
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                            tmp.write(uploaded_profesores_compare_system.read())
                            tmp_path = Path(tmp.name)
                        (
                            compare_rows_system,
                            compare_summary_system,
                            compare_errors_system,
                            compare_catalog_system,
                        ) = (
                            compare_profesores_sistema_excel(
                                token=token,
                                colegio_id=colegio_id_int,
                                excel_path=tmp_path,
                                empresa_id=DEFAULT_EMPRESA_ID,
                                ciclo_id=int(ciclo_id),
                                timeout=int(timeout),
                            )
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                        st.stop()
                    finally:
                        if tmp_path:
                            try:
                                tmp_path.unlink()
                            except OSError:
                                pass

                    st.session_state["profesores_compare_system_rows"] = compare_rows_system
                    st.session_state["profesores_compare_system_summary"] = compare_summary_system
                    st.session_state["profesores_compare_system_source_name"] = str(
                        uploaded_profesores_compare_system.name or "profesores.xlsx"
                    )
                    st.session_state["profesores_compare_system_errors"] = (
                        compare_errors_system
                    )
                    st.session_state["profesores_compare_system_catalog"] = (
                        compare_catalog_system
                    )
                    st.success(
                        "Cruce directo listo. Sistema: {sistema_total}, Excel: {excel_total}, "
                        "Coincidencias: {coincidencias_total}, Sin referencia: {sin_referencia_total}, "
                        "Observaciones API: {consultas_error}.".format(
                            **compare_summary_system
                        )
                    )

                compare_system_rows_cached = (
                    st.session_state.get("profesores_compare_system_rows") or []
                )
                compare_system_summary_cached = (
                    st.session_state.get("profesores_compare_system_summary") or {}
                )
                compare_system_source_name_cached = str(
                    st.session_state.get("profesores_compare_system_source_name")
                    or "profesores.xlsx"
                )
                compare_system_catalog_cached = (
                    st.session_state.get("profesores_compare_system_catalog") or []
                )
                compare_system_errors_cached = (
                    st.session_state.get("profesores_compare_system_errors") or []
                )
                if compare_system_errors_cached:
                    st.error("Observaciones al consultar profesores del sistema:")
                    _show_dataframe(compare_system_errors_cached, use_container_width=True)

                if compare_system_rows_cached:
                    st.info(
                        "Cruce directo -> Coincidencias: {coincidencias_total} | Sin referencia sistema: {sin_referencia_total}".format(
                            **compare_system_summary_cached
                        )
                    )
                    if compare_system_catalog_cached:
                        st.markdown("**Profesores disponibles en sistema**")
                        _show_dataframe(
                            [
                                {
                                    "Id": row.get("Id", ""),
                                    "Nombre": row.get("Nombre", ""),
                                    "Apellido Paterno": row.get("Apellido Paterno", ""),
                                    "Apellido Materno": row.get("Apellido Materno", ""),
                                    "Estado": row.get("Estado", ""),
                                    "DNI": row.get("DNI", ""),
                                    "E-mail": row.get("E-mail", ""),
                                    "Login": row.get("Login", ""),
                                    "Inicial": row.get("Inicial", ""),
                                    "Primaria": row.get("Primaria", ""),
                                    "Secundaria": row.get("Secundaria", ""),
                                    "Referencia": row.get("label", ""),
                                }
                                for row in compare_system_catalog_cached
                            ],
                            use_container_width=True,
                        )

                    reference_options = [""]
                    reference_by_label = {}
                    for item in compare_system_catalog_cached:
                        label = str(item.get("label") or "").strip()
                        if not label or label in reference_by_label:
                            continue
                        reference_options.append(label)
                        reference_by_label[label] = item

                    st.markdown("**Cruce y referencia manual**")
                    edited_system_rows_df = st.data_editor(
                        pd.DataFrame(
                            [
                                {
                                    "Profesor Colegio": row.get("Profesor Colegio", ""),
                                    "Referencia sistema": row.get("Referencia sistema", ""),
                                    "Coincidencia por": row.get("Coincidencia por", ""),
                                }
                                for row in compare_system_rows_cached
                            ]
                        ),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Referencia sistema": st.column_config.SelectboxColumn(
                                "Referencia sistema",
                                options=reference_options,
                                required=False,
                                help=(
                                    "Si no hubo match automatico, puedes escoger cualquier docente "
                                    "del sistema para actualizarlo con los datos del Excel."
                                ),
                            ),
                        },
                        disabled=["Profesor Colegio", "Coincidencia por"],
                        key="profesores_compare_system_editor",
                    )
                    edited_system_rows = edited_system_rows_df.to_dict("records")

                    base_system_rows: List[Dict[str, object]] = []
                    create_system_rows: List[Dict[str, object]] = []
                    for index, excel_row in enumerate(compare_system_rows_cached):
                        if index < len(edited_system_rows):
                            selected_reference = str(
                                edited_system_rows[index].get("Referencia sistema") or ""
                            ).strip()
                        else:
                            selected_reference = str(
                                excel_row.get("Referencia sistema") or ""
                            ).strip()
                        selected_catalog = reference_by_label.get(selected_reference)
                        reference_base_record = {}
                        if isinstance(selected_catalog, dict):
                            reference_base_record = selected_catalog.get(
                                "_reference_base_record"
                            ) or {}
                        if not isinstance(reference_base_record, dict) or not reference_base_record:
                            create_system_rows.append(dict(excel_row))
                            continue

                        merged_reference = merge_profesores_reference_base_record(
                            reference_base_record,
                            excel_row,
                        )
                        output_row = dict(excel_row)
                        output_row["_tiene_referencia"] = True
                        output_row["_reference_base_record"] = merged_reference
                        output_row["Referencia sistema"] = selected_reference
                        output_row["Profesor referencia del sistema"] = selected_reference
                        original_reference = str(
                            excel_row.get("Referencia sistema") or ""
                        ).strip()
                        if selected_reference != original_reference:
                            output_row["Coincidencia por"] = "Manual"
                        base_system_rows.append(output_row)

                    base_system_rows.sort(
                        key=lambda row: (
                            str(row.get("Apellido Paterno") or "").upper(),
                            str(row.get("Apellido Materno") or "").upper(),
                            str(row.get("Nombre") or "").upper(),
                            str(row.get("DNI") or ""),
                        )
                    )
                    create_system_rows.sort(
                        key=lambda row: (
                            str(row.get("Apellido Paterno") or "").upper(),
                            str(row.get("Apellido Materno") or "").upper(),
                            str(row.get("Nombre") or "").upper(),
                            str(row.get("DNI") or ""),
                        )
                    )

                    st.markdown("**Profesores encontrados en sistema para base**")
                    if base_system_rows:
                        base_system_preview_rows = []
                        for row in base_system_rows:
                            reference_row = row.get("_reference_base_record")
                            if not isinstance(reference_row, dict):
                                continue
                            base_system_preview_rows.append(
                                {
                                    "Id": reference_row.get("Id", ""),
                                    "Nombre": reference_row.get("Nombre", ""),
                                    "Apellido Paterno": reference_row.get(
                                        "Apellido Paterno", ""
                                    ),
                                    "Apellido Materno": reference_row.get(
                                        "Apellido Materno", ""
                                    ),
                                    "Estado": reference_row.get("Estado", ""),
                                    "Sexo": reference_row.get("Sexo", ""),
                                    "DNI": reference_row.get("DNI", ""),
                                    "E-mail": reference_row.get("E-mail", ""),
                                    "Login": reference_row.get("Login", ""),
                                    "Inicial": reference_row.get("Inicial", ""),
                                    "Primaria": reference_row.get("Primaria", ""),
                                    "Secundaria": reference_row.get("Secundaria", ""),
                                    "Coincidencia por": row.get("Coincidencia por", ""),
                                }
                            )
                        _show_dataframe(base_system_preview_rows, use_container_width=True)
                        st.download_button(
                            label="Descargar profesores base desde sistema",
                            data=export_profesores_base_excel(base_system_rows),
                            file_name=build_profesores_base_filename(
                                compare_system_source_name_cached
                            ),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="profesores_base_system_download",
                        )
                        st.caption(
                            "Este base conserva el Id del sistema y arrastra los valores nuevos del Excel sobre la referencia elegida."
                        )
                    else:
                        st.caption("No hay profesores encontrados en sistema para base.")

                    st.markdown("**Profesores a crear desde el Excel**")
                    if create_system_rows:
                        create_system_preview_rows = [
                            {
                                "Nombre": row.get("Nombre", ""),
                                "Apellido Paterno": row.get("Apellido Paterno", ""),
                                "Apellido Materno": row.get("Apellido Materno", ""),
                                "DNI": row.get("DNI", ""),
                                "E-mail": row.get("E-mail", ""),
                                "Login": row.get("Login", ""),
                                "Inicial": row.get("Inicial", ""),
                                "Primaria": row.get("Primaria", ""),
                                "Secundaria": row.get("Secundaria", ""),
                            }
                            for row in create_system_rows
                        ]
                        _show_dataframe(create_system_preview_rows, use_container_width=True)
                        st.download_button(
                            label="Descargar profesores a crear",
                            data=export_profesores_crear_excel(create_system_rows),
                            file_name=build_profesores_crear_filename(
                                compare_system_source_name_cached
                            ),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="profesores_compare_system_download",
                        )
                    else:
                        st.caption(
                            "No hay profesores para crear. Si quieres forzar uno, desmarca 'Usar referencia sistema'."
                        )
            if profesores_crud_view == "manual":
                st.subheader("Asignacion manual de clases")
                if PROFESORES_MANUAL_IMPORT_ERROR:
                    st.error(
                        "La asignacion manual no esta disponible en este despliegue: "
                        f"{PROFESORES_MANUAL_IMPORT_ERROR}"
                    )

                col_load_manual, col_clear_manual = st.columns([2, 1], gap="small")
                run_manual_load = col_load_manual.button(
                    "Cargar docentes y clases",
                    type="primary",
                    key="profesores_manual_load",
                    use_container_width=True,
                    disabled=bool(PROFESORES_MANUAL_IMPORT_ERROR),
                )
                clear_manual = col_clear_manual.button(
                    "Limpiar",
                    key="profesores_manual_clear",
                    use_container_width=True,
                )

                if clear_manual:
                    for state_key in (
                        "profesores_manual_rows",
                        "profesores_manual_clases",
                        "profesores_manual_summary",
                        "profesores_manual_errors",
                        "profesores_manual_colegio_id",
                        "profesores_manual_selected",
                        "profesores_manual_search",
                        "profesores_manual_confirm_apply",
                    ):
                        st.session_state.pop(state_key, None)
                    st.rerun()

                if run_manual_load:
                    token = _get_shared_token()
                    if not token:
                        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                        st.stop()
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                        st.stop()

                    progress = st.progress(0)
                    status = st.empty()

                    def _manual_load_progress(
                        phase: str, current: int, total: int, message: str
                    ) -> None:
                        percent = int((current / total) * 100) if total else 0
                        progress.progress(percent)
                        status.write(f"{phase}: {message} ({current}/{total})")

                    try:
                        profesores_manual_rows, profesores_manual_clases, profesores_manual_summary, profesores_manual_errors = listar_profesores_clases_panel_data(
                            token=token,
                            colegio_id=colegio_id_int,
                            empresa_id=DEFAULT_EMPRESA_ID,
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                            on_progress=_manual_load_progress,
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                        st.stop()
                    finally:
                        progress.empty()
                        status.empty()

                    st.session_state["profesores_manual_rows"] = profesores_manual_rows
                    st.session_state["profesores_manual_clases"] = profesores_manual_clases
                    st.session_state["profesores_manual_summary"] = profesores_manual_summary
                    st.session_state["profesores_manual_errors"] = profesores_manual_errors
                    st.session_state["profesores_manual_colegio_id"] = int(colegio_id_int)

                profesores_manual_rows_cached = st.session_state.get("profesores_manual_rows") or []
                profesores_manual_clases_cached = (
                    st.session_state.get("profesores_manual_clases") or []
                )
                profesores_manual_summary_cached = (
                    st.session_state.get("profesores_manual_summary") or {}
                )
                profesores_manual_errors_cached = (
                    st.session_state.get("profesores_manual_errors") or []
                )

                if profesores_manual_summary_cached:
                    st.caption(
                        "Docentes: {profesores_total} | Clases: {clases_total} | Staff con error: {staff_consultas_error}".format(
                            **profesores_manual_summary_cached
                        )
                    )

                if profesores_manual_errors_cached:
                    with st.expander(
                        f"Errores de carga de staff ({len(profesores_manual_errors_cached)})"
                    ):
                        _show_dataframe(
                            profesores_manual_errors_cached, use_container_width=True
                        )

                if profesores_manual_rows_cached and profesores_manual_clases_cached:
                    search_profesor_manual = st.text_input(
                        "Buscar profesor",
                        key="profesores_manual_search",
                        placeholder="Nombre, login, DNI o persona ID",
                    )
                    search_profesor_manual_norm = _normalize_plain_text(search_profesor_manual)
                    filtered_profesores_manual = []
                    for row in profesores_manual_rows_cached:
                        haystack = " ".join(
                            [
                                str(row.get("nombre") or ""),
                                str(row.get("login") or ""),
                                str(row.get("dni") or ""),
                                str(row.get("email") or ""),
                                str(row.get("persona_id") or ""),
                            ]
                        )
                        if (
                            not search_profesor_manual_norm
                            or search_profesor_manual_norm in _normalize_plain_text(haystack)
                        ):
                            filtered_profesores_manual.append(row)

                    profesores_manual_preview = [
                        {
                            "Persona ID": row.get("persona_id", ""),
                            "Docente": row.get("nombre", ""),
                            "Login": row.get("login", ""),
                            "DNI": row.get("dni", ""),
                            "Estado": row.get("estado", ""),
                            "Clases actuales": row.get("clases_actuales_count", 0),
                        }
                        for row in filtered_profesores_manual
                    ]
                    if profesores_manual_preview:
                        _show_dataframe(profesores_manual_preview, use_container_width=True)
                    else:
                        st.caption("No hay docentes para ese filtro.")

                    profesores_manual_by_id = {
                        int(row["persona_id"]): row
                        for row in filtered_profesores_manual
                        if _safe_int(row.get("persona_id")) is not None
                    }
                    clases_manual_by_id = {
                        int(row["clase_id"]): row
                        for row in profesores_manual_clases_cached
                        if _safe_int(row.get("clase_id")) is not None
                    }

                    if profesores_manual_by_id:
                        profesores_manual_options = sorted(profesores_manual_by_id.keys())
                        selected_profesor_cached = _safe_int(
                            st.session_state.get("profesores_manual_selected")
                        )
                        if selected_profesor_cached not in profesores_manual_options:
                            st.session_state["profesores_manual_selected"] = int(
                                profesores_manual_options[0]
                            )
                        selected_profesor_manual = st.selectbox(
                            "Profesor",
                            options=profesores_manual_options,
                            format_func=lambda persona_id: str(
                                profesores_manual_by_id.get(int(persona_id), {}).get("label")
                                or f"Persona {persona_id}"
                            ),
                            key="profesores_manual_selected",
                        )
                        profesor_manual_row = profesores_manual_by_id.get(
                            int(selected_profesor_manual)
                        )
                    else:
                        profesor_manual_row = None

                    if profesor_manual_row:
                        cols_manual_left, cols_manual_right = st.columns(
                            [1.5, 2.5], gap="large"
                        )
                        with cols_manual_left:
                            st.markdown(
                                "\n".join(
                                    [
                                        f"- ID: {profesor_manual_row.get('persona_id', '')}",
                                        f"- Nombre: {profesor_manual_row.get('nombre', '') or '-'}",
                                        f"- Login: {profesor_manual_row.get('login', '') or '-'}",
                                        f"- DNI: {profesor_manual_row.get('dni', '') or '-'}",
                                        f"- Estado: {profesor_manual_row.get('estado', '') or '-'}",
                                        f"- Clases actuales: {profesor_manual_row.get('clases_actuales_count', 0)}",
                                    ]
                                )
                            )
                            clases_actuales_txt = profesor_manual_row.get("clases_actuales") or []
                            if clases_actuales_txt:
                                st.markdown(
                                    "\n".join(
                                        f"- {item}" for item in clases_actuales_txt[:20]
                                    )
                                )
                                if len(clases_actuales_txt) > 20:
                                    st.caption(
                                        f"... y {len(clases_actuales_txt) - 20} mas."
                                    )

                        with cols_manual_right:
                            clases_options = sorted(clases_manual_by_id.keys())
                            current_manual_class_ids = [
                                int(item)
                                for item in profesor_manual_row.get("clase_ids_actuales", [])
                                if int(item) in clases_manual_by_id
                            ]
                            selected_manual_class_ids = st.multiselect(
                                "Clases",
                                options=clases_options,
                                default=current_manual_class_ids,
                                format_func=lambda clase_id: str(
                                    clases_manual_by_id.get(int(clase_id), {}).get("clase_label")
                                    or f"Clase {clase_id}"
                                ),
                                key=f"profesores_manual_clases_{int(profesor_manual_row['persona_id'])}",
                            )
                            confirm_manual_apply = st.checkbox(
                                "Confirmar cambios",
                                value=False,
                                key="profesores_manual_confirm_apply",
                            )
                            run_manual_apply = st.button(
                                "Aplicar cambios",
                                type="primary",
                                key="profesores_manual_apply",
                                use_container_width=True,
                                disabled=bool(PROFESORES_MANUAL_IMPORT_ERROR),
                            )

                            if run_manual_apply:
                                token = _get_shared_token()
                                if not token:
                                    st.error(
                                        "Falta el token. Configura el token global o PEGASUS_TOKEN."
                                    )
                                    st.stop()
                                if not confirm_manual_apply:
                                    st.error("Debes confirmar antes de aplicar cambios.")
                                    st.stop()

                                progress = st.progress(0)
                                status = st.empty()

                                def _manual_assign_progress(
                                    current: int, total: int, message: str
                                ) -> None:
                                    percent = int((current / total) * 100) if total else 0
                                    progress.progress(percent)
                                    status.write(f"{message} ({current}/{total})")

                                target_manual_nivel_ids = sorted(
                                    {
                                        int(_safe_int(clases_manual_by_id.get(int(clase_id), {}).get("nivel_id")))
                                        for clase_id in selected_manual_class_ids
                                        if _safe_int(clase_id) is not None
                                        and _safe_int(
                                            clases_manual_by_id.get(int(clase_id), {}).get("nivel_id")
                                        )
                                        is not None
                                    }
                                )

                                try:
                                    manual_summary, manual_warnings, manual_results = asignar_clases_profesor_manual(
                                        token=token,
                                        persona_id=int(profesor_manual_row["persona_id"]),
                                        clase_ids=selected_manual_class_ids,
                                        current_clase_ids=current_manual_class_ids,
                                        nivel_ids=target_manual_nivel_ids,
                                        colegio_id=int(_parse_colegio_id(colegio_id_raw)),
                                        empresa_id=DEFAULT_EMPRESA_ID,
                                        ciclo_id=int(ciclo_id),
                                        timeout=int(timeout),
                                        dry_run=False,
                                        on_progress=_manual_assign_progress,
                                    )
                                except Exception as exc:  # pragma: no cover - UI
                                    st.error(f"Error: {exc}")
                                    st.stop()
                                finally:
                                    progress.empty()
                                    status.empty()

                                st.success(
                                    "Niveles: {niveles_actualizados}/{niveles_total} | Asignadas: {asignadas} | Quitadas: {desasignadas} | Ya asignadas: {ya_asignadas} | Errores: {errores_api}".format(
                                        **manual_summary
                                    )
                                )
                                if manual_warnings:
                                    st.caption(" | ".join(str(item) for item in manual_warnings))
                                if manual_results:
                                    manual_results_display = []
                                    for item in manual_results:
                                        clase_id_result = _safe_int(item.get("clase_id"))
                                        clase_info = (
                                            clases_manual_by_id.get(int(clase_id_result))
                                            if clase_id_result is not None
                                            else {}
                                        )
                                        manual_results_display.append(
                                            {
                                                "Clase ID": clase_id_result or "",
                                                "Clase": clase_info.get("clase_label", ""),
                                                "Estado": item.get("estado", ""),
                                                "Detalle": item.get("detalle", ""),
                                            }
                                        )
                                    _show_dataframe(
                                        manual_results_display, use_container_width=True
                                    )

                                clases_confirmadas_asignadas = set(
                                    int(item.get("clase_id"))
                                    for item in manual_results
                                    if item.get("estado") in {"asignada", "ya_asignada"}
                                    and _safe_int(item.get("clase_id")) is not None
                                )
                                clases_confirmadas_quitadas = set(
                                    int(item.get("clase_id"))
                                    for item in manual_results
                                    if item.get("estado") in {"desasignada", "ya_desasignada"}
                                    and _safe_int(item.get("clase_id")) is not None
                                )
                                if clases_confirmadas_asignadas or clases_confirmadas_quitadas:
                                    for row in st.session_state.get(
                                        "profesores_manual_rows", []
                                    ):
                                        if int(row.get("persona_id") or 0) != int(
                                            profesor_manual_row["persona_id"]
                                        ):
                                            continue
                                        current_ids = {
                                            int(item)
                                            for item in row.get("clase_ids_actuales", [])
                                            if _safe_int(item) is not None
                                        }
                                        current_ids.difference_update(
                                            clases_confirmadas_quitadas
                                        )
                                        current_ids.update(clases_confirmadas_asignadas)
                                        row["clase_ids_actuales"] = sorted(current_ids)
                                        current_labels = []
                                        for clase_id_current in row["clase_ids_actuales"]:
                                            clase_info = clases_manual_by_id.get(
                                                int(clase_id_current), {}
                                            )
                                            clase_label = str(
                                                clase_info.get("clase_label") or ""
                                            ).strip()
                                            if clase_label and clase_label not in current_labels:
                                                current_labels.append(clase_label)
                                        row["clases_actuales"] = sorted(current_labels)
                                        row["clases_actuales_count"] = len(
                                            row["clase_ids_actuales"]
                                        )
                                    for row in st.session_state.get(
                                        "profesores_manual_clases", []
                                    ):
                                        clase_id_row = _safe_int(row.get("clase_id"))
                                        if clase_id_row is None:
                                            continue
                                        current_staff_ids = {
                                            int(item)
                                            for item in row.get("staff_persona_ids", [])
                                            if _safe_int(item) is not None
                                        }
                                        if int(clase_id_row) in clases_confirmadas_quitadas:
                                            current_staff_ids.discard(
                                                int(profesor_manual_row["persona_id"])
                                            )
                                        if int(clase_id_row) in clases_confirmadas_asignadas:
                                            current_staff_ids.add(
                                                int(profesor_manual_row["persona_id"])
                                            )
                                        row["staff_persona_ids"] = sorted(current_staff_ids)
                                        row["staff_count"] = len(current_staff_ids)
            if profesores_crud_view == "editar":
                st.subheader("Editar docente")
                st.caption(
                    "Lista docentes del colegio, carga el detalle por nivel y actualiza datos base, login y password."
                )
                st.caption(
                    "Puedes cambiar solo el login o enviar tambien una nueva password."
                )

                notice_profesores_edit = st.session_state.pop(
                    "profesores_edit_notice", None
                )
                if isinstance(notice_profesores_edit, dict):
                    notice_type = str(
                        notice_profesores_edit.get("type") or ""
                    ).strip().lower()
                    notice_message = str(
                        notice_profesores_edit.get("message") or ""
                    ).strip()
                    if notice_message:
                        if notice_type == "success":
                            st.success(notice_message)
                        elif notice_type == "warning":
                            st.warning(notice_message)
                        elif notice_type == "error":
                            st.error(notice_message)
                        else:
                            st.info(notice_message)

                col_load_edit, col_clear_edit = st.columns([2, 1], gap="small")
                run_edit_load = col_load_edit.button(
                    "Cargar docentes",
                    type="primary",
                    key="profesores_edit_load",
                    use_container_width=True,
                )
                clear_edit = col_clear_edit.button(
                    "Limpiar",
                    key="profesores_edit_clear",
                    use_container_width=True,
                )

                if clear_edit:
                    _clear_profesores_edit_state()
                    st.rerun()

                if run_edit_load:
                    token = _get_shared_token()
                    if not token:
                        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                        st.stop()
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                        st.stop()
                    try:
                        with st.spinner("Cargando docentes del colegio..."):
                            profesores_edit_rows, profesores_edit_summary, profesores_edit_errors = listar_profesores_filters_data(
                                token=token,
                                colegio_id=colegio_id_int,
                                empresa_id=DEFAULT_EMPRESA_ID,
                                ciclo_id=int(ciclo_id),
                                timeout=int(timeout),
                            )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                        st.stop()

                    for state_key in (
                        "profesores_edit_selected_persona_id",
                        "profesores_edit_selected_nivel_id",
                        "profesores_edit_loaded_persona_id",
                        "profesores_edit_loaded_nivel_id",
                        "profesores_edit_detail",
                        "profesores_edit_fetch_error",
                        "profesores_edit_nombre",
                        "profesores_edit_apellido_paterno",
                        "profesores_edit_apellido_materno",
                        "profesores_edit_sexo",
                        "profesores_edit_dni",
                        "profesores_edit_email",
                        "profesores_edit_login",
                        "profesores_edit_original_login",
                        "profesores_edit_password",
                        "profesores_edit_pending_detail_refresh",
                    ):
                        st.session_state.pop(state_key, None)
                    st.session_state["profesores_edit_rows"] = profesores_edit_rows
                    st.session_state["profesores_edit_summary"] = profesores_edit_summary
                    st.session_state["profesores_edit_errors"] = profesores_edit_errors
                    st.session_state["profesores_edit_colegio_id"] = int(colegio_id_int)

                profesores_edit_rows = st.session_state.get("profesores_edit_rows") or []
                profesores_edit_summary = (
                    st.session_state.get("profesores_edit_summary") or {}
                )
                profesores_edit_errors = st.session_state.get("profesores_edit_errors") or []
                profesores_edit_pending_refresh = st.session_state.pop(
                    "profesores_edit_pending_detail_refresh", None
                )
                if isinstance(profesores_edit_pending_refresh, dict):
                    pending_detail = profesores_edit_pending_refresh.get("detail")
                    pending_persona_id = _safe_int(
                        profesores_edit_pending_refresh.get("persona_id")
                    )
                    pending_nivel_id = _safe_int(
                        profesores_edit_pending_refresh.get("nivel_id")
                    )
                    pending_fetch_error = str(
                        profesores_edit_pending_refresh.get("fetch_error") or ""
                    ).strip()
                    if (
                        isinstance(pending_detail, dict)
                        and pending_persona_id is not None
                        and pending_nivel_id is not None
                    ):
                        _store_profesor_edit_detail_state(
                            pending_detail,
                            persona_id=int(pending_persona_id),
                            nivel_id=int(pending_nivel_id),
                        )
                    elif pending_fetch_error:
                        st.session_state["profesores_edit_fetch_error"] = (
                            pending_fetch_error
                        )

                if profesores_edit_errors:
                    st.error("Errores al listar docentes:")
                    _show_dataframe(profesores_edit_errors, use_container_width=True)

                if profesores_edit_rows:
                    st.caption(
                        "Docentes cargados: {profesores_total} | Consultas con error: {consultas_error}".format(
                            profesores_total=int(
                                profesores_edit_summary.get("profesores_total", 0)
                            ),
                            consultas_error=int(
                                profesores_edit_summary.get("consultas_error", 0)
                            ),
                        )
                    )
                    profesores_edit_filtered_rows = list(profesores_edit_rows)
                    if not profesores_edit_filtered_rows:
                        st.warning("No hay docentes disponibles para este colegio.")
                    else:
                        with st.expander("Vista previa de docentes cargados", expanded=False):
                            _show_dataframe(
                                [
                                    {
                                        "Persona ID": row.get("persona_id", ""),
                                        "Docente": " ".join(
                                            part
                                            for part in (
                                                str(row.get("nombre") or "").strip(),
                                                str(row.get("apellido_paterno") or "").strip(),
                                                str(row.get("apellido_materno") or "").strip(),
                                            )
                                            if part
                                        ).strip(),
                                        "DNI": row.get("dni", ""),
                                        "E-mail": row.get("email", ""),
                                        "Login": row.get("login", ""),
                                        "Estado": row.get("estado", ""),
                                        "Niveles": ", ".join(
                                            _profesor_edit_level_label(nivel_id)
                                            for nivel_id in _profesor_edit_level_ids(row)
                                        ),
                                    }
                                    for row in profesores_edit_filtered_rows
                                ],
                                use_container_width=True,
                            )

                        profesores_edit_rows_by_id = {
                            int(row["persona_id"]): row
                            for row in profesores_edit_rows
                            if _safe_int(row.get("persona_id")) is not None
                        }
                        profesores_edit_options = [
                            int(row["persona_id"])
                            for row in profesores_edit_filtered_rows
                            if _safe_int(row.get("persona_id")) is not None
                        ]
                        current_selected_profesor_id = _safe_int(
                            st.session_state.get("profesores_edit_selected_persona_id")
                        )
                        if (
                            current_selected_profesor_id is not None
                            and int(current_selected_profesor_id)
                            not in profesores_edit_options
                        ):
                            st.session_state.pop(
                                "profesores_edit_selected_persona_id", None
                            )
                            st.session_state.pop(
                                "profesores_edit_selected_nivel_id", None
                            )

                        selected_profesor_persona_id = st.selectbox(
                            "Docente",
                            options=profesores_edit_options,
                            index=None,
                            placeholder="Selecciona un docente",
                            key="profesores_edit_selected_persona_id",
                            format_func=lambda persona_id: _profesor_edit_option_label(
                                profesores_edit_rows_by_id.get(int(persona_id), {})
                            ),
                        )

                        if selected_profesor_persona_id is not None:
                            profesor_edit_row = profesores_edit_rows_by_id.get(
                                int(selected_profesor_persona_id), {}
                            )
                            profesor_edit_level_ids = _profesor_edit_level_ids(
                                profesor_edit_row
                            )
                            if not profesor_edit_level_ids:
                                st.warning(
                                    "El docente no tiene niveles activos disponibles para editar."
                                )
                            else:
                                selected_profesor_level_id = int(
                                    profesor_edit_level_ids[0]
                                )
                                st.session_state[
                                    "profesores_edit_selected_nivel_id"
                                ] = int(selected_profesor_level_id)
                                if selected_profesor_level_id is not None:
                                    loaded_profesor_persona_id = _safe_int(
                                        st.session_state.get(
                                            "profesores_edit_loaded_persona_id"
                                        )
                                    )
                                    loaded_profesor_level_id = _safe_int(
                                        st.session_state.get(
                                            "profesores_edit_loaded_nivel_id"
                                        )
                                    )
                                    current_profesor_detail = st.session_state.get(
                                        "profesores_edit_detail"
                                    )
                                    if not isinstance(current_profesor_detail, dict):
                                        current_profesor_detail = {}

                                    if (
                                        int(selected_profesor_persona_id)
                                        != int(loaded_profesor_persona_id or 0)
                                        or int(selected_profesor_level_id)
                                        != int(loaded_profesor_level_id or 0)
                                        or not current_profesor_detail
                                    ):
                                        token = _get_shared_token()
                                        if not token:
                                            st.error(
                                                "Falta el token. Configura el token global o PEGASUS_TOKEN."
                                            )
                                            st.stop()
                                        try:
                                            colegio_id_int = _parse_colegio_id(colegio_id_raw)
                                        except ValueError as exc:
                                            st.error(f"Error: {exc}")
                                            st.stop()

                                        with st.spinner("Cargando detalle del docente..."):
                                            profesor_detail, profesor_detail_msg = _fetch_profesor_edit_detail_web(
                                                token=token,
                                                colegio_id=int(colegio_id_int),
                                                empresa_id=DEFAULT_EMPRESA_ID,
                                                ciclo_id=int(ciclo_id),
                                                nivel_id=int(selected_profesor_level_id),
                                                persona_id=int(selected_profesor_persona_id),
                                                timeout=int(timeout),
                                            )
                                        if profesor_detail is None:
                                            st.session_state["profesores_edit_detail"] = {}
                                            st.session_state[
                                                "profesores_edit_loaded_persona_id"
                                            ] = int(selected_profesor_persona_id)
                                            st.session_state[
                                                "profesores_edit_loaded_nivel_id"
                                            ] = int(selected_profesor_level_id)
                                            st.session_state[
                                                "profesores_edit_fetch_error"
                                            ] = str(
                                                profesor_detail_msg
                                                or "No se pudo cargar el detalle."
                                            )
                                        else:
                                            _store_profesor_edit_detail_state(
                                                profesor_detail,
                                                persona_id=int(selected_profesor_persona_id),
                                                nivel_id=int(selected_profesor_level_id),
                                            )

                                    profesor_edit_fetch_error = str(
                                        st.session_state.get(
                                            "profesores_edit_fetch_error"
                                        )
                                        or ""
                                    ).strip()
                                    if profesor_edit_fetch_error:
                                        st.error(
                                            f"No se pudo cargar el detalle del docente: {profesor_edit_fetch_error}"
                                        )

                                    current_profesor_detail = st.session_state.get(
                                        "profesores_edit_detail"
                                    )
                                    if (
                                        isinstance(current_profesor_detail, dict)
                                        and current_profesor_detail
                                    ):
                                        niveles_docente_txt = ", ".join(
                                            _profesor_edit_level_label(nivel_id)
                                            for nivel_id in profesor_edit_level_ids
                                        )
                                        persona_login_current = (
                                            current_profesor_detail.get("personaLogin")
                                            if isinstance(
                                                current_profesor_detail.get("personaLogin"),
                                                dict,
                                            )
                                            else {}
                                        )
                                        st.caption(
                                            "Persona ID: {persona_id} | Login actual: {login}".format(
                                                persona_id=int(selected_profesor_persona_id),
                                                login=str(
                                                    persona_login_current.get("login") or "-"
                                                ).strip()
                                                or "-",
                                            )
                                        )
                                        st.caption(
                                            f"Niveles del docente: {niveles_docente_txt or '-'}"
                                        )

                                        profesor_edit_group_rows = _build_profesor_edit_group_rows(
                                            current_profesor_detail,
                                            int(selected_profesor_level_id),
                                        )
                                        if profesor_edit_group_rows:
                                            with st.expander(
                                                "Grupos del nivel seleccionado",
                                                expanded=False,
                                            ):
                                                _show_dataframe(
                                                    profesor_edit_group_rows,
                                                    use_container_width=True,
                                                )

                                        name_col_1, name_col_2, name_col_3 = st.columns(
                                            3, gap="small"
                                        )
                                        name_col_1.text_input(
                                            "Nombre",
                                            key="profesores_edit_nombre",
                                        )
                                        name_col_2.text_input(
                                            "Apellido paterno",
                                            key="profesores_edit_apellido_paterno",
                                        )
                                        name_col_3.text_input(
                                            "Apellido materno",
                                            key="profesores_edit_apellido_materno",
                                        )

                                        sexo_actual_profesor = str(
                                            st.session_state.get("profesores_edit_sexo")
                                            or ""
                                        ).strip().upper()
                                        sexo_profesor_options = ["", "M", "F"]
                                        if (
                                            sexo_actual_profesor
                                            and sexo_actual_profesor
                                            not in sexo_profesor_options
                                        ):
                                            sexo_profesor_options = [
                                                sexo_actual_profesor
                                            ] + sexo_profesor_options

                                        data_col_1, data_col_2, data_col_3 = st.columns(
                                            3, gap="small"
                                        )
                                        data_col_1.selectbox(
                                            "Sexo",
                                            options=sexo_profesor_options,
                                            key="profesores_edit_sexo",
                                        )
                                        data_col_2.text_input(
                                            "DNI / identificador",
                                            key="profesores_edit_dni",
                                        )
                                        data_col_3.text_input(
                                            "E-mail",
                                            key="profesores_edit_email",
                                        )

                                        cred_col_1, cred_col_2 = st.columns(
                                            2, gap="small"
                                        )
                                        cred_col_1.text_input(
                                            "Login",
                                            key="profesores_edit_login",
                                        )
                                        cred_col_1.caption(
                                            "Minimo 6 caracteres. Solo letras, numeros y @ . - _"
                                        )
                                        cred_col_2.text_input(
                                            "Nueva password",
                                            key="profesores_edit_password",
                                            type="password",
                                        )
                                        cred_col_2.caption(
                                            "Opcional. Si la completas, tambien actualiza la password."
                                        )

                                        run_profesor_edit_save = st.button(
                                            "Guardar cambios del docente",
                                            type="primary",
                                            use_container_width=True,
                                            key="profesores_edit_save",
                                        )
                                        if run_profesor_edit_save:
                                            token = _get_shared_token()
                                            if not token:
                                                st.error(
                                                    "Falta el token. Configura el token global o PEGASUS_TOKEN."
                                                )
                                                st.stop()
                                            try:
                                                colegio_id_int = _parse_colegio_id(
                                                    colegio_id_raw
                                                )
                                            except ValueError as exc:
                                                st.error(f"Error: {exc}")
                                                st.stop()

                                            nombre_txt = str(
                                                st.session_state.get(
                                                    "profesores_edit_nombre"
                                                )
                                                or ""
                                            ).strip()
                                            apellido_paterno_txt = str(
                                                st.session_state.get(
                                                    "profesores_edit_apellido_paterno"
                                                )
                                                or ""
                                            ).strip()
                                            apellido_materno_txt = str(
                                                st.session_state.get(
                                                    "profesores_edit_apellido_materno"
                                                )
                                                or ""
                                            ).strip()
                                            sexo_txt = str(
                                                st.session_state.get(
                                                    "profesores_edit_sexo"
                                                )
                                                or ""
                                            ).strip().upper()
                                            dni_txt = str(
                                                st.session_state.get(
                                                    "profesores_edit_dni"
                                                )
                                                or ""
                                            ).strip()
                                            email_txt = str(
                                                st.session_state.get(
                                                    "profesores_edit_email"
                                                )
                                                or ""
                                            ).strip()
                                            login_txt = str(
                                                st.session_state.get(
                                                    "profesores_edit_login"
                                                )
                                                or ""
                                            ).strip()
                                            original_login_txt = str(
                                                st.session_state.get(
                                                    "profesores_edit_original_login"
                                                )
                                                or ""
                                            ).strip()
                                            password_txt = str(
                                                st.session_state.get(
                                                    "profesores_edit_password"
                                                )
                                                or ""
                                            )

                                            if not nombre_txt:
                                                st.error(
                                                    "El nombre del docente es obligatorio."
                                                )
                                                st.stop()
                                            if sexo_txt not in {"M", "F"}:
                                                st.error(
                                                    "Selecciona un sexo valido para el docente."
                                                )
                                                st.stop()

                                            login_changed = (
                                                login_txt != original_login_txt
                                            )
                                            password_provided = bool(password_txt)
                                            if login_changed or password_provided:
                                                login_error = _validar_login_reglas(
                                                    login_txt
                                                )
                                                if login_error:
                                                    st.error(login_error)
                                                    st.stop()
                                                if password_provided:
                                                    password_error = _validar_password_reglas(
                                                        password_txt
                                                    )
                                                    if password_error:
                                                        st.error(password_error)
                                                        st.stop()

                                            with st.spinner(
                                                "Guardando cambios del docente..."
                                            ):
                                                update_ok, _update_data, update_msg = _update_profesor_edit_web(
                                                    token=token,
                                                    colegio_id=int(colegio_id_int),
                                                    empresa_id=DEFAULT_EMPRESA_ID,
                                                    ciclo_id=int(ciclo_id),
                                                    nivel_id=int(
                                                        selected_profesor_level_id
                                                    ),
                                                    persona_id=int(
                                                        selected_profesor_persona_id
                                                    ),
                                                    nombre=nombre_txt,
                                                    apellido_paterno=apellido_paterno_txt,
                                                    apellido_materno=apellido_materno_txt,
                                                    sexo=sexo_txt,
                                                    email=email_txt,
                                                    id_oficial=dni_txt,
                                                    timeout=int(timeout),
                                                )
                                                if not update_ok:
                                                    st.error(
                                                        "No se pudo actualizar el docente: {msg}".format(
                                                            msg=update_msg or "sin detalle"
                                                        )
                                                    )
                                                    st.stop()

                                                login_notice_type = "success"
                                                login_notice_message = (
                                                    "Datos del docente actualizados."
                                                )
                                                if login_changed or password_provided:
                                                    login_ok, login_msg = _validar_login_profesor_web(
                                                        token=token,
                                                        empresa_id=DEFAULT_EMPRESA_ID,
                                                        login=login_txt,
                                                        persona_id=int(
                                                            selected_profesor_persona_id
                                                        ),
                                                        timeout=int(timeout),
                                                    )
                                                    if not login_ok:
                                                        login_notice_type = "warning"
                                                        login_notice_message = (
                                                            "Datos base actualizados, pero el login no es valido: {msg}".format(
                                                                msg=login_msg
                                                                or "sin detalle"
                                                            )
                                                        )
                                                    else:
                                                        login_update_ok, _login_update_data, login_update_msg = _update_login_profesor_web(
                                                            token=token,
                                                            colegio_id=int(
                                                                colegio_id_int
                                                            ),
                                                            empresa_id=DEFAULT_EMPRESA_ID,
                                                            ciclo_id=int(ciclo_id),
                                                            nivel_id=int(
                                                                selected_profesor_level_id
                                                            ),
                                                            persona_id=int(
                                                                selected_profesor_persona_id
                                                            ),
                                                            login=login_txt,
                                                            password=password_txt,
                                                            timeout=int(timeout),
                                                        )
                                                        if not login_update_ok:
                                                            login_notice_type = (
                                                                "warning"
                                                            )
                                                            login_notice_message = (
                                                                "Datos base actualizados, pero no se pudo actualizar login/password: {msg}".format(
                                                                    msg=login_update_msg
                                                                    or "sin detalle"
                                                                )
                                                            )
                                                        else:
                                                            login_notice_message = (
                                                                "Docente actualizado correctamente."
                                                            )

                                            refresh_warning = ""
                                            try:
                                                profesores_edit_rows_refresh, profesores_edit_summary_refresh, profesores_edit_errors_refresh = listar_profesores_filters_data(
                                                    token=token,
                                                    colegio_id=int(colegio_id_int),
                                                    empresa_id=DEFAULT_EMPRESA_ID,
                                                    ciclo_id=int(ciclo_id),
                                                    timeout=int(timeout),
                                                )
                                            except Exception as exc:  # pragma: no cover - UI
                                                refresh_warning = (
                                                    f" No se pudo refrescar la lista: {exc}"
                                                )
                                            else:
                                                st.session_state[
                                                    "profesores_edit_rows"
                                                ] = profesores_edit_rows_refresh
                                                st.session_state[
                                                    "profesores_edit_summary"
                                                ] = profesores_edit_summary_refresh
                                                st.session_state[
                                                    "profesores_edit_errors"
                                                ] = profesores_edit_errors_refresh
                                                st.session_state[
                                                    "profesores_edit_colegio_id"
                                                ] = int(colegio_id_int)
                                                refreshed_detail, refreshed_detail_msg = _fetch_profesor_edit_detail_web(
                                                    token=token,
                                                    colegio_id=int(colegio_id_int),
                                                    empresa_id=DEFAULT_EMPRESA_ID,
                                                    ciclo_id=int(ciclo_id),
                                                    nivel_id=int(
                                                        selected_profesor_level_id
                                                    ),
                                                    persona_id=int(
                                                        selected_profesor_persona_id
                                                    ),
                                                    timeout=int(timeout),
                                                )
                                                if refreshed_detail is None:
                                                    st.session_state[
                                                        "profesores_edit_pending_detail_refresh"
                                                    ] = {
                                                        "detail": None,
                                                        "persona_id": int(
                                                            selected_profesor_persona_id
                                                        ),
                                                        "nivel_id": int(
                                                            selected_profesor_level_id
                                                        ),
                                                        "fetch_error": str(
                                                            refreshed_detail_msg
                                                            or "No se pudo refrescar el detalle."
                                                        ),
                                                    }
                                                else:
                                                    st.session_state[
                                                        "profesores_edit_pending_detail_refresh"
                                                    ] = {
                                                        "detail": refreshed_detail,
                                                        "persona_id": int(
                                                            selected_profesor_persona_id
                                                        ),
                                                        "nivel_id": int(
                                                            selected_profesor_level_id
                                                        ),
                                                        "fetch_error": "",
                                                    }

                                            st.session_state["profesores_edit_notice"] = {
                                                "type": login_notice_type,
                                                "message": f"{login_notice_message}{refresh_warning}",
                                            }
                                            st.rerun()
                elif st.session_state.get("profesores_edit_colegio_id"):
                    st.warning("No se encontraron docentes para este colegio.")
            if profesores_crud_view == "base":
                with st.container(border=True):
                    st.markdown("**3) Generar Excel base operativo de profesores**")
                    st.caption("Incluye profesores activos e inactivos.")
                    run_generar_base = st.button(
                        "Generar Excel base",
                        type="primary",
                        key="profesores_generar",
                    )

                if run_generar_base:
                    token = _get_shared_token()
                    if not token:
                        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                        st.stop()
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                        st.stop()
                    try:
                        data, summary, errores = listar_profesores_data(
                            token=token,
                            colegio_id=colegio_id_int,
                            empresa_id=DEFAULT_EMPRESA_ID,
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                        st.stop()

                    filas: List[Dict[str, object]] = []
                    for entry in data:
                        dni = entry.get("dni", "") or ""
                        email = entry.get("email", "") or ""
                        login = entry.get("login", "") or email
                        filas.append(
                            {
                                "Id": entry.get("persona_id", ""),
                                "Nombre": entry.get("nombre", ""),
                                "Apellido Paterno": entry.get("apellido_paterno", ""),
                                "Apellido Materno": entry.get("apellido_materno", ""),
                                "Estado": entry.get("estado", ""),
                                "Sexo": entry.get("sexo", ""),
                                "DNI": dni,
                                "E-mail": email,
                                "Login": login,
                                "Password": "",
                                "Inicial": "",
                                "Primaria": "",
                                "Secundaria": "",
                                "I3": "",
                                "I4": "",
                                "I5": "",
                                "P1": "",
                                "P2": "",
                                "P3": "",
                                "P4": "",
                                "P5": "",
                                "P6": "",
                                "S1": "",
                                "S2": "",
                                "S3": "",
                                "S4": "",
                                "S5": "",
                                "Clases": "",
                                "Secciones": "",
                            }
                        )

                    if not filas:
                        st.warning("No se encontraron profesores para generar el Excel.")
                    else:
                        output_bytes = export_profesores_excel(filas)
                        file_name = f"profesores_base_{colegio_id_int}.xlsx"
                        st.session_state["profesores_excel_base"] = output_bytes
                        st.session_state["profesores_excel_base_name"] = file_name
                        st.success(
                            "Excel base listo. Profesores: {profesores_total}, Errores detalle: {detalle_error}.".format(
                                **summary
                            )
                        )
                        if errores:
                            st.error("Errores al obtener profesores:")
                            _show_dataframe(errores, use_container_width=True)

                if st.session_state.get("profesores_excel_base"):
                    st.download_button(
                        label="Descargar Excel base",
                        data=st.session_state["profesores_excel_base"],
                        file_name=st.session_state["profesores_excel_base_name"],
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
            if profesores_crud_view == "asignar":
                st.subheader("4) Asignar profesores a clases")
                st.caption("Sube la hoja con persona_id y CURSO. Secciones y Estado son opcionales.")
                st.markdown("**Procesos**")
                col_proc1, col_proc2 = st.columns(2)
                do_password = col_proc1.checkbox("Actualizar login/password", value=True)
                do_niveles = col_proc1.checkbox("Asignar niveles (asignarNivel)", value=True)
                do_estado = col_proc1.checkbox("Activar/Inactivar (Estado)", value=True)
                do_clases = col_proc2.checkbox("Asignar clases y secciones", value=True)
                inactivar_no_en_clases = col_proc2.checkbox(
                    "Inactivar IDs fuera de Profesores_clases",
                    value=True,
                    disabled=not do_estado,
                    help=(
                        "Marca Inactivo (por Estado) a IDs presentes en hoja Profesores "
                        "que no estÃ©n en Profesores_clases."
                    ),
                )
                remove_missing = col_proc2.checkbox(
                    "Eliminar profesores que no estÃ¡n en el Excel (solo clases evaluadas)",
                    value=False,
                    key="profesores_remove",
                    disabled=not do_clases,
                )
                if inactivar_no_en_clases and do_estado:
                    st.warning(
                        "Se inactivarÃ¡n por Estado los IDs que no aparezcan en Profesores_clases."
                    )
                if remove_missing and do_clases:
                    st.warning(
                        "Eliminar profesores quita asignaciones en las clases evaluadas. "
                        "Revisa el Excel antes de aplicar."
                    )
                uploaded_profesores = st.file_uploader(
                    "Excel de profesores",
                    type=["xlsx", "csv", "txt"],
                    key="profesores_excel",
                )
                sheet_name = st.text_input(
                    "Hoja (opcional)",
                    value="Profesores_clases",
                    help="Nombre de la hoja. Si lo dejas en blanco se intentarÃ¡ usar Profesores_clases.",
                )
                confirm_apply = st.checkbox(
                    "Confirmo aplicar cambios",
                    value=False,
                    key="profesores_confirm_apply",
                )

                col_run, col_apply = st.columns(2)
                run_sim = col_run.button("Simular", type="primary", key="profesores_simular")
                run_apply = col_apply.button(
                    "Aplicar cambios", type="secondary", key="profesores_apply"
                )
                st.info("Para aplicar cambios, marca 'Confirmo aplicar cambios'.")

                if run_sim or run_apply:
                    if not uploaded_profesores:
                        st.error("Sube un Excel de profesores.")
                        st.stop()

                    token = _get_shared_token()
                    if not token:
                        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                        st.stop()
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                        st.stop()
                    if run_apply and not confirm_apply:
                        st.error("Debes confirmar antes de aplicar cambios.")
                        st.stop()
                    if not any([do_password, do_niveles, do_estado, do_clases]):
                        st.error("Selecciona al menos un proceso.")
                        st.stop()

                    suffix = Path(uploaded_profesores.name).suffix or ".xlsx"
                    tmp_path = None
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                            tmp.write(uploaded_profesores.read())
                            tmp_path = Path(tmp.name)

                        logs: List[str] = []

                        def _on_log(line: str) -> None:
                            logs.append(line)

                        progress = st.progress(0)
                        status = st.empty()

                        def _on_progress(phase: str, current: int, total: int, message: str) -> None:
                            percent = int((current / total) * 100) if total else 0
                            progress.progress(percent)
                            status.write(f"{phase}: {message} ({current}/{total})")

                        if do_password:
                            pwd_summary, pwd_warnings, pwd_errors = actualizar_passwords_docentes(
                                token=token,
                                colegio_id=colegio_id_int,
                                excel_path=tmp_path,
                                sheet_name=sheet_name.strip() or None,
                                empresa_id=DEFAULT_EMPRESA_ID,
                                ciclo_id=int(ciclo_id),
                                timeout=int(timeout),
                                dry_run=not run_apply,
                                on_progress=lambda current, total, msg: _on_progress(
                                    "passwords", current, total, msg
                                ),
                            )
                            st.info(
                                "Passwords -> Docentes: {docentes_total}, Niveles: {niveles_total}, "
                                "Actualizaciones: {actualizaciones}, Errores API: {errores_api}.".format(
                                    **pwd_summary
                                )
                            )
                            if pwd_warnings:
                                st.warning("Warnings passwords:")
                                st.markdown("\n".join(f"- {item}" for item in pwd_warnings))
                            if pwd_errors:
                                st.error("Errores passwords:")
                                _show_dataframe(pwd_errors, use_container_width=True)

                        run_asignacion = any([do_niveles, do_estado, do_clases])
                        if run_asignacion:
                            summary, warnings, errors = asignar_profesores_clases(
                                token=token,
                                empresa_id=DEFAULT_EMPRESA_ID,
                                ciclo_id=int(ciclo_id),
                                colegio_id=colegio_id_int,
                                excel_path=tmp_path,
                                sheet_name=sheet_name.strip() or None,
                                timeout=int(timeout),
                                dry_run=not run_apply,
                                remove_missing=remove_missing if do_clases else False,
                                on_log=_on_log,
                                on_progress=_on_progress,
                                do_niveles=do_niveles,
                                do_estado=do_estado,
                                inactivar_no_en_clases=inactivar_no_en_clases if do_estado else False,
                                do_clases=do_clases,
                                do_grupos=do_clases,
                            )
                        else:
                            summary, warnings, errors = {}, [], []
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                        st.stop()
                    finally:
                        if tmp_path:
                            try:
                                tmp_path.unlink()
                            except OSError:
                                pass

                    if summary:
                        resumen = [
                            f"Docentes: {summary.get('docentes_procesados', 0)}",
                            f"Omitidos (no colegio): {summary.get('docentes_omitidos_no_colegio', 0)}",
                            f"Sin match: {summary.get('docentes_sin_match', 0)}",
                            f"Clases: {summary.get('clases_encontradas', 0)}",
                            f"Asignaciones nuevas: {summary.get('asignaciones_nuevas', 0)}",
                            f"Asig. omitidas: {summary.get('asignaciones_omitidas', 0)}",
                            f"Grupos asignados: {summary.get('grupos_asignados', 0)}",
                            f"Grupos omitidos: {summary.get('grupos_omitidos', 0)}",
                            f"Eliminaciones: {summary.get('eliminaciones', 0)}",
                            f"Estado activaciones: {summary.get('estado_activaciones', 0)}",
                            f"Estado inactivaciones: {summary.get('estado_inactivaciones', 0)}",
                            f"Estado omitidas: {summary.get('estado_omitidas', 0)}",
                            "Estado forzadas (fuera de Profesores_clases): "
                            f"{summary.get('estado_forzadas_fuera_clases', 0)}",
                            f"Errores API: {summary.get('errores_api', 0)}",
                        ]
                        st.success("Resumen de ejecuciÃ³n")
                        st.markdown("\n".join(f"- {item}" for item in resumen))
                        if warnings:
                            st.warning("Advertencias:")
                            st.markdown("\n".join(f"- {item}" for item in warnings))
                        if errors:
                            st.error("Errores al asignar profesores:")
                            _show_dataframe(errors, use_container_width=True)
                        if logs:
                            display_logs = [line for line in logs if line is not None]
                            while display_logs and not str(display_logs[0]).strip():
                                display_logs.pop(0)
                            while display_logs and not str(display_logs[-1]).strip():
                                display_logs.pop()
                            st.text_area(
                                "Log de ejecuciÃ³n",
                                value="\n".join(display_logs),
                                height=300,
                            )
                    else:
                        st.success("Listo. Solo se procesaron passwords.")

with tab_crud_alumnos:
    st.subheader("CRUD Alumnos")
    st.caption("Selecciona una funcion a la izquierda y trabaja en el panel derecho.")
    colegio_id_raw = str(
        st.session_state.get("shared_colegio_id", "")
        or st.session_state.get("alumnos_colegio_text", "")
    ).strip()
    if colegio_id_raw:
        st.session_state["alumnos_colegio_text"] = colegio_id_raw
    ciclo_id = ALUMNOS_CICLO_ID_DEFAULT
    empresa_id = DEFAULT_EMPRESA_ID
    timeout = 30
    loaded_alumnos_edit_colegio_id = _safe_int(
        st.session_state.get("alumnos_edit_colegio_id")
    )
    current_alumnos_edit_colegio_id = _safe_int(colegio_id_raw)
    if (
        loaded_alumnos_edit_colegio_id is not None
        and current_alumnos_edit_colegio_id is not None
        and loaded_alumnos_edit_colegio_id != current_alumnos_edit_colegio_id
    ):
        _clear_alumnos_edit_state()
    if str(st.session_state.get("alumnos_crud_nav") or "").strip() == "mover":
        st.session_state["alumnos_crud_nav"] = "editar"
    alumnos_nav_col, alumnos_body_col = st.columns([1.15, 4.85], gap="large")
    with alumnos_nav_col:
        alumnos_crud_view = _render_crud_menu(
            "Funciones de alumnos",
            [
                ("otros", "Otros", "Plantilla y censo"),
                ("comparar", "Comparar", "Compara BD vs actualizada"),
                ("editar", "Editar", "Edita datos y mueve de seccion"),
                ("crear", "Crear", "Crea alumno nuevo"),
            ],
            state_key="alumnos_crud_nav",
        )
    with alumnos_body_col:
        loaded_niveles = st.session_state.get("alumnos_manual_move_niveles") or []
        loaded_colegio_id = _safe_int(st.session_state.get("alumnos_manual_move_colegio_id"))
        current_colegio_id = _safe_int(colegio_id_raw)
        if alumnos_crud_view == "otros":
            with st.container(border=True):
                st.markdown("**1) Otros**")
                st.caption("Agrupa la descarga de plantilla y el censo de alumnos activos.")

                col_plantilla, col_censo = st.columns(2, gap="large")
                with col_plantilla:
                    st.markdown("**Plantilla de alumnos registrados**")
                    st.caption("Descarga la plantilla de edicion masiva.")

                    if st.button("Descargar plantilla", type="primary", key="alumnos_descargar"):
                        token = _get_shared_token()
                        if not token:
                            st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                            st.stop()
                        try:
                            colegio_id_int = _parse_colegio_id(colegio_id_raw)
                        except ValueError as exc:
                            st.error(f"Error: {exc}")
                            st.stop()
                        try:
                            with st.spinner("Descargando plantilla..."):
                                output_bytes, summary = descargar_plantilla_edicion_masiva(
                                    token=token,
                                    colegio_id=colegio_id_int,
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                )
                        except Exception as exc:  # pragma: no cover - UI
                            st.error(f"Error: {exc}")
                            st.stop()

                        file_name = f"plantilla_edicion_alumnos_{colegio_id_int}.xlsx"
                        st.success(f"Listo. Alumnos: {summary['alumnos_total']}.")
                        st.download_button(
                            label="Descargar plantilla",
                            data=output_bytes,
                            file_name=file_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )

                with col_censo:
                    st.markdown("**Censo de alumnos activos**")
                    st.caption("Consulta todas las secciones del colegio y muestra solo alumnos activos.")
                    col_censo_run, col_censo_clear = st.columns([2, 1], gap="small")
                    run_censo_activos = col_censo_run.button(
                        "Cargar censo",
                        type="primary",
                        key="alumnos_censo_activos_load_btn",
                        use_container_width=True,
                    )
                    clear_censo_activos = col_censo_clear.button(
                        "Limpiar",
                        key="alumnos_censo_activos_clear_btn",
                        use_container_width=True,
                    )

                    if clear_censo_activos:
                        for state_key in (
                            "alumnos_censo_activos_rows",
                            "alumnos_censo_activos_export_rows",
                            "alumnos_censo_activos_errors",
                            "alumnos_censo_activos_colegio_id",
                        ):
                            st.session_state.pop(state_key, None)
                        st.rerun()

                    if run_censo_activos:
                        token = _get_shared_token()
                        if not token:
                            st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                            st.stop()
                        try:
                            colegio_id_int = _parse_colegio_id(colegio_id_raw)
                        except ValueError as exc:
                            st.error(f"Error: {exc}")
                            st.stop()

                        niveles = _fetch_niveles_grados_grupos_censo(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                        contexts = _build_contexts_for_nivel_grado(niveles=niveles)
                        rows_activos: List[Dict[str, object]] = []
                        export_rows_activos: List[Dict[str, object]] = []
                        errors_activos: List[str] = []
                        try:
                            login_lookup_by_alumno, login_lookup_by_persona = _fetch_login_password_lookup_censo(
                                token=token,
                                colegio_id=int(colegio_id_int),
                                empresa_id=int(empresa_id),
                                ciclo_id=int(ciclo_id),
                                timeout=int(timeout),
                            )
                        except Exception:
                            login_lookup_by_alumno = {}
                            login_lookup_by_persona = {}
                        for ctx in contexts:
                            try:
                                alumnos_ctx = _fetch_alumnos_censo(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    nivel_id=int(ctx.get("nivel_id") or 0),
                                    grado_id=int(ctx.get("grado_id") or 0),
                                    grupo_id=int(ctx.get("grupo_id") or 0),
                                    timeout=int(timeout),
                                )
                            except Exception as exc:  # pragma: no cover - UI
                                errors_activos.append(
                                    "Error en {nivel} | {grado} ({seccion}): {err}".format(
                                        nivel=str(ctx.get("nivel") or ""),
                                        grado=str(ctx.get("grado") or ""),
                                        seccion=str(ctx.get("seccion") or ""),
                                        err=str(exc),
                                    )
                                )
                                continue
                            for item in alumnos_ctx:
                                if not isinstance(item, dict):
                                    continue
                                flat = _flatten_censo_alumno_for_auto_plan(item=item, fallback=ctx)
                                if not _to_bool(flat.get("activo")):
                                    continue
                                login_txt, _password_txt = _resolve_alumno_login_password(
                                    item,
                                    login_lookup_by_alumno,
                                    login_lookup_by_persona,
                                )
                                row_activo = {
                                    "Nivel": flat.get("nivel") or "",
                                    "Grado": flat.get("grado") or "",
                                    "Grupo": flat.get("seccion_norm") or flat.get("seccion") or "",
                                    "Nombre del alumno": flat.get("nombre_completo") or "",
                                    "Login": login_txt,
                                    "Password": "",
                                }
                                rows_activos.append(dict(row_activo))
                                export_rows_activos.append(dict(row_activo))

                        rows_activos = _normalize_censo_activos_export_rows(rows_activos)
                        export_rows_activos = _normalize_censo_activos_export_rows(export_rows_activos)
                        st.session_state["alumnos_censo_activos_rows"] = rows_activos
                        st.session_state["alumnos_censo_activos_export_rows"] = export_rows_activos
                        st.session_state["alumnos_censo_activos_errors"] = errors_activos
                        st.session_state["alumnos_censo_activos_colegio_id"] = int(colegio_id_int)
                        st.success(
                            "Censo cargado. Activos: {total} | Errores de consulta: {errors}".format(
                                total=len(rows_activos),
                                errors=len(errors_activos),
                            )
                        )

            censo_rows_cached = st.session_state.get("alumnos_censo_activos_rows") or []
            censo_export_rows_cached = st.session_state.get("alumnos_censo_activos_export_rows") or []
            censo_errors_cached = st.session_state.get("alumnos_censo_activos_errors") or []
            censo_display_rows = _normalize_censo_activos_export_rows(
                censo_export_rows_cached or censo_rows_cached
            )
            if censo_display_rows:
                _show_dataframe(censo_display_rows, use_container_width=True)
                censo_colegio_id = _safe_int(st.session_state.get("alumnos_censo_activos_colegio_id"))
                file_suffix = str(censo_colegio_id) if censo_colegio_id is not None else "colegio"
                st.download_button(
                    label="Descargar censo activos",
                    data=_export_simple_excel(
                        censo_display_rows,
                        sheet_name="activos",
                    ),
                    file_name=f"censo_alumnos_activos_{file_suffix}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="alumnos_censo_activos_download",
                )
            else:
                st.caption("Presiona 'Cargar censo de alumnos activos' para iniciar.")

            if censo_errors_cached:
                st.warning("Hubo errores al consultar algunas secciones del censo.")
                st.write("\n".join(f"- {item}" for item in censo_errors_cached[:20]))
                pending = len(censo_errors_cached) - 20
                if pending > 0:
                    st.caption(f"... y {pending} errores mas.")

        if alumnos_crud_view == "comparar":
            with st.container(border=True):
                st.markdown("**2) Comparar Plantilla_BD vs Plantilla_Actualizada**")
                st.caption("Genera altas, match e inactivados.")
                uploaded_compare = st.file_uploader(
                    "Archivo .xlsx",
                    type=["xlsx"],
                    key="alumnos_compare_excel",
                )
                compare_mode_options = {
                    "Por DNI": COMPARE_MODE_DNI,
                    "Por apellido paterno + materno": COMPARE_MODE_APELLIDOS,
                    "Por apellidos y luego DNI": COMPARE_MODE_AMBOS,
                }
                compare_mode_label = st.selectbox(
                    "Criterio de comparacion",
                    options=list(compare_mode_options.keys()),
                    index=2,
                    key="alumnos_compare_mode",
                )
                run_alumnos_compare = st.button(
                    "Generar resultado",
                    type="primary",
                    key="alumnos_compare",
                )
                if run_alumnos_compare:
                    for state_key in (
                        "alumnos_compare_summary",
                        "alumnos_compare_resultado_bytes",
                        "alumnos_compare_actualizacion_bytes",
                        "alumnos_compare_alta_bytes",
                        "alumnos_compare_coincidencias_rows",
                        "alumnos_compare_sin_referencia_rows",
                        "alumnos_compare_source_name",
                    ):
                        st.session_state.pop(state_key, None)
                    if not uploaded_compare:
                        st.error("Sube un Excel .xlsx con Plantilla_BD y Plantilla_Actualizada.")
                        st.stop()
                    suffix = Path(uploaded_compare.name).suffix or ".xlsx"
                    tmp_path = None
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                            tmp.write(uploaded_compare.read())
                            tmp_path = Path(tmp.name)
                        compare_result = comparar_plantillas_detalle(
                            excel_path=tmp_path,
                            compare_mode=compare_mode_options.get(compare_mode_label, COMPARE_MODE_AMBOS),
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                        st.stop()
                    finally:
                        if tmp_path:
                            try:
                                tmp_path.unlink()
                            except OSError:
                                pass

                    st.session_state["alumnos_compare_summary"] = dict(
                        compare_result.get("summary") or {}
                    )
                    st.session_state["alumnos_compare_resultado_bytes"] = (
                        compare_result.get("resultado_bytes") or b""
                    )
                    st.session_state["alumnos_compare_actualizacion_bytes"] = (
                        compare_result.get("actualizacion_bytes") or b""
                    )
                    st.session_state["alumnos_compare_alta_bytes"] = (
                        compare_result.get("alta_bytes") or b""
                    )
                    st.session_state["alumnos_compare_coincidencias_rows"] = list(
                        compare_result.get("coincidencias_rows") or []
                    )
                    st.session_state["alumnos_compare_sin_referencia_rows"] = list(
                        compare_result.get("sin_referencia_rows") or []
                    )
                    st.session_state["alumnos_compare_source_name"] = str(
                        uploaded_compare.name or "alumnos.xlsx"
                    )
                    summary = st.session_state["alumnos_compare_summary"]
                    st.success(
                        "Listo. Base: {base_total}, Actualizada: {actualizados_total}, "
                        "Match: {match_total}, Nuevos: {nuevos_total}, "
                        "Inactivados: {inactivados_total}.".format(**summary)
                    )

                alumnos_compare_summary_cached = (
                    st.session_state.get("alumnos_compare_summary") or {}
                )
                alumnos_compare_source_name_cached = str(
                    st.session_state.get("alumnos_compare_source_name") or "alumnos.xlsx"
                )
                alumnos_compare_resultado_bytes_cached = (
                    st.session_state.get("alumnos_compare_resultado_bytes") or b""
                )
                alumnos_compare_actualizacion_bytes_cached = (
                    st.session_state.get("alumnos_compare_actualizacion_bytes") or b""
                )
                alumnos_compare_alta_bytes_cached = (
                    st.session_state.get("alumnos_compare_alta_bytes") or b""
                )
                alumnos_compare_coincidencias_cached = (
                    st.session_state.get("alumnos_compare_coincidencias_rows") or []
                )
                alumnos_compare_sin_referencia_cached = (
                    st.session_state.get("alumnos_compare_sin_referencia_rows") or []
                )

                if alumnos_compare_summary_cached:
                    st.info(
                        "Referencias BD: {match_total} | Sin referencia BD: {nuevos_total} | "
                        "Inactivados BD: {inactivados_total}".format(
                            **alumnos_compare_summary_cached
                        )
                    )

                    st.markdown("**Alumnos con referencia BD**")
                    if alumnos_compare_coincidencias_cached:
                        _show_dataframe(
                            alumnos_compare_coincidencias_cached,
                            use_container_width=True,
                        )
                    else:
                        st.caption("No se encontraron alumnos de Plantilla_Actualizada con referencia en BD.")

                    st.markdown("**Alumnos sin referencia BD**")
                    if alumnos_compare_sin_referencia_cached:
                        _show_dataframe(
                            alumnos_compare_sin_referencia_cached,
                            use_container_width=True,
                        )
                    else:
                        st.caption("Todos los alumnos de Plantilla_Actualizada tienen referencia en BD.")

                    compare_source_stem = Path(alumnos_compare_source_name_cached).stem
                    download_cols = st.columns(3)
                    with download_cols[0]:
                        st.download_button(
                            label="Descargar actualizacion masiva",
                            data=alumnos_compare_actualizacion_bytes_cached,
                            file_name=f"alumnos_actualizacion_masiva_{compare_source_stem}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            disabled=not bool(alumnos_compare_actualizacion_bytes_cached),
                            key="alumnos_compare_download_actualizacion",
                        )
                    with download_cols[1]:
                        st.download_button(
                            label="Descargar alta",
                            data=alumnos_compare_alta_bytes_cached,
                            file_name=f"alumnos_alta_{compare_source_stem}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            disabled=not bool(alumnos_compare_alta_bytes_cached),
                            key="alumnos_compare_download_alta",
                        )
                    with download_cols[2]:
                        st.download_button(
                            label="Descargar resultado completo",
                            data=alumnos_compare_resultado_bytes_cached,
                            file_name=f"alumnos_resultados_{compare_source_stem}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            disabled=not bool(alumnos_compare_resultado_bytes_cached),
                            key="alumnos_compare_download_completo",
                        )

        if alumnos_crud_view == "mover":
            with st.container(border=True):
                title_cols = st.columns([10, 1], gap="small")
                with title_cols[1]:
                    run_load_students = st.button(
                        "↻",
                        key="alumnos_manual_move_load_btn",
                        help="Actualizar listado de alumnos",
                        type="tertiary",
                        use_container_width=False,
                    )
            loaded_students = st.session_state.get("alumnos_manual_move_students") or []
            loaded_niveles = st.session_state.get("alumnos_manual_move_niveles") or []
            loaded_errors = st.session_state.get("alumnos_manual_move_errors") or []
            loaded_colegio_id = _safe_int(st.session_state.get("alumnos_manual_move_colegio_id"))
            current_colegio_id = _safe_int(colegio_id_raw)
            should_auto_load_students = (
                current_colegio_id is not None
                and (
                    loaded_colegio_id is None
                    or int(loaded_colegio_id) != int(current_colegio_id)
                    or not loaded_students
                )
            )

            if run_load_students or should_auto_load_students:
                token = _get_shared_token()
                if not token:
                    st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                else:
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                    else:
                        try:
                            status_box = st.empty()
    
                            def _on_status_load(message: str) -> None:
                                msg = str(message or "").strip()
                                if not msg:
                                    return
                                status_box.info(msg)
    
                            with st.spinner("Listando alumnos del colegio..."):
                                manual_catalog = _fetch_alumnos_catalog_for_manual_move(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                    on_status=_on_status_load,
                                )
                            status_box.empty()
                        except Exception as exc:  # pragma: no cover - UI
                            st.error(f"Error listando alumnos: {exc}")
                        else:
                            students_loaded = manual_catalog.get("students") or []
                            niveles_loaded = manual_catalog.get("niveles") or []
                            errors_loaded = manual_catalog.get("errors") or []
                            st.session_state["alumnos_manual_move_students"] = students_loaded
                            st.session_state["alumnos_manual_move_niveles"] = niveles_loaded
                            st.session_state["alumnos_manual_move_errors"] = errors_loaded
                            st.session_state["alumnos_manual_move_colegio_id"] = int(colegio_id_int)
                            for state_key in list(st.session_state.keys()):
                                if str(state_key).startswith("alumnos_manual_move_dest_"):
                                    st.session_state.pop(state_key, None)
                            if run_load_students:
                                st.success(
                                    "Listado actualizado. Alumnos: {students} | Errores de consulta: {errors}".format(
                                        students=len(students_loaded),
                                        errors=len(errors_loaded),
                                    )
                                )

            loaded_students = st.session_state.get("alumnos_manual_move_students") or []
            loaded_niveles = st.session_state.get("alumnos_manual_move_niveles") or []
            loaded_errors = st.session_state.get("alumnos_manual_move_errors") or []
            loaded_colegio_id = _safe_int(st.session_state.get("alumnos_manual_move_colegio_id"))
            current_colegio_id = _safe_int(colegio_id_raw)
            pending_reset_keys = st.session_state.pop("alumnos_manual_move_pending_reset_keys", [])
            if isinstance(pending_reset_keys, list):
                for state_key in pending_reset_keys:
                    if state_key:
                        st.session_state.pop(str(state_key), None)
    
            if loaded_errors:
                st.caption(f"Advertencia: hubo {len(loaded_errors)} errores al listar algunas secciones.")
    
            if loaded_colegio_id is not None and current_colegio_id is not None and int(loaded_colegio_id) != int(current_colegio_id):
                st.warning("No se pudo sincronizar el listado con el colegio actual.")
            elif not loaded_students:
                st.caption("No hay alumnos disponibles para mostrar.")
            else:
                search_text = st_keyup(
                    "Buscar alumno (login, nombre, apellido o DNI)",
                    value=str(st.session_state.get("alumnos_manual_move_search", "") or ""),
                    key="alumnos_manual_move_search",
                    placeholder="Ejemplo: lnjmf90942147, CHERO o 91092564",
                    debounce=150,
                ).strip()
                filtered_students: List[Dict[str, object]] = []
                for row in loaded_students:
                    if _manual_move_alumno_matches_filter(row, search_text):
                        filtered_students.append(row)
    
                st.caption(f"Resultados del filtro: {len(filtered_students)} alumno(s).")
                manual_move_notice = st.session_state.pop("alumnos_manual_move_notice", None)
                if isinstance(manual_move_notice, dict):
                    notice_type = str(manual_move_notice.get("type") or "").strip().lower()
                    notice_message = str(manual_move_notice.get("message") or "").strip()
                    if notice_message:
                        if notice_type == "success":
                            st.success(notice_message)
                        elif notice_type == "warning":
                            st.warning(notice_message)
                        else:
                            st.info(notice_message)
                if not filtered_students:
                    st.info("No hay alumnos con ese filtro.")
                else:
                    valid_students = [
                        row for row in filtered_students if _safe_int(row.get("alumno_id")) is not None
                    ]
                    destination_catalog = _build_manual_move_destination_catalog(loaded_niveles)
                    nivel_ids = destination_catalog.get("nivel_ids") or []
                    nivel_name_by_id = destination_catalog.get("nivel_name_by_id") or {}
                    grado_payload_by_id = destination_catalog.get("grado_payload_by_id") or {}
                    grado_ids_by_nivel = destination_catalog.get("grado_ids_by_nivel") or {}
                    seccion_options = destination_catalog.get("seccion_options") or []
                    grupo_payload_by_grado_seccion = (
                        destination_catalog.get("grupo_payload_by_grado_seccion") or {}
                    )
    
                    if not valid_students:
                        st.warning("No hay alumnos validos para mover.")
                    elif not nivel_ids:
                        st.warning("No hay grados/secciones disponibles para destino.")
                    else:
                        valid_students = sorted(
                            valid_students,
                            key=lambda row: (
                                int(_safe_int(row.get("nivel_id")) or 0),
                                int(_safe_int(row.get("grado_id")) or 0),
                                _grupo_sort_key(
                                    str(row.get("seccion_norm") or ""),
                                    str(row.get("seccion") or ""),
                                ),
                                str(row.get("apellido_paterno") or "").upper(),
                                str(row.get("apellido_materno") or "").upper(),
                                str(row.get("nombre") or "").upper(),
                            ),
                        )

                        secciones_by_grado: Dict[int, List[str]] = {}
                        for (grado_id_tmp, seccion), payload in grupo_payload_by_grado_seccion.items():
                            grado_id_int = _safe_int(grado_id_tmp)
                            if grado_id_int is None or not isinstance(payload, dict) or not payload:
                                continue
                            secciones_by_grado.setdefault(int(grado_id_int), []).append(str(seccion))
                        for grado_id_int, secciones in list(secciones_by_grado.items()):
                            secciones_by_grado[grado_id_int] = sorted(
                                list(dict.fromkeys(secciones)),
                                key=lambda value: _grupo_sort_key(str(value), str(value)),
                            )

                        total_valid_students = len(valid_students)
                        visible_students = valid_students
                        st.caption(f"Mostrando {total_valid_students} alumno(s).")
                        st.markdown(
                            """
                            <style>
                            div[data-testid="stSelectbox"] div[data-baseweb="select"] > div {
                                min-height: 2.1rem;
                                padding-top: 0;
                                padding-bottom: 0;
                            }
                            div[data-testid="stButton"] > button {
                                min-height: 2.1rem;
                                padding-top: 0.2rem;
                                padding-bottom: 0.2rem;
                            }
                            </style>
                            """,
                            unsafe_allow_html=True,
                        )

                        current_group_key: Optional[Tuple[str, str, str]] = None
                        for row in visible_students:
                            alumno_id = _safe_int(row.get("alumno_id"))
                            if alumno_id is None:
                                continue
                            alumno_id_int = int(alumno_id)
    
                            nivel_key = f"alumnos_manual_move_dest_nivel_{alumno_id_int}"
                            grado_key = f"alumnos_manual_move_dest_grado_{alumno_id_int}"
                            grupo_key = f"alumnos_manual_move_dest_seccion_{alumno_id_int}"
                            alumno_seccion = _normalize_seccion_key(
                                row.get("seccion_norm") or row.get("seccion") or ""
                            )
                            alumno_header = _manual_move_alumno_option_label(row)
                            group_key = (
                                str(row.get("nivel") or "").strip(),
                                str(row.get("grado") or "").strip(),
                                alumno_seccion or "-",
                            )
                            if group_key != current_group_key:
                                current_group_key = group_key
                                st.caption(
                                    "{nivel} | {grado} | Seccion {seccion}".format(
                                        nivel=group_key[0] or "-",
                                        grado=group_key[1] or "-",
                                        seccion=group_key[2] or "-",
                                    )
                                )
                                header_cols = st.columns([4.2, 1.4, 2.2, 1.0, 1.0], gap="small")
                                header_cols[0].markdown("**Alumno**")
                                header_cols[1].markdown("**Nuevo nivel**")
                                header_cols[2].markdown("**Nuevo grado**")
                                header_cols[3].markdown("**Seccion**")
                                header_cols[4].markdown("**Guardar**")
    
                            if nivel_key not in st.session_state:
                                st.session_state[nivel_key] = None
                            if grado_key not in st.session_state:
                                st.session_state[grado_key] = None
                            if grupo_key not in st.session_state:
                                st.session_state[grupo_key] = None
    
                            selected_nivel_id = _safe_int(st.session_state.get(nivel_key))
                            if selected_nivel_id not in nivel_ids:
                                st.session_state[nivel_key] = None
                                selected_nivel_id = None
    
                            grado_options = []
                            if selected_nivel_id is not None:
                                grado_options = [
                                    int(value)
                                    for value in (grado_ids_by_nivel.get(int(selected_nivel_id)) or [])
                                ]
    
                            selected_grado_id = _safe_int(st.session_state.get(grado_key))
                            if selected_grado_id not in grado_options:
                                st.session_state[grado_key] = None
                                selected_grado_id = None
    
                            secciones_grado = (
                                secciones_by_grado.get(int(selected_grado_id), [])
                                if selected_grado_id is not None
                                else []
                            )
    
                            selected_seccion = _normalize_seccion_key(st.session_state.get(grupo_key) or "")
                            if not selected_seccion or selected_seccion not in secciones_grado:
                                st.session_state[grupo_key] = None
                                selected_seccion = ""
    
                            row_cols = st.columns([4.2, 1.4, 2.2, 1.0, 1.0], gap="small")
                            row_cols[0].caption(alumno_header)
    
                            with row_cols[1]:
                                st.selectbox(
                                    "Nuevo nivel",
                                    options=nivel_ids,
                                    index=None,
                                    placeholder="Nivel",
                                    format_func=lambda value: str(
                                        nivel_name_by_id.get(int(value), value)
                                    ).strip(),
                                    key=nivel_key,
                                    on_change=_clear_manual_move_selection,
                                    args=(grado_key, grupo_key),
                                    label_visibility="collapsed",
                                )
    
                            selected_nivel_id = _safe_int(st.session_state.get(nivel_key))
                            grado_options = []
                            if selected_nivel_id is not None:
                                grado_options = [
                                    int(value)
                                    for value in (grado_ids_by_nivel.get(int(selected_nivel_id)) or [])
                                ]
    
                            with row_cols[2]:
                                st.selectbox(
                                    "Nuevo grado",
                                    options=grado_options,
                                    index=None,
                                    placeholder="Grado",
                                    format_func=lambda value: str(
                                        (
                                            grado_payload_by_id.get(int(value), {}) or {}
                                        ).get("grado")
                                        or value
                                    ).strip(),
                                    key=grado_key,
                                    on_change=_clear_manual_move_selection,
                                    args=(grupo_key,),
                                    disabled=not grado_options,
                                    label_visibility="collapsed",
                                )
    
                            selected_grado_id = _safe_int(st.session_state.get(grado_key))
                            secciones_grado = (
                                secciones_by_grado.get(int(selected_grado_id), [])
                                if selected_grado_id is not None
                                else []
                            )
    
                            with row_cols[3]:
                                st.selectbox(
                                    "Seccion",
                                    options=secciones_grado,
                                    index=None,
                                    placeholder="Seccion",
                                    key=grupo_key,
                                    disabled=not secciones_grado,
                                    label_visibility="collapsed",
                                )
    
                            save_clicked = row_cols[4].button(
                                "Guardar",
                                key=f"alumnos_manual_move_save_btn_{alumno_id_int}",
                                type="primary",
                                use_container_width=True,
                            )
    
                            if save_clicked:
                                token = _get_shared_token()
                                if not token:
                                    st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                                    st.stop()
                                try:
                                    colegio_id_int = _parse_colegio_id(colegio_id_raw)
                                except ValueError as exc:
                                    st.error(f"Error: {exc}")
                                    st.stop()
                                if (
                                    loaded_colegio_id is not None
                                    and int(loaded_colegio_id) != int(colegio_id_int)
                                ):
                                    st.error("El colegio global cambio. Vuelve a listar alumnos.")
                                    st.stop()
    
                                selected_nivel_id = _safe_int(st.session_state.get(nivel_key))
                                selected_grado_id = _safe_int(st.session_state.get(grado_key))
                                selected_seccion = _normalize_seccion_key(
                                    st.session_state.get(grupo_key) or ""
                                )
                                if (
                                    selected_nivel_id is None
                                    or selected_grado_id is None
                                    or not selected_seccion
                                ):
                                    st.session_state["alumnos_manual_move_notice"] = {
                                        "type": "warning",
                                        "message": (
                                            f"Completa nivel, grado y seccion antes de guardar "
                                            f"el alumno {alumno_id_int}."
                                        ),
                                    }
                                    st.rerun()
    
                                grado_payload = grado_payload_by_id.get(int(selected_grado_id), {})
                                grado_nivel_id = _safe_int(grado_payload.get("nivel_id"))
                                if grado_nivel_id is None:
                                    st.session_state["alumnos_manual_move_notice"] = {
                                        "type": "warning",
                                        "message": f"No se pudo resolver el grado del alumno {alumno_id_int}.",
                                    }
                                    st.rerun()
                                if int(grado_nivel_id) != int(selected_nivel_id):
                                    st.session_state["alumnos_manual_move_notice"] = {
                                        "type": "warning",
                                        "message": (
                                            f"El nivel seleccionado no corresponde al grado elegido "
                                            f"para el alumno {alumno_id_int}."
                                        ),
                                    }
                                    st.rerun()
    
                                destino_payload = grupo_payload_by_grado_seccion.get(
                                    (int(selected_grado_id), selected_seccion)
                                )
                                if not isinstance(destino_payload, dict):
                                    st.session_state["alumnos_manual_move_notice"] = {
                                        "type": "warning",
                                        "message": (
                                            f"La seccion {selected_seccion} no esta disponible para el grado "
                                            f"seleccionado del alumno {alumno_id_int}."
                                        ),
                                    }
                                    st.rerun()
    
                                status_box = st.empty()

                                def _on_status_move(message: str) -> None:
                                    msg = str(message or "").strip()
                                    if not msg:
                                        return
                                    status_box.info(f"Alumno {alumno_id_int}: {msg}")

                                try:
                                    with st.spinner(f"Moviendo alumno {alumno_id_int}..."):
                                        result = _apply_single_alumno_move_and_reassign(
                                            token=token,
                                            colegio_id=int(colegio_id_int),
                                            empresa_id=int(empresa_id),
                                            ciclo_id=int(ciclo_id),
                                            timeout=int(timeout),
                                            alumno_row=row,
                                            nuevo_nivel_id=int(destino_payload.get("nivel_id") or 0),
                                            nuevo_grado_id=int(destino_payload.get("grado_id") or 0),
                                            nuevo_grupo_id=int(destino_payload.get("grupo_id") or 0),
                                            nueva_seccion=str(destino_payload.get("seccion") or ""),
                                            on_status=_on_status_move,
                                        )
                                except Exception as exc:  # pragma: no cover - UI
                                    status_box.error(f"Alumno {alumno_id_int}: error durante el proceso.")
                                    st.session_state["alumnos_manual_move_notice"] = {
                                        "type": "warning",
                                        "message": f"No se pudo mover el alumno {alumno_id_int}: {exc}",
                                    }
                                    st.rerun()
    
                                if not _to_bool(result.get("move_ok")):
                                    status_box.warning(f"Alumno {alumno_id_int}: no se pudo completar el movimiento.")
                                    st.session_state["alumnos_manual_move_notice"] = {
                                        "type": "warning",
                                        "message": (
                                            f"No se pudo mover el alumno {alumno_id_int}: "
                                            f"{str(result.get('move_msg') or 'sin detalle')}"
                                        ),
                                    }
                                    st.rerun()
    
                                cached_students = st.session_state.get("alumnos_manual_move_students") or []
                                _update_manual_move_cached_student(
                                    students=cached_students,
                                    alumno_id=alumno_id_int,
                                    destino_payload=destino_payload,
                                )
                                st.session_state["alumnos_manual_move_students"] = cached_students
                                _queue_manual_move_reset(nivel_key, grado_key, grupo_key)
                                status_box.success(
                                    "Alumno {alumno}: movimiento OK | clases quitadas={quitadas} | clases asignadas={asignadas}".format(
                                        alumno=alumno_id_int,
                                        quitadas=int(result.get("removed_ok") or 0),
                                        asignadas=int(result.get("assigned_ok") or 0),
                                    )
                                )
                                st.session_state["alumnos_manual_move_notice"] = {
                                    "type": "success",
                                    "message": (
                                        "Alumno actualizado: {alumno} -> {nivel} | {grado} ({seccion})".format(
                                            alumno=alumno_header,
                                            nivel=str(destino_payload.get("nivel") or "").strip(),
                                            grado=str(destino_payload.get("grado") or "").strip(),
                                            seccion=str(destino_payload.get("seccion") or "").strip() or "-",
                                        )
                                    ),
                                }
                                st.rerun()
    
                            st.divider()
    
        if alumnos_crud_view == "editar":
            with st.container(border=True):
                st.markdown("**3) Editar alumno**")
                st.caption(
                    "Lista alumnos del colegio, carga el detalle, actualiza datos base y permite moverlo desde el boton superior derecho."
                )
                st.caption(
                    "Puedes cambiar solo el login o enviar tambien una nueva password."
                )

            alumnos_edit_notice = st.session_state.pop("alumnos_edit_notice", None)
            if isinstance(alumnos_edit_notice, dict):
                notice_type = str(alumnos_edit_notice.get("type") or "").strip().lower()
                notice_message = str(alumnos_edit_notice.get("message") or "").strip()
                if notice_message:
                    if notice_type == "success":
                        st.success(notice_message)
                    elif notice_type == "warning":
                        st.warning(notice_message)
                    elif notice_type == "error":
                        st.error(notice_message)
                    else:
                        st.info(notice_message)

            col_edit_load, col_edit_clear = st.columns([2, 1], gap="small")
            run_edit_load = col_edit_load.button(
                "Cargar alumnos",
                type="primary",
                key="alumnos_edit_load_btn",
                use_container_width=True,
            )
            run_edit_clear = col_edit_clear.button(
                "Limpiar",
                key="alumnos_edit_clear_btn",
                use_container_width=True,
            )

            if run_edit_clear:
                _clear_alumnos_edit_state()
                st.rerun()

            if run_edit_load:
                token = _get_shared_token()
                if not token:
                    st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                    st.stop()
                try:
                    colegio_id_int = _parse_colegio_id(colegio_id_raw)
                except ValueError as exc:
                    st.error(f"Error: {exc}")
                    st.stop()

                try:
                    with st.spinner("Cargando alumnos del colegio..."):
                        alumnos_catalog_edit = _fetch_alumnos_catalog_for_manual_move(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                except Exception as exc:  # pragma: no cover - UI
                    st.error(f"Error: {exc}")
                    st.stop()

                _clear_alumnos_edit_state()
                st.session_state["alumnos_edit_rows"] = (
                    alumnos_catalog_edit.get("students") or []
                )
                st.session_state["alumnos_edit_errors"] = (
                    alumnos_catalog_edit.get("errors") or []
                )
                st.session_state["alumnos_edit_niveles"] = (
                    alumnos_catalog_edit.get("niveles") or []
                )
                st.session_state["alumnos_edit_colegio_id"] = int(colegio_id_int)

            alumnos_edit_rows = st.session_state.get("alumnos_edit_rows") or []
            alumnos_edit_errors = st.session_state.get("alumnos_edit_errors") or []
            alumnos_edit_pending_refresh = st.session_state.pop(
                "alumnos_edit_pending_detail_refresh", None
            )
            if isinstance(alumnos_edit_pending_refresh, dict):
                pending_detail = alumnos_edit_pending_refresh.get("detail")
                pending_context = alumnos_edit_pending_refresh.get("context")
                pending_fetch_error = str(
                    alumnos_edit_pending_refresh.get("fetch_error") or ""
                ).strip()
                if isinstance(pending_detail, dict) and isinstance(pending_context, dict):
                    _store_alumno_edit_detail_state(
                        pending_detail,
                        context=pending_context,
                    )
                elif pending_fetch_error:
                    st.session_state["alumnos_edit_fetch_error"] = pending_fetch_error

            if alumnos_edit_errors:
                with st.expander(
                    f"Errores de carga ({len(alumnos_edit_errors)})", expanded=False
                ):
                    st.write("\n".join(f"- {item}" for item in alumnos_edit_errors[:50]))
                    pending_errors = len(alumnos_edit_errors) - 50
                    if pending_errors > 0:
                        st.caption(f"... y {pending_errors} errores mas.")

            if alumnos_edit_rows:
                st.caption(f"Alumnos cargados: {len(alumnos_edit_rows)}")
                with st.expander("Vista previa de alumnos cargados", expanded=False):
                    _show_dataframe(
                        [
                            {
                                "Alumno ID": row.get("alumno_id", ""),
                                "Persona ID": row.get("persona_id", ""),
                                "Alumno": str(row.get("nombre_completo") or "").strip(),
                                "DNI": row.get("id_oficial", ""),
                                "Login": row.get("login", ""),
                                "Nivel": row.get("nivel", ""),
                                "Grado": row.get("grado", ""),
                                "Seccion": row.get("seccion", ""),
                                "Activo": "Si" if bool(row.get("activo")) else "No",
                            }
                            for row in alumnos_edit_rows
                        ],
                        use_container_width=True,
                    )

                alumnos_edit_rows_by_id = {
                    int(row["alumno_id"]): row
                    for row in alumnos_edit_rows
                    if _safe_int(row.get("alumno_id")) is not None
                }
                alumnos_edit_options = list(alumnos_edit_rows_by_id.keys())
                current_selected_alumno_id = _safe_int(
                    st.session_state.get("alumnos_edit_selected_alumno_id")
                )
                if (
                    current_selected_alumno_id is not None
                    and int(current_selected_alumno_id) not in alumnos_edit_options
                ):
                    st.session_state.pop("alumnos_edit_selected_alumno_id", None)

                selected_alumno_id = st.selectbox(
                    "Alumno",
                    options=alumnos_edit_options,
                    index=None,
                    placeholder="Selecciona un alumno",
                    key="alumnos_edit_selected_alumno_id",
                    format_func=lambda alumno_id: _alumno_edit_option_label(
                        alumnos_edit_rows_by_id.get(int(alumno_id), {})
                    ),
                )
                move_dialog_alumno_id = _safe_int(
                    st.session_state.get("alumnos_edit_move_dialog_alumno_id")
                )
                if (
                    move_dialog_alumno_id is not None
                    and (
                        selected_alumno_id is None
                        or int(move_dialog_alumno_id) != int(selected_alumno_id)
                    )
                ):
                    _clear_alumnos_edit_move_state(close_dialog=True)

                if selected_alumno_id is not None:
                    alumno_edit_row = alumnos_edit_rows_by_id.get(int(selected_alumno_id), {})
                    alumno_edit_context = _alumno_edit_context_from_row(alumno_edit_row)
                    if not isinstance(alumno_edit_context, dict):
                        st.error(
                            "No se pudo resolver nivel, grado o grupo actual del alumno."
                        )
                        st.stop()

                    loaded_alumno_id = _safe_int(
                        st.session_state.get("alumnos_edit_loaded_alumno_id")
                    )
                    current_alumno_detail = st.session_state.get("alumnos_edit_detail")
                    if not isinstance(current_alumno_detail, dict):
                        current_alumno_detail = {}

                    if (
                        int(selected_alumno_id) != int(loaded_alumno_id or 0)
                        or not current_alumno_detail
                    ):
                        token = _get_shared_token()
                        if not token:
                            st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                            st.stop()
                        try:
                            colegio_id_int = _parse_colegio_id(colegio_id_raw)
                        except ValueError as exc:
                            st.error(f"Error: {exc}")
                            st.stop()

                        with st.spinner("Cargando detalle del alumno..."):
                            alumno_detail, alumno_detail_msg = _fetch_alumno_edit_detail_web(
                                token=token,
                                colegio_id=int(colegio_id_int),
                                empresa_id=int(empresa_id),
                                ciclo_id=int(ciclo_id),
                                nivel_id=int(alumno_edit_context["nivel_id"]),
                                grado_id=int(alumno_edit_context["grado_id"]),
                                grupo_id=int(alumno_edit_context["grupo_id"]),
                                alumno_id=int(alumno_edit_context["alumno_id"]),
                                timeout=int(timeout),
                            )
                        if alumno_detail is None:
                            st.session_state["alumnos_edit_detail"] = {}
                            st.session_state["alumnos_edit_loaded_alumno_id"] = int(
                                selected_alumno_id
                            )
                            st.session_state["alumnos_edit_fetch_error"] = str(
                                alumno_detail_msg or "No se pudo cargar el detalle."
                            )
                        else:
                            _store_alumno_edit_detail_state(
                                alumno_detail,
                                context=alumno_edit_context,
                            )

                    alumno_edit_fetch_error = str(
                        st.session_state.get("alumnos_edit_fetch_error") or ""
                    ).strip()
                    if alumno_edit_fetch_error:
                        st.error(
                            f"No se pudo cargar el detalle del alumno: {alumno_edit_fetch_error}"
                        )

                    current_alumno_detail = st.session_state.get("alumnos_edit_detail")
                    if isinstance(current_alumno_detail, dict) and current_alumno_detail:
                        persona_detail = (
                            current_alumno_detail.get("persona")
                            if isinstance(current_alumno_detail.get("persona"), dict)
                            else {}
                        )
                        detail_header_cols = st.columns([8.4, 1.6], gap="small")
                        with detail_header_cols[0]:
                            st.caption(
                                "Alumno ID: {alumno_id} | Persona ID: {persona_id} | {nivel} | {grado} | {seccion}".format(
                                    alumno_id=int(alumno_edit_context["alumno_id"]),
                                    persona_id=int(alumno_edit_context["persona_id"]),
                                    nivel=str(alumno_edit_row.get("nivel") or "").strip() or "-",
                                    grado=str(alumno_edit_row.get("grado") or "").strip() or "-",
                                    seccion=str(alumno_edit_row.get("seccion") or "").strip() or "-",
                                )
                            )
                            st.caption(
                                "Login actual: {login}".format(
                                    login=str(
                                        st.session_state.get("alumnos_edit_original_login") or "-"
                                    ).strip()
                                    or "-"
                                )
                            )
                        with detail_header_cols[1]:
                            if st.button(
                                "->",
                                key=f"alumnos_edit_move_open_btn_{int(alumno_edit_context['alumno_id'])}",
                                help="Mover alumno de seccion",
                                use_container_width=True,
                            ):
                                st.session_state["alumnos_edit_move_dialog_alumno_id"] = int(
                                    alumno_edit_context["alumno_id"]
                                )
                                _clear_alumnos_edit_move_state(close_dialog=False)
                        name_col_1, name_col_2, name_col_3 = st.columns(3, gap="small")
                        name_col_1.text_input("Nombre", key="alumnos_edit_nombre")
                        name_col_2.text_input(
                            "Apellido paterno",
                            key="alumnos_edit_apellido_paterno",
                        )
                        name_col_3.text_input(
                            "Apellido materno",
                            key="alumnos_edit_apellido_materno",
                        )

                        sexo_actual_alumno = str(
                            st.session_state.get("alumnos_edit_sexo") or ""
                        ).strip().upper()
                        sexo_alumno_options = ["", "M", "F"]
                        if (
                            sexo_actual_alumno
                            and sexo_actual_alumno not in sexo_alumno_options
                        ):
                            sexo_alumno_options = [sexo_actual_alumno] + sexo_alumno_options

                        data_col_1, data_col_2, data_col_3, data_col_4 = st.columns(
                            4, gap="small"
                        )
                        data_col_1.selectbox(
                            "Sexo",
                            options=sexo_alumno_options,
                            key="alumnos_edit_sexo",
                        )
                        data_col_2.text_input(
                            "DNI / identificador",
                            key="alumnos_edit_dni",
                        )
                        data_col_3.date_input(
                            "Fecha nacimiento",
                            key="alumnos_edit_fecha",
                            format="DD/MM/YYYY",
                        )
                        data_col_4.checkbox(
                            "Extranjero",
                            key="alumnos_edit_extranjero",
                        )

                        cred_col_1, cred_col_2 = st.columns(2, gap="small")
                        cred_col_1.text_input("Login", key="alumnos_edit_login")
                        cred_col_1.caption(
                            "Minimo 6 caracteres. Solo letras, numeros y @ . - _"
                        )
                        cred_col_2.text_input(
                            "Nueva password",
                            key="alumnos_edit_password",
                            type="password",
                        )
                        cred_col_2.caption(
                            "Opcional. Si la completas, tambien actualiza la password."
                        )

                        run_alumno_edit_save = st.button(
                            "Guardar cambios del alumno",
                            type="primary",
                            use_container_width=True,
                            key="alumnos_edit_save_btn",
                        )

                        if run_alumno_edit_save:
                            token = _get_shared_token()
                            if not token:
                                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                                st.stop()
                            try:
                                colegio_id_int = _parse_colegio_id(colegio_id_raw)
                            except ValueError as exc:
                                st.error(f"Error: {exc}")
                                st.stop()

                            alumnos_edit_context_state = st.session_state.get(
                                "alumnos_edit_context"
                            )
                            if not isinstance(alumnos_edit_context_state, dict):
                                alumnos_edit_context_state = dict(alumno_edit_context)

                            nombre_txt = str(
                                st.session_state.get("alumnos_edit_nombre") or ""
                            ).strip()
                            apellido_paterno_txt = str(
                                st.session_state.get("alumnos_edit_apellido_paterno") or ""
                            ).strip()
                            apellido_materno_txt = str(
                                st.session_state.get("alumnos_edit_apellido_materno") or ""
                            ).strip()
                            sexo_txt = str(
                                st.session_state.get("alumnos_edit_sexo") or ""
                            ).strip().upper()
                            dni_txt = str(
                                st.session_state.get("alumnos_edit_dni") or ""
                            ).strip()
                            fecha_txt = st.session_state.get("alumnos_edit_fecha")
                            extranjero_flag = bool(
                                st.session_state.get("alumnos_edit_extranjero", False)
                            )
                            login_txt = str(
                                st.session_state.get("alumnos_edit_login") or ""
                            ).strip()
                            original_login_txt = str(
                                st.session_state.get("alumnos_edit_original_login") or ""
                            ).strip()
                            password_txt = str(
                                st.session_state.get("alumnos_edit_password") or ""
                            )

                            if not nombre_txt:
                                st.error("El nombre del alumno es obligatorio.")
                                st.stop()
                            if sexo_txt not in {"M", "F"}:
                                st.error("Selecciona un sexo valido para el alumno.")
                                st.stop()
                            try:
                                _alumno_birthdate_to_api(fecha_txt)
                            except ValueError as exc:
                                st.error(str(exc))
                                st.stop()

                            if dni_txt:
                                id_ok, id_msg = _validar_identificador_alumno_web(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    nivel_id=int(alumnos_edit_context_state["nivel_id"]),
                                    persona_id=int(alumnos_edit_context_state["persona_id"]),
                                    identificador=dni_txt,
                                    timeout=int(timeout),
                                )
                                if not id_ok:
                                    st.error(
                                        "Identificador invalido: {msg}".format(
                                            msg=id_msg or "sin detalle"
                                        )
                                    )
                                    st.stop()

                            login_changed = login_txt != original_login_txt
                            password_provided = bool(password_txt)
                            if login_changed or password_provided:
                                login_error = _validar_login_reglas(login_txt)
                                if login_error:
                                    st.error(login_error)
                                    st.stop()
                                if password_provided:
                                    password_error = _validar_password_reglas(password_txt)
                                    if password_error:
                                        st.error(password_error)
                                        st.stop()

                                login_ok, login_msg = _validar_login_alumno_web(
                                    token=token,
                                    empresa_id=int(empresa_id),
                                    login=login_txt,
                                    timeout=int(timeout),
                                    persona_id=int(alumnos_edit_context_state["persona_id"]),
                                    grado_id=int(alumnos_edit_context_state["grado_id"]),
                                )
                                if not login_ok:
                                    st.error(
                                        "El login no es valido: {msg}".format(
                                            msg=login_msg or "sin detalle"
                                        )
                                    )
                                    st.stop()

                            with st.spinner("Guardando cambios del alumno..."):
                                alumno_update_ok, _alumno_update_data, alumno_update_msg = _update_alumno_edit_web(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    nivel_id=int(alumnos_edit_context_state["nivel_id"]),
                                    grado_id=int(alumnos_edit_context_state["grado_id"]),
                                    grupo_id=int(alumnos_edit_context_state["grupo_id"]),
                                    alumno_id=int(alumnos_edit_context_state["alumno_id"]),
                                    nombre=nombre_txt,
                                    apellido_paterno=apellido_paterno_txt,
                                    apellido_materno=apellido_materno_txt,
                                    sexo=sexo_txt,
                                    fecha_nacimiento=fecha_txt,
                                    id_oficial=dni_txt,
                                    extranjero=extranjero_flag,
                                    timeout=int(timeout),
                                )
                                if not alumno_update_ok:
                                    st.error(
                                        "No se pudo actualizar el alumno: {msg}".format(
                                            msg=alumno_update_msg or "sin detalle"
                                        )
                                    )
                                    st.stop()

                                alumno_notice_type = "success"
                                alumno_notice_message = "Datos del alumno actualizados."
                                if login_changed or password_provided:
                                    login_update_ok, _login_update_data, login_update_msg = _update_login_alumno_web(
                                        token=token,
                                        colegio_id=int(colegio_id_int),
                                        empresa_id=int(empresa_id),
                                        ciclo_id=int(ciclo_id),
                                        nivel_id=int(alumnos_edit_context_state["nivel_id"]),
                                        grado_id=int(alumnos_edit_context_state["grado_id"]),
                                        grupo_id=int(alumnos_edit_context_state["grupo_id"]),
                                        alumno_id=int(alumnos_edit_context_state["alumno_id"]),
                                        login=login_txt,
                                        password=password_txt,
                                        timeout=int(timeout),
                                    )
                                    if not login_update_ok:
                                        alumno_notice_type = "warning"
                                        alumno_notice_message = (
                                            "Datos base actualizados, pero no se pudo actualizar login/password: {msg}".format(
                                                msg=login_update_msg or "sin detalle"
                                            )
                                        )
                                    else:
                                        alumno_notice_message = (
                                            "Alumno actualizado correctamente."
                                        )

                            refresh_warning = ""
                            refresh_warning = _refresh_alumnos_edit_catalog_and_detail(
                                token=token,
                                colegio_id=int(colegio_id_int),
                                empresa_id=int(empresa_id),
                                ciclo_id=int(ciclo_id),
                                timeout=int(timeout),
                                alumno_id=int(alumnos_edit_context_state["alumno_id"]),
                                fallback_context=dict(alumnos_edit_context_state),
                            )

                            st.session_state["alumnos_edit_notice"] = {
                                "type": alumno_notice_type,
                                "message": f"{alumno_notice_message}{refresh_warning}",
                            }
                            st.rerun()
                        if _safe_int(
                            st.session_state.get("alumnos_edit_move_dialog_alumno_id")
                        ) == int(alumno_edit_context["alumno_id"]):
                            _show_alumno_edit_move_dialog(
                                alumno_row=alumno_edit_row,
                                alumno_context=alumno_edit_context,
                                niveles_data=st.session_state.get("alumnos_edit_niveles") or [],
                                colegio_id=int(
                                    _safe_int(st.session_state.get("alumnos_edit_colegio_id"))
                                    or 0
                                ),
                                empresa_id=int(empresa_id),
                                ciclo_id=int(ciclo_id),
                                timeout=int(timeout),
                            )
            elif st.session_state.get("alumnos_edit_colegio_id"):
                st.warning("No se encontraron alumnos para este colegio.")

        if alumnos_crud_view == "crear":
            with st.container(border=True):
                st.markdown("**4) Crear alumno**")
                st.caption(
                    "Valida DNI, crea el alumno en el grado/seccion elegidos y luego actualiza login/password."
                )

            col_create_load, col_create_clear = st.columns([2, 1], gap="small")
            run_create_load = col_create_load.button(
                "Cargar niveles para crear",
                type="primary",
                key="alumnos_create_load_btn",
                use_container_width=True,
            )
            run_create_clear = col_create_clear.button(
                "Limpiar formulario",
                key="alumnos_create_clear_btn",
                use_container_width=True,
            )

            if run_create_clear:
                for state_key in (
                    "alumnos_create_niveles",
                    "alumnos_create_colegio_id",
                    "alumnos_create_notice",
                ):
                    st.session_state.pop(state_key, None)
                for state_key in list(st.session_state.keys()):
                    if str(state_key).startswith("alumnos_create_"):
                        st.session_state.pop(state_key, None)
                st.rerun()

            if run_create_load:
                token = _get_shared_token()
                if not token:
                    st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                    st.stop()
                try:
                    colegio_id_int = _parse_colegio_id(colegio_id_raw)
                except ValueError as exc:
                    st.error(f"Error: {exc}")
                    st.stop()

                try:
                    with st.spinner("Cargando niveles, grados y secciones..."):
                        niveles_create = _fetch_niveles_grados_grupos_censo(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                except Exception as exc:  # pragma: no cover - UI
                    st.error(f"Error cargando opciones: {exc}")
                    st.stop()

                st.session_state["alumnos_create_niveles"] = niveles_create
                st.session_state["alumnos_create_colegio_id"] = int(colegio_id_int)
                st.success("Opciones cargadas para crear alumno.")

            create_notice = st.session_state.pop("alumnos_create_notice", None)
            if isinstance(create_notice, dict):
                create_notice_type = str(create_notice.get("type") or "").strip().lower()
                create_notice_message = str(create_notice.get("message") or "").strip()
                if create_notice_message:
                    if create_notice_type == "success":
                        st.success(create_notice_message)
                    elif create_notice_type == "warning":
                        st.warning(create_notice_message)
                    else:
                        st.info(create_notice_message)

            create_niveles = st.session_state.get("alumnos_create_niveles")
            create_colegio_id = _safe_int(st.session_state.get("alumnos_create_colegio_id"))
            if (
                not create_niveles
                and loaded_niveles
                and loaded_colegio_id is not None
                and current_colegio_id is not None
                and int(loaded_colegio_id) == int(current_colegio_id)
            ):
                create_niveles = loaded_niveles
                create_colegio_id = int(loaded_colegio_id)

            if (
                create_colegio_id is not None
                and current_colegio_id is not None
                and int(create_colegio_id) != int(current_colegio_id)
            ):
                st.warning("El colegio global cambio. Vuelve a cargar las opciones para crear.")
            elif not create_niveles:
                st.caption("Primero presiona 'Cargar niveles para crear'.")
            else:
                create_catalog = _build_manual_move_destination_catalog(create_niveles)
                create_nivel_ids = create_catalog.get("nivel_ids") or []
                create_nivel_name_by_id = create_catalog.get("nivel_name_by_id") or {}
                create_grado_ids_by_nivel = create_catalog.get("grado_ids_by_nivel") or {}
                create_grado_payload_by_id = create_catalog.get("grado_payload_by_id") or {}
                create_group_by_grade_section = create_catalog.get("grupo_payload_by_grado_seccion") or {}

                create_nivel_key = "alumnos_create_nivel_id"
                create_grado_key = "alumnos_create_grado_id"
                create_grupo_key = "alumnos_create_seccion"
                if create_nivel_key not in st.session_state:
                    st.session_state[create_nivel_key] = None
                if create_grado_key not in st.session_state:
                    st.session_state[create_grado_key] = None
                if create_grupo_key not in st.session_state:
                    st.session_state[create_grupo_key] = None

                create_selected_nivel = _safe_int(st.session_state.get(create_nivel_key))
                if create_selected_nivel not in create_nivel_ids:
                    st.session_state[create_nivel_key] = None
                    create_selected_nivel = None

                create_grado_options = []
                if create_selected_nivel is not None:
                    create_grado_options = [
                        int(value)
                        for value in (create_grado_ids_by_nivel.get(int(create_selected_nivel)) or [])
                    ]

                create_selected_grado = _safe_int(st.session_state.get(create_grado_key))
                if create_selected_grado not in create_grado_options:
                    st.session_state[create_grado_key] = None
                    create_selected_grado = None

                create_seccion_options: List[str] = []
                if create_selected_grado is not None:
                    create_seccion_options = sorted(
                        [
                            seccion
                            for (grado_id_tmp, seccion), payload in create_group_by_grade_section.items()
                            if int(grado_id_tmp) == int(create_selected_grado)
                            and isinstance(payload, dict)
                            and payload
                        ],
                        key=lambda value: _grupo_sort_key(str(value), str(value)),
                    )

                create_selected_seccion = _normalize_seccion_key(
                    st.session_state.get(create_grupo_key) or ""
                )
                if (
                    create_selected_seccion
                    and create_selected_seccion not in create_seccion_options
                ):
                    st.session_state[create_grupo_key] = None

                dest_col_1, dest_col_2, dest_col_3 = st.columns(3, gap="small")
                with dest_col_1:
                    st.selectbox(
                        "Nivel destino",
                        options=create_nivel_ids,
                        index=None,
                        placeholder="Nivel",
                        format_func=lambda value: str(
                            create_nivel_name_by_id.get(int(value), value)
                        ).strip(),
                        key=create_nivel_key,
                        on_change=_clear_manual_move_selection,
                        args=(create_grado_key, create_grupo_key),
                    )
                selected_nivel_form = _safe_int(st.session_state.get(create_nivel_key))
                grado_options_form = []
                if selected_nivel_form is not None:
                    grado_options_form = [
                        int(value)
                        for value in (create_grado_ids_by_nivel.get(int(selected_nivel_form)) or [])
                    ]
                with dest_col_2:
                    st.selectbox(
                        "Grado destino",
                        options=grado_options_form,
                        index=None,
                        placeholder="Grado",
                        format_func=lambda value: str(
                            (create_grado_payload_by_id.get(int(value), {}) or {}).get("grado")
                            or value
                        ).strip(),
                        key=create_grado_key,
                        on_change=_clear_manual_move_selection,
                        args=(create_grupo_key,),
                        disabled=not grado_options_form,
                    )
                selected_grado_form = _safe_int(st.session_state.get(create_grado_key))
                seccion_options_form: List[str] = []
                if selected_grado_form is not None:
                    seccion_options_form = sorted(
                        [
                            seccion
                            for (grado_id_tmp, seccion), payload in create_group_by_grade_section.items()
                            if int(grado_id_tmp) == int(selected_grado_form)
                            and isinstance(payload, dict)
                            and payload
                        ],
                        key=lambda value: _grupo_sort_key(str(value), str(value)),
                    )
                with dest_col_3:
                    st.selectbox(
                        "Seccion destino",
                        options=seccion_options_form,
                        index=None,
                        placeholder="Seccion",
                        key=create_grupo_key,
                        disabled=not seccion_options_form,
                    )

                name_col_1, name_col_2, name_col_3 = st.columns(3, gap="small")
                name_col_1.text_input("Nombre", key="alumnos_create_nombre")
                name_col_2.text_input(
                    "Apellido paterno",
                    key="alumnos_create_apellido_paterno",
                )
                name_col_3.text_input(
                    "Apellido materno",
                    key="alumnos_create_apellido_materno",
                )

                data_col_1, data_col_2, data_col_3, data_col_4 = st.columns(4, gap="small")
                data_col_1.text_input(
                    "DNI / identificador",
                    key="alumnos_create_dni",
                )
                data_col_2.selectbox(
                    "Sexo",
                    options=["M", "F"],
                    index=None,
                    placeholder="Sexo",
                    key="alumnos_create_sexo",
                )
                data_col_3.date_input(
                    "Fecha nacimiento",
                    value=date.today(),
                    format="DD/MM/YYYY",
                    key="alumnos_create_fecha",
                )
                data_col_4.checkbox(
                    "Extranjero",
                    key="alumnos_create_extranjero",
                )

                cred_col_1, cred_col_2 = st.columns(2, gap="small")
                cred_col_1.text_input(
                    "Login",
                    key="alumnos_create_login",
                )
                cred_col_1.caption(
                    "Minimo 6 caracteres. Solo letras, numeros y @ . - _"
                )
                cred_col_2.text_input(
                    "Password",
                    type="password",
                    key="alumnos_create_password",
                )
                cred_col_2.caption(
                    "Minimo 6 caracteres. Solo letras y numeros."
                )

                create_submit = st.button(
                    "Crear alumno",
                    type="primary",
                    use_container_width=True,
                    key="alumnos_create_submit_btn",
                )

                if create_submit:
                    token = _get_shared_token()
                    if not token:
                        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                        st.stop()
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                        st.stop()

                    selected_nivel_id = _safe_int(st.session_state.get(create_nivel_key))
                    selected_grado_id = _safe_int(st.session_state.get(create_grado_key))
                    selected_seccion = _normalize_seccion_key(
                        st.session_state.get(create_grupo_key) or ""
                    )
                    if (
                        selected_nivel_id is None
                        or selected_grado_id is None
                        or not selected_seccion
                    ):
                        st.error("Completa nivel, grado y seccion destino.")
                        st.stop()

                    destino_payload = create_group_by_grade_section.get(
                        (int(selected_grado_id), selected_seccion)
                    )
                    if not isinstance(destino_payload, dict):
                        st.error("No se pudo resolver la seccion destino.")
                        st.stop()

                    grado_payload = create_grado_payload_by_id.get(int(selected_grado_id), {})
                    grado_nivel_id = _safe_int(grado_payload.get("nivel_id"))
                    if grado_nivel_id is None or int(grado_nivel_id) != int(selected_nivel_id):
                        st.error("El grado no corresponde al nivel seleccionado.")
                        st.stop()

                    nombre_txt = str(st.session_state.get("alumnos_create_nombre") or "").strip()
                    ap_pat_txt = str(st.session_state.get("alumnos_create_apellido_paterno") or "").strip()
                    ap_mat_txt = str(st.session_state.get("alumnos_create_apellido_materno") or "").strip()
                    dni_txt = re.sub(r"\D", "", str(st.session_state.get("alumnos_create_dni") or ""))
                    sexo_txt = str(st.session_state.get("alumnos_create_sexo") or "").strip().upper()
                    fecha_txt = str(st.session_state.get("alumnos_create_fecha") or "").strip()
                    login_txt = str(st.session_state.get("alumnos_create_login") or "").strip()
                    password_txt = str(st.session_state.get("alumnos_create_password") or "")
                    extranjero_flag = bool(st.session_state.get("alumnos_create_extranjero", False))

                    if not all([nombre_txt, ap_pat_txt, ap_mat_txt, dni_txt, sexo_txt, fecha_txt, login_txt, password_txt]):
                        st.error("Completa todos los campos obligatorios.")
                        st.stop()

                    login_error = _validar_login_reglas(login_txt)
                    if login_error:
                        st.error(login_error)
                        st.stop()

                    password_error = _validar_password_reglas(password_txt)
                    if password_error:
                        st.error(password_error)
                        st.stop()

                    try:
                        _alumno_birthdate_to_api(fecha_txt)
                    except ValueError as exc:
                        st.error(str(exc))
                        st.stop()

                    with st.spinner("Creando alumno..."):
                        existing_students = []
                        if (
                            loaded_colegio_id is not None
                            and int(loaded_colegio_id) == int(colegio_id_int)
                        ):
                            existing_students = st.session_state.get("alumnos_manual_move_students") or []
                        if not existing_students:
                            try:
                                existing_catalog = _fetch_alumnos_catalog_for_manual_move(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                )
                            except Exception:
                                existing_students = []
                            else:
                                existing_students = existing_catalog.get("students") or []

                        existing_row = _find_existing_alumno_by_identificador(
                            existing_students,
                            dni_txt,
                        )
                        if isinstance(existing_row, dict):
                            existing_nivel = str(existing_row.get("nivel") or "").strip()
                            existing_grado = str(existing_row.get("grado") or "").strip()
                            existing_seccion = _normalize_seccion_key(
                                existing_row.get("seccion_norm") or existing_row.get("seccion") or ""
                            )
                            st.error(
                                "El alumno ya existe en {nivel} | {grado} ({seccion}). "
                                "No se puede crear duplicado; usa el boton de mover en Editar.".format(
                                    nivel=existing_nivel or "-",
                                    grado=existing_grado or "-",
                                    seccion=existing_seccion or "-",
                                )
                            )
                            st.stop()

                        id_ok, id_msg = _validar_identificador_alumno_web(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            nivel_id=int(selected_nivel_id),
                            identificador=dni_txt,
                            timeout=int(timeout),
                        )
                        if not id_ok:
                            st.error(f"Identificador invalido: {id_msg}")
                            st.stop()

                        login_ok, login_msg = _validar_login_alumno_web(
                            token=token,
                            empresa_id=int(empresa_id),
                            login=login_txt,
                            timeout=int(timeout),
                        )
                        if not login_ok:
                            st.error(
                                "El login no es valido. No se creo el alumno: {msg}".format(
                                    msg=login_msg or "sin detalle"
                                )
                            )
                            st.stop()

                        created_ok, created_data, created_msg = _crear_alumno_web(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            nivel_id=int(selected_nivel_id),
                            grado_id=int(selected_grado_id),
                            grupo_id=int(destino_payload.get("grupo_id") or 0),
                            nombre=nombre_txt,
                            apellido_paterno=ap_pat_txt,
                            apellido_materno=ap_mat_txt,
                            sexo=sexo_txt,
                            fecha_nacimiento=fecha_txt,
                            id_oficial=dni_txt,
                            extranjero=extranjero_flag,
                            timeout=int(timeout),
                        )
                        if not created_ok:
                            st.error(f"No se pudo crear el alumno: {created_msg}")
                            st.stop()

                        alumno_id_created = _safe_int(created_data.get("alumnoId"))
                        if alumno_id_created is None:
                            st.error("El alta no devolvio alumnoId valido.")
                            st.stop()

                        update_ok, update_data, update_msg = _update_login_alumno_web(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            nivel_id=int(selected_nivel_id),
                            grado_id=int(selected_grado_id),
                            grupo_id=int(destino_payload.get("grupo_id") or 0),
                            alumno_id=int(alumno_id_created),
                            login=login_txt,
                            password=password_txt,
                            timeout=int(timeout),
                        )
                        if not update_ok:
                            st.session_state["alumnos_create_notice"] = {
                                "type": "warning",
                                "message": (
                                    "Alumno creado, pero no se pudo actualizar login/password: {msg}".format(
                                        msg=update_msg or "sin detalle"
                                    )
                                ),
                            }
                            st.rerun()

                    created_row = _flatten_censo_alumno_for_auto_plan(
                        item=update_data or created_data,
                        fallback={
                            "nivel_id": int(selected_nivel_id),
                            "nivel": str(destino_payload.get("nivel") or "").strip(),
                            "grado_id": int(selected_grado_id),
                            "grado": str(destino_payload.get("grado") or "").strip(),
                            "grupo_id": int(destino_payload.get("grupo_id") or 0),
                            "seccion": str(destino_payload.get("seccion") or "").strip(),
                        },
                    )
                    created_row["login"] = str(login_txt).strip()
                    created_row["password"] = str(password_txt)
                    if (
                        loaded_colegio_id is not None
                        and int(loaded_colegio_id) == int(colegio_id_int)
                    ):
                        cached_students = st.session_state.get("alumnos_manual_move_students") or []
                        cached_students.append(created_row)
                        cached_students.sort(
                            key=lambda row: (
                                int(_safe_int(row.get("nivel_id")) or 0),
                                int(_safe_int(row.get("grado_id")) or 0),
                                _grupo_sort_key(
                                    str(row.get("seccion_norm") or ""),
                                    str(row.get("seccion") or ""),
                                ),
                                str(row.get("apellido_paterno") or "").upper(),
                                str(row.get("apellido_materno") or "").upper(),
                                str(row.get("nombre") or "").upper(),
                            ),
                        )
                        st.session_state["alumnos_manual_move_students"] = cached_students

                    _queue_manual_move_reset(
                        create_nivel_key,
                        create_grado_key,
                        create_grupo_key,
                        "alumnos_create_nombre",
                        "alumnos_create_apellido_paterno",
                        "alumnos_create_apellido_materno",
                        "alumnos_create_dni",
                        "alumnos_create_sexo",
                        "alumnos_create_fecha",
                        "alumnos_create_extranjero",
                        "alumnos_create_login",
                        "alumnos_create_password",
                    )
                    st.session_state["alumnos_create_notice"] = {
                        "type": "success",
                        "message": (
                            "Alumno creado: {nombre} | {dni} | {login}".format(
                                nombre=str(created_row.get("nombre_completo") or nombre_txt).strip(),
                                dni=dni_txt,
                                login=login_txt,
                            )
                        ),
                    }
                    st.rerun()

