import os
import base64
import json
import re
import csv
import tempfile
import threading
import traceback
import unicodedata
from datetime import date, datetime
from html import escape
from io import BytesIO, StringIO
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import unquote, urljoin
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
try:
    from st_keyup import st_keyup
except ModuleNotFoundError:
    def st_keyup(
        label: str,
        value: str = "",
        key: Optional[str] = None,
        placeholder: Optional[str] = None,
        debounce: int = 0,
        **kwargs,
    ) -> str:
        return str(
            st.text_input(
                label,
                value=value,
                key=key,
                placeholder=placeholder,
                **kwargs,
            )
        )

from santillana_format.jira import render_jira_focus_web
from santillana_format.loqueleo import render_loqueleo_view as render_loqueleo_domain_view
from santillana_format.richmond import (
    read_richmondstudio_browser_token,
    render_richmond_studio_view,
)
from santillana_format.sumun import (
    generate_sumun_template_from_excel,
    inspect_sumun_workbook_sheets,
)
from santillana_format.pegasus import (
    ALUMNOS_CICLO_ID_DEFAULT,
    CODE_COLUMN_NAME,
    COMPARE_MODE_AMBOS,
    COMPARE_MODE_APELLIDOS,
    COMPARE_MODE_DNI,
    DEFAULT_EMPRESA_ID,
    OUTPUT_FILENAME,
    PROFESORES_CICLO_ID_DEFAULT,
    SHEET_NAME,
    actualizar_passwords_docentes,
    asignar_profesores_clases,
    build_profesores_bd_filename,
    comparar_plantillas_detalle,
    descargar_plantilla_edicion_masiva,
    export_profesores_bd_excel,
    export_profesores_excel,
    listar_profesores_bd_data,
    listar_profesores_data,
    listar_profesores_filters_data,
    listar_y_mapear_clases,
    process_excel,
)

APP_ASSETS_DIR = Path(__file__).resolve().parent / "assets"
APP_TAB_LOGO_PATH = APP_ASSETS_DIR / "tab_logo.png"
APP_NAVBAR_LOGO_PATH = APP_ASSETS_DIR / "navbar_logo.png"
APP_EXTENSION_DIR = Path(__file__).resolve().parent / "browser_extension" / "santillana_session_helper"

PROFESORES_COMPARE_IMPORT_ERROR = ""
try:
    from santillana_format.pegasus.profesores_compare import (
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
    from santillana_format.pegasus.profesores_manual import (
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
GESTION_ESCOLAR_STAFF_CLASE_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases/{clase_id}/staff"
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
CENSO_PROFESOR_ACTIVAR_INACTIVAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/profesores/{persona_id}/activarInactivar"
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
CENSO_ACTIVOS_EXPORT_COLUMNS = [
    "Nivel",
    "Grado",
    "Grupo",
    "Nombre del alumno",
    "DNI",
    "Login",
    "Password",
]
CENSO_PROFESORES_ACTIVOS_EXPORT_COLUMNS = [
    "Nombre",
    "Apellido paterno",
    "Apellido materno",
    "Login",
]
RESTRICTED_SECTIONS_PASSWORD = "Ted2026"
RESTRICTED_SECTIONS_ENABLED = False
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
LOQUELEO_SESSION_BRIDGE_PENDING = "__pending__"
LOQUELEO_SESSION_BRIDGE_COMPONENT = components.declare_component(
    "loqueleo_session_bridge",
    path=str(Path(__file__).resolve().parent / "components" / "loqueleo_session_bridge"),
)
_PARTICIPANTES_SYNC_STATUS_LIMIT = 12


@st.cache_resource
def _get_participantes_sync_state() -> Dict[str, object]:
    return {
        "lock": threading.Lock(),
        "jobs": {},
        "scope_to_job": {},
    }








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


def _read_browser_loqueleo_session_id(mode: str = "read", value: object = "") -> str:
    try:
        browser_value = LOQUELEO_SESSION_BRIDGE_COMPONENT(
            key="loqueleo_session_bridge_component",
            default=LOQUELEO_SESSION_BRIDGE_PENDING,
            mode=str(mode or "read").strip().lower() or "read",
            value=str(value or "").strip(),
        )
    except Exception:
        return ""
    if str(browser_value or "") == LOQUELEO_SESSION_BRIDGE_PENDING:
        return LOQUELEO_SESSION_BRIDGE_PENDING
    return str(browser_value or "").strip()


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
        <style>
        section.main > div[data-testid="stMainBlockContainer"],
        div[data-testid="stAppViewContainer"] .main .block-container {
            padding-top: 3rem;
        }
        div[data-testid="stButton"],
        div[data-testid="stDownloadButton"] {
            display: flex;
            justify-content: center;
        }
        div[data-testid="stButton"] > button,
        div[data-testid="stDownloadButton"] > button {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            text-align: center;
            margin-left: auto;
            margin-right: auto;
            line-height: 1.2;
        }
        div[data-testid="stButton"] > button > div,
        div[data-testid="stDownloadButton"] > button > div,
        div[data-testid="stButton"] > button > span,
        div[data-testid="stDownloadButton"] > button > span {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            text-align: center;
            width: 100%;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _inject_selectbox_title_cleanup() -> None:
    components.html(
        """
        <script>
        (function () {
          const selectors = [
            'div[data-baseweb="select"] [title]',
            'div[data-baseweb="select"] [aria-label][title]',
            'li[role="option"][title]',
            'li[role="option"] [title]',
            '[role="listbox"] [title]'
          ];

          function stripTitles(doc) {
            if (!doc) return;
            doc.querySelectorAll(selectors.join(',')).forEach((node) => {
              if (node && node.hasAttribute && node.hasAttribute('title')) {
                node.removeAttribute('title');
              }
            });
          }

          function boot() {
            const parentDoc = window.parent && window.parent.document;
            if (!parentDoc || !parentDoc.body) return;
            stripTitles(parentDoc);
            const observer = new MutationObserver(() => stripTitles(parentDoc));
            observer.observe(parentDoc.body, {
              childList: true,
              subtree: true,
              attributes: true,
              attributeFilter: ['title']
            });
          }

          boot();
        })();
        </script>
        """,
        height=0,
        width=0,
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


def _extract_snapshot_source(payload: object, source_id: str) -> Dict[str, object]:
    if not isinstance(payload, dict):
        return {}
    sources = payload.get("sources")
    if not isinstance(sources, dict):
        return {}
    source = sources.get(source_id)
    if not isinstance(source, dict):
        return {}
    return source


def _extract_snapshot_text(payload: object, source_id: str, key: str) -> str:
    source = _extract_snapshot_source(payload, source_id)
    return str(source.get(key) or "").strip()


def _extract_loqueleo_session_id_from_source(source: object) -> str:
    if not isinstance(source, dict):
        return ""

    direct_session_id = str(source.get("sessionId") or "").strip()
    if direct_session_id:
        return direct_session_id

    cookie_header = str(source.get("cookieHeader") or "").strip()
    if cookie_header:
        for chunk in cookie_header.split(";"):
            name, separator, value = chunk.strip().partition("=")
            if separator and name.strip() == "_session_id":
                return value.strip()

    cookies = source.get("cookies")
    if isinstance(cookies, list):
        for item in cookies:
            if not isinstance(item, dict):
                continue
            if str(item.get("name") or "").strip() == "_session_id":
                return str(item.get("value") or "").strip()
    return ""


def _mask_secret_value(value: object, visible_chars: int = 6) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= visible_chars * 2:
        return text
    return f"{text[:visible_chars]}...{text[-visible_chars:]}"


def _sync_token_reader_bridge_state() -> None:
    pegasus_bridge_mode = str(
        st.session_state.get("shared_pegasus_token_bridge_mode") or "read"
    ).strip().lower() or "read"
    pegasus_bridge_value = _clean_token_value(
        st.session_state.get("shared_pegasus_token_bridge_value", "")
    )
    browser_pegasus_token = _read_browser_pegasus_token(
        mode=pegasus_bridge_mode,
        value=pegasus_bridge_value,
    )
    if pegasus_bridge_mode != "read":
        st.session_state["shared_pegasus_token_bridge_mode"] = "read"
        st.session_state["shared_pegasus_token_bridge_value"] = ""
    if (
        browser_pegasus_token not in ("", PEGASUS_TOKEN_BRIDGE_PENDING)
        and not _clean_token_value(st.session_state.get("shared_pegasus_token", ""))
    ):
        st.session_state["shared_pegasus_token"] = browser_pegasus_token
        st.session_state["shared_pegasus_token_input"] = browser_pegasus_token

    richmond_bridge_mode = str(
        st.session_state.get("rs_bearer_token_bridge_mode") or "read"
    ).strip().lower() or "read"
    richmond_bridge_value = _clean_token_value(
        st.session_state.get("rs_bearer_token_bridge_value", "")
    )
    browser_richmond_token = read_richmondstudio_browser_token(
        mode=richmond_bridge_mode,
        value=richmond_bridge_value,
    )
    if richmond_bridge_mode != "read":
        st.session_state["rs_bearer_token_bridge_mode"] = "read"
        st.session_state["rs_bearer_token_bridge_value"] = ""
    if browser_richmond_token not in ("", "__pending__"):
        if not _clean_token_value(st.session_state.get("rs_groups_bearer_token", "")):
            st.session_state["rs_groups_bearer_token"] = browser_richmond_token
        if not _clean_token_value(st.session_state.get("rs_bearer_token", "")):
            st.session_state["rs_bearer_token"] = browser_richmond_token
        if not _clean_token_value(
            st.session_state.get("rs_groups_bearer_token_input", "")
        ):
            st.session_state["rs_groups_bearer_token_input"] = browser_richmond_token

    loqueleo_bridge_mode = str(
        st.session_state.get("loqueleo_session_bridge_mode") or "read"
    ).strip().lower() or "read"
    loqueleo_bridge_value = str(
        st.session_state.get("loqueleo_session_bridge_value", "") or ""
    ).strip()
    browser_loqueleo_session_id = _read_browser_loqueleo_session_id(
        mode=loqueleo_bridge_mode,
        value=loqueleo_bridge_value,
    )
    if loqueleo_bridge_mode != "read":
        st.session_state["loqueleo_session_bridge_mode"] = "read"
        st.session_state["loqueleo_session_bridge_value"] = ""
    if (
        browser_loqueleo_session_id not in ("", LOQUELEO_SESSION_BRIDGE_PENDING)
        and not str(st.session_state.get("loqueleo_session_id", "") or "").strip()
    ):
        st.session_state["loqueleo_session_id"] = browser_loqueleo_session_id
        st.session_state["loqueleo_session_id_input"] = browser_loqueleo_session_id


def _apply_token_snapshot_payload(payload: Dict[str, object]) -> Dict[str, str]:
    read_at = str(payload.get("readAt") or "").strip()
    sources = payload.get("sources")
    if not isinstance(sources, dict):
        raise ValueError("El JSON no contiene un objeto `sources` válido.")

    summary: Dict[str, str] = {}

    pegasus_token = _clean_token_value(_extract_snapshot_text(payload, "pegasus", "value"))
    if pegasus_token:
        previous_pegasus_token = _clean_token_value(
            st.session_state.get("shared_pegasus_token", "")
        )
        st.session_state["shared_pegasus_token"] = pegasus_token
        st.session_state["shared_pegasus_token_input"] = pegasus_token
        st.session_state["shared_pegasus_token_bridge_mode"] = "write"
        st.session_state["shared_pegasus_token_bridge_value"] = pegasus_token
        if pegasus_token != previous_pegasus_token:
            _clear_shared_colegios_cache(clear_selection=False)
        summary["pegasus"] = "guardado"
    else:
        summary["pegasus"] = "sin valor"

    richmond_token = _clean_token_value(_extract_snapshot_text(payload, "richmond", "value"))
    if richmond_token:
        st.session_state["rs_groups_bearer_token"] = richmond_token
        st.session_state["rs_bearer_token"] = richmond_token
        st.session_state["rs_groups_bearer_token_input"] = richmond_token
        st.session_state["rs_bearer_token_bridge_mode"] = "write"
        st.session_state["rs_bearer_token_bridge_value"] = richmond_token
        summary["richmond"] = "guardado"
    else:
        summary["richmond"] = "sin valor"

    loqueleo_source = _extract_snapshot_source(payload, "loqueleo")
    loqueleo_session_id = _extract_loqueleo_session_id_from_source(loqueleo_source)
    loqueleo_cookie_header = str(loqueleo_source.get("cookieHeader") or "").strip()
    loqueleo_cookie_items = loqueleo_source.get("cookies")
    if loqueleo_session_id:
        st.session_state["loqueleo_session_id"] = loqueleo_session_id
        st.session_state["loqueleo_session_id_input"] = loqueleo_session_id
        st.session_state["loqueleo_session_bridge_mode"] = "write"
        st.session_state["loqueleo_session_bridge_value"] = loqueleo_session_id
        summary["loqueleo"] = "guardado"
    else:
        summary["loqueleo"] = "sin valor"
    st.session_state["loqueleo_cookie_header"] = loqueleo_cookie_header
    if isinstance(loqueleo_cookie_items, list):
        st.session_state["loqueleo_cookie_items"] = loqueleo_cookie_items

    if read_at:
        st.session_state["token_reader_last_read_at"] = read_at
    st.session_state["token_reader_last_snapshot_json"] = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
    )
    return summary


def _build_token_reader_status_rows() -> List[Dict[str, str]]:
    return [
        {
            "Seccion": "Procesos Pegasus",
            "Estado": (
                "Guardado"
                if _clean_token_value(st.session_state.get("shared_pegasus_token", ""))
                else "Sin dato"
            ),
            "Valor": _mask_secret_value(
                _clean_token_value(st.session_state.get("shared_pegasus_token", ""))
            ),
        },
        {
            "Seccion": "Richmond Studio",
            "Estado": (
                "Guardado"
                if _clean_token_value(st.session_state.get("rs_groups_bearer_token", ""))
                else "Sin dato"
            ),
            "Valor": _mask_secret_value(
                _clean_token_value(st.session_state.get("rs_groups_bearer_token", ""))
            ),
        },
        {
            "Seccion": "Loqueleo",
            "Estado": (
                "Guardado"
                if str(st.session_state.get("loqueleo_session_id", "") or "").strip()
                else "Sin dato"
            ),
            "Valor": _mask_secret_value(
                str(st.session_state.get("loqueleo_session_id", "") or "").strip()
            ),
        },
    ]


def _apply_pending_token_reader_snapshot_input() -> None:
    pending_snapshot = st.session_state.pop(
        "token_reader_snapshot_input_pending",
        None,
    )
    if pending_snapshot is None:
        return
    st.session_state["token_reader_snapshot_input"] = str(pending_snapshot)


def _render_token_reader_view() -> None:
    _sync_token_reader_bridge_state()
    _apply_pending_token_reader_snapshot_input()

    st.subheader("Lectura Tokens")
    st.caption(
        "Pega el JSON completo exportado por la extension para guardar Pegasus, Richmond y Loqueleo en esta app."
    )

    success_notice = str(st.session_state.pop("token_reader_success_notice", "") or "").strip()
    error_notice = str(st.session_state.pop("token_reader_error_notice", "") or "").strip()
    if success_notice:
        st.success(success_notice)
    if error_notice:
        st.error(error_notice)

    last_read_at = str(st.session_state.get("token_reader_last_read_at", "") or "").strip()
    if last_read_at:
        st.caption(f"Ultima lectura importada: {last_read_at}")

    with st.container(border=True):
        snapshot_text = st.text_area(
            "JSON de la extension",
            key="token_reader_snapshot_input",
            height=360,
            placeholder='{"readAt":"...","sources":{...}}',
        )
        col_save, col_load_last = st.columns([1.25, 1], gap="small")
        if col_save.button(
            "Guardar desde JSON",
            type="primary",
            key="token_reader_save_json_btn",
            use_container_width=True,
        ):
            try:
                parsed_payload = json.loads(str(snapshot_text or "").strip())
                if not isinstance(parsed_payload, dict):
                    raise ValueError("El JSON debe ser un objeto en la raiz.")
                summary = _apply_token_snapshot_payload(parsed_payload)
            except json.JSONDecodeError as exc:
                st.session_state["token_reader_error_notice"] = f"JSON invalido: {exc.msg}"
            except ValueError as exc:
                st.session_state["token_reader_error_notice"] = str(exc)
            else:
                st.session_state["token_reader_snapshot_input_pending"] = json.dumps(
                    parsed_payload,
                    ensure_ascii=False,
                    indent=2,
                )
                st.session_state["token_reader_success_notice"] = (
                    "Guardado completado. "
                    f"Pegasus: {summary['pegasus']} | "
                    f"Richmond: {summary['richmond']} | "
                    f"Loqueleo: {summary['loqueleo']}."
                )
            st.rerun()
        if col_load_last.button(
            "Cargar ultimo JSON",
            key="token_reader_load_last_json_btn",
            use_container_width=True,
        ):
            last_snapshot_json = str(
                st.session_state.get("token_reader_last_snapshot_json", "") or ""
            )
            if last_snapshot_json:
                st.session_state["token_reader_snapshot_input_pending"] = last_snapshot_json
            st.rerun()

    st.markdown("**Estado actual**")
    token_status_df = pd.DataFrame(_build_token_reader_status_rows())
    if not token_status_df.empty:
        token_status_df.index = range(1, len(token_status_df) + 1)
    st.dataframe(token_status_df, use_container_width=True)

    loqueleo_cookie_header = str(
        st.session_state.get("loqueleo_cookie_header", "") or ""
    ).strip()
    if loqueleo_cookie_header:
        st.caption("Cookie header de Loqueleo guardado en sesion.")


@st.cache_data(show_spinner=False)
def _build_browser_extension_zip() -> Optional[bytes]:
    if not APP_EXTENSION_DIR.exists() or not APP_EXTENSION_DIR.is_dir():
        return None

    buffer = BytesIO()
    with ZipFile(buffer, mode="w", compression=ZIP_DEFLATED) as zip_file:
        for file_path in sorted(APP_EXTENSION_DIR.rglob("*")):
            if not file_path.is_file():
                continue
            arcname = file_path.relative_to(APP_EXTENSION_DIR)
            zip_file.write(file_path, arcname.as_posix())
    buffer.seek(0)
    return buffer.getvalue()


@st.cache_data(show_spinner=False)
def _read_binary_file_base64(file_path: str) -> str:
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        return ""
    return base64.b64encode(path.read_bytes()).decode("ascii")


def _render_logo_with_hidden_extension_download() -> None:
    if not APP_NAVBAR_LOGO_PATH.exists():
        return

    extension_zip = _build_browser_extension_zip()
    if not extension_zip:
        st.image(str(APP_NAVBAR_LOGO_PATH), width=150)
        return

    logo_base64 = _read_binary_file_base64(str(APP_NAVBAR_LOGO_PATH))
    if not logo_base64:
        st.image(str(APP_NAVBAR_LOGO_PATH), width=150)
        return

    extension_base64 = base64.b64encode(extension_zip).decode("ascii")
    st.markdown(
        (
            f'<a href="data:application/zip;base64,{extension_base64}" '
            'download="santillana_session_helper.zip" '
            'title="Descargar extension" '
            'style="display:inline-block;text-decoration:none;">'
            f'<img src="data:image/png;base64,{logo_base64}" '
            'alt="SANTED" '
            'style="width:150px;max-width:100%;display:block;" />'
            '</a>'
        ),
        unsafe_allow_html=True,
    )


def _render_sumun_template_view() -> None:
    st.subheader("SUMUN")
    st.markdown("**Generar plantilla de carga**")
    st.caption("Sube una matriz SUMUN y genera la plantilla plana de carga.")
    uploaded_sumun = st.file_uploader(
        "Excel matriz SUMUN",
        type=["xlsx"],
        key="sumun_matrix_upload",
        help="Puede tener todos los itinerarios en la primera hoja o hitos repartidos en varias hojas.",
    )

    selected_sumun_sheet_names: List[str] = []
    sumun_sheets = []
    if uploaded_sumun is not None:
        sumun_upload_bytes = uploaded_sumun.getvalue()
        try:
            sumun_sheets = inspect_sumun_workbook_sheets(sumun_upload_bytes)
        except Exception as exc:  # pragma: no cover - UI
            sumun_sheets = []
            st.error(f"No se pudieron leer las hojas del Excel: {exc}")

        if sumun_sheets:
            st.dataframe(
                [
                    {
                        "Indice": item.index,
                        "Hoja": item.sheet_name,
                        "Matriz detectada": "Si" if item.detected else "No",
                        "Filas estimadas": item.estimated_rows,
                        "Detalle": item.reason,
                    }
                    for item in sumun_sheets
                ],
                use_container_width=True,
                hide_index=True,
            )
            selected_sumun_sheet_names = [item.sheet_name for item in sumun_sheets]

    if st.button("Generar plantilla SUMUN", type="primary", key="sumun_generate_btn"):
        if uploaded_sumun is None:
            st.error("Sube un Excel de matriz SUMUN.")
            st.stop()
        if not selected_sumun_sheet_names:
            st.error("No se pudieron identificar hojas para procesar.")
            st.stop()

        try:
            output_bytes, summary = generate_sumun_template_from_excel(
                uploaded_sumun.getvalue(),
                source_name=uploaded_sumun.name or "matriz_sumun.xlsx",
                sheet_names=selected_sumun_sheet_names,
            )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"No se pudo generar la plantilla SUMUN: {exc}")
            st.stop()

        source_stem = Path(uploaded_sumun.name or "matriz_sumun").stem
        download_name = f"plantilla_carga_matrices_sumun_{source_stem}.xlsx"
        st.session_state["sumun_output_bytes"] = output_bytes
        st.session_state["sumun_output_name"] = download_name
        st.session_state["sumun_summary"] = summary.to_dict()
        st.success(
            "Plantilla lista. Filas: {rows}. Prefijo ID: {prefix}.".format(
                rows=summary.generated_rows,
                prefix=summary.prefix,
            )
        )

    sumun_output_bytes = st.session_state.get("sumun_output_bytes") or b""
    sumun_output_name = str(
        st.session_state.get("sumun_output_name")
        or "plantilla_carga_matrices_sumun.xlsx"
    )
    sumun_summary = st.session_state.get("sumun_summary") or {}
    if sumun_summary:
        summary_cols = st.columns(4)
        summary_cols[0].metric("Filas", int(sumun_summary.get("generated_rows") or 0))
        summary_cols[1].metric("Prefijo", str(sumun_summary.get("prefix") or ""))
        summary_cols[2].metric(
            "Hojas", len(sumun_summary.get("processed_sheets") or [])
        )
        summary_cols[3].metric("Micro", int(sumun_summary.get("micro_count") or 0))
        unique_micro_count = int(sumun_summary.get("unique_micro_count") or 0)
        if unique_micro_count:
            st.caption(f"Micro unicas: {unique_micro_count}")
        st.caption(
            "Cada celda con valor en RECORDAR/COMPRENDER/APLICAR/ANALIZAR/EVALUAR/CREAR genera una sola fila. "
            "El texto interno de la celda no se divide."
        )

        rows_by_sheet = sumun_summary.get("rows_by_sheet") or {}
        if rows_by_sheet:
            st.markdown("**Hojas procesadas**")
            st.dataframe(
                [
                    {"Hoja": sheet_name, "Filas generadas": row_count}
                    for sheet_name, row_count in rows_by_sheet.items()
                ],
                use_container_width=True,
                hide_index=True,
            )
        specific_rows_by_itinerary = sumun_summary.get("specific_rows_by_itinerary") or []
        if specific_rows_by_itinerary:
            st.markdown("**Microhabilidades Especificas Por Itinerario**")
            st.dataframe(
                [
                    {
                        "# Itinerario": int(item.get("itinerary_number") or 0),
                        "Itinerario": str(item.get("itinerary") or ""),
                        "Filas generadas": int(item.get("specific_rows") or 0),
                    }
                    for item in specific_rows_by_itinerary
                ],
                use_container_width=True,
                hide_index=True,
            )
        specific_rows_by_knowledge = sumun_summary.get("specific_rows_by_knowledge") or []
        if specific_rows_by_knowledge:
            st.markdown("**Microhabilidades Especificas Por Conocimientos**")
            st.dataframe(
                [
                    {
                        "# Itinerario": int(item.get("itinerary_number") or 0),
                        "Itinerario": str(item.get("itinerary") or ""),
                        "# Estacion": int(item.get("station_number") or 0),
                        "Estacion": str(item.get("station") or ""),
                        "Conocimientos": str(item.get("knowledge") or ""),
                        "Filas generadas": int(item.get("specific_rows") or 0),
                    }
                    for item in specific_rows_by_knowledge
                ],
                use_container_width=True,
                hide_index=True,
            )
        if sumun_sheets and selected_sumun_sheet_names:
            processed_sheet_names = set(sumun_summary.get("processed_sheets") or [])
            sheet_by_name = {item.sheet_name: item for item in sumun_sheets}
            skipped_sheet_rows = [
                {
                    "Hoja": sheet_name,
                    "Filas estimadas": int(
                        (sheet_by_name.get(sheet_name).estimated_rows if sheet_by_name.get(sheet_name) else 0)
                        or 0
                    ),
                    "Detalle": str(
                        (
                            sheet_by_name.get(sheet_name).reason
                            if sheet_by_name.get(sheet_name)
                            else "No hay diagnostico disponible."
                        )
                        or ""
                    ),
                }
                for sheet_name in selected_sumun_sheet_names
                if sheet_name not in processed_sheet_names
            ]
            if skipped_sheet_rows:
                st.markdown("**Hojas omitidas**")
                st.dataframe(
                    skipped_sheet_rows,
                    use_container_width=True,
                    hide_index=True,
                )
        inherited_rows = sumun_summary.get("nonnumber_station_rows") or []
        if inherited_rows:
            st.warning(
                "Algunas filas no tenian una estacion identificable y se omitieron: "
                + ", ".join(map(str, inherited_rows[:20]))
            )

    if sumun_output_bytes:
        st.download_button(
            label="Descargar plantilla SUMUN",
            data=sumun_output_bytes,
            file_name=sumun_output_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="sumun_download_btn",
        )




st.set_page_config(
    page_title="SANTED",
    page_icon=str(APP_TAB_LOGO_PATH) if APP_TAB_LOGO_PATH.exists() else None,
    layout="wide",
)
_inject_professional_theme()
_inject_selectbox_title_cleanup()
menu_logo_col, menu_main_col = st.columns([0.8, 4.2], gap="small")
with menu_logo_col:
    _render_logo_with_hidden_extension_download()
with menu_main_col:
    st.markdown("**Menu principal**")
    if st.session_state.get("main_top_menu") == "IPA":
        st.session_state["main_top_menu"] = "Procesos Pegasus"
    menu_option = st.radio(
        "Menu",
        [
            "Procesos Pegasus",
            "SUMUN",
            "Richmond Studio",
            "Loqueleo",
            "Lectura Tokens",
            "Jira Focus Web",
        ],
        horizontal=True,
        label_visibility="collapsed",
        key="main_top_menu",
    )
if menu_option == "Jira Focus Web":
    render_jira_focus_web()
    st.stop()

if menu_option == "Loqueleo":
    loqueleo_bridge_mode = str(
        st.session_state.get("loqueleo_session_bridge_mode") or "read"
    ).strip().lower() or "read"
    loqueleo_bridge_value = str(
        st.session_state.get("loqueleo_session_bridge_value", "") or ""
    ).strip()
    browser_loqueleo_session_id = _read_browser_loqueleo_session_id(
        mode=loqueleo_bridge_mode,
        value=loqueleo_bridge_value,
    )
    if loqueleo_bridge_mode != "read":
        st.session_state["loqueleo_session_bridge_mode"] = "read"
        st.session_state["loqueleo_session_bridge_value"] = ""
    if "loqueleo_session_id" not in st.session_state:
        if browser_loqueleo_session_id not in ("", LOQUELEO_SESSION_BRIDGE_PENDING):
            st.session_state["loqueleo_session_id"] = browser_loqueleo_session_id
        else:
            st.session_state["loqueleo_session_id"] = ""
    elif (
        not str(st.session_state.get("loqueleo_session_id", "") or "").strip()
        and browser_loqueleo_session_id not in ("", LOQUELEO_SESSION_BRIDGE_PENDING)
    ):
        st.session_state["loqueleo_session_id"] = browser_loqueleo_session_id
    if "loqueleo_session_id_input" not in st.session_state:
        st.session_state["loqueleo_session_id_input"] = str(
            st.session_state.get("loqueleo_session_id", "") or ""
        )
    elif (
        not str(st.session_state.get("loqueleo_session_id_input", "") or "").strip()
        and browser_loqueleo_session_id not in ("", LOQUELEO_SESSION_BRIDGE_PENDING)
        and str(st.session_state.get("loqueleo_session_id", "") or "").strip()
        == browser_loqueleo_session_id
    ):
        st.session_state["loqueleo_session_id_input"] = browser_loqueleo_session_id
    render_loqueleo_domain_view()
    st.stop()

if menu_option == "Lectura Tokens":
    _render_token_reader_view()
    st.stop()

if menu_option == "SUMUN":
    _render_sumun_template_view()
    st.stop()

if menu_option != "Richmond Studio":
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
            st.markdown(
                "<div style='height: 1.85rem;' aria-hidden='true'></div>",
                unsafe_allow_html=True,
            )
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
    if False:
        st.caption("Sube una matriz SUMUN y genera la plantilla plana de carga.")
        st.markdown(
            """
            <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin:8px 0 14px 0;">
              <div style="border-left:6px solid #2f80ed;background:#eef5ff;padding:10px;border-radius:6px;">
                <b>Azul: contexto</b><br>
                ITINERARIO, ESTACION, COMPETENCIA, MACROHABILIDAD, MICROHABILIDAD y CONOCIMIENTOS se copian como datos base.
              </div>
              <div style="border-left:6px solid #27ae60;background:#eefaf2;padding:10px;border-radius:6px;">
                <b>Verde: habilidades</b><br>
                RECORDAR, COMPRENDER, APLICAR, ANALIZAR, EVALUAR y CREAR generan filas nuevas.
              </div>
              <div style="border-left:6px solid #f2994a;background:#fff6eb;padding:10px;border-radius:6px;">
                <b>Naranja: codigos</b><br>
                El ID se arma con Nivel + Curso + Grado + I + E + MA + MI + ME.
              </div>
            </div>
            <div style="font-size:0.92rem;margin-bottom:12px;">
              <b>Regla estandar:</b> el numero va a <code># ITINERARIO</code>.
              La columna <code>ITINERARIO</code> muestra el nombre descriptivo, por ejemplo <code>La celula</code>.
            </div>
            """,
            unsafe_allow_html=True,
        )
        uploaded_sumun = st.file_uploader(
            "Excel matriz SUMUN",
            type=["xlsx"],
            key="sumun_matrix_upload",
            help="Puede tener todos los itinerarios en la primera hoja o hitos repartidos en varias hojas.",
        )

        selected_sumun_sheet_names: List[str] = []
        if uploaded_sumun is not None:
            sumun_upload_bytes = uploaded_sumun.getvalue()
            try:
                sumun_sheets = inspect_sumun_workbook_sheets(sumun_upload_bytes)
            except Exception as exc:  # pragma: no cover - UI
                sumun_sheets = []
                st.error(f"No se pudieron leer las hojas del Excel: {exc}")

            if sumun_sheets:
                st.dataframe(
                    [
                        {
                            "Indice": item.index,
                            "Hoja": item.sheet_name,
                            "Matriz detectada": "Si" if item.detected else "No",
                            "Filas estimadas": item.estimated_rows,
                            "Detalle": item.reason,
                        }
                        for item in sumun_sheets
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
                sheet_by_index = {item.index: item for item in sumun_sheets}
                detected_indices = [
                    item.index for item in sumun_sheets if item.detected
                ]
                if len(sumun_sheets) > 1:
                    sheet_options = ["__all__", "__detected__"] + [
                        str(item.index) for item in sumun_sheets
                    ]

                    def _format_sumun_sheet_option(option: str) -> str:
                        if option == "__detected__":
                            return "Todas las hojas detectadas"
                        if option == "__all__":
                            return "Todas las hojas"
                        item = sheet_by_index.get(int(option))
                        if not item:
                            return str(option)
                        return "{idx} - {name}".format(
                            idx=item.index,
                            name=item.sheet_name,
                        )

                    selected_sheet_option = st.selectbox(
                        "Indice de hoja a procesar",
                        options=sheet_options,
                        index=0,
                        format_func=_format_sumun_sheet_option,
                        key="sumun_sheet_option",
                        help="Por defecto se procesan todas las hojas visibles del archivo. Si quieres, puedes limitarlo a las detectadas o a una hoja puntual.",
                    )
                    if selected_sheet_option == "__all__":
                        selected_indices = [item.index for item in sumun_sheets]
                    elif selected_sheet_option == "__detected__":
                        selected_indices = detected_indices
                    else:
                        selected_indices = [int(selected_sheet_option)]
                else:
                    selected_indices = [sumun_sheets[0].index]
                    st.caption(
                        "Solo hay una hoja; se procesara: "
                        f"{sumun_sheets[0].sheet_name}."
                    )
                selected_sumun_sheet_names = [
                    sheet_by_index[int(idx)].sheet_name
                    for idx in selected_indices
                    if int(idx) in sheet_by_index
                ]
                selected_with_zero_rows = [
                    sheet_by_index[int(idx)].sheet_name
                    for idx in selected_indices
                    if int(idx) in sheet_by_index
                    and not sheet_by_index[int(idx)].estimated_rows
                ]
                if selected_with_zero_rows:
                    st.warning(
                        "Estas hojas seleccionadas no tienen filas estimadas de matriz: "
                        + ", ".join(selected_with_zero_rows)
                    )

        col_sumun_code, col_sumun_grade, col_sumun_level, col_sumun_area = st.columns(4)
        sumun_course_code_choice = col_sumun_code.selectbox(
            "Codigo de curso",
            options=[
                "Detectar automaticamente",
                "COM",
                "CT",
                "CCT",
                "MA",
                "MAT",
                "CO",
                "CCSS",
                "PS",
            ],
            index=0,
            key="sumun_course_code_combo",
        )
        sumun_course_code = (
            "" if sumun_course_code_choice == "Detectar automaticamente" else sumun_course_code_choice
        )
        sumun_grade_choice = col_sumun_grade.selectbox(
            "Grado",
            options=["Detectar automaticamente", "1", "2", "3", "4", "5", "6"],
            index=0,
            key="sumun_grade_combo",
        )
        sumun_grade_raw = (
            "" if sumun_grade_choice == "Detectar automaticamente" else sumun_grade_choice
        )
        sumun_level = col_sumun_level.selectbox(
            "Nivel",
            options=["Secundaria", "Primaria"],
            index=0,
            key="sumun_level",
        )
        sumun_area_choice = col_sumun_area.selectbox(
            "Area",
            options=[
                "Inferir por codigo",
                "Ciencia y Tecnologia",
                "Matematica",
                "Comunicacion",
                "Ciencias sociales",
                "Personal Social",
            ],
            index=0,
            key="sumun_area_combo",
        )
        sumun_area_by_choice = {
            "Inferir por codigo": "",
            "Ciencia y Tecnologia": "Ciencia y Tecnolog\u00eda",
            "Matematica": "Matem\u00e1tica",
            "Comunicacion": "Comunicaci\u00f3n",
            "Ciencias sociales": "Ciencias sociales",
            "Personal Social": "Personal Social",
        }
        sumun_area = sumun_area_by_choice.get(sumun_area_choice, "")

        if st.button("Generar plantilla SUMUN", type="primary", key="sumun_generate_btn"):
            if uploaded_sumun is None:
                st.error("Sube un Excel de matriz SUMUN.")
                st.stop()
            if not selected_sumun_sheet_names:
                st.error("Selecciona al menos una hoja para procesar.")
                st.stop()

            grade_override: Optional[int] = None
            if sumun_grade_raw.strip():
                try:
                    grade_override = int(sumun_grade_raw.strip())
                except ValueError:
                    st.error("El grado debe ser un numero, por ejemplo 1.")
                    st.stop()

            try:
                output_bytes, summary = generate_sumun_template_from_excel(
                    uploaded_sumun.getvalue(),
                    source_name=uploaded_sumun.name or "matriz_sumun.xlsx",
                    area=sumun_area.strip() or None,
                    grade=grade_override,
                    level=sumun_level,
                    course_code=sumun_course_code.strip() or None,
                    sheet_names=selected_sumun_sheet_names,
                )
            except Exception as exc:  # pragma: no cover - UI
                st.error(f"No se pudo generar la plantilla SUMUN: {exc}")
                st.stop()

            source_stem = Path(uploaded_sumun.name or "matriz_sumun").stem
            download_name = f"plantilla_carga_matrices_sumun_{source_stem}.xlsx"
            st.session_state["sumun_output_bytes"] = output_bytes
            st.session_state["sumun_output_name"] = download_name
            st.session_state["sumun_summary"] = summary.to_dict()
            st.success(
                "Plantilla lista. Filas: {rows}. Prefijo ID: {prefix}.".format(
                    rows=summary.generated_rows,
                    prefix=summary.prefix,
                )
            )

        sumun_output_bytes = st.session_state.get("sumun_output_bytes") or b""
        sumun_output_name = str(
            st.session_state.get("sumun_output_name")
            or "plantilla_carga_matrices_sumun.xlsx"
        )
        sumun_summary = st.session_state.get("sumun_summary") or {}
        if sumun_summary:
            summary_cols = st.columns(4)
            summary_cols[0].metric(
                "Filas", int(sumun_summary.get("generated_rows") or 0)
            )
            summary_cols[1].metric("Prefijo", str(sumun_summary.get("prefix") or ""))
            summary_cols[2].metric(
                "Hojas", len(sumun_summary.get("processed_sheets") or [])
            )
            summary_cols[3].metric(
                "Micro", int(sumun_summary.get("micro_count") or 0)
            )
            unique_micro_count = int(sumun_summary.get("unique_micro_count") or 0)
            if unique_micro_count:
                st.caption(f"Micro unicas: {unique_micro_count}")
            st.caption(
                "Cada celda con valor en RECORDAR/COMPRENDER/APLICAR/ANALIZAR/EVALUAR/CREAR genera una sola fila. "
                "El texto interno de la celda no se divide."
            )

            rows_by_sheet = sumun_summary.get("rows_by_sheet") or {}
            if rows_by_sheet:
                st.markdown("**Hojas procesadas**")
                st.dataframe(
                    [
                        {"Hoja": sheet_name, "Filas generadas": row_count}
                        for sheet_name, row_count in rows_by_sheet.items()
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
            specific_rows_by_itinerary = sumun_summary.get("specific_rows_by_itinerary") or []
            if specific_rows_by_itinerary:
                st.markdown("**Microhabilidades Especificas Por Itinerario**")
                st.dataframe(
                    [
                        {
                            "# Itinerario": int(item.get("itinerary_number") or 0),
                            "Itinerario": str(item.get("itinerary") or ""),
                            "Filas generadas": int(item.get("specific_rows") or 0),
                        }
                        for item in specific_rows_by_itinerary
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
            specific_rows_by_knowledge = sumun_summary.get("specific_rows_by_knowledge") or []
            if specific_rows_by_knowledge:
                st.markdown("**Microhabilidades Especificas Por Conocimientos**")
                st.dataframe(
                    [
                        {
                            "# Itinerario": int(item.get("itinerary_number") or 0),
                            "Itinerario": str(item.get("itinerary") or ""),
                            "# Estacion": int(item.get("station_number") or 0),
                            "Estacion": str(item.get("station") or ""),
                            "Conocimientos": str(item.get("knowledge") or ""),
                            "Filas generadas": int(item.get("specific_rows") or 0),
                        }
                        for item in specific_rows_by_knowledge
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
            inherited_rows = sumun_summary.get("nonnumber_station_rows") or []
            if inherited_rows:
                st.warning(
                    "Algunas filas no tenian una estacion identificable y se omitieron: "
                    + ", ".join(map(str, inherited_rows[:20]))
                )

        if sumun_output_bytes:
            st.download_button(
                label="Descargar plantilla SUMUN",
                data=sumun_output_bytes,
                file_name=sumun_output_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="sumun_download_btn",
            )

    tab_crud_clases, tab_crud_profesores, tab_crud_alumnos, tab_otras_funcionalidades = st.tabs(
        [
            "CRUD Clases",
            "CRUD Profesores",
            "CRUD Alumnos",
            "Otras funcionalidades",
        ]
    )


def _clean_token(token: str) -> str:
    return _clean_token_value(token)


def _get_shared_token() -> str:
    token_saved = _clean_token(str(st.session_state.get("shared_pegasus_token", "")))
    if token_saved:
        return token_saved
    return _clean_token(os.environ.get("PEGASUS_TOKEN", ""))
































































































































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




































def _export_simple_excel(rows: List[Dict[str, object]], sheet_name: str = "data") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.getvalue()


def _export_censo_activos_excel(rows: List[Dict[str, object]]) -> bytes:
    output = BytesIO()
    df = pd.DataFrame(rows)
    df = df.reindex(columns=CENSO_ACTIVOS_EXPORT_COLUMNS)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="activos")
    output.seek(0)
    return output.getvalue()


def _export_censo_profesores_activos_excel(rows: List[Dict[str, object]]) -> bytes:
    output = BytesIO()
    df = pd.DataFrame(rows)
    df = df.reindex(columns=CENSO_PROFESORES_ACTIVOS_EXPORT_COLUMNS)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="profesores_activos")
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
                "DNI": str(
                    row.get("DNI")
                    or row.get("dni")
                    or row.get("id_oficial")
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


def _load_censo_activos_for_colegio(
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
                on_status(str(message or "").strip())
            except Exception:
                pass

    _status("Leyendo niveles, grados y secciones...")
    niveles = _fetch_niveles_grados_grupos_censo(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
    )
    contexts = _build_contexts_for_nivel_grado(niveles=niveles)
    rows_activos: List[Dict[str, object]] = []
    export_rows_activos: List[Dict[str, object]] = []
    errors_activos: List[str] = []

    _status("Leyendo logins...")
    try:
        (
            login_lookup_by_alumno,
            login_lookup_by_persona,
        ) = _fetch_login_password_lookup_censo(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
    except Exception:
        login_lookup_by_alumno = {}
        login_lookup_by_persona = {}

    total_contexts = len(contexts)
    for idx, ctx in enumerate(contexts, start=1):
        _status(
            "[{idx}/{total}] {nivel} | {grado} ({seccion})".format(
                idx=idx,
                total=total_contexts,
                nivel=str(ctx.get("nivel") or "").strip(),
                grado=str(ctx.get("grado") or "").strip(),
                seccion=str(ctx.get("seccion") or "").strip(),
            )
        )
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
        except Exception as exc:
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
                "DNI": flat.get("id_oficial") or "",
                "Login": login_txt,
                "Password": "",
            }
            rows_activos.append(dict(row_activo))
            export_rows_activos.append(dict(row_activo))

    rows_activos = _normalize_censo_activos_export_rows(rows_activos)
    export_rows_activos = _normalize_censo_activos_export_rows(export_rows_activos)
    return {
        "rows": rows_activos,
        "export_rows": export_rows_activos,
        "errors": errors_activos,
        "contexts_total": total_contexts,
    }


def _normalize_censo_profesores_activos_export_rows(
    rows: List[Dict[str, object]]
) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "Nombre": str(row.get("Nombre") or row.get("nombre") or "").strip(),
                "Apellido paterno": str(
                    row.get("Apellido paterno")
                    or row.get("apellido_paterno")
                    or ""
                ).strip(),
                "Apellido materno": str(
                    row.get("Apellido materno")
                    or row.get("apellido_materno")
                    or ""
                ).strip(),
                "Login": str(row.get("Login") or row.get("login") or "").strip(),
            }
        )
    normalized.sort(
        key=lambda row: (
            str(row.get("Apellido paterno") or "").upper(),
            str(row.get("Apellido materno") or "").upper(),
            str(row.get("Nombre") or "").upper(),
            str(row.get("Login") or "").upper(),
        )
    )
    return normalized


def _load_censo_profesores_activos_for_colegio(
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
                on_status(str(message or "").strip())
            except Exception:
                pass

    _status("Leyendo profesores del colegio...")
    profesores_rows, summary, errors_raw = listar_profesores_filters_data(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
    )
    export_rows: List[Dict[str, object]] = []
    for row in profesores_rows:
        if not isinstance(row, dict):
            continue
        estado_norm = _normalize_plain_text(row.get("estado"))
        if estado_norm not in {"ACTIVO", "ACTIVA"}:
            continue
        export_rows.append(
            {
                "Nombre": str(row.get("nombre") or "").strip(),
                "Apellido paterno": str(row.get("apellido_paterno") or "").strip(),
                "Apellido materno": str(row.get("apellido_materno") or "").strip(),
                "Login": str(row.get("login") or "").strip(),
            }
        )
    normalized_rows = _normalize_censo_profesores_activos_export_rows(export_rows)
    errors: List[str] = []
    for item in errors_raw or []:
        if not isinstance(item, dict):
            continue
        err_txt = str(item.get("error") or "").strip()
        if err_txt:
            errors.append(err_txt)
    return {
        "rows": normalized_rows,
        "export_rows": normalized_rows,
        "errors": errors,
        "summary": dict(summary or {}),
    }


def _parse_colegio_ids_text(
    raw: object,
    field_name: str = "Colegios ID",
) -> List[int]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError(f"{field_name} es obligatorio.")
    values: List[int] = []
    seen: Set[int] = set()
    for token in re.split(r"[\s,;]+", text):
        token_clean = str(token or "").strip()
        if not token_clean:
            continue
        if not token_clean.isdigit():
            raise ValueError(
                f"{field_name} invalido: '{token_clean}'. Usa IDs numericos separados por coma, espacio o salto de linea."
            )
        value = int(token_clean)
        if value <= 0:
            raise ValueError(f"{field_name} invalido: '{token_clean}'. Debe ser mayor a 0.")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    if not values:
        raise ValueError(f"{field_name} es obligatorio.")
    return values


def _sanitize_zip_component(text: object, fallback: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        raw = str(fallback or "").strip()
    raw = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip(" .")
    return raw or str(fallback or "colegio").strip() or "colegio"


def _get_colegio_export_base_name(colegio_id: int) -> str:
    for row in st.session_state.get("shared_colegios_rows") or []:
        try:
            row_id = int(row.get("colegio_id") or 0)
        except (TypeError, ValueError):
            continue
        if row_id != int(colegio_id):
            continue
        colegio_nombre = str(row.get("colegio") or "").strip()
        if colegio_nombre:
            return _sanitize_zip_component(
                f"{colegio_nombre} - {int(colegio_id)}",
                f"Colegio {int(colegio_id)}",
            )
    return _sanitize_zip_component(
        f"Colegio {int(colegio_id)}",
        f"Colegio {int(colegio_id)}",
    )


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
        if text in {"true", "1", "si", "s", "yes", "y", "activo", "active", "enabled"}:
            return True
        if text in {"false", "0", "no", "n", "inactivo", "inactive", "disabled"}:
            return False
        return False
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


def _normalize_import_column(value: object) -> str:
    text = _normalize_plain_text(value)
    return re.sub(r"\s+", " ", text).strip()


def _resolve_import_column(
    normalized_columns: Dict[str, str],
    aliases: Sequence[str],
    *,
    required: bool = False,
) -> str:
    for alias in aliases:
        match = normalized_columns.get(_normalize_import_column(alias))
        if match:
            return match
    if required:
        raise ValueError(
            "No se encontro ninguna de las columnas requeridas: "
            + ", ".join(str(alias) for alias in aliases)
        )
    return ""


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


def _safe_int(value: object) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fetch_profesores_clase_gestion_escolar(
    token: str,
    clase_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_STAFF_CLASE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        clase_id=int(clase_id),
    )
    try:
        response = requests.get(
            url,
            headers=headers,
            params={"rolClave": "PROF"},
            timeout=int(timeout),
        )
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

    data = payload.get("data")
    raw_rows: List[Dict[str, object]] = []
    if isinstance(data, list):
        raw_rows = [item for item in data if isinstance(item, dict)]
    elif isinstance(data, dict):
        for key in (
            "claseStaff",
            "staff",
            "personas",
            "personaRoles",
            "content",
            "items",
            "lista",
            "data",
        ):
            value = data.get(key)
            if isinstance(value, list):
                raw_rows = [item for item in value if isinstance(item, dict)]
                break
        if not raw_rows and "personaId" in data:
            raw_rows = [data]
    else:
        raise RuntimeError("Campo data no es lista")

    rows: List[Dict[str, object]] = []
    for item in raw_rows:
        rol = item.get("rol") if isinstance(item.get("rol"), dict) else {}
        rol_clave = str(rol.get("rolClave") or "").strip()
        if rol_clave and rol_clave != "PROF":
            continue
        persona = item.get("persona") if isinstance(item.get("persona"), dict) else {}
        persona_id = _safe_int(item.get("personaId")) or _safe_int(
            persona.get("personaId")
        )
        if persona_id is None:
            continue
        persona_login = (
            persona.get("personaLogin")
            if isinstance(persona.get("personaLogin"), dict)
            else {}
        )
        nombre = str(persona.get("nombreCompleto") or "").strip()
        if not nombre:
            nombre = " ".join(
                part
                for part in (
                    str(persona.get("nombre") or "").strip(),
                    str(persona.get("apellidoPaterno") or "").strip(),
                    str(persona.get("apellidoMaterno") or "").strip(),
                )
                if part
            ).strip()
        rows.append(
            {
                "persona_id": int(persona_id),
                "nombre": nombre or f"Persona {int(persona_id)}",
                "nombre_base": str(persona.get("nombre") or "").strip(),
                "apellido_paterno": str(persona.get("apellidoPaterno") or "").strip(),
                "apellido_materno": str(persona.get("apellidoMaterno") or "").strip(),
                "login": str(persona_login.get("login") or "").strip(),
                "dni": str(persona.get("idOficial") or "").strip(),
                "activo": bool(item.get("activo", True)),
            }
        )
    rows.sort(
        key=lambda row: (
            str(row.get("apellido_paterno") or "").upper(),
            str(row.get("apellido_materno") or "").upper(),
            str(row.get("nombre") or "").upper(),
            int(row.get("persona_id") or 0),
        )
    )
    return rows


def _assign_profesor_to_clase_web(
    token: str,
    clase_id: int,
    persona_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> Tuple[bool, str]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_STAFF_CLASE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        clase_id=int(clase_id),
    )
    payload = {"rolClave": "PROF", "personaId": int(persona_id)}
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=int(timeout),
        )
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json() if response.content else {}
    except ValueError:
        body = {}

    if not response.ok:
        message = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        return False, message or f"HTTP {status_code}"
    if isinstance(body, dict) and body.get("success", True) is False:
        message = str(body.get("message") or "Respuesta invalida").strip()
        return False, message
    return True, ""


def _delete_profesor_clase_gestion_escolar(
    token: str,
    clase_id: int,
    persona_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> None:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    base_url = GESTION_ESCOLAR_STAFF_CLASE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        clase_id=int(clase_id),
    )
    url = f"{base_url}/{int(persona_id)}"
    try:
        response = requests.delete(url, headers=headers, timeout=int(timeout))
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json() if response.content else {}
    except ValueError:
        payload = {}

    if not response.ok:
        message = str(payload.get("message") or "").strip() if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if isinstance(payload, dict) and payload.get("success") is False:
        message = str(payload.get("message") or "Respuesta invalida").strip()
        raise RuntimeError(message)


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

    # Prefer the class key because it is the identifier shown/exported in the UI
    # and avoids ambiguous matches like "Ingles" repeated across grades/sections.
    clase_nombre = str(item.get("geClaseClave") or item.get("geClase") or "")
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
    search_text = " ".join(
        part
        for part in (
            _normalize_plain_text(item.get("geClase")),
            _normalize_plain_text(item.get("geClaseClave")),
            _normalize_plain_text(item.get("clase")),
            _normalize_plain_text(item.get("clase_codigo")),
            _normalize_plain_text(item.get("clase_nombre")),
            _normalize_plain_text(item.get("alias")),
        )
        if part
    )
    target = "SANTILLANA INCLUSIVA"
    return target in search_text


def _iter_ingles_class_search_values(item: Dict[str, object]) -> Iterable[str]:
    for value in (
        item.get("geClase"),
        item.get("geClaseClave"),
        item.get("alias"),
    ):
        text = _normalize_plain_text(value)
        if text:
            yield text

    clase_materias = item.get("claseMaterias") if isinstance(item.get("claseMaterias"), list) else []
    for entry in clase_materias:
        if not isinstance(entry, dict):
            continue
        materia = entry.get("materia") if isinstance(entry.get("materia"), dict) else {}
        for value in (
            materia.get("materia"),
            materia.get("materiaClave"),
        ):
            text = _normalize_plain_text(value)
            if text:
                yield text


def _is_ingles_por_niveles_class(item: Dict[str, object]) -> bool:
    search_text = " ".join(_iter_ingles_class_search_values(item))
    if "INGLES" in search_text or "ENGLISH" in search_text:
        return True
    return "PAI" in search_text and "EMERGENT" in search_text


def _build_clase_participantes_row(item: Dict[str, object]) -> Optional[Dict[str, object]]:
    base_meta = _extract_clase_base_meta(item)
    if not isinstance(base_meta, dict):
        return None
    clase_nombre = str(item.get("geClase") or "").strip()
    clase_codigo = str(item.get("geClaseClave") or "").strip()
    alias = str(item.get("alias") or "").strip()
    display_name = clase_nombre or alias or clase_codigo or str(
        base_meta.get("clase_nombre") or ""
    ).strip()
    tipo = "Regular"
    if _is_santillana_inclusiva_class(item):
        tipo = "Santillana Inclusiva"
    elif _is_ingles_por_niveles_class(item):
        tipo = "Ingles por niveles"
    row = {
        **base_meta,
        "clase": display_name,
        "clase_codigo": clase_codigo,
        "alias": alias,
        "tipo": tipo,
        "activo": bool(item.get("activo", False)),
        "baja": bool(item.get("baja", False)),
    }
    row["label"] = _clase_participantes_label(row)
    return row


def _clase_participantes_label(row: Dict[str, object]) -> str:
    clase = str(row.get("clase") or row.get("clase_nombre") or "").strip()
    clase_id = _safe_int(row.get("clase_id"))
    tipo = str(row.get("tipo") or "").strip()
    nivel = str(row.get("nivel_nombre") or "").strip()
    grado = str(row.get("grado_nombre") or "").strip()
    grupo = str(row.get("grupo_clave_actual") or "").strip()
    context = " | ".join(part for part in (nivel, grado, grupo) if part)
    base = clase or f"Clase {clase_id or '-'}"
    if tipo:
        base = f"{tipo} | {base}"
    if clase_id is not None:
        base = f"{base} | ID {int(clase_id)}"
    if context:
        base = f"{base} | {context}"
    return base


def _extract_clase_alumno_rows(clase_data: Dict[str, object]) -> List[Dict[str, object]]:
    clase_alumnos = clase_data.get("claseAlumnos") if isinstance(clase_data, dict) else []
    if not isinstance(clase_alumnos, list):
        return []
    rows: List[Dict[str, object]] = []
    for entry in clase_alumnos:
        if not isinstance(entry, dict):
            continue
        alumno = entry.get("alumno") if isinstance(entry.get("alumno"), dict) else entry
        persona = alumno.get("persona") if isinstance(alumno.get("persona"), dict) else {}
        persona_login = (
            persona.get("personaLogin")
            if isinstance(persona.get("personaLogin"), dict)
            else {}
        )
        alumno_id = _safe_int(alumno.get("alumnoId")) or _safe_int(entry.get("alumnoId"))
        if alumno_id is None:
            continue
        nombre = str(persona.get("nombreCompleto") or "").strip()
        if not nombre:
            nombre = " ".join(
                part
                for part in (
                    str(persona.get("nombre") or "").strip(),
                    str(persona.get("apellidoPaterno") or "").strip(),
                    str(persona.get("apellidoMaterno") or "").strip(),
                )
                if part
            ).strip()
        rows.append(
            {
                "alumno_id": int(alumno_id),
                "persona_id": (
                    _safe_int(persona.get("personaId"))
                    or _safe_int(alumno.get("personaId"))
                    or _safe_int(entry.get("personaId"))
                    or ""
                ),
                "nombre_completo": nombre or f"Alumno {int(alumno_id)}",
                "nombre": str(persona.get("nombre") or "").strip(),
                "apellido_paterno": str(persona.get("apellidoPaterno") or "").strip(),
                "apellido_materno": str(persona.get("apellidoMaterno") or "").strip(),
                "dni": str(
                    persona.get("idOficial")
                    or alumno.get("idOficial")
                    or entry.get("idOficial")
                    or ""
                ).strip(),
                "login": str(
                    persona_login.get("login")
                    or alumno.get("login")
                    or entry.get("login")
                    or ""
                ).strip(),
            }
        )
    rows.sort(
        key=lambda row: (
            str(row.get("apellido_paterno") or "").upper(),
            str(row.get("apellido_materno") or "").upper(),
            str(row.get("nombre_completo") or "").upper(),
            int(row.get("alumno_id") or 0),
        )
    )
    return rows


def _load_clase_participantes_detail(
    token: str,
    clase_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> Dict[str, object]:
    errors: List[str] = []
    alumnos_rows: List[Dict[str, object]] = []
    profesores_rows: List[Dict[str, object]] = []
    try:
        clase_data = _fetch_alumnos_clase_gestion_escolar(
            token=token,
            clase_id=int(clase_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
        alumnos_rows = _extract_clase_alumno_rows(clase_data)
    except Exception as exc:
        errors.append(f"Alumnos: {exc}")
    try:
        profesores_rows = _fetch_profesores_clase_gestion_escolar(
            token=token,
            clase_id=int(clase_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
    except Exception as exc:
        errors.append(f"Profesores: {exc}")
    return {
        "alumnos": alumnos_rows,
        "profesores": profesores_rows,
        "errors": errors,
    }


def _clase_person_label(row: Dict[str, object], id_key: str) -> str:
    nombre = str(
        row.get("nombre_completo")
        or row.get("nombre")
        or row.get("label")
        or ""
    ).strip()
    entity_id = _safe_int(row.get(id_key))
    login = str(row.get("login") or "").strip()
    dni = str(row.get("dni") or row.get("id_oficial") or "").strip()
    parts = [nombre or f"ID {entity_id or '-'}"]
    if entity_id is not None:
        parts.append(f"ID {int(entity_id)}")
    if login:
        parts.append(login)
    if dni:
        parts.append(f"DNI {dni}")
    return " | ".join(parts)


def _row_matches_text(row: Dict[str, object], search_text: object, fields: Sequence[str]) -> bool:
    search_norm = _normalize_compare_text(search_text)
    if not search_norm:
        return True
    haystack = _normalize_compare_text(
        " ".join(str(row.get(field) or "") for field in fields)
    )
    return search_norm in haystack


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


def _build_ingles_grade_catalog_options_for_participantes(
    niveles_data: List[Dict[str, object]],
    detected_options: Optional[Sequence[Dict[str, object]]] = None,
) -> List[Dict[str, object]]:
    detected_by_key: Dict[str, Dict[str, object]] = {}
    for option in detected_options or []:
        if not isinstance(option, dict):
            continue
        option_key = str(option.get("key") or "").strip()
        if not option_key:
            continue
        class_names = sorted(
            {
                str(item).strip()
                for item in list(option.get("class_names") or [])
                if str(item).strip()
            },
            key=lambda value: _normalize_compare_text(value),
        )
        detected_by_key[option_key] = {
            "key": option_key,
            "nivel_id": _safe_int(option.get("nivel_id")),
            "grado_id": _safe_int(option.get("grado_id")),
            "nivel_nombre": str(option.get("nivel_nombre") or "").strip(),
            "grado_nombre": str(option.get("grado_nombre") or "").strip(),
            "class_names": class_names,
        }

    options_by_key: Dict[str, Dict[str, object]] = {}
    for nivel_entry in niveles_data:
        if not isinstance(nivel_entry, dict):
            continue
        nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
        nivel_id = _safe_int(nivel.get("nivelId"))
        if nivel_id is None:
            continue
        nivel_nombre = str(nivel.get("nivel") or "").strip()
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
            option_key = _participantes_ingles_grade_key(nivel_id, grado_id)
            if not option_key:
                continue
            detected_option = detected_by_key.get(option_key) or {}
            options_by_key.setdefault(
                option_key,
                {
                    "key": option_key,
                    "nivel_id": int(nivel_id),
                    "grado_id": int(grado_id),
                    "nivel_nombre": str(
                        detected_option.get("nivel_nombre") or nivel_nombre
                    ).strip(),
                    "grado_nombre": str(
                        detected_option.get("grado_nombre")
                        or grado.get("grado")
                        or grado.get("gradoClave")
                        or ""
                    ).strip(),
                    "class_names": list(detected_option.get("class_names") or []),
                },
            )

    for option_key, option in detected_by_key.items():
        options_by_key.setdefault(option_key, dict(option))

    options = list(options_by_key.values())
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
    if grupo_id_actual is not None:
        for option in options:
            if int(option["grupo_id"]) == int(grupo_id_actual):
                return int(option["grupo_id"])
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
    return None


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
    source = _extract_alumno_payload(item)
    persona = (
        source.get("persona")
        if isinstance(source.get("persona"), dict)
        else (
            item.get("persona")
            if isinstance(item.get("persona"), dict)
            else {}
        )
    )
    persona_login = (
        persona.get("personaLogin") if isinstance(persona.get("personaLogin"), dict) else {}
    )
    nivel = (
        source.get("nivel")
        if isinstance(source.get("nivel"), dict)
        else (item.get("nivel") if isinstance(item.get("nivel"), dict) else {})
    )
    grado = (
        source.get("grado")
        if isinstance(source.get("grado"), dict)
        else (item.get("grado") if isinstance(item.get("grado"), dict) else {})
    )
    grupo = (
        source.get("grupo")
        if isinstance(source.get("grupo"), dict)
        else (item.get("grupo") if isinstance(item.get("grupo"), dict) else {})
    )
    seccion = str(
        grupo.get("grupoClave")
        or grupo.get("grupo")
        or fallback.get("seccion")
        or ""
    ).strip()
    seccion_norm = _normalize_seccion_key(seccion)
    return {
        "alumno_id": _safe_int(source.get("alumnoId")) or _safe_int(item.get("alumnoId")),
        "persona_id": (
            _safe_int(persona.get("personaId"))
            or _safe_int(source.get("personaId"))
            or _safe_int(item.get("personaId"))
        ),
        "nombre": str(persona.get("nombre") or "").strip(),
        "apellido_paterno": str(persona.get("apellidoPaterno") or "").strip(),
        "apellido_materno": str(persona.get("apellidoMaterno") or "").strip(),
        "nombre_completo": str(persona.get("nombreCompleto") or "").strip(),
        "id_oficial": str(persona.get("idOficial") or "").strip(),
        "login": str(
            persona_login.get("login")
            or source.get("login")
            or item.get("login")
            or ""
        ).strip(),
        "password": str(source.get("password") or item.get("password") or "").strip(),
        "nivel_id": _safe_int(nivel.get("nivelId")) or _safe_int(fallback.get("nivel_id")),
        "grado_id": _safe_int(grado.get("gradoId")) or _safe_int(fallback.get("grado_id")),
        "grupo_id": _safe_int(grupo.get("grupoId")) or _safe_int(fallback.get("grupo_id")),
        "nivel": str(nivel.get("nivel") or fallback.get("nivel") or "").strip(),
        "grado": str(grado.get("grado") or fallback.get("grado") or "").strip(),
        "seccion": seccion,
        "seccion_norm": seccion_norm,
        "activo": _to_bool(
            source.get("activo")
            if source.get("activo") is not None
            else item.get("activo")
        ),
        "con_pago": _to_bool(
            source.get("conPago")
            if source.get("conPago") is not None
            else item.get("conPago")
        ),
        "fecha_desde": str(source.get("fechaDesde") or item.get("fechaDesde") or "").strip(),
    }


def _censo_alumno_matches_context(
    item: Dict[str, object],
    context: Dict[str, object],
) -> bool:
    source = _extract_alumno_payload(item)
    nivel = (
        source.get("nivel")
        if isinstance(source.get("nivel"), dict)
        else (item.get("nivel") if isinstance(item.get("nivel"), dict) else {})
    )
    grado = (
        source.get("grado")
        if isinstance(source.get("grado"), dict)
        else (item.get("grado") if isinstance(item.get("grado"), dict) else {})
    )
    grupo = (
        source.get("grupo")
        if isinstance(source.get("grupo"), dict)
        else (item.get("grupo") if isinstance(item.get("grupo"), dict) else {})
    )

    expected_nivel_id = _safe_int(context.get("nivel_id"))
    expected_grado_id = _safe_int(context.get("grado_id"))
    expected_grupo_id = _safe_int(context.get("grupo_id"))
    expected_seccion = _normalize_seccion_key(context.get("seccion_norm") or context.get("seccion") or "")

    raw_nivel_id = _safe_int(nivel.get("nivelId"))
    raw_grado_id = _safe_int(grado.get("gradoId"))
    raw_grupo_id = _safe_int(grupo.get("grupoId"))
    raw_seccion = _normalize_seccion_key(grupo.get("grupoClave") or grupo.get("grupo") or "")

    if raw_nivel_id is not None and expected_nivel_id is not None and raw_nivel_id != expected_nivel_id:
        return False
    if raw_grado_id is not None and expected_grado_id is not None and raw_grado_id != expected_grado_id:
        return False
    if raw_grupo_id is not None and expected_grupo_id is not None:
        return raw_grupo_id == expected_grupo_id
    if raw_seccion and expected_seccion:
        return raw_seccion == expected_seccion
    return False


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
    ingles_class_ids: Optional[Set[int]] = None,
) -> Tuple[List[Dict[str, object]], List[str], List[str]]:
    grupos_por_grado = _build_grupos_disponibles_por_grado(niveles_data)
    rows_auto: List[Dict[str, object]] = []
    warnings_auto: List[str] = []
    skipped_ingles: List[str] = []

    for item in clases:
        if not isinstance(item, dict):
            continue
        if _is_santillana_inclusiva_class(item):
            base_meta = _extract_clase_base_meta(item)
            if not base_meta:
                warnings_auto.append(
                    f"Clase omitida por metadata incompleta: {item.get('geClaseId')}"
                )
                continue
            rows_auto.append(
                {
                    **base_meta,
                    "options": [],
                    "selected_group_id": base_meta.get("grupo_id_actual"),
                    "clear_current_students": True,
                    "clear_reason": "Santillana Inclusiva",
                }
            )
            continue
        base_meta = _extract_clase_base_meta(item)
        if not base_meta:
            warnings_auto.append(
                f"Clase omitida por metadata incompleta: {item.get('geClaseId')}"
            )
            continue
        ingles_grade_key = _participantes_ingles_option_key_from_meta(base_meta)
        clase_id = _safe_int(base_meta.get("clase_id")) or _safe_int(item.get("geClaseId"))
        if (
            exclude_ingles_por_niveles
            and (
                (
                    _is_ingles_por_niveles_class(item)
                    and ingles_grade_key
                    and ingles_grade_key in (ingles_grade_keys or set())
                )
                or (
                    clase_id is not None
                    and int(clase_id) in (ingles_class_ids or set())
                )
            )
        ):
            skipped_ingles.append(
                "Clase {clase_id} omitida por Ingles por niveles: conserva sus "
                "alumnos actuales y se administra desde el Excel de ingles.".format(
                    clase_id=clase_id or base_meta.get("clase_id") or item.get("geClaseId") or "-"
                )
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
                "clear_reason": "",
            }
        )

    rows_auto.sort(key=_participantes_auto_row_sort_key)
    return rows_auto, warnings_auto, skipped_ingles


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
    return None


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


def _reconcile_participantes_sync_job(job_id: object) -> None:
    job_key = str(job_id or "").strip()
    if not job_key:
        return
    state = _get_participantes_sync_state()
    lock = state["lock"]
    jobs = state["jobs"]
    with lock:
        job = jobs.get(job_key)
        if not isinstance(job, dict):
            return
        state = str(job.get("state") or "").strip()
        if state not in {"starting", "running"}:
            return
        worker = job.get("thread")
        if isinstance(worker, threading.Thread) and worker.is_alive():
            return
        if worker is None:
            error_detail = "Proceso sin hilo activo asociado."
        else:
            error_detail = (
                "El hilo en segundo plano termino antes de reportar el estado final."
            )
        messages = list(job.get("status_messages") or [])
        message = f"Proceso interrumpido: {error_detail}"
        if not messages or messages[-1] != message:
            messages.append(message)
        job["status_messages"] = messages[-_PARTICIPANTES_SYNC_STATUS_LIMIT:]
        if not str(job.get("error") or "").strip():
            job["error"] = error_detail
        if not str(job.get("error_trace") or "").strip():
            job["error_trace"] = error_detail
        job["state"] = "error"


def _set_participantes_sync_job(job_id: str, **fields: object) -> None:
    if not str(job_id or "").strip():
        return
    state = _get_participantes_sync_state()
    lock = state["lock"]
    jobs = state["jobs"]
    with lock:
        job = jobs.get(str(job_id))
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
    state = _get_participantes_sync_state()
    lock = state["lock"]
    jobs = state["jobs"]
    with lock:
        job = jobs.get(str(job_id))
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
    _reconcile_participantes_sync_job(job_key)
    state = _get_participantes_sync_state()
    lock = state["lock"]
    jobs = state["jobs"]
    with lock:
        job = jobs.get(job_key)
        if not isinstance(job, dict):
            return None
        return _copy_participantes_sync_job(job)


def _get_participantes_sync_job_id_for_scope(
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
) -> Optional[str]:
    scope = (int(empresa_id), int(ciclo_id), int(colegio_id))
    state = _get_participantes_sync_state()
    lock = state["lock"]
    jobs = state["jobs"]
    scope_to_job = state["scope_to_job"]
    with lock:
        job_id = scope_to_job.get(scope)
        if not str(job_id or "").strip():
            return None
        if not isinstance(jobs.get(str(job_id)), dict):
            return None
        resolved_job_id = str(job_id)
    _reconcile_participantes_sync_job(resolved_job_id)
    return resolved_job_id


def _is_participantes_sync_job_active(job: Optional[Dict[str, object]]) -> bool:
    if not isinstance(job, dict):
        return False
    return str(job.get("state") or "").strip() in {"starting", "running"}


def _request_cancel_participantes_sync_job(job_id: object) -> bool:
    job_key = str(job_id or "").strip()
    if not job_key:
        return False
    state = _get_participantes_sync_state()
    lock = state["lock"]
    jobs = state["jobs"]
    with lock:
        job = jobs.get(job_key)
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
    state = _get_participantes_sync_state()
    lock = state["lock"]
    jobs = state["jobs"]
    with lock:
        job = jobs.get(job_key)
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
    ingles_class_ids: Tuple[int, ...],
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

        rows_auto, warnings_auto, skipped_ingles_auto = _build_auto_group_rows_for_participantes(
            clases=clases,
            niveles_data=niveles_data,
            exclude_ingles_por_niveles=bool(exclude_ingles_por_niveles),
            ingles_grade_keys={str(item) for item in ingles_grade_keys if str(item).strip()},
            ingles_class_ids={
                int(item)
                for item in ingles_class_ids
                if _safe_int(item) is not None
            },
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
        if skipped_ingles_auto:
            _append_participantes_sync_job_message(
                job_id,
                "Clases de Ingles por niveles protegidas={total}. No se vacian ni se sincronizan en la asignacion masiva.".format(
                    total=len(skipped_ingles_auto)
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
            error_trace="",
        )
        _append_participantes_sync_job_message(job_id, "Proceso cancelado por el usuario.")
        return
    except Exception as exc:
        trace_text = traceback.format_exc()
        _set_participantes_sync_job(
            job_id,
            state="error",
            summary=summary_auto,
            detail_rows=detail_rows_auto,
            warnings=warnings_auto,
            group_error_lines=group_error_lines,
            error=str(exc),
            error_trace=trace_text,
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
        error_trace="",
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
    ingles_class_ids: Optional[Sequence[object]] = None,
) -> str:
    scope = (int(empresa_id), int(ciclo_id), int(colegio_id))
    ingles_grade_keys_tuple = tuple(
        str(item).strip()
        for item in (ingles_grade_keys or [])
        if str(item).strip()
    )
    ingles_class_ids_tuple = tuple(
        int(_safe_int(item))
        for item in (ingles_class_ids or [])
        if _safe_int(item) is not None
    )
    state_store = _get_participantes_sync_state()
    lock = state_store["lock"]
    jobs = state_store["jobs"]
    scope_to_job = state_store["scope_to_job"]
    existing_id = ""
    existing_job: Optional[Dict[str, object]] = None
    with lock:
        existing_id = str(scope_to_job.get(scope) or "").strip()
        existing_job = jobs.get(existing_id) if existing_id else None
    if existing_id:
        _reconcile_participantes_sync_job(existing_id)
        with lock:
            existing_job = jobs.get(existing_id)
    with lock:
        if isinstance(existing_job, dict) and str(existing_job.get("state") or "").strip() in {
            "starting",
            "running",
        }:
            return str(existing_id)

        job_id = uuid4().hex
        jobs[job_id] = {
            "job_id": job_id,
            "scope": scope,
            "state": "starting",
            "cancel_requested": False,
            "exclude_ingles_por_niveles": bool(exclude_ingles_por_niveles),
            "ingles_grade_keys": list(ingles_grade_keys_tuple),
            "ingles_class_ids": list(ingles_class_ids_tuple),
            "status_messages": [],
            "summary": _make_participantes_sync_summary(),
            "warnings": [],
            "group_error_lines": [],
            "detail_rows": [],
            "error": "",
            "error_trace": "",
        }
        scope_to_job[scope] = job_id

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
            ingles_class_ids_tuple,
        ),
        daemon=True,
        name=f"participantes-sync-{job_id[:8]}",
    )
    with lock:
        job = jobs.get(job_id)
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
        clear_reason = str(row.get("clear_reason") or "").strip() or "Clase especial"
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
                "Vaciando clase ({reason}) {idx}/{total}: {clase_id} | {clase}".format(
                    reason=clear_reason,
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
                        "Detalle": (
                            f"No se pudo listar alumnos actuales para vaciado "
                            f"({clear_reason}): {exc}"
                        ),
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
                detalle_txt = f"Vaciado ({clear_reason}) con errores al eliminar={len(remove_errors)}"
            elif not to_remove:
                summary["clases_skip"] += 1
                resultado = "Sin cambios"
                detalle_txt = f"La clase ({clear_reason}) ya no tenia alumnos."
            else:
                summary["clases_ok"] += 1
                resultado = "OK"
                detalle_txt = f"Se quitaron todos los alumnos de la clase ({clear_reason})."

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
                omitted_outside_context = 0
                for item in alumnos_ctx:
                    if not isinstance(item, dict):
                        continue
                    if not _censo_alumno_matches_context(item=item, context=ctx):
                        omitted_outside_context += 1
                        continue
                    flat = _flatten_censo_alumno_for_auto_plan(item=item, fallback=ctx)
                    if not _to_bool(flat.get("activo")):
                        continue
                    alumno_id = _safe_int(flat.get("alumno_id"))
                    if alumno_id is None:
                        continue
                    activos_tmp[int(alumno_id)] = flat
                if omitted_outside_context:
                    _status(
                        "Omitidos {count} alumno(s) fuera del grupo consultado: nivel={nivel} grado={grado} grupo={grupo}".format(
                            count=omitted_outside_context,
                            nivel=int(nivel_id),
                            grado=int(grado_id),
                            grupo=int(selected_group_id),
                        )
                    )
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
    exclude_santillana_inclusiva: bool = False,
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
        if exclude_santillana_inclusiva and _is_santillana_inclusiva_class(clase):
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


def _ingles_assignment_template_rows() -> List[Dict[str, str]]:
    return [
        {
            "Nombre": "Mia Cataleya",
            "Apellido Paterno": "BEJARANO",
            "Apellido Materno": "IPANAQUE",
            "Clase": "PAI 2 - EMERGENT D",
        }
    ]


def _clear_ingles_por_niveles_assignment_state() -> None:
    for state_key in (
        "clases_auto_group_ingles_excel_preview_rows",
        "clases_auto_group_ingles_excel_apply_rows",
        "clases_auto_group_ingles_excel_fetch_errors",
        "clases_auto_group_ingles_excel_reference_students",
        "clases_auto_group_ingles_excel_result_notice",
    ):
        st.session_state.pop(state_key, None)
    for state_key in list(st.session_state.keys()):
        if str(state_key).startswith("clases_auto_group_ingles_ref_select_"):
            st.session_state.pop(state_key, None)


def _set_ingles_por_niveles_result_notice(kind: str, message: str) -> None:
    st.session_state["clases_auto_group_ingles_excel_result_notice"] = {
        "kind": str(kind or "success").strip().lower() or "success",
        "message": str(message or "").strip(),
    }


def _set_participantes_ingles_grade_selection(
    selected_keys: Sequence[object],
    grade_options: Optional[Sequence[Dict[str, object]]] = None,
) -> None:
    normalized_keys: List[str] = []
    seen_keys: Set[str] = set()
    for value in (selected_keys or []):
        key = str(value or "").strip()
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        normalized_keys.append(key)

    st.session_state["clases_auto_group_ingles_grade_selected_keys"] = list(
        normalized_keys
    )

    options_source = (
        list(grade_options)
        if isinstance(grade_options, (list, tuple))
        else list(st.session_state.get("clases_auto_group_ingles_grade_options") or [])
    )
    for option in options_source:
        if not isinstance(option, dict):
            continue
        option_key = str(option.get("key") or "").strip()
        if not option_key:
            continue
        st.session_state[_participantes_ingles_grade_checkbox_key(option_key)] = (
            option_key in seen_keys
        )


def _format_participantes_ingles_grade_label(
    option_row: Dict[str, object],
    include_class_count: bool = True,
) -> str:
    label = (
        f"{str(option_row.get('nivel_nombre') or '').strip() or '-'} | "
        f"{str(option_row.get('grado_nombre') or '').strip() or '-'}"
    )
    if not include_class_count:
        return label
    class_names = (
        option_row.get("class_names")
        if isinstance(option_row.get("class_names"), list)
        else []
    )
    if class_names:
        return f"{label} ({len(class_names)} clase(s))"
    return label


def _fetch_alumnos_context_catalog_for_ingles_detection(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    niveles_data: Optional[List[Dict[str, object]]] = None,
    on_status: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message or ""))
            except Exception:
                pass

    niveles = (
        list(niveles_data)
        if isinstance(niveles_data, list)
        else _fetch_niveles_grados_grupos_censo(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
    )
    contexts = _build_contexts_for_nivel_grado(niveles=niveles)
    if not contexts:
        raise RuntimeError("No hay niveles/grados/secciones configurados para este colegio.")

    alumnos_raw: List[Dict[str, object]] = []
    errors: List[str] = []
    _status("Consultando alumnos del colegio para validar Ingles por niveles...")
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
            alumnos_raw.append(_flatten_censo_alumno_for_auto_plan(item=item, fallback={}))
    except Exception as exc:
        errors.append(f"alumnosByFilters: {exc}")
        total_contexts = len(contexts)
        for idx_ctx, ctx in enumerate(contexts, start=1):
            _status(
                "Respaldo alumnos {idx}/{total} | nivelId={nivel} gradoId={grado} grupoId={grupo}".format(
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
                alumnos_raw.append(
                    _flatten_censo_alumno_for_auto_plan(item=item, fallback=ctx)
                )

    students = _dedupe_and_sort_censo_students(alumnos_raw)
    return {
        "niveles": niveles,
        "students": students,
        "errors": errors,
    }


def _detect_ingles_por_niveles_behavior(
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
                on_status(str(message or ""))
            except Exception:
                pass

    _status("Listando clases del colegio...")
    clases = _fetch_clases_gestion_escolar(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
        ordered=True,
    )
    english_classes = [
        item
        for item in clases
        if isinstance(item, dict) and _is_ingles_por_niveles_class(item)
    ]
    detected_grade_options = _build_ingles_grade_options_for_participantes(clases)
    grade_options = list(detected_grade_options)
    niveles_data: List[Dict[str, object]] = []
    try:
        niveles_data = _fetch_niveles_grados_grupos_censo(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
    except Exception:
        niveles_data = []
    else:
        grade_options = _build_ingles_grade_catalog_options_for_participantes(
            niveles_data,
            detected_options=detected_grade_options,
        )
    if not english_classes:
        return {
            "detected": False,
            "error": "",
            "grade_options": grade_options,
            "affected_grade_keys": [],
            "affected_grade_labels": [],
            "evidence_rows": [],
            "evidence_total": 0,
        }

    students_catalog = _fetch_alumnos_context_catalog_for_ingles_detection(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
        niveles_data=niveles_data or None,
        on_status=on_status,
    )
    students = [
        row
        for row in (students_catalog.get("students") or [])
        if isinstance(row, dict) and _safe_int(row.get("alumno_id")) is not None
    ]
    if not students:
        raise RuntimeError(
            "No se pudieron leer los alumnos del colegio para validar Ingles por niveles."
        )

    student_by_id = {
        int(row["alumno_id"]): row
        for row in students
        if _safe_int(row.get("alumno_id")) is not None
    }
    grade_option_by_key = {
        str(option.get("key") or "").strip(): option
        for option in grade_options
        if isinstance(option, dict) and str(option.get("key") or "").strip()
    }
    affected_grade_keys: Set[str] = set()
    evidence_rows: List[Dict[str, object]] = []
    evidence_total = 0
    class_errors: List[str] = []
    total_english_classes = len(english_classes)

    for idx_class, item in enumerate(english_classes, start=1):
        meta = _extract_clase_base_meta(item)
        if not isinstance(meta, dict):
            continue
        clase_id = _safe_int(meta.get("clase_id"))
        if clase_id is None:
            continue
        class_label = str(meta.get("clase_nombre") or f"Clase {int(clase_id)}").strip()
        _status(
            "Validando clases de Ingles {idx}/{total}: {clase}".format(
                idx=idx_class,
                total=total_english_classes,
                clase=class_label or "-",
            )
        )
        try:
            class_data = _fetch_alumnos_clase_gestion_escolar(
                token=token,
                clase_id=int(clase_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
            )
        except Exception as exc:
            class_errors.append(f"Clase {int(clase_id)}: {exc}")
            continue

        class_nivel_id = _safe_int(meta.get("nivel_id"))
        class_grado_id = _safe_int(meta.get("grado_id"))
        class_grupo_id = _safe_int(meta.get("grupo_id_actual"))
        class_nivel = str(meta.get("nivel_nombre") or "").strip()
        class_grado = str(meta.get("grado_nombre") or "").strip()
        class_seccion = str(meta.get("grupo_clave_actual") or "").strip()
        class_seccion_norm = _normalize_seccion_key(class_seccion)
        option_key = _participantes_ingles_option_key_from_meta(meta)

        for member in _extract_clase_alumno_rows(class_data):
            alumno_id = _safe_int(member.get("alumno_id"))
            if alumno_id is None:
                continue
            student = student_by_id.get(int(alumno_id))
            if not isinstance(student, dict):
                continue

            student_nivel_id = _safe_int(student.get("nivel_id"))
            student_grado_id = _safe_int(student.get("grado_id"))
            student_grupo_id = _safe_int(student.get("grupo_id"))
            student_nivel = str(student.get("nivel") or "").strip()
            student_grado = str(student.get("grado") or "").strip()
            student_seccion = str(
                student.get("seccion_norm") or student.get("seccion") or ""
            ).strip()
            student_seccion_norm = _normalize_seccion_key(student_seccion)

            nivel_diff = (
                class_nivel_id is not None
                and student_nivel_id is not None
                and int(class_nivel_id) != int(student_nivel_id)
            )
            grado_diff = (
                class_grado_id is not None
                and student_grado_id is not None
                and int(class_grado_id) != int(student_grado_id)
            )
            seccion_diff = False
            if class_grupo_id is not None and student_grupo_id is not None:
                seccion_diff = int(class_grupo_id) != int(student_grupo_id)
            elif class_seccion_norm and student_seccion_norm:
                seccion_diff = class_seccion_norm != student_seccion_norm

            if not (nivel_diff or grado_diff or seccion_diff):
                continue

            mismatch_parts: List[str] = []
            if nivel_diff or grado_diff:
                mismatch_parts.append("otro grado")
            if seccion_diff:
                mismatch_parts.append("otra seccion")
            evidence_total += 1
            if option_key:
                affected_grade_keys.add(option_key)
            if len(evidence_rows) >= 40:
                continue
            evidence_rows.append(
                {
                    "Alumno": str(
                        student.get("nombre_completo")
                        or member.get("nombre_completo")
                        or f"Alumno {int(alumno_id)}"
                    ).strip(),
                    "Clase ingles": class_label,
                    "Curso ingles": " | ".join(
                        part for part in (class_nivel, class_grado, class_seccion) if part
                    ),
                    "Alumno base": " | ".join(
                        part
                        for part in (student_nivel, student_grado, student_seccion)
                        if part
                    ),
                    "Diferencia": " y ".join(mismatch_parts),
                }
            )

    affected_grade_keys_sorted = [
        option_key
        for option_key in grade_option_by_key.keys()
        if option_key in affected_grade_keys
    ]
    affected_grade_labels = [
        _format_participantes_ingles_grade_label(
            grade_option_by_key[option_key],
            include_class_count=False,
        )
        for option_key in affected_grade_keys_sorted
        if option_key in grade_option_by_key
    ]

    if evidence_total:
        return {
            "detected": True,
            "error": "",
            "grade_options": grade_options,
            "affected_grade_keys": affected_grade_keys_sorted,
            "affected_grade_labels": affected_grade_labels,
            "evidence_rows": evidence_rows,
            "evidence_total": int(evidence_total),
            "class_errors": class_errors[:12],
        }

    if class_errors:
        return {
            "detected": False,
            "error": (
                "No se pudo validar Ingles por niveles en {count} clase(s) de Ingles. "
                "Reintenta antes de actualizar la asignacion."
            ).format(count=len(class_errors)),
            "grade_options": grade_options,
            "affected_grade_keys": [],
            "affected_grade_labels": [],
            "evidence_rows": [],
            "evidence_total": 0,
            "class_errors": class_errors[:12],
        }

    return {
        "detected": False,
        "error": "",
        "grade_options": grade_options,
        "affected_grade_keys": [],
        "affected_grade_labels": [],
        "evidence_rows": [],
        "evidence_total": 0,
        "class_errors": [],
    }


@st.dialog("Ingles por niveles detectado", width="large")
def _show_ingles_por_niveles_detected_dialog() -> None:
    payload = (
        dict(st.session_state.get("clases_auto_group_ingles_detected_dialog") or {})
        if isinstance(st.session_state.get("clases_auto_group_ingles_detected_dialog"), dict)
        else {}
    )
    if not payload:
        return

    st.warning(
        "Este colegio lleva ingles por niveles. Debes seleccionar los grados que llevan "
        "para que no se mezcle y no se cambie al alumno de seccion."
    )

    affected_grade_labels = [
        str(item).strip()
        for item in list(payload.get("affected_grade_labels") or [])
        if str(item).strip()
    ]
    if affected_grade_labels:
        st.markdown("**Grados detectados**")
        st.markdown("\n".join(f"- {item}" for item in affected_grade_labels))

    evidence_rows = [
        row for row in list(payload.get("evidence_rows") or []) if isinstance(row, dict)
    ]
    evidence_total = int(_safe_int(payload.get("evidence_total")) or len(evidence_rows))
    if evidence_rows:
        st.markdown("**Ejemplos detectados**")
        _show_dataframe(evidence_rows[:12], use_container_width=True)
        if evidence_total > len(evidence_rows):
            st.caption(
                f"Mostrando {len(evidence_rows)} de {evidence_total} coincidencia(s)."
            )

    class_errors = [
        str(item).strip()
        for item in list(payload.get("class_errors") or [])
        if str(item).strip()
    ]
    if class_errors:
        with st.expander(f"Observaciones de lectura ({len(class_errors)})"):
            st.write("\n".join(f"- {item}" for item in class_errors))

    col_enable, col_close = st.columns([1.7, 1], gap="small")
    if col_enable.button(
        "Activar ingles por niveles",
        key="clases_auto_group_ingles_detected_enable_btn",
        use_container_width=True,
    ):
        suggested_grade_keys = list(payload.get("affected_grade_keys") or [])
        grade_options = [
            row for row in list(payload.get("grade_options") or []) if isinstance(row, dict)
        ]
        scope = payload.get("scope")
        st.session_state["clases_auto_group_exclude_ingles_checkbox"] = True
        if scope is not None:
            st.session_state["clases_auto_group_ingles_grades_scope"] = scope
        st.session_state["clases_auto_group_ingles_grade_options"] = list(grade_options)
        st.session_state["clases_auto_group_ingles_grade_error"] = ""
        _set_participantes_ingles_grade_selection(
            suggested_grade_keys,
            grade_options=grade_options,
        )
        st.session_state.pop("clases_auto_group_ingles_detected_dialog", None)
        _set_ingles_por_niveles_result_notice(
            "warning",
            "Se detecto Ingles por niveles. Revisa los grados sugeridos antes de actualizar.",
        )
        st.rerun()

    if col_close.button(
        "Cerrar",
        key="clases_auto_group_ingles_detected_close_btn",
        use_container_width=True,
    ):
        st.session_state.pop("clases_auto_group_ingles_detected_dialog", None)
        st.rerun()


def _load_ingles_assignment_rows_from_excel(excel_bytes: bytes) -> List[Dict[str, object]]:
    try:
        df = pd.read_excel(BytesIO(excel_bytes), dtype=str)
    except Exception as exc:
        raise ValueError(f"No se pudo leer el Excel: {exc}") from exc

    normalized_columns = {
        _normalize_import_column(column): str(column)
        for column in list(df.columns)
    }
    nombre_column = _resolve_import_column(
        normalized_columns,
        ["Nombre", "Nombres", "First name"],
        required=True,
    )
    ap_pat_column = _resolve_import_column(
        normalized_columns,
        ["Apellido Paterno", "Apellido paterno", "Paterno", "Last name"],
        required=True,
    )
    ap_mat_column = _resolve_import_column(
        normalized_columns,
        ["Apellido Materno", "Apellido materno", "Materno", "Second last name"],
        required=True,
    )
    clase_column = _resolve_import_column(
        normalized_columns,
        ["Clase", "Class", "Class name", "Grupo"],
        required=True,
    )

    rows: List[Dict[str, object]] = []
    for idx, item in enumerate(df.fillna("").to_dict("records"), start=2):
        if not isinstance(item, dict):
            continue
        normalized_row = {
            "Nombre": str(item.get(nombre_column) or "").strip(),
            "Apellido Paterno": str(item.get(ap_pat_column) or "").strip(),
            "Apellido Materno": str(item.get(ap_mat_column) or "").strip(),
            "Clase": str(item.get(clase_column) or "").strip(),
            "_row_number": int(idx),
        }
        values = [
            str(normalized_row.get("Nombre") or "").strip(),
            str(normalized_row.get("Apellido Paterno") or "").strip(),
            str(normalized_row.get("Apellido Materno") or "").strip(),
            str(normalized_row.get("Clase") or "").strip(),
        ]
        if not any(values):
            continue
        rows.append(normalized_row)

    if not rows:
        raise ValueError("El Excel no tiene filas con datos para procesar.")
    return rows


def _build_ingles_assignment_class_key(value: object) -> str:
    return _normalize_compare_text(value)


def _build_ingles_assignment_students_lookup(
    students: List[Dict[str, object]]
) -> Dict[Tuple[str, str], List[Dict[str, object]]]:
    lookup: Dict[Tuple[str, str], List[Dict[str, object]]] = {}
    for row in students:
        if not isinstance(row, dict):
            continue
        key = (
            row.get("apellido_paterno"),
            row.get("apellido_materno"),
        )
        key = (
            _normalize_compare_apellido(key[0]),
            _normalize_compare_apellido(key[1]),
        )
        if not any(key):
            continue
        lookup.setdefault(key, []).append(row)
    return lookup


def _build_ingles_assignment_students_full_name_lookup(
    students: List[Dict[str, object]]
) -> Dict[str, List[Dict[str, object]]]:
    lookup: Dict[str, List[Dict[str, object]]] = {}
    for row in students:
        if not isinstance(row, dict):
            continue
        key = _normalize_compare_text(row.get("nombre_completo"))
        if not key:
            continue
        lookup.setdefault(key, []).append(row)
    return lookup


def _build_ingles_assignment_default_reference_option(
    row: Dict[str, object],
    students_full_name_lookup: Dict[str, List[Dict[str, object]]],
) -> str:
    alumno_id = _safe_int(row.get("_alumno_id"))
    if alumno_id is not None:
        return str(int(alumno_id))
    excel_full_name_key = _normalize_compare_text(
        _build_ingles_assignment_excel_full_name(row)
    )
    if not excel_full_name_key:
        return ""
    matched_ids = sorted(
        {
            int(candidate_id)
            for candidate in (students_full_name_lookup.get(excel_full_name_key) or [])
            for candidate_id in [_safe_int(candidate.get("alumno_id"))]
            if candidate_id is not None
        }
    )
    if len(matched_ids) == 1:
        return str(int(matched_ids[0]))
    return ""


def _sort_ingles_assignment_review_rows(
    rows: List[Dict[str, object]]
) -> List[Dict[str, object]]:
    sorted_rows = [dict(row) for row in rows if isinstance(row, dict)]
    sorted_rows.sort(
        key=lambda row: (
            1 if _safe_int(row.get("_alumno_id")) is None else 0,
            0 if str(row.get("Estado") or "").strip() == "Listo" else 1,
            int(_safe_int(row.get("Fila")) or 0),
            _normalize_compare_text(_build_ingles_assignment_excel_full_name(row)),
        )
    )
    return sorted_rows


def _merge_ingles_assignment_rows(
    base_rows: List[Dict[str, object]],
    updated_rows: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    updated_by_fila: Dict[int, Dict[str, object]] = {}
    for row in updated_rows:
        if not isinstance(row, dict):
            continue
        fila = _safe_int(row.get("Fila"))
        if fila is None:
            continue
        updated_by_fila[int(fila)] = dict(row)

    merged_rows: List[Dict[str, object]] = []
    for row in base_rows:
        if not isinstance(row, dict):
            continue
        fila = _safe_int(row.get("Fila"))
        if fila is not None and int(fila) in updated_by_fila:
            merged_rows.append(dict(updated_by_fila[int(fila)]))
        else:
            merged_rows.append(dict(row))
    return merged_rows


def _filter_ingles_assignment_rows_by_selected_ingles_grades(
    rows: List[Dict[str, object]],
    selected_ingles_grade_keys: Sequence[object],
    include_unresolved: bool = True,
) -> List[Dict[str, object]]:
    selected_keys = {
        str(value or "").strip()
        for value in (selected_ingles_grade_keys or [])
        if str(value or "").strip()
    }
    if not selected_keys:
        return []
    return [
        dict(row)
        for row in rows
        if isinstance(row, dict)
        and (
            str(row.get("_ingles_grade_key") or "").strip() in selected_keys
            or (
                include_unresolved
                and not str(row.get("_ingles_grade_key") or "").strip()
            )
            )
        ]


def _collect_ingles_class_ids_from_rows(
    rows: List[Dict[str, object]],
    selected_ingles_grade_keys: Sequence[object],
) -> List[int]:
    class_ids: List[int] = []
    seen: Set[int] = set()
    for row in _filter_ingles_assignment_rows_by_selected_ingles_grades(
        rows,
        selected_ingles_grade_keys,
        include_unresolved=False,
    ):
        if not isinstance(row, dict):
            continue
        clase_id = _safe_int(row.get("_clase_id"))
        if clase_id is None or int(clase_id) in seen:
            continue
        seen.add(int(clase_id))
        class_ids.append(int(clase_id))
    return class_ids


def _prepare_ingles_assignment_review_rows(
    preview_rows: List[Dict[str, object]],
    students: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    if not preview_rows:
        return []
    students_full_name_lookup = _build_ingles_assignment_students_full_name_lookup(students)
    selected_student_ids_by_fila: Dict[int, Optional[int]] = {}
    for row in preview_rows:
        if not isinstance(row, dict):
            continue
        fila = int(_safe_int(row.get("Fila")) or 0)
        default_option = _build_ingles_assignment_default_reference_option(
            row=row,
            students_full_name_lookup=students_full_name_lookup,
        )
        selected_student_ids_by_fila[fila] = _safe_int(default_option)
    reviewed_rows = _build_ingles_assignment_review_rows_from_selection(
        preview_rows=preview_rows,
        students=students,
        selected_student_ids_by_fila=selected_student_ids_by_fila,
    )
    return _sort_ingles_assignment_review_rows(reviewed_rows)


def _build_ingles_assignment_name_tokens(value: object) -> List[str]:
    return [token for token in _normalize_compare_text(value).split() if token]


def _ingles_assignment_given_name_match_score(
    requested_name: object,
    candidate_name: object,
) -> int:
    requested_norm = _normalize_compare_text(requested_name)
    candidate_norm = _normalize_compare_text(candidate_name)
    if not requested_norm or not candidate_norm:
        return 0
    if requested_norm == candidate_norm:
        return 3

    requested_tokens = _build_ingles_assignment_name_tokens(requested_name)
    candidate_tokens = _build_ingles_assignment_name_tokens(candidate_name)
    if not requested_tokens or not candidate_tokens:
        return 0
    if requested_tokens == candidate_tokens[: len(requested_tokens)]:
        return 2
    if all(token in candidate_tokens for token in requested_tokens):
        return 1
    return 0


def _find_ingles_assignment_student_matches(
    nombre: object,
    apellido_paterno: object,
    apellido_materno: object,
    students_lookup: Dict[Tuple[str, str], List[Dict[str, object]]],
    students_full_name_lookup: Dict[str, List[Dict[str, object]]],
) -> Tuple[List[Dict[str, object]], str]:
    surname_key = (
        _normalize_compare_apellido(apellido_paterno),
        _normalize_compare_apellido(apellido_materno),
    )
    candidates = list(students_lookup.get(surname_key) or [])
    matched_by_score: Dict[int, List[Dict[str, object]]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        score = max(
            _ingles_assignment_given_name_match_score(nombre, candidate.get("nombre")),
            _ingles_assignment_given_name_match_score(
                nombre,
                candidate.get("nombre_completo"),
            ),
        )
        if score <= 0:
            continue
        matched_by_score.setdefault(int(score), []).append(candidate)

    for score, mode in ((3, "exacto"), (2, "prefijo"), (1, "parcial")):
        matches = matched_by_score.get(score) or []
        if matches:
            return matches, mode
    requested_full_name = _normalize_compare_text(
        " ".join(
            part
            for part in (
                str(nombre or "").strip(),
                str(apellido_paterno or "").strip(),
                str(apellido_materno or "").strip(),
            )
            if part
        )
    )
    if requested_full_name:
        full_name_matches = list(students_full_name_lookup.get(requested_full_name) or [])
        if full_name_matches:
            return full_name_matches, "nombre_completo"
    return [], ""


def _build_ingles_assignment_classes_lookup(
    clases: List[Dict[str, object]]
) -> Dict[str, List[Dict[str, object]]]:
    lookup: Dict[str, List[Dict[str, object]]] = {}
    seen_class_ids: Set[int] = set()
    for item in clases:
        if not isinstance(item, dict):
            continue
        meta = _extract_clase_base_meta(item)
        clase_id = _safe_int(meta.get("clase_id")) if isinstance(meta, dict) else None
        if not isinstance(meta, dict) or clase_id is None or int(clase_id) in seen_class_ids:
            continue
        seen_class_ids.add(int(clase_id))
        class_name = str(meta.get("clase_nombre") or "").strip()
        key = _build_ingles_assignment_class_key(class_name)
        if not key:
            continue
        lookup.setdefault(key, []).append(
            {
                "clase_id": int(clase_id),
                "clase_nombre": class_name,
                "nivel_id": _safe_int(meta.get("nivel_id")),
                "nivel_nombre": str(meta.get("nivel_nombre") or "").strip(),
                "grado_id": _safe_int(meta.get("grado_id")),
                "grado_nombre": str(meta.get("grado_nombre") or "").strip(),
                "seccion": str(meta.get("grupo_clave_actual") or "").strip(),
                "ingles_grade_key": _participantes_ingles_option_key_from_meta(meta),
            }
        )
    return lookup


def _build_ingles_assignment_students_by_id(
    students: List[Dict[str, object]]
) -> Dict[int, Dict[str, object]]:
    mapping: Dict[int, Dict[str, object]] = {}
    for row in students:
        if not isinstance(row, dict):
            continue
        alumno_id = _safe_int(row.get("alumno_id"))
        if alumno_id is None or int(alumno_id) in mapping:
            continue
        mapping[int(alumno_id)] = row
    return mapping


def _build_ingles_assignment_reference_option_label(row: Dict[str, object]) -> str:
    return str(row.get("nombre_completo") or "").strip() or "SIN NOMBRE"


def _build_ingles_assignment_excel_full_name(row: Dict[str, object]) -> str:
    parts = [
        str(row.get("Nombre") or "").strip(),
        str(row.get("Apellido Paterno") or "").strip(),
        str(row.get("Apellido Materno") or "").strip(),
    ]
    return " ".join(part for part in parts if part).strip() or "SIN NOMBRE"


def _build_ingles_assignment_grade_filter_label(row: Dict[str, object]) -> str:
    nivel = str(row.get("Nivel clase") or "").strip()
    grado = str(row.get("Grado clase") or "").strip()
    if nivel and grado:
        return f"{nivel} | {grado}"
    if grado:
        return grado
    return "Sin grado/clase"


def _hydrate_ingles_assignment_preview_row(
    row: Dict[str, object],
    student_row: Optional[Dict[str, object]],
    duplicate_filas: Optional[List[int]] = None,
) -> Dict[str, object]:
    updated = dict(row) if isinstance(row, dict) else {}
    alumno_id = _safe_int(student_row.get("alumno_id")) if isinstance(student_row, dict) else None
    auto_alumno_id = _safe_int(updated.get("_auto_alumno_id"))
    student_match_count = int(
        _safe_int(updated.get("_student_match_count"))
        or (1 if auto_alumno_id is not None else 0)
    )
    class_match_count = int(
        _safe_int(updated.get("_class_match_count"))
        or (1 if _safe_int(updated.get("_clase_id")) is not None else 0)
    )
    student_match_mode = str(updated.get("_student_match_mode") or "").strip()
    fila = int(_safe_int(updated.get("Fila")) or 0)
    duplicate_filas_clean = sorted(
        {
            int(_safe_int(item) or 0)
            for item in (duplicate_filas or [])
            if _safe_int(item) is not None and int(_safe_int(item) or 0) != fila
        }
    )
    manual_selected = alumno_id is not None and alumno_id != auto_alumno_id

    updated["Alumno encontrado"] = _format_alumno_label(student_row) if student_row else ""
    updated["Alumno ID"] = alumno_id if alumno_id is not None else ""
    updated["DNI"] = (
        str(student_row.get("id_oficial") or "").strip()
        if isinstance(student_row, dict)
        else ""
    )
    if isinstance(student_row, dict):
        updated["Activo"] = "Si" if _to_bool(student_row.get("activo")) else "No"
        updated["Seccion actual"] = str(
            student_row.get("seccion_norm") or student_row.get("seccion") or ""
        ).strip()
        updated["_nivel_id"] = _safe_int(student_row.get("nivel_id"))
        updated["_grado_id"] = _safe_int(student_row.get("grado_id"))
        updated["_grupo_id"] = _safe_int(student_row.get("grupo_id"))
        updated["_activo"] = bool(_to_bool(student_row.get("activo")))
    else:
        updated["Activo"] = ""
        updated["Seccion actual"] = ""
        updated["_nivel_id"] = None
        updated["_grado_id"] = None
        updated["_grupo_id"] = None
        updated["_activo"] = False
    updated["_alumno_id"] = alumno_id

    nombre = str(updated.get("Nombre") or "").strip()
    ap_pat = str(updated.get("Apellido Paterno") or "").strip()
    ap_mat = str(updated.get("Apellido Materno") or "").strip()
    clase = str(updated.get("Clase solicitada") or "").strip()
    has_student_reference = alumno_id is not None
    updated["_grado_filtro"] = _build_ingles_assignment_grade_filter_label(updated)

    estado = "Listo"
    detalle = "Se asignara a la clase encontrada."
    if not clase:
        estado = "Error"
        detalle = "Falta Clase."
    elif not has_student_reference and not all([nombre, ap_pat, ap_mat]):
        estado = "Error"
        detalle = (
            "Faltan Nombre, Apellido Paterno o Apellido Materno. "
            "Tambien puedes seleccionar el alumno manualmente en Referencia del alumno."
        )
    elif alumno_id is None:
        if student_match_count > 1:
            estado = "Revisar"
            detalle = (
                "Alumno ambiguo: {total} coincidencias por nombre {mode}. "
                "Selecciona el correcto en Referencia del alumno."
            ).format(
                total=student_match_count,
                mode=student_match_mode or "compatible",
            )
        else:
            estado = "Error"
            detalle = "Alumno no encontrado. Selecciona el correcto en Referencia del alumno."
    elif class_match_count <= 0:
        estado = "Error"
        detalle = "Clase no encontrada."
    elif class_match_count > 1:
        estado = "Error"
        detalle = f"Clase ambigua: {class_match_count} coincidencias exactas."
    elif duplicate_filas_clean:
        estado = "Revisar"
        duplicate_txt = ", ".join(str(item) for item in duplicate_filas_clean)
        detail_parts: List[str] = []
        if manual_selected:
            detail_parts.append("Alumno seleccionado manualmente desde Referencia del alumno.")
        detail_parts.append(
            f"El mismo alumno ya esta referenciado en la(s) fila(s) {duplicate_txt}."
        )
        detail_parts.append("Corrige la referencia antes de aplicar.")
        detalle = " ".join(detail_parts)
    else:
        detail_parts = []
        if manual_selected:
            detail_parts.append("Alumno seleccionado manualmente desde Referencia del alumno.")
        elif student_match_mode == "prefijo":
            detail_parts.append(
                "Alumno encontrado por coincidencia de prefijo en nombres."
            )
        elif student_match_mode == "parcial":
            detail_parts.append(
                "Alumno encontrado por coincidencia parcial en nombres."
            )
        elif student_match_mode == "nombre_completo":
            detail_parts.append(
                "Alumno encontrado por coincidencia exacta de nombre completo."
            )
        if isinstance(student_row, dict) and not _to_bool(student_row.get("activo")):
            detail_parts.append("Alumno inactivo: se activara antes de asignar.")
        detail_parts.append("Se asignara a la clase encontrada.")
        detalle = " ".join(detail_parts)

    updated["Estado"] = estado
    updated["Detalle"] = detalle
    return updated


def _build_ingles_assignment_preview_rows(
    excel_rows: List[Dict[str, object]],
    students: List[Dict[str, object]],
    clases: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    students_lookup = _build_ingles_assignment_students_lookup(students)
    students_full_name_lookup = _build_ingles_assignment_students_full_name_lookup(students)
    classes_lookup = _build_ingles_assignment_classes_lookup(clases)
    preview_rows: List[Dict[str, object]] = []

    for raw_row in excel_rows:
        if not isinstance(raw_row, dict):
            continue
        nombre = str(raw_row.get("Nombre") or "").strip()
        ap_pat = str(raw_row.get("Apellido Paterno") or "").strip()
        ap_mat = str(raw_row.get("Apellido Materno") or "").strip()
        clase = str(raw_row.get("Clase") or "").strip()
        row_number = _safe_int(raw_row.get("_row_number")) or 0

        matched_students, student_match_mode = _find_ingles_assignment_student_matches(
            nombre,
            ap_pat,
            ap_mat,
            students_lookup,
            students_full_name_lookup,
        )
        matched_classes = classes_lookup.get(
            _build_ingles_assignment_class_key(clase),
            [],
        )

        student_row = matched_students[0] if len(matched_students) == 1 else {}
        class_row = matched_classes[0] if len(matched_classes) == 1 else {}
        estado = "Listo"
        detalle = "Se asignara a la clase encontrada."

        if not all([nombre, ap_pat, ap_mat, clase]):
            estado = "Error"
            detalle = "Faltan Nombre, Apellido Paterno, Apellido Materno o Clase."
        elif not matched_students:
            estado = "Error"
            detalle = "Alumno no encontrado."
        elif len(matched_students) > 1:
            estado = "Error"
            detalle = (
                "Alumno ambiguo: {total} coincidencias por nombre {mode}.".format(
                    total=len(matched_students),
                    mode=student_match_mode or "compatible",
                )
            )
        elif not matched_classes:
            estado = "Error"
            detalle = "Clase no encontrada."
        elif len(matched_classes) > 1:
            estado = "Error"
            detalle = f"Clase ambigua: {len(matched_classes)} coincidencias exactas."
        else:
            detail_parts: List[str] = []
            if student_match_mode == "prefijo":
                detail_parts.append(
                    "Alumno encontrado por coincidencia de prefijo en nombres."
                )
            elif student_match_mode == "parcial":
                detail_parts.append(
                    "Alumno encontrado por coincidencia parcial en nombres."
                )
            elif student_match_mode == "nombre_completo":
                detail_parts.append(
                    "Alumno encontrado por coincidencia exacta de nombre completo."
                )
            if not _to_bool(student_row.get("activo")):
                detail_parts.append("Alumno inactivo: se activara antes de asignar.")
            detail_parts.append("Se asignara a la clase encontrada.")
            detalle = " ".join(detail_parts)

        preview_rows.append(
            _hydrate_ingles_assignment_preview_row(
                {
                    "Fila": int(row_number),
                    "Nombre": nombre,
                    "Apellido Paterno": ap_pat,
                    "Apellido Materno": ap_mat,
                    "Clase solicitada": clase,
                    "Alumno encontrado": _format_alumno_label(student_row) if student_row else "",
                    "Alumno ID": _safe_int(student_row.get("alumno_id")) if student_row else "",
                    "DNI": str(student_row.get("id_oficial") or "").strip() if student_row else "",
                    "Activo": "Si" if _to_bool(student_row.get("activo")) else "No" if student_row else "",
                    "Seccion actual": str(
                        student_row.get("seccion_norm") or student_row.get("seccion") or ""
                    ).strip() if student_row else "",
                    "Clase encontrada": str(class_row.get("clase_nombre") or "").strip() if class_row else "",
                    "Clase ID": _safe_int(class_row.get("clase_id")) if class_row else "",
                    "Nivel clase": str(class_row.get("nivel_nombre") or "").strip() if class_row else "",
                    "Grado clase": str(class_row.get("grado_nombre") or "").strip() if class_row else "",
                    "Seccion clase": str(class_row.get("seccion") or "").strip() if class_row else "",
                    "Estado": estado,
                    "Detalle": detalle,
                    "_alumno_id": _safe_int(student_row.get("alumno_id")) if student_row else None,
                    "_auto_alumno_id": _safe_int(student_row.get("alumno_id")) if student_row else None,
                    "_clase_id": _safe_int(class_row.get("clase_id")) if class_row else None,
                    "_clase_nivel_id": _safe_int(class_row.get("nivel_id")) if class_row else None,
                    "_clase_grado_id": _safe_int(class_row.get("grado_id")) if class_row else None,
                    "_ingles_grade_key": str(class_row.get("ingles_grade_key") or "").strip() if class_row else "",
                    "_nivel_id": _safe_int(student_row.get("nivel_id")) if student_row else None,
                    "_grado_id": _safe_int(student_row.get("grado_id")) if student_row else None,
                    "_grupo_id": _safe_int(student_row.get("grupo_id")) if student_row else None,
                    "_activo": bool(_to_bool(student_row.get("activo"))) if student_row else False,
                    "_student_match_count": int(len(matched_students)),
                    "_student_match_mode": student_match_mode,
                    "_class_match_count": int(len(matched_classes)),
                },
                student_row if student_row else None,
            )
        )

    preview_rows.sort(
        key=lambda row: (
            int(_safe_int(row.get("Fila")) or 0),
            str(row.get("Apellido Paterno") or "").upper(),
            str(row.get("Apellido Materno") or "").upper(),
            str(row.get("Nombre") or "").upper(),
        )
    )
    return preview_rows


def _build_ingles_assignment_preview_display_rows(
    rows: List[Dict[str, object]]
) -> List[Dict[str, object]]:
    return [
        {
            "Fila": row.get("Fila", ""),
            "Alumno Excel": _build_ingles_assignment_excel_full_name(row),
            "Clase solicitada": row.get("Clase solicitada", ""),
            "Alumno encontrado": row.get("Alumno encontrado", ""),
            "Clase encontrada": row.get("Clase encontrada", ""),
            "Grado clase": row.get("Grado clase", ""),
            "Seccion clase": row.get("Seccion clase", ""),
            "Estado": row.get("Estado", ""),
            "Detalle": row.get("Detalle", ""),
        }
        for row in rows
        if isinstance(row, dict)
    ]


def _build_ingles_assignment_review_rows_from_selection(
    preview_rows: List[Dict[str, object]],
    students: List[Dict[str, object]],
    selected_student_ids_by_fila: Dict[int, Optional[int]],
) -> List[Dict[str, object]]:
    students_by_id = _build_ingles_assignment_students_by_id(students)
    selected_filas_by_alumno: Dict[int, List[int]] = {}
    selected_rows: List[Tuple[Dict[str, object], Optional[Dict[str, object]]]] = []

    for row in preview_rows:
        if not isinstance(row, dict):
            continue
        fila = int(_safe_int(row.get("Fila")) or 0)
        selected_student_id = _safe_int(selected_student_ids_by_fila.get(fila))
        student_row = (
            students_by_id.get(int(selected_student_id))
            if selected_student_id is not None
            else None
        )
        selected_rows.append((row, student_row))
        if fila > 0 and selected_student_id is not None:
            selected_filas_by_alumno.setdefault(int(selected_student_id), []).append(fila)

    duplicate_map: Dict[int, List[int]] = {}
    for filas in selected_filas_by_alumno.values():
        if len(filas) <= 1:
            continue
        for fila in filas:
            duplicate_map[int(fila)] = sorted(
                other_fila for other_fila in filas if int(other_fila) != int(fila)
            )

    reviewed_rows: List[Dict[str, object]] = []
    for row, student_row in selected_rows:
        fila = int(_safe_int(row.get("Fila")) or 0)
        reviewed_rows.append(
            _hydrate_ingles_assignment_preview_row(
                row=row,
                student_row=student_row,
                duplicate_filas=duplicate_map.get(fila) or [],
            )
        )
    return reviewed_rows


def _render_ingles_assignment_reference_review(
    preview_rows: List[Dict[str, object]],
    students: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    if not preview_rows:
        return []
    students_by_id = _build_ingles_assignment_students_by_id(students)
    students_full_name_lookup = _build_ingles_assignment_students_full_name_lookup(students)
    if not students_by_id:
        st.warning(
            "No hay catalogo de alumnos para revisar referencias. "
            "Vuelve a buscar alumnos y clases."
        )
        return preview_rows

    option_values: List[str] = [""]
    option_labels: Dict[str, str] = {"": "Selecciona un alumno"}
    sorted_students = sorted(
        students_by_id.items(),
        key=lambda item: (
            _build_ingles_assignment_reference_option_label(item[1]).upper(),
            int(item[0]),
        ),
    )
    for alumno_id, row in sorted_students:
        option_key = str(int(alumno_id))
        option_values.append(option_key)
        option_labels[option_key] = _build_ingles_assignment_reference_option_label(row)

    st.markdown(
        """
        <style>
        div[data-testid="stSelectbox"] div[data-baseweb="select"] > div {
            min-height: 2.15rem;
            padding-top: 0;
            padding-bottom: 0;
        }
        .ingles-ref-name {
            font-size: 0.95rem;
            font-weight: 600;
            line-height: 1.2;
            margin-top: 0.2rem;
            margin-bottom: 0.35rem;
            overflow-wrap: anywhere;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    selected_student_ids_by_fila: Dict[int, Optional[int]] = {}
    for row_start in range(0, len(preview_rows), 2):
        row_pair = [
            row
            for row in preview_rows[row_start : row_start + 2]
            if isinstance(row, dict)
        ]
        if not row_pair:
            continue
        review_cols = st.columns(len(row_pair), gap="small")
        for col_idx, row in enumerate(row_pair):
            fila = int(_safe_int(row.get("Fila")) or 0)
            row_key = f"clases_auto_group_ingles_ref_select_{fila}"
            default_option = _build_ingles_assignment_default_reference_option(
                row=row,
                students_full_name_lookup=students_full_name_lookup,
            )
            current_option = str(st.session_state.get(row_key) or "").strip()
            if current_option not in option_values or (
                not current_option and default_option in option_values and default_option
            ):
                current_option = default_option if default_option in option_values else ""
                st.session_state[row_key] = current_option
            selected_index = (
                option_values.index(current_option) if current_option in option_values else 0
            )

            with review_cols[col_idx]:
                st.markdown(
                    (
                        f"<div class='ingles-ref-name'>"
                        f"{escape(_build_ingles_assignment_excel_full_name(row))}"
                        f"</div>"
                    ),
                    unsafe_allow_html=True,
                )
                selected_option = st.selectbox(
                    f"Referencia del alumno fila {fila}",
                    options=option_values,
                    index=int(selected_index),
                    key=row_key,
                    format_func=lambda value: option_labels.get(str(value), str(value)),
                    label_visibility="collapsed",
                )
                selected_student_ids_by_fila[fila] = _safe_int(selected_option)

    reviewed_rows = _build_ingles_assignment_review_rows_from_selection(
        preview_rows=preview_rows,
        students=students,
        selected_student_ids_by_fila=selected_student_ids_by_fila,
    )
    duplicate_rows = [
        row
        for row in reviewed_rows
        if str(row.get("Estado") or "").strip() == "Revisar"
        and "referenciado" in str(row.get("Detalle") or "").lower()
    ]
    if duplicate_rows:
        st.warning(
            "Hay referencias duplicadas en {total} fila(s). "
            "Corrigelas antes de aplicar.".format(total=len(duplicate_rows))
        )
    return reviewed_rows


def _apply_ingles_assignment_preview_rows(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    preview_rows: List[Dict[str, object]],
    on_status: Optional[Callable[[str], None]] = None,
) -> List[Dict[str, object]]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message or ""))
            except Exception:
                pass

    actionable_rows: List[Tuple[Dict[str, object], int, int]] = []
    target_members_by_class: Dict[int, Set[int]] = {}
    class_ids: List[int] = []
    seen_class_ids: Set[int] = set()

    for row in preview_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("Estado") or "").strip() != "Listo":
            continue
        alumno_id = _safe_int(row.get("_alumno_id"))
        clase_id = _safe_int(row.get("_clase_id"))
        if alumno_id is None or clase_id is None:
            continue
        actionable_rows.append((row, int(alumno_id), int(clase_id)))
        target_members_by_class.setdefault(int(clase_id), set()).add(int(alumno_id))
        if int(clase_id) not in seen_class_ids:
            seen_class_ids.add(int(clase_id))
            class_ids.append(int(clase_id))

    members_by_class: Dict[int, Set[int]] = {}
    class_errors: Dict[int, str] = {}
    class_sync_notes: Dict[int, Dict[str, object]] = {}
    total_classes = len(class_ids)
    for idx_class, clase_id in enumerate(class_ids, start=1):
        _status(f"Validando clase {idx_class}/{total_classes}: {clase_id}")
        try:
            class_data = _fetch_alumnos_clase_gestion_escolar(
                token=token,
                clase_id=int(clase_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
            )
        except Exception as exc:
            class_errors[int(clase_id)] = str(exc)
        else:
            members_by_class[int(clase_id)] = _extract_alumno_ids_from_clase_data(
                class_data
            )

    synced_classes = 0
    for clase_id in class_ids:
        if int(clase_id) in class_errors:
            continue
        synced_classes += 1
        current_members = members_by_class.setdefault(int(clase_id), set())
        target_members = target_members_by_class.get(int(clase_id), set())
        to_remove = sorted(current_members - target_members)
        remove_errors: List[str] = []
        if to_remove:
            _status(
                "Retirando alumnos fuera del Excel {idx}/{total}: clase {clase}".format(
                    idx=synced_classes,
                    total=max(total_classes, 1),
                    clase=int(clase_id),
                )
            )
        removed_ok = 0
        for alumno_id in to_remove:
            try:
                _delete_alumno_clase_gestion_escolar(
                    token=token,
                    clase_id=int(clase_id),
                    alumno_id=int(alumno_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
                current_members.discard(int(alumno_id))
                removed_ok += 1
            except Exception as exc:
                remove_errors.append(f"{int(alumno_id)}: {exc}")
        class_sync_notes[int(clase_id)] = {
            "removed_ok": int(removed_ok),
            "removed_error": len(remove_errors),
            "remove_errors": list(remove_errors),
        }

    results: List[Dict[str, object]] = []
    total_actions = len(actionable_rows)
    executed_actions = 0
    class_note_attached: Set[int] = set()
    for row in preview_rows:
        result_row = dict(row) if isinstance(row, dict) else {}
        alumno_id = _safe_int(result_row.get("_alumno_id"))
        clase_id = _safe_int(result_row.get("_clase_id"))
        nivel_id = _safe_int(result_row.get("_nivel_id"))
        grado_id = _safe_int(result_row.get("_grado_id"))
        grupo_id = _safe_int(result_row.get("_grupo_id"))
        alumno_activo = bool(result_row.get("_activo"))
        estado = str(result_row.get("Estado") or "").strip()
        class_sync_note = (
            class_sync_notes.get(int(clase_id), {})
            if clase_id is not None
            else {}
        )
        result_row["_class_removed_ok"] = 0
        result_row["_class_removed_error"] = 0
        if clase_id is not None:
            result_row["_class_removed_ok"] = int(
                _safe_int(class_sync_note.get("removed_ok")) or 0
            )
            result_row["_class_removed_error"] = int(
                _safe_int(class_sync_note.get("removed_error")) or 0
            )
        result_row["_class_cleanup_counted"] = False
        class_note_suffix = ""
        if (
            estado == "Listo"
            and clase_id is not None
            and int(clase_id) not in class_note_attached
        ):
            removed_ok = int(_safe_int(class_sync_note.get("removed_ok")) or 0)
            removed_error = int(_safe_int(class_sync_note.get("removed_error")) or 0)
            result_row["_class_cleanup_counted"] = True
            if removed_ok or removed_error:
                note_parts: List[str] = []
                if removed_ok:
                    note_parts.append(f"retirados del Excel={removed_ok}")
                if removed_error:
                    note_parts.append(f"error al retirar={removed_error}")
                class_note_suffix = " Clase sincronizada: " + " | ".join(note_parts) + "."
            class_note_attached.add(int(clase_id))

        if estado != "Listo":
            result_row["Resultado aplicar"] = "SKIP"
            result_row["Detalle aplicar"] = str(result_row.get("Detalle") or "").strip()
            results.append(result_row)
            continue

        if alumno_id is None or clase_id is None:
            result_row["Resultado aplicar"] = "Error"
            result_row["Detalle aplicar"] = "Fila valida sin alumno_id o clase_id."
            results.append(result_row)
            continue

        if int(clase_id) in class_errors:
            result_row["Resultado aplicar"] = "Error"
            result_row["Detalle aplicar"] = (
                "No se pudo validar la clase actual: "
                f"{class_errors[int(clase_id)]}"
            ) + class_note_suffix
            results.append(result_row)
            continue

        activation_prefix = ""
        if not alumno_activo:
            if nivel_id is None or grado_id is None or grupo_id is None:
                result_row["Resultado aplicar"] = "Error"
                result_row["Detalle aplicar"] = (
                    "No se pudo activar al alumno: falta nivel_id, grado_id o grupo_id."
                )
                results.append(result_row)
                continue
            _status(
                "Activando alumno antes de asignar: {alumno}".format(
                    alumno=str(
                        result_row.get("Alumno encontrado")
                        or result_row.get("Nombre")
                        or alumno_id
                    ).strip()
                )
            )
            activation_ok, activation_msg = _set_alumno_activo_web(
                token=token,
                colegio_id=int(colegio_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                nivel_id=int(nivel_id),
                grado_id=int(grado_id),
                grupo_id=int(grupo_id),
                alumno_id=int(alumno_id),
                activo=1,
                observaciones="Activado automaticamente antes de asignacion de ingles por niveles.",
                timeout=int(timeout),
            )
            if not activation_ok:
                result_row["Resultado aplicar"] = "Error"
                result_row["Detalle aplicar"] = (
                    "No se pudo activar al alumno antes de asignar: {msg}".format(
                        msg=str(activation_msg or "sin detalle").strip()
                    )
                ) + class_note_suffix
                results.append(result_row)
                continue
            result_row["_activo"] = True
            result_row["Activo"] = "Si"
            activation_prefix = "Alumno activado. "

        class_members = members_by_class.setdefault(int(clase_id), set())
        if int(alumno_id) in class_members:
            result_row["Resultado aplicar"] = "Sin cambios"
            result_row["Detalle aplicar"] = (
                f"{activation_prefix}El alumno ya estaba asignado a la clase."
            ).strip() + class_note_suffix
            results.append(result_row)
            continue

        executed_actions += 1
        _status(
            "Asignando {idx}/{total}: {alumno} -> {clase}".format(
                idx=executed_actions,
                total=max(total_actions, 1),
                alumno=str(result_row.get("Alumno encontrado") or result_row.get("Nombre") or "-"),
                clase=str(result_row.get("Clase encontrada") or result_row.get("Clase solicitada") or "-"),
            )
        )
        ok_assign, msg_assign = _asignar_alumno_a_clase_web(
            token=token,
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            clase_id=int(clase_id),
            alumno_id=int(alumno_id),
            timeout=int(timeout),
        )
        if ok_assign:
            class_members.add(int(alumno_id))
            result_row["Resultado aplicar"] = "OK"
            result_row["Detalle aplicar"] = (
                f"{activation_prefix}Asignado correctamente."
            ).strip() + class_note_suffix
        else:
            result_row["Resultado aplicar"] = "Error"
            result_row["Detalle aplicar"] = (
                f"{activation_prefix}{str(msg_assign or 'No se pudo asignar.').strip()}"
            ).strip() + class_note_suffix
        results.append(result_row)

    return results


def _apply_ingles_assignment_for_selected_grades(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    preview_rows: List[Dict[str, object]],
    selected_ingles_grade_keys: Sequence[object],
    status_placeholder: object = None,
) -> Tuple[bool, str]:
    rows_to_apply = _filter_ingles_assignment_rows_by_selected_ingles_grades(
        preview_rows,
        selected_ingles_grade_keys,
        include_unresolved=False,
    )
    if not rows_to_apply:
        return (
            False,
            "No hay filas de Ingles por niveles para los grados seleccionados arriba.",
        )

    on_status = None
    if status_placeholder is not None:
        on_status = lambda message: status_placeholder.write(str(message or ""))

    try:
        with st.spinner("Aplicando asignacion de ingles..."):
            apply_rows = _apply_ingles_assignment_preview_rows(
                token=token,
                colegio_id=int(colegio_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
                preview_rows=rows_to_apply,
                on_status=on_status,
            )
    except Exception as exc:
        if status_placeholder is not None:
            try:
                status_placeholder.empty()
            except Exception:
                pass
        return False, f"No se pudo aplicar la asignacion: {exc}"

    if status_placeholder is not None:
        try:
            status_placeholder.empty()
        except Exception:
            pass

    st.session_state["clases_auto_group_ingles_excel_apply_rows"] = apply_rows
    ok_count = sum(
        1
        for row in apply_rows
        if str(row.get("Resultado aplicar") or "").strip() == "OK"
    )
    same_count = sum(
        1
        for row in apply_rows
        if str(row.get("Resultado aplicar") or "").strip() == "Sin cambios"
    )
    err_count = sum(
        1
        for row in apply_rows
        if str(row.get("Resultado aplicar") or "").strip() == "Error"
    )
    removed_ok_count = sum(
        int(_safe_int(row.get("_class_removed_ok")) or 0)
        for row in apply_rows
        if bool(row.get("_class_cleanup_counted"))
    )
    removed_error_count = sum(
        int(_safe_int(row.get("_class_removed_error")) or 0)
        for row in apply_rows
        if bool(row.get("_class_cleanup_counted"))
    )
    if err_count:
        _set_ingles_por_niveles_result_notice(
            "warning",
            "Asignacion aplicada con observaciones. "
            f"OK={ok_count} | Sin cambios={same_count} | Retirados={removed_ok_count} | "
            f"Error retirar={removed_error_count} | Error={err_count}",
        )
    else:
        _set_ingles_por_niveles_result_notice(
            "success",
            "Asignacion aplicada. "
            f"OK={ok_count} | Sin cambios={same_count} | Retirados={removed_ok_count} | "
            f"Error retirar={removed_error_count}",
        )
    return True, ""


def _build_ingles_assignment_apply_display_rows(
    rows: List[Dict[str, object]]
) -> List[Dict[str, object]]:
    return [
        {
            "Fila": row.get("Fila", ""),
            "Alumno Excel": _build_ingles_assignment_excel_full_name(row),
            "Alumno encontrado": row.get("Alumno encontrado", ""),
            "Clase encontrada": row.get("Clase encontrada", ""),
            "Grado clase": row.get("Grado clase", ""),
            "Resultado aplicar": row.get("Resultado aplicar", ""),
            "Detalle aplicar": row.get("Detalle aplicar", ""),
        }
        for row in rows
        if isinstance(row, dict)
    ]


def _render_ingles_por_niveles_excel_assignment_block(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    selected_ingles_grade_keys: Optional[Sequence[object]] = None,
) -> None:
    with st.container(border=True):
        st.markdown("**Asignacion de ingles por niveles**")
        st.caption("Excel: Nombre, Apellido Paterno, Apellido Materno y Clase.")

        template_bytes = _export_simple_excel(
            _ingles_assignment_template_rows(),
            sheet_name="ingles_por_niveles",
        )
        col_template, col_file = st.columns([1, 2], gap="small")
        col_template.download_button(
            label="Descargar plantilla",
            data=template_bytes,
            file_name="plantilla_ingles_por_niveles.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="clases_auto_group_ingles_excel_template_download",
            use_container_width=True,
        )
        uploaded_excel = col_file.file_uploader(
            "Excel de asignacion de ingles",
            type=["xlsx"],
            key="clases_auto_group_ingles_excel_uploader",
            help=(
                "Columnas requeridas: Nombre, Apellido Paterno, Apellido Materno, Clase."
            ),
        )

        uploaded_rows: List[Dict[str, object]] = []
        uploaded_error = ""
        uploaded_bytes = b""
        if uploaded_excel is not None:
            uploaded_bytes = uploaded_excel.getvalue()
            try:
                uploaded_rows = _load_ingles_assignment_rows_from_excel(uploaded_bytes)
            except Exception as exc:
                uploaded_error = str(exc)
                st.error(f"Error en Excel: {exc}")
            else:
                st.caption(f"Filas detectadas en el Excel: {len(uploaded_rows)}")
                _show_dataframe(
                    [
                        {
                            key: value
                            for key, value in row.items()
                            if str(key) != "_row_number"
                        }
                        for row in uploaded_rows[:100]
                        if isinstance(row, dict)
                    ],
                    use_container_width=True,
                )

        preview_rows_state = st.session_state.get(
            "clases_auto_group_ingles_excel_preview_rows"
        ) or []
        reference_students_state = st.session_state.get(
            "clases_auto_group_ingles_excel_reference_students"
        ) or []
        apply_rows_state = st.session_state.get(
            "clases_auto_group_ingles_excel_apply_rows"
        ) or []
        fetch_errors_state = st.session_state.get(
            "clases_auto_group_ingles_excel_fetch_errors"
        ) or []
        result_notice_state = st.session_state.get(
            "clases_auto_group_ingles_excel_result_notice"
        ) or {}

        action_col_analyze, action_col_apply = st.columns(2, gap="small")
        run_analyze = action_col_analyze.button(
            "Buscar alumnos y clases",
            key="clases_auto_group_ingles_excel_analyze_btn",
            use_container_width=True,
        )
        run_apply_only = action_col_apply.button(
            "Aplicar solo ingles",
            key="clases_auto_group_ingles_excel_apply_only_btn",
            use_container_width=True,
            disabled=not bool(preview_rows_state),
        )
        st.caption(
            "`Buscar alumnos y clases` prepara la vista previa. "
            "`Aplicar solo ingles` ejecuta solo el Excel de ingles. "
            "`Actualizar asignacion` aplica ingles y luego corre la sincronizacion masiva."
        )

        if run_analyze:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
            elif uploaded_excel is None:
                st.error("Sube el Excel de asignacion de ingles.")
            elif uploaded_error:
                st.error(f"Corrige el Excel antes de continuar: {uploaded_error}")
            else:
                analyze_progress = st.progress(0)
                analyze_status = st.empty()
                try:
                    analyze_progress.progress(5)
                    analyze_status.write("Cargando alumnos del colegio...")
                    catalog = _fetch_alumnos_catalog_for_manual_move(
                        token=token,
                        colegio_id=int(colegio_id),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                        on_status=lambda message: analyze_status.write(str(message or "")),
                    )
                    analyze_progress.progress(55)
                    analyze_status.write("Cargando clases del colegio...")
                    clases = _fetch_clases_gestion_escolar(
                        token=token,
                        colegio_id=int(colegio_id),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                        ordered=True,
                    )
                    analyze_progress.progress(75)
                    analyze_status.write("Comparando Excel contra alumnos y clases...")
                    preview_rows = _build_ingles_assignment_preview_rows(
                        uploaded_rows,
                        list(catalog.get("students") or []),
                        clases,
                    )
                    analyze_progress.progress(90)
                    analyze_status.write(
                        "Precargando referencias y ordenando filas para edicion..."
                    )
                    preview_rows = _prepare_ingles_assignment_review_rows(
                        preview_rows=preview_rows,
                        students=list(catalog.get("students") or []),
                    )
                except Exception as exc:
                    analyze_progress.empty()
                    analyze_status.empty()
                    st.error(f"No se pudo analizar la asignacion: {exc}")
                else:
                    analyze_progress.progress(100)
                    analyze_status.write("Analisis completo.")
                    _clear_ingles_por_niveles_assignment_state()
                    st.session_state[
                        "clases_auto_group_ingles_excel_preview_rows"
                    ] = preview_rows
                    st.session_state[
                        "clases_auto_group_ingles_excel_reference_students"
                    ] = list(catalog.get("students") or [])
                    st.session_state[
                        "clases_auto_group_ingles_excel_fetch_errors"
                    ] = list(catalog.get("errors") or [])
                    total_ready = sum(
                        1
                        for row in preview_rows
                        if str(row.get("Estado") or "").strip() == "Listo"
                    )
                    total_errors = len(preview_rows) - total_ready
                    st.session_state[
                        "clases_auto_group_ingles_excel_result_notice"
                    ] = {
                        "kind": "warning" if total_errors else "success",
                        "message": (
                            f"Analisis listo. Filas validas: {total_ready} | Observaciones: {total_errors}."
                            if total_errors
                            else f"Analisis listo. Filas validas: {total_ready}."
                        ),
                    }
                    st.rerun()
        if run_apply_only:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
            elif not selected_ingles_grade_keys:
                st.error("Selecciona uno o mas grados de ingles arriba.")
            elif not preview_rows_state:
                st.error("Primero pulsa `Buscar alumnos y clases`.")
            else:
                apply_status_placeholder = st.empty()
                apply_ok, apply_error = _apply_ingles_assignment_for_selected_grades(
                    token=token,
                    colegio_id=int(colegio_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                    preview_rows=list(preview_rows_state),
                    selected_ingles_grade_keys=selected_ingles_grade_keys,
                    status_placeholder=apply_status_placeholder,
                )
                if apply_ok:
                    st.rerun()
                else:
                    st.error(apply_error)
        if isinstance(result_notice_state, dict) and result_notice_state.get("message"):
            notice_kind = str(result_notice_state.get("kind") or "success").strip().lower()
            notice_message = str(result_notice_state.get("message") or "").strip()
            if notice_kind == "warning":
                st.warning(notice_message)
            elif notice_kind == "error":
                st.error(notice_message)
            else:
                st.success(notice_message)
            st.session_state.pop("clases_auto_group_ingles_excel_result_notice", None)

        if fetch_errors_state:
            st.warning(
                "Hubo observaciones al cargar alumnos del colegio. "
                f"Se registraron {len(fetch_errors_state)} error(es) de consulta."
            )

        if preview_rows_state:
            visible_preview_rows = _filter_ingles_assignment_rows_by_selected_ingles_grades(
                preview_rows_state,
                selected_ingles_grade_keys,
            )
            if not selected_ingles_grade_keys:
                st.info("Selecciona uno o mas grados de ingles arriba.")
            reviewed_preview_rows = _render_ingles_assignment_reference_review(
                preview_rows=visible_preview_rows,
                students=reference_students_state,
            )
            reviewed_preview_rows = _sort_ingles_assignment_review_rows(
                reviewed_preview_rows
            )
            merged_preview_rows = _sort_ingles_assignment_review_rows(
                _merge_ingles_assignment_rows(
                    preview_rows_state,
                    reviewed_preview_rows,
                )
            )
            if merged_preview_rows != preview_rows_state:
                st.session_state[
                    "clases_auto_group_ingles_excel_preview_rows"
                ] = merged_preview_rows
                st.session_state.pop(
                    "clases_auto_group_ingles_excel_apply_rows",
                    None,
                )
                preview_rows_state = merged_preview_rows
                apply_rows_state = []
            visible_preview_rows = _filter_ingles_assignment_rows_by_selected_ingles_grades(
                preview_rows_state,
                selected_ingles_grade_keys,
            )
            total_ready = sum(
                1
                for row in visible_preview_rows
                if str(row.get("Estado") or "").strip() == "Listo"
            )
            total_errors = len(visible_preview_rows) - total_ready
            unresolved_grade_rows = sum(
                1
                for row in visible_preview_rows
                if not str(row.get("_ingles_grade_key") or "").strip()
            )
            st.caption(
                f"Filas: {len(visible_preview_rows)}/{len(preview_rows_state)} | "
                f"Listas {total_ready} | Obs {total_errors}"
            )
            if unresolved_grade_rows:
                st.info(
                    "Se muestran tambien filas sin grado/clase resuelto para que "
                    "puedas revisar errores de coincidencia."
                )
            if visible_preview_rows:
                _show_dataframe(
                    _build_ingles_assignment_preview_display_rows(visible_preview_rows),
                    use_container_width=True,
                )
            else:
                st.info("No hay filas para los grados seleccionados.")

        if apply_rows_state:
            st.caption(f"Resultado de aplicacion: {len(apply_rows_state)} fila(s)")
            _show_dataframe(
                _build_ingles_assignment_apply_display_rows(apply_rows_state),
                use_container_width=True,
            )


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

    pagados_y = _filter_users_payments_paid_students(
        students=alumnos_all,
        only_origin_section=True,
        allowed_grade_keys=grade_keys_with_y,
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
                exclude_santillana_inclusiva=True,
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

    alumnos_grid = _build_users_payments_students_grid(alumnos_all)

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
        "paid_students_grid": _build_users_payments_students_grid(
            _filter_users_payments_paid_students(students=alumnos_all)
        ),
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


def _profesor_edit_estado_activo(estado: object) -> bool:
    if isinstance(estado, bool):
        return bool(estado)
    estado_txt = _normalize_compare_text(estado)
    return estado_txt in {"ACTIVO", "ACTIVA", "1", "SI", "TRUE", "YES"}


def _profesor_edit_estado_dot_html(activo: bool) -> str:
    color = "#138a52" if activo else "#c62828"
    return (
        "<div style='display:flex;align-items:center;height:1.55rem;margin:0;'>"
        f"<span style='display:inline-block;width:0.68rem;height:0.68rem;border-radius:999px;background:{color};'></span>"
        "</div>"
    )


def _profesor_class_level_palette(
    nivel_id: object = None,
    nivel_text: object = None,
) -> Dict[str, str]:
    nivel_id_int = _safe_int(nivel_id)
    nivel_norm = _normalize_plain_text(nivel_text)
    if nivel_id_int == 38 or "INICIAL" in nivel_norm:
        return {
            "bg": "#FEF3C7",
            "border": "#F59E0B",
            "text": "#92400E",
        }
    if nivel_id_int == 39 or "PRIMARIA" in nivel_norm:
        return {
            "bg": "#DCFCE7",
            "border": "#22C55E",
            "text": "#166534",
        }
    if nivel_id_int == 40 or "SECUNDARIA" in nivel_norm:
        return {
            "bg": "#DBEAFE",
            "border": "#3B82F6",
            "text": "#1D4ED8",
        }
    return {
        "bg": "#F3F4F6",
        "border": "#9CA3AF",
        "text": "#374151",
    }


def _build_profesor_class_chip_html(
    label: object,
    nivel_id: object = None,
    nivel_text: object = None,
) -> str:
    palette = _profesor_class_level_palette(
        nivel_id=nivel_id,
        nivel_text=nivel_text,
    )
    return (
        "<span style='display:inline-flex;align-items:center;"
        "padding:0.26rem 0.62rem;margin:0 0.38rem 0.38rem 0;"
        "border-radius:999px;font-size:0.84rem;font-weight:600;"
        f"background:{palette['bg']};border:1px solid {palette['border']};"
        f"color:{palette['text']};'>{escape(str(label or '').strip())}</span>"
    )


def _render_profesor_class_chips_html(
    class_ids: Sequence[object],
    clases_by_id: Dict[int, Dict[str, object]],
    empty_text: str = "Sin clases",
) -> str:
    chips: List[str] = []
    seen: Set[int] = set()
    for raw_clase_id in class_ids or []:
        clase_id = _safe_int(raw_clase_id)
        if clase_id is None or int(clase_id) in seen:
            continue
        seen.add(int(clase_id))
        row = clases_by_id.get(int(clase_id)) or {}
        label = str(
            row.get("clase_label")
            or row.get("clase_nombre")
            or row.get("clase")
            or f"Clase {int(clase_id)}"
        ).strip()
        chips.append(
            _build_profesor_class_chip_html(
                label=label,
                nivel_id=row.get("nivel_id"),
                nivel_text=row.get("nivel"),
            )
        )
    if not chips:
        return (
            "<div style='color:#6B7280;font-size:0.9rem;'>"
            f"{escape(empty_text)}"
            "</div>"
        )
    return "<div style='display:flex;flex-wrap:wrap;align-items:flex-start;'>" + "".join(chips) + "</div>"


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


def _update_profesor_edit_estado_web(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    persona_id: int,
    nivel_ids: Sequence[int],
    activo: bool,
    timeout: int,
) -> Tuple[Dict[str, int], List[Dict[str, object]]]:
    unique_nivel_ids: List[int] = []
    seen_levels: Set[int] = set()
    for item in nivel_ids:
        nivel_id = _safe_int(item)
        if nivel_id is None or int(nivel_id) in seen_levels:
            continue
        unique_nivel_ids.append(int(nivel_id))
        seen_levels.add(int(nivel_id))

    summary = {
        "niveles_total": len(unique_nivel_ids),
        "niveles_actualizados": 0,
        "errores_api": 0,
    }
    errors: List[Dict[str, object]] = []
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    for nivel_id in unique_nivel_ids:
        url = CENSO_PROFESOR_ACTIVAR_INACTIVAR_URL.format(
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            colegio_id=int(colegio_id),
            nivel_id=int(nivel_id),
            persona_id=int(persona_id),
        )
        payload = {"activo": 1 if activo else 0}
        try:
            response = requests.put(
                url, headers=headers, json=payload, timeout=int(timeout)
            )
        except requests.RequestException as exc:
            summary["errores_api"] += 1
            errors.append(
                {
                    "nivel_id": int(nivel_id),
                    "error": f"Error de red: {exc}",
                }
            )
            continue

        status_code = response.status_code
        try:
            body = response.json() if response.content else {}
        except ValueError:
            body = {}

        if not response.ok:
            message = (
                str(body.get("message") or "").strip()
                if isinstance(body, dict)
                else ""
            )
            summary["errores_api"] += 1
            errors.append(
                {
                    "nivel_id": int(nivel_id),
                    "error": message or f"HTTP {status_code}",
                }
            )
            continue

        if isinstance(body, dict) and body.get("success", True) is False:
            summary["errores_api"] += 1
            errors.append(
                {
                    "nivel_id": int(nivel_id),
                    "error": str(body.get("message") or "Respuesta invalida").strip(),
                }
            )
            continue

        summary["niveles_actualizados"] += 1

    return summary, errors


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


def _assign_alumno_to_matching_classes_for_context(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    alumno_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    seccion: str,
    on_status: Optional[Callable[[str], None]] = None,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, object]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message or ""))
            except Exception:
                pass

    def _progress(current: int, total: int, message: str) -> None:
        if callable(on_progress):
            try:
                on_progress(int(current), int(total), str(message or ""))
            except Exception:
                pass

    _status("Listando clases del colegio...")
    _progress(0, 1, "Listando clases del colegio...")
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

    target_classes = _build_clases_destino_for_plan(
        clases_rows=clases_unicas,
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_destino_id=int(grupo_id),
        seccion_destino=str(seccion or ""),
        exclude_santillana_inclusiva=True,
    )
    target_total = len(target_classes)
    _status(f"Clases destino detectadas: {target_total}.")
    if target_total == 0:
        _progress(1, 1, "Sin clases destino para el grado/seccion.")
        return {
            "target_classes_total": 0,
            "assigned_ok": 0,
            "assigned_error": 0,
            "assigned_errors": [],
        }

    assign_ok = 0
    assign_err = 0
    assign_errors: List[str] = []
    for idx, clase in enumerate(target_classes, start=1):
        clase_id = _safe_int(clase.get("clase_id")) if isinstance(clase, dict) else None
        if clase_id is None:
            continue
        status_message = "Asignando clases {idx}/{total}...".format(
            idx=idx,
            total=target_total,
        )
        _status(status_message)
        _progress(idx, target_total, status_message)
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

    return {
        "target_classes_total": int(target_total),
        "assigned_ok": int(assign_ok),
        "assigned_error": int(assign_err),
        "assigned_errors": assign_errors,
    }


def _apply_auto_move_changes(
    token: str,
    colegio_id: Optional[int],
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    plan_rows: List[Dict[str, object]],
    on_status: Optional[Callable[[str], None]] = None,
    on_progress: Optional[Callable[[Dict[str, object]], None]] = None,
) -> Tuple[Dict[str, int], List[Dict[str, object]]]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message or ""))
            except Exception:
                pass

    def _progress(
        processed: int,
        total: int,
        current_student: str = "",
        current_colegio_id: Optional[int] = None,
        current_status: str = "",
    ) -> None:
        if callable(on_progress):
            try:
                on_progress(
                    {
                        "processed": int(processed),
                        "total": int(total),
                        "current_student": str(current_student or "").strip(),
                        "current_colegio_id": (
                            int(current_colegio_id)
                            if current_colegio_id is not None
                            else 0
                        ),
                        "current_status": str(current_status or "").strip(),
                        "summary": dict(summary),
                    }
                )
            except Exception:
                pass

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
    total_rows = int(len(plan_rows))
    _progress(0, total_rows, current_status="Iniciando guardado")
    for idx_plan, plan in enumerate(plan_rows, start=1):
        pagado = plan.get("alumno_pagado") if isinstance(plan.get("alumno_pagado"), dict) else {}
        inactivar = plan.get("alumno_inactivar") if isinstance(plan.get("alumno_inactivar"), dict) else {}
        alumno_pagado_id = _safe_int(pagado.get("alumno_id"))
        plan_colegio_id = _safe_int(plan.get("colegio_id"))
        if plan_colegio_id is None:
            plan_colegio_id = _safe_int(colegio_id)
        label_pagado = _format_alumno_label(pagado)
        _status(
            "Alumno {idx}/{total}: {alumno}".format(
                idx=idx_plan,
                total=max(total_rows, 1),
                alumno=label_pagado or "-",
            )
        )
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
            _progress(
                idx_plan,
                total_rows,
                current_student=label_pagado,
                current_colegio_id=plan_colegio_id,
                current_status="Sin colegio destino",
            )
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
        _progress(
            idx_plan,
            total_rows,
            current_student=label_pagado,
            current_colegio_id=plan_colegio_id,
            current_status=(
                "Inactivar={inactivar} | Mover={mover} | Asignar={asignar}".format(
                    inactivar=str(result_row.get("Inactivar no pagado") or ""),
                    mover=str(result_row.get("Mover") or ""),
                    asignar=str(result_row.get("Asignar clases") or ""),
                )
            ),
        )

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


def _render_clases_participantes_section(
    colegio_id_raw: str,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> None:
    token = _get_shared_token()
    current_colegio_id = _safe_int(colegio_id_raw)
    cached_colegio_id = _safe_int(
        st.session_state.get("clases_participantes_colegio_id")
    )
    if (
        cached_colegio_id is not None
        and current_colegio_id is not None
        and int(cached_colegio_id) != int(current_colegio_id)
    ):
        for state_key in list(st.session_state.keys()):
            if str(state_key).startswith("clases_participantes_"):
                st.session_state.pop(state_key, None)

    def _refresh_detail(clase_id_value: int) -> Dict[str, object]:
        payload = _load_clase_participantes_detail(
            token=token,
            clase_id=int(clase_id_value),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
        st.session_state["clases_participantes_detail"] = payload
        st.session_state["clases_participantes_detail_clase_id"] = int(clase_id_value)
        return payload

    def _clear_action_widgets() -> None:
        for state_key in (
            "clases_participantes_remove_alumnos",
            "clases_participantes_confirm_remove_alumnos",
            "clases_participantes_add_alumnos",
            "clases_participantes_remove_profesores",
            "clases_participantes_confirm_remove_profesores",
            "clases_participantes_add_profesores",
        ):
            st.session_state.pop(state_key, None)

    def _load_class_rows_for_current_colegio(colegio_id_int: int) -> List[Dict[str, object]]:
        raw_clases = _fetch_clases_gestion_escolar(
            token=token,
            colegio_id=int(colegio_id_int),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
            ordered=True,
        )
        class_rows = [
            row
            for row in (
                _build_clase_participantes_row(item)
                for item in raw_clases
                if isinstance(item, dict)
            )
            if isinstance(row, dict) and _safe_int(row.get("clase_id")) is not None
        ]
        class_rows.sort(
            key=lambda row: (
                str(row.get("tipo") or "").upper(),
                _participantes_nivel_sort_rank(row.get("nivel_nombre")),
                _participantes_grado_sort_rank(row.get("grado_nombre")),
                _grupo_sort_key(
                    str(row.get("grupo_clave_actual") or ""),
                    str(row.get("grupo_clave_actual") or ""),
                ),
                str(row.get("clase") or "").upper(),
                int(row.get("clase_id") or 0),
            )
        )
        st.session_state["clases_participantes_class_rows"] = class_rows
        st.session_state["clases_participantes_colegio_id"] = int(colegio_id_int)
        st.session_state.pop("clases_participantes_detail", None)
        st.session_state.pop("clases_participantes_detail_clase_id", None)
        return class_rows

    def _load_catalogs_for_current_colegio(colegio_id_int: int) -> Tuple[List[Dict[str, object]], List[Dict[str, object]], List[str]]:
        alumnos_catalog = _fetch_alumnos_catalog_for_manual_move(
            token=token,
            colegio_id=int(colegio_id_int),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
        profesores_rows, _summary_prof, profesores_errors = listar_profesores_filters_data(
            token=token,
            colegio_id=int(colegio_id_int),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
        alumnos_rows = [
            row for row in (alumnos_catalog.get("students") or []) if isinstance(row, dict)
        ]
        profesores_catalog: List[Dict[str, object]] = []
        for row in profesores_rows:
            if not isinstance(row, dict):
                continue
            persona_id = _safe_int(row.get("persona_id"))
            if persona_id is None:
                continue
            nombre = " ".join(
                part
                for part in (
                    str(row.get("nombre") or "").strip(),
                    str(row.get("apellido_paterno") or "").strip(),
                    str(row.get("apellido_materno") or "").strip(),
                )
                if part
            ).strip()
            profesores_catalog.append(
                {
                    "persona_id": int(persona_id),
                    "nombre": nombre or f"Persona {int(persona_id)}",
                    "login": str(row.get("login") or "").strip(),
                    "dni": str(row.get("dni") or "").strip(),
                    "email": str(row.get("email") or "").strip(),
                    "estado": str(row.get("estado") or "").strip(),
                }
            )
        catalog_errors = list(alumnos_catalog.get("errors") or []) + [
            str(item)
            for item in (profesores_errors or [])
            if str(item or "").strip()
        ]
        st.session_state["clases_participantes_students_catalog"] = alumnos_rows
        st.session_state["clases_participantes_professors_catalog"] = profesores_catalog
        st.session_state["clases_participantes_catalog_errors"] = catalog_errors
        st.session_state["clases_participantes_colegio_id"] = int(colegio_id_int)
        return alumnos_rows, profesores_catalog, catalog_errors

    with st.container(border=True):
        st.markdown("**Participantes por clase**")
        st.caption(
            "Selecciona una clase del combo para ver y administrar alumnos y profesores."
        )
    if not token:
        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
        return
    try:
        colegio_id_int = _parse_colegio_id(colegio_id_raw)
    except ValueError as exc:
        st.error(f"Error: {exc}")
        return

    class_rows_state = [
        row
        for row in (st.session_state.get("clases_participantes_class_rows") or [])
        if isinstance(row, dict) and _safe_int(row.get("clase_id")) is not None
    ]
    if not class_rows_state:
        try:
            with st.spinner("Cargando clases del colegio..."):
                class_rows_state = _load_class_rows_for_current_colegio(colegio_id_int)
        except Exception as exc:
            st.error(f"No se pudieron cargar clases: {exc}")
            return

    if not class_rows_state:
        st.info("No se encontraron clases para este colegio.")
        return

    class_by_id = {int(row["clase_id"]): row for row in class_rows_state}
    class_options = [int(row["clase_id"]) for row in class_rows_state]

    selected_class_key = "clases_participantes_selected_class_id"
    selected_class_cached = _safe_int(st.session_state.get(selected_class_key))
    if selected_class_cached is not None and int(selected_class_cached) not in class_options:
        st.session_state.pop(selected_class_key, None)
    selected_class_id = st.selectbox(
        "Clase",
        options=class_options,
        index=None,
        key=selected_class_key,
        format_func=lambda value: str(
            (class_by_id.get(int(value)) or {}).get("label") or f"Clase {value}"
        ),
    )
    if selected_class_id is None:
        st.caption("Selecciona una clase para ver sus participantes.")
        return

    selected_class_id_int = int(selected_class_id)
    selected_class_row = class_by_id.get(selected_class_id_int, {})
    detail_state = st.session_state.get("clases_participantes_detail")
    detail_class_id = _safe_int(
        st.session_state.get("clases_participantes_detail_clase_id")
    )
    if not isinstance(detail_state, dict) or detail_class_id != selected_class_id_int:
        _clear_action_widgets()
        with st.spinner("Cargando alumnos y profesores de la clase..."):
            detail_payload = _refresh_detail(selected_class_id_int)
        detail_state = detail_payload
        detail_class_id = selected_class_id_int

    students_catalog = [
        row
        for row in (st.session_state.get("clases_participantes_students_catalog") or [])
        if isinstance(row, dict) and _safe_int(row.get("alumno_id")) is not None
    ]
    professors_catalog = [
        row
        for row in (st.session_state.get("clases_participantes_professors_catalog") or [])
        if isinstance(row, dict) and _safe_int(row.get("persona_id")) is not None
    ]
    if not students_catalog and not professors_catalog:
        try:
            with st.spinner("Cargando catalogos del colegio..."):
                (
                    students_catalog,
                    professors_catalog,
                    _catalog_errors_loaded,
                ) = _load_catalogs_for_current_colegio(colegio_id_int)
        except Exception as exc:
            st.session_state["clases_participantes_catalog_errors"] = [str(exc)]
            students_catalog = []
            professors_catalog = []

    col_info, col_export = st.columns([2, 1], gap="small")
    col_info.caption(
        "Ordenado por tipo, nivel, grado y seccion. La seleccion carga participantes automaticamente."
    )

    alumnos_current = [
        row for row in (detail_state.get("alumnos") or []) if isinstance(row, dict)
    ]
    profesores_current = [
        row for row in (detail_state.get("profesores") or []) if isinstance(row, dict)
    ]
    detail_errors = [
        str(item).strip()
        for item in (detail_state.get("errors") or [])
        if str(item).strip()
    ]

    export_rows = [
        {
            "Tipo": "Alumno",
            "ID": row.get("alumno_id", ""),
            "Nombre": row.get("nombre_completo", ""),
            "Login": row.get("login", ""),
            "DNI": row.get("dni", ""),
        }
        for row in alumnos_current
    ] + [
        {
            "Tipo": "Profesor",
            "ID": row.get("persona_id", ""),
            "Nombre": row.get("nombre", ""),
            "Login": row.get("login", ""),
            "DNI": row.get("dni", ""),
        }
        for row in profesores_current
    ]
    if export_rows:
        col_export.download_button(
            "Descargar participantes",
            data=_export_simple_excel(export_rows, sheet_name="participantes"),
            file_name=f"participantes_clase_{selected_class_id_int}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="clases_participantes_download",
            use_container_width=True,
        )

    st.caption(
        "Clase: {label} | Alumnos: {alumnos} | Profesores: {profesores}".format(
            label=str(selected_class_row.get("label") or selected_class_id_int),
            alumnos=len(alumnos_current),
            profesores=len(profesores_current),
        )
    )
    if detail_errors:
        with st.expander(f"Errores de carga ({len(detail_errors)})", expanded=False):
            st.write("\n".join(f"- {item}" for item in detail_errors))

    notice = st.session_state.pop("clases_participantes_notice", None)
    if isinstance(notice, dict):
        notice_type = str(notice.get("type") or "info").strip().lower()
        message = str(notice.get("message") or "").strip()
        if message:
            if notice_type == "success":
                st.success(message)
            elif notice_type == "warning":
                st.warning(message)
            elif notice_type == "error":
                st.error(message)
            else:
                st.info(message)

    catalog_errors = [
        str(item).strip()
        for item in (st.session_state.get("clases_participantes_catalog_errors") or [])
        if str(item).strip()
    ]
    if catalog_errors:
        with st.expander(
            f"Observaciones de catalogos ({len(catalog_errors)})",
            expanded=False,
        ):
            st.write("\n".join(f"- {item}" for item in catalog_errors[:60]))
            pending = len(catalog_errors) - 60
            if pending > 0:
                st.caption(f"... y {pending} observaciones mas.")

    tab_alumnos, tab_profesores = st.tabs(["Alumnos", "Profesores"])

    with tab_alumnos:
        alumnos_by_id = {
            int(row["alumno_id"]): row
            for row in alumnos_current
            if _safe_int(row.get("alumno_id")) is not None
        }
        if alumnos_current:
            _show_dataframe(
                [
                    {
                        "Alumno ID": row.get("alumno_id", ""),
                        "Persona ID": row.get("persona_id", ""),
                        "Alumno": row.get("nombre_completo", ""),
                        "Login": row.get("login", ""),
                        "DNI": row.get("dni", ""),
                    }
                    for row in alumnos_current
                ],
                use_container_width=True,
            )
        else:
            st.caption("La clase no tiene alumnos cargados.")

        remove_alumno_ids = st.multiselect(
            "Alumnos a quitar",
            options=sorted(alumnos_by_id.keys()),
            key="clases_participantes_remove_alumnos",
            format_func=lambda value: _clase_person_label(
                alumnos_by_id.get(int(value), {}),
                "alumno_id",
            ),
            placeholder="Selecciona alumnos registrados en la clase",
        )
        confirm_remove_alumnos = st.checkbox(
            "Confirmo quitar los alumnos seleccionados de esta clase.",
            key="clases_participantes_confirm_remove_alumnos",
        )
        if st.button(
            "Quitar alumnos",
            key="clases_participantes_remove_alumnos_btn",
            disabled=not bool(remove_alumno_ids),
            use_container_width=True,
        ):
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
            if not confirm_remove_alumnos:
                st.error("Confirma la accion antes de quitar alumnos.")
                st.stop()
            ok_count = 0
            errors: List[str] = []
            for alumno_id in remove_alumno_ids:
                try:
                    _delete_alumno_clase_gestion_escolar(
                        token=token,
                        clase_id=selected_class_id_int,
                        alumno_id=int(alumno_id),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                    )
                    ok_count += 1
                except Exception as exc:
                    errors.append(f"Alumno {int(alumno_id)}: {exc}")
            _refresh_detail(selected_class_id_int)
            _clear_action_widgets()
            st.session_state["clases_participantes_notice"] = {
                "type": "warning" if errors else "success",
                "message": "Alumnos quitados: {ok}. Errores: {err}".format(
                    ok=ok_count,
                    err=len(errors),
                )
                + ((" | " + " | ".join(errors[:3])) if errors else ""),
            }
            st.rerun()

        st.divider()
        st.markdown("**Agregar alumnos**")
        if not students_catalog:
            st.caption("No se pudieron cargar los alumnos del colegio para esta vista.")
        else:
            current_alumno_ids = set(alumnos_by_id.keys())
            search_add_student = st.text_input(
                "Buscar alumno para agregar",
                key="clases_participantes_search_add_student",
                placeholder="Nombre, login, DNI o alumno ID",
            )
            candidates_students = [
                row
                for row in students_catalog
                if int(row.get("alumno_id")) not in current_alumno_ids
                and _row_matches_text(
                    row,
                    search_add_student,
                    (
                        "alumno_id",
                        "persona_id",
                        "nombre_completo",
                        "nombre",
                        "apellido_paterno",
                        "apellido_materno",
                        "login",
                        "id_oficial",
                    ),
                )
            ][:250]
            candidate_students_by_id = {
                int(row["alumno_id"]): row
                for row in candidates_students
                if _safe_int(row.get("alumno_id")) is not None
            }
            add_alumno_ids = st.multiselect(
                "Alumnos a agregar",
                options=sorted(candidate_students_by_id.keys()),
                key="clases_participantes_add_alumnos",
                format_func=lambda value: _clase_person_label(
                    candidate_students_by_id.get(int(value), {}),
                    "alumno_id",
                ),
                placeholder="Filtra y selecciona alumnos",
            )
            if st.button(
                "Agregar alumnos",
                key="clases_participantes_add_alumnos_btn",
                disabled=not bool(add_alumno_ids),
                use_container_width=True,
            ):
                if not token:
                    st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                    st.stop()
                ok_count = 0
                same_count = 0
                errors: List[str] = []
                for alumno_id in add_alumno_ids:
                    if int(alumno_id) in current_alumno_ids:
                        same_count += 1
                        continue
                    ok, msg = _asignar_alumno_a_clase_web(
                        token=token,
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        clase_id=selected_class_id_int,
                        alumno_id=int(alumno_id),
                        timeout=int(timeout),
                    )
                    if ok:
                        ok_count += 1
                    else:
                        errors.append(f"Alumno {int(alumno_id)}: {msg}")
                _refresh_detail(selected_class_id_int)
                _clear_action_widgets()
                st.session_state["clases_participantes_notice"] = {
                    "type": "warning" if errors else "success",
                    "message": "Alumnos agregados: {ok} | Ya estaban: {same} | Errores: {err}".format(
                        ok=ok_count,
                        same=same_count,
                        err=len(errors),
                    )
                    + ((" | " + " | ".join(errors[:3])) if errors else ""),
                }
                st.rerun()

    with tab_profesores:
        profesores_by_id = {
            int(row["persona_id"]): row
            for row in profesores_current
            if _safe_int(row.get("persona_id")) is not None
        }
        if profesores_current:
            _show_dataframe(
                [
                    {
                        "Persona ID": row.get("persona_id", ""),
                        "Profesor": row.get("nombre", ""),
                        "Login": row.get("login", ""),
                        "DNI": row.get("dni", ""),
                        "Activo": "Si" if bool(row.get("activo", True)) else "No",
                    }
                    for row in profesores_current
                ],
                use_container_width=True,
            )
        else:
            st.caption("La clase no tiene profesores cargados.")

        remove_profesor_ids = st.multiselect(
            "Profesores a quitar",
            options=sorted(profesores_by_id.keys()),
            key="clases_participantes_remove_profesores",
            format_func=lambda value: _clase_person_label(
                profesores_by_id.get(int(value), {}),
                "persona_id",
            ),
            placeholder="Selecciona profesores registrados en la clase",
        )
        confirm_remove_profesores = st.checkbox(
            "Confirmo quitar los profesores seleccionados de esta clase.",
            key="clases_participantes_confirm_remove_profesores",
        )
        if st.button(
            "Quitar profesores",
            key="clases_participantes_remove_profesores_btn",
            disabled=not bool(remove_profesor_ids),
            use_container_width=True,
        ):
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
            if not confirm_remove_profesores:
                st.error("Confirma la accion antes de quitar profesores.")
                st.stop()
            ok_count = 0
            errors: List[str] = []
            for persona_id in remove_profesor_ids:
                try:
                    _delete_profesor_clase_gestion_escolar(
                        token=token,
                        clase_id=selected_class_id_int,
                        persona_id=int(persona_id),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                    )
                    ok_count += 1
                except Exception as exc:
                    errors.append(f"Profesor {int(persona_id)}: {exc}")
            _refresh_detail(selected_class_id_int)
            _clear_action_widgets()
            st.session_state["clases_participantes_notice"] = {
                "type": "warning" if errors else "success",
                "message": "Profesores quitados: {ok}. Errores: {err}".format(
                    ok=ok_count,
                    err=len(errors),
                )
                + ((" | " + " | ".join(errors[:3])) if errors else ""),
            }
            st.rerun()

        st.divider()
        st.markdown("**Agregar profesores**")
        if not professors_catalog:
            st.caption("No se pudieron cargar los docentes del colegio para esta vista.")
        else:
            current_profesor_ids = set(profesores_by_id.keys())
            search_add_profesor = st.text_input(
                "Buscar profesor para agregar",
                key="clases_participantes_search_add_profesor",
                placeholder="Nombre, login, DNI, email o persona ID",
            )
            candidates_profesores = [
                row
                for row in professors_catalog
                if int(row.get("persona_id")) not in current_profesor_ids
                and _row_matches_text(
                    row,
                    search_add_profesor,
                    ("persona_id", "nombre", "login", "dni", "email", "estado"),
                )
            ][:250]
            candidate_profesores_by_id = {
                int(row["persona_id"]): row
                for row in candidates_profesores
                if _safe_int(row.get("persona_id")) is not None
            }
            add_profesor_ids = st.multiselect(
                "Profesores a agregar",
                options=sorted(candidate_profesores_by_id.keys()),
                key="clases_participantes_add_profesores",
                format_func=lambda value: _clase_person_label(
                    candidate_profesores_by_id.get(int(value), {}),
                    "persona_id",
                ),
                placeholder="Filtra y selecciona profesores",
            )
            if st.button(
                "Agregar profesores",
                key="clases_participantes_add_profesores_btn",
                disabled=not bool(add_profesor_ids),
                use_container_width=True,
            ):
                if not token:
                    st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                    st.stop()
                ok_count = 0
                same_count = 0
                errors: List[str] = []
                for persona_id in add_profesor_ids:
                    if int(persona_id) in current_profesor_ids:
                        same_count += 1
                        continue
                    ok, msg = _assign_profesor_to_clase_web(
                        token=token,
                        clase_id=selected_class_id_int,
                        persona_id=int(persona_id),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                    )
                    if ok:
                        ok_count += 1
                    else:
                        errors.append(f"Profesor {int(persona_id)}: {msg}")
                _refresh_detail(selected_class_id_int)
                _clear_action_widgets()
                st.session_state["clases_participantes_notice"] = {
                    "type": "warning" if errors else "success",
                    "message": "Profesores agregados: {ok} | Ya estaban: {same} | Errores: {err}".format(
                        ok=ok_count,
                        same=same_count,
                        err=len(errors),
                    )
                    + ((" | " + " | ".join(errors[:3])) if errors else ""),
                }
                st.rerun()


def _render_alumnos_global_login_search_section(
    colegio_id_raw: str,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> None:
    with st.container(border=True):
        st.markdown("**Buscar alumno por login en colegios**")
        st.caption(
            "Consulta el catalogo de colegios del token global y busca coincidencias de login."
        )

        token_for_catalog = _get_shared_token()
        if token_for_catalog:
            _ensure_shared_colegios_loaded(
                token=token_for_catalog,
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
            )

        colegio_rows = [
            row
            for row in (st.session_state.get("shared_colegios_rows") or [])
            if isinstance(row, dict) and _safe_int(row.get("colegio_id")) is not None
        ]
        current_colegio_id = _safe_int(colegio_id_raw)
        row_by_id = {
            int(row["colegio_id"]): row
            for row in colegio_rows
            if _safe_int(row.get("colegio_id")) is not None
        }
        if current_colegio_id is not None and int(current_colegio_id) not in row_by_id:
            manual_row = {
                "colegio_id": int(current_colegio_id),
                "colegio": f"Colegio {int(current_colegio_id)}",
                "label": f"Colegio {int(current_colegio_id)}",
            }
            colegio_rows.append(manual_row)
            row_by_id[int(current_colegio_id)] = manual_row

        login_col, mode_col, scope_col = st.columns([2.1, 1.1, 1.4], gap="small")
        login_value = login_col.text_input(
            "Login",
            key="alumnos_global_login_value",
            placeholder="Ejemplo: lnjmf90942147",
        )
        match_mode = mode_col.selectbox(
            "Coincidencia",
            options=["exact", "contains"],
            key="alumnos_global_login_match_mode",
            format_func=lambda value: (
                "Exacta" if str(value) == "exact" else "Contiene"
            ),
        )
        scope_value = scope_col.selectbox(
            "Alcance",
            options=["all", "selected", "current"],
            key="alumnos_global_login_scope",
            format_func=lambda value: {
                "all": "Todos los colegios",
                "selected": "Colegios seleccionados",
                "current": "Colegio actual",
            }.get(str(value), str(value)),
        )

        colegio_options = sorted(
            row_by_id.keys(),
            key=lambda value: str((row_by_id.get(int(value)) or {}).get("label") or ""),
        )
        if scope_value == "selected":
            st.multiselect(
                "Colegios",
                options=colegio_options,
                key="alumnos_global_login_selected_ids",
                format_func=lambda value: str(
                    (row_by_id.get(int(value)) or {}).get("label")
                    or f"Colegio {int(value)}"
                ),
                placeholder=(
                    "Busca y selecciona colegios"
                    if colegio_options
                    else "No hay colegios cargados"
                ),
            )

        fallback_enabled = st.checkbox(
            "Si falla la consulta rapida, reintentar por nivel/grado/seccion",
            value=False,
            key="alumnos_global_login_fallback",
        )
        if fallback_enabled:
            st.caption(
                "El respaldo es mas lento porque recorre secciones del colegio cuando alumnosByFilters falla."
            )

        run_col, clear_col = st.columns([2, 1], gap="small")
        run_search = run_col.button(
            "Buscar login",
            type="primary",
            key="alumnos_global_login_run_btn",
            use_container_width=True,
            disabled=not bool(row_by_id),
        )
        clear_results = clear_col.button(
            "Limpiar",
            key="alumnos_global_login_clear_btn",
            use_container_width=True,
        )

        colegio_error = str(st.session_state.get("shared_colegios_error") or "").strip()
        if colegio_error:
            st.warning(f"No se pudo cargar la lista global de colegios: {colegio_error}")
        elif row_by_id:
            st.caption(f"Colegios disponibles para buscar: {len(row_by_id)}")
        else:
            st.info("Guarda un token global para cargar los colegios.")

    if clear_results:
        for state_key in (
            "alumnos_global_login_result",
            "alumnos_global_login_error",
        ):
            st.session_state.pop(state_key, None)
        st.rerun()

    if run_search:
        token = _get_shared_token()
        if not token:
            st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
            st.stop()

        selected_ids: List[int] = []
        if scope_value == "all":
            selected_ids = list(colegio_options)
        elif scope_value == "current":
            if current_colegio_id is None:
                st.error("No hay colegio actual seleccionado.")
                st.stop()
            selected_ids = [int(current_colegio_id)]
        else:
            selected_ids = [
                int(value)
                for value in (
                    st.session_state.get("alumnos_global_login_selected_ids") or []
                )
                if _safe_int(value) is not None
            ]

        if not selected_ids:
            st.error("Selecciona al menos un colegio.")
            st.stop()

        progress_bar = st.progress(0)
        status_box = st.empty()

        def _on_global_login_status(idx: int, total: int, message: str) -> None:
            progress_bar.progress(min(1.0, max(0.0, idx / max(total, 1))))
            status_box.caption(f"[{idx}/{total}] {message}")

        try:
            with st.spinner("Buscando login en colegios..."):
                result = _search_alumno_login_global(
                    token=token,
                    login=str(login_value or "").strip(),
                    colegio_rows=colegio_rows,
                    colegio_ids=selected_ids,
                    match_mode=str(match_mode or "exact"),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                    use_fallback=bool(fallback_enabled),
                    on_status=_on_global_login_status,
                )
        except Exception as exc:
            st.session_state["alumnos_global_login_error"] = str(exc)
            progress_bar.empty()
            status_box.empty()
            st.error(f"No se pudo buscar el login: {exc}")
            st.stop()

        progress_bar.empty()
        status_box.empty()
        st.session_state["alumnos_global_login_result"] = result
        st.session_state.pop("alumnos_global_login_error", None)
        st.success(
            "Busqueda lista. Coincidencias: {matches} | Colegios consultados: {schools}".format(
                matches=len(result.get("matches") or []),
                schools=int(result.get("colegios_consultados") or 0),
            )
        )

    result_error = str(st.session_state.get("alumnos_global_login_error") or "").strip()
    if result_error:
        st.error(result_error)

    result_cached = st.session_state.get("alumnos_global_login_result")
    if isinstance(result_cached, dict) and result_cached:
        matches_cached = list(result_cached.get("matches") or [])
        errors_cached = list(result_cached.get("errors") or [])
        with st.container(border=True):
            metric_cols = st.columns(4)
            metric_cols[0].metric("Coincidencias", len(matches_cached))
            metric_cols[1].metric(
                "Colegios",
                int(result_cached.get("colegios_consultados") or 0),
            )
            metric_cols[2].metric(
                "Alumnos revisados",
                int(result_cached.get("alumnos_revisados") or 0),
            )
            metric_cols[3].metric("Errores", len(errors_cached))

            if matches_cached:
                st.download_button(
                    label="Descargar resultado",
                    data=_export_simple_excel(matches_cached, sheet_name="login_global"),
                    file_name="busqueda_login_alumnos_colegios.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="alumnos_global_login_download",
                    use_container_width=True,
                )
                _show_dataframe(matches_cached, use_container_width=True)
            else:
                st.info("No se encontraron alumnos con ese login.")

            if errors_cached:
                with st.expander(
                    f"Errores u observaciones ({len(errors_cached)})",
                    expanded=False,
                ):
                    st.write("\n".join(f"- {item}" for item in errors_cached[:80]))
                    pending = len(errors_cached) - 80
                    if pending > 0:
                        st.caption(f"... y {pending} errores mas.")


def _render_otras_funcionalidades_view() -> None:
    st.subheader("Otras funcionalidades")
    st.caption("Procesos transversales que no pertenecen a un CRUD puntual.")

    token = _get_shared_token()
    empresa_id = DEFAULT_EMPRESA_ID
    ciclo_id = ALUMNOS_CICLO_ID_DEFAULT
    timeout = 30
    colegio_id_raw = str(st.session_state.get("shared_colegio_id", "") or "").strip()
    current_colegio_id = _safe_int(colegio_id_raw)

    if token:
        _ensure_shared_colegios_loaded(
            token=token,
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )

    _render_alumnos_global_login_search_section(
        colegio_id_raw=colegio_id_raw,
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
    )

    with st.container(border=True):
        st.markdown("**Censo activos por varios colegios**")
        st.caption(
            "Selecciona colegios y genera un ZIP masivo. Puedes descargar solo alumnos o solo docentes."
        )
        colegio_rows_multi = st.session_state.get("shared_colegios_rows") or []
        colegio_error_multi = str(
            st.session_state.get("shared_colegios_error") or ""
        ).strip()
        row_by_id_multi = {
            int(row["colegio_id"]): row
            for row in colegio_rows_multi
            if isinstance(row, dict) and row.get("colegio_id") is not None
        }
        selected_multi_key = "otras_censo_activos_multi_ids"
        if selected_multi_key not in st.session_state:
            default_multi_ids: List[int] = []
            if (
                current_colegio_id is not None
                and int(current_colegio_id) in row_by_id_multi
            ):
                default_multi_ids = [int(current_colegio_id)]
            st.session_state[selected_multi_key] = default_multi_ids

        st.multiselect(
            "Colegios",
            options=sorted(
                row_by_id_multi.keys(),
                key=lambda value: str((row_by_id_multi.get(int(value)) or {}).get("label") or ""),
            ),
            key=selected_multi_key,
            format_func=lambda value: str(
                (row_by_id_multi.get(int(value)) or {}).get("label")
                or f"Colegio {int(value)}"
            ),
            placeholder=(
                "Busca y selecciona uno o varios colegios"
                if row_by_id_multi
                else "Guarda un token para cargar colegios"
            ),
            disabled=not bool(row_by_id_multi),
        )
        if colegio_error_multi:
            st.caption(f"No se pudo cargar la lista de colegios: {colegio_error_multi}")
        elif not row_by_id_multi:
            st.caption("Guarda un token en Configuracion global para cargar colegios.")

        action_cols = st.columns(2, gap="small")
        run_censo_alumnos_multi = action_cols[0].button(
            "Censo de alumnos",
            type="primary",
            key="otras_censo_alumnos_multi_btn",
            use_container_width=True,
            disabled=not bool(row_by_id_multi),
        )
        run_censo_docentes_multi = action_cols[1].button(
            "Censo de docentes",
            type="primary",
            key="otras_censo_docentes_multi_btn",
            use_container_width=True,
            disabled=not bool(row_by_id_multi),
        )

    if run_censo_alumnos_multi or run_censo_docentes_multi:
        if not token:
            st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
            st.stop()
        colegio_ids_multi_raw = st.session_state.get(selected_multi_key) or []
        colegio_ids_multi = [
            int(value)
            for value in colegio_ids_multi_raw
            if _safe_int(value) is not None
        ]
        if not colegio_ids_multi:
            st.error("Selecciona al menos un colegio.")
            st.stop()

        censo_kind = "alumnos" if run_censo_alumnos_multi else "docentes"
        status_placeholder = st.empty()
        total_colegios_multi = len(colegio_ids_multi)
        summary_rows_multi: List[Dict[str, object]] = []
        errors_multi: List[str] = []
        zip_buffer = BytesIO()
        zip_name_multi = (
            f"censo_{censo_kind}_activos_colegios_{date.today().isoformat()}.zip"
        )
        zip_root_folder = _sanitize_zip_component(
            f"censo_{censo_kind}_activos_colegios_{date.today().isoformat()}",
            f"censo_{censo_kind}_activos_colegios",
        )

        with ZipFile(zip_buffer, "w", ZIP_DEFLATED) as zip_file:
            for idx, colegio_id_multi in enumerate(colegio_ids_multi, start=1):
                colegio_base_name = _get_colegio_export_base_name(int(colegio_id_multi))

                def _on_multi_status(
                    message: str,
                    current_idx: int = idx,
                    current_total: int = total_colegios_multi,
                    colegio_name: str = colegio_base_name,
                ) -> None:
                    status_placeholder.caption(
                        "[{idx}/{total}] {colegio}: {message}".format(
                            idx=current_idx,
                            total=current_total,
                            colegio=colegio_name,
                            message=str(message or "").strip(),
                        )
                    )

                try:
                    if censo_kind == "alumnos":
                        payload_multi = _load_censo_activos_for_colegio(
                            token=token,
                            colegio_id=int(colegio_id_multi),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                            on_status=_on_multi_status,
                        )
                        export_rows_multi = list(payload_multi.get("export_rows") or [])
                        errors_colegio_multi = list(payload_multi.get("errors") or [])
                        file_name = f"censo_alumnos_activos_{colegio_base_name}.xlsx"
                        zip_path = f"{zip_root_folder}/{colegio_base_name}/{file_name}"
                        zip_file.writestr(
                            zip_path,
                            _export_censo_activos_excel(export_rows_multi),
                        )
                        summary_rows_multi.append(
                            {
                                "Colegio ID": int(colegio_id_multi),
                                "Colegio": colegio_base_name,
                                "Alumnos activos": len(export_rows_multi),
                                "Errores": len(errors_colegio_multi),
                                "Archivo": zip_path,
                                "Estado": (
                                    "OK" if not errors_colegio_multi else "OK con errores"
                                ),
                            }
                        )
                    else:
                        payload_multi = _load_censo_profesores_activos_for_colegio(
                            token=token,
                            colegio_id=int(colegio_id_multi),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                            on_status=_on_multi_status,
                        )
                        export_rows_multi = list(payload_multi.get("export_rows") or [])
                        errors_colegio_multi = list(payload_multi.get("errors") or [])
                        file_name = f"censo_docentes_activos_{colegio_base_name}.xlsx"
                        zip_path = f"{zip_root_folder}/{colegio_base_name}/{file_name}"
                        zip_file.writestr(
                            zip_path,
                            _export_censo_profesores_activos_excel(export_rows_multi),
                        )
                        summary_rows_multi.append(
                            {
                                "Colegio ID": int(colegio_id_multi),
                                "Colegio": colegio_base_name,
                                "Docentes activos": len(export_rows_multi),
                                "Errores": len(errors_colegio_multi),
                                "Archivo": zip_path,
                                "Estado": (
                                    "OK" if not errors_colegio_multi else "OK con errores"
                                ),
                            }
                        )
                    errors_multi.extend(
                        [
                            f"Colegio {int(colegio_id_multi)}: {item}"
                            for item in errors_colegio_multi
                            if str(item or "").strip()
                        ]
                    )
                except Exception as exc:
                    errors_multi.append(f"Colegio {int(colegio_id_multi)}: {exc}")
                    summary_rows_multi.append(
                        {
                            "Colegio ID": int(colegio_id_multi),
                            "Colegio": colegio_base_name,
                            (
                                "Alumnos activos"
                                if censo_kind == "alumnos"
                                else "Docentes activos"
                            ): 0,
                            "Errores": 1,
                            "Archivo": "",
                            "Estado": f"ERROR: {exc}",
                        }
                    )

        zip_buffer.seek(0)
        st.session_state["otras_censo_activos_multi_zip_bytes"] = zip_buffer.getvalue()
        st.session_state["otras_censo_activos_multi_zip_name"] = zip_name_multi
        st.session_state["otras_censo_activos_multi_summary_rows"] = summary_rows_multi
        st.session_state["otras_censo_activos_multi_errors"] = errors_multi
        st.session_state["otras_censo_activos_multi_kind"] = censo_kind
        status_placeholder.empty()
        st.success(
            "ZIP de {kind} listo. Colegios: {total} | Errores acumulados: {errors}".format(
                kind="alumnos" if censo_kind == "alumnos" else "docentes",
                total=len(summary_rows_multi),
                errors=len(errors_multi),
            )
        )

    censo_multi_summary_cached = (
        st.session_state.get("otras_censo_activos_multi_summary_rows") or []
    )
    censo_multi_errors_cached = (
        st.session_state.get("otras_censo_activos_multi_errors") or []
    )
    censo_multi_zip_bytes_cached = (
        st.session_state.get("otras_censo_activos_multi_zip_bytes") or b""
    )
    censo_multi_zip_name_cached = str(
        st.session_state.get("otras_censo_activos_multi_zip_name") or ""
    ).strip()
    censo_multi_kind_cached = str(
        st.session_state.get("otras_censo_activos_multi_kind") or ""
    ).strip()
    if censo_multi_summary_cached:
        with st.container(border=True):
            multi_col_text, multi_col_total, multi_col_errors, multi_col_download = st.columns(
                [2.2, 1, 1, 1.5], gap="small"
            )
            with multi_col_text:
                st.markdown("**Resultado masivo por colegios**")
                st.caption(
                    "Tipo: {kind}. Se genero un Excel por colegio dentro del ZIP.".format(
                        kind=(
                            "alumnos"
                            if censo_multi_kind_cached == "alumnos"
                            else "docentes"
                        )
                    )
                )
            multi_col_total.metric("Colegios", len(censo_multi_summary_cached))
            multi_col_errors.metric("Errores", len(censo_multi_errors_cached))
            multi_col_download.download_button(
                label="Descargar ZIP",
                data=censo_multi_zip_bytes_cached,
                file_name=(
                    censo_multi_zip_name_cached
                    or f"censo_activos_colegios_{date.today().isoformat()}.zip"
                ),
                mime="application/zip",
                key="otras_censo_activos_multi_download",
                use_container_width=True,
            )
            _show_dataframe(censo_multi_summary_cached, use_container_width=True)
    if censo_multi_errors_cached:
        with st.expander(
            f"Errores del censo masivo ({len(censo_multi_errors_cached)})",
            expanded=False,
        ):
            st.markdown(
                "\n".join(f"- {item}" for item in censo_multi_errors_cached[:80])
            )
            pending_multi = len(censo_multi_errors_cached) - 80
            if pending_multi > 0:
                st.caption(f"... y {pending_multi} errores mas.")


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


def _build_users_payments_students_grid(
    rows: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    grid: List[Dict[str, object]] = []
    for row in rows:
        grid.append(
            {
                "NivelId": row.get("nivel_id"),
                "GradoId": row.get("grado_id"),
                "AlumnoId": row.get("alumno_id"),
                "PersonaId": row.get("persona_id"),
                "Apellido Paterno": row.get("apellido_paterno"),
                "Apellido Materno": row.get("apellido_materno"),
                "Nombre": row.get("nombre"),
                "Nombre Completo": row.get("nombre_completo"),
                "DNI": row.get("id_oficial"),
                "Seccion": row.get("seccion_norm") or row.get("seccion"),
                "GrupoId": row.get("grupo_id"),
                "Activo": "SI" if _to_bool(row.get("activo")) else "NO",
                "ConPago": "SI" if _to_bool(row.get("con_pago")) else "NO",
                "Fecha Desde": row.get("fecha_desde"),
                "Login": row.get("login"),
            }
        )
    return grid


def _filter_users_payments_paid_students(
    students: List[Dict[str, object]],
    only_origin_section: bool = False,
    allowed_grade_keys: Optional[Sequence[Tuple[int, int]]] = None,
) -> List[Dict[str, object]]:
    allowed_keys: Set[Tuple[int, int]] = set()
    if allowed_grade_keys:
        for item in allowed_grade_keys:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                continue
            nivel_id = _safe_int(item[0])
            grado_id = _safe_int(item[1])
            if nivel_id is None or grado_id is None:
                continue
            allowed_keys.add((int(nivel_id), int(grado_id)))

    filtered: List[Dict[str, object]] = []
    for row in students:
        if not _to_bool(row.get("con_pago")):
            continue
        nivel_id = _safe_int(row.get("nivel_id"))
        grado_id = _safe_int(row.get("grado_id"))
        if allowed_keys and (
            nivel_id is None
            or grado_id is None
            or (int(nivel_id), int(grado_id)) not in allowed_keys
        ):
            continue
        if only_origin_section:
            seccion = _normalize_seccion_key(
                row.get("seccion_norm") or row.get("seccion") or ""
            )
            if seccion != AUTO_MOVE_SECCION_ORIGEN:
                continue
        filtered.append(row)

    return sorted(
        filtered,
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


def _normalize_login_search_key(value: object) -> str:
    return _normalize_compare_id(value)


def _alumno_login_matches_search(
    login: object,
    target_key: str,
    match_mode: str,
) -> bool:
    candidate_key = _normalize_login_search_key(login)
    if not candidate_key or not target_key:
        return False
    if str(match_mode or "").strip().lower() == "contains":
        return target_key in candidate_key
    return candidate_key == target_key


def _format_global_alumno_match(
    student: Dict[str, object],
    colegio_row: Dict[str, object],
) -> Dict[str, object]:
    colegio_id = _safe_int(colegio_row.get("colegio_id"))
    colegio_name = str(colegio_row.get("colegio") or "").strip()
    if not colegio_name:
        colegio_name = str(colegio_row.get("label") or "").strip()
    nombre_completo = str(student.get("nombre_completo") or "").strip()
    if not nombre_completo:
        nombre_completo = " ".join(
            part
            for part in (
                str(student.get("nombre") or "").strip(),
                str(student.get("apellido_paterno") or "").strip(),
                str(student.get("apellido_materno") or "").strip(),
            )
            if part
        ).strip()
    return {
        "Colegio ID": int(colegio_id) if colegio_id is not None else "",
        "Colegio": colegio_name,
        "Alumno ID": student.get("alumno_id") or "",
        "Persona ID": student.get("persona_id") or "",
        "Alumno": nombre_completo,
        "DNI": str(student.get("id_oficial") or "").strip(),
        "Login": str(student.get("login") or "").strip(),
        "Nivel": str(student.get("nivel") or "").strip(),
        "Grado": str(student.get("grado") or "").strip(),
        "Seccion": str(
            student.get("seccion_norm") or student.get("seccion") or ""
        ).strip(),
        "Activo": "Si" if _to_bool(student.get("activo")) else "No",
        "Con pago": "Si" if _to_bool(student.get("con_pago")) else "No",
    }


def _search_alumno_login_in_colegio(
    token: str,
    colegio_row: Dict[str, object],
    login_key: str,
    match_mode: str,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    use_fallback: bool = False,
) -> Tuple[List[Dict[str, object]], List[str], int]:
    colegio_id = _safe_int(colegio_row.get("colegio_id"))
    if colegio_id is None:
        return [], ["Colegio sin ID valido."], 0

    colegio_label = str(
        colegio_row.get("label")
        or colegio_row.get("colegio")
        or f"Colegio {int(colegio_id)}"
    ).strip()
    errors: List[str] = []
    matches: List[Dict[str, object]] = []
    scanned = 0
    login_lookup_by_alumno: Dict[str, Dict[str, str]] = {}
    login_lookup_by_persona: Dict[str, Dict[str, str]] = {}
    try:
        (
            login_lookup_by_alumno,
            login_lookup_by_persona,
        ) = _fetch_login_password_lookup_censo(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
    except Exception as exc:
        errors.append(f"{colegio_label}: no se pudo leer plantilla de logins: {exc}")

    try:
        alumnos_raw = _fetch_alumnos_censo_by_filters(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
        scanned = len(alumnos_raw)
        for item in alumnos_raw:
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
            if _alumno_login_matches_search(flat.get("login"), login_key, match_mode):
                matches.append(_format_global_alumno_match(flat, colegio_row))
        return matches, errors, scanned
    except Exception as exc:
        errors.append(f"{colegio_label}: alumnosByFilters fallo: {exc}")

    if not use_fallback:
        return matches, errors, scanned

    try:
        catalog = _fetch_alumnos_catalog_for_manual_move(
            token=token,
            colegio_id=int(colegio_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
    except Exception as exc:
        errors.append(f"{colegio_label}: respaldo por secciones fallo: {exc}")
        return matches, errors, scanned

    students = [
        row for row in (catalog.get("students") or []) if isinstance(row, dict)
    ]
    scanned = max(scanned, len(students))
    for item in catalog.get("errors") or []:
        if str(item or "").strip():
            errors.append(f"{colegio_label}: {item}")
    for flat in students:
        if _alumno_login_matches_search(flat.get("login"), login_key, match_mode):
            matches.append(_format_global_alumno_match(flat, colegio_row))
    return matches, errors, scanned


def _search_alumno_login_global(
    token: str,
    login: str,
    colegio_rows: List[Dict[str, object]],
    colegio_ids: List[int],
    match_mode: str,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    use_fallback: bool = False,
    on_status: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, object]:
    login_key = _normalize_login_search_key(login)
    if not login_key:
        raise ValueError("Ingresa un login para buscar.")

    wanted_ids = {int(value) for value in colegio_ids if _safe_int(value) is not None}
    rows_by_id = {
        int(row["colegio_id"]): row
        for row in colegio_rows
        if isinstance(row, dict) and _safe_int(row.get("colegio_id")) is not None
    }
    rows_to_scan = [
        rows_by_id[int(colegio_id)]
        for colegio_id in sorted(
            wanted_ids,
            key=lambda value: str((rows_by_id.get(int(value)) or {}).get("label") or ""),
        )
        if int(colegio_id) in rows_by_id
    ]
    if not rows_to_scan:
        raise ValueError("Selecciona al menos un colegio valido para buscar.")

    matches: List[Dict[str, object]] = []
    errors: List[str] = []
    scanned_students = 0
    total = len(rows_to_scan)
    for idx, colegio_row in enumerate(rows_to_scan, start=1):
        colegio_label = str(
            colegio_row.get("label")
            or colegio_row.get("colegio")
            or colegio_row.get("colegio_id")
            or ""
        ).strip()
        if callable(on_status):
            on_status(idx, total, f"Consultando {colegio_label}")
        colegio_matches, colegio_errors, colegio_scanned = _search_alumno_login_in_colegio(
            token=token,
            colegio_row=colegio_row,
            login_key=login_key,
            match_mode=match_mode,
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
            use_fallback=bool(use_fallback),
        )
        matches.extend(colegio_matches)
        errors.extend(colegio_errors)
        scanned_students += int(colegio_scanned or 0)

    matches.sort(
        key=lambda row: (
            str(row.get("Colegio") or "").upper(),
            str(row.get("Alumno") or "").upper(),
            str(row.get("Login") or "").upper(),
        )
    )
    return {
        "matches": matches,
        "errors": errors,
        "colegios_consultados": total,
        "alumnos_revisados": scanned_students,
        "login_buscado": str(login or "").strip(),
        "match_mode": str(match_mode or "exact"),
    }


def _fetch_alumnos_con_pago_for_users_payments(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
    only_origin_section: bool = False,
    on_status: Optional[Callable[[str], None]] = None,
) -> Dict[str, object]:
    catalog = _fetch_alumnos_catalog_for_manual_move(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
        on_status=on_status,
    )
    students = list(catalog.get("students") or [])
    paid_students = _filter_users_payments_paid_students(
        students=students,
        only_origin_section=only_origin_section,
    )
    return {
        "niveles": catalog.get("niveles") or [],
        "students": paid_students,
        "students_grid": _build_users_payments_students_grid(paid_students),
        "errors": catalog.get("errors") or [],
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
        exclude_santillana_inclusiva=True,
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








if menu_option == "Richmond Studio":
    render_richmond_studio_view()
    st.stop()

def _render_users_payments_section(
    colegio_id_raw: str,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> None:
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

        col_prepare, col_paid, col_clear = st.columns([2, 2, 1], gap="small")
        run_prepare_auto_plan = col_prepare.button(
            "Analizar y preparar lista de cambios",
            type="primary",
            key="auto_move_prepare_btn",
            use_container_width=True,
        )
        run_paid_students = col_paid.button(
            "Traer alumnos con pago",
            key="auto_move_paid_students_btn",
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
                "auto_move_paid_students_grid",
                "auto_move_paid_students_errors",
            ):
                st.session_state.pop(state_key, None)
            st.rerun()

        if run_paid_students:
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
                paid_status_box = st.empty()

                def _on_paid_status(message: str) -> None:
                    msg = str(message or "").strip()
                    if not msg:
                        return
                    paid_status_box.info(msg)

                with st.spinner("Consultando alumnos con pago..."):
                    paid_catalog = _fetch_alumnos_con_pago_for_users_payments(
                        token=token,
                        colegio_id=int(colegio_id_int),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                        only_origin_section=False,
                        on_status=_on_paid_status,
                    )
                paid_status_box.empty()
            except Exception as exc:  # pragma: no cover - UI
                st.error(f"Error: {exc}")
                st.stop()

            st.session_state["auto_move_paid_students_grid"] = (
                paid_catalog.get("students_grid") or []
            )
            st.session_state["auto_move_paid_students_errors"] = (
                paid_catalog.get("errors") or []
            )
            st.success(
                "Alumnos con pago detectados: "
                f"{len(st.session_state['auto_move_paid_students_grid'])}"
            )

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
            st.session_state["auto_move_paid_students_grid"] = (
                simulation.get("paid_students_grid") or []
            )
            st.session_state["auto_move_paid_students_errors"] = (
                simulation.get("errors") or []
            )
            st.session_state["auto_move_removed_ref_ids"] = []

            total_plan = len(st.session_state["auto_move_plan_rows"])
            st.success(f"Simulacion lista. Alumnos candidatos a modificar: {total_plan}")

        paid_students_errors_cached = (
            st.session_state.get("auto_move_paid_students_errors") or []
        )
        if paid_students_errors_cached:
            st.warning("Hubo errores consultando alumnos con pago.")
            st.write("\n".join(f"- {item}" for item in paid_students_errors_cached[:20]))
            pending_paid = len(paid_students_errors_cached) - 20
            if pending_paid > 0:
                st.caption(f"... y {pending_paid} errores mas.")

        if "auto_move_paid_students_grid" in st.session_state:
            paid_students_grid_cached = (
                st.session_state.get("auto_move_paid_students_grid") or []
            )
            if paid_students_grid_cached:
                st.markdown("**Alumnos con pago detectados**")
                st.dataframe(
                    pd.DataFrame(paid_students_grid_cached),
                    use_container_width=True,
                    hide_index=True,
                    height=320,
                )
            else:
                st.info("No se encontraron alumnos con pago para el colegio actual.")

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
                    apply_progress = st.progress(
                        0,
                        text="Iniciando guardado de cambios autorizados...",
                    )
                    apply_status_box = st.empty()
                    apply_metrics_box = st.empty()

                    def _on_apply_status(message: str) -> None:
                        msg = str(message or "").strip()
                        if msg:
                            apply_status_box.info(msg)

                    def _on_apply_progress(payload: Dict[str, object]) -> None:
                        processed = int(payload.get("processed") or 0)
                        total = max(int(payload.get("total") or 0), 1)
                        alumno_actual = str(
                            payload.get("current_student") or ""
                        ).strip()
                        current_status = str(
                            payload.get("current_status") or ""
                        ).strip()
                        summary_payload = (
                            payload.get("summary")
                            if isinstance(payload.get("summary"), dict)
                            else {}
                        )
                        apply_progress.progress(
                            min(processed / total, 1.0),
                            text=(
                                f"Procesados {processed}/{total} alumno(s)"
                            ),
                        )
                        if alumno_actual or current_status:
                            apply_status_box.info(
                                "Alumno {processed}/{total}: {alumno} | {status}".format(
                                    processed=processed,
                                    total=total,
                                    alumno=alumno_actual or "-",
                                    status=current_status or "Procesando",
                                )
                            )
                        apply_metrics_box.caption(
                            "Avance: "
                            f"{processed}/{total} | "
                            f"Inactivar OK={int(summary_payload.get('inactivar_ok', 0))}, ERROR={int(summary_payload.get('inactivar_error', 0))} | "
                            f"Mover OK={int(summary_payload.get('mover_ok', 0))}, ERROR={int(summary_payload.get('mover_error', 0))} | "
                            f"Asignar OK={int(summary_payload.get('asignar_ok', 0))}, ERROR={int(summary_payload.get('asignar_error', 0))}, SKIP={int(summary_payload.get('asignar_skip', 0))}"
                        )

                    st.info(
                        "Iniciando guardado de cambios autorizados: "
                        f"{len(authorized_plans)} alumno(s)."
                    )
                    summary_apply, results_apply = _apply_auto_move_changes(
                        token=token,
                        colegio_id=int(colegio_id_exec),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                        plan_rows=authorized_plans,
                        on_status=_on_apply_status,
                        on_progress=_on_apply_progress,
                    )
                    apply_progress.progress(
                        1.0,
                        text=(
                            "Guardado completado: "
                            f"{len(authorized_plans)}/{len(authorized_plans)} alumno(s)"
                        ),
                    )
                except Exception as exc:  # pragma: no cover - UI
                    apply_status_box.error(
                        f"Error durante el guardado: {exc}"
                    )
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
                    apply_progress_multi = st.progress(
                        0,
                        text="Iniciando guardado masivo de cambios autorizados...",
                    )
                    apply_status_box_multi = st.empty()
                    apply_metrics_box_multi = st.empty()

                    def _on_apply_status_multi(message: str) -> None:
                        msg = str(message or "").strip()
                        if msg:
                            apply_status_box_multi.info(msg)

                    def _on_apply_progress_multi(payload: Dict[str, object]) -> None:
                        processed = int(payload.get("processed") or 0)
                        total = max(int(payload.get("total") or 0), 1)
                        alumno_actual = str(
                            payload.get("current_student") or ""
                        ).strip()
                        colegio_actual = _safe_int(
                            payload.get("current_colegio_id")
                        )
                        current_status = str(
                            payload.get("current_status") or ""
                        ).strip()
                        summary_payload = (
                            payload.get("summary")
                            if isinstance(payload.get("summary"), dict)
                            else {}
                        )
                        apply_progress_multi.progress(
                            min(processed / total, 1.0),
                            text=(
                                f"Procesados {processed}/{total} alumno(s) | "
                                f"Ultimo colegio: {colegio_actual or '-'}"
                            ),
                        )
                        if alumno_actual or current_status:
                            apply_status_box_multi.info(
                                "Alumno {processed}/{total}: {alumno} | Colegio {colegio} | {status}".format(
                                    processed=processed,
                                    total=total,
                                    alumno=alumno_actual or "-",
                                    colegio=colegio_actual or "-",
                                    status=current_status or "Procesando",
                                )
                            )
                        apply_metrics_box_multi.caption(
                            "Avance acumulado: "
                            f"{processed}/{total} | "
                            f"Inactivar OK={int(summary_payload.get('inactivar_ok', 0))}, ERROR={int(summary_payload.get('inactivar_error', 0))} | "
                            f"Mover OK={int(summary_payload.get('mover_ok', 0))}, ERROR={int(summary_payload.get('mover_error', 0))} | "
                            f"Asignar OK={int(summary_payload.get('asignar_ok', 0))}, ERROR={int(summary_payload.get('asignar_error', 0))}, SKIP={int(summary_payload.get('asignar_skip', 0))}"
                        )

                    st.info(
                        "Iniciando guardado masivo de cambios autorizados: "
                        f"{len(authorized_plans_multi)} alumno(s)."
                    )
                    summary_apply_multi, results_apply_multi = _apply_auto_move_changes(
                        token=token,
                        colegio_id=None,
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                        plan_rows=authorized_plans_multi,
                        on_status=_on_apply_status_multi,
                        on_progress=_on_apply_progress_multi,
                    )
                    apply_progress_multi.progress(
                        1.0,
                        text=(
                            "Guardado masivo completado: "
                            f"{len(authorized_plans_multi)}/{len(authorized_plans_multi)} alumno(s)"
                        ),
                    )
                except Exception as exc:  # pragma: no cover - UI
                    apply_status_box_multi.error(
                        f"Error durante el guardado masivo: {exc}"
                    )
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
        colegio_id_for_fragment: Optional[int] = None
        if colegio_id_raw:
            try:
                colegio_id_for_fragment = _parse_colegio_id(colegio_id_raw)
            except ValueError:
                colegio_id_for_fragment = None
        asignacion_job_id_for_fragment = ""
        if colegio_id_for_fragment is not None:
            asignacion_job_id_for_fragment = str(
                _get_participantes_sync_job_id_for_scope(
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    colegio_id=int(colegio_id_for_fragment),
                )
                or ""
            ).strip()
        if not asignacion_job_id_for_fragment:
            asignacion_job_id_for_fragment = str(
                st.session_state.get("clases_auto_group_job_id") or ""
            ).strip()
        asignacion_job_for_fragment = _get_participantes_sync_job(
            asignacion_job_id_for_fragment
        )
        asignacion_fragment_run_every = (
            "2s"
            if _is_participantes_sync_job_active(asignacion_job_for_fragment)
            else None
        )
        if asignacion_fragment_run_every and asignacion_job_id_for_fragment:
            st.session_state["clases_auto_group_polling_job_id"] = (
                asignacion_job_id_for_fragment
            )

        @st.fragment(run_every=asignacion_fragment_run_every)
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
                    "clases_auto_group_ingles_detected_dialog",
                ):
                    st.session_state.pop(state_key, None)
                _clear_ingles_por_niveles_assignment_state()

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
            polling_job_id = str(
                st.session_state.get("clases_auto_group_polling_job_id") or ""
            ).strip()

            with st.container(border=True):
                st.markdown("**Asignacion**")
                st.caption("Sincronizacion por grado y seccion.")
                exclude_ingles_por_niveles = st.checkbox(
                    "Ingles por niveles",
                    key="clases_auto_group_exclude_ingles_checkbox",
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
                        detected_ingles_grade_options: List[Dict[str, object]] = []
                        try:
                            with st.spinner("Cargando grados del colegio..."):
                                niveles_ingles = _fetch_niveles_grados_grupos_censo(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
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
                            try:
                                clases_ingles = _fetch_clases_gestion_escolar(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                    ordered=True,
                                )
                            except Exception:
                                clases_ingles = []
                            if clases_ingles:
                                detected_ingles_grade_options = (
                                    _build_ingles_grade_options_for_participantes(
                                        clases_ingles
                                    )
                                )
                            ingles_grade_options = (
                                _build_ingles_grade_catalog_options_for_participantes(
                                    niveles_ingles,
                                    detected_options=detected_ingles_grade_options,
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

                    if ingles_grade_error:
                        st.error(
                            f"No se pudieron cargar los grados de Ingles: {ingles_grade_error}"
                        )
                    elif ingles_grade_options:
                        st.caption(
                            "Selecciona los grados que llevan ingles por niveles. "
                            "Si una clase no fue detectada por nombre, puedes marcar "
                            "su grado manualmente."
                        )
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
                        st.markdown("**Grados de ingles**")
                        checkbox_cols = st.columns(2, gap="small")
                        selected_ingles_grade_keys = []
                        for idx_option, option_key in enumerate(valid_ingles_option_keys):
                            option_row = ingles_grade_option_by_key.get(
                                str(option_key), {}
                            )
                            checkbox_label = _format_participantes_ingles_grade_label(
                                option_row
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
                            if is_selected:
                                selected_ingles_grade_keys.append(str(option_key))
                        st.session_state[
                            "clases_auto_group_ingles_grade_selected_keys"
                        ] = list(selected_ingles_grade_keys)
                    else:
                        st.caption("No hay grados disponibles para seleccionar.")
                    if colegio_id_int is not None:
                        _render_ingles_por_niveles_excel_assignment_block(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                            selected_ingles_grade_keys=selected_ingles_grade_keys,
                        )
                    else:
                        st.caption(
                            "Ingresa un Colegio Clave valido para usar la asignacion de ingles por Excel."
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
                        can_start_sync = True
                        selected_ingles_class_ids: List[int] = []
                        if not exclude_ingles_por_niveles:
                            detection_status_placeholder = st.empty()
                            try:
                                with st.spinner(
                                    "Validando comportamiento de Ingles por niveles..."
                                ):
                                    detection_result = _detect_ingles_por_niveles_behavior(
                                        token=token,
                                        colegio_id=int(colegio_id_int),
                                        empresa_id=int(empresa_id),
                                        ciclo_id=int(ciclo_id),
                                        timeout=int(timeout),
                                        on_status=lambda message: detection_status_placeholder.write(
                                            str(message or "")
                                        ),
                                    )
                            except Exception as exc:
                                detection_status_placeholder.empty()
                                st.error(
                                    "No se pudo validar Ingles por niveles antes de actualizar: "
                                    f"{exc}"
                                )
                                can_start_sync = False
                            else:
                                detection_status_placeholder.empty()
                                detected_grade_options = [
                                    row
                                    for row in list(
                                        detection_result.get("grade_options") or []
                                    )
                                    if isinstance(row, dict)
                                ]
                                if current_ingles_scope is not None:
                                    st.session_state[
                                        "clases_auto_group_ingles_grades_scope"
                                    ] = current_ingles_scope
                                    st.session_state[
                                        "clases_auto_group_ingles_grade_options"
                                    ] = list(detected_grade_options)
                                    st.session_state[
                                        "clases_auto_group_ingles_grade_error"
                                    ] = ""
                                detection_error = str(
                                    detection_result.get("error") or ""
                                ).strip()
                                if detection_result.get("detected"):
                                    st.session_state[
                                        "clases_auto_group_ingles_detected_dialog"
                                    ] = {
                                        "scope": current_ingles_scope,
                                        "grade_options": list(detected_grade_options),
                                        "affected_grade_keys": list(
                                            detection_result.get("affected_grade_keys") or []
                                        ),
                                        "affected_grade_labels": list(
                                            detection_result.get("affected_grade_labels") or []
                                        ),
                                        "evidence_rows": list(
                                            detection_result.get("evidence_rows") or []
                                        ),
                                        "evidence_total": int(
                                            _safe_int(
                                                detection_result.get("evidence_total")
                                            )
                                            or 0
                                        ),
                                        "class_errors": list(
                                            detection_result.get("class_errors") or []
                                        ),
                                    }
                                    can_start_sync = False
                                elif detection_error:
                                    st.error(detection_error)
                                    can_start_sync = False
                        if (
                            can_start_sync
                            and
                            exclude_ingles_por_niveles
                            and selected_ingles_grade_keys
                        ):
                            preview_rows_for_apply = list(
                                st.session_state.get(
                                    "clases_auto_group_ingles_excel_preview_rows"
                                )
                                or []
                            )
                            selected_ingles_class_ids = _collect_ingles_class_ids_from_rows(
                                preview_rows_for_apply,
                                selected_ingles_grade_keys,
                            )
                            if not selected_ingles_class_ids:
                                selected_ingles_class_ids = _collect_ingles_class_ids_from_rows(
                                    list(
                                        st.session_state.get(
                                            "clases_auto_group_ingles_excel_apply_rows"
                                        )
                                        or []
                                    ),
                                    selected_ingles_grade_keys,
                                )
                            if preview_rows_for_apply:
                                apply_status_placeholder = st.empty()
                                apply_ok, apply_error = (
                                    _apply_ingles_assignment_for_selected_grades(
                                        token=token,
                                        colegio_id=int(colegio_id_int),
                                        empresa_id=int(empresa_id),
                                        ciclo_id=int(ciclo_id),
                                        timeout=int(timeout),
                                        preview_rows=preview_rows_for_apply,
                                        selected_ingles_grade_keys=selected_ingles_grade_keys,
                                        status_placeholder=apply_status_placeholder,
                                    )
                                )
                                if not apply_ok:
                                    st.error(apply_error)
                                    can_start_sync = False
                            else:
                                _set_ingles_por_niveles_result_notice(
                                    "warning",
                                    "No se aplico Excel de ingles. Las clases de Ingles "
                                    "de los grados seleccionados se conservaran sin cambios.",
                                )
                        if can_start_sync:
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
                                ingles_class_ids=selected_ingles_class_ids,
                            )
                            st.session_state["clases_auto_group_job_id"] = current_job_id
                            st.session_state["clases_auto_group_polling_job_id"] = (
                                current_job_id
                            )
                            st.rerun()

                if run_cancelar_participantes_auto:
                    if _request_cancel_participantes_sync_job(current_job_id):
                        if current_job_id:
                            st.session_state["clases_auto_group_polling_job_id"] = (
                                current_job_id
                            )
                        st.rerun()
                    else:
                        st.info("No hay un proceso activo para cancelar.")

                if isinstance(
                    st.session_state.get("clases_auto_group_ingles_detected_dialog"),
                    dict,
                ):
                    _show_ingles_por_niveles_detected_dialog()

                if colegio_error:
                    st.caption(f"Colegio actual invalido: {colegio_error}")

                if not isinstance(current_job, dict):
                    return

                state = str(current_job.get("state") or "").strip()
                summary_auto = (
                    dict(current_job.get("summary"))
                    if isinstance(current_job.get("summary"), dict)
                    else {}
                )
                warnings_auto = list(current_job.get("warnings") or [])
                group_error_lines = list(current_job.get("group_error_lines") or [])
                detail_rows = list(current_job.get("detail_rows") or [])
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
                error_trace = str(current_job.get("error_trace") or "").strip()

                if (
                    current_job_id
                    and polling_job_id == current_job_id
                    and state in {"done", "cancelled", "error"}
                ):
                    st.session_state.pop("clases_auto_group_polling_job_id", None)
                    st.rerun()

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
                    f"Alumnos objetivo={summary_auto.get('alumnos_objetivo', 0)} | "
                    f"Ya estaban={summary_auto.get('alumnos_sin_cambios', 0)} | "
                    f"Alumnos asignados={summary_auto.get('agregados_ok', 0)} | "
                    f"Error asignar={summary_auto.get('agregados_error', 0)} | "
                    f"Alumnos eliminados={summary_auto.get('eliminados_ok', 0)} | "
                    f"Error eliminar={summary_auto.get('eliminados_error', 0)} | "
                    f"Clases sin cambios={summary_auto.get('clases_skip', 0)} | "
                    f"Clases con error={summary_auto.get('clases_error', 0)} | "
                    f"Ingles por niveles={'Si' if exclude_ingles_job else 'No'} | "
                    f"Grados ingles={len(ingles_grade_keys_job)}"
                )
                if detail_rows:
                    with st.expander(
                        f"Detalle por clase ({len(detail_rows)})",
                        expanded=state in {"done", "error"},
                    ):
                        _show_dataframe(detail_rows[:200], use_container_width=True)
                        if len(detail_rows) > 200:
                            st.caption(
                                f"Mostrando 200 de {len(detail_rows)} fila(s)."
                            )
                if warnings_auto:
                    with st.expander(
                        f"Advertencias de mapeo de clases ({len(warnings_auto)})"
                    ):
                        st.write("\n".join(f"- {item}" for item in warnings_auto))
                if group_error_lines:
                    with st.expander(
                        f"Errores al consultar secciones ({len(group_error_lines)})"
                    ):
                        st.write("\n".join(f"- {item}" for item in group_error_lines))
                if error_trace and state == "error":
                    with st.expander("Detalle tecnico del error", expanded=True):
                        st.code(error_trace)

        @st.fragment
        def _render_clases_gestion_section() -> None:
            listed_class_rows = st.session_state.get("clases_gestion_rows") or []
            selected_class_widget_key = "clases_gestion_selected_ids_widget"
            selected_class_ids_state = {
                int(item)
                for item in (st.session_state.get("clases_gestion_selected_ids") or [])
                if _safe_int(item) is not None
            }
            action_status_box = st.empty()

            col_list, col_selected = st.columns([2.2, 1.4], gap="large")
            with col_list:
                with st.container(border=True):
                    st.markdown("**Clases disponibles**")
                    run_listar_clases = st.button("Listar clases", key="clases_listar_btn")
                    if run_listar_clases:
                        if not token:
                            st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                        else:
                            action_status_box.info("Validando parametros para listar clases...")
                            try:
                                colegio_id_int = _parse_colegio_id(colegio_id_raw)
                            except ValueError as exc:
                                action_status_box.error(f"Error: {exc}")
                                st.error(f"Error: {exc}")
                                st.stop()
                            try:
                                action_status_box.info("Consultando clases en Pegasus...")
                                clases = _fetch_clases_gestion_escolar(
                                    token=token,
                                    colegio_id=colegio_id_int,
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                )
                            except Exception as exc:  # pragma: no cover - UI
                                action_status_box.error(f"Error al listar clases: {exc}")
                                st.error(f"Error: {exc}")
                            else:
                                if not clases:
                                    listed_class_rows = []
                                    st.session_state["clases_gestion_rows"] = []
                                    st.session_state["clases_gestion_selected_ids"] = []
                                    st.session_state[selected_class_widget_key] = []
                                    action_status_box.info("No se encontraron clases.")
                                else:
                                    action_status_box.info(
                                        f"Procesando {len(clases)} clase(s) encontradas..."
                                    )
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
                                    listed_class_rows = tabla
                                    selected_class_ids_state = set()
                                    st.session_state["clases_gestion_rows"] = tabla
                                    st.session_state["clases_gestion_selected_ids"] = []
                                    st.session_state[selected_class_widget_key] = []
                                    action_status_box.success(
                                        f"Clases encontradas: {len(tabla)}"
                                    )
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
                        selected_widget_ids_raw = (
                            st.session_state.get(selected_class_widget_key)
                            if isinstance(
                                st.session_state.get(selected_class_widget_key), list
                            )
                            else sorted(selected_class_ids_state)
                        )
                        selected_widget_ids = [
                            int(class_id)
                            for class_id in selected_widget_ids_raw
                            if _safe_int(class_id) is not None
                            and int(class_id) in class_name_by_id
                        ]
                        if (
                            selected_class_widget_key not in st.session_state
                            or st.session_state.get(selected_class_widget_key)
                            != selected_widget_ids
                        ):
                            st.session_state[selected_class_widget_key] = (
                                selected_widget_ids
                            )
                        selected_ids = st.multiselect(
                            "Clases a eliminar",
                            options=list(class_name_by_id.keys()),
                            format_func=lambda class_id: class_name_by_id.get(
                                int(class_id), str(class_id)
                            ),
                            placeholder="Selecciona una o varias clases.",
                            key=selected_class_widget_key,
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

                action_status_box.info(
                    f"Preparando eliminacion de {len(selected_class_rows)} clase(s)..."
                )
                errores: List[str] = []
                eliminadas_ids: Set[int] = set()
                total_selected = len(selected_class_rows)
                for idx_item, item in enumerate(selected_class_rows, start=1):
                    clase_id = item.get("ID") if isinstance(item, dict) else None
                    if clase_id is None:
                        errores.append("Clase sin ID.")
                        continue
                    clase_label = str(item.get("Clase") or clase_id).strip()
                    action_status_box.info(
                        "Eliminando clase {idx}/{total}: {label}".format(
                            idx=idx_item,
                            total=total_selected,
                            label=clase_label,
                        )
                    )
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
                st.session_state[selected_class_widget_key] = list(
                    st.session_state["clases_gestion_selected_ids"]
                )
                eliminadas = len(eliminadas_ids)
                if errores:
                    action_status_box.warning(
                        "Eliminacion completada con observaciones. "
                        f"Eliminadas: {eliminadas} | Errores: {len(errores)}"
                    )
                else:
                    action_status_box.success(f"Clases eliminadas: {eliminadas}")
                st.success(f"Clases eliminadas: {eliminadas}")
                if errores:
                    st.error("Errores al eliminar:")
                    st.write("\n".join(f"- {item}" for item in errores))

        if str(st.session_state.get("clases_crud_nav") or "").strip() == "simulador":
            st.session_state["clases_crud_nav"] = "gestion"
        clases_nav_col, clases_body_col = st.columns([1.15, 4.85], gap="large")
        with clases_nav_col:
            clases_crud_view = _render_crud_menu(
                "Funciones de clases",
                [
                    ("crear", "Crear", "Genera clases desde Excel"),
                    ("gestion", "Gestion", "Lista, vacia o elimina clases"),
                    ("participantes", "Participantes", "Busca una clase y administra alumnos/docentes"),
                    ("otros", "Asignacion de clases a usuarios", "Asignacion de clases a usuarios"),
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
                    create_progress = st.progress(0)
                    create_status = st.empty()
                    if not uploaded_excel:
                        create_progress.empty()
                        create_status.error("Sube un Excel de entrada.")
                        st.error("Sube un Excel de entrada.")
                        st.stop()
                    if not codigo.strip():
                        create_progress.empty()
                        create_status.error("Ingresa un codigo.")
                        st.error("Ingresa un cÃ³digo.")
                        st.stop()
                    if not grupos.strip():
                        create_progress.empty()
                        create_status.error("Ingresa las secciones (A,B,C,D).")
                        st.error("Ingresa las secciones (A,B,C,D).")
                        st.stop()

                    create_progress.progress(10)
                    create_status.info("Leyendo Excel de entrada...")
                    excel_bytes = uploaded_excel.read()
                    plantilla_path = Path(OUTPUT_FILENAME) if Path(OUTPUT_FILENAME).exists() else None

                    try:
                        create_progress.progress(35)
                        create_status.info("Procesando clases y generando plantilla...")
                        output_bytes, summary = process_excel(
                            excel_bytes,
                            codigo=codigo,
                            columna_codigo=CODE_COLUMN_NAME,
                            hoja=SHEET_NAME,
                            plantilla_path=plantilla_path,
                            grupos=grupos,
                        )
                        create_progress.progress(100)
                        create_status.success(
                            "Listo. Filtradas: {filtradas}, Salida: {salida} filas.".format(
                                filtradas=summary["filas_filtradas"],
                                salida=summary["filas_salida"],
                            )
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
                        create_progress.empty()
                        create_status.error(f"Error al generar clases: {exc}")
                        st.error(f"Error: {exc}")
            if clases_crud_view == "gestion":
                st.markdown("**2) Gestion de clases**")
                _render_clases_gestion_section()
            if clases_crud_view == "participantes":
                _render_clases_participantes_section(
                    colegio_id_raw=colegio_id_raw,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            if clases_crud_view == "otros":
                _render_asignacion_clases_usuarios_section()
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
        if str(st.session_state.get("profesores_crud_nav") or "").strip() in {
            "bd",
            "base",
            "asignar",
        }:
            st.session_state["profesores_crud_nav"] = "editar"
        profesores_nav_col, profesores_body_col = st.columns([1.15, 4.85], gap="large")
        with profesores_nav_col:
            profesores_crud_view = _render_crud_menu(
                "Funciones de profesores",
                [
                    ("manual", "Manual", "Asigna clases por docente"),
                    ("editar", "Editar", "Edita datos, estado, login, password y clases"),
                ],
                state_key="profesores_crud_nav",
            )
        with profesores_body_col:
            if False and profesores_crud_view == "bd":
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
                        current_manual_class_ids = [
                            int(item)
                            for item in profesor_manual_row.get("clase_ids_actuales", [])
                            if int(item) in clases_manual_by_id
                        ]
                        clases_options = sorted(clases_manual_by_id.keys())
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
                            if current_manual_class_ids:
                                st.markdown("**Clases actuales**")
                                st.markdown(
                                    _render_profesor_class_chips_html(
                                        current_manual_class_ids,
                                        clases_manual_by_id,
                                        empty_text="Sin clases actuales",
                                    ),
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.caption("Sin clases actuales.")

                        with cols_manual_right:
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
                            st.markdown("**Clases seleccionadas**")
                            st.markdown(
                                _render_profesor_class_chips_html(
                                    selected_manual_class_ids,
                                    clases_manual_by_id,
                                    empty_text="Sin clases seleccionadas",
                                ),
                                unsafe_allow_html=True,
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
                    "Lista docentes del colegio, carga el detalle y actualiza datos base, estado, login, password y clases."
                )
                st.caption(
                    "Las clases seleccionadas sincronizan automaticamente los niveles Inicial, Primaria y Secundaria."
                )

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

                                        current_profesor_activo = _profesor_edit_estado_activo(
                                            profesor_edit_row.get("estado")
                                        )
                                        current_profesor_level_ids_set = {
                                            int(item)
                                            for item in profesor_edit_level_ids
                                            if _safe_int(item) is not None
                                        }
                                        status_target_level_ids = sorted(
                                            {
                                                int(item)
                                                for item in (
                                                    list(current_profesor_level_ids_set)
                                                    + [int(selected_profesor_level_id)]
                                                )
                                                if _safe_int(item) is not None
                                            }
                                        )
                                        status_col = st.container()
                                        with status_col:
                                            estado_button_key = (
                                                "profesores_edit_estado_toggle_"
                                                f"{int(selected_profesor_persona_id)}"
                                            )
                                            estado_color = (
                                                "#138a52"
                                                if current_profesor_activo
                                                else "#c62828"
                                            )
                                            st.markdown(
                                                """
                                                <style>
                                                div.st-key-__ESTADO_KEY__ button {
                                                    padding: 0 !important;
                                                    min-height: 1.55rem !important;
                                                    border: none !important;
                                                    background: transparent !important;
                                                    box-shadow: none !important;
                                                    color: __ESTADO_COLOR__ !important;
                                                    font-weight: 700 !important;
                                                    justify-content: flex-start !important;
                                                }
                                                div.st-key-__ESTADO_KEY__ button:hover,
                                                div.st-key-__ESTADO_KEY__ button:focus,
                                                div.st-key-__ESTADO_KEY__ button:active {
                                                    border: none !important;
                                                    background: transparent !important;
                                                    box-shadow: none !important;
                                                    color: __ESTADO_COLOR__ !important;
                                                }
                                                div.st-key-__ESTADO_KEY__ button p {
                                                    color: __ESTADO_COLOR__ !important;
                                                    margin: 0 !important;
                                                    line-height: 1.1 !important;
                                                }
                                                </style>
                                                """
                                                .replace("__ESTADO_KEY__", estado_button_key)
                                                .replace("__ESTADO_COLOR__", estado_color),
                                                unsafe_allow_html=True,
                                            )
                                            status_click_col_a, status_click_col_b = (
                                                st.columns([0.045, 0.18], gap="small")
                                            )
                                            status_click_col_a.markdown(
                                                _profesor_edit_estado_dot_html(
                                                    current_profesor_activo
                                                ),
                                                unsafe_allow_html=True,
                                            )
                                            toggle_profesor_estado = (
                                                status_click_col_b.button(
                                                    "Activo"
                                                    if current_profesor_activo
                                                    else "Inactivo",
                                                    key=estado_button_key,
                                                    type="tertiary",
                                                    use_container_width=False,
                                                )
                                            )
                                            if toggle_profesor_estado:
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

                                                target_profesor_activo = not bool(
                                                    current_profesor_activo
                                                )
                                                estado_summary, _estado_errors = _update_profesor_edit_estado_web(
                                                    token=token,
                                                    colegio_id=int(
                                                        colegio_id_int
                                                    ),
                                                    empresa_id=DEFAULT_EMPRESA_ID,
                                                    ciclo_id=int(ciclo_id),
                                                    persona_id=int(
                                                        selected_profesor_persona_id
                                                    ),
                                                    nivel_ids=status_target_level_ids,
                                                    activo=bool(
                                                        target_profesor_activo
                                                    ),
                                                    timeout=int(timeout),
                                                )

                                                refresh_warning = ""
                                                try:
                                                    profesores_edit_rows_refresh, profesores_edit_summary_refresh, profesores_edit_errors_refresh = listar_profesores_filters_data(
                                                        token=token,
                                                        colegio_id=int(
                                                            colegio_id_int
                                                        ),
                                                        empresa_id=DEFAULT_EMPRESA_ID,
                                                        ciclo_id=int(ciclo_id),
                                                        timeout=int(timeout),
                                                    )
                                                except Exception as exc:  # pragma: no cover - UI
                                                    refresh_warning = (
                                                        f"No se pudo refrescar la lista: {exc}"
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

                                                if (
                                                    int(
                                                        estado_summary.get(
                                                            "errores_api", 0
                                                        )
                                                    )
                                                    > 0
                                                ):
                                                    st.session_state[
                                                        "profesores_edit_notice"
                                                    ] = {
                                                        "type": "warning",
                                                        "message": "No se pudo actualizar el estado en todos los niveles.",
                                                    }
                                                elif refresh_warning:
                                                    st.session_state[
                                                        "profesores_edit_notice"
                                                    ] = {
                                                        "type": "warning",
                                                        "message": refresh_warning,
                                                    }
                                                else:
                                                    st.session_state.pop(
                                                        "profesores_edit_notice",
                                                        None,
                                                    )
                                                st.rerun()

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
                                                notice_messages = [
                                                    "Datos del docente actualizados."
                                                ]
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
                                                        notice_messages = [
                                                            "Datos base actualizados, pero el login no es valido: {msg}".format(
                                                                msg=login_msg
                                                                or "sin detalle"
                                                            )
                                                        ]
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
                                                            notice_messages = [
                                                                "Datos base actualizados, pero no se pudo actualizar login/password: {msg}".format(
                                                                    msg=login_update_msg
                                                                    or "sin detalle"
                                                                )
                                                            ]
                                                        else:
                                                            notice_messages = [
                                                                "Docente actualizado correctamente."
                                                            ]

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
                                                "message": "{message}{warning}".format(
                                                    message=" | ".join(
                                                        part
                                                        for part in notice_messages
                                                        if str(part).strip()
                                                    ),
                                                    warning=(
                                                        f" {refresh_warning}"
                                                        if refresh_warning
                                                        else ""
                                                    ),
                                                ).strip(),
                                            }
                                            st.rerun()

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
                elif st.session_state.get("profesores_edit_colegio_id"):
                    st.warning("No se encontraron docentes para este colegio.")
            if False and profesores_crud_view == "base":
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
            if False and profesores_crud_view == "asignar":
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
    alumnos_nav_current = str(st.session_state.get("alumnos_crud_nav") or "").strip()
    if alumnos_nav_current == "mover":
        st.session_state["alumnos_crud_nav"] = "editar"
    elif alumnos_nav_current in {"otros", "buscar_login"}:
        st.session_state["alumnos_crud_nav"] = "comparar"
    alumnos_nav_col, alumnos_body_col = st.columns([1.15, 4.85], gap="large")
    with alumnos_nav_col:
        alumnos_crud_view = _render_crud_menu(
            "Funciones de alumnos",
            [
                ("comparar", "Comparar", "Compara BD vs actualizada y descarga plantilla"),
                ("editar", "Editar", "Edita datos y mueve de seccion"),
                ("crear", "Crear", "Crea alumno nuevo"),
                ("payments", "Actualizar users Payments", "Prepara y aplica cambios de users payments"),
            ],
            state_key="alumnos_crud_nav",
        )
    with alumnos_body_col:
        loaded_niveles = st.session_state.get("alumnos_manual_move_niveles") or []
        loaded_colegio_id = _safe_int(st.session_state.get("alumnos_manual_move_colegio_id"))
        current_colegio_id = _safe_int(colegio_id_raw)
        if alumnos_crud_view == "payments":
            _render_users_payments_section(
                colegio_id_raw=colegio_id_raw,
                empresa_id=int(empresa_id),
                ciclo_id=int(GESTION_ESCOLAR_CICLO_ID_DEFAULT),
                timeout=int(timeout),
            )
        if alumnos_crud_view == "otros":
            current_otros_colegio_id = _safe_int(colegio_id_raw)
            cached_censo_colegio_id = _safe_int(
                st.session_state.get("alumnos_censo_activos_colegio_id")
            )
            if (
                current_otros_colegio_id is not None
                and cached_censo_colegio_id is not None
                and current_otros_colegio_id != cached_censo_colegio_id
            ):
                for state_key in (
                    "alumnos_censo_activos_rows",
                    "alumnos_censo_activos_export_rows",
                    "alumnos_censo_activos_errors",
                    "alumnos_censo_activos_colegio_id",
                ):
                    st.session_state.pop(state_key, None)
            cached_censo_profesores_colegio_id = _safe_int(
                st.session_state.get("profesores_censo_activos_colegio_id")
            )
            if (
                current_otros_colegio_id is not None
                and cached_censo_profesores_colegio_id is not None
                and current_otros_colegio_id != cached_censo_profesores_colegio_id
            ):
                for state_key in (
                    "profesores_censo_activos_rows",
                    "profesores_censo_activos_errors",
                    "profesores_censo_activos_colegio_id",
                ):
                    st.session_state.pop(state_key, None)

            censo_rows_cached = st.session_state.get("alumnos_censo_activos_rows") or []
            censo_export_rows_cached = (
                st.session_state.get("alumnos_censo_activos_export_rows") or []
            )
            censo_errors_cached = (
                st.session_state.get("alumnos_censo_activos_errors") or []
            )
            censo_display_rows = _normalize_censo_activos_export_rows(
                censo_export_rows_cached or censo_rows_cached
            )
            censo_colegio_id = _safe_int(
                st.session_state.get("alumnos_censo_activos_colegio_id")
            )
            censo_profesores_rows_cached = (
                st.session_state.get("profesores_censo_activos_rows") or []
            )
            censo_profesores_errors_cached = (
                st.session_state.get("profesores_censo_activos_errors") or []
            )
            censo_profesores_colegio_id = _safe_int(
                st.session_state.get("profesores_censo_activos_colegio_id")
            )
            censo_multi_summary_cached = (
                st.session_state.get("alumnos_censo_activos_multi_summary_rows") or []
            )
            censo_multi_errors_cached = (
                st.session_state.get("alumnos_censo_activos_multi_errors") or []
            )
            censo_multi_zip_bytes_cached = (
                st.session_state.get("alumnos_censo_activos_multi_zip_bytes") or b""
            )
            censo_multi_zip_name_cached = str(
                st.session_state.get("alumnos_censo_activos_multi_zip_name") or ""
            ).strip()

            with st.container(border=True):
                run_censo_activos = st.button(
                    "Censo de alumnos activos",
                    type="primary",
                    key="alumnos_censo_activos_load_btn",
                    use_container_width=True,
                )

                if run_censo_activos:
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
                    status_placeholder = st.empty()

                    def _on_censo_status(message: str) -> None:
                        status_placeholder.caption(str(message or "").strip())

                    censo_payload = _load_censo_activos_for_colegio(
                        token=token,
                        colegio_id=int(colegio_id_int),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                        on_status=_on_censo_status,
                    )
                    rows_activos = list(censo_payload.get("rows") or [])
                    export_rows_activos = list(censo_payload.get("export_rows") or [])
                    errors_activos = list(censo_payload.get("errors") or [])

                    st.session_state["alumnos_censo_activos_rows"] = rows_activos
                    st.session_state["alumnos_censo_activos_export_rows"] = export_rows_activos
                    st.session_state["alumnos_censo_activos_errors"] = errors_activos
                    st.session_state["alumnos_censo_activos_colegio_id"] = int(
                        colegio_id_int
                    )
                    censo_display_rows = export_rows_activos
                    censo_errors_cached = errors_activos
                    censo_colegio_id = int(colegio_id_int)
                    status_placeholder.empty()
                    st.success(
                        "Censo cargado. Activos: {total} | Errores de consulta: {errors}".format(
                            total=len(rows_activos),
                            errors=len(errors_activos),
                        )
                    )

            if censo_display_rows:
                with st.container(border=True):
                    result_col_text, result_col_rows, result_col_errors, result_col_download = st.columns(
                        [2.4, 1, 1, 1.4], gap="small"
                    )
                    with result_col_text:
                        st.markdown("**Resultado del censo**")
                        st.caption(
                            "Vista consolidada de alumnos activos lista para revisar o exportar."
                        )
                    result_col_rows.metric("Activos", len(censo_display_rows))
                    result_col_errors.metric("Errores", len(censo_errors_cached))
                    file_suffix = (
                        str(censo_colegio_id)
                        if censo_colegio_id is not None
                        else "colegio"
                    )
                    result_col_download.download_button(
                        label="Descargar Excel",
                        data=_export_censo_activos_excel(censo_display_rows),
                        file_name=f"censo_alumnos_activos_{file_suffix}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="alumnos_censo_activos_download",
                        use_container_width=True,
                    )
                    _show_dataframe(censo_display_rows, use_container_width=True)
            if censo_errors_cached:
                with st.expander(
                    f"Errores de consulta del censo ({len(censo_errors_cached)})",
                    expanded=False,
                ):
                    st.markdown(
                        "\n".join(
                            f"- {item}" for item in censo_errors_cached[:40]
                        )
                    )
                    pending = len(censo_errors_cached) - 40
                    if pending > 0:
                        st.caption(f"... y {pending} errores mas.")

            with st.container(border=True):
                run_censo_profesores_activos = st.button(
                    "Censo de profesores activos",
                    type="primary",
                    key="profesores_censo_activos_load_btn",
                    use_container_width=True,
                )

                if run_censo_profesores_activos:
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
                    status_placeholder = st.empty()

                    def _on_censo_prof_status(message: str) -> None:
                        status_placeholder.caption(str(message or "").strip())

                    censo_prof_payload = _load_censo_profesores_activos_for_colegio(
                        token=token,
                        colegio_id=int(colegio_id_int),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                        on_status=_on_censo_prof_status,
                    )
                    censo_profesores_rows_cached = list(
                        censo_prof_payload.get("export_rows") or []
                    )
                    censo_profesores_errors_cached = list(
                        censo_prof_payload.get("errors") or []
                    )
                    st.session_state["profesores_censo_activos_rows"] = (
                        censo_profesores_rows_cached
                    )
                    st.session_state["profesores_censo_activos_errors"] = (
                        censo_profesores_errors_cached
                    )
                    st.session_state["profesores_censo_activos_colegio_id"] = int(
                        colegio_id_int
                    )
                    censo_profesores_colegio_id = int(colegio_id_int)
                    status_placeholder.empty()
                    st.success(
                        "Censo de profesores cargado. Activos: {total} | Errores de consulta: {errors}".format(
                            total=len(censo_profesores_rows_cached),
                            errors=len(censo_profesores_errors_cached),
                        )
                    )

            if censo_profesores_rows_cached:
                with st.container(border=True):
                    prof_col_text, prof_col_rows, prof_col_errors, prof_col_download = st.columns(
                        [2.4, 1, 1, 1.4], gap="small"
                    )
                    with prof_col_text:
                        st.markdown("**Resultado del censo de profesores**")
                        st.caption(
                            "Vista consolidada de profesores activos lista para revisar o exportar."
                        )
                    prof_col_rows.metric("Activos", len(censo_profesores_rows_cached))
                    prof_col_errors.metric("Errores", len(censo_profesores_errors_cached))
                    file_suffix_prof = (
                        str(censo_profesores_colegio_id)
                        if censo_profesores_colegio_id is not None
                        else "colegio"
                    )
                    prof_col_download.download_button(
                        label="Descargar Excel",
                        data=_export_censo_profesores_activos_excel(
                            censo_profesores_rows_cached
                        ),
                        file_name=f"censo_profesores_activos_{file_suffix_prof}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="profesores_censo_activos_download",
                        use_container_width=True,
                    )
                    _show_dataframe(
                        censo_profesores_rows_cached,
                        use_container_width=True,
                    )
            if censo_profesores_errors_cached:
                with st.expander(
                    f"Errores de consulta del censo de profesores ({len(censo_profesores_errors_cached)})",
                    expanded=False,
                ):
                    st.markdown(
                        "\n".join(
                            f"- {item}" for item in censo_profesores_errors_cached[:40]
                        )
                    )
                    pending_prof = len(censo_profesores_errors_cached) - 40
                    if pending_prof > 0:
                        st.caption(f"... y {pending_prof} errores mas.")

            if False:
                st.markdown("**Censo activos por varios colegios**")
                st.caption(
                    "Busca por nombre y selecciona varios colegios. Se generara un ZIP con una carpeta por colegio y dos archivos: alumnos y profesores activos."
                )
                colegio_rows_multi = st.session_state.get("shared_colegios_rows") or []
                colegio_error_multi = str(
                    st.session_state.get("shared_colegios_error") or ""
                ).strip()
                row_by_id_multi = {
                    int(row["colegio_id"]): row
                    for row in colegio_rows_multi
                    if row.get("colegio_id") is not None
                }
                selected_multi_key = "alumnos_censo_activos_multi_ids"
                if selected_multi_key not in st.session_state:
                    default_multi_ids: List[int] = []
                    if (
                        current_otros_colegio_id is not None
                        and current_otros_colegio_id in row_by_id_multi
                    ):
                        default_multi_ids = [int(current_otros_colegio_id)]
                    st.session_state[selected_multi_key] = default_multi_ids
                st.multiselect(
                    "Colegios",
                    options=sorted(row_by_id_multi.keys(), key=lambda value: str((row_by_id_multi.get(int(value)) or {}).get("label") or "")),
                    key=selected_multi_key,
                    format_func=lambda value: str(
                        (row_by_id_multi.get(int(value)) or {}).get("label")
                        or f"Colegio {int(value)}"
                    ),
                    placeholder=(
                        "Busca y selecciona uno o varios colegios"
                        if row_by_id_multi
                        else "Guarda un token para cargar colegios"
                    ),
                    disabled=not bool(row_by_id_multi),
                )
                if colegio_error_multi:
                    st.caption(
                        f"No se pudo cargar la lista de colegios: {colegio_error_multi}"
                    )
                elif not row_by_id_multi:
                    st.caption(
                        "Guarda un token en Configuracion global para cargar y buscar colegios."
                    )
                run_censo_multi = st.button(
                    "Generar ZIP por colegios",
                    key="alumnos_censo_activos_multi_btn",
                    use_container_width=True,
                    disabled=not bool(row_by_id_multi),
                )
                if run_censo_multi:
                    token = _get_shared_token()
                    if not token:
                        st.error(
                            "Falta el token. Configura el token global o PEGASUS_TOKEN."
                        )
                        st.stop()
                    colegio_ids_multi_raw = st.session_state.get(selected_multi_key) or []
                    colegio_ids_multi = [
                        int(value)
                        for value in colegio_ids_multi_raw
                        if _safe_int(value) is not None
                    ]
                    if not colegio_ids_multi:
                        st.error("Selecciona al menos un colegio.")
                        st.stop()

                    status_placeholder = st.empty()
                    total_colegios_multi = len(colegio_ids_multi)
                    summary_rows_multi: List[Dict[str, object]] = []
                    errors_multi: List[str] = []
                    zip_buffer = BytesIO()
                    zip_name_multi = (
                        f"censo_alumnos_activos_colegios_{date.today().isoformat()}.zip"
                    )
                    zip_root_folder = _sanitize_zip_component(
                        f"censo_alumnos_activos_colegios_{date.today().isoformat()}",
                        "censo_alumnos_activos_colegios",
                    )

                    with ZipFile(zip_buffer, "w", ZIP_DEFLATED) as zip_file:
                        for idx, colegio_id_multi in enumerate(colegio_ids_multi, start=1):
                            colegio_base_name = _get_colegio_export_base_name(
                                int(colegio_id_multi)
                            )

                            def _on_multi_status(
                                message: str,
                                current_idx: int = idx,
                                current_total: int = total_colegios_multi,
                                colegio_name: str = colegio_base_name,
                            ) -> None:
                                status_placeholder.caption(
                                    "[{idx}/{total}] {colegio}: {message}".format(
                                        idx=current_idx,
                                        total=current_total,
                                        colegio=colegio_name,
                                        message=str(message or "").strip(),
                                    )
                                )

                            try:
                                payload_multi = _load_censo_activos_for_colegio(
                                    token=token,
                                    colegio_id=int(colegio_id_multi),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                    on_status=_on_multi_status,
                                )
                                export_rows_multi = list(
                                    payload_multi.get("export_rows") or []
                                )
                                errors_colegio_multi = list(
                                    payload_multi.get("errors") or []
                                )
                                payload_profesores_multi = (
                                    _load_censo_profesores_activos_for_colegio(
                                        token=token,
                                        colegio_id=int(colegio_id_multi),
                                        empresa_id=int(empresa_id),
                                        ciclo_id=int(ciclo_id),
                                        timeout=int(timeout),
                                        on_status=lambda message, idx_now=idx, total_now=total_colegios_multi, colegio_name=colegio_base_name: status_placeholder.caption(
                                            "[{idx}/{total}] {colegio}: {message}".format(
                                                idx=idx_now,
                                                total=total_now,
                                                colegio=colegio_name,
                                                message=str(message or "").strip(),
                                            )
                                        ),
                                    )
                                )
                                export_rows_profesores_multi = list(
                                    payload_profesores_multi.get("export_rows") or []
                                )
                                errors_profesores_multi = list(
                                    payload_profesores_multi.get("errors") or []
                                )
                                folder_name = colegio_base_name
                                alumnos_file_name = (
                                    f"censo_alumnos_activos_{colegio_base_name}.xlsx"
                                )
                                profesores_file_name = (
                                    f"censo_profesores_activos_{colegio_base_name}.xlsx"
                                )
                                zip_path_alumnos = (
                                    f"{zip_root_folder}/{folder_name}/{alumnos_file_name}"
                                )
                                zip_path_profesores = (
                                    f"{zip_root_folder}/{folder_name}/{profesores_file_name}"
                                )
                                zip_file.writestr(
                                    zip_path_alumnos,
                                    _export_censo_activos_excel(export_rows_multi),
                                )
                                zip_file.writestr(
                                    zip_path_profesores,
                                    _export_censo_profesores_activos_excel(
                                        export_rows_profesores_multi
                                    ),
                                )
                                summary_rows_multi.append(
                                    {
                                        "Colegio ID": int(colegio_id_multi),
                                        "Colegio": colegio_base_name,
                                        "Alumnos activos": len(export_rows_multi),
                                        "Profesores activos": len(export_rows_profesores_multi),
                                        "Errores": len(errors_colegio_multi) + len(errors_profesores_multi),
                                        "Archivo alumnos": zip_path_alumnos,
                                        "Archivo profesores": zip_path_profesores,
                                        "Estado": (
                                            "OK"
                                            if not errors_colegio_multi and not errors_profesores_multi
                                            else "OK con errores"
                                        ),
                                    }
                                )
                                errors_multi.extend(
                                    [
                                        f"Colegio {int(colegio_id_multi)}: {item}"
                                        for item in errors_colegio_multi
                                        if str(item or "").strip()
                                    ]
                                )
                                errors_multi.extend(
                                    [
                                        f"Colegio {int(colegio_id_multi)} (profesores): {item}"
                                        for item in errors_profesores_multi
                                        if str(item or "").strip()
                                    ]
                                )
                            except Exception as exc:
                                errors_multi.append(
                                    f"Colegio {int(colegio_id_multi)}: {exc}"
                                )
                                summary_rows_multi.append(
                                    {
                                        "Colegio ID": int(colegio_id_multi),
                                        "Colegio": colegio_base_name,
                                        "Alumnos activos": 0,
                                        "Profesores activos": 0,
                                        "Errores": 1,
                                        "Archivo alumnos": "",
                                        "Archivo profesores": "",
                                        "Estado": f"ERROR: {exc}",
                                    }
                                )

                    zip_buffer.seek(0)
                    censo_multi_zip_bytes_cached = zip_buffer.getvalue()
                    censo_multi_zip_name_cached = zip_name_multi
                    censo_multi_summary_cached = summary_rows_multi
                    censo_multi_errors_cached = errors_multi
                    st.session_state["alumnos_censo_activos_multi_zip_bytes"] = (
                        censo_multi_zip_bytes_cached
                    )
                    st.session_state["alumnos_censo_activos_multi_zip_name"] = (
                        censo_multi_zip_name_cached
                    )
                    st.session_state["alumnos_censo_activos_multi_summary_rows"] = (
                        censo_multi_summary_cached
                    )
                    st.session_state["alumnos_censo_activos_multi_errors"] = (
                        censo_multi_errors_cached
                    )
                    status_placeholder.empty()
                    st.success(
                        "ZIP listo. Colegios: {total} | Errores acumulados: {errors}".format(
                            total=len(summary_rows_multi),
                            errors=len(errors_multi),
                        )
                    )

            if False and censo_multi_summary_cached:
                with st.container(border=True):
                    multi_col_text, multi_col_total, multi_col_errors, multi_col_download = st.columns(
                        [2.2, 1, 1, 1.5], gap="small"
                    )
                    with multi_col_text:
                        st.markdown("**Resultado masivo por colegios**")
                        st.caption(
                            "Se genero un Excel por colegio dentro de una carpeta con el mismo nombre."
                        )
                    multi_col_total.metric("Colegios", len(censo_multi_summary_cached))
                    multi_col_errors.metric("Errores", len(censo_multi_errors_cached))
                    multi_col_download.download_button(
                        label="Descargar ZIP",
                        data=censo_multi_zip_bytes_cached,
                        file_name=(
                            censo_multi_zip_name_cached
                            or f"censo_alumnos_activos_colegios_{date.today().isoformat()}.zip"
                        ),
                        mime="application/zip",
                        key="alumnos_censo_activos_multi_download",
                        use_container_width=True,
                    )
                    _show_dataframe(censo_multi_summary_cached, use_container_width=True)
            if False and censo_multi_errors_cached:
                with st.expander(
                    f"Errores del censo masivo ({len(censo_multi_errors_cached)})",
                    expanded=False,
                ):
                    st.markdown(
                        "\n".join(
                            f"- {item}" for item in censo_multi_errors_cached[:80]
                        )
                    )
                    pending_multi = len(censo_multi_errors_cached) - 80
                    if pending_multi > 0:
                        st.caption(f"... y {pending_multi} errores mas.")

        if alumnos_crud_view == "comparar":
            current_compare_colegio_id = _safe_int(colegio_id_raw)
            cached_template_colegio_id = _safe_int(
                st.session_state.get("alumnos_plantilla_edicion_colegio_id")
            )
            if (
                current_compare_colegio_id is not None
                and cached_template_colegio_id is not None
                and current_compare_colegio_id != cached_template_colegio_id
            ):
                for state_key in (
                    "alumnos_plantilla_edicion_bytes",
                    "alumnos_plantilla_edicion_name",
                    "alumnos_plantilla_edicion_summary",
                    "alumnos_plantilla_edicion_colegio_id",
                ):
                    st.session_state.pop(state_key, None)

            plantilla_bytes_cached = (
                st.session_state.get("alumnos_plantilla_edicion_bytes") or b""
            )
            plantilla_file_name_cached = str(
                st.session_state.get("alumnos_plantilla_edicion_name") or ""
            ).strip()
            plantilla_summary_cached = (
                st.session_state.get("alumnos_plantilla_edicion_summary") or {}
            )

            with st.container(border=True):
                st.markdown("**2) Comparar Plantilla_BD vs Plantilla_Actualizada**")
                st.caption("Genera altas, match e inactivados.")
                if not (plantilla_bytes_cached and plantilla_file_name_cached):
                    token = _get_shared_token()
                    if token:
                        try:
                            colegio_id_int = _parse_colegio_id(colegio_id_raw)
                        except ValueError as exc:
                            st.warning(f"No se puede preparar la plantilla: {exc}")
                        else:
                            try:
                                with st.spinner("Preparando plantilla para descarga..."):
                                    output_bytes, summary = (
                                        descargar_plantilla_edicion_masiva(
                                            token=token,
                                            colegio_id=colegio_id_int,
                                            empresa_id=int(empresa_id),
                                            ciclo_id=int(ciclo_id),
                                            timeout=int(timeout),
                                        )
                                    )
                            except Exception as exc:  # pragma: no cover - UI
                                st.warning(f"No se pudo preparar la plantilla: {exc}")
                            else:
                                file_name = (
                                    f"plantilla_edicion_alumnos_{colegio_id_int}.xlsx"
                                )
                                st.session_state["alumnos_plantilla_edicion_bytes"] = (
                                    output_bytes
                                )
                                st.session_state["alumnos_plantilla_edicion_name"] = (
                                    file_name
                                )
                                st.session_state["alumnos_plantilla_edicion_summary"] = (
                                    dict(summary)
                                )
                                st.session_state[
                                    "alumnos_plantilla_edicion_colegio_id"
                                ] = int(colegio_id_int)
                                plantilla_bytes_cached = output_bytes
                                plantilla_file_name_cached = file_name
                                plantilla_summary_cached = dict(summary)
                    else:
                        st.caption(
                            "Configura el token global para habilitar la descarga de plantilla."
                        )

                compare_top_cols = st.columns([1.4, 2.6], gap="small")
                compare_top_cols[0].download_button(
                    label="Descargar plantilla",
                    data=plantilla_bytes_cached,
                    file_name=plantilla_file_name_cached or "plantilla_edicion_alumnos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="alumnos_plantilla_edicion_download",
                    use_container_width=True,
                    disabled=not bool(plantilla_bytes_cached and plantilla_file_name_cached),
                )
                if plantilla_bytes_cached and plantilla_file_name_cached:
                    compare_top_cols[1].caption(
                        "Plantilla lista para descargar: {total} alumno(s).".format(
                            total=int(
                                plantilla_summary_cached.get("alumnos_total", 0)
                            )
                        )
                    )

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
                        alumno_activo_raw = (
                            current_alumno_detail.get("activo")
                            if current_alumno_detail.get("activo") is not None
                            else alumno_edit_row.get("activo")
                        )
                        current_alumno_activo = _to_bool(alumno_activo_raw)
                        detail_header_cols = st.columns([7.1, 1.5, 1.4], gap="small")
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
                            status_cols = st.columns([0.28, 0.72], gap="small")
                            status_cols[0].markdown(
                                _profesor_edit_estado_dot_html(current_alumno_activo),
                                unsafe_allow_html=True,
                            )
                            toggle_alumno_estado = status_cols[1].button(
                                "Activo" if current_alumno_activo else "Inactivo",
                                key=f"alumnos_edit_estado_toggle_{int(alumno_edit_context['alumno_id'])}",
                                type="tertiary",
                                help=(
                                    "Inactivar alumno"
                                    if current_alumno_activo
                                    else "Activar alumno"
                                ),
                                use_container_width=True,
                            )
                            if toggle_alumno_estado:
                                token = _get_shared_token()
                                if not token:
                                    st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                                    st.stop()
                                try:
                                    colegio_id_int = _parse_colegio_id(colegio_id_raw)
                                except ValueError as exc:
                                    st.error(f"Error: {exc}")
                                    st.stop()

                                target_alumno_activo = not bool(current_alumno_activo)
                                with st.spinner("Actualizando estado del alumno..."):
                                    estado_ok, estado_msg = _set_alumno_activo_web(
                                        token=token,
                                        colegio_id=int(colegio_id_int),
                                        empresa_id=int(empresa_id),
                                        ciclo_id=int(ciclo_id),
                                        nivel_id=int(alumno_edit_context["nivel_id"]),
                                        grado_id=int(alumno_edit_context["grado_id"]),
                                        grupo_id=int(alumno_edit_context["grupo_id"]),
                                        alumno_id=int(alumno_edit_context["alumno_id"]),
                                        activo=1 if target_alumno_activo else 0,
                                        observaciones="",
                                        timeout=int(timeout),
                                    )
                                if not estado_ok:
                                    st.error(
                                        "No se pudo actualizar el estado del alumno: {msg}".format(
                                            msg=estado_msg or "sin detalle"
                                        )
                                    )
                                    st.stop()

                                refresh_warning = _refresh_alumnos_edit_catalog_and_detail(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                    alumno_id=int(alumno_edit_context["alumno_id"]),
                                    fallback_context=dict(alumno_edit_context),
                                )
                                st.session_state["alumnos_edit_notice"] = {
                                    "type": "warning" if refresh_warning else "success",
                                    "message": (
                                        "Alumno {estado}.{warning}".format(
                                            estado=(
                                                "activado"
                                                if target_alumno_activo
                                                else "inactivado"
                                            ),
                                            warning=refresh_warning,
                                        )
                                    ).strip(),
                                }
                                st.rerun()
                        with detail_header_cols[2]:
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
                            if login_changed:
                                login_error = _validar_login_reglas(login_txt)
                                if login_error:
                                    st.error(login_error)
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
                            if password_provided:
                                password_error = _validar_password_reglas(password_txt)
                                if password_error:
                                    st.error(password_error)
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

            create_actions_cols = st.columns([2, 1], gap="small")
            run_create_clear = create_actions_cols[1].button(
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

            create_autoload_error = ""
            if current_colegio_id is not None and (
                not create_niveles
                or create_colegio_id is None
                or int(create_colegio_id) != int(current_colegio_id)
            ):
                token = _get_shared_token()
                if not token:
                    create_autoload_error = (
                        "Falta el token. Configura el token global o PEGASUS_TOKEN."
                    )
                else:
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        create_autoload_error = f"Error: {exc}"
                    else:
                        try:
                            with st.spinner(
                                "Cargando niveles, grados y secciones para crear alumno..."
                            ):
                                niveles_create = _fetch_niveles_grados_grupos_censo(
                                    token=token,
                                    colegio_id=int(colegio_id_int),
                                    empresa_id=int(empresa_id),
                                    ciclo_id=int(ciclo_id),
                                    timeout=int(timeout),
                                )
                        except Exception as exc:  # pragma: no cover - UI
                            create_autoload_error = f"Error cargando opciones: {exc}"
                        else:
                            st.session_state["alumnos_create_niveles"] = (
                                niveles_create
                            )
                            st.session_state["alumnos_create_colegio_id"] = int(
                                colegio_id_int
                            )
                            create_niveles = niveles_create
                            create_colegio_id = int(colegio_id_int)

            if create_autoload_error:
                st.error(create_autoload_error)

            if (
                create_colegio_id is not None
                and current_colegio_id is not None
                and int(create_colegio_id) != int(current_colegio_id)
            ):
                st.warning(
                    "El colegio global cambio. Se volveran a cargar las opciones automaticamente."
                )
            elif not create_niveles:
                st.caption("No hay opciones disponibles para crear en este colegio.")
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
                create_progress_placeholder = st.empty()
                create_status_placeholder = st.empty()

                if create_submit:
                    create_progress_bar = create_progress_placeholder.progress(
                        0, text="Preparando alta de alumno..."
                    )

                    def _set_create_status(progress_value: int, message: str) -> None:
                        percent = max(0, min(100, int(progress_value)))
                        text = str(message or "").strip()
                        create_progress_bar.progress(percent, text=text or None)
                        if text:
                            create_status_placeholder.caption(text)

                    token = _get_shared_token()
                    if not token:
                        st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                        st.stop()
                    try:
                        colegio_id_int = _parse_colegio_id(colegio_id_raw)
                    except ValueError as exc:
                        st.error(f"Error: {exc}")
                        st.stop()

                    _set_create_status(10, "Validando destino y datos obligatorios...")
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

                    _set_create_status(24, "Revisando si el alumno ya existe en el colegio...")
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

                    _set_create_status(38, "Validando identificador del alumno...")
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

                    _set_create_status(50, "Validando login del alumno...")
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

                    _set_create_status(68, "Creando alumno en censo...")
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

                    create_notice_type = "success"
                    notice_parts: List[str] = []

                    _set_create_status(82, "Actualizando login y password del alumno...")
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
                        create_notice_type = "warning"
                        notice_parts.append(
                            "Login/password no actualizado: {msg}".format(
                                msg=update_msg or "sin detalle"
                            )
                        )

                    def _on_create_assign_status(message: str) -> None:
                        _set_create_status(92, message)

                    def _on_create_assign_progress(
                        current: int, total: int, message: str
                    ) -> None:
                        total_safe = max(int(total or 0), 1)
                        current_safe = max(0, min(int(current or 0), total_safe))
                        mapped_progress = 92 + int((current_safe / total_safe) * 6)
                        _set_create_status(mapped_progress, message)

                    assign_summary: Dict[str, object] = {
                        "target_classes_total": 0,
                        "assigned_ok": 0,
                        "assigned_error": 0,
                        "assigned_errors": [],
                    }
                    try:
                        assign_summary = _assign_alumno_to_matching_classes_for_context(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                            alumno_id=int(alumno_id_created),
                            nivel_id=int(selected_nivel_id),
                            grado_id=int(selected_grado_id),
                            grupo_id=int(destino_payload.get("grupo_id") or 0),
                            seccion=str(destino_payload.get("seccion") or "").strip(),
                            on_status=_on_create_assign_status,
                            on_progress=_on_create_assign_progress,
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        create_notice_type = "warning"
                        notice_parts.append(
                            "No se pudieron asignar las clases: {msg}".format(
                                msg=str(exc).strip() or "sin detalle"
                            )
                        )
                    else:
                        if int(assign_summary.get("assigned_error", 0)) > 0:
                            create_notice_type = "warning"
                            notice_parts.append(
                                "Clases: OK {ok} | ERROR {error}".format(
                                    ok=int(assign_summary.get("assigned_ok", 0)),
                                    error=int(assign_summary.get("assigned_error", 0)),
                                )
                            )
                        elif int(assign_summary.get("target_classes_total", 0)) > 0:
                            notice_parts.append(
                                "Clases asignadas: {ok}".format(
                                    ok=int(assign_summary.get("assigned_ok", 0))
                                )
                            )
                        else:
                            notice_parts.append(
                                "Sin clases destino para ese grado/seccion."
                            )

                    _set_create_status(100, "Proceso completado.")

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
                        "type": create_notice_type,
                        "message": (
                            "Alumno creado: {nombre} | {dni} | {login}".format(
                                nombre=str(created_row.get("nombre_completo") or nombre_txt).strip(),
                                dni=dni_txt,
                                login=login_txt,
                            )
                            + (
                                " | " + " | ".join(
                                    part for part in notice_parts if str(part).strip()
                                )
                                if notice_parts
                                else ""
                            )
                        ).strip(),
                    }
                    st.rerun()

with tab_otras_funcionalidades:
    _render_otras_funcionalidades_view()

