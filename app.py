import os
import re
import tempfile
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
import streamlit as st

from santillana_format.alumnos import (
    DEFAULT_CICLO_ID as ALUMNOS_CICLO_ID_DEFAULT,
    DEFAULT_EMPRESA_ID,
    descargar_plantilla_edicion_masiva,
)
from santillana_format.alumnos_compare import comparar_plantillas
from santillana_format.processor import (
    CODE_COLUMN_NAME,
    OUTPUT_FILENAME,
    SHEET_NAME,
    process_excel,
)
from santillana_format.jira_focus_web import render_jira_focus_web
from santillana_format.profesores import (
    DEFAULT_CICLO_ID as PROFESORES_CICLO_ID_DEFAULT,
    export_profesores_excel,
    listar_profesores_data,
)
from santillana_format.profesores_clases import asignar_profesores_clases
from santillana_format.profesores_password import actualizar_passwords_docentes


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
CENSO_NIVELES_GRADOS_GRUPOS_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/alumnos/nivelesGradosGrupos"
)
CENSO_PLANTILLA_EDICION_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/descargarPlantillaEdicionMasiva"
)
GESTION_ESCOLAR_CICLO_ID_DEFAULT = 207
RICHMONDSTUDIO_USERS_URL = "https://richmondstudio.global/api/users"
RICHMONDSTUDIO_GROUPS_URL = "https://richmondstudio.global/api/groups"
RESTRICTED_SECTIONS_PASSWORD = "Palabr@leatoria123!"


def _restricted_sections_unlocked() -> bool:
    return bool(st.session_state.get("restricted_sections_unlocked", False))


@st.dialog("Acceso restringido", width="small")
def _show_restricted_unlock_dialog() -> None:
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
    st.markdown(
        """
        <style>
        .restricted-blur-box{
            filter: blur(3px);
            opacity: 0.45;
            border: 1px dashed #9aa0a6;
            border-radius: 12px;
            padding: 18px;
            margin: 8px 0 14px 0;
            background: linear-gradient(135deg,#f8f9fa,#eef2f6);
            text-align:center;
        }
        .restricted-blur-box small{
            display:block;
            margin-top:6px;
        }
        </style>
        <div class="restricted-blur-box">
            <strong>Funcion bloqueada</strong>
            <small>Acceso restringido por contrasena.</small>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(f"{section_name} requiere desbloqueo.")
    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        if st.button(
            "Desbloquear funciones restringidas",
            key=f"restricted_unlock_open_{key_suffix}",
            use_container_width=True,
        ):
            _show_restricted_unlock_dialog()


def _inject_professional_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@500&display=swap');

        :root{
            --app-bg-a: #f4f6f9;
            --app-bg-b: #edf1f6;
            --surface: #ffffff;
            --surface-strong: #ffffff;
            --text-strong: #0f1722;
            --text-muted: #1f3042;
            --border-soft: #ccd7e4;
            --accent: #0f4f7a;
            --accent-strong: #0b3f62;
            --shadow-soft: 0 4px 14px rgba(15, 23, 34, 0.08);
            --radius-md: 10px;
            --radius-lg: 12px;
        }

        html, body, [class*="css"]{
            font-family: "Manrope", "Segoe UI", sans-serif;
            color: var(--text-strong);
        }
        code, pre, .stCode, .stJson{
            font-family: "IBM Plex Mono", Consolas, monospace !important;
        }

        [data-testid="stAppViewContainer"]{
            background: linear-gradient(180deg, var(--app-bg-a) 0%, var(--app-bg-b) 100%);
        }
        [data-testid="stHeader"]{
            background: transparent;
        }
        .main .block-container{
            max-width: 1500px;
            padding-top: 0.6rem;
            padding-bottom: 1.1rem;
            padding-left: 0.75rem;
            padding-right: 0.75rem;
        }
        div[data-testid="stVerticalBlock"]{
            gap: 0.35rem;
        }

        h1, h2, h3, h4{
            color: var(--text-strong);
            letter-spacing: -0.01em;
        }
        h1{
            font-size: 1.72rem;
            font-weight: 800;
            margin-bottom: 0.02rem;
        }
        [data-testid="stMarkdownContainer"] p,
        [data-testid="stCaptionContainer"]{
            color: var(--text-muted);
            margin-top: 0.08rem;
            margin-bottom: 0.32rem;
            line-height: 1.25;
            font-size: 0.92rem;
        }

        .app-hero{
            border: 1px solid var(--border-soft);
            border-radius: var(--radius-lg);
            padding: 0.68rem 0.82rem 0.64rem 0.82rem;
            margin: 0.1rem 0 0.5rem 0;
            background: #ffffff;
            box-shadow: var(--shadow-soft);
        }
        .app-hero-eyebrow{
            font-size: 0.76rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #34627d;
            margin-bottom: 0.35rem;
        }
        .app-hero-title{
            font-size: 1.56rem;
            line-height: 1.15;
            font-weight: 800;
            color: var(--text-strong);
            margin: 0;
        }
        .app-hero-subtitle{
            margin: 0.22rem 0 0 0;
            color: var(--text-muted);
            font-size: 0.9rem;
        }

        div[data-testid="stVerticalBlockBorderWrapper"]{
            border: 1px solid var(--border-soft);
            border-radius: var(--radius-lg);
            background: var(--surface);
            box-shadow: var(--shadow-soft);
        }
        div[data-testid="stVerticalBlockBorderWrapper"] > div{
            padding: 0.14rem 0.16rem 0.08rem 0.16rem;
        }

        .stTabs [data-baseweb="tab-list"]{
            gap: 0.2rem;
            background: #ffffff;
            border: 1px solid var(--border-soft);
            border-radius: var(--radius-md);
            padding: 0.16rem;
        }
        .stTabs [data-baseweb="tab"]{
            height: 33px;
            border-radius: 8px;
            padding: 0 0.72rem;
            font-weight: 700;
            color: #1d3348;
            font-size: 0.88rem;
        }
        .stTabs [aria-selected="true"]{
            background: linear-gradient(180deg, var(--accent) 0%, var(--accent-strong) 100%);
            color: #ffffff !important;
            box-shadow: 0 2px 8px rgba(15, 79, 122, 0.24);
        }

        div[data-testid="stRadio"] > div{
            gap: 0.32rem;
            width: fit-content;
            background: rgba(255,255,255,0.74);
            border: 1px solid var(--border-soft);
            border-radius: 9px;
            padding: 0.2rem;
        }
        div[data-testid="stRadio"] label{
            border: 1px solid transparent;
            border-radius: 7px;
            padding: 0.12rem 0.6rem;
            background: transparent;
            font-weight: 700;
            color: #1e3348;
            font-size: 0.86rem;
        }
        div[data-testid="stRadio"] label:has(input:checked){
            color: #ffffff;
            background: linear-gradient(180deg, var(--accent) 0%, var(--accent-strong) 100%);
            box-shadow: 0 2px 8px rgba(15, 79, 122, 0.24);
        }

        .stButton > button,
        .stDownloadButton > button{
            border-radius: 8px;
            border: 1px solid #9eb0c2;
            background: linear-gradient(180deg, #ffffff 0%, #f5f8fb 100%);
            color: #15283d;
            font-weight: 700;
            min-height: 2.12rem;
            padding-top: 0.24rem;
            padding-bottom: 0.24rem;
            transition: all 120ms ease;
        }
        .stButton > button:hover,
        .stDownloadButton > button:hover{
            border-color: #7a9ab3;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(23, 50, 77, 0.11);
        }
        .stButton > button[kind="primary"],
        .stDownloadButton > button[kind="primary"]{
            border: 1px solid var(--accent-strong);
            background: linear-gradient(180deg, var(--accent) 0%, var(--accent-strong) 100%);
            color: #ffffff;
        }

        .stTextInput input,
        .stTextArea textarea,
        .stNumberInput input{
            border-radius: 8px !important;
            border: 1px solid #b7c8d9 !important;
            background: rgba(255,255,255,0.98) !important;
        }
        .stTextArea textarea{
            min-height: 82px;
        }
        div[data-baseweb="select"] > div{
            border-radius: 8px !important;
            border: 1px solid #b7c8d9 !important;
            background: rgba(255,255,255,0.98) !important;
        }

        div[data-testid="stDataFrame"]{
            border: 1px solid var(--border-soft);
            border-radius: 12px;
            overflow: hidden;
            background: #ffffff;
        }

        .stAlert{
            border-radius: 12px;
            border: 1px solid var(--border-soft);
        }
        div[data-testid="stExpander"]{
            border: 1px solid var(--border-soft);
            border-radius: 12px;
            background: rgba(255,255,255,0.90);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


st.set_page_config(page_title="Generador de Plantilla", layout="wide")
_inject_professional_theme()
st.markdown("**Menu principal**")
menu_option = st.radio(
    "Menu",
    ["Procesos Pegasus", "Jira Focus Web"],
    horizontal=True,
    label_visibility="collapsed",
    key="main_top_menu",
)
if menu_option == "Jira Focus Web":
    if not _restricted_sections_unlocked():
        _render_restricted_blur("Jira Focus Web", "jira_web")
        st.stop()
    render_jira_focus_web(height=1400)
    st.stop()
st.markdown(
    """
    <section class="app-hero">
      <div class="app-hero-eyebrow">Panel Operativo</div>
      <h1 class="app-hero-title">Procesos Pegasus</h1>
      <p class="app-hero-subtitle">
        Gestion integrada de clases, profesores y alumnos con ejecucion directa desde web.
      </p>
    </section>
    """,
    unsafe_allow_html=True,
)
st.markdown("**Configuracion global**")
global_col_token, global_col_colegio = st.columns([2.3, 1.1])
with global_col_token:
    st.text_input(
        "Token (sin Bearer)",
        type="password",
        key="shared_pegasus_token",
        help="Se usa en todas las funciones. Si queda vacio, toma PEGASUS_TOKEN.",
    )
with global_col_colegio:
    st.text_input(
        "Colegio Clave (global)",
        key="shared_colegio_id",
        placeholder="2326",
        help="Se reutiliza en las funciones que requieren colegio.",
    )
tab_crud_clases, tab_crud_profesores, tab_crud_alumnos = st.tabs(
    [
        "CRUD Clases",
        "CRUD Profesores",
        "CRUD Alumnos",
    ]
)


def _clean_token(token: str) -> str:
    token = token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token


def _get_shared_token() -> str:
    token = _clean_token(str(st.session_state.get("shared_pegasus_token", "")))
    if token:
        return token
    return _clean_token(os.environ.get("PEGASUS_TOKEN", ""))


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


def _fetch_richmondstudio_groups(token: str, timeout: int = 30) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    next_url = RICHMONDSTUDIO_GROUPS_URL
    next_params: Optional[Dict[str, object]] = None
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

def _parse_colegio_id(raw: object, field_name: str = "Colegio Clave") -> int:
    text = str(raw or "").strip()
    if not text:
        raise ValueError(f"{field_name} es obligatorio.")
    compact = re.sub(r"\s+", "", text)
    if not compact.isdigit():
        raise ValueError(
            f"{field_name} inválido: '{text}'. Usa un ID numérico (ej: 2326)."
        )
    value = int(compact)
    if value <= 0:
        raise ValueError(f"{field_name} inválido: '{text}'. Debe ser mayor a 0.")
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
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta inválida"
        raise RuntimeError(message or "Respuesta inválida")

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
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta inválida"
        raise RuntimeError(message or "Respuesta inválida")

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
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta inválida"
        raise RuntimeError(message or "Respuesta inválida")

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
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta inválida"
        raise RuntimeError(message or "Respuesta inválida")

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
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta inválida"
        raise RuntimeError(message or "Respuesta inválida")

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
            message = payload.get("message") or "Respuesta inválida"
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
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta inv�f¡lida"
        raise RuntimeError(message or "Respuesta inv�f¡lida")


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

    if nivel_id is None or grado_id is None:
        return None

    clase_nombre = str(item.get("geClase") or item.get("geClaseClave") or "")
    nivel_nombre = str(nivel.get("nivel") or "")
    grado_nombre = str(grado.get("grado") or "")
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


with tab_crud_clases:
    if not _restricted_sections_unlocked():
        _render_restricted_blur("CRUD Clases", "clases_1")
    else:
        st.subheader("CRUD Clases")
        st.markdown("**1) Crear clases**")
        st.caption("Carga Excel, codigo CRM y secciones.")
        with st.expander("Opciones de entrada", expanded=True):
            uploaded_excel = st.file_uploader(
                "Excel de entrada",
                type=["xlsx"],
                help="Ejemplo: PreOnboarding_Detalle_20251212.xlsx",
            )
            col1, col2 = st.columns(2)
            codigo = col1.text_input("Código (CRM)", placeholder="00001053")
            columna_codigo = col2.text_input(
                "Columna de código",
                value=CODE_COLUMN_NAME,
                help="Nombre de la columna donde buscar el código",
            )
            hoja = col1.text_input("Hoja a leer", value=SHEET_NAME, help="Nombre de la hoja")
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
                st.error("Ingresa un código.")
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
                        columna_codigo=columna_codigo,
                        hoja=hoja,
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
    
    
with tab_crud_profesores:
    if not _restricted_sections_unlocked():
        _render_restricted_blur("CRUD Profesores", "profesores")
    else:
        st.subheader("CRUD Profesores")
        st.caption("Flujo: genera base, luego simula y aplica asignaciones.")
        st.caption("Usando el token global configurado arriba.")
        colegio_id_raw = str(st.session_state.get("shared_colegio_id", "")).strip()
        ciclo_id = PROFESORES_CICLO_ID_DEFAULT
        timeout = 30
    
        with st.container(border=True):
            st.markdown("**1) Generar Excel base de profesores**")
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
    
        st.subheader("2) Asignar profesores a clases")
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
                "que no estén en Profesores_clases."
            ),
        )
        remove_missing = col_proc2.checkbox(
            "Eliminar profesores que no están en el Excel (solo clases evaluadas)",
            value=False,
            key="profesores_remove",
            disabled=not do_clases,
        )
        if inactivar_no_en_clases and do_estado:
            st.warning(
                "Se inactivarán por Estado los IDs que no aparezcan en Profesores_clases."
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
            help="Nombre de la hoja. Si lo dejas en blanco se intentará usar Profesores_clases.",
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
                st.success("Resumen de ejecución")
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
                        "Log de ejecución",
                        value="\n".join(display_logs),
                        height=300,
                    )
            else:
                st.success("Listo. Solo se procesaron passwords.")
    
with tab_crud_alumnos:
    st.subheader("CRUD Alumnos")
    st.caption("Funciones principales de alumnos en tarjetas.")
    crud_col_left, crud_col_right = st.columns(2, gap="large")
    with crud_col_left:
        with st.container(border=True):
            st.markdown("**1) Plantilla de alumnos registrados**")
            st.caption("Descarga la plantilla de edicion masiva.")
            colegio_id_raw = str(
                st.session_state.get("shared_colegio_id", "")
                or st.session_state.get("alumnos_colegio_text", "")
            ).strip()
            if colegio_id_raw:
                st.session_state["alumnos_colegio_text"] = colegio_id_raw
            ciclo_id = ALUMNOS_CICLO_ID_DEFAULT
            empresa_id = DEFAULT_EMPRESA_ID
            timeout = 30
    
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
    
    with crud_col_right:
        with st.container(border=True):
            st.markdown("**2) Comparar Plantilla_BD vs Plantilla_Actualizada**")
            st.caption("Genera altas, match e inactivados.")
            uploaded_compare = st.file_uploader(
                "Archivo .xlsx",
                type=["xlsx"],
                key="alumnos_compare_excel",
            )
            if st.button("Generar resultado", type="primary", key="alumnos_compare"):
                if not uploaded_compare:
                    st.error("Sube un Excel .xlsx con Plantilla_BD y Plantilla_Actualizada.")
                    st.stop()
                suffix = Path(uploaded_compare.name).suffix or ".xlsx"
                tmp_path = None
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                        tmp.write(uploaded_compare.read())
                        tmp_path = Path(tmp.name)
                    output_bytes, summary = comparar_plantillas(excel_path=tmp_path)
                except Exception as exc:  # pragma: no cover - UI
                    st.error(f"Error: {exc}")
                    st.stop()
                finally:
                    if tmp_path:
                        try:
                            tmp_path.unlink()
                        except OSError:
                            pass
    
                st.success(
                    "Listo. Base: {base_total}, Actualizada: {actualizados_total}, "
                    "Match NUIP: {nuip_match}, Nuevos: {nuevos_total}, "
                    "Inactivados: {inactivados_total}.".format(**summary)
                )
                download_name = f"alumnos_resultados_{Path(uploaded_compare.name).stem}.xlsx"
                st.download_button(
                    label="Descargar alumnos_resultados",
                    data=output_bytes,
                    file_name=download_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

    with st.container(border=True):
        st.markdown("**3) EXCEL RS**")
        st.caption(
            "Richmond Studio: CLASS NAME, CLASS CODE, STUDENT NAME, IDENTIFIER. "
            "Solo roles student/teacher."
        )
        rs_token_raw = st.text_input(
            "Bearer token RS",
            type="password",
            key="rs_bearer_token",
            placeholder="password",
        )
        run_rs_excel = st.button("EXCEL RS", type="primary", key="rs_excel_generate")

        if run_rs_excel:
            rs_token = _clean_token(str(rs_token_raw or ""))
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

            allowed_roles = {"student", "teacher"}
            excluded_roles: Dict[str, int] = {}
            filtered_users: List[Dict[str, object]] = []
            for item in rs_users:
                attrs = (
                    item.get("attributes")
                    if isinstance(item.get("attributes"), dict)
                    else {}
                )
                role = str(attrs.get("role") or "").strip().lower()
                if role not in allowed_roles:
                    role_key = role or "sin_rol"
                    excluded_roles[role_key] = int(excluded_roles.get(role_key, 0)) + 1
                    continue
                filtered_users.append(item)

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

            rows_rs: List[Dict[str, str]] = []
            for item in filtered_users:
                attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
                relationships = (
                    item.get("relationships")
                    if isinstance(item.get("relationships"), dict)
                    else {}
                )
                groups_rel = (
                    relationships.get("groups")
                    if isinstance(relationships.get("groups"), dict)
                    else {}
                )
                groups_data = groups_rel.get("data") if isinstance(groups_rel.get("data"), list) else []

                first_name = str(attrs.get("firstName") or "").strip()
                last_name = str(attrs.get("lastName") or "").strip()
                student_name = " ".join(part for part in [first_name, last_name] if part).strip()
                identifier = str(attrs.get("identifier") or "").strip()

                group_ids: List[str] = []
                seen_group_ids = set()
                for rel in groups_data:
                    if not isinstance(rel, dict):
                        continue
                    group_id = str(rel.get("id") or "").strip()
                    if not group_id or group_id in seen_group_ids:
                        continue
                    seen_group_ids.add(group_id)
                    group_ids.append(group_id)

                if group_ids:
                    for group_id in group_ids:
                        group_meta = group_lookup.get(group_id) or {}
                        rows_rs.append(
                            {
                                "CLASS NAME": str(group_meta.get("class_name") or "").strip(),
                                "CLASS CODE": str(group_meta.get("class_code") or "").strip(),
                                "STUDENT NAME": student_name,
                                "IDENTIFIER": identifier,
                            }
                        )
                else:
                    rows_rs.append(
                        {
                            "CLASS NAME": "",
                            "CLASS CODE": "",
                            "STUDENT NAME": student_name,
                            "IDENTIFIER": identifier,
                        }
                    )
            rows_rs = [
                row
                for row in rows_rs
                if row.get("CLASS NAME")
                or row.get("CLASS CODE")
                or row.get("STUDENT NAME")
                or row.get("IDENTIFIER")
            ]
            rows_rs = sorted(
                rows_rs,
                key=lambda row: (
                    str(row.get("CLASS NAME") or "").lower(),
                    str(row.get("CLASS CODE") or "").lower(),
                    str(row.get("STUDENT NAME") or "").lower(),
                    str(row.get("IDENTIFIER") or "").lower(),
                ),
            )

            rs_excel_bytes = _export_simple_excel(rows_rs, sheet_name="users")
            st.session_state["rs_excel_bytes"] = rs_excel_bytes
            st.session_state["rs_excel_count"] = int(len(rows_rs))
            st.success(
                "EXCEL RS listo. Filas: {filas} | Usuarios validos: {validos}/{total}.".format(
                    filas=len(rows_rs),
                    validos=len(filtered_users),
                    total=len(rs_users),
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

        if st.session_state.get("rs_excel_bytes"):
            st.download_button(
                label="Descargar EXCEL RS",
                data=st.session_state["rs_excel_bytes"],
                file_name="excel_rs.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="rs_excel_download",
            )
    
with tab_crud_clases:
    if not _restricted_sections_unlocked():
        _render_restricted_blur("CRUD Clases", "clases_2")
    else:
        st.markdown("**2) Gestion de clases**")
        st.caption("Listado, desasignacion de alumnos y eliminacion de clases.")
        colegio_id_raw = str(st.session_state.get("shared_colegio_id", "")).strip()
        ciclo_id = GESTION_ESCOLAR_CICLO_ID_DEFAULT
    
        token = _get_shared_token()
        empresa_id = DEFAULT_EMPRESA_ID
        timeout = 30
        run_cargar_clases_delete = False
        run_cargar_clases_alumnos = False
        run_eliminar_clases_selected = False
        run_eliminar_clases_masivo = False
        confirm_delete_selected = False
        confirm_delete_masivo = False
    
        col_list, col_alumnos, col_delete = st.columns(3, gap="small")
        with col_list:
            with st.container(border=True):
                st.markdown("**Listar clases**")
                st.caption("Consulta rapida por colegio.")
                run_listar_clases = st.button(
                    "Listar clases",
                    key="clases_listar_btn",
                    use_container_width=True,
                )
        with col_alumnos:
            with st.container(border=True):
                st.markdown("**Desasignacion de alumnos por clase**")
                st.caption("Selecciona una clase y vacia sus alumnos.")
                run_cargar_clases_alumnos = st.button(
                    "Cargar clases para desasignar",
                    key="clases_alumnos_load_options",
                    use_container_width=True,
                )
                alumnos_options = st.session_state.get("clases_alumnos_options") or []
                alumnos_option_ids: List[int] = []
                alumnos_labels: Dict[int, str] = {}
                for item in alumnos_options:
                    if not isinstance(item, dict):
                        continue
                    clase_id_tmp = _safe_int(item.get("id"))
                    if clase_id_tmp is None:
                        continue
                    alumnos_option_ids.append(int(clase_id_tmp))
                    alumnos_labels[int(clase_id_tmp)] = str(
                        item.get("nombre") or f"Clase {clase_id_tmp}"
                    )
    
                selected_clase_id: Optional[int] = None
                if alumnos_option_ids:
                    selected_clase_id = int(
                        st.selectbox(
                            "Clase seleccionada",
                            options=alumnos_option_ids,
                            format_func=lambda cid, lbl=alumnos_labels: (
                                f"{cid} | {lbl.get(int(cid), '')}"
                            ),
                            key="clases_alumnos_selected_id",
                        )
                    )
                else:
                    st.caption("Sin clases cargadas para seleccionar.")

                clase_id_manual = st.text_input(
                    "Clase ID manual (opcional)",
                    key="clases_alumnos_clase_id",
                    placeholder="20143933",
                )
                clase_id_raw = str(
                    clase_id_manual or (selected_clase_id if selected_clase_id is not None else "")
                ).strip()
                col_ver, col_vaciar = st.columns(2)
                run_ver_alumnos_clase = col_ver.button(
                    "Ver alumnos",
                    key="clases_ver_alumnos_btn",
                    use_container_width=True,
                )
                confirm_vaciar_clase = st.checkbox(
                    "Confirmo vaciar la clase (eliminar todos los alumnos).",
                    key="clases_vaciar_confirm",
                )
                run_vaciar_clase = col_vaciar.button(
                    "Vaciar clase",
                    key="clases_vaciar_btn",
                    use_container_width=True,
                )
        with col_delete:
            with st.container(border=True):
                st.markdown("**Eliminar clases**")
                st.caption("Accion irreversible.")
                run_cargar_clases_delete = st.button(
                    "Cargar clases para seleccionar",
                    key="clases_delete_load_options",
                    use_container_width=True,
                )
                delete_options = st.session_state.get("clases_delete_options") or []
                option_ids: List[int] = []
                labels_delete: Dict[int, str] = {}
                for item in delete_options:
                    if not isinstance(item, dict):
                        continue
                    clase_id_tmp = _safe_int(item.get("id"))
                    if clase_id_tmp is None:
                        continue
                    option_ids.append(int(clase_id_tmp))
                    labels_delete[int(clase_id_tmp)] = str(
                        item.get("nombre") or f"Clase {clase_id_tmp}"
                    )
    
                if option_ids:
                    st.multiselect(
                        "Clases seleccionadas",
                        options=option_ids,
                        format_func=lambda cid, lbl=labels_delete: (
                            f"{cid} | {lbl.get(int(cid), '')}"
                        ),
                        key="clases_delete_selected_ids",
                    )
                else:
                    st.caption("Sin clases cargadas para seleccion.")

                col_delete_selected, col_delete_all = st.columns(2)
                with col_delete_selected:
                    confirm_delete_selected = st.checkbox(
                        "Confirmo seleccionadas.",
                        key="clases_confirm_delete_selected",
                    )
                    run_eliminar_clases_selected = st.button(
                        "Eliminar seleccionadas",
                        key="clases_eliminar_selected_btn",
                        use_container_width=True,
                    )
                with col_delete_all:
                    confirm_delete_masivo = st.checkbox(
                        "Confirmo eliminacion masiva.",
                        key="clases_confirm_delete_masivo",
                    )
                    run_eliminar_clases_masivo = st.button(
                        "Eliminar masivo",
                        key="clases_eliminar_masivo_btn",
                        use_container_width=True,
                    )
        run_cargar_asignacion = False
        run_eliminar_participantes = False
        run_asignar_participantes = False
        confirm_eliminar_participantes = False
        confirm_asignar_participantes = False
        st.divider()
        with st.container(border=True):
            st.markdown("**Asignacion de Participantes**")
            st.caption(
                "Ejecuta por separado: primero elimina alumnos, luego asigna grupo."
            )

            col_auto_load, col_auto_del, col_auto_asig = st.columns(
                [1.1, 1.5, 1.5],
                gap="small",
            )
            run_cargar_asignacion = col_auto_load.button(
                "Cargar clases",
                key="clases_auto_group_load",
                use_container_width=True,
            )
            confirm_eliminar_participantes = col_auto_del.checkbox(
                "Confirmo eliminar alumnos de las clases.",
                key="clases_auto_group_confirm_delete_participants",
            )
            run_eliminar_participantes = col_auto_del.button(
                "Eliminar participantes",
                key="clases_auto_group_delete_participants",
                use_container_width=True,
            )
            confirm_asignar_participantes = col_auto_asig.checkbox(
                "Confirmo asignar grupos a las clases.",
                key="clases_auto_group_confirm_assign_participants",
            )
            run_asignar_participantes = col_auto_asig.button(
                "Asignar participantes",
                key="clases_auto_group_assign_participants",
                use_container_width=True,
            )

        if run_cargar_asignacion:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
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
                    ordered=True,
                )
                niveles_data = _fetch_niveles_grados_grupos_censo(
                    token=token,
                    colegio_id=colegio_id_int,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as exc:  # pragma: no cover - UI
                st.error(f"Error: {exc}")
                st.stop()
    
            grupos_por_grado = _build_grupos_disponibles_por_grado(niveles_data)
            rows_auto: List[Dict[str, object]] = []
            warnings_auto: List[str] = []
            for item in clases:
                if not isinstance(item, dict):
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
                    warnings_auto.append(
                        f"Clase {meta['clase_id']} sin grupo sugerido."
                    )
                    continue
    
                rows_auto.append(
                    {
                        **meta,
                        "options": options,
                        "selected_group_id": int(default_group_id),
                    }
                )
    
            st.session_state["clases_auto_group_rows"] = rows_auto
            st.session_state["clases_auto_group_warnings"] = warnings_auto
            st.session_state["clases_auto_group_context"] = {
                "colegio_id": int(colegio_id_int),
                "ciclo_id": int(ciclo_id),
                "empresa_id": int(empresa_id),
            }
    
            st.success(
                "Clases cargadas: {total}. Con opciones de grupo: {ok}. Omitidas: {omitidas}.".format(
                    total=len(clases),
                    ok=len(rows_auto),
                    omitidas=max(len(clases) - len(rows_auto), 0),
                )
            )
        auto_rows = st.session_state.get("clases_auto_group_rows") or []
        auto_warnings = st.session_state.get("clases_auto_group_warnings") or []
        show_auto_group_grid = st.toggle(
            "Mostrar grilla por grado",
            value=False,
            key="clases_auto_group_show_grid",
        )
        if show_auto_group_grid and auto_rows:
            st.markdown("**Asignacion por grado (grilla compacta 7 columnas)**")
            auto_rows = sorted(
                auto_rows,
                key=lambda row: (
                    str(row.get("nivel_nombre") or "").upper(),
                    str(row.get("grado_nombre") or "").upper(),
                    int(row.get("nivel_id") or 0),
                    int(row.get("grado_id") or 0),
                    (
                        _extract_group_hint_from_class_name(row.get("clase_nombre"))
                        or "ZZ"
                    ),
                    str(row.get("clase_nombre") or "").upper(),
                    int(row.get("clase_id") or 0),
                ),
            )
            grouped_rows: Dict[Tuple[int, int, str, str], List[Dict[str, object]]] = {}
            for row in auto_rows:
                key = (
                    int(row.get("nivel_id") or 0),
                    int(row.get("grado_id") or 0),
                    str(row.get("nivel_nombre") or ""),
                    str(row.get("grado_nombre") or ""),
                )
                grouped_rows.setdefault(key, []).append(row)

            for group_key in sorted(
                grouped_rows.keys(),
                key=lambda item: (
                    item[2].upper(),
                    item[3].upper(),
                    item[0],
                    item[1],
                ),
            ):
                nivel_id, grado_id, nivel_nombre, grado_nombre = group_key
                rows_group = grouped_rows[group_key]
                titulo_nivel = nivel_nombre or f"Nivel {nivel_id}"
                titulo_grado = grado_nombre or f"Grado {grado_id}"
                st.caption(
                    f"{titulo_nivel} | {titulo_grado} | Clases: {len(rows_group)}"
                )
                rows_group = sorted(
                    rows_group,
                    key=lambda row: (
                        (
                            _extract_group_hint_from_class_name(
                                row.get("clase_nombre")
                            )
                            or "ZZ"
                        ),
                        str(row.get("clase_nombre") or "").upper(),
                        int(row.get("clase_id") or 0),
                    ),
                )
                cols_grid = st.columns(7, gap="small")
                for idx_row, row in enumerate(rows_group):
                    with cols_grid[idx_row % 7]:
                        with st.container(border=True):
                            clase_id = int(row["clase_id"])
                            options = row.get("options") or []
                            if not options:
                                st.caption(f"`{clase_id}` sin grupos")
                                continue
                            option_ids = [int(opt["grupo_id"]) for opt in options]
                            labels: Dict[int, str] = {}
                            for opt in options:
                                alumnos_contratados = opt.get("alumnos_contratados")
                                count_txt = (
                                    f" ({int(alumnos_contratados)})"
                                    if alumnos_contratados is not None
                                    else ""
                                )
                                clave = str(opt.get("grupo_clave") or "").strip()
                                nombre = str(opt.get("grupo_nombre") or "").strip()
                                grupo_txt = clave or nombre or str(opt.get("grupo_id"))
                                labels[int(opt["grupo_id"])] = f"{grupo_txt}{count_txt}"

                            selected_default = int(
                                row.get("selected_group_id") or option_ids[0]
                            )
                            if selected_default not in option_ids:
                                selected_default = option_ids[0]

                            clase_nombre = str(row.get("clase_nombre") or "").strip()
                            label_txt = f"`{clase_id}` {clase_nombre}"
                            if len(label_txt) > 38:
                                label_txt = f"{label_txt[:35].rstrip()}..."
                            st.caption(label_txt)
                            key_select = f"clases_auto_group_select_{clase_id}"
                            selected_val = st.selectbox(
                                "Grupo",
                                options=option_ids,
                                index=option_ids.index(selected_default),
                                format_func=lambda gid, lbl=labels: lbl.get(
                                    int(gid), str(gid)
                                ),
                                key=key_select,
                                label_visibility="collapsed",
                            )
                            row["selected_group_id"] = int(selected_val)
            st.session_state["clases_auto_group_rows"] = auto_rows
        elif auto_rows:
            st.caption(f"Grilla oculta. Clases cargadas: {len(auto_rows)}")

        if auto_warnings:
            st.warning("Hay clases omitidas o sin opciones de grupo.")
            st.write("\n".join(f"- {item}" for item in auto_warnings[:20]))
            restantes = len(auto_warnings) - 20
            if restantes > 0:
                st.caption(f"... y {restantes} advertencias mas.")

        if run_eliminar_participantes or run_asignar_participantes:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
            if run_eliminar_participantes and not confirm_eliminar_participantes:
                st.error("Debes confirmar antes de eliminar participantes.")
                st.stop()
            if run_asignar_participantes and not confirm_asignar_participantes:
                st.error("Debes confirmar antes de asignar participantes.")
                st.stop()
    
            rows_auto = st.session_state.get("clases_auto_group_rows") or []
            context_auto = st.session_state.get("clases_auto_group_context") or {}
            if not rows_auto:
                st.error("Primero carga las clases.")
                st.stop()
    
            try:
                colegio_id_int = _parse_colegio_id(colegio_id_raw)
            except ValueError as exc:
                st.error(f"Error: {exc}")
                st.stop()
            if int(context_auto.get("colegio_id", -1)) != int(colegio_id_int):
                st.error("El colegio global cambio. Vuelve a cargar clases.")
                st.stop()
            if int(context_auto.get("ciclo_id", -1)) != int(ciclo_id) or int(
                context_auto.get("empresa_id", -1)
            ) != int(empresa_id):
                st.error("El contexto cambio. Vuelve a cargar clases.")
                st.stop()
    
            total = len(rows_auto)
    
            if run_eliminar_participantes:
                resultados_delete: List[Dict[str, object]] = []
                ok_count = 0
                skip_count = 0
                err_count = 0
                progress = st.progress(0)
                status = st.empty()
    
                for idx, row in enumerate(rows_auto, start=1):
                    clase_id = int(row["clase_id"])
                    clase_nombre = str(row.get("clase_nombre") or "").strip()
                    try:
                        status.write(
                            f"Eliminando {idx}/{total}: clase {clase_id} | listando alumnos actuales"
                        )
                        clase_data_actual = _fetch_alumnos_clase_gestion_escolar(
                            token=token,
                            clase_id=int(clase_id),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        err_count += 1
                        resultados_delete.append(
                            {
                                "Clase ID": clase_id,
                                "Clase": row.get("clase_nombre", ""),
                                "Resultado": "Error",
                                "Detalle": f"No se pudo listar alumnos actuales: {exc}",
                            }
                        )
                        progress.progress(int((idx / total) * 100))
                        continue
    
                    alumnos_actuales = clase_data_actual.get("claseAlumnos") or []
                    if not isinstance(alumnos_actuales, list):
                        err_count += 1
                        resultados_delete.append(
                            {
                                "Clase ID": clase_id,
                                "Clase": row.get("clase_nombre", ""),
                                "Resultado": "Error",
                                "Detalle": "Respuesta invalida: claseAlumnos no es lista.",
                            }
                        )
                        progress.progress(int((idx / total) * 100))
                        continue
    
                    alumnos_ids_actuales: List[int] = []
                    seen_alumnos = set()
                    for entry in alumnos_actuales:
                        if not isinstance(entry, dict):
                            continue
                        alumno = entry.get("alumno")
                        if not isinstance(alumno, dict):
                            continue
                        alumno_id_tmp = _safe_int(alumno.get("alumnoId"))
                        if alumno_id_tmp is None:
                            continue
                        if int(alumno_id_tmp) in seen_alumnos:
                            continue
                        seen_alumnos.add(int(alumno_id_tmp))
                        alumnos_ids_actuales.append(int(alumno_id_tmp))
    
                    if not alumnos_ids_actuales:
                        skip_count += 1
                        resultados_delete.append(
                            {
                                "Clase ID": clase_id,
                                "Clase": row.get("clase_nombre", ""),
                                "Resultado": "Sin cambios",
                                "Detalle": "No habia alumnos en la clase.",
                            }
                        )
                        progress.progress(int((idx / total) * 100))
                        continue
    
                    delete_errors: List[str] = []
                    deleted_count = 0
                    total_alumnos_actuales = len(alumnos_ids_actuales)
                    for del_idx, alumno_id_actual in enumerate(alumnos_ids_actuales, start=1):
                        status.write(
                            "Eliminando {idx}/{total}: clase {clase} | eliminando {del_idx}/{tot} "
                            "alumno {alumno}".format(
                                idx=idx,
                                total=total,
                                clase=clase_id,
                                del_idx=del_idx,
                                tot=total_alumnos_actuales,
                                alumno=alumno_id_actual,
                            )
                        )
                        try:
                            _delete_alumno_clase_gestion_escolar(
                                token=token,
                                clase_id=int(clase_id),
                                alumno_id=int(alumno_id_actual),
                                empresa_id=int(empresa_id),
                                ciclo_id=int(ciclo_id),
                                timeout=int(timeout),
                            )
                            deleted_count += 1
                        except Exception as exc:  # pragma: no cover - UI
                            delete_errors.append(f"{alumno_id_actual}: {exc}")
    
                    if delete_errors:
                        err_count += 1
                        resultados_delete.append(
                            {
                                "Clase ID": clase_id,
                                "Clase": row.get("clase_nombre", ""),
                                "Resultado": "Error",
                                "Detalle": (
                                    f"Fallo eliminacion, eliminados {deleted_count}/"
                                    f"{len(alumnos_ids_actuales)}"
                                ),
                            }
                        )
                    else:
                        ok_count += 1
                        resultados_delete.append(
                            {
                                "Clase ID": clase_id,
                                "Clase": row.get("clase_nombre", ""),
                                "Resultado": "OK",
                                "Detalle": f"Eliminados {deleted_count} alumnos.",
                            }
                        )
                    progress.progress(int((idx / total) * 100))
    
                status.empty()
                st.success(
                    f"Eliminacion completada. OK: {ok_count} | Sin cambios: {skip_count} | Errores: {err_count}"
                )
                if resultados_delete:
                    _show_dataframe(resultados_delete, use_container_width=True)
    
            if run_asignar_participantes:
                alumnos_cache: Dict[Tuple[int, int, int], int] = {}
                resultados_assign: List[Dict[str, object]] = []
                ok_count = 0
                skip_count = 0
                err_count = 0
                progress = st.progress(0)
                status = st.empty()
    
                for idx, row in enumerate(rows_auto, start=1):
                    clase_id = int(row["clase_id"])
                    clase_nombre = str(row.get("clase_nombre") or "").strip()
                    nivel_id = int(row["nivel_id"])
                    grado_id = int(row["grado_id"])
                    options = row.get("options") or []
                    key_select = f"clases_auto_group_select_{clase_id}"
                    auto_group_id = _pick_default_group_id(
                        row.get("clase_nombre"),
                        options if isinstance(options, list) else [],
                        row.get("grupo_id_actual"),
                    )
                    selected_group_id = _safe_int(auto_group_id)
                    if selected_group_id is None:
                        selected_group_id = _safe_int(
                            st.session_state.get(
                                key_select,
                                row.get("selected_group_id"),
                            )
                        )
    
                    status.write(
                        f"Asignando {idx}/{total}: clase {clase_id} | {clase_nombre}"
                    )
                    if selected_group_id is None:
                        err_count += 1
                        resultados_assign.append(
                            {
                                "Clase ID": clase_id,
                                "Clase": row.get("clase_nombre", ""),
                                "Resultado": "Error",
                                "Detalle": "Grupo seleccionado invalido.",
                            }
                        )
                        progress.progress(int((idx / total) * 100))
                        continue
    
                    alumnos_count: Optional[int] = None
                    censo_validacion_txt = ""
                    try:
                        status.write(
                            f"Asignando {idx}/{total}: clase {clase_id} | validando alumnos contratados"
                        )
                        alumnos_count = _fetch_grupo_alumnos_count(
                            token=token,
                            colegio_id=int(colegio_id_int),
                            nivel_id=int(nivel_id),
                            grado_id=int(grado_id),
                            grupo_id=int(selected_group_id),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                            cache=alumnos_cache,
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        censo_validacion_txt = (
                            f" No se pudo validar alumnos contratados en censo: {exc}."
                        )
    
                    if alumnos_count is not None and int(alumnos_count) <= 0:
                        censo_validacion_txt += (
                            " Censo reporta 0 alumnos contratados para el grupo."
                        )
    
                    try:
                        status.write(
                            f"Asignando {idx}/{total}: clase {clase_id} | asignando grupo {selected_group_id}"
                        )
                        _post_clase_participantes_gestion_escolar(
                            token=token,
                            clase_id=int(clase_id),
                            nivel_id=int(nivel_id),
                            grado_id=int(grado_id),
                            grupo_ids=[int(selected_group_id)],
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                        ok_count += 1
                        detalle_ok = f"Grupo {selected_group_id} asignado."
                        if censo_validacion_txt:
                            detalle_ok = f"{detalle_ok}{censo_validacion_txt}"
                        resultados_assign.append(
                            {
                                "Clase ID": clase_id,
                                "Clase": row.get("clase_nombre", ""),
                                "Resultado": "OK",
                                "Detalle": detalle_ok,
                            }
                        )
                        row["grupo_id_actual"] = int(selected_group_id)
                        for opt in row.get("options", []):
                            if int(opt.get("grupo_id", -1)) == int(selected_group_id):
                                row["grupo_clave_actual"] = str(
                                    opt.get("grupo_clave") or opt.get("grupo_nombre") or ""
                                )
                                break
                    except Exception as exc:  # pragma: no cover - UI
                        err_count += 1
                        resultados_assign.append(
                            {
                                "Clase ID": clase_id,
                                "Clase": row.get("clase_nombre", ""),
                                "Resultado": "Error",
                                "Detalle": str(exc),
                            }
                        )
                    progress.progress(int((idx / total) * 100))
    
                status.empty()
                st.session_state["clases_auto_group_rows"] = rows_auto
                st.success(
                    f"Asignacion completada. OK: {ok_count} | Sin cambios: {skip_count} | Errores: {err_count}"
                )
                if resultados_assign:
                    _show_dataframe(resultados_assign, use_container_width=True)
    
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
                                "Clase": item.get("geClase")
                                or item.get("geClaseClave")
                                or "",
                            }
                            for item in clases
                            if isinstance(item, dict)
                        ]
                        st.write(f"Clases encontradas: {len(tabla)}")
                        _show_dataframe(tabla, use_container_width=True)
    
        if run_cargar_clases_alumnos:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
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
                st.stop()
    
            options_alumnos: List[Dict[str, object]] = []
            for item in clases:
                if not isinstance(item, dict):
                    continue
                clase_id_tmp = _safe_int(item.get("geClaseId"))
                if clase_id_tmp is None:
                    continue
                options_alumnos.append(
                    {
                        "id": int(clase_id_tmp),
                        "nombre": str(item.get("geClase") or item.get("geClaseClave") or ""),
                    }
                )
            options_alumnos = sorted(
                options_alumnos,
                key=lambda row: (
                    str(row.get("nombre") or "").upper(),
                    int(row.get("id") or 0),
                ),
            )
            st.session_state["clases_alumnos_options"] = options_alumnos
            st.session_state["clases_alumnos_context"] = {
                "colegio_id": int(colegio_id_int),
                "ciclo_id": int(ciclo_id),
                "empresa_id": int(empresa_id),
            }
            st.success(f"Clases cargadas para desasignacion: {len(options_alumnos)}")
    
        if run_cargar_clases_delete:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
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
                st.stop()
    
            options_delete: List[Dict[str, object]] = []
            for item in clases:
                if not isinstance(item, dict):
                    continue
                clase_id_tmp = _safe_int(item.get("geClaseId"))
                if clase_id_tmp is None:
                    continue
                options_delete.append(
                    {
                        "id": int(clase_id_tmp),
                        "nombre": str(item.get("geClase") or item.get("geClaseClave") or ""),
                    }
                )
            st.session_state["clases_delete_options"] = options_delete
            st.session_state["clases_delete_context"] = {
                "colegio_id": int(colegio_id_int),
                "ciclo_id": int(ciclo_id),
                "empresa_id": int(empresa_id),
            }
            st.session_state["clases_delete_selected_ids"] = []
            st.success(f"Clases cargadas para seleccion: {len(options_delete)}")
    
        if run_eliminar_clases_selected:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
            if not confirm_delete_selected:
                st.error("Debes confirmar antes de eliminar seleccionadas.")
                st.stop()
    
            selected_raw = st.session_state.get("clases_delete_selected_ids") or []
            selected_ids: List[int] = []
            seen_ids = set()
            for value in selected_raw:
                class_id_tmp = _safe_int(value)
                if class_id_tmp is None:
                    continue
                if int(class_id_tmp) in seen_ids:
                    continue
                seen_ids.add(int(class_id_tmp))
                selected_ids.append(int(class_id_tmp))
            if not selected_ids:
                st.error("Selecciona al menos una clase para eliminar.")
                st.stop()
    
            try:
                colegio_id_int = _parse_colegio_id(colegio_id_raw)
            except ValueError as exc:
                st.error(f"Error: {exc}")
                st.stop()
            delete_context = st.session_state.get("clases_delete_context") or {}
            if int(delete_context.get("colegio_id", -1)) != int(colegio_id_int):
                st.error("El colegio global cambio. Vuelve a cargar clases para seleccionar.")
                st.stop()
            if int(delete_context.get("ciclo_id", -1)) != int(ciclo_id) or int(
                delete_context.get("empresa_id", -1)
            ) != int(empresa_id):
                st.error("El contexto cambio. Vuelve a cargar clases para seleccionar.")
                st.stop()
    
            delete_options = st.session_state.get("clases_delete_options") or []
            labels_delete: Dict[int, str] = {}
            for item in delete_options:
                if not isinstance(item, dict):
                    continue
                clase_id_tmp = _safe_int(item.get("id"))
                if clase_id_tmp is None:
                    continue
                labels_delete[int(clase_id_tmp)] = str(item.get("nombre") or "")
    
            total = len(selected_ids)
            progress = st.progress(0)
            status = st.empty()
            resultados_delete: List[Dict[str, object]] = []
            ok_count = 0
            err_count = 0
            ok_ids = set()
    
            for idx, clase_id in enumerate(selected_ids, start=1):
                status.write(f"Eliminando {idx}/{total}: clase {clase_id}")
                try:
                    _delete_clase_gestion_escolar(
                        token=token,
                        clase_id=int(clase_id),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                    )
                    ok_count += 1
                    ok_ids.add(int(clase_id))
                    resultados_delete.append(
                        {
                            "Clase ID": int(clase_id),
                            "Clase": labels_delete.get(int(clase_id), ""),
                            "Resultado": "OK",
                            "Detalle": "Clase eliminada.",
                        }
                    )
                except Exception as exc:  # pragma: no cover - UI
                    err_count += 1
                    resultados_delete.append(
                        {
                            "Clase ID": int(clase_id),
                            "Clase": labels_delete.get(int(clase_id), ""),
                            "Resultado": "Error",
                            "Detalle": str(exc),
                        }
                    )
                progress.progress(int((idx / total) * 100))
            status.empty()
    
            if ok_ids:
                filtered_options = []
                for item in delete_options:
                    if not isinstance(item, dict):
                        continue
                    clase_id_tmp = _safe_int(item.get("id"))
                    if clase_id_tmp is None or int(clase_id_tmp) in ok_ids:
                        continue
                    filtered_options.append(item)
                st.session_state["clases_delete_options"] = filtered_options
                st.session_state["clases_delete_selected_ids"] = [
                    cid for cid in selected_ids if int(cid) not in ok_ids
                ]
    
            st.success(
                f"Eliminacion seleccionada completada. OK: {ok_count} | Errores: {err_count}"
            )
            if resultados_delete:
                _show_dataframe(resultados_delete, use_container_width=True)
    
        if run_ver_alumnos_clase:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
            clase_id_text = str(clase_id_raw or "").strip()
            if not clase_id_text:
                st.error("Ingresa un Clase ID.")
                st.stop()
            try:
                clase_id_int = int(clase_id_text)
            except ValueError:
                st.error("Clase ID invalido. Debe ser numerico.")
                st.stop()
            try:
                clase_data = _fetch_alumnos_clase_gestion_escolar(
                    token=token,
                    clase_id=clase_id_int,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as exc:  # pragma: no cover - UI
                st.error(f"Error: {exc}")
                st.stop()
    
            clase_nombre = str(clase_data.get("geClase") or clase_data.get("geClaseClave") or "")
            alumnos_data = clase_data.get("claseAlumnos") or []
            if not isinstance(alumnos_data, list):
                st.error("Respuesta invalida: claseAlumnos no es lista.")
                st.stop()
    
            alumnos_rows: List[Dict[str, object]] = []
            for entry in alumnos_data:
                if not isinstance(entry, dict):
                    continue
                alumno = entry.get("alumno")
                if not isinstance(alumno, dict):
                    alumno = {}
                persona = alumno.get("persona")
                if not isinstance(persona, dict):
                    persona = {}
                persona_login = persona.get("personaLogin")
                if not isinstance(persona_login, dict):
                    persona_login = {}
    
                alumnos_rows.append(
                    {
                        "Alumno ID": alumno.get("alumnoId", ""),
                        "Persona ID": persona.get("personaId", ""),
                        "Nombre": persona.get("nombre", ""),
                        "Apellido Paterno": persona.get("apellidoPaterno", ""),
                        "Apellido Materno": persona.get("apellidoMaterno", ""),
                        "Nombre Completo": persona.get("nombreCompleto", ""),
                        "Login": persona_login.get("login", ""),
                        "NUIP": persona.get("idOficial", ""),
                        "Activo censo": bool(alumno.get("activo", False)),
                        "Activo clase": bool(entry.get("activo", False)),
                    }
                )
    
            st.success(
                f"Clase {clase_id_int} {clase_nombre} - Alumnos: {len(alumnos_rows)}"
            )
            if alumnos_rows:
                _show_dataframe(alumnos_rows, use_container_width=True)
            else:
                st.info("No hay alumnos en esta clase.")
    
        if run_vaciar_clase:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
            if not confirm_vaciar_clase:
                st.error("Debes confirmar antes de vaciar la clase.")
                st.stop()
    
            clase_id_text = str(clase_id_raw or "").strip()
            if not clase_id_text:
                st.error("Ingresa un Clase ID.")
                st.stop()
            try:
                clase_id_int = int(clase_id_text)
            except ValueError:
                st.error("Clase ID invalido. Debe ser numerico.")
                st.stop()
    
            try:
                clase_data = _fetch_alumnos_clase_gestion_escolar(
                    token=token,
                    clase_id=clase_id_int,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as exc:  # pragma: no cover - UI
                st.error(f"Error: {exc}")
                st.stop()
    
            clase_nombre = str(clase_data.get("geClase") or clase_data.get("geClaseClave") or "")
            alumnos_data = clase_data.get("claseAlumnos") or []
            if not isinstance(alumnos_data, list):
                st.error("Respuesta invalida: claseAlumnos no es lista.")
                st.stop()
            if not alumnos_data:
                st.info("No hay alumnos para eliminar en esta clase.")
                st.stop()
    
            targets: List[Dict[str, object]] = []
            seen_ids = set()
            for entry in alumnos_data:
                if not isinstance(entry, dict):
                    continue
                alumno = entry.get("alumno")
                if not isinstance(alumno, dict):
                    continue
                alumno_id_raw = alumno.get("alumnoId")
                if alumno_id_raw is None:
                    continue
                try:
                    alumno_id = int(alumno_id_raw)
                except (TypeError, ValueError):
                    continue
                if alumno_id in seen_ids:
                    continue
                seen_ids.add(alumno_id)
                persona = alumno.get("persona") if isinstance(alumno.get("persona"), dict) else {}
                targets.append(
                    {
                        "Alumno ID": alumno_id,
                        "Nombre Completo": str(persona.get("nombreCompleto") or ""),
                    }
                )
    
            if not targets:
                st.info("No se encontraron alumnoId validos para eliminar.")
                st.stop()
    
            errores: List[str] = []
            eliminados: List[Dict[str, object]] = []
            total = len(targets)
            progress = st.progress(0)
            status = st.empty()
            for idx, target in enumerate(targets, start=1):
                alumno_id = int(target["Alumno ID"])
                status.write(f"Eliminando {idx}/{total}: alumnoId {alumno_id}")
                try:
                    _delete_alumno_clase_gestion_escolar(
                        token=token,
                        clase_id=clase_id_int,
                        alumno_id=alumno_id,
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                    )
                    eliminados.append(target)
                except Exception as exc:  # pragma: no cover - UI
                    errores.append(f"{alumno_id}: {exc}")
                progress.progress(int((idx / total) * 100))
            status.empty()
    
            st.success(
                f"Clase {clase_id_int} {clase_nombre} - Eliminados: {len(eliminados)} de {total}"
            )
            if eliminados:
                _show_dataframe(eliminados, use_container_width=True)
            if errores:
                st.error("Errores al eliminar alumnos:")
                st.write("\n".join(f"- {item}" for item in errores[:30]))
                restantes = len(errores) - 30
                if restantes > 0:
                    st.caption(f"... y {restantes} errores mas.")
    
        if run_eliminar_clases_masivo:
            if not token:
                st.error("Falta el token. Configura el token global o PEGASUS_TOKEN.")
                st.stop()
            if not confirm_delete_masivo:
                st.error("Debes confirmar antes de eliminar masivo.")
                st.stop()
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
                st.stop()
    
            if not clases:
                st.info("No se encontraron clases.")
                st.stop()
    
            errores: List[str] = []
            for item in clases:
                clase_id = item.get("geClaseId") if isinstance(item, dict) else None
                if clase_id is None:
                    errores.append("Clase sin geClaseId.")
                    continue
                try:
                    _delete_clase_gestion_escolar(
                        token=token,
                        clase_id=int(clase_id),
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                    )
                except Exception as exc:  # pragma: no cover - UI
                    errores.append(f"{clase_id}: {exc}")
    
            colegios = _collect_colegios(clases)
            if colegios:
                st.write("Colegios eliminados (id, nombre):")
                _show_dataframe(colegios, use_container_width=True)
            eliminadas = len(clases) - len(errores)
            st.success(f"Clases eliminadas: {eliminadas}")
            if errores:
                st.error("Errores al eliminar:")
                st.write("\n".join(f"- {item}" for item in errores))
    
with tab_crud_alumnos:

    colegio_id_raw = str(
        st.session_state.get("shared_colegio_id", "")
        or st.session_state.get("alumnos_colegio_text", "")
    ).strip()
    ciclo_id = int(st.session_state.get("alumnos_ciclo", ALUMNOS_CICLO_ID_DEFAULT))
    empresa_id = int(st.session_state.get("alumnos_empresa", DEFAULT_EMPRESA_ID))
    timeout = int(st.session_state.get("alumnos_timeout", 30))

    full_width_col = st.columns(1)[0]
    with full_width_col:
        with st.container(border=True):
            st.markdown("**4) Generar Excel por niveles, grados y secciones (Censo)**")
            st.caption("Exporta alumnos desde Censo.")
            solo_activos_censo = st.checkbox(
                "Solo alumnos activos en censo",
                value=False,
                key="clases_alumnos_excel_solo_activos",
            )
            excluir_5to_sec_z = False
            if st.button(
                "Generar Excel alumnos (Censo)",
                type="primary",
                key="clases_alumnos_excel_generar",
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
    
                try:
                    with st.spinner("Consultando niveles/grados/grupos..."):
                        niveles_data = _fetch_niveles_grados_grupos_censo(
                            token=token,
                            colegio_id=colegio_id_int,
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                except Exception as exc:  # pragma: no cover - UI
                    st.error(f"Error: {exc}")
                    st.stop()
    
                contexts: List[Dict[str, object]] = []
                seen_contexts = set()
                for nivel_entry in niveles_data:
                    if not isinstance(nivel_entry, dict):
                        continue
                    nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
                    nivel_id = nivel.get("nivelId")
                    if nivel_id is None:
                        continue
                    try:
                        nivel_id_int = int(nivel_id)
                    except (TypeError, ValueError):
                        continue
                    nivel_name = str(nivel.get("nivel") or "")
                    nivel_order = _to_int_or_default(nivel.get("nivel_orden"), 9999)
                    grados = nivel_entry.get("grados") or []
                    if not isinstance(grados, list):
                        continue
    
                    for grado_entry in grados:
                        if not isinstance(grado_entry, dict):
                            continue
                        grado = (
                            grado_entry.get("grado")
                            if isinstance(grado_entry.get("grado"), dict)
                            else {}
                        )
                        grado_id = grado.get("gradoId")
                        if grado_id is None:
                            continue
                        try:
                            grado_id_int = int(grado_id)
                        except (TypeError, ValueError):
                            continue
                        grado_name = str(grado.get("grado") or "")
                        grado_order = _to_int_or_default(grado_id, 9999)
                        grupos = grado_entry.get("grupos") or []
                        if not isinstance(grupos, list):
                            continue
    
                        for grupo_entry in grupos:
                            if not isinstance(grupo_entry, dict):
                                continue
                            grupo = (
                                grupo_entry.get("grupo")
                                if isinstance(grupo_entry.get("grupo"), dict)
                                else {}
                            )
                            grupo_id = grupo.get("grupoId")
                            if grupo_id is None:
                                continue
                            try:
                                grupo_id_int = int(grupo_id)
                            except (TypeError, ValueError):
                                continue
    
                            grupo_name = str(grupo.get("grupo") or "")
                            grupo_clave = str(grupo.get("grupoClave") or "")
                            key = (nivel_id_int, grado_id_int, grupo_id_int)
                            if key in seen_contexts:
                                continue
                            seen_contexts.add(key)
                            contexts.append(
                                {
                                    "nivel_id": nivel_id_int,
                                    "nivel": nivel_name,
                                    "nivel_order": nivel_order,
                                    "grado_id": grado_id_int,
                                    "grado": grado_name,
                                    "grado_order": grado_order,
                                    "grupo_id": grupo_id_int,
                                    "grupo": grupo_name,
                                    "grupo_clave": grupo_clave,
                                }
                            )
    
                if not contexts:
                    st.warning("No se encontraron niveles/grados/grupos para el colegio.")
                    st.stop()
    
                contexts = sorted(
                    contexts,
                    key=lambda ctx: (
                        int(ctx.get("nivel_order", 9999)),
                        int(ctx.get("grado_order", 9999)),
                        _grupo_sort_key(
                            str(ctx.get("grupo_clave", "")),
                            str(ctx.get("grupo", "")),
                        ),
                    ),
                )
                if excluir_5to_sec_z:
                    before_count = len(contexts)
                    contexts = [
                        ctx
                        for ctx in contexts
                        if not _is_quinto_secundaria_z(
                            int(ctx.get("nivel_id", 0)),
                            ctx.get("nivel", ""),
                            ctx.get("grado", ""),
                            ctx.get("grupo_clave") or ctx.get("grupo") or "",
                        )
                    ]
                    excluded_count = before_count - len(contexts)
                    if excluded_count > 0:
                        st.info(f"Se excluyeron {excluded_count} combinaciones de 5to Sec Z.")
                    if not contexts:
                        st.warning("No quedaron combinaciones para consultar despues del filtro.")
                        st.stop()
    
                by_alumno_id: Dict[str, Dict[str, str]] = {}
                by_persona_id: Dict[str, Dict[str, str]] = {}
                try:
                    by_alumno_id, by_persona_id = _fetch_login_password_lookup_censo(
                        token=token,
                        colegio_id=colegio_id_int,
                        empresa_id=int(empresa_id),
                        ciclo_id=int(ciclo_id),
                        timeout=int(timeout),
                    )
                except Exception as exc:  # pragma: no cover - UI
                    st.warning(
                        "No se pudo cargar lookup de login/password desde plantilla de "
                        f"edición masiva: {exc}"
                    )
    
                rows_excel: List[Dict[str, object]] = []
                seen_excel_keys = set()
                errores_excel: List[str] = []
                total = len(contexts)
                progress = st.progress(0)
                status = st.empty()
    
                for index, ctx in enumerate(contexts, start=1):
                    progress.progress(int((index / total) * 100))
                    status.write(
                        "Consultando {idx}/{total}: N{nivel} G{grado} S{seccion}".format(
                            idx=index,
                            total=total,
                            nivel=ctx["nivel_id"],
                            grado=ctx["grado_id"],
                            seccion=ctx["grupo_id"],
                        )
                    )
                    try:
                        alumnos_data = _fetch_alumnos_censo(
                            token=token,
                            colegio_id=colegio_id_int,
                            nivel_id=int(ctx["nivel_id"]),
                            grado_id=int(ctx["grado_id"]),
                            grupo_id=int(ctx["grupo_id"]),
                            empresa_id=int(empresa_id),
                            ciclo_id=int(ciclo_id),
                            timeout=int(timeout),
                        )
                    except Exception as exc:  # pragma: no cover - UI
                        errores_excel.append(
                            "nivelId={nivel} gradoId={grado} grupoId={grupo}: {error}".format(
                                nivel=ctx["nivel_id"],
                                grado=ctx["grado_id"],
                                grupo=ctx["grupo_id"],
                                error=exc,
                            )
                        )
                        continue
    
                    for item in alumnos_data:
                        if not isinstance(item, dict):
                            continue
                        source = _extract_alumno_payload(item)
                        activo_value = source.get("activo", item.get("activo"))
                        if solo_activos_censo and not _to_bool(activo_value):
                            continue

                        persona = source.get("persona") if isinstance(source.get("persona"), dict) else {}
                        dedupe_key = _build_alumno_export_key(item, source, persona)
                        if dedupe_key and dedupe_key in seen_excel_keys:
                            continue
                        if dedupe_key:
                            seen_excel_keys.add(dedupe_key)
                        login, password = _resolve_alumno_login_password(
                            item=item,
                            by_alumno_id=by_alumno_id,
                            by_persona_id=by_persona_id,
                        )
                        rows_excel.append(
                            {
                                "_nivel_order": int(ctx["nivel_order"]),
                                "_grado_order": int(ctx["grado_order"]),
                                "_grupo_sort": _grupo_sort_key(
                                    str(ctx.get("grupo_clave", "")),
                                    str(ctx.get("grupo", "")),
                                ),
                                "Nivel": str(ctx.get("nivel", "")),
                                "Grado": str(ctx.get("grado", "")),
                                "Seccion": str(ctx.get("grupo_clave") or ctx.get("grupo") or ""),
                                "Nombre": str(persona.get("nombre") or ""),
                                "Apellido Paterno": str(persona.get("apellidoPaterno") or ""),
                                "Apellido Materno": str(persona.get("apellidoMaterno") or ""),
                                "Login": login,
                                "Password": password,
                                "_dedupe_key": dedupe_key,
                            }
                        )

                if contexts:
                    status.write(
                        "Consolidando por nivel/grado para asegurar cobertura..."
                    )
                    fallback_pairs = sorted(
                        {
                            (
                                int(ctx["nivel_id"]),
                                int(ctx["grado_id"]),
                                str(ctx.get("nivel", "")),
                                str(ctx.get("grado", "")),
                                int(ctx["nivel_order"]),
                                int(ctx["grado_order"]),
                            )
                            for ctx in contexts
                        },
                        key=lambda item: (item[4], item[5]),
                    )
                    for (
                        nivel_id_fb,
                        grado_id_fb,
                        nivel_name_fb,
                        grado_name_fb,
                        nivel_order_fb,
                        grado_order_fb,
                    ) in fallback_pairs:
                        try:
                            alumnos_fallback = _fetch_alumnos_censo(
                                token=token,
                                colegio_id=colegio_id_int,
                                nivel_id=nivel_id_fb,
                                grado_id=grado_id_fb,
                                grupo_id=None,
                                empresa_id=int(empresa_id),
                                ciclo_id=int(ciclo_id),
                                timeout=int(timeout),
                            )
                        except Exception as exc:  # pragma: no cover - UI
                            errores_excel.append(
                                "fallback nivelId={nivel} gradoId={grado}: {error}".format(
                                    nivel=nivel_id_fb,
                                    grado=grado_id_fb,
                                    error=exc,
                                )
                            )
                            continue
    
                        for item in alumnos_fallback:
                            if not isinstance(item, dict):
                                continue
                            source = _extract_alumno_payload(item)
                            activo_value = source.get("activo", item.get("activo"))
                            if solo_activos_censo and not _to_bool(activo_value):
                                continue

                            persona = (
                                source.get("persona")
                                if isinstance(source.get("persona"), dict)
                                else {}
                            )
                            grupo_info = (
                                source.get("grupo")
                                if isinstance(source.get("grupo"), dict)
                                else (
                                    item.get("grupo")
                                    if isinstance(item.get("grupo"), dict)
                                    else {}
                                )
                            )
                            grupo_clave = str(grupo_info.get("grupoClave") or "")
                            grupo_nombre = str(grupo_info.get("grupo") or "")
                            if excluir_5to_sec_z and _is_quinto_secundaria_z(
                                int(nivel_id_fb),
                                nivel_name_fb,
                                grado_name_fb,
                                grupo_clave or grupo_nombre,
                            ):
                                continue
                            dedupe_key = _build_alumno_export_key(item, source, persona)
                            if dedupe_key and dedupe_key in seen_excel_keys:
                                continue
                            if dedupe_key:
                                seen_excel_keys.add(dedupe_key)
                            login, password = _resolve_alumno_login_password(
                                item=item,
                                by_alumno_id=by_alumno_id,
                                by_persona_id=by_persona_id,
                            )
                            rows_excel.append(
                                {
                                    "_nivel_order": nivel_order_fb,
                                    "_grado_order": grado_order_fb,
                                    "_grupo_sort": _grupo_sort_key(grupo_clave, grupo_nombre),
                                    "Nivel": nivel_name_fb,
                                    "Grado": grado_name_fb,
                                    "Seccion": grupo_clave or grupo_nombre,
                                    "Nombre": str(persona.get("nombre") or ""),
                                    "Apellido Paterno": str(persona.get("apellidoPaterno") or ""),
                                    "Apellido Materno": str(persona.get("apellidoMaterno") or ""),
                                    "Login": login,
                                    "Password": password,
                                    "_dedupe_key": dedupe_key,
                                }
                            )
    
                progress.progress(100)
                status.empty()
    
                if rows_excel:
                    rows_excel = sorted(
                        rows_excel,
                        key=lambda row: (
                            row["_nivel_order"],
                            row["_grado_order"],
                            row["_grupo_sort"],
                            str(row.get("Apellido Paterno", "")).lower(),
                            str(row.get("Apellido Materno", "")).lower(),
                            str(row.get("Nombre", "")).lower(),
                        ),
                    )
    
                output = BytesIO()
                excel_columns = [
                    "Nivel",
                    "Grado",
                    "Seccion",
                    "Nombre",
                    "Apellido Paterno",
                    "Apellido Materno",
                    "Login",
                    "Password",
                ]
                df_excel = pd.DataFrame(rows_excel)
                if df_excel.empty:
                    df_excel = pd.DataFrame(columns=excel_columns)
                else:
                    df_excel = df_excel.drop(
                        columns=[
                            "_nivel_order",
                            "_grado_order",
                            "_grupo_sort",
                            "_dedupe_key",
                        ],
                        errors="ignore",
                    )
                    df_excel = df_excel.reindex(columns=excel_columns)
    
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    df_excel.to_excel(writer, index=False, sheet_name="Alumnos")
                    if errores_excel:
                        pd.DataFrame({"error": errores_excel}).to_excel(
                            writer, index=False, sheet_name="Errores"
                        )
                    ws = writer.book["Alumnos"]
                    ws.freeze_panes = "A2"
                    ws.auto_filter.ref = ws.dimensions
    
                output.seek(0)
                file_name = f"alumnos_censo_{colegio_id_int}_{int(ciclo_id)}.xlsx"
                st.success("Excel generado.")
                st.markdown(f"- Combinaciones evaluadas: `{total}`")
                st.markdown(f"- Filas en Excel: `{len(df_excel)}`")
                st.markdown(f"- Errores: `{len(errores_excel)}`")
                st.download_button(
                    label="Descargar Excel alumnos",
                    data=output.getvalue(),
                    file_name=file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="clases_alumnos_excel_download",
                )
    
                if not df_excel.empty:
                    _show_dataframe(df_excel, use_container_width=True)
                if errores_excel:
                    st.warning("Hubo errores en algunas combinaciones.")
                    st.write("\n".join(f"- {item}" for item in errores_excel[:20]))
                    restantes = len(errores_excel) - 20
                    if restantes > 0:
                        st.caption(f"... y {restantes} errores más.")


