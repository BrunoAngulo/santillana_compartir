import os
import re
import tempfile
import unicodedata
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote, urljoin
from uuid import uuid4

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
JIRA_ADMIN_DISPLAY_NAME = "Bruno Ricardo Adrian Angulo Perez"
JIRA_ADMIN_QUERY_PARAM = "jira_admin"
JIRA_ADMIN_COOKIE_NAME = "jira_focus_admin_access"
JIRA_USER_QUERY_PARAM = "jira_user"
JIRA_USER_COOKIE_NAME = "jira_focus_user_display_name"
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


def _sync_jira_user_identity() -> None:
    jira_user_value = st.query_params.get(JIRA_USER_QUERY_PARAM, "")
    if isinstance(jira_user_value, list):
        jira_user_value = jira_user_value[0] if jira_user_value else ""
    try:
        if not jira_user_value:
            jira_user_value = st.context.cookies.get(JIRA_USER_COOKIE_NAME, "") or ""
    except Exception:
        pass
    jira_user_text = unquote(str(jira_user_value or "").strip())
    if jira_user_text:
        st.session_state["jira_focus_user_display_name"] = jira_user_text


def _restricted_sections_unlocked() -> bool:
    jira_admin_flag = st.query_params.get(JIRA_ADMIN_QUERY_PARAM, "")
    if isinstance(jira_admin_flag, list):
        jira_admin_flag = jira_admin_flag[0] if jira_admin_flag else ""
    jira_user_flag = st.query_params.get(JIRA_USER_QUERY_PARAM, "")
    if isinstance(jira_user_flag, list):
        jira_user_flag = jira_user_flag[0] if jira_user_flag else ""
    jira_admin_cookie = ""
    jira_user_cookie = ""
    try:
        jira_admin_cookie = str(st.context.cookies.get(JIRA_ADMIN_COOKIE_NAME, "") or "").strip()
        jira_user_cookie = str(st.context.cookies.get(JIRA_USER_COOKIE_NAME, "") or "").strip()
    except Exception:
        jira_admin_cookie = ""
        jira_user_cookie = ""

    admin_name_norm = _normalize_display_name(JIRA_ADMIN_DISPLAY_NAME)
    jira_user_flag_norm = _normalize_display_name(unquote(str(jira_user_flag or "").strip()))
    jira_user_cookie_norm = _normalize_display_name(unquote(jira_user_cookie))
    session_jira_user_norm = _normalize_display_name(
        st.session_state.get("jira_focus_user_display_name", "")
    )

    return (
        bool(st.session_state.get("restricted_sections_unlocked", False))
        or str(jira_admin_flag or "").strip() == "1"
        or jira_admin_cookie == "1"
        or jira_user_flag_norm == admin_name_norm
        or jira_user_cookie_norm == admin_name_norm
        or session_jira_user_norm == admin_name_norm
    )


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
    st.warning("Funcion bloqueada. Acceso restringido por contrasena.")
    st.caption(f"{section_name} requiere desbloqueo.")
    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        if st.button(
            "Desbloquear funciones restringidas",
            key=f"restricted_unlock_open_{key_suffix}",
            use_container_width=True,
        ):
            _show_restricted_unlock_dialog()


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
    if text.lower().startswith("bearer "):
        text = text[7:].strip()
    return text


def _sync_shared_token_from_input() -> None:
    token_input = _clean_token_value(st.session_state.get("shared_pegasus_token_input", ""))
    if token_input:
        st.session_state["shared_pegasus_token"] = token_input


st.set_page_config(page_title="Generador de Plantilla", layout="wide")
_inject_professional_theme()
st.components.v1.html(
    f"""
    <script>
      (function () {{
        const storageKey = 'jira_focus_admin_access';
        const queryKey = {JIRA_ADMIN_QUERY_PARAM!r};
        let syncTimer = null;

        function applyDesired(desired) {{
          let desiredUser = '';
          try {{
            desiredUser = window.localStorage.getItem('jira_focus_user_display_name') || '';
          }} catch (_err) {{
            desiredUser = '';
          }}
          try {{
            const maxAge = desired === '1' ? '31536000' : '0';
            document.cookie = {f"{JIRA_ADMIN_COOKIE_NAME}="!r} + desired + '; path=/; max-age=' + maxAge + '; SameSite=Lax';
          }} catch (_err) {{
            // No-op when cookies are not available.
          }}
          try {{
            const userMaxAge = desiredUser ? '31536000' : '0';
            const userValue = desiredUser ? encodeURIComponent(desiredUser) : '';
            document.cookie = {f"{JIRA_USER_COOKIE_NAME}="!r} + userValue + '; path=/; max-age=' + userMaxAge + '; SameSite=Lax';
          }} catch (_err) {{
            // No-op when cookies are not available.
          }}
          try {{
            const targetWindow = window.top && window.top !== window
              ? window.top
              : (window.parent && window.parent !== window ? window.parent : window);
            const targetUrl = new URL(targetWindow.location.href);
            const current = targetUrl.searchParams.get(queryKey) || '0';
            const currentUser = targetUrl.searchParams.get({JIRA_USER_QUERY_PARAM!r}) || '';
            if (current === desired && currentUser === desiredUser) return;
            if (desired === '1') {{
              targetUrl.searchParams.set(queryKey, '1');
            }} else {{
              targetUrl.searchParams.delete(queryKey);
            }}
            if (desiredUser) {{
              targetUrl.searchParams.set({JIRA_USER_QUERY_PARAM!r}, desiredUser);
            }} else {{
              targetUrl.searchParams.delete({JIRA_USER_QUERY_PARAM!r});
            }}
            targetWindow.location.replace(targetUrl.toString());
          }} catch (_err) {{
            // No-op when parent location is not accessible.
          }}
        }}

        function syncAdminFlag() {{
          let desired = '0';
          try {{
            desired = (window.localStorage.getItem(storageKey) || '') === '1' ? '1' : '0';
          }} catch (_err) {{
            desired = '0';
          }}
          applyDesired(desired);
        }}

        try {{
          const targetWindow = window.top && window.top !== window
            ? window.top
            : (window.parent && window.parent !== window ? window.parent : window);
          targetWindow.addEventListener('message', function (event) {{
            const data = event && event.data ? event.data : null;
            if (!data || data.type !== 'jira-focus-admin-access') return;
            try {{
              if (data.displayName) {{
                window.localStorage.setItem('jira_focus_user_display_name', String(data.displayName || '').trim());
              }} else {{
                window.localStorage.removeItem('jira_focus_user_display_name');
              }}
            }} catch (_err) {{
              // No-op when localStorage is not available.
            }}
            applyDesired(data.enabled ? '1' : '0');
          }});
        }} catch (_err) {{
          // No-op when parent messaging is not accessible.
        }}

        syncAdminFlag();
        if (!syncTimer) {{
          syncTimer = window.setInterval(syncAdminFlag, 1000);
        }}
      }})();
    </script>
    """,
    height=0,
)
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
    if "shared_pegasus_token" not in st.session_state:
        st.session_state["shared_pegasus_token"] = _clean_token_value(
            os.environ.get("PEGASUS_TOKEN", "")
        )
    if "shared_pegasus_token_input" not in st.session_state:
        st.session_state["shared_pegasus_token_input"] = str(
            st.session_state.get("shared_pegasus_token", "")
        )

    st.markdown("**Configuracion global**")
    global_col_token, global_col_colegio = st.columns([2.7, 1.1])
    with global_col_token:
        token_col_input, token_col_save, token_col_clear = st.columns([4.1, 1, 1], gap="small")
        with token_col_input:
            st.text_input(
                "Token (sin Bearer)",
                type="password",
                key="shared_pegasus_token_input",
                on_change=_sync_shared_token_from_input,
                help="Se usa en todas las funciones y queda guardado en la sesion actual.",
            )
        with token_col_save:
            if st.button("Guardar", key="shared_token_save_btn", use_container_width=True):
                _sync_shared_token_from_input()
        with token_col_clear:
            if st.button("Limpiar", key="shared_token_clear_btn", use_container_width=True):
                st.session_state["shared_pegasus_token"] = ""
                st.session_state["shared_pegasus_token_input"] = ""
                st.rerun()
        if st.session_state.get("shared_pegasus_token"):
            st.caption("Token guardado en sesion.")
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
    return _clean_token_value(token)


def _get_shared_token() -> str:
    token_saved = _clean_token(str(st.session_state.get("shared_pegasus_token", "")))
    if token_saved:
        return token_saved
    token_input = _clean_token(str(st.session_state.get("shared_pegasus_token_input", "")))
    if token_input:
        st.session_state["shared_pegasus_token"] = token_input
        return token_input
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
        "Accept": "application/json",
        "Content-Type": "application/json",
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
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        raise RuntimeError(_richmondstudio_error_detail(body, status_code))
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
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        raise RuntimeError(_richmondstudio_error_detail(body, status_code))
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
        body = {"detail": response.text or f"HTTP {status_code}"}
    raise RuntimeError(_richmondstudio_error_detail(body, status_code))


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
        parsed = date.fromisoformat(raw)
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
        "Test level": "lower secondary",
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
                    else "lower secondary"
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

    header_cols = st.columns([0.35, 0.55, 1.7, 1.7, 1.45, 1.25, 0.6, 0.8, 0.8], gap="small")
    header_cols[0].caption("#")
    header_cols[1].caption("Crear")
    header_cols[2].caption("Class name")
    header_cols[3].caption("Description")
    header_cols[4].caption("Grado")
    header_cols[5].caption("Test level")
    header_cols[6].caption("iRead")
    header_cols[7].caption(" ")
    header_cols[8].caption(" ")

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
        row_cols = st.columns([0.35, 0.55, 1.7, 1.7, 1.45, 1.25, 0.6, 0.8, 0.8], gap="small")
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
        iread = row_cols[6].checkbox(
            "iRead",
            value=bool(row.get("iRead", False)),
            key=f"{widget_prefix}_iread_{row_id}",
            label_visibility="collapsed",
        )
        if row_cols[7].button(
            "Duplicar",
            key=f"{widget_prefix}_duplicate_{row_id}",
            use_container_width=True,
        ):
            duplicate_after_row_id = row_id
        if row_cols[8].button(
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
                "iRead": bool(iread),
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
    level_value = _richmondstudio_level_from_test_level(
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
        level_value = _richmondstudio_level_from_test_level(
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
    start_date_obj, end_date_obj = _richmondstudio_default_dates()
    attributes: Dict[str, object] = {
        "name": class_name,
        "description": description,
        "grade": grade_code,
        "startDate": start_date_obj.isoformat(),
        "endDate": end_date_obj.isoformat(),
        "iread": bool(row.get("iRead", False)),
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
    users_data = row.get("_users_data")
    if not isinstance(users_data, list):
        users_data = []
    attributes = {
        "name": class_name,
        "description": description,
        "grade": grade_code,
        "startDate": _coerce_iso_date(row.get("Start date"), "Start date"),
        "endDate": _coerce_iso_date(row.get("End date"), "End date"),
        "iread": bool(row.get("iRead", False)),
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
            type="password",
            key="rs_groups_bearer_token",
            help="Se usa para clases RS y EXCEL RS.",
        )
    )
    st.session_state["rs_bearer_token"] = rs_token

    tab_rs_clases, tab_rs_excel = st.tabs(["Clases RS", "EXCEL RS"])
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
                        st.stop()

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
                        st.stop()

                    rows_to_delete = [
                        row for row in rs_loaded_rows if bool(row.get("Seleccionar"))
                    ]
                    if not rows_to_delete:
                        st.error("Selecciona al menos una clase RS para eliminar.")
                        st.stop()

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
                "Llena una clase por bloque. Description se completa con Class name si lo dejas vacio. Al crear: inicio = hoy, fin = 31/12 del ano actual y Test level vacio se manda como null."
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
                    st.stop()

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
    with tab_rs_excel:
        with st.container(border=True):
            st.markdown("**EXCEL RS**")
            st.caption(
                "Richmond Studio: CLASS NAME, CLASS CODE, STUDENT NAME, IDENTIFIER. Solo roles student/teacher."
            )
            run_rs_excel = st.button(
                "EXCEL RS",
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
                    key="rs_rs_excel_download",
                )


if menu_option == "Richmond Studio":
    render_richmond_studio_view()
    st.stop()

with tab_crud_clases:
    if not _restricted_sections_unlocked():
        _render_restricted_blur("CRUD Clases", "clases_1")
    else:
        st.subheader("CRUD Clases")
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
        st.caption("Lista clases y ejecuta acciones sobre seleccion.")
        colegio_id_raw = str(st.session_state.get("shared_colegio_id", "")).strip()
        ciclo_id = GESTION_ESCOLAR_CICLO_ID_DEFAULT
    
        token = _get_shared_token()
        rs_token_default = _get_richmondstudio_token()
        empresa_id = DEFAULT_EMPRESA_ID
        timeout = 30
        run_cargar_clases_delete = False
        run_eliminar_clases_selected = False
        confirm_delete_selected = False

        st.markdown("**RS | Listado y creacion masiva de clases**")
        st.caption(
            "Lista clases de Richmond Studio, filtralas y crea varias filas en una sola grilla."
        )
        if "rs_groups_bearer_token" not in st.session_state:
            st.session_state["rs_groups_bearer_token"] = rs_token_default
        with st.container(border=True):
            rs_token = st.text_input(
                "Bearer token RS",
                type="password",
                key="rs_groups_bearer_token",
                help="Se usa para GET, POST, PUT y DELETE sobre /api/groups.",
            )
            if "rs_groups_create_rows" not in st.session_state:
                st.session_state["rs_groups_create_rows"] = [
                    _default_richmondstudio_group_row()
                ]
            col_rs_a, col_rs_b, col_rs_c = st.columns([1, 1, 1], gap="small")
            run_rs_groups_load = col_rs_a.button(
                "Cargar clases RS",
                key="rs_groups_load_btn",
                use_container_width=True,
            )
            if col_rs_b.button(
                "Agregar fila abajo",
                key="rs_groups_new_row_btn",
                use_container_width=True,
            ):
                current_rs_rows = _normalize_richmondstudio_create_rows(
                    st.session_state.get("rs_groups_create_rows") or []
                )
                current_rs_rows.append(_default_richmondstudio_group_row())
                st.session_state["rs_groups_create_rows"] = current_rs_rows
            current_rs_rows = _normalize_richmondstudio_create_rows(
                st.session_state.get("rs_groups_create_rows") or []
            )
            duplicate_options = list(range(len(current_rs_rows)))
            duplicate_labels = {
                idx: f"Fila {idx + 1}: {str(row.get('Class name') or '').strip() or 'Sin nombre'}"
                for idx, row in enumerate(current_rs_rows)
            }
            duplicate_idx = 0
            if duplicate_options:
                duplicate_idx = int(
                    col_rs_c.selectbox(
                        "Fila base",
                        options=duplicate_options,
                        format_func=lambda idx: duplicate_labels.get(int(idx), f"Fila {int(idx) + 1}"),
                        key="rs_groups_duplicate_source",
                    )
                )
            if col_rs_c.button(
                "Duplicar fila base",
                key="rs_groups_duplicate_btn",
                use_container_width=True,
                disabled=not duplicate_options,
            ):
                current_rs_rows = _normalize_richmondstudio_create_rows(
                    st.session_state.get("rs_groups_create_rows") or []
                )
                if current_rs_rows:
                    base_row = dict(current_rs_rows[min(max(duplicate_idx, 0), len(current_rs_rows) - 1)])
                    base_row["Crear"] = True
                    st.session_state["rs_groups_create_rows"] = current_rs_rows + [base_row]

            if run_rs_groups_load:
                if not _clean_token(rs_token):
                    st.error("Ingresa el bearer token de Richmond Studio.")
                    st.stop()
                try:
                    with st.spinner("Consultando clases RS..."):
                        rs_groups_loaded = _fetch_richmondstudio_groups(
                            _clean_token(rs_token),
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
                    key="rs_groups_filter_text",
                    placeholder="Ej: 2026 Inglés 2SA",
                )
                rs_filter_level = col_rs_filter_b.selectbox(
                    "Test level",
                    options=["Todos"] + RICHMONDSTUDIO_TEST_LEVEL_LABELS,
                    key="rs_groups_filter_level",
                )
                rs_filter_iread = col_rs_filter_c.selectbox(
                    "iRead",
                    options=["Todos", "Si", "No"],
                    key="rs_groups_filter_iread",
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
                    key="rs_groups_loaded_editor",
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
                    key="rs_groups_update_btn",
                    use_container_width=True,
                )
                run_rs_groups_delete = col_rs_delete.button(
                    "Eliminar clases RS",
                    key="rs_groups_delete_btn",
                    use_container_width=True,
                )
                confirm_rs_delete = st.checkbox(
                    "Confirmar eliminacion de clases RS seleccionadas",
                    key="rs_groups_delete_confirm",
                    value=False,
                )

                if run_rs_groups_update:
                    rs_token_clean = _clean_token(rs_token)
                    if not rs_token_clean:
                        st.error("Ingresa el bearer token de Richmond Studio.")
                        st.stop()

                    rows_to_update = [
                        row for row in rs_loaded_rows if bool(row.get("Seleccionar"))
                    ]
                    if not rows_to_update:
                        st.error("Selecciona al menos una clase RS para actualizar.")
                        st.stop()

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
                                rs_token_clean,
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
                                rs_token_clean,
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
                    rs_token_clean = _clean_token(rs_token)
                    if not rs_token_clean:
                        st.error("Ingresa el bearer token de Richmond Studio.")
                        st.stop()
                    if not confirm_rs_delete:
                        st.error("Marca la confirmacion para eliminar clases RS.")
                        st.stop()

                    rows_to_delete = [
                        row for row in rs_loaded_rows if bool(row.get("Seleccionar"))
                    ]
                    if not rows_to_delete:
                        st.error("Selecciona al menos una clase RS para eliminar.")
                        st.stop()

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
                                rs_token_clean,
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
                                rs_token_clean,
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
                "Agrega filas como si fuera Excel. Description se completa con Class name si lo dejas vacio. Al crear: inicio = hoy, fin = 31/12 del ano actual y Test level vacio se manda como null."
            )
            rs_create_rows = _normalize_richmondstudio_create_rows(
                st.session_state.get("rs_groups_create_rows") or []
            )
            rs_create_columns = [
                "Crear",
                "Class name",
                "Description",
                "Grade",
                "Test level",
                "iRead",
            ]
            rs_create_df = pd.DataFrame(
                [
                    {column: row.get(column) for column in rs_create_columns}
                    for row in rs_create_rows
                ],
                columns=rs_create_columns,
            )
            edited_rs_create_df = st.data_editor(
                rs_create_df,
                key="rs_groups_create_editor",
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "Crear": st.column_config.CheckboxColumn(
                        "Crear",
                        help="Marca las filas que quieres enviar a RS.",
                        default=True,
                    ),
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
                    "iRead": st.column_config.CheckboxColumn("iRead"),
                },
            )
            if isinstance(edited_rs_create_df, pd.DataFrame):
                st.session_state["rs_groups_create_rows"] = _normalize_richmondstudio_create_rows(
                    edited_rs_create_df.to_dict("records")
                )

            run_rs_groups_create = st.button(
                "Crear clases RS",
                type="primary",
                key="rs_groups_create_btn",
                use_container_width=True,
            )
            if run_rs_groups_create:
                rs_token_clean = _clean_token(rs_token)
                if not rs_token_clean:
                    st.error("Ingresa el bearer token de Richmond Studio.")
                    st.stop()

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
                    st.stop()

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
                            rs_token_clean,
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
                            rs_token_clean,
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

        st.divider()

        with st.container(border=True):
            st.markdown("**Listar clases**")
            st.caption("Todas se marcan por defecto. Desmarca las que no quieras tocar.")
            run_cargar_clases_delete = st.button(
                "Cargar y listar clases",
                key="clases_delete_load_options",
                use_container_width=True,
            )
            delete_options = st.session_state.get("clases_delete_options") or []
            if delete_options:
                if "clases_delete_rows" not in st.session_state:
                    st.session_state["clases_delete_rows"] = []
                if "clases_delete_select_all_last" not in st.session_state:
                    st.session_state["clases_delete_select_all_last"] = True
                if not st.session_state.get("clases_delete_rows"):
                    st.session_state["clases_delete_rows"] = [
                        {
                            "Seleccionar": True,
                            "ID": int(item.get("id")),
                            "Clase": str(item.get("nombre") or ""),
                        }
                        for item in delete_options
                        if _safe_int(item.get("id")) is not None
                    ]
                if "clases_delete_select_all" not in st.session_state:
                    st.session_state["clases_delete_select_all"] = True

                select_all_delete = st.checkbox(
                    "Seleccionar todo",
                    value=bool(st.session_state.get("clases_delete_select_all", True)),
                    key="clases_delete_select_all_ui",
                )
                st.session_state["clases_delete_select_all"] = bool(select_all_delete)
                if (
                    st.session_state.get("clases_delete_select_all_last")
                    != bool(st.session_state.get("clases_delete_select_all", True))
                ):
                    rows_toggle = st.session_state.get("clases_delete_rows") or []
                    for row in rows_toggle:
                        row["Seleccionar"] = bool(
                            st.session_state.get("clases_delete_select_all", True)
                        )
                    st.session_state["clases_delete_rows"] = rows_toggle
                    st.session_state["clases_delete_select_all_last"] = bool(
                        st.session_state.get("clases_delete_select_all", True)
                    )

                delete_df = pd.DataFrame(st.session_state.get("clases_delete_rows") or [])
                edited_delete_df = st.data_editor(
                    delete_df,
                    key="clases_delete_editor",
                    hide_index=True,
                    use_container_width=True,
                    disabled=["ID", "Clase"],
                    column_config={
                        "Seleccionar": st.column_config.CheckboxColumn(
                            "Seleccionar",
                            help="Desmarca las clases que no quieres usar.",
                        ),
                        "ID": st.column_config.NumberColumn("ID", format="%d"),
                        "Clase": st.column_config.TextColumn("Clase"),
                    },
                )
                if isinstance(edited_delete_df, pd.DataFrame):
                    edited_rows = edited_delete_df.to_dict("records")
                    st.session_state["clases_delete_rows"] = edited_rows
                    selected_ids_editor = [
                        int(row.get("ID"))
                        for row in edited_rows
                        if bool(row.get("Seleccionar")) and _safe_int(row.get("ID")) is not None
                    ]
                    st.session_state["clases_delete_selected_ids"] = selected_ids_editor
                    total_editor = len(edited_rows)
                    selected_total = len(selected_ids_editor)
                    st.caption(f"Seleccionadas: {selected_total}/{total_editor}")
            else:
                st.caption("Sin clases cargadas para seleccion.")

            confirm_delete_selected = st.checkbox(
                "Confirmo eliminar las clases seleccionadas.",
                key="clases_confirm_delete_selected",
            )
            run_eliminar_clases_selected = st.button(
                "Eliminar clases seleccionadas",
                key="clases_eliminar_selected_btn",
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

        if auto_rows:
            auto_ids = [
                int(row["clase_id"])
                for row in auto_rows
                if _safe_int(row.get("clase_id")) is not None
            ]
            selection_rows = st.session_state.get("clases_auto_group_select_rows") or []
            selection_ids = [
                int(row.get("ID"))
                for row in selection_rows
                if _safe_int(row.get("ID")) is not None
            ]
            if selection_ids != auto_ids:
                selection_rows = [
                    {
                        "Seleccionar": True,
                        "ID": int(row.get("clase_id")),
                        "Clase": str(row.get("clase_nombre") or ""),
                    }
                    for row in auto_rows
                    if _safe_int(row.get("clase_id")) is not None
                ]
                st.session_state["clases_auto_group_select_rows"] = selection_rows
                st.session_state["clases_auto_group_selected_ids"] = auto_ids
                st.session_state["clases_auto_group_select_all"] = True
                st.session_state["clases_auto_group_select_all_last"] = True

            st.markdown("**Clases para asignacion/eliminacion de participantes**")
            select_all_auto = st.checkbox(
                "Seleccionar todo para participantes",
                value=bool(st.session_state.get("clases_auto_group_select_all", True)),
                key="clases_auto_group_select_all_ui",
            )
            st.session_state["clases_auto_group_select_all"] = bool(select_all_auto)
            if (
                st.session_state.get("clases_auto_group_select_all_last")
                != bool(st.session_state.get("clases_auto_group_select_all", True))
            ):
                rows_toggle = st.session_state.get("clases_auto_group_select_rows") or []
                for row in rows_toggle:
                    row["Seleccionar"] = bool(
                        st.session_state.get("clases_auto_group_select_all", True)
                    )
                st.session_state["clases_auto_group_select_rows"] = rows_toggle
                st.session_state["clases_auto_group_select_all_last"] = bool(
                    st.session_state.get("clases_auto_group_select_all", True)
                )

            auto_select_df = pd.DataFrame(
                st.session_state.get("clases_auto_group_select_rows") or []
            )
            edited_auto_df = st.data_editor(
                auto_select_df,
                key="clases_auto_group_select_editor",
                hide_index=True,
                use_container_width=True,
                disabled=["ID", "Clase"],
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn("Seleccionar"),
                    "ID": st.column_config.NumberColumn("ID", format="%d"),
                    "Clase": st.column_config.TextColumn("Clase"),
                },
            )
            if isinstance(edited_auto_df, pd.DataFrame):
                edited_auto_rows = edited_auto_df.to_dict("records")
                st.session_state["clases_auto_group_select_rows"] = edited_auto_rows
                selected_auto_ids = [
                    int(row.get("ID"))
                    for row in edited_auto_rows
                    if bool(row.get("Seleccionar")) and _safe_int(row.get("ID")) is not None
                ]
                st.session_state["clases_auto_group_selected_ids"] = selected_auto_ids
                st.caption(f"Seleccionadas: {len(selected_auto_ids)}/{len(edited_auto_rows)}")
        else:
            st.session_state["clases_auto_group_selected_ids"] = []

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
    
            selected_raw = st.session_state.get("clases_auto_group_selected_ids") or []
            selected_ids_auto: List[int] = []
            selected_seen = set()
            for value in selected_raw:
                clase_id_tmp = _safe_int(value)
                if clase_id_tmp is None:
                    continue
                if int(clase_id_tmp) in selected_seen:
                    continue
                selected_seen.add(int(clase_id_tmp))
                selected_ids_auto.append(int(clase_id_tmp))

            if not selected_ids_auto:
                st.error("Selecciona al menos una clase para ejecutar participantes.")
                st.stop()

            selected_set_auto = set(selected_ids_auto)
            rows_auto_selected = [
                row
                for row in rows_auto
                if _safe_int(row.get("clase_id")) is not None
                and int(row.get("clase_id")) in selected_set_auto
            ]
            if not rows_auto_selected:
                st.error("No hay clases validas seleccionadas para ejecutar.")
                st.stop()

            total = len(rows_auto_selected)
    
            if run_eliminar_participantes:
                resultados_delete: List[Dict[str, object]] = []
                ok_count = 0
                skip_count = 0
                err_count = 0
                progress = st.progress(0)
                status = st.empty()
    
                for idx, row in enumerate(rows_auto_selected, start=1):
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
    
                for idx, row in enumerate(rows_auto_selected, start=1):
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
            options_delete = sorted(
                options_delete,
                key=lambda row: (
                    str(row.get("nombre") or "").upper(),
                    int(row.get("id") or 0),
                ),
            )
            st.session_state["clases_delete_options"] = options_delete
            st.session_state["clases_delete_context"] = {
                "colegio_id": int(colegio_id_int),
                "ciclo_id": int(ciclo_id),
                "empresa_id": int(empresa_id),
            }
            rows_delete = [
                {
                    "Seleccionar": True,
                    "ID": int(item.get("id")),
                    "Clase": str(item.get("nombre") or ""),
                }
                for item in options_delete
            ]
            st.session_state["clases_delete_rows"] = rows_delete
            st.session_state["clases_delete_selected_ids"] = [
                int(item.get("id")) for item in options_delete
            ]
            st.session_state["clases_delete_select_all"] = True
            st.session_state["clases_delete_select_all_last"] = True
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
                st.session_state["clases_delete_rows"] = [
                    {
                        "Seleccionar": True,
                        "ID": int(item.get("id")),
                        "Clase": str(item.get("nombre") or ""),
                    }
                    for item in filtered_options
                ]
                st.session_state["clases_delete_selected_ids"] = [
                    int(item.get("id")) for item in filtered_options
                ]
                st.session_state["clases_delete_select_all"] = True
                st.session_state["clases_delete_select_all_last"] = True
    
            st.success(
                f"Eliminacion seleccionada completada. OK: {ok_count} | Errores: {err_count}"
            )
            if resultados_delete:
                _show_dataframe(resultados_delete, use_container_width=True)
    
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
                        f"ediciÃ³n masiva: {exc}"
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
                        st.caption(f"... y {restantes} errores mÃ¡s.")




