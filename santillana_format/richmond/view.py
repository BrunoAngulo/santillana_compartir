from __future__ import annotations

"""Richmond Studio domain view and helpers.

This module owns the Richmond Studio Streamlit flow. The app entrypoint
imports only the facade exposed by ``santillana_format.richmond``.
"""

import csv
import os
import re
import unicodedata
from datetime import date, datetime
from io import BytesIO, StringIO
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin
from uuid import uuid4

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _clean_token_value(token: object) -> str:
    text = str(token or "").strip()
    return re.sub(r"^bearer\s+", "", text, flags=re.IGNORECASE).strip()


def _clean_token(token: str) -> str:
    return _clean_token_value(token)


def _normalize_plain_text(value: object) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text


def _normalize_compare_text(value: object) -> str:
    text = _normalize_plain_text(value)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return text.strip()


def _safe_int(value: object) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def _show_dataframe(data: object, use_container_width: bool = True) -> None:
    if isinstance(data, pd.DataFrame):
        df_view = data.copy()
    else:
        df_view = pd.DataFrame(data)
    if not df_view.empty:
        df_view.index = range(1, len(df_view) + 1)
    st.dataframe(df_view, use_container_width=use_container_width)


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


RICHMONDSTUDIO_USERS_URL = "https://richmondstudio.global/api/users"

RICHMONDSTUDIO_GROUPS_URL = "https://richmondstudio.global/api/groups"

RICHMONDSTUDIO_CURRENT_USER_URL = "https://richmondstudio.global/api/users/current"

RICHMONDSTUDIO_BULK_USER_EDITION_URL = (
    "https://richmondstudio.global/api/administration/users/bulk/user-edition"
)

RICHMONDSTUDIO_TOKEN_BRIDGE_PENDING = "__pending__"

RICHMONDSTUDIO_TOKEN_BRIDGE_COMPONENT = components.declare_component(
    "richmondstudio_token_bridge",
    path=str(
        PROJECT_ROOT / "components" / "richmondstudio_token_bridge"
    ),
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

def _read_browser_richmondstudio_token(mode: str = "read", value: object = "") -> str:
    try:
        browser_value = RICHMONDSTUDIO_TOKEN_BRIDGE_COMPONENT(
            key="richmondstudio_token_bridge_component",
            default=RICHMONDSTUDIO_TOKEN_BRIDGE_PENDING,
            mode=str(mode or "read").strip().lower() or "read",
            value=_clean_token_value(value),
        )
    except Exception:
        return ""
    if str(browser_value or "") == RICHMONDSTUDIO_TOKEN_BRIDGE_PENDING:
        return RICHMONDSTUDIO_TOKEN_BRIDGE_PENDING
    return _clean_token_value(browser_value)

def read_richmondstudio_browser_token(mode: str = "read", value: object = "") -> str:
    return _read_browser_richmondstudio_token(mode=mode, value=value)

def _sync_richmondstudio_token_from_input() -> None:
    token_input = _clean_token_value(
        st.session_state.get("rs_groups_bearer_token_input", "")
    )
    st.session_state["rs_groups_bearer_token"] = token_input
    st.session_state["rs_bearer_token"] = token_input

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

def _richmondstudio_bulk_user_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://richmondstudio.global",
        "Referer": "https://richmondstudio.global/settings/users",
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
    by_code: Dict[str, List[Dict[str, object]]] = {}
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
        code_key = _normalize_compare_text(group_code)
        if code_key:
            by_code.setdefault(code_key, []).append(meta)
        name_key = _normalize_compare_text(class_name)
        if name_key:
            by_name.setdefault(name_key, []).append(meta)

    return {
        "by_id": by_id,
        "by_code": by_code,
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
    group_by_code = groups_lookup.get("by_code") if isinstance(groups_lookup.get("by_code"), dict) else {}
    group_by_name = groups_lookup.get("by_name") if isinstance(groups_lookup.get("by_name"), dict) else {}

    by_id_match = group_by_id.get(raw)
    if isinstance(by_id_match, dict):
        return by_id_match

    code_key = _normalize_compare_text(raw)
    code_matches = group_by_code.get(code_key) if isinstance(group_by_code, dict) else None
    if isinstance(code_matches, list) and code_matches:
        if len(code_matches) > 1:
            raise ValueError(f"Codigo de clase ambiguo en RS: {raw}")
        code_match = code_matches[0]
        return dict(code_match) if isinstance(code_match, dict) else None

    name_key = code_key
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
    url = f"{RICHMONDSTUDIO_USERS_URL}/{user_id_txt}"

    def _request_update(method: str) -> Tuple[requests.Response, object]:
        try:
            response = requests.request(
                method.upper(),
                url,
                headers=_richmondstudio_headers(token),
                json=payload,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Error de red: {exc}") from exc

        try:
            body_local = response.json() if response.content else {}
        except ValueError:
            body_local = None
        return response, body_local

    response, body = _request_update("PUT")
    status_code = response.status_code
    if response.ok:
        if body is None:
            raise RuntimeError(f"Respuesta no JSON (status {status_code})")
        if not isinstance(body, dict):
            raise RuntimeError("Respuesta invalida al actualizar usuario en RS.")
        return body

    put_error = _richmondstudio_response_error(response, status_code, body)
    if int(status_code) not in {404, 405, 501}:
        raise RuntimeError(put_error)

    patch_response, patch_body = _request_update("PATCH")
    patch_status_code = patch_response.status_code
    if not patch_response.ok:
        patch_error = _richmondstudio_response_error(
            patch_response,
            patch_status_code,
            patch_body,
        )
        raise RuntimeError(
            "PUT fallo ({put_status}) y PATCH tambien fallo: {patch_error}".format(
                put_status=status_code,
                patch_error=patch_error,
            )
        )
    if patch_body is None:
        raise RuntimeError(f"Respuesta no JSON (status {patch_status_code})")
    if not isinstance(patch_body, dict):
        raise RuntimeError("Respuesta invalida al actualizar usuario en RS.")
    return patch_body

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

def _richmondstudio_parse_year_month(value: object) -> Optional[Tuple[int, int]]:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        return int(parsed.year), int(parsed.month)
    except ValueError:
        match = re.match(r"^(\d{4})-(\d{2})", text)
        if match:
            try:
                return int(match.group(1)), int(match.group(2))
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

def _richmondstudio_subscription_rows_expiring_in_year(
    detail_body: Dict[str, object],
    year: int,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for row in _richmondstudio_subscription_rows_from_detail(detail_body):
        expiration_date = _richmondstudio_parse_year_month(row.get("expiration_date"))
        if expiration_date is None:
            continue
        expiration_year, _ = expiration_date
        if int(expiration_year) != int(year):
            continue
        rows.append(dict(row))
    return rows

def _list_richmondstudio_users_with_subscriptions_expiring_in_year(
    token: str,
    rows: List[Dict[str, object]],
    timeout: int = 30,
    target_year: Optional[int] = None,
    on_status: Optional[Callable[[str], None]] = None,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> Tuple[Dict[str, int], List[Dict[str, str]]]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message or ""))
            except Exception:
                pass

    def _progress(current: int, total: int) -> None:
        if callable(on_progress):
            try:
                on_progress(int(current), int(total))
            except Exception:
                pass

    target_year_int = int(target_year or (date.today().year + 1))
    eligible_rows = [
        row
        for row in rows
        if str(row.get("RS USER ID") or "").strip()
    ]
    summary = {
        "eligible_total": int(len(eligible_rows)),
        "processed_total": 0,
        "matched_total": 0,
        "error_total": 0,
        "subscriptions_total": 0,
    }
    result_rows: List[Dict[str, str]] = []

    total_rows = len(eligible_rows)
    if total_rows <= 0:
        _progress(1, 1)
        return summary, result_rows

    _progress(0, total_rows)
    for idx_row, row in enumerate(eligible_rows, start=1):
        user_id = str(row.get("RS USER ID") or "").strip()
        first_name = str(row.get("First name") or "").strip()
        last_name = str(row.get("Last name") or "").strip()
        user_name = " ".join(
            part for part in (first_name, last_name) if part
        ).strip() or str(row.get("Username") or "").strip()
        _status(
            "Revisando suscripciones RS {idx}/{total}: {user}".format(
                idx=idx_row,
                total=max(total_rows, 1),
                user=user_name or user_id or "(sin usuario)",
            )
        )

        try:
            detail_body = _fetch_richmondstudio_user_detail(
                token=token,
                user_id=user_id,
                timeout=int(timeout),
            )
            matching_subscription_rows = (
                _richmondstudio_subscription_rows_expiring_in_year(
                    detail_body,
                    year=int(target_year_int),
                )
            )
            summary["processed_total"] += 1
            if not matching_subscription_rows:
                _progress(idx_row, total_rows)
                continue

            summary["matched_total"] += 1
            summary["subscriptions_total"] += int(len(matching_subscription_rows))

            expiration_dates_raw = [
                str(item.get("expiration_date") or "").strip()
                for item in matching_subscription_rows
                if str(item.get("expiration_date") or "").strip()
            ]
            expiration_dates = [
                _richmondstudio_date_display(item)
                for item in sorted(set(expiration_dates_raw))
                if str(item).strip()
            ]
            product_names = sorted(
                {
                    str(item.get("product_name") or "").strip()
                    for item in matching_subscription_rows
                    if str(item.get("product_name") or "").strip()
                }
            )
            subscription_ids = [
                str(item.get("id") or "").strip()
                for item in matching_subscription_rows
                if str(item.get("id") or "").strip()
            ]

            result_rows.append(
                {
                    "RS USER ID": user_id,
                    "USER NAME": user_name,
                    "Username": str(row.get("Username") or "").strip(),
                    "Email": str(row.get("Email") or "").strip(),
                    "Role": str(row.get("Role") or "").strip(),
                    "IDENTIFIER": str(row.get("IDENTIFIER") or "").strip(),
                    "level": str(row.get("level") or "").strip(),
                    "CLASSES COUNT": str(row.get("Classes count") or "").strip(),
                    "CLASS NAMES": str(row.get("CLASS NAMES") or "").strip(),
                    "CLASS CODES": str(row.get("CLASS CODES") or "").strip(),
                    "SUBSCRIPTIONS EXPIRING": str(len(matching_subscription_rows)),
                    "EXPIRATION DATES": " | ".join(expiration_dates),
                    "PRODUCT NAMES": " | ".join(product_names),
                    "SUBSCRIPTION IDS": " | ".join(subscription_ids),
                    "createdAt": str(row.get("createdAt") or "").strip(),
                    "lastSignInAt": str(row.get("lastSignInAt") or "").strip(),
                    "STATUS": "COINCIDE",
                    "DETAIL": (
                        "Tiene {count} suscripcion(es) con expirationDate en {year}.".format(
                            count=len(matching_subscription_rows),
                            year=target_year_int,
                        )
                    ),
                }
            )
        except Exception as exc:
            summary["error_total"] += 1
            result_rows.append(
                {
                    "RS USER ID": user_id,
                    "USER NAME": user_name,
                    "Username": str(row.get("Username") or "").strip(),
                    "Email": str(row.get("Email") or "").strip(),
                    "Role": str(row.get("Role") or "").strip(),
                    "IDENTIFIER": str(row.get("IDENTIFIER") or "").strip(),
                    "level": str(row.get("level") or "").strip(),
                    "CLASSES COUNT": str(row.get("Classes count") or "").strip(),
                    "CLASS NAMES": str(row.get("CLASS NAMES") or "").strip(),
                    "CLASS CODES": str(row.get("CLASS CODES") or "").strip(),
                    "SUBSCRIPTIONS EXPIRING": "0",
                    "EXPIRATION DATES": "",
                    "PRODUCT NAMES": "",
                    "SUBSCRIPTION IDS": "",
                    "createdAt": str(row.get("createdAt") or "").strip(),
                    "lastSignInAt": str(row.get("lastSignInAt") or "").strip(),
                    "STATUS": "ERROR",
                    "DETAIL": str(exc).strip() or "sin detalle",
                }
            )
        finally:
            _progress(idx_row, total_rows)

    result_rows = sorted(
        result_rows,
        key=lambda item: (
            str(item.get("STATUS") or "").upper() == "ERROR",
            -int(_safe_int(item.get("SUBSCRIPTIONS EXPIRING")) or 0),
            str(item.get("USER NAME") or "").lower(),
        ),
    )
    return summary, result_rows

def _build_richmondstudio_user_patch_payload_from_detail(
    detail_body: Dict[str, object],
    subscription_ids: Optional[Sequence[object]] = None,
    group_ids: Optional[Sequence[object]] = None,
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

    normalized_group_ids: List[str] = []
    seen_group_ids = set()
    for item in (
        group_ids
        if group_ids is not None
        else _richmondstudio_relationship_ids(data, "groups")
    ):
        group_id = str(item or "").strip()
        if not group_id or group_id in seen_group_ids:
            continue
        seen_group_ids.add(group_id)
        normalized_group_ids.append(group_id)

    normalized_subscription_ids: List[str] = []
    seen_subscription_ids = set()
    for item in (
        subscription_ids
        if subscription_ids is not None
        else _richmondstudio_relationship_ids(data, "subscriptions")
    ):
        subscription_id = str(item or "").strip()
        if not subscription_id or subscription_id in seen_subscription_ids:
            continue
        seen_subscription_ids.add(subscription_id)
        normalized_subscription_ids.append(subscription_id)

    payload_attributes = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "role": role,
    }
    level = str(attrs.get("level") or "").strip().lower()
    if level:
        payload_attributes["level"] = level
    if "teachermatic" in attrs:
        payload_attributes["teachermatic"] = bool(attrs.get("teachermatic"))

    return {
        "data": {
            "type": "users",
            "id": user_id,
            "attributes": payload_attributes,
            "relationships": {
                "groups": {
                    "data": [
                        {"type": "groups", "id": group_id}
                        for group_id in normalized_group_ids
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
    registered_user_rows: List[Dict[str, str]] = []
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
        level = str(attrs.get("level") or "").strip().lower()
        student_name = " ".join(part for part in [first_name, last_name] if part).strip()
        identifier = str(attrs.get("identifier") or "").strip()
        email = str(attrs.get("email") or "").strip()
        login = (
            email
            or str(attrs.get("login") or "").strip()
            or str(attrs.get("username") or "").strip()
            or identifier
        )
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

        primary_class_name = class_names[0] if len(class_names) == 1 else ""
        primary_class_code = class_codes[0] if len(class_codes) == 1 else ""
        registered_user_rows.append(
            {
                "RS USER ID": user_id,
                "Username": login,
                "Login": login,
                "Email": email,
                "Role": role,
                "IDENTIFIER": identifier,
                "First name": first_name,
                "Last name": last_name,
                "level": level,
                "Class name": primary_class_name,
                "Class code": primary_class_code,
                "CLASS NAMES": " | ".join(class_names),
                "CLASS CODES": " | ".join(class_codes),
                "Classes count": str(len(group_ids)),
                "createdAt": created_at,
                "lastSignInAt": last_sign_in_at,
                "_group_ids": list(group_ids),
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
    registered_user_rows = sorted(
        registered_user_rows,
        key=lambda row: (
            str(row.get("Class name") or "").lower(),
            str(row.get("Last name") or "").lower(),
            str(row.get("First name") or "").lower(),
            str(row.get("Username") or "").lower(),
        ),
    )
    return {
        "registered_rows": registered_rows,
        "registered_user_rows": registered_user_rows,
        "excluded_roles": excluded_roles,
        "valid_users_count": int(len(filtered_users)),
        "total_users_count": int(len(rs_users)),
    }

def _load_richmondstudio_registered_panel_data(
    token: str,
    timeout: int = 30,
) -> Dict[str, object]:
    institution_name = ""
    try:
        current_context = _fetch_richmondstudio_current_user_context(
            token,
            timeout=int(timeout),
        )
    except Exception:
        current_context = {}

    institution_name = str(
        current_context.get("institution_name")
        if isinstance(current_context, dict)
        else ""
    ).strip()
    rs_users = _fetch_richmondstudio_users(token, timeout=int(timeout))
    rs_groups = _fetch_richmondstudio_groups(token, timeout=int(timeout))
    return {
        "institution_name": institution_name,
        "listing_data": _build_richmondstudio_registered_listing_data(
            rs_users,
            rs_groups,
        ),
        "groups_lookup": _build_richmondstudio_groups_lookup(rs_groups),
    }

def _store_richmondstudio_registered_panel_data(
    panel_data: Dict[str, object],
) -> Dict[str, object]:
    listing_data = (
        panel_data.get("listing_data")
        if isinstance(panel_data.get("listing_data"), dict)
        else {}
    )
    institution_name = str(panel_data.get("institution_name") or "").strip()
    groups_lookup = (
        panel_data.get("groups_lookup")
        if isinstance(panel_data.get("groups_lookup"), dict)
        else {"by_id": {}, "by_name": {}}
    )

    rows_rs = list(listing_data.get("registered_rows") or [])
    rs_registered_user_rows = list(listing_data.get("registered_user_rows") or [])
    excluded_roles = (
        listing_data.get("excluded_roles")
        if isinstance(listing_data.get("excluded_roles"), dict)
        else {}
    )

    rs_password_template_rows = _build_richmondstudio_password_update_template_rows(
        rs_registered_user_rows
    )
    st.session_state["rs_excel_bytes"] = _export_simple_excel(
        rows_rs,
        sheet_name="users",
    )
    st.session_state["rs_excel_count"] = int(len(rows_rs))
    st.session_state["rs_registered_user_rows"] = rs_registered_user_rows
    st.session_state["rs_registered_groups_lookup"] = groups_lookup
    st.session_state["rs_password_update_template_bytes"] = (
        _export_simple_excel(
            rs_password_template_rows,
            sheet_name="password_update_rs",
        )
        if rs_password_template_rows
        else b""
    )
    st.session_state["rs_password_update_template_count"] = int(
        len(rs_password_template_rows)
    )
    st.session_state["rs_password_update_template_filename"] = (
        _build_richmondstudio_password_update_filename(
            institution_name,
            prefix="plantilla_password_rs",
        )
    )
    for state_key in (
        "rs_multi_class_students_rows",
        "rs_multi_class_students_bytes",
        "rs_expiring_next_year_summary",
        "rs_expiring_next_year_rows",
        "rs_expiring_next_year_bytes",
    ):
        st.session_state.pop(state_key, None)

    return {
        "institution_name": institution_name,
        "listing_data": listing_data,
        "rows": rows_rs,
        "registered_user_rows": rs_registered_user_rows,
        "excluded_roles": excluded_roles,
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

def _request_richmondstudio_confirmation(
    action_key: str,
    action_label: str,
    token: Optional[str] = None,
    timeout: Optional[int] = None,
) -> None:
    rs_token = _clean_token(token or st.session_state.get("rs_bearer_token", ""))
    if not rs_token:
        st.error("Ingresa el bearer token de Richmond Studio.")
        return
    timeout_value = int(timeout or st.session_state.get("rs_timeout") or 30)
    try:
        with st.spinner("Validando institucion RS..."):
            current_context = _fetch_richmondstudio_current_user_context(
                rs_token,
                timeout=timeout_value,
            )
    except Exception as exc:
        st.error(f"No se pudo obtener la institucion actual de RS: {exc}")
        return
    _set_richmondstudio_pending_confirmation(
        action_key=action_key,
        action_label=action_label,
        context=current_context,
    )

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

def _build_richmondstudio_password_update_filename(
    institution_name: object,
    prefix: str = "actualizacion_password_rs",
) -> str:
    raw = str(institution_name or "").strip()
    cleaned = re.sub(r'[\\/:*?"<>|]+', " ", raw)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if cleaned:
        return f"{prefix}_{cleaned}.xlsx"
    return f"{prefix}.xlsx"

def _build_richmondstudio_password_update_template_rows(
    rows: List[Dict[str, object]],
) -> List[Dict[str, str]]:
    template_rows: List[Dict[str, str]] = []
    seen_usernames: Set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        username = (
            str(row.get("Username") or "").strip()
            or str(row.get("Login") or "").strip()
            or str(row.get("Email") or "").strip()
            or str(row.get("IDENTIFIER") or "").strip()
        )
        if not username:
            continue
        username_key = _normalize_plain_text(username)
        if username_key in seen_usernames:
            continue
        seen_usernames.add(username_key)

        class_codes_raw = (
            str(row.get("Class code") or "").strip()
            or str(row.get("CLASS CODE") or "").strip()
            or str(row.get("CLASS CODES") or "").strip()
        )
        normalized_class_code = ""
        if class_codes_raw:
            class_code_parts = [
                part.strip()
                for part in re.split(r"[|;,]+", class_codes_raw)
                if str(part).strip()
            ]
            if len(class_code_parts) == 1:
                normalized_class_code = class_code_parts[0]

        last_name = str(
            row.get("Last name")
            or row.get("LAST NAME")
            or row.get("Apellido")
            or ""
        ).strip()
        first_name = str(
            row.get("First name")
            or row.get("FIRST NAME")
            or row.get("Nombre")
            or ""
        ).strip()
        template_rows.append(
            {
                "Username(Email)": username,
                "New last name(optional)": last_name,
                "New first name(optional)": first_name,
                "New class code(optional)": normalized_class_code,
                "New password(optional)": "",
                "Keep in classes(optional)": "yes",
            }
        )

    template_rows.sort(
        key=lambda item: (
            _normalize_plain_text(item.get("New last name(optional)")),
            _normalize_plain_text(item.get("New first name(optional)")),
            _normalize_plain_text(item.get("Username(Email)")),
        )
    )
    return template_rows

def _build_richmondstudio_class_sync_create_payload(
    first_name: object,
    last_name: object,
    email: object,
    level: object,
    group_ids: Sequence[object],
) -> Dict[str, object]:
    first_name_txt = str(first_name or "").strip()
    last_name_txt = str(last_name or "").strip()
    email_txt = str(email or "").strip()
    level_txt = _normalize_richmondstudio_user_level(level)

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

    return {
        "data": {
            "type": "users",
            "attributes": {
                "first_name": first_name_txt,
                "last_name": last_name_txt,
                "email": email_txt,
                "role": "student",
                "level": level_txt,
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

def _build_richmondstudio_class_sync_template_rows(
    rows: List[Dict[str, object]],
    groups_lookup: Dict[str, Dict[str, object]],
) -> List[Dict[str, str]]:
    template_rows: List[Dict[str, str]] = []
    groups_by_id = (
        groups_lookup.get("by_id")
        if isinstance(groups_lookup.get("by_id"), dict)
        else {}
    )

    for row in rows:
        if not isinstance(row, dict):
            continue
        username = (
            str(row.get("Email") or "").strip()
            or str(row.get("Username") or "").strip()
            or str(row.get("Login") or "").strip()
        )
        if not username:
            continue

        group_ids = [
            str(item or "").strip()
            for item in (row.get("_group_ids") or [])
            if str(item or "").strip()
        ]
        if not group_ids:
            template_rows.append(
                {
                    RICHMONDSTUDIO_USER_IMPORT_LAST_NAME: str(
                        row.get("Last name") or ""
                    ).strip(),
                    RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME: str(
                        row.get("First name") or ""
                    ).strip(),
                    RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME: "",
                    RICHMONDSTUDIO_USER_IMPORT_EMAIL: username,
                    RICHMONDSTUDIO_USER_IMPORT_ROLE: str(
                        row.get("Role") or "Student"
                    ).strip().title()
                    or "Student",
                    RICHMONDSTUDIO_USER_IMPORT_LEVEL: str(
                        row.get("level") or ""
                    ).strip(),
                }
            )
            continue

        for group_id in group_ids:
            group_meta = groups_by_id.get(group_id) or {}
            template_rows.append(
                {
                    RICHMONDSTUDIO_USER_IMPORT_LAST_NAME: str(
                        row.get("Last name") or ""
                    ).strip(),
                    RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME: str(
                        row.get("First name") or ""
                    ).strip(),
                    RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME: str(
                        group_meta.get("class_name") or ""
                    ).strip(),
                    RICHMONDSTUDIO_USER_IMPORT_EMAIL: username,
                    RICHMONDSTUDIO_USER_IMPORT_ROLE: str(
                        row.get("Role") or "Student"
                    ).strip().title()
                    or "Student",
                    RICHMONDSTUDIO_USER_IMPORT_LEVEL: str(
                        row.get("level") or ""
                    ).strip(),
                }
            )

    template_rows.sort(
        key=lambda item: (
            _normalize_plain_text(item.get(RICHMONDSTUDIO_USER_IMPORT_EMAIL)),
            _normalize_plain_text(item.get(RICHMONDSTUDIO_USER_IMPORT_LAST_NAME)),
            _normalize_plain_text(item.get(RICHMONDSTUDIO_USER_IMPORT_FIRST_NAME)),
            _normalize_plain_text(item.get(RICHMONDSTUDIO_USER_IMPORT_CLASS_NAME)),
        )
    )
    return template_rows

def _load_richmondstudio_bulk_class_sync_rows(
    file_bytes: bytes,
    file_name: str,
) -> List[Dict[str, str]]:
    file_name_txt = str(file_name or "").strip().lower()
    if not file_bytes:
        raise ValueError("El archivo de clases RS esta vacio.")

    if file_name_txt.endswith((".csv", ".txt")):
        try:
            df = pd.read_csv(BytesIO(file_bytes), dtype=str).fillna("")
        except Exception as exc:
            raise ValueError(f"No se pudo leer el CSV de clases RS: {exc}") from exc
    else:
        try:
            df = pd.read_excel(BytesIO(file_bytes), dtype=str).fillna("")
        except Exception as exc:
            raise ValueError(f"No se pudo leer el Excel de clases RS: {exc}") from exc

    header_aliases = {
        "USERNAME": "Username",
        "EMAIL": "Username",
        "EMAIL* MANDATORY": "Username",
        "EMAIL MANDATORY": "Username",
        "CORREO": "Username",
        "USERNAME EMAIL": "Username",
        "USERNAME(EMAIL)": "Username",
        "FIRST NAME": "First name",
        "FIRST NAME* MANDATORY": "First name",
        "FIRST NAME MANDATORY": "First name",
        "FIRSTNAME": "First name",
        "NOMBRE": "First name",
        "LAST NAME": "Last name",
        "LAST NAME* MANDATORY": "Last name",
        "LAST NAME MANDATORY": "Last name",
        "LASTNAME": "Last name",
        "APELLIDO": "Last name",
        "LEVEL": "level",
        "NIVEL": "level",
        "CLASS": "Class name",
        "CLASS OPTIONAL": "Class name",
        "CLASS NAME": "Class name",
        "CLASE": "Class name",
        "NOMBRE CLASE": "Class name",
        "CLASS CODE": "Class code",
        "CLASSCODE": "Class code",
        "CODIGO CLASE": "Class code",
        "CODIGO DE CLASE": "Class code",
    }

    normalized_columns: Dict[str, str] = {}
    for column in df.columns:
        normalized = re.sub(r"\s+", " ", _normalize_plain_text(column))
        canonical = header_aliases.get(normalized, "")
        if canonical and canonical not in normalized_columns:
            normalized_columns[canonical] = column

    if "Username" not in normalized_columns:
        raise ValueError(
            "Falta la columna Username(Email) o Email en el archivo de clases RS."
        )
    if (
        "Class name" not in normalized_columns
        and "Class code" not in normalized_columns
    ):
        raise ValueError(
            "Falta la columna Class name o Class code en el archivo de clases RS."
        )

    rows: List[Dict[str, str]] = []
    for idx, item in enumerate(df.to_dict("records"), start=2):
        if not isinstance(item, dict):
            continue
        username = str(
            item.get(normalized_columns.get("Username", "")) or ""
        ).strip()
        first_name = str(
            item.get(normalized_columns.get("First name", "")) or ""
        ).strip()
        last_name = str(
            item.get(normalized_columns.get("Last name", "")) or ""
        ).strip()
        level = str(
            item.get(normalized_columns.get("level", "")) or ""
        ).strip()
        class_name = str(
            item.get(normalized_columns.get("Class name", "")) or ""
        ).strip()
        class_code = str(
            item.get(normalized_columns.get("Class code", "")) or ""
        ).strip()

        if not username and not class_name and not class_code:
            continue
        if not username:
            raise ValueError(f"Falta Username(Email) en la fila {idx}.")
        if not class_name and not class_code:
            continue

        rows.append(
            {
                "Username": username,
                "First name": first_name,
                "Last name": last_name,
                "level": level,
                "Class name": class_name,
                "Class code": class_code,
            }
        )

    if not rows:
        raise ValueError(
            "El archivo no tiene filas con Username(Email) y clase para procesar."
        )
    return rows

def _build_richmondstudio_bulk_class_sync_preview_rows(
    rows: List[Dict[str, str]],
) -> List[Dict[str, object]]:
    grouped: Dict[str, Dict[str, object]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        username = str(row.get("Username") or "").strip()
        if not username:
            continue
        bucket = grouped.setdefault(
            username,
            {
                "Username(Email)": username,
                "First name": str(row.get("First name") or "").strip(),
                "Last name": str(row.get("Last name") or "").strip(),
                "level": str(row.get("level") or "").strip(),
                "_classes": [],
            },
        )
        if not str(bucket.get("First name") or "").strip():
            bucket["First name"] = str(row.get("First name") or "").strip()
        if not str(bucket.get("Last name") or "").strip():
            bucket["Last name"] = str(row.get("Last name") or "").strip()
        if not str(bucket.get("level") or "").strip():
            bucket["level"] = str(row.get("level") or "").strip()
        class_label = str(row.get("Class code") or "").strip() or str(
            row.get("Class name") or ""
        ).strip()
        if class_label and class_label not in bucket["_classes"]:
            bucket["_classes"].append(class_label)

    preview_rows: List[Dict[str, object]] = []
    for username, bucket in grouped.items():
        classes = list(bucket.get("_classes") or [])
        preview_rows.append(
            {
                "Username(Email)": username,
                "First name": str(bucket.get("First name") or "").strip(),
                "Last name": str(bucket.get("Last name") or "").strip(),
                "level": str(bucket.get("level") or "").strip(),
                "Classes requested": len(classes),
                "Classes": " | ".join(classes),
            }
        )

    preview_rows.sort(
        key=lambda item: _normalize_plain_text(item.get("Username(Email)"))
    )
    return preview_rows

def _build_richmondstudio_group_labels_from_ids(
    group_ids: Sequence[object],
    groups_lookup: Dict[str, Dict[str, object]],
) -> List[str]:
    group_by_id = (
        groups_lookup.get("by_id")
        if isinstance(groups_lookup.get("by_id"), dict)
        else {}
    )
    labels: List[str] = []
    seen_labels = set()
    for item in group_ids or []:
        group_id = str(item or "").strip()
        if not group_id:
            continue
        label = _richmondstudio_group_label(group_by_id.get(group_id)) or group_id
        if label in seen_labels:
            continue
        seen_labels.add(label)
        labels.append(label)
    return labels

def _build_richmondstudio_bulk_class_refresh_preview(
    rows: List[Dict[str, str]],
    registered_user_rows: List[Dict[str, object]],
    groups_lookup: Dict[str, Dict[str, object]],
) -> Tuple[Dict[str, int], List[Dict[str, object]]]:
    user_row_by_key: Dict[str, Dict[str, object]] = {}
    for row in registered_user_rows:
        if not isinstance(row, dict):
            continue
        for raw in (
            row.get("Email"),
            row.get("Username"),
            row.get("Login"),
            row.get("IDENTIFIER"),
        ):
            key = _normalize_compare_text(raw)
            if key and key not in user_row_by_key:
                user_row_by_key[key] = row

    pending_existing_by_user_id: Dict[str, Dict[str, object]] = {}
    pending_create_by_username: Dict[str, Dict[str, object]] = {}
    preview_rows: List[Dict[str, object]] = []
    summary = {
        "input_rows": int(len(rows)),
        "users_found": 0,
        "users_to_replace": 0,
        "users_unchanged": 0,
        "users_to_create": 0,
        "error_total": 0,
    }

    for row in rows:
        username = str(row.get("Username") or "").strip()
        first_name = str(row.get("First name") or "").strip()
        last_name = str(row.get("Last name") or "").strip()
        level = str(row.get("level") or "").strip()
        class_code = str(row.get("Class code") or "").strip()
        class_name = str(row.get("Class name") or "").strip()
        class_value = class_code or class_name
        if not username or not class_value:
            continue

        user_row = user_row_by_key.get(_normalize_compare_text(username)) or {}
        user_id = str(user_row.get("RS USER ID") or "").strip()

        try:
            group_meta = _resolve_richmondstudio_group_for_user_row(
                class_value,
                groups_lookup,
            )
        except Exception as exc:
            summary["error_total"] += 1
            preview_rows.append(
                {
                    "Username(Email)": username,
                    "First name": first_name,
                    "Last name": last_name,
                    "RS USER ID": user_id,
                    "Estado actual RS": "",
                    "Quedara en RS": "",
                    "Agregar": "",
                    "Quitar": "",
                    "Accion": "ERROR",
                    "Detalle": str(exc),
                }
            )
            continue

        group_id = str(
            (group_meta.get("id") if isinstance(group_meta, dict) else "") or ""
        ).strip()
        if not group_id:
            summary["error_total"] += 1
            preview_rows.append(
                {
                    "Username(Email)": username,
                    "First name": first_name,
                    "Last name": last_name,
                    "RS USER ID": user_id,
                    "Estado actual RS": "",
                    "Quedara en RS": "",
                    "Agregar": "",
                    "Quitar": "",
                    "Accion": "ERROR",
                    "Detalle": "No se pudo resolver la clase RS.",
                }
            )
            continue

        if user_id:
            bucket = pending_existing_by_user_id.setdefault(
                user_id,
                {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "user_id": user_id,
                    "current_group_ids": list(user_row.get("_group_ids") or []),
                    "target_group_ids": [],
                    "target_group_labels": [],
                },
            )
        else:
            bucket = pending_create_by_username.setdefault(
                _normalize_compare_text(username),
                {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "level": level,
                    "target_group_ids": [],
                    "target_group_labels": [],
                },
            )

        if group_id not in bucket["target_group_ids"]:
            bucket["target_group_ids"].append(group_id)
            bucket["target_group_labels"].append(
                _richmondstudio_group_label(group_meta) or class_value
            )

    summary["users_found"] = int(len(pending_existing_by_user_id))

    for pending in pending_existing_by_user_id.values():
        current_group_ids = [
            str(item or "").strip()
            for item in (pending.get("current_group_ids") or [])
            if str(item or "").strip()
        ]
        target_group_ids = [
            str(item or "").strip()
            for item in (pending.get("target_group_ids") or [])
            if str(item or "").strip()
        ]
        current_labels = _build_richmondstudio_group_labels_from_ids(
            current_group_ids,
            groups_lookup,
        )
        target_labels = _build_richmondstudio_group_labels_from_ids(
            target_group_ids,
            groups_lookup,
        )
        add_labels = _build_richmondstudio_group_labels_from_ids(
            [group_id for group_id in target_group_ids if group_id not in current_group_ids],
            groups_lookup,
        )
        remove_labels = _build_richmondstudio_group_labels_from_ids(
            [group_id for group_id in current_group_ids if group_id not in target_group_ids],
            groups_lookup,
        )
        action = "SIN CAMBIOS"
        detail = "El usuario ya esta exactamente en las clases del archivo."
        if add_labels or remove_labels:
            action = "REEMPLAZAR"
            detail = "Se dejara al usuario solo en las clases del archivo."
            summary["users_to_replace"] += 1
        else:
            summary["users_unchanged"] += 1

        preview_rows.append(
            {
                "Username(Email)": str(pending.get("username") or "").strip(),
                "First name": str(pending.get("first_name") or "").strip(),
                "Last name": str(pending.get("last_name") or "").strip(),
                "RS USER ID": str(pending.get("user_id") or "").strip(),
                "Estado actual RS": " | ".join(current_labels),
                "Quedara en RS": " | ".join(target_labels),
                "Agregar": " | ".join(add_labels),
                "Quitar": " | ".join(remove_labels),
                "Accion": action,
                "Detalle": detail,
            }
        )

    for pending in pending_create_by_username.values():
        target_group_ids = [
            str(item or "").strip()
            for item in (pending.get("target_group_ids") or [])
            if str(item or "").strip()
        ]
        target_labels = _build_richmondstudio_group_labels_from_ids(
            target_group_ids,
            groups_lookup,
        )
        summary["users_to_create"] += 1
        preview_rows.append(
            {
                "Username(Email)": str(pending.get("username") or "").strip(),
                "First name": str(pending.get("first_name") or "").strip(),
                "Last name": str(pending.get("last_name") or "").strip(),
                "RS USER ID": "",
                "Estado actual RS": "",
                "Quedara en RS": " | ".join(target_labels),
                "Agregar": " | ".join(target_labels),
                "Quitar": "",
                "Accion": "CREAR",
                "Detalle": "El usuario no existe en RS y se crearia con esas clases.",
            }
        )

    preview_rows.sort(
        key=lambda item: (
            str(item.get("Accion") or "").upper().startswith("ERROR") is False,
            _normalize_plain_text(item.get("Username(Email)")),
        )
    )
    return summary, preview_rows

def _sync_richmondstudio_user_classes_from_excel_rows(
    token: str,
    rows: List[Dict[str, str]],
    registered_user_rows: List[Dict[str, object]],
    groups_lookup: Dict[str, Dict[str, object]],
    timeout: int = 30,
    on_status: Optional[Callable[[str], None]] = None,
) -> Tuple[Dict[str, int], List[Dict[str, str]]]:
    def _status(message: str) -> None:
        if callable(on_status):
            try:
                on_status(str(message or ""))
            except Exception:
                pass

    user_row_by_key: Dict[str, Dict[str, object]] = {}
    for row in registered_user_rows:
        if not isinstance(row, dict):
            continue
        for raw in (
            row.get("Email"),
            row.get("Username"),
            row.get("Login"),
            row.get("IDENTIFIER"),
        ):
            key = _normalize_compare_text(raw)
            if key and key not in user_row_by_key:
                user_row_by_key[key] = row

    pending_existing_by_user_id: Dict[str, Dict[str, object]] = {}
    pending_create_by_username: Dict[str, Dict[str, object]] = {}
    result_rows: List[Dict[str, str]] = []
    summary = {
        "input_rows": int(len(rows)),
        "users_found": 0,
        "users_updated": 0,
        "users_unchanged": 0,
        "users_created": 0,
        "error_total": 0,
    }

    for idx_row, row in enumerate(rows, start=1):
        username = str(row.get("Username") or "").strip()
        first_name = str(row.get("First name") or "").strip()
        last_name = str(row.get("Last name") or "").strip()
        level = str(row.get("level") or "").strip()
        class_code = str(row.get("Class code") or "").strip()
        class_name = str(row.get("Class name") or "").strip()
        class_value = class_code or class_name
        result_row = {
            "Username(Email)": username,
            "First name": first_name,
            "Last name": last_name,
            "level": level,
            "Class name": class_name,
            "Class code": class_code,
            "RS USER ID": "",
            "STATUS": "",
            "DETAIL": "",
        }
        if not username or not class_value:
            result_row["STATUS"] = "IGNORADA"
            result_row["DETAIL"] = "Fila sin Username(Email) o clase."
            result_rows.append(result_row)
            continue

        user_row = user_row_by_key.get(_normalize_compare_text(username)) or {}
        user_id = str(user_row.get("RS USER ID") or "").strip()

        try:
            group_meta = _resolve_richmondstudio_group_for_user_row(
                class_value,
                groups_lookup,
            )
        except Exception as exc:
            summary["error_total"] += 1
            result_row["RS USER ID"] = user_id
            result_row["STATUS"] = "ERROR"
            result_row["DETAIL"] = str(exc)
            result_rows.append(result_row)
            continue

        group_id = str(
            (group_meta.get("id") if isinstance(group_meta, dict) else "") or ""
        ).strip()
        if not group_id:
            summary["error_total"] += 1
            result_row["RS USER ID"] = user_id
            result_row["STATUS"] = "ERROR"
            result_row["DETAIL"] = "No se pudo resolver la clase RS."
            result_rows.append(result_row)
            continue

        if user_id:
            bucket = pending_existing_by_user_id.setdefault(
                user_id,
                {
                    "username": username,
                    "user_id": user_id,
                    "group_ids": [],
                    "group_labels": [],
                },
            )
        else:
            create_key = _normalize_compare_text(username)
            bucket = pending_create_by_username.setdefault(
                create_key,
                {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "level": level,
                    "group_ids": [],
                    "group_labels": [],
                },
            )
            if not str(bucket.get("first_name") or "").strip():
                bucket["first_name"] = first_name
            if not str(bucket.get("last_name") or "").strip():
                bucket["last_name"] = last_name
            if not str(bucket.get("level") or "").strip():
                bucket["level"] = level
        if group_id not in bucket["group_ids"]:
            bucket["group_ids"].append(group_id)
            bucket["group_labels"].append(
                _richmondstudio_group_label(group_meta) or class_value
            )
        result_row["RS USER ID"] = user_id
        result_row["STATUS"] = "LISTA" if user_id else "LISTA CREATE"
        result_row["DETAIL"] = _richmondstudio_group_label(group_meta) or class_value
        result_rows.append(result_row)

    summary["users_found"] = int(len(pending_existing_by_user_id))

    for idx_user, pending in enumerate(pending_existing_by_user_id.values(), start=1):
        user_id = str(pending.get("user_id") or "").strip()
        username = str(pending.get("username") or "").strip()
        requested_group_ids = [
            str(item or "").strip()
            for item in (pending.get("group_ids") or [])
            if str(item or "").strip()
        ]
        _status(
            "Actualizando clases RS {idx}/{total}: {username}".format(
                idx=idx_user,
                total=max(len(pending_existing_by_user_id), 1),
                username=username or user_id,
            )
        )
        try:
            detail_body = _fetch_richmondstudio_user_detail(
                token=token,
                user_id=user_id,
                timeout=int(timeout),
            )
            detail_data = (
                detail_body.get("data")
                if isinstance(detail_body.get("data"), dict)
                else {}
            )
            current_group_ids = _richmondstudio_relationship_ids(
                detail_data,
                "groups",
            )
            final_group_ids = list(current_group_ids)
            added_group_ids: List[str] = []
            added_group_labels: List[str] = []
            for group_id in requested_group_ids:
                if group_id in final_group_ids:
                    continue
                final_group_ids.append(group_id)
                added_group_ids.append(group_id)
                for idx_label, pending_group_id in enumerate(
                    pending.get("group_ids") or []
                ):
                    if str(pending_group_id or "").strip() != group_id:
                        continue
                    label = str(
                        (pending.get("group_labels") or [])[idx_label]
                        if idx_label < len(pending.get("group_labels") or [])
                        else group_id
                    ).strip() or group_id
                    if label not in added_group_labels:
                        added_group_labels.append(label)
                    break
            if not added_group_ids:
                summary["users_unchanged"] += 1
                result_rows.append(
                    {
                        "Username(Email)": username,
                        "Class name": "",
                        "Class code": "",
                        "RS USER ID": user_id,
                        "STATUS": "SIN CAMBIOS",
                        "DETAIL": "El usuario ya estaba en las clases del Excel.",
                    }
                )
                continue
            payload = _build_richmondstudio_user_patch_payload_from_detail(
                detail_body,
                group_ids=final_group_ids,
            )
            _update_richmondstudio_user(
                token=token,
                user_id=user_id,
                payload=payload,
                timeout=int(timeout),
            )
        except Exception as exc:
            summary["error_total"] += 1
            result_rows.append(
                {
                    "Username(Email)": username,
                    "Class name": "",
                    "Class code": "",
                    "RS USER ID": user_id,
                    "STATUS": "ERROR UPDATE",
                    "DETAIL": str(exc),
                }
            )
            continue
        summary["users_updated"] += 1
        result_rows.append(
            {
                "Username(Email)": username,
                "Class name": "",
                "Class code": "",
                "RS USER ID": user_id,
                "STATUS": "ACTUALIZADO",
                "DETAIL": "Clases agregadas: {classes}".format(
                    classes=" | ".join(added_group_labels)
                    or "sin cambios",
                ),
            }
        )

    for idx_user, pending in enumerate(pending_create_by_username.values(), start=1):
        username = str(pending.get("username") or "").strip()
        first_name = str(pending.get("first_name") or "").strip()
        last_name = str(pending.get("last_name") or "").strip()
        level = str(pending.get("level") or "").strip()
        requested_group_ids = [
            str(item or "").strip()
            for item in (pending.get("group_ids") or [])
            if str(item or "").strip()
        ]
        _status(
            "Creando usuario RS {idx}/{total}: {username}".format(
                idx=idx_user,
                total=max(len(pending_create_by_username), 1),
                username=username,
            )
        )
        if not first_name or not last_name or not level:
            summary["error_total"] += 1
            result_rows.append(
                {
                    "Username(Email)": username,
                    "First name": first_name,
                    "Last name": last_name,
                    "level": level,
                    "Class name": "",
                    "Class code": "",
                    "RS USER ID": "",
                    "STATUS": "ERROR CREATE",
                    "DETAIL": (
                        "No se encontro el usuario RS y faltan First name, Last name o level para crearlo."
                    ),
                }
            )
            continue
        try:
            create_payload = _build_richmondstudio_class_sync_create_payload(
                first_name=first_name,
                last_name=last_name,
                email=username,
                level=level,
                group_ids=requested_group_ids,
            )
            created_user = _create_richmondstudio_user(
                token=token,
                payload=create_payload,
                timeout=int(timeout),
            )
            created_meta = _extract_richmondstudio_user_create_result(
                created_user,
                fallback_email=username,
            )
        except Exception as exc:
            summary["error_total"] += 1
            result_rows.append(
                {
                    "Username(Email)": username,
                    "First name": first_name,
                    "Last name": last_name,
                    "level": level,
                    "Class name": "",
                    "Class code": "",
                    "RS USER ID": "",
                    "STATUS": "ERROR CREATE",
                    "DETAIL": str(exc),
                }
            )
            continue
        summary["users_created"] += 1
        result_rows.append(
            {
                "Username(Email)": username,
                "First name": first_name,
                "Last name": last_name,
                "level": level,
                "Class name": "",
                "Class code": "",
                "RS USER ID": str(created_meta.get("user_id") or "").strip(),
                "STATUS": "CREADO",
                "DETAIL": "Usuario creado y clases asignadas: {classes}".format(
                    classes=" | ".join(pending.get("group_labels") or [])
                    or "sin clases",
                ),
            }
        )

    result_rows.sort(
        key=lambda item: (
            str(item.get("STATUS") or "").upper().startswith("ERROR") is False,
            _normalize_plain_text(item.get("Username(Email)")),
            _normalize_plain_text(item.get("DETAIL")),
        )
    )
    return summary, result_rows

def _render_richmondstudio_class_sync_section(
    rs_token: str,
    timeout: int,
) -> None:
    st.markdown("**Actualizar clases RS por Excel**")
    st.caption(
        "Sube un Excel con el mismo formato de usuarios RS: Last name, First name, Class, Email, Role y level. "
        "Si el usuario ya existe, la app agregara las clases del archivo. Si no existe, lo creara como student. "
        "Usa 'Previsualizar refresh RS' para ver como quedaria cada alumno si RS se dejara solo con las clases del archivo."
    )
    rs_class_sync_token = _clean_token_value(rs_token)
    cached_class_sync_token = _clean_token_value(
        st.session_state.get("rs_class_sync_template_token", "")
    )
    needs_class_sync_template = bool(rs_class_sync_token) and (
        rs_class_sync_token != cached_class_sync_token
        or not bytes(st.session_state.get("rs_class_sync_template_bytes") or b"")
    )
    if needs_class_sync_template:
        try:
            with st.spinner("Preparando plantilla clases RS..."):
                panel_data = _load_richmondstudio_registered_panel_data(
                    rs_token,
                    timeout=int(timeout),
                )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"No se pudo preparar la plantilla de clases RS: {exc}")
        else:
            _store_richmondstudio_registered_panel_data(panel_data)
            listing_data = (
                panel_data.get("listing_data")
                if isinstance(panel_data.get("listing_data"), dict)
                else {}
            )
            groups_lookup = (
                panel_data.get("groups_lookup")
                if isinstance(panel_data.get("groups_lookup"), dict)
                else {"by_id": {}, "by_code": {}, "by_name": {}}
            )
            template_rows = _build_richmondstudio_class_sync_template_rows(
                list(listing_data.get("registered_user_rows") or []),
                groups_lookup,
            )
            institution_name = str(panel_data.get("institution_name") or "").strip()
            st.session_state["rs_class_sync_template_bytes"] = (
                _export_simple_excel(
                    template_rows,
                    sheet_name="class_sync_rs",
                )
                if template_rows
                else b""
            )
            st.session_state["rs_class_sync_template_filename"] = (
                _build_richmondstudio_password_update_filename(
                    institution_name,
                    prefix="plantilla_clases_rs",
                )
            )
            st.session_state["rs_class_sync_template_count"] = int(len(template_rows))
            st.session_state["rs_class_sync_template_token"] = rs_class_sync_token

    rs_class_sync_template_bytes = bytes(
        st.session_state.get("rs_class_sync_template_bytes") or b""
    )
    rs_class_sync_template_filename = str(
        st.session_state.get("rs_class_sync_template_filename")
        or "plantilla_clases_rs.xlsx"
    ).strip() or "plantilla_clases_rs.xlsx"
    rs_class_sync_template_count = int(
        st.session_state.get("rs_class_sync_template_count") or 0
    )
    if not rs_token:
        st.caption("Ingresa el bearer token de Richmond Studio para descargar la plantilla.")
    if rs_class_sync_template_bytes:
        st.caption(
            "Plantilla lista: {rows} fila(s).".format(
                rows=rs_class_sync_template_count
            )
        )
        st.download_button(
            label="Descargar plantilla clases RS",
            data=rs_class_sync_template_bytes,
            file_name=rs_class_sync_template_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="rs_class_sync_template_download",
            use_container_width=True,
        )

    uploaded_rs_class_sync = st.file_uploader(
        "Excel para sincronizar clases RS",
        type=["xlsx", "csv", "txt"],
        key="rs_class_sync_upload_file",
        help=(
            "Columnas aceptadas: Last name, First name, Class, Email, Role y level. "
            "Si el alumno no existe en RS, First name, Last name y level son obligatorios. "
            "Puedes repetir el mismo correo en varias filas para agregar varias clases."
        ),
    )
    rs_class_sync_bytes = b""
    rs_class_sync_name = ""
    rs_class_sync_rows: List[Dict[str, str]] = []
    rs_class_sync_error = ""
    rs_class_sync_preview_rows: List[Dict[str, object]] = []
    if uploaded_rs_class_sync is not None:
        rs_class_sync_bytes = uploaded_rs_class_sync.getvalue()
        rs_class_sync_name = str(
            uploaded_rs_class_sync.name or "class_sync_rs.xlsx"
        ).strip()
        try:
            rs_class_sync_rows = _load_richmondstudio_bulk_class_sync_rows(
                rs_class_sync_bytes,
                rs_class_sync_name,
            )
        except Exception as exc:
            rs_class_sync_error = str(exc)
            st.session_state.pop("rs_class_refresh_preview_summary", None)
            st.session_state.pop("rs_class_refresh_preview_rows", None)
            st.session_state.pop("rs_class_refresh_preview_bytes", None)
            st.session_state.pop("rs_class_refresh_preview_upload_name", None)
            st.session_state.pop("rs_class_refresh_preview_upload_size", None)
            st.error(f"Error en archivo de clases RS: {exc}")
        else:
            rs_class_sync_preview_rows = (
                _build_richmondstudio_bulk_class_sync_preview_rows(
                    rs_class_sync_rows
                )
            )
            st.caption(
                "Filas validas: {rows} | Usuarios detectados: {users}".format(
                    rows=len(rs_class_sync_rows),
                    users=len(rs_class_sync_preview_rows),
                )
            )
            if rs_class_sync_preview_rows:
                _show_dataframe(
                    rs_class_sync_preview_rows[:200],
                    use_container_width=True,
                )
    else:
        st.session_state.pop("rs_class_refresh_preview_summary", None)
        st.session_state.pop("rs_class_refresh_preview_rows", None)
        st.session_state.pop("rs_class_refresh_preview_bytes", None)
        st.session_state.pop("rs_class_refresh_preview_upload_name", None)
        st.session_state.pop("rs_class_refresh_preview_upload_size", None)

    current_preview_upload_name = str(
        st.session_state.get("rs_class_refresh_preview_upload_name") or ""
    ).strip()
    current_preview_upload_size = int(
        st.session_state.get("rs_class_refresh_preview_upload_size") or 0
    )
    if (
        rs_class_sync_name
        and (
            current_preview_upload_name != rs_class_sync_name
            or current_preview_upload_size != len(rs_class_sync_bytes)
        )
    ):
        st.session_state.pop("rs_class_refresh_preview_summary", None)
        st.session_state.pop("rs_class_refresh_preview_rows", None)
        st.session_state.pop("rs_class_refresh_preview_bytes", None)
        st.session_state.pop("rs_class_refresh_preview_upload_name", None)
        st.session_state.pop("rs_class_refresh_preview_upload_size", None)

    preview_rs_class_refresh = False
    action_col_preview, action_col_sync = st.columns(2)
    with action_col_preview:
        preview_rs_class_refresh = st.button(
            "Previsualizar refresh RS",
            key="rs_class_refresh_preview_btn",
            use_container_width=True,
            help=(
                "Muestra como quedaria cada usuario en RS si se reemplazan sus clases "
                "actuales y se dejan solo las del archivo."
            ),
        )
    with action_col_sync:
        run_rs_class_sync = st.button(
            "Sincronizar clases RS por Excel",
            type="primary",
            key="rs_class_sync_run_btn",
            use_container_width=True,
        )

    if preview_rs_class_refresh:
        if not rs_token:
            st.error("Ingresa el bearer token de Richmond Studio.")
        elif uploaded_rs_class_sync is None:
            st.error("Sube el Excel de clases RS.")
        elif rs_class_sync_error:
            st.error(
                f"Corrige el archivo antes de continuar: {rs_class_sync_error}"
            )
        elif not rs_class_sync_rows:
            st.error("No hay filas validas para previsualizar el refresh RS.")
        else:
            try:
                with st.spinner("Construyendo previsualizacion refresh RS..."):
                    preview_panel_data = _load_richmondstudio_registered_panel_data(
                        rs_token,
                        timeout=int(timeout),
                    )
                    _store_richmondstudio_registered_panel_data(preview_panel_data)
                    preview_listing_data = (
                        preview_panel_data.get("listing_data")
                        if isinstance(preview_panel_data.get("listing_data"), dict)
                        else {}
                    )
                    preview_registered_user_rows = list(
                        preview_listing_data.get("registered_user_rows") or []
                    )
                    preview_groups_lookup = (
                        preview_panel_data.get("groups_lookup")
                        if isinstance(preview_panel_data.get("groups_lookup"), dict)
                        else {"by_id": {}, "by_code": {}, "by_name": {}}
                    )
                    refresh_preview_summary, refresh_preview_rows = (
                        _build_richmondstudio_bulk_class_refresh_preview(
                            rs_class_sync_rows,
                            preview_registered_user_rows,
                            preview_groups_lookup,
                        )
                    )
            except Exception as exc:
                st.error(f"No se pudo construir la previsualizacion refresh RS: {exc}")
            else:
                st.session_state["rs_class_refresh_preview_summary"] = dict(
                    refresh_preview_summary
                )
                st.session_state["rs_class_refresh_preview_rows"] = list(
                    refresh_preview_rows
                )
                st.session_state["rs_class_refresh_preview_bytes"] = (
                    _export_simple_excel(
                        refresh_preview_rows,
                        sheet_name="preview_refresh_rs",
                    )
                    if refresh_preview_rows
                    else b""
                )
                st.session_state["rs_class_refresh_preview_upload_name"] = (
                    rs_class_sync_name
                )
                st.session_state["rs_class_refresh_preview_upload_size"] = int(
                    len(rs_class_sync_bytes)
                )
                st.success(
                    "Previsualizacion refresh RS lista. "
                    "Usuarios en RS: {users_found} | Reemplazar: {users_to_replace} | "
                    "Crear: {users_to_create} | Sin cambios: {users_unchanged} | "
                    "Errores: {error_total}".format(
                        **refresh_preview_summary
                    )
                )
    if run_rs_class_sync:
        if not rs_token:
            st.error("Ingresa el bearer token de Richmond Studio.")
        elif uploaded_rs_class_sync is None:
            st.error("Sube el Excel de clases RS.")
        elif rs_class_sync_error:
            st.error(
                f"Corrige el archivo antes de continuar: {rs_class_sync_error}"
            )
        elif not rs_class_sync_rows:
            st.error("No hay filas validas para sincronizar clases RS.")
        else:
            st.session_state["rs_class_sync_upload_bytes"] = rs_class_sync_bytes
            st.session_state["rs_class_sync_upload_name"] = rs_class_sync_name
            _request_richmondstudio_confirmation(
                "rs_users_classes_sync",
                (
                    "sincronizar clases RS de "
                    f"{len(rs_class_sync_preview_rows)} usuario(s) desde Excel"
                ),
            )

    run_rs_class_sync_confirmed = _consume_richmondstudio_confirmed_action(
        "rs_users_classes_sync"
    )
    if run_rs_class_sync_confirmed:
        stored_class_sync_bytes = bytes(
            st.session_state.get("rs_class_sync_upload_bytes") or b""
        )
        stored_class_sync_name = str(
            st.session_state.get("rs_class_sync_upload_name") or ""
        ).strip()
        if not rs_token:
            st.error("Ingresa el bearer token de Richmond Studio.")
        elif not stored_class_sync_bytes:
            st.error("No se encontro el Excel cargado para sincronizar clases RS.")
        else:
            status_placeholder = st.empty()
            try:
                rows_to_sync = _load_richmondstudio_bulk_class_sync_rows(
                    stored_class_sync_bytes,
                    stored_class_sync_name or "class_sync_rs.xlsx",
                )
                with st.spinner("Sincronizando clases RS por Excel..."):
                    fresh_panel_data = _load_richmondstudio_registered_panel_data(
                        rs_token,
                        timeout=int(timeout),
                    )
                    fresh_listing_data = (
                        fresh_panel_data.get("listing_data")
                        if isinstance(fresh_panel_data.get("listing_data"), dict)
                        else {}
                    )
                    fresh_registered_user_rows = list(
                        fresh_listing_data.get("registered_user_rows") or []
                    )
                    fresh_groups_lookup = (
                        fresh_panel_data.get("groups_lookup")
                        if isinstance(fresh_panel_data.get("groups_lookup"), dict)
                        else {"by_id": {}, "by_code": {}, "by_name": {}}
                    )
                    class_sync_summary, class_sync_result_rows = (
                        _sync_richmondstudio_user_classes_from_excel_rows(
                            token=rs_token,
                            rows=rows_to_sync,
                            registered_user_rows=fresh_registered_user_rows,
                            groups_lookup=fresh_groups_lookup,
                            timeout=int(timeout),
                            on_status=lambda message: status_placeholder.write(
                                message
                            ),
                        )
                    )
                    refreshed_panel_after_sync = (
                        _load_richmondstudio_registered_panel_data(
                            rs_token,
                            timeout=int(timeout),
                        )
                    )
            except Exception as exc:
                status_placeholder.empty()
                st.error(f"No se pudieron sincronizar clases RS: {exc}")
            else:
                status_placeholder.empty()
                _store_richmondstudio_registered_panel_data(
                    refreshed_panel_after_sync
                )
                st.session_state["rs_class_sync_summary"] = dict(
                    class_sync_summary
                )
                st.session_state["rs_class_sync_result_rows"] = list(
                    class_sync_result_rows
                )
                st.session_state["rs_class_sync_result_bytes"] = (
                    _export_simple_excel(
                        class_sync_result_rows,
                        sheet_name="class_sync_rs",
                    )
                    if class_sync_result_rows
                    else b""
                )
                st.success(
                    "Actualizacion de clases RS completada. "
                    "Filas: {input_rows} | Usuarios encontrados: {users_found} | "
                    "Usuarios creados: {users_created} | Usuarios actualizados: {users_updated} | Sin cambios: {users_unchanged} | "
                    "Errores: {error_total}".format(
                        **class_sync_summary
                    )
                )

    rs_class_sync_summary_cached = (
        st.session_state.get("rs_class_sync_summary") or {}
    )
    rs_class_sync_result_rows_cached = (
        st.session_state.get("rs_class_sync_result_rows") or []
    )
    rs_class_sync_result_bytes_cached = (
        st.session_state.get("rs_class_sync_result_bytes") or b""
    )
    rs_class_refresh_preview_summary_cached = (
        st.session_state.get("rs_class_refresh_preview_summary") or {}
    )
    rs_class_refresh_preview_rows_cached = (
        st.session_state.get("rs_class_refresh_preview_rows") or []
    )
    rs_class_refresh_preview_bytes_cached = (
        st.session_state.get("rs_class_refresh_preview_bytes") or b""
    )
    if rs_class_refresh_preview_summary_cached:
        st.info(
            "Previsualizacion refresh RS: Filas {input_rows} | "
            "Usuarios en RS {users_found} | Reemplazar {users_to_replace} | "
            "Crear {users_to_create} | Sin cambios {users_unchanged} | "
            "Errores {error_total}".format(
                **rs_class_refresh_preview_summary_cached
            )
        )
        if rs_class_refresh_preview_rows_cached:
            _show_dataframe(
                rs_class_refresh_preview_rows_cached[:200],
                use_container_width=True,
            )
        if rs_class_refresh_preview_bytes_cached:
            st.download_button(
                label="Descargar previsualizacion refresh RS",
                data=rs_class_refresh_preview_bytes_cached,
                file_name=_build_richmondstudio_password_update_filename(
                    "",
                    prefix="preview_refresh_clases_rs",
                ),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="rs_class_refresh_preview_download",
                use_container_width=True,
            )
    if rs_class_sync_summary_cached:
        st.info(
            "Ultima actualizacion clases RS: Filas {input_rows} | "
            "Usuarios encontrados {users_found} | Creados {users_created} | Actualizados {users_updated} | "
            "Sin cambios {users_unchanged} | Errores {error_total}".format(
                **rs_class_sync_summary_cached
            )
        )
        if rs_class_sync_result_rows_cached:
            _show_dataframe(
                rs_class_sync_result_rows_cached[:200],
                use_container_width=True,
            )
        if rs_class_sync_result_bytes_cached:
            st.download_button(
                label="Descargar resultado clases RS",
                data=rs_class_sync_result_bytes_cached,
                file_name=_build_richmondstudio_password_update_filename(
                    "",
                    prefix="resultado_clases_rs",
                ),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="rs_class_sync_result_download",
                use_container_width=True,
            )

def _render_richmondstudio_students_password_panel(
    rs_token: str,
    timeout: int,
) -> None:
    with st.container(border=True):
        st.markdown("**CRUD Alumnos RS**")
        st.caption(
            "Carga alumnos de Richmond Studio, buscalos y actualiza su password desde esta vista. "
            "La app envia internamente un CSV temporal de una sola fila al bulk endpoint."
        )

        rs_students_notice = st.session_state.pop("rs_students_crud_notice", None)
        if isinstance(rs_students_notice, dict):
            notice_type = str(rs_students_notice.get("type") or "").strip().lower()
            notice_message = str(rs_students_notice.get("message") or "").strip()
            if notice_message:
                if notice_type == "success":
                    st.success(notice_message)
                elif notice_type == "warning":
                    st.warning(notice_message)
                elif notice_type == "error":
                    st.error(notice_message)
                else:
                    st.info(notice_message)

        if st.button(
            "Cargar alumnos RS",
            type="primary",
            key="rs_students_crud_load_btn",
            use_container_width=True,
        ):
            if not rs_token:
                st.error("Ingresa el bearer token de Richmond Studio.")
            else:
                try:
                    with st.spinner("Cargando alumnos RS..."):
                        panel_data = _load_richmondstudio_registered_panel_data(
                            rs_token,
                            timeout=int(timeout),
                        )
                except Exception as exc:  # pragma: no cover - UI
                    st.error(f"Error RS: {exc}")
                else:
                    _store_richmondstudio_registered_panel_data(panel_data)
                    listing_data = (
                        panel_data.get("listing_data")
                        if isinstance(panel_data.get("listing_data"), dict)
                        else {}
                    )
                    registered_user_rows = list(
                        listing_data.get("registered_user_rows") or []
                    )
                    student_rows = [
                        dict(row)
                        for row in registered_user_rows
                        if isinstance(row, dict)
                        and str(row.get("Role") or "").strip().lower() == "student"
                    ]
                    student_rows.sort(
                        key=lambda row: (
                            _normalize_plain_text(
                                " ".join(
                                    part
                                    for part in (
                                        str(row.get("First name") or "").strip(),
                                        str(row.get("Last name") or "").strip(),
                                    )
                                    if part
                                )
                            ),
                            _normalize_plain_text(
                                row.get("Email") or row.get("Username") or ""
                            ),
                        )
                    )
                    st.session_state["rs_students_crud_rows"] = student_rows
                    st.session_state["rs_students_crud_loaded_token"] = _clean_token_value(
                        rs_token
                    )
                    st.session_state["rs_students_crud_selected_user_id"] = ""
                    st.session_state["rs_students_crud_form_loaded_user_id"] = ""

        students_rows_cached = [
            dict(row)
            for row in st.session_state.get("rs_students_crud_rows") or []
            if isinstance(row, dict)
        ]
        if (
            not students_rows_cached
            and _clean_token_value(rs_token)
            == _clean_token_value(
                st.session_state.get("rs_students_crud_loaded_token", "")
            )
        ):
            students_rows_cached = [
                dict(row)
                for row in st.session_state.get("rs_registered_user_rows") or []
                if isinstance(row, dict)
                and str(row.get("Role") or "").strip().lower() == "student"
            ]
            if students_rows_cached:
                st.session_state["rs_students_crud_rows"] = students_rows_cached

        if not students_rows_cached:
            st.caption("Pulsa `Cargar alumnos RS` para empezar.")
            return

        rs_students_search = st.text_input(
            "Filtrar alumnos RS",
            key="rs_students_crud_search_text",
            placeholder="Nombre, email, login o identificador",
        )
        rs_students_search_norm = _normalize_plain_text(rs_students_search)
        students_rows_by_id: Dict[str, Dict[str, object]] = {}
        students_option_labels: Dict[str, str] = {}
        preview_rows: List[Dict[str, object]] = []
        for row in students_rows_cached:
            user_id = str(row.get("RS USER ID") or "").strip()
            if not user_id:
                continue
            full_name = " ".join(
                part
                for part in (
                    str(row.get("First name") or "").strip(),
                    str(row.get("Last name") or "").strip(),
                )
                if part
            ).strip()
            email_txt = str(row.get("Email") or row.get("Username") or "").strip()
            login_txt = str(row.get("Username") or row.get("Login") or "").strip()
            identifier_txt = str(row.get("IDENTIFIER") or "").strip()
            haystack = " ".join((full_name, email_txt, login_txt, identifier_txt))
            if rs_students_search_norm and (
                rs_students_search_norm not in _normalize_plain_text(haystack)
            ):
                continue
            students_rows_by_id[user_id] = row
            students_option_labels[user_id] = (
                f"{full_name or email_txt or user_id} | {email_txt or login_txt or '-'}"
            )
            preview_rows.append(
                {
                    "Alumno": full_name,
                    "Email": email_txt,
                    "Login": login_txt,
                    "IDENTIFIER": identifier_txt,
                }
            )

        filtered_student_ids = list(students_rows_by_id.keys())
        st.caption(
            "Mostrando {filtered} de {total} alumnos RS.".format(
                filtered=len(filtered_student_ids),
                total=len(students_rows_cached),
            )
        )
        if preview_rows:
            _show_dataframe(preview_rows[:200], use_container_width=True)
        if not filtered_student_ids:
            st.warning("No hay alumnos RS que coincidan con el filtro.")
            return

        current_selected_student_id = str(
            st.session_state.get("rs_students_crud_selected_user_id") or ""
        ).strip()
        if current_selected_student_id not in filtered_student_ids:
            current_selected_student_id = filtered_student_ids[0]
            st.session_state["rs_students_crud_selected_user_id"] = (
                current_selected_student_id
            )

        selected_student_id = str(
            st.selectbox(
                "Alumno RS",
                options=filtered_student_ids,
                key="rs_students_crud_selected_user_id",
                format_func=lambda user_id: students_option_labels.get(
                    str(user_id or "").strip(),
                    str(user_id or "").strip(),
                ),
            )
            or ""
        ).strip()
        selected_student_row = students_rows_by_id.get(selected_student_id) or {}

        loaded_student_id = str(
            st.session_state.get("rs_students_crud_form_loaded_user_id") or ""
        ).strip()
        if loaded_student_id != selected_student_id:
            st.session_state["rs_students_crud_username"] = str(
                selected_student_row.get("Email")
                or selected_student_row.get("Username")
                or selected_student_row.get("Login")
                or ""
            ).strip()
            st.session_state["rs_students_crud_password"] = ""
            st.session_state["rs_students_crud_form_loaded_user_id"] = selected_student_id
            st.session_state.pop("rs_students_crud_last_sent_csv_bytes", None)
            st.session_state.pop("rs_students_crud_last_sent_user", None)
        elif bool(st.session_state.pop("rs_students_crud_reset_password", False)):
            st.session_state["rs_students_crud_password"] = ""

        st.caption(
            "Seleccionado: {name} | Email: {email} | Login: {login}".format(
                name=" ".join(
                    part
                    for part in (
                        str(selected_student_row.get("First name") or "").strip(),
                        str(selected_student_row.get("Last name") or "").strip(),
                    )
                    if part
                ).strip()
                or "(sin nombre)",
                email=str(
                    selected_student_row.get("Email")
                    or selected_student_row.get("Username")
                    or ""
                ).strip()
                or "-",
                login=str(
                    selected_student_row.get("Username")
                    or selected_student_row.get("Login")
                    or ""
                ).strip()
                or "-",
            )
        )

        with st.form("rs_students_crud_password_form", clear_on_submit=False):
            rs_password_col_1, rs_password_col_2 = st.columns(2, gap="small")
            rs_password_col_1.text_input(
                "Usuario RS / Email",
                key="rs_students_crud_username",
            )
            rs_password_col_2.text_input(
                "Nueva password RS",
                key="rs_students_crud_password",
                type="password",
            )
            update_clicked = st.form_submit_button(
                "Actualizar password RS",
                use_container_width=True,
            )
        rs_username = str(
            st.session_state.get("rs_students_crud_username") or ""
        ).strip()
        rs_password = str(
            st.session_state.get("rs_students_crud_password") or ""
        )

        if update_clicked:
            if not rs_token:
                st.error("Ingresa el bearer token de Richmond Studio.")
            elif not rs_username:
                st.error("Ingresa el usuario o email de RS.")
            elif not rs_password:
                st.error("Ingresa la nueva password de RS.")
            else:
                rs_update_rows = [
                    {
                        "Username": rs_username,
                        "New password": rs_password,
                        "Keep in class": "yes",
                    }
                ]
                rs_sent_csv_bytes = _build_richmondstudio_bulk_user_csv_bytes(
                    rs_update_rows
                )
                try:
                    with st.spinner("Actualizando password RS..."):
                        rs_response_message = _submit_richmondstudio_bulk_user_update(
                            rs_token,
                            rs_update_rows,
                            csv_bytes=rs_sent_csv_bytes,
                            timeout=max(120, int(timeout)),
                        )
                except Exception as exc:  # pragma: no cover - UI
                    st.session_state["rs_students_crud_notice"] = {
                        "type": "error",
                        "message": "No se pudo actualizar la password RS: {msg}".format(
                            msg=str(exc).strip() or "sin detalle"
                        ),
                    }
                else:
                    st.session_state["rs_students_crud_last_sent_csv_bytes"] = (
                        rs_sent_csv_bytes
                    )
                    st.session_state["rs_students_crud_last_sent_user"] = rs_username
                    st.session_state["rs_students_crud_reset_password"] = True
                    st.session_state["rs_students_crud_notice"] = {
                        "type": "success",
                        "message": "Password RS actualizada para {user}. Respuesta: {response}".format(
                            user=rs_username,
                            response=str(rs_response_message or "ok").strip() or "ok",
                        ),
                    }
                st.rerun()

        rs_last_sent_csv_bytes = bytes(
            st.session_state.get("rs_students_crud_last_sent_csv_bytes") or b""
        )
        rs_last_sent_user = str(
            st.session_state.get("rs_students_crud_last_sent_user") or ""
        ).strip()
        if rs_last_sent_csv_bytes:
            st.caption(
                "CSV enviado en la ultima actualizacion para: {user}".format(
                    user=rs_last_sent_user or "-"
                )
            )
            st.download_button(
                "Descargar CSV enviado",
                data=rs_last_sent_csv_bytes,
                file_name="CSV_Template_Edit_User.csv",
                mime="text/csv",
                key="rs_students_crud_download_csv_btn",
                use_container_width=True,
            )

def _build_richmondstudio_bulk_user_csv_bytes(
    rows: List[Dict[str, object]]
) -> bytes:
    output = StringIO()
    writer = csv.writer(output, lineterminator="\r\n")
    headers = [
        "Username(Email)",
        "New last name(optional)",
        "New first name(optional)",
        "New class code(optional)",
        "New password(optional)",
        "Keep in classes(optional)",
    ]
    writer.writerow(headers)
    for row in rows:
        if not isinstance(row, dict):
            continue
        writer.writerow(
            [
                str(row.get("Username") or row.get("Username(Email)") or "").strip(),
                str(
                    row.get("New last name")
                    or row.get("New last name(optional)")
                    or ""
                ).strip(),
                str(
                    row.get("New first name")
                    or row.get("New first name(optional)")
                    or ""
                ).strip(),
                str(
                    row.get("New class code")
                    or row.get("New class code(optional)")
                    or ""
                ).strip(),
                str(
                    row.get("New password")
                    or row.get("New password(optional)")
                    or ""
                ).strip(),
                str(
                    row.get("Keep in class")
                    or row.get("Keep in classes(optional)")
                    or ""
                ).strip(),
            ]
        )
    return output.getvalue().encode("utf-8")

def _normalize_richmondstudio_bulk_keep_in_class(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "yes"
    normalized = _normalize_plain_text(raw)
    if normalized in {"YES", "Y", "SI", "S", "TRUE", "1"}:
        return "yes"
    if normalized in {"NO", "N", "FALSE", "0"}:
        return "no"
    return raw.lower()

def _load_richmondstudio_bulk_user_update_rows(
    file_bytes: bytes,
    file_name: str,
) -> List[Dict[str, str]]:
    file_name_txt = str(file_name or "").strip().lower()
    if not file_bytes:
        raise ValueError("El archivo de actualizacion RS esta vacio.")

    if file_name_txt.endswith((".csv", ".txt")):
        try:
            df = pd.read_csv(BytesIO(file_bytes), dtype=str).fillna("")
        except Exception as exc:
            raise ValueError(f"No se pudo leer el CSV de actualizacion RS: {exc}") from exc
    else:
        try:
            df = pd.read_excel(BytesIO(file_bytes), dtype=str).fillna("")
        except Exception as exc:
            raise ValueError(f"No se pudo leer el Excel de actualizacion RS: {exc}") from exc

    header_aliases = {
        "USERNAME": "Username",
        "USERNAME EMAIL": "Username",
        "USERNAME(EMAIL)": "Username",
        "NEW LAST NAME": "New last name",
        "NEW LAST NAME OPTIONAL": "New last name",
        "NEW LAST NAME(OPTIONAL)": "New last name",
        "NEW FIRST NAME": "New first name",
        "NEW FIRST NAME OPTIONAL": "New first name",
        "NEW FIRST NAME(OPTIONAL)": "New first name",
        "NEW CLASS CODE": "New class code",
        "NEW CLASS CODE OPTIONAL": "New class code",
        "NEW CLASS CODE(OPTIONAL)": "New class code",
        "NEW PASSWORD": "New password",
        "NEW PASSWORD OPTIONAL": "New password",
        "NEW PASSWORD(OPTIONAL)": "New password",
        "KEEP IN CLASS": "Keep in class",
        "KEEP IN CLASSES": "Keep in class",
        "KEEP IN CLASS OPTIONAL": "Keep in class",
        "KEEP IN CLASSES OPTIONAL": "Keep in class",
        "KEEP IN CLASS(OPTIONAL)": "Keep in class",
        "KEEP IN CLASSES(OPTIONAL)": "Keep in class",
    }
    renamed_columns: Dict[str, str] = {}
    used_columns: Set[str] = set()
    for column in df.columns:
        normalized = _normalize_plain_text(column)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        normalized_compact = re.sub(r"[^A-Z0-9]+", " ", normalized).strip()
        canonical = header_aliases.get(normalized)
        if not canonical:
            canonical = header_aliases.get(normalized_compact)
        if not canonical:
            if normalized.startswith("NEW LAST NAM"):
                canonical = "New last name"
            elif normalized.startswith("NEW FIRST NAM"):
                canonical = "New first name"
            elif normalized.startswith("NEW CLASS CO"):
                canonical = "New class code"
            elif normalized.startswith("NEW PASSWOR"):
                canonical = "New password"
            elif normalized.startswith("KEEP IN CLASS") or normalized.startswith(
                "KEEP IN CLASSES"
            ):
                canonical = "Keep in class"
            elif normalized.startswith("USERNAME(") or normalized.startswith(
                "USERNAME EMAIL"
            ):
                canonical = "Username"
        if canonical and canonical not in used_columns:
            renamed_columns[str(column)] = canonical
            used_columns.add(canonical)

    df = df.rename(columns=renamed_columns)
    required_headers = {"Username", "New password"}
    missing_headers = [
        header for header in required_headers if header not in set(df.columns)
    ]
    if missing_headers:
        raise ValueError(
            "Faltan columnas obligatorias en el archivo RS: "
            + ", ".join(missing_headers)
        )

    rows: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        normalized_row = {
            "Username": str(row.get("Username") or "").strip(),
            "New last name": str(row.get("New last name") or "").strip(),
            "New first name": str(row.get("New first name") or "").strip(),
            "New class code": str(row.get("New class code") or "").strip(),
            "New password": str(row.get("New password") or "").strip(),
            "Keep in class": _normalize_richmondstudio_bulk_keep_in_class(
                row.get("Keep in class")
            ),
        }
        if any(str(value).strip() for value in normalized_row.values()):
            rows.append(normalized_row)
    return rows

def _build_richmondstudio_bulk_user_update_preview_rows(
    rows: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    preview_rows: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        password_txt = str(row.get("New password") or "").strip()
        preview_rows.append(
            {
                "Username(Email)": str(row.get("Username") or "").strip(),
                "New last name(optional)": str(
                    row.get("New last name") or ""
                ).strip(),
                "New first name(optional)": str(
                    row.get("New first name") or ""
                ).strip(),
                "New class code(optional)": str(
                    row.get("New class code") or ""
                ).strip(),
                "New password(optional)": "********" if password_txt else "",
                "Keep in classes(optional)": str(
                    row.get("Keep in class") or ""
                ).strip(),
                "Aplicar": "Si"
                if str(row.get("Username") or "").strip() and password_txt
                else "No",
            }
        )
    return preview_rows

def _submit_richmondstudio_bulk_user_update(
    token: str,
    rows: List[Dict[str, str]],
    csv_bytes: Optional[bytes] = None,
    timeout: int = 120,
) -> str:
    actionable_rows = [
        row
        for row in rows
        if str(row.get("Username") or "").strip()
        and str(row.get("New password") or "").strip()
    ]
    if not actionable_rows:
        raise ValueError("No hay filas con Username y New password para actualizar.")

    payload_csv_bytes = (
        bytes(csv_bytes)
        if csv_bytes
        else _build_richmondstudio_bulk_user_csv_bytes(actionable_rows)
    )
    try:
        response = requests.post(
            RICHMONDSTUDIO_BULK_USER_EDITION_URL,
            headers=_richmondstudio_bulk_user_headers(token),
            files={
                "csv_file": (
                    "rs_bulk_user_edition.csv",
                    payload_csv_bytes,
                    "text/csv",
                )
            },
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    response_text = str(response.text or "").strip()
    parsed_body: object = None
    if response.content:
        try:
            parsed_body = response.json()
        except ValueError:
            parsed_body = response_text

    if not response.ok:
        raise RuntimeError(
            _richmondstudio_response_error(response, status_code, parsed_body)
        )

    if isinstance(parsed_body, str) and parsed_body.strip():
        return parsed_body.strip()
    if isinstance(parsed_body, dict):
        message = str(
            parsed_body.get("message")
            or parsed_body.get("detail")
            or parsed_body.get("status")
            or ""
        ).strip()
        if message:
            return message
    return response_text or "ok"

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

def _render_richmondstudio_classes_manage_panel(
    rs_token: str,
    timeout: int,
) -> None:
    with st.container(border=True):
        st.markdown("**1) Gestion de clases RS**")
        st.caption(
            "Carga las clases de Richmond Studio y trabaja sobre el listado filtrado."
        )
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
            st.session_state["rs_groups_loaded_rows"] = (
                _normalize_richmondstudio_loaded_rows(
                    sorted(
                        rs_group_rows,
                        key=lambda row: (
                            str(row.get("Class name") or "").upper(),
                            str(row.get("Code") or "").upper(),
                        ),
                    )
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
            st.markdown("**Editar o eliminar clases cargadas**")
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
                        st.success(
                            f"Clases RS actualizadas correctamente: {ok_rs_update}."
                        )
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
                        st.success(
                            f"Clases RS eliminadas correctamente: {ok_rs_delete}."
                        )
                    elif ok_rs_delete and err_rs_delete:
                        st.warning(
                            f"Resultado parcial RS: OK {ok_rs_delete} | Error {err_rs_delete}."
                        )
                    else:
                        st.error("No se pudo eliminar ninguna clase RS.")
                    _show_dataframe(resultados_rs_delete, use_container_width=True)
        else:
            st.info("Aun no has cargado clases RS.")

def _render_richmondstudio_classes_create_panel(
    rs_token: str,
    timeout: int,
) -> None:
    with st.container(border=True):
        st.markdown("**2) Crear clases RS**")
        st.caption(
            "Llena una clase por fila. Description se completa con Class name si lo dejas vacio. Al crear: inicio = hoy, fin = 31/12 del ano actual y Test level vacio no se manda."
        )
        _render_richmondstudio_create_rows_form(
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
                        st.session_state["rs_groups_loaded_rows"] = (
                            _normalize_richmondstudio_loaded_rows(
                                sorted(
                                    rs_group_rows,
                                    key=lambda row: (
                                        str(row.get("Class name") or "").upper(),
                                        str(row.get("Code") or "").upper(),
                                    ),
                                )
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

def render_richmond_studio_view() -> None:
    timeout = 30
    st.session_state["rs_timeout"] = int(timeout)
    bridge_mode = str(
        st.session_state.get("rs_bearer_token_bridge_mode") or "read"
    ).strip().lower() or "read"
    bridge_value = _clean_token_value(
        st.session_state.get("rs_bearer_token_bridge_value", "")
    )
    browser_rs_token = _read_browser_richmondstudio_token(
        mode=bridge_mode,
        value=bridge_value,
    )
    if bridge_mode != "read":
        st.session_state["rs_bearer_token_bridge_mode"] = "read"
        st.session_state["rs_bearer_token_bridge_value"] = ""

    rs_token_default = _get_richmondstudio_token()
    if "rs_groups_bearer_token" not in st.session_state:
        initial_rs_token = ""
        if browser_rs_token not in ("", RICHMONDSTUDIO_TOKEN_BRIDGE_PENDING):
            initial_rs_token = browser_rs_token
        else:
            initial_rs_token = rs_token_default
        st.session_state["rs_groups_bearer_token"] = initial_rs_token
    elif (
        not _clean_token_value(st.session_state.get("rs_groups_bearer_token", ""))
        and browser_rs_token not in ("", RICHMONDSTUDIO_TOKEN_BRIDGE_PENDING)
    ):
        st.session_state["rs_groups_bearer_token"] = browser_rs_token
    if "rs_bearer_token" not in st.session_state:
        st.session_state["rs_bearer_token"] = str(
            st.session_state.get("rs_groups_bearer_token", rs_token_default) or ""
        )
    if "rs_groups_bearer_token_input" not in st.session_state:
        st.session_state["rs_groups_bearer_token_input"] = str(
            st.session_state.get("rs_groups_bearer_token", "")
        )
    elif (
        not _clean_token_value(st.session_state.get("rs_groups_bearer_token_input", ""))
        and browser_rs_token not in ("", RICHMONDSTUDIO_TOKEN_BRIDGE_PENDING)
        and _clean_token_value(st.session_state.get("rs_groups_bearer_token", ""))
        == browser_rs_token
    ):
        st.session_state["rs_groups_bearer_token_input"] = browser_rs_token

    st.markdown("**Configuracion RS**")
    rs_col_input, rs_col_save = st.columns([5.1, 1], gap="small")
    with rs_col_input:
        rs_token = _clean_token(
            st.text_input(
                "Bearer token RS",
                key="rs_groups_bearer_token_input",
                help="Se usa para clases RS y EXCEL RS. Pulsa Guardar para conservarlo en el navegador.",
            )
        )
    with rs_col_save:
        st.markdown(
            "<div style='height: 1.85rem;' aria-hidden='true'></div>",
            unsafe_allow_html=True,
        )
        if st.button("Guardar", key="rs_token_save_btn", use_container_width=True):
            _sync_richmondstudio_token_from_input()
            st.session_state["rs_bearer_token_bridge_mode"] = "write"
            st.session_state["rs_bearer_token_bridge_value"] = str(
                st.session_state.get("rs_groups_bearer_token", "")
            )
            st.rerun()
    st.session_state["rs_bearer_token"] = rs_token
    saved_rs_token = _clean_token_value(st.session_state.get("rs_groups_bearer_token", ""))
    if saved_rs_token:
        st.caption("Token RS guardado en sesion y navegador.")
    if rs_token and rs_token != saved_rs_token:
        st.caption("Hay cambios no guardados en el token RS.")

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

    (
        tab_rs_clases,
        tab_rs_usuarios,
        tab_rs_alumnos,
        tab_rs_docentes,
        tab_rs_excel,
    ) = st.tabs(
        [
            "Clases RS",
            "Usuarios RS",
            "CRUD Alumnos RS",
            "Asignar clases a docentes",
            "Listar alumnos registrados",
        ]
    )
    with tab_rs_clases:
        if "rs_groups_create_rows" not in st.session_state:
            st.session_state["rs_groups_create_rows"] = [
                _default_richmondstudio_group_row()
            ]
        if str(st.session_state.get("rs_clases_nav") or "").strip() not in {
            "gestion",
            "crear",
        }:
            st.session_state["rs_clases_nav"] = "gestion"

        rs_clases_nav_col, rs_clases_body_col = st.columns([1.15, 4.85], gap="large")
        with rs_clases_nav_col:
            rs_clases_view = _render_crud_menu(
                "Funciones de clases RS",
                [
                    ("gestion", "Gestion", "Lista, filtra, edita o elimina clases"),
                    ("crear", "Crear", "Crea varias clases en una sola grilla"),
                ],
                state_key="rs_clases_nav",
            )
        with rs_clases_body_col:
            if rs_clases_view == "gestion":
                _render_richmondstudio_classes_manage_panel(
                    rs_token=rs_token,
                    timeout=int(timeout),
                )
            if rs_clases_view == "crear":
                _render_richmondstudio_classes_create_panel(
                    rs_token=rs_token,
                    timeout=int(timeout),
                )
    with tab_rs_usuarios:
        if str(st.session_state.get("rs_users_nav") or "").strip() not in {
            "crear",
            "clases",
        }:
            st.session_state["rs_users_nav"] = "crear"
        rs_users_sidebar_col, rs_users_body_col = st.columns([1.15, 4.85], gap="large")
        with rs_users_sidebar_col:
            rs_users_view = _render_crud_menu(
                "Funciones usuarios RS",
                [
                    ("crear", "Crear", "Alta masiva de usuarios desde Excel"),
                    ("clases", "Clases", "Sincroniza clases o crea alumnos"),
                ],
                state_key="rs_users_nav",
            )
        with rs_users_body_col:
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
                                rs_password_template_rows = (
                                    _build_richmondstudio_password_update_template_rows(
                                        resultados_rs_users
                                    )
                                )
                                st.session_state["rs_users_password_template_bytes"] = (
                                    _export_simple_excel(
                                        rs_password_template_rows,
                                        sheet_name="password_update_rs",
                                    )
                                    if rs_password_template_rows
                                    else b""
                                )
                                st.session_state["rs_users_password_template_filename"] = (
                                    _build_richmondstudio_password_update_filename(
                                        institution_name_for_file,
                                        prefix="plantilla_password_rs",
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
                    col_rs_users_download_a, col_rs_users_download_b = st.columns(
                        2, gap="small"
                    )
                    col_rs_users_download_a.download_button(
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
                    if st.session_state.get("rs_users_password_template_bytes"):
                        col_rs_users_download_b.download_button(
                            label="Descargar plantilla password RS (creados)",
                            data=st.session_state["rs_users_password_template_bytes"],
                            file_name=str(
                                st.session_state.get(
                                    "rs_users_password_template_filename"
                                )
                                or "plantilla_password_rs.xlsx"
                            ).strip()
                            or "plantilla_password_rs.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="rs_users_password_template_download",
                            use_container_width=True,
                        )
            with st.container(border=True):
                _render_richmondstudio_class_sync_section(
                    rs_token=rs_token,
                    timeout=int(timeout),
                )

    with tab_rs_docentes:
        rs_docentes_sidebar_col, rs_docentes_body_col = st.columns([1.15, 4.85], gap="large")
        with rs_docentes_sidebar_col:
            with st.container(border=True):
                st.markdown("**Docentes RS**")
                st.caption(
                    "Carga, crea o edita docentes y sincroniza sus clases desde un solo panel."
                )
                st.markdown("**Bloques**")
                st.caption("Carga y listado de docentes")
                st.caption("Formulario de creacion o edicion")
                loaded_teachers = len(st.session_state.get("rs_teachers_loaded_rows") or [])
                if loaded_teachers:
                    st.caption(f"Docentes cargados: {loaded_teachers}")
        with rs_docentes_body_col:
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

    with tab_rs_alumnos:
        if str(st.session_state.get("rs_students_crud_nav") or "").strip() not in {
            "password",
        }:
            st.session_state["rs_students_crud_nav"] = "password"
        rs_alumnos_nav_col, rs_alumnos_body_col = st.columns([1.15, 4.85], gap="large")
        with rs_alumnos_nav_col:
            rs_alumnos_view = _render_crud_menu(
                "Funciones alumnos RS",
                [
                    (
                        "password",
                        "Password",
                        "Busca alumnos RS y actualiza password",
                    ),
                ],
                state_key="rs_students_crud_nav",
            )
        with rs_alumnos_body_col:
            if rs_alumnos_view == "password":
                _render_richmondstudio_students_password_panel(
                    rs_token=rs_token,
                    timeout=int(timeout),
                )

    with tab_rs_excel:
        rs_listado_sidebar_col, rs_listado_body_col = st.columns([1.15, 4.85], gap="large")
        with rs_listado_sidebar_col:
            with st.container(border=True):
                st.markdown("**Alumnos registrados RS**")
                st.caption(
                    "Desde aqui puedes listar usuarios, revisar suscripciones, gestionar clases y actualizar passwords."
                )
                st.markdown("**Bloques**")
                st.caption("Listado y exportaciones")
                st.caption("Consulta de suscripciones")
                st.caption("Gestionar clases por usuario")
                st.caption("Actualizar password")
                listed_rows = int(st.session_state.get("rs_excel_count") or 0)
                if listed_rows:
                    st.caption(f"Ultimo listado: {listed_rows} fila(s).")
        with rs_listado_body_col:
            with st.container(border=True):
                expiring_next_year = int(date.today().year + 1)
                expiring_next_year_label = f"01/01/{expiring_next_year}"
                expiring_next_year_sheet = (
                    f"subscriptions_expiring_{expiring_next_year}"
                )
                expiring_next_year_file_name = (
                    "rs_suscripciones_expiran_"
                    f"{expiring_next_year}.xlsx"
                )
                st.markdown("**Listar alumnos registrados**")
                st.caption(
                    "Richmond Studio: CLASS NAME, CLASS CODE, STUDENT NAME, IDENTIFIER, createdAt y lastSignInAt. Solo roles student/teacher."
                )
                run_rs_user_classes_replace_confirmed = _consume_richmondstudio_confirmed_action(
                    "rs_user_classes_replace"
                )
                run_rs_user_classes_remove_confirmed = _consume_richmondstudio_confirmed_action(
                    "rs_user_classes_remove"
                )
                run_rs_user_classes_clear_confirmed = _consume_richmondstudio_confirmed_action(
                    "rs_user_classes_clear"
                )
                run_rs_password_update_confirmed = _consume_richmondstudio_confirmed_action(
                    "rs_users_password_update"
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
                            rs_registered_panel_data = (
                                _load_richmondstudio_registered_panel_data(
                                    rs_token,
                                    timeout=int(timeout),
                                )
                            )
                    except Exception as exc:  # pragma: no cover - UI
                        st.error(f"Error: {exc}")
                        st.stop()

                    registered_panel_state = _store_richmondstudio_registered_panel_data(
                        rs_registered_panel_data
                    )
                    listing_data = (
                        registered_panel_state.get("listing_data")
                        if isinstance(registered_panel_state.get("listing_data"), dict)
                        else {}
                    )
                    rows_rs = list(registered_panel_state.get("rows") or [])
                    excluded_roles = (
                        registered_panel_state.get("excluded_roles")
                        if isinstance(registered_panel_state.get("excluded_roles"), dict)
                        else {}
                    )
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
                registered_user_rows_cached = list(
                    st.session_state.get("rs_registered_user_rows") or []
                )

                st.markdown(
                    f"**Usuarios con suscripciones que expiran desde {expiring_next_year_label}**"
                )
                st.caption(
                    "Consulta los usuarios RS que tengan al menos una suscripcion con expirationDate en el proximo ano."
                )
                if st.button(
                    f"Listar suscripciones que expiran en {expiring_next_year}",
                    key="rs_expiring_next_year_list_btn",
                    use_container_width=True,
                ):
                    if not rs_token:
                        st.error("Ingresa el bearer token de Richmond Studio.")
                    elif not registered_user_rows_cached:
                        st.warning(
                            "Primero ejecuta `Listar alumnos registrados` para cargar usuarios RS."
                        )
                    else:
                        status_placeholder = st.empty()
                        progress_placeholder = st.empty()
                        progress_bar = progress_placeholder.progress(0)
                        try:
                            expiring_summary, expiring_rows = (
                                _list_richmondstudio_users_with_subscriptions_expiring_in_year(
                                    token=rs_token,
                                    rows=registered_user_rows_cached,
                                    timeout=int(timeout),
                                    target_year=int(expiring_next_year),
                                    on_status=lambda message: status_placeholder.write(
                                        message
                                    ),
                                    on_progress=lambda current, total: progress_bar.progress(
                                        min(
                                            100,
                                            max(
                                                0,
                                                int(
                                                    (float(current) / max(int(total), 1))
                                                    * 100
                                                ),
                                            ),
                                        )
                                    ),
                                )
                            )
                        except Exception as exc:  # pragma: no cover - UI
                            status_placeholder.empty()
                            progress_placeholder.empty()
                            st.error(f"Error RS: {exc}")
                        else:
                            status_placeholder.empty()
                            progress_placeholder.empty()
                            st.session_state["rs_expiring_next_year_summary"] = dict(
                                expiring_summary
                            )
                            st.session_state["rs_expiring_next_year_rows"] = list(
                                expiring_rows
                            )
                            st.session_state["rs_expiring_next_year_bytes"] = (
                                _export_simple_excel(
                                    expiring_rows,
                                    sheet_name=expiring_next_year_sheet,
                                )
                                if expiring_rows
                                else b""
                            )
                            st.success(
                                "Consulta de suscripciones RS completada. "
                                "Usuarios revisados: {processed_total}/{eligible_total} | "
                                "Usuarios con coincidencia: {matched_total} | "
                                "Suscripciones detectadas: {subscriptions_total} | "
                                "Errores: {error_total}.".format(
                                    **expiring_summary
                                )
                            )

                expiring_summary_cached = (
                    st.session_state.get("rs_expiring_next_year_summary") or {}
                )
                expiring_rows_cached = (
                    st.session_state.get("rs_expiring_next_year_rows") or []
                )
                expiring_bytes_cached = (
                    st.session_state.get("rs_expiring_next_year_bytes") or b""
                )
                if expiring_summary_cached:
                    st.markdown(
                        f"**Resultado suscripciones que expiran desde {expiring_next_year_label}**"
                    )
                    st.info(
                        "Usuarios revisados: {processed_total}/{eligible_total} | "
                        "Usuarios con coincidencia: {matched_total} | "
                        "Suscripciones detectadas: {subscriptions_total} | "
                        "Errores: {error_total}".format(
                            **expiring_summary_cached
                        )
                    )
                    if expiring_rows_cached:
                        _show_dataframe(
                            expiring_rows_cached[:200],
                            use_container_width=True,
                        )
                    else:
                        st.caption(
                            f"No se encontraron suscripciones con expirationDate en {expiring_next_year}."
                        )
                    if expiring_bytes_cached:
                        st.download_button(
                            label=(
                                "Descargar usuarios con suscripciones que expiran en "
                                f"{expiring_next_year}"
                            ),
                            data=expiring_bytes_cached,
                            file_name=expiring_next_year_file_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="rs_expiring_next_year_download",
                            use_container_width=True,
                        )

                if st.session_state.get("rs_excel_bytes"):
                    col_rs_download_a, col_rs_download_b = st.columns(
                        2, gap="small"
                    )
                    col_rs_download_a.download_button(
                        label="Descargar listado RS",
                        data=st.session_state["rs_excel_bytes"],
                        file_name="excel_rs.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="rs_rs_excel_download",
                        use_container_width=True,
                    )
                    if st.session_state.get("rs_password_update_template_bytes"):
                        col_rs_download_b.download_button(
                            label="Descargar todos los usuarios para actualizar",
                            data=st.session_state["rs_password_update_template_bytes"],
                            file_name=str(
                                st.session_state.get(
                                    "rs_password_update_template_filename"
                                )
                                or "plantilla_password_rs.xlsx"
                            ).strip()
                            or "plantilla_password_rs.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="rs_password_update_template_download_list",
                            use_container_width=True,
                        )

                st.markdown("**Gestionar clases de usuario RS**")
                st.caption(
                    "Usa el listado registrado para seleccionar un usuario, reemplazar sus clases, "
                    "quitar algunas o dejarlo sin clases."
                )

                rs_user_classes_notice = str(
                    st.session_state.pop("rs_user_classes_notice", "") or ""
                ).strip()
                if rs_user_classes_notice:
                    st.success(rs_user_classes_notice)

                registered_user_rows_cached = [
                    dict(row)
                    for row in st.session_state.get("rs_registered_user_rows") or []
                    if isinstance(row, dict)
                ]
                registered_groups_lookup = (
                    st.session_state.get("rs_registered_groups_lookup")
                    if isinstance(
                        st.session_state.get("rs_registered_groups_lookup"), dict
                    )
                    else {"by_id": {}, "by_name": {}}
                )
                registered_groups_by_id = (
                    registered_groups_lookup.get("by_id")
                    if isinstance(registered_groups_lookup.get("by_id"), dict)
                    else {}
                )
                registered_user_rows_by_id = {
                    str(row.get("RS USER ID") or "").strip(): row
                    for row in registered_user_rows_cached
                    if str(row.get("RS USER ID") or "").strip()
                }
                registered_user_options = list(registered_user_rows_by_id.keys())
                registered_group_options = sorted(
                    list(registered_groups_by_id.keys()),
                    key=lambda group_id: _richmondstudio_group_label(
                        registered_groups_by_id.get(group_id)
                    ).upper(),
                )
                valid_registered_group_ids = set(registered_group_options)
                confirmed_user_classes_mode = ""
                if run_rs_user_classes_replace_confirmed:
                    confirmed_user_classes_mode = "replace"
                elif run_rs_user_classes_remove_confirmed:
                    confirmed_user_classes_mode = "remove"
                elif run_rs_user_classes_clear_confirmed:
                    confirmed_user_classes_mode = "clear"

                if confirmed_user_classes_mode:
                    pending_user_classes = st.session_state.pop(
                        "rs_user_classes_pending_action",
                        None,
                    )
                    if not isinstance(pending_user_classes, dict):
                        st.error("No se encontro el borrador de clases RS del usuario.")
                    elif (
                        str(pending_user_classes.get("mode") or "").strip()
                        != confirmed_user_classes_mode
                    ):
                        st.error(
                            "La confirmacion no coincide con la accion de clases RS."
                        )
                    elif not rs_token:
                        st.error("Ingresa el bearer token de Richmond Studio.")
                    else:
                        target_user_id = str(
                            pending_user_classes.get("user_id") or ""
                        ).strip()
                        try:
                            with st.spinner("Actualizando clases del usuario en RS..."):
                                detail_body = _fetch_richmondstudio_user_detail(
                                    rs_token,
                                    target_user_id,
                                    timeout=int(timeout),
                                )
                                detail_data = (
                                    detail_body.get("data")
                                    if isinstance(detail_body.get("data"), dict)
                                    else {}
                                )
                                current_group_ids = _richmondstudio_relationship_ids(
                                    detail_data,
                                    "groups",
                                )
                                final_group_ids = list(current_group_ids)
                                if confirmed_user_classes_mode == "replace":
                                    final_group_ids = [
                                        str(group_id or "").strip()
                                        for group_id in (
                                            pending_user_classes.get("target_group_ids")
                                            or []
                                        )
                                        if str(group_id or "").strip()
                                    ]
                                elif confirmed_user_classes_mode == "remove":
                                    remove_group_ids = {
                                        str(group_id or "").strip()
                                        for group_id in (
                                            pending_user_classes.get("remove_group_ids")
                                            or []
                                        )
                                        if str(group_id or "").strip()
                                    }
                                    final_group_ids = [
                                        group_id
                                        for group_id in current_group_ids
                                        if group_id not in remove_group_ids
                                    ]
                                elif confirmed_user_classes_mode == "clear":
                                    final_group_ids = []

                                payload_user_classes = (
                                    _build_richmondstudio_user_patch_payload_from_detail(
                                        detail_body,
                                        group_ids=final_group_ids,
                                    )
                                )
                                _update_richmondstudio_user(
                                    rs_token,
                                    target_user_id,
                                    payload_user_classes,
                                    timeout=int(timeout),
                                )
                                refreshed_registered_panel_data = (
                                    _load_richmondstudio_registered_panel_data(
                                        rs_token,
                                        timeout=int(timeout),
                                    )
                                )
                        except Exception as exc:  # pragma: no cover - UI
                            st.error(f"Error RS: {exc}")
                        else:
                            _store_richmondstudio_registered_panel_data(
                                refreshed_registered_panel_data
                            )
                            st.session_state["rs_user_classes_selected_user_id"] = (
                                target_user_id
                            )
                            st.session_state["rs_user_classes_form_loaded_user_id"] = ""
                            st.session_state["rs_user_classes_remove_group_ids"] = []
                            action_label = (
                                "Clases del usuario RS actualizadas correctamente."
                                if confirmed_user_classes_mode == "replace"
                                else (
                                    "Clases seleccionadas removidas correctamente."
                                    if confirmed_user_classes_mode == "remove"
                                    else "Todas las clases del usuario fueron removidas."
                                )
                            )
                            st.session_state["rs_user_classes_notice"] = action_label
                            st.rerun()

                if not registered_user_rows_cached:
                    st.caption(
                        "Primero ejecuta `Listar alumnos registrados` para cargar usuarios y clases."
                    )
                else:
                    current_selected_user_id = str(
                        st.session_state.get("rs_user_classes_selected_user_id") or ""
                    ).strip()
                    if current_selected_user_id not in registered_user_rows_by_id:
                        current_selected_user_id = registered_user_options[0]
                        st.session_state["rs_user_classes_selected_user_id"] = (
                            current_selected_user_id
                        )

                    user_option_labels: Dict[str, str] = {}
                    for user_id, row in registered_user_rows_by_id.items():
                        full_name = " ".join(
                            part
                            for part in (
                                str(row.get("First name") or "").strip(),
                                str(row.get("Last name") or "").strip(),
                            )
                            if part
                        ).strip()
                        login_txt = str(row.get("Username") or row.get("Login") or "").strip()
                        identifier_txt = str(row.get("IDENTIFIER") or "").strip()
                        role_txt = str(row.get("Role") or "").strip().lower() or "user"
                        classes_txt = str(row.get("Classes count") or "0").strip() or "0"
                        main_label = full_name or login_txt or identifier_txt or user_id
                        extra_label = login_txt or identifier_txt
                        if extra_label:
                            user_option_labels[user_id] = (
                                f"{main_label} | {extra_label} | {role_txt} | {classes_txt} clase(s)"
                            )
                        else:
                            user_option_labels[user_id] = (
                                f"{main_label} | {role_txt} | {classes_txt} clase(s)"
                            )

                    selected_user_id = str(
                        st.selectbox(
                            "Usuario RS",
                            options=registered_user_options,
                            key="rs_user_classes_selected_user_id",
                            format_func=lambda user_id: user_option_labels.get(
                                str(user_id or "").strip(),
                                str(user_id or "").strip(),
                            ),
                        )
                        or ""
                    ).strip()
                    selected_user_row = (
                        registered_user_rows_by_id.get(selected_user_id) or {}
                    )

                    loaded_user_id = str(
                        st.session_state.get("rs_user_classes_form_loaded_user_id") or ""
                    ).strip()
                    if loaded_user_id != selected_user_id:
                        current_group_ids = []
                        for item in selected_user_row.get("_group_ids") or []:
                            group_id = str(item or "").strip()
                            if group_id and group_id in valid_registered_group_ids:
                                current_group_ids.append(group_id)
                        st.session_state["rs_user_classes_target_group_ids"] = current_group_ids
                        st.session_state["rs_user_classes_remove_group_ids"] = []
                        st.session_state["rs_user_classes_form_loaded_user_id"] = (
                            selected_user_id
                        )

                    current_group_ids = []
                    for item in selected_user_row.get("_group_ids") or []:
                        group_id = str(item or "").strip()
                        if group_id and group_id in valid_registered_group_ids:
                            current_group_ids.append(group_id)

                    selected_name = " ".join(
                        part
                        for part in (
                            str(selected_user_row.get("First name") or "").strip(),
                            str(selected_user_row.get("Last name") or "").strip(),
                        )
                        if part
                    ).strip()
                    if not selected_name:
                        selected_name = (
                            str(selected_user_row.get("Username") or "").strip()
                            or str(selected_user_row.get("IDENTIFIER") or "").strip()
                            or selected_user_id
                        )

                    current_group_labels = [
                        _richmondstudio_group_label(registered_groups_by_id.get(group_id))
                        or group_id
                        for group_id in current_group_ids
                    ]
                    st.caption(
                        "Seleccionado: {name} | Email: {email} | Role: {role} | Clases actuales: {classes}".format(
                            name=selected_name or "(sin nombre)",
                            email=str(
                                selected_user_row.get("Email")
                                or selected_user_row.get("Username")
                                or ""
                            ).strip()
                            or "(sin email)",
                            role=str(selected_user_row.get("Role") or "").strip() or "-",
                            classes=(
                                " | ".join(current_group_labels)
                                if current_group_labels
                                else "sin clases"
                            ),
                        )
                    )

                    st.multiselect(
                        "Clases finales del usuario",
                        options=registered_group_options,
                        key="rs_user_classes_target_group_ids",
                        format_func=lambda group_id: _richmondstudio_group_label(
                            registered_groups_by_id.get(str(group_id or "").strip())
                        )
                        or str(group_id or "").strip(),
                        placeholder="Selecciona una o varias clases",
                    )
                    st.multiselect(
                        "Clases actuales a quitar",
                        options=current_group_ids,
                        key="rs_user_classes_remove_group_ids",
                        format_func=lambda group_id: _richmondstudio_group_label(
                            registered_groups_by_id.get(str(group_id or "").strip())
                        )
                        or str(group_id or "").strip(),
                        placeholder="Marca una o varias clases actuales",
                        disabled=not bool(current_group_ids),
                    )

                    (
                        col_rs_user_classes_a,
                        col_rs_user_classes_b,
                        col_rs_user_classes_c,
                    ) = st.columns(3, gap="small")
                    run_rs_user_classes_replace = col_rs_user_classes_a.button(
                        "Guardar clases RS",
                        type="primary",
                        key="rs_user_classes_replace_btn",
                        use_container_width=True,
                    )
                    run_rs_user_classes_remove = col_rs_user_classes_b.button(
                        "Quitar clases marcadas",
                        key="rs_user_classes_remove_btn",
                        use_container_width=True,
                    )
                    run_rs_user_classes_clear = col_rs_user_classes_c.button(
                        "Quitar todas las clases",
                        key="rs_user_classes_clear_btn",
                        use_container_width=True,
                    )

                    if run_rs_user_classes_replace:
                        replace_group_ids = [
                            str(group_id or "").strip()
                            for group_id in (
                                st.session_state.get("rs_user_classes_target_group_ids")
                                or []
                            )
                            if str(group_id or "").strip()
                        ]
                        st.session_state["rs_user_classes_pending_action"] = {
                            "mode": "replace",
                            "user_id": selected_user_id,
                            "target_group_ids": replace_group_ids,
                        }
                        _request_richmondstudio_confirmation(
                            "rs_user_classes_replace",
                            (
                                f"actualizar clases RS de {selected_name} "
                                f"({len(replace_group_ids)} clase(s) finales)"
                            ),
                        )

                    if run_rs_user_classes_remove:
                        remove_group_ids = [
                            str(group_id or "").strip()
                            for group_id in (
                                st.session_state.get("rs_user_classes_remove_group_ids")
                                or []
                            )
                            if str(group_id or "").strip()
                        ]
                        if not remove_group_ids:
                            st.error("Selecciona al menos una clase actual para quitar.")
                        else:
                            st.session_state["rs_user_classes_pending_action"] = {
                                "mode": "remove",
                                "user_id": selected_user_id,
                                "remove_group_ids": remove_group_ids,
                            }
                            _request_richmondstudio_confirmation(
                                "rs_user_classes_remove",
                                (
                                    f"quitar {len(remove_group_ids)} clase(s) RS de "
                                    f"{selected_name}"
                                ),
                            )

                    if run_rs_user_classes_clear:
                        st.session_state["rs_user_classes_pending_action"] = {
                            "mode": "clear",
                            "user_id": selected_user_id,
                        }
                        _request_richmondstudio_confirmation(
                            "rs_user_classes_clear",
                            f"quitar todas las clases RS de {selected_name}",
                        )

                st.markdown("**Actualizar password usuarios RS**")
                st.caption(
                    "Descarga el Excel de todos los usuarios registrados, completa New password(optional) "
                    "y vuelve a subirlo. La app lo convierte a CSV y lo envia al endpoint bulk de RS."
                )
                uploaded_rs_password_update = st.file_uploader(
                    "Plantilla de actualizacion password RS",
                    type=["xlsx", "csv", "txt"],
                    key="rs_password_update_upload_file",
                    help=(
                        "Columnas esperadas: Username(Email), New last name(optional), "
                        "New first name(optional), New class code(optional), "
                        "New password(optional), Keep in classes(optional)."
                    ),
                )
                rs_password_update_bytes = b""
                rs_password_update_name = ""
                rs_password_update_rows: List[Dict[str, str]] = []
                rs_password_update_error = ""
                if uploaded_rs_password_update is not None:
                    rs_password_update_bytes = uploaded_rs_password_update.getvalue()
                    rs_password_update_name = str(
                        uploaded_rs_password_update.name or "password_update_rs.xlsx"
                    ).strip()
                    try:
                        rs_password_update_rows = _load_richmondstudio_bulk_user_update_rows(
                            rs_password_update_bytes,
                            rs_password_update_name,
                        )
                    except Exception as exc:
                        rs_password_update_error = str(exc)
                        st.error(f"Error en plantilla RS: {exc}")
                    else:
                        preview_rows = _build_richmondstudio_bulk_user_update_preview_rows(
                            rs_password_update_rows
                        )
                        actionable_count = sum(
                            1
                            for row in rs_password_update_rows
                            if str(row.get("Username") or "").strip()
                            and str(row.get("New password") or "").strip()
                        )
                        st.caption(
                            "Filas cargadas: {total} | Listas para actualizar: {actionable}".format(
                                total=len(rs_password_update_rows),
                                actionable=actionable_count,
                            )
                        )
                        if preview_rows:
                            _show_dataframe(preview_rows[:200], use_container_width=True)

                run_rs_password_update = st.button(
                    "Actualizar passwords RS",
                    type="primary",
                    key="rs_password_update_run_btn",
                    use_container_width=True,
                )
                if run_rs_password_update:
                    if not rs_token:
                        st.error("Ingresa el bearer token de Richmond Studio.")
                    elif uploaded_rs_password_update is None:
                        st.error("Sube la plantilla de actualizacion password RS.")
                    elif rs_password_update_error:
                        st.error(
                            f"Corrige la plantilla antes de continuar: {rs_password_update_error}"
                        )
                    else:
                        actionable_count = sum(
                            1
                            for row in rs_password_update_rows
                            if str(row.get("Username") or "").strip()
                            and str(row.get("New password") or "").strip()
                        )
                        if not actionable_count:
                            st.error(
                                "No hay filas con Username y New password para actualizar."
                            )
                        else:
                            st.session_state["rs_password_update_upload_bytes"] = (
                                rs_password_update_bytes
                            )
                            st.session_state["rs_password_update_upload_name"] = (
                                rs_password_update_name
                            )
                            _request_richmondstudio_confirmation(
                                "rs_users_password_update",
                                f"actualizar password de {actionable_count} usuarios RS",
                            )

                if run_rs_password_update_confirmed:
                    stored_upload_bytes = bytes(
                        st.session_state.get("rs_password_update_upload_bytes") or b""
                    )
                    stored_upload_name = str(
                        st.session_state.get("rs_password_update_upload_name") or ""
                    ).strip()
                    if not rs_token:
                        st.error("Ingresa el bearer token de Richmond Studio.")
                    elif not stored_upload_bytes:
                        st.error("No se encontro la plantilla cargada para actualizar.")
                    else:
                        try:
                            rows_to_update = _load_richmondstudio_bulk_user_update_rows(
                                stored_upload_bytes,
                                stored_upload_name or "password_update_rs.xlsx",
                            )
                            actionable_rows = [
                                row
                                for row in rows_to_update
                                if str(row.get("Username") or "").strip()
                                and str(row.get("New password") or "").strip()
                            ]
                            response_message = _submit_richmondstudio_bulk_user_update(
                                rs_token,
                                actionable_rows,
                                timeout=max(120, int(timeout)),
                            )
                        except Exception as exc:
                            st.error(f"No se pudo actualizar passwords RS: {exc}")
                        else:
                            st.success(
                                "Actualizacion bulk RS enviada correctamente. "
                                f"Filas procesadas: {len(actionable_rows)} | Respuesta: {response_message}"
                            )

    if isinstance(st.session_state.get("rs_pending_confirmation"), dict):
        _render_richmondstudio_confirmation_dialog()
